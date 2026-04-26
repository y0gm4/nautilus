//! PostgreSQL row stream and value decoder.

use crate::error::{ConnectorError as Error, Result};
use crate::row_stream::RowStream;
use crate::Row;
use nautilus_core::Value;
use sqlx::postgres::types::PgHstore;
use sqlx::postgres::PgRow;
use sqlx::{Column, Row as SqlxRow, TypeInfo, ValueRef};
use uuid::Uuid;

/// Stream type for PostgreSQL query results.
///
/// A thin alias for the shared [`RowStream`] type.
pub type PgRowStream = RowStream;

/// Decode a sqlx `PgRow` into a Nautilus `Row`.
///
/// This function is public within the crate for use by the postgres executor.
pub(crate) fn decode_row_internal(row: PgRow) -> Result<Row> {
    let columns = row.columns();
    let mut row_data = Vec::with_capacity(columns.len());

    for (i, column) in columns.iter().enumerate() {
        let name = column.name().to_string();
        let type_info = column.type_info();
        let value = decode_value(&row, i, type_info)?;
        row_data.push((name, value));
    }

    Ok(Row::new(row_data))
}

/// Decode a value from a sqlx row by index and type.
fn decode_value(row: &PgRow, idx: usize, type_info: &sqlx::postgres::PgTypeInfo) -> Result<Value> {
    let type_name = type_info.name();
    let normalized_type_name = type_name.to_ascii_uppercase();

    if let Ok(is_null) = sqlx::Row::try_get_raw(row, idx).map(|raw| raw.is_null()) {
        if is_null {
            return Ok(Value::Null);
        }
    }

    match normalized_type_name.as_str() {
        "BOOL" => row
            .try_get::<bool, _>(idx)
            .map(Value::Bool)
            .map_err(|e| Error::row_decode(e, "Failed to decode BOOL")),

        "INT2" | "INT4" | "INT8" | "SERIAL" | "BIGSERIAL" => {
            if let Ok(val) = row.try_get::<i64, _>(idx) {
                Ok(Value::I64(val))
            } else if let Ok(val) = row.try_get::<i32, _>(idx) {
                Ok(Value::I64(val as i64))
            } else if let Ok(val) = row.try_get::<i16, _>(idx) {
                Ok(Value::I64(val as i64))
            } else {
                Err(Error::row_decode_msg(format!(
                    "Failed to decode integer type: {}",
                    type_name
                )))
            }
        }

        "FLOAT4" | "FLOAT8" | "REAL" | "DOUBLE PRECISION" => {
            if let Ok(val) = row.try_get::<f64, _>(idx) {
                Ok(Value::F64(val))
            } else if let Ok(val) = row.try_get::<f32, _>(idx) {
                Ok(Value::F64(val as f64))
            } else {
                Err(Error::row_decode_msg(format!(
                    "Failed to decode float type: {}",
                    type_name
                )))
            }
        }

        "VARCHAR" | "TEXT" | "CHAR" | "BPCHAR" | "NAME" | "CITEXT" | "LTREE" => row
            .try_get::<String, _>(idx)
            .map(Value::String)
            .map_err(|e| Error::row_decode(e, "Failed to decode string")),

        "HSTORE" => row
            .try_get::<PgHstore, _>(idx)
            .map(|map| Value::Hstore(map.0))
            .map_err(|e| Error::row_decode(e, "Failed to decode HSTORE")),

        "VECTOR" => sqlx::Row::try_get_unchecked::<String, _>(row, idx)
            .map_err(|e| Error::row_decode(e, "Failed to decode VECTOR"))
            .and_then(|raw| parse_pg_vector(&raw)),

        "BYTEA" => row
            .try_get::<Vec<u8>, _>(idx)
            .map(Value::Bytes)
            .map_err(|e| Error::row_decode(e, "Failed to decode bytes")),

        "UUID" => row
            .try_get::<Uuid, _>(idx)
            .map(Value::Uuid)
            .map_err(|e| Error::row_decode(e, "Failed to decode UUID")),

        "TIMESTAMP" => row
            .try_get::<chrono::NaiveDateTime, _>(idx)
            .map(Value::DateTime)
            .map_err(|e| Error::row_decode(e, "Failed to decode TIMESTAMP")),

        "TIMESTAMPTZ" => row
            .try_get::<chrono::DateTime<chrono::Utc>, _>(idx)
            .map(|dt| Value::DateTime(dt.naive_utc()))
            .map_err(|e| Error::row_decode(e, "Failed to decode TIMESTAMPTZ")),

        "DATE" => row
            .try_get::<chrono::NaiveDate, _>(idx)
            .map(|d| {
                Value::DateTime(
                    d.and_hms_opt(0, 0, 0)
                        .expect("midnight (0, 0, 0) is always a valid time"),
                )
            })
            .map_err(|e| Error::row_decode(e, "Failed to decode DATE")),

        "TIME" => row
            .try_get::<chrono::NaiveTime, _>(idx)
            .map(|t| Value::String(t.to_string()))
            .map_err(|e| Error::row_decode(e, "Failed to decode TIME")),

        "NUMERIC" => row
            .try_get::<rust_decimal::Decimal, _>(idx)
            .map(Value::Decimal)
            .map_err(|e| Error::row_decode(e, "Failed to decode NUMERIC")),

        // Handle PostgreSQL 2D array types (TEXT[][], INT4[][], etc.)
        // sqlx doesn't support 2D array decoding natively, so we decode
        // the text representation and parse the PostgreSQL array literal.
        _ if normalized_type_name.ends_with("[][]") => {
            let element_type = &normalized_type_name[..normalized_type_name.len() - 4];
            row.try_get::<String, _>(idx)
                .map_err(|e| Error::row_decode(e, "Failed to decode 2D array"))
                .and_then(|s| parse_pg_2d_array(&s, element_type))
        }

        _ if normalized_type_name.ends_with("[]") => {
            let element_type = &normalized_type_name[..normalized_type_name.len() - 2];
            match element_type {
                "TEXT" | "VARCHAR" | "CHAR" | "BPCHAR" | "NAME" | "CITEXT" | "LTREE" => row
                    .try_get::<Vec<String>, _>(idx)
                    .map(|vec| Value::Array(vec.into_iter().map(Value::String).collect()))
                    .map_err(|e| Error::row_decode(e, "Failed to decode TEXT[]")),
                "HSTORE" => row
                    .try_get::<Vec<PgHstore>, _>(idx)
                    .map(|vec| {
                        Value::Array(vec.into_iter().map(|item| Value::Hstore(item.0)).collect())
                    })
                    .map_err(|e| Error::row_decode(e, "Failed to decode HSTORE[]")),
                "INT2" | "INT4" => row
                    .try_get::<Vec<i32>, _>(idx)
                    .map(|vec| Value::Array(vec.into_iter().map(Value::I32).collect()))
                    .map_err(|e| Error::row_decode(e, "Failed to decode INT[]")),
                "INT8" | "BIGINT" => row
                    .try_get::<Vec<i64>, _>(idx)
                    .map(|vec| Value::Array(vec.into_iter().map(Value::I64).collect()))
                    .map_err(|e| Error::row_decode(e, "Failed to decode BIGINT[]")),
                "FLOAT4" | "FLOAT8" | "REAL" | "DOUBLE PRECISION" => row
                    .try_get::<Vec<f64>, _>(idx)
                    .map(|vec| Value::Array(vec.into_iter().map(Value::F64).collect()))
                    .map_err(|e| Error::row_decode(e, "Failed to decode FLOAT[]")),
                "BOOL" => row
                    .try_get::<Vec<bool>, _>(idx)
                    .map(|vec| Value::Array(vec.into_iter().map(Value::Bool).collect()))
                    .map_err(|e| Error::row_decode(e, "Failed to decode BOOL[]")),
                _ => Err(Error::row_decode_msg(format!(
                    "Unsupported array element type: {}",
                    element_type
                ))),
            }
        }

        "JSON" | "JSONB" => row
            .try_get::<serde_json::Value, _>(idx)
            .map(Value::Json)
            .map_err(|e| Error::row_decode(e, "Failed to decode JSON")),

        _ => {
            // For unknown types (custom enums, domains, composite types, etc.)
            // we bypass sqlx's type-compatibility check so the raw text
            // representation is returned regardless of the server-side type OID.
            sqlx::Row::try_get_unchecked::<String, _>(row, idx)
                .map(Value::String)
                .map_err(|e| {
                    Error::row_decode_msg(format!(
                        "Unsupported type '{}' at column {}: {}",
                        type_name, idx, e
                    ))
                })
        }
    }
}

fn parse_pg_vector(input: &str) -> Result<Value> {
    let trimmed = input.trim();
    let Some(inner) = trimmed.strip_prefix('[').and_then(|s| s.strip_suffix(']')) else {
        return Err(Error::row_decode_msg(format!(
            "Invalid vector literal: {}",
            input
        )));
    };

    if inner.trim().is_empty() {
        return Ok(Value::Vector(Vec::new()));
    }

    let mut values = Vec::new();
    for (idx, raw) in inner.split(',').enumerate() {
        let value = raw.trim().parse::<f32>().map_err(|e| {
            Error::row_decode_msg(format!(
                "Invalid vector element at index {} in {:?}: {}",
                idx, input, e
            ))
        })?;
        if !value.is_finite() {
            return Err(Error::row_decode_msg(format!(
                "Invalid non-finite vector element at index {} in {:?}",
                idx, input
            )));
        }
        values.push(value);
    }

    Ok(Value::Vector(values))
}

/// Parse a PostgreSQL 2D array literal (e.g. `{{1,2},{3,4}}`) into `Value::Array2D`.
fn parse_pg_2d_array(input: &str, element_type: &str) -> Result<Value> {
    let trimmed = input.trim();
    if !trimmed.starts_with('{') || !trimmed.ends_with('}') {
        return Err(Error::row_decode_msg(format!(
            "Invalid 2D array literal: {}",
            input
        )));
    }

    let inner = &trimmed[1..trimmed.len() - 1];
    let rows = split_pg_inner_arrays(inner)?;

    let mut result = Vec::with_capacity(rows.len());
    for row_str in rows {
        let elements = split_pg_array_elements(row_str)?;
        let row: Vec<Value> = elements
            .into_iter()
            .map(|elem| parse_pg_element(elem, element_type))
            .collect::<Result<_>>()?;
        result.push(row);
    }

    Ok(Value::Array2D(result))
}

/// Split the inner content of a 2D array into individual sub-array strings.
///
/// Input: `{1,2},{3,4}` -> `["1,2", "3,4"]`
fn split_pg_inner_arrays(input: &str) -> Result<Vec<&str>> {
    let mut arrays = Vec::new();
    let mut depth = 0;
    let mut start = None;

    for (i, ch) in input.char_indices() {
        match ch {
            '{' => {
                if depth == 0 {
                    start = Some(i + 1);
                }
                depth += 1;
            }
            '}' => {
                depth -= 1;
                if depth == 0 {
                    let s = start.ok_or_else(|| {
                        Error::row_decode_msg("Malformed 2D array: unmatched brace".to_string())
                    })?;
                    arrays.push(&input[s..i]);
                    start = None;
                }
            }
            _ => {}
        }
    }

    if depth != 0 {
        return Err(Error::row_decode_msg(
            "Malformed 2D array: unbalanced braces".to_string(),
        ));
    }

    Ok(arrays)
}

/// Split a comma-separated list of PostgreSQL array elements, respecting quoted strings.
///
/// Input: `"hello","world"` -> `[r#""hello""#, r#""world""#]`
/// Input: `1,2,NULL` -> `["1", "2", "NULL"]`
fn split_pg_array_elements(input: &str) -> Result<Vec<&str>> {
    let mut elements = Vec::new();
    let mut start = 0;
    let mut in_quotes = false;
    let mut i = 0;
    let bytes = input.as_bytes();

    while i < bytes.len() {
        match bytes[i] {
            b'"' => {
                in_quotes = !in_quotes;
            }
            b'\\' if in_quotes => {
                i += 1;
            }
            b',' if !in_quotes => {
                elements.push(&input[start..i]);
                start = i + 1;
            }
            _ => {}
        }
        i += 1;
    }

    if start <= input.len() {
        elements.push(&input[start..]);
    }

    Ok(elements)
}

/// Parse a single PostgreSQL array element string into a `Value`.
fn parse_pg_element(elem: &str, element_type: &str) -> Result<Value> {
    let trimmed = elem.trim();

    if trimmed.eq_ignore_ascii_case("NULL") {
        return Ok(Value::Null);
    }

    match element_type {
        "TEXT" | "VARCHAR" | "CHAR" | "BPCHAR" => Ok(Value::String(unquote_pg_string(trimmed))),
        "INT2" | "INT4" => trimmed
            .parse::<i32>()
            .map(Value::I32)
            .map_err(|e| Error::row_decode_msg(format!("Invalid integer '{}': {}", trimmed, e))),
        "INT8" | "BIGINT" => trimmed
            .parse::<i64>()
            .map(Value::I64)
            .map_err(|e| Error::row_decode_msg(format!("Invalid bigint '{}': {}", trimmed, e))),
        "FLOAT4" | "FLOAT8" | "REAL" | "DOUBLE PRECISION" => trimmed
            .parse::<f64>()
            .map(Value::F64)
            .map_err(|e| Error::row_decode_msg(format!("Invalid float '{}': {}", trimmed, e))),
        "BOOL" => match trimmed {
            "t" | "true" | "TRUE" => Ok(Value::Bool(true)),
            "f" | "false" | "FALSE" => Ok(Value::Bool(false)),
            _ => Err(Error::row_decode_msg(format!(
                "Invalid boolean: {}",
                trimmed
            ))),
        },
        _ => Ok(Value::String(unquote_pg_string(trimmed))),
    }
}

/// Remove surrounding double-quotes and unescape backslash sequences.
fn unquote_pg_string(s: &str) -> String {
    if s.len() >= 2 && s.starts_with('"') && s.ends_with('"') {
        let inner = &s[1..s.len() - 1];
        let mut result = String::with_capacity(inner.len());
        let mut chars = inner.chars();
        while let Some(ch) = chars.next() {
            if ch == '\\' {
                if let Some(escaped) = chars.next() {
                    result.push(escaped);
                }
            } else {
                result.push(ch);
            }
        }
        result
    } else {
        s.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_2d_int_array() {
        let result = parse_pg_2d_array("{{1,2},{3,4}}", "INT4").unwrap();
        assert_eq!(
            result,
            Value::Array2D(vec![
                vec![Value::I32(1), Value::I32(2)],
                vec![Value::I32(3), Value::I32(4)],
            ])
        );
    }

    #[test]
    fn parse_2d_bigint_array() {
        let result = parse_pg_2d_array("{{100,200},{300,400}}", "INT8").unwrap();
        assert_eq!(
            result,
            Value::Array2D(vec![
                vec![Value::I64(100), Value::I64(200)],
                vec![Value::I64(300), Value::I64(400)],
            ])
        );
    }

    #[test]
    fn parse_2d_text_array() {
        let result = parse_pg_2d_array(r#"{{"hello","world"},{"foo","bar"}}"#, "TEXT").unwrap();
        assert_eq!(
            result,
            Value::Array2D(vec![
                vec![
                    Value::String("hello".to_string()),
                    Value::String("world".to_string())
                ],
                vec![
                    Value::String("foo".to_string()),
                    Value::String("bar".to_string())
                ],
            ])
        );
    }

    #[test]
    fn parse_2d_float_array() {
        let result = parse_pg_2d_array("{{1.5,2.5},{3.5,4.5}}", "FLOAT8").unwrap();
        assert_eq!(
            result,
            Value::Array2D(vec![
                vec![Value::F64(1.5), Value::F64(2.5)],
                vec![Value::F64(3.5), Value::F64(4.5)],
            ])
        );
    }

    #[test]
    fn parse_vector_literal() {
        assert_eq!(
            parse_pg_vector("[1,2.5,3.25]").unwrap(),
            Value::Vector(vec![1.0, 2.5, 3.25])
        );
    }

    #[test]
    fn parse_vector_rejects_invalid_literal() {
        assert!(parse_pg_vector("{1,2,3}").is_err());
    }

    #[test]
    fn parse_2d_bool_array() {
        let result = parse_pg_2d_array("{{t,f},{f,t}}", "BOOL").unwrap();
        assert_eq!(
            result,
            Value::Array2D(vec![
                vec![Value::Bool(true), Value::Bool(false)],
                vec![Value::Bool(false), Value::Bool(true)],
            ])
        );
    }

    #[test]
    fn parse_2d_array_with_nulls() {
        let result = parse_pg_2d_array("{{1,NULL},{NULL,4}}", "INT4").unwrap();
        assert_eq!(
            result,
            Value::Array2D(vec![
                vec![Value::I32(1), Value::Null],
                vec![Value::Null, Value::I32(4)],
            ])
        );
    }

    #[test]
    fn parse_2d_text_with_escaped_quotes() {
        let result = parse_pg_2d_array(r#"{{"say \"hi\"","normal"}}"#, "TEXT").unwrap();
        assert_eq!(
            result,
            Value::Array2D(vec![vec![
                Value::String("say \"hi\"".to_string()),
                Value::String("normal".to_string())
            ],])
        );
    }

    #[test]
    fn parse_2d_single_row() {
        let result = parse_pg_2d_array("{{1,2,3}}", "INT4").unwrap();
        assert_eq!(
            result,
            Value::Array2D(vec![vec![Value::I32(1), Value::I32(2), Value::I32(3)],])
        );
    }

    #[test]
    fn parse_2d_array_invalid_format() {
        assert!(parse_pg_2d_array("not an array", "INT4").is_err());
    }

    #[test]
    fn unquote_plain_string() {
        assert_eq!(unquote_pg_string("hello"), "hello");
    }

    #[test]
    fn unquote_quoted_string() {
        assert_eq!(unquote_pg_string(r#""hello""#), "hello");
    }

    #[test]
    fn unquote_escaped_string() {
        assert_eq!(unquote_pg_string(r#""say \"hi\"""#), r#"say "hi""#);
    }
}
