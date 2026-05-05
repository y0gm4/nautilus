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
pub type PgRowStream<'conn> = RowStream<'conn>;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PgTypeKind<'a> {
    Bool,
    Int2,
    Int4,
    Int8,
    Float4,
    Float8,
    Text,
    Geometry,
    Geography,
    Hstore,
    Vector,
    Bytes,
    Uuid,
    Timestamp,
    TimestampTz,
    Date,
    Time,
    Numeric,
    Json,
    Array(&'a str),
    Array2D(&'a str),
    Unknown,
}

const PG_SCALAR_TYPE_ALIASES: &[(&[&str], PgTypeKind<'static>)] = &[
    (&["BOOL"], PgTypeKind::Bool),
    (&["INT2"], PgTypeKind::Int2),
    (&["INT4", "SERIAL"], PgTypeKind::Int4),
    (&["INT8", "BIGINT", "BIGSERIAL"], PgTypeKind::Int8),
    (&["FLOAT4", "REAL"], PgTypeKind::Float4),
    (&["FLOAT8", "DOUBLE PRECISION"], PgTypeKind::Float8),
    (
        &[
            "VARCHAR", "TEXT", "CHAR", "BPCHAR", "NAME", "CITEXT", "LTREE",
        ],
        PgTypeKind::Text,
    ),
    (&["GEOMETRY"], PgTypeKind::Geometry),
    (&["GEOGRAPHY"], PgTypeKind::Geography),
    (&["HSTORE"], PgTypeKind::Hstore),
    (&["VECTOR"], PgTypeKind::Vector),
    (&["BYTEA"], PgTypeKind::Bytes),
    (&["UUID"], PgTypeKind::Uuid),
    (&["TIMESTAMP"], PgTypeKind::Timestamp),
    (&["TIMESTAMPTZ"], PgTypeKind::TimestampTz),
    (&["DATE"], PgTypeKind::Date),
    (&["TIME"], PgTypeKind::Time),
    (&["NUMERIC"], PgTypeKind::Numeric),
    (&["JSON", "JSONB"], PgTypeKind::Json),
];

/// Decode a value from a sqlx row by index and type.
fn decode_value(row: &PgRow, idx: usize, type_info: &sqlx::postgres::PgTypeInfo) -> Result<Value> {
    let type_name = type_info.name();

    if let Ok(is_null) = sqlx::Row::try_get_raw(row, idx).map(|raw| raw.is_null()) {
        if is_null {
            return Ok(Value::Null);
        }
    }

    match classify_pg_type(type_name) {
        PgTypeKind::Bool => sqlx::Row::try_get_unchecked::<bool, _>(row, idx)
            .map(Value::Bool)
            .map_err(|e| Error::row_decode(e, "Failed to decode BOOL")),

        PgTypeKind::Int2 => sqlx::Row::try_get_unchecked::<i16, _>(row, idx)
            .map(|value| Value::I64(value as i64))
            .map_err(|e| Error::row_decode(e, "Failed to decode INT2")),

        PgTypeKind::Int4 => sqlx::Row::try_get_unchecked::<i32, _>(row, idx)
            .map(|value| Value::I64(value as i64))
            .map_err(|e| Error::row_decode(e, "Failed to decode INT4")),

        PgTypeKind::Int8 => sqlx::Row::try_get_unchecked::<i64, _>(row, idx)
            .map(Value::I64)
            .map_err(|e| Error::row_decode(e, "Failed to decode INT8")),

        PgTypeKind::Float4 => sqlx::Row::try_get_unchecked::<f32, _>(row, idx)
            .map(|value| Value::F64(value as f64))
            .map_err(|e| Error::row_decode(e, "Failed to decode FLOAT4")),

        PgTypeKind::Float8 => sqlx::Row::try_get_unchecked::<f64, _>(row, idx)
            .map(Value::F64)
            .map_err(|e| Error::row_decode(e, "Failed to decode FLOAT8")),

        PgTypeKind::Text => sqlx::Row::try_get_unchecked::<String, _>(row, idx)
            .map(Value::String)
            .map_err(|e| Error::row_decode(e, "Failed to decode string")),

        PgTypeKind::Geometry => sqlx::Row::try_get_unchecked::<String, _>(row, idx)
            .map(Value::Geometry)
            .map_err(|e| Error::row_decode(e, "Failed to decode GEOMETRY")),

        PgTypeKind::Geography => sqlx::Row::try_get_unchecked::<String, _>(row, idx)
            .map(Value::Geography)
            .map_err(|e| Error::row_decode(e, "Failed to decode GEOGRAPHY")),

        PgTypeKind::Hstore => sqlx::Row::try_get_unchecked::<PgHstore, _>(row, idx)
            .map(|map| Value::Hstore(map.0))
            .map_err(|e| Error::row_decode(e, "Failed to decode HSTORE")),

        PgTypeKind::Vector => sqlx::Row::try_get_unchecked::<String, _>(row, idx)
            .map_err(|e| Error::row_decode(e, "Failed to decode VECTOR"))
            .and_then(|raw| parse_pg_vector(&raw)),

        PgTypeKind::Bytes => sqlx::Row::try_get_unchecked::<Vec<u8>, _>(row, idx)
            .map(Value::Bytes)
            .map_err(|e| Error::row_decode(e, "Failed to decode bytes")),

        PgTypeKind::Uuid => sqlx::Row::try_get_unchecked::<Uuid, _>(row, idx)
            .map(Value::Uuid)
            .map_err(|e| Error::row_decode(e, "Failed to decode UUID")),

        PgTypeKind::Timestamp => sqlx::Row::try_get_unchecked::<chrono::NaiveDateTime, _>(row, idx)
            .map(Value::DateTime)
            .map_err(|e| Error::row_decode(e, "Failed to decode TIMESTAMP")),

        PgTypeKind::TimestampTz => {
            sqlx::Row::try_get_unchecked::<chrono::DateTime<chrono::Utc>, _>(row, idx)
                .map(|dt| Value::DateTime(dt.naive_utc()))
                .map_err(|e| Error::row_decode(e, "Failed to decode TIMESTAMPTZ"))
        }

        PgTypeKind::Date => sqlx::Row::try_get_unchecked::<chrono::NaiveDate, _>(row, idx)
            .map(|d| {
                Value::DateTime(
                    d.and_hms_opt(0, 0, 0)
                        .expect("midnight (0, 0, 0) is always a valid time"),
                )
            })
            .map_err(|e| Error::row_decode(e, "Failed to decode DATE")),

        PgTypeKind::Time => sqlx::Row::try_get_unchecked::<chrono::NaiveTime, _>(row, idx)
            .map(|t| Value::String(t.to_string()))
            .map_err(|e| Error::row_decode(e, "Failed to decode TIME")),

        PgTypeKind::Numeric => sqlx::Row::try_get_unchecked::<rust_decimal::Decimal, _>(row, idx)
            .map(Value::Decimal)
            .map_err(|e| Error::row_decode(e, "Failed to decode NUMERIC")),

        // Handle PostgreSQL 2D array types (TEXT[][], INT4[][], etc.)
        // sqlx doesn't support 2D array decoding natively, so we decode
        // the text representation and parse the PostgreSQL array literal.
        PgTypeKind::Array2D(element_type) => sqlx::Row::try_get_unchecked::<String, _>(row, idx)
            .map_err(|e| Error::row_decode(e, "Failed to decode 2D array"))
            .and_then(|s| parse_pg_2d_array(&s, element_type)),

        PgTypeKind::Array(element_type) => {
            if matches_pg_type(
                element_type,
                &[
                    "TEXT", "VARCHAR", "CHAR", "BPCHAR", "NAME", "CITEXT", "LTREE",
                ],
            ) {
                sqlx::Row::try_get_unchecked::<Vec<String>, _>(row, idx)
                    .map(|vec| Value::Array(vec.into_iter().map(Value::String).collect()))
                    .map_err(|e| Error::row_decode(e, "Failed to decode TEXT[]"))
            } else if pg_type_is(element_type, "GEOMETRY") {
                sqlx::Row::try_get_unchecked::<Vec<String>, _>(row, idx)
                    .map(|vec| Value::Array(vec.into_iter().map(Value::Geometry).collect()))
                    .map_err(|e| Error::row_decode(e, "Failed to decode GEOMETRY[]"))
            } else if pg_type_is(element_type, "GEOGRAPHY") {
                sqlx::Row::try_get_unchecked::<Vec<String>, _>(row, idx)
                    .map(|vec| Value::Array(vec.into_iter().map(Value::Geography).collect()))
                    .map_err(|e| Error::row_decode(e, "Failed to decode GEOGRAPHY[]"))
            } else if pg_type_is(element_type, "HSTORE") {
                sqlx::Row::try_get_unchecked::<Vec<PgHstore>, _>(row, idx)
                    .map(|vec| {
                        Value::Array(vec.into_iter().map(|item| Value::Hstore(item.0)).collect())
                    })
                    .map_err(|e| Error::row_decode(e, "Failed to decode HSTORE[]"))
            } else if pg_type_is(element_type, "INT2") {
                sqlx::Row::try_get_unchecked::<Vec<i16>, _>(row, idx)
                    .map(|vec| {
                        Value::Array(
                            vec.into_iter()
                                .map(|item| Value::I32(item as i32))
                                .collect(),
                        )
                    })
                    .map_err(|e| Error::row_decode(e, "Failed to decode SMALLINT[]"))
            } else if matches_pg_type(element_type, &["INT4", "SERIAL"]) {
                sqlx::Row::try_get_unchecked::<Vec<i32>, _>(row, idx)
                    .map(|vec| Value::Array(vec.into_iter().map(Value::I32).collect()))
                    .map_err(|e| Error::row_decode(e, "Failed to decode INT[]"))
            } else if matches_pg_type(element_type, &["INT8", "BIGINT", "BIGSERIAL"]) {
                sqlx::Row::try_get_unchecked::<Vec<i64>, _>(row, idx)
                    .map(|vec| Value::Array(vec.into_iter().map(Value::I64).collect()))
                    .map_err(|e| Error::row_decode(e, "Failed to decode BIGINT[]"))
            } else if matches_pg_type(element_type, &["FLOAT4", "REAL"]) {
                sqlx::Row::try_get_unchecked::<Vec<f32>, _>(row, idx)
                    .map(|vec| {
                        Value::Array(
                            vec.into_iter()
                                .map(|item| Value::F64(item as f64))
                                .collect(),
                        )
                    })
                    .map_err(|e| Error::row_decode(e, "Failed to decode REAL[]"))
            } else if matches_pg_type(element_type, &["FLOAT8", "DOUBLE PRECISION"]) {
                sqlx::Row::try_get_unchecked::<Vec<f64>, _>(row, idx)
                    .map(|vec| Value::Array(vec.into_iter().map(Value::F64).collect()))
                    .map_err(|e| Error::row_decode(e, "Failed to decode FLOAT[]"))
            } else if pg_type_is(element_type, "BOOL") {
                sqlx::Row::try_get_unchecked::<Vec<bool>, _>(row, idx)
                    .map(|vec| Value::Array(vec.into_iter().map(Value::Bool).collect()))
                    .map_err(|e| Error::row_decode(e, "Failed to decode BOOL[]"))
            } else {
                Err(Error::row_decode_msg(format!(
                    "Unsupported array element type: {}",
                    element_type
                )))
            }
        }

        PgTypeKind::Json => sqlx::Row::try_get_unchecked::<serde_json::Value, _>(row, idx)
            .map(Value::Json)
            .map_err(|e| Error::row_decode(e, "Failed to decode JSON")),

        PgTypeKind::Unknown => {
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

fn classify_pg_type(type_name: &str) -> PgTypeKind<'_> {
    match classify_pg_array_type(type_name) {
        Some(kind) => kind,
        None => classify_pg_scalar_type(type_name).unwrap_or(PgTypeKind::Unknown),
    }
}

fn classify_pg_array_type(type_name: &str) -> Option<PgTypeKind<'_>> {
    if let Some(element_type) = type_name.strip_suffix("[][]") {
        Some(PgTypeKind::Array2D(element_type))
    } else if let Some(element_type) = type_name.strip_suffix("[]") {
        Some(PgTypeKind::Array(element_type))
    } else {
        None
    }
}

fn classify_pg_scalar_type(type_name: &str) -> Option<PgTypeKind<'static>> {
    PG_SCALAR_TYPE_ALIASES
        .iter()
        .find_map(|(aliases, kind)| matches_pg_type(type_name, aliases).then_some(*kind))
}

fn pg_type_is(type_name: &str, expected: &str) -> bool {
    type_name.eq_ignore_ascii_case(expected)
}

fn matches_pg_type(type_name: &str, candidates: &[&str]) -> bool {
    candidates
        .iter()
        .any(|candidate| pg_type_is(type_name, candidate))
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

    let parts = inner.split(',');
    let mut values = Vec::with_capacity(parts.size_hint().0);
    for (idx, raw) in parts.enumerate() {
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
    fn classify_pg_type_is_case_insensitive_without_normalizing_strings() {
        assert_eq!(classify_pg_type("jsonb"), PgTypeKind::Json);
        assert_eq!(classify_pg_type("TeXt"), PgTypeKind::Text);
        assert_eq!(classify_pg_type("int4[]"), PgTypeKind::Array("int4"));
        assert_eq!(
            classify_pg_type("VaRcHaR[][]"),
            PgTypeKind::Array2D("VaRcHaR")
        );
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
