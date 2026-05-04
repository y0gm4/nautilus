//! SQLite row stream and value decoder.

use crate::error::{ConnectorError as Error, Result};
use crate::row_stream::RowStream;
use crate::Row;
use nautilus_core::Value;
use sqlx::sqlite::SqliteRow;
use sqlx::{Column, Row as SqlxRow, TypeInfo, ValueRef};

/// Stream type for SQLite query results.
///
/// A thin alias for the shared [`RowStream`] type.
pub type SqliteRowStream<'conn> = RowStream<'conn>;

/// Decode a sqlx `SqliteRow` into a Nautilus `Row`.
///
/// This function is public within the crate for use by the SQLite executor.
pub(crate) fn decode_row_internal(row: SqliteRow) -> Result<Row> {
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

/// Decode a value from a sqlx SQLite row by index and type.
fn decode_value(
    row: &SqliteRow,
    idx: usize,
    type_info: &sqlx::sqlite::SqliteTypeInfo,
) -> Result<Value> {
    let type_name = type_info.name();

    if let Ok(raw) = sqlx::Row::try_get_raw(row, idx) {
        if raw.is_null() {
            return Ok(Value::Null);
        }
    }

    match type_name {
        "BOOLEAN" | "BOOL" => row
            .try_get::<bool, _>(idx)
            .map(Value::Bool)
            .map_err(|e| Error::row_decode(e, "Failed to decode BOOL")),

        "INTEGER" | "INT" | "BIGINT" | "INT2" | "INT4" | "INT8" | "TINYINT" | "SMALLINT"
        | "MEDIUMINT" => {
            if let Ok(val) = row.try_get::<i64, _>(idx) {
                Ok(Value::I64(val))
            } else if let Ok(val) = row.try_get::<i32, _>(idx) {
                Ok(Value::I64(val as i64))
            } else {
                Err(Error::row_decode_msg(format!(
                    "Failed to decode integer type: {}",
                    type_name
                )))
            }
        }

        "REAL" | "FLOAT" | "DOUBLE" | "FLOAT4" | "FLOAT8" | "DOUBLE PRECISION" => {
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

        "DATE" => row
            .try_get::<chrono::NaiveDate, _>(idx)
            .map(|d| {
                Value::DateTime(
                    d.and_hms_opt(0, 0, 0)
                        .expect("midnight (0, 0, 0) is always a valid time"),
                )
            })
            .map_err(|e| Error::row_decode(e, "Failed to decode DATE")),

        "DATETIME" => row
            .try_get::<chrono::NaiveDateTime, _>(idx)
            .map(Value::DateTime)
            .map_err(|e| Error::row_decode(e, "Failed to decode DATETIME")),

        "TIME" => row
            .try_get::<chrono::NaiveTime, _>(idx)
            .map(|t| Value::String(t.to_string()))
            .map_err(|e| Error::row_decode(e, "Failed to decode TIME")),

        "TEXT" | "VARCHAR" | "CHAR" | "CLOB" | "NAME" => row
            .try_get::<String, _>(idx)
            .map(Value::String)
            .map_err(|e| Error::row_decode(e, "Failed to decode string")),

        "BLOB" | "BYTEA" => row
            .try_get::<Vec<u8>, _>(idx)
            .map(Value::Bytes)
            .map_err(|e| Error::row_decode(e, "Failed to decode bytes")),

        _ => decode_dynamic_value(row, idx, type_name),
    }
}

fn decode_dynamic_value(row: &SqliteRow, idx: usize, type_name: &str) -> Result<Value> {
    // SQLite + sqlx may report "NULL" type info for non-null values stored in
    // columns declared with custom affinities such as DECIMAL(...) or JSON.
    // Fall back to probing the runtime value instead of trusting the declared
    // type name here.
    if let Ok(val) = row.try_get::<i64, _>(idx) {
        Ok(Value::I64(val))
    } else if let Ok(val) = row.try_get::<f64, _>(idx) {
        Ok(Value::F64(val))
    } else if let Ok(val) = row.try_get::<String, _>(idx) {
        Ok(Value::String(val))
    } else if let Ok(val) = row.try_get::<Vec<u8>, _>(idx) {
        Ok(Value::Bytes(val))
    } else {
        Err(Error::row_decode_msg(format!(
            "Unsupported type '{}' at column {}",
            type_name, idx
        )))
    }
}
