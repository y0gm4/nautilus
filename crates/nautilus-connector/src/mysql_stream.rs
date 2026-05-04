//! MySQL row stream and value decoder.

use crate::error::{ConnectorError as Error, Result};
use crate::row_stream::RowStream;
use crate::Row;
use nautilus_core::Value;
use sqlx::mysql::MySqlRow;
use sqlx::{Column, Row as SqlxRow, TypeInfo, ValueRef};

/// Stream type for MySQL query results.
///
/// A thin alias for the shared [`RowStream`] type.
pub type MysqlRowStream<'conn> = RowStream<'conn>;

/// Decode a sqlx `MySqlRow` into a Nautilus `Row`.
///
/// This function is public within the crate for use by the MySQL executor.
pub(crate) fn decode_row_internal(row: MySqlRow) -> Result<Row> {
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

/// Decode a value from a sqlx MySQL row by index and type.
fn decode_value(
    row: &MySqlRow,
    idx: usize,
    type_info: &sqlx::mysql::MySqlTypeInfo,
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

        "BIGINT" | "INT" | "INTEGER" | "SMALLINT" | "TINYINT" | "MEDIUMINT" | "INT UNSIGNED"
        | "BIGINT UNSIGNED" | "SMALLINT UNSIGNED" | "TINYINT UNSIGNED" | "MEDIUMINT UNSIGNED" => {
            if let Ok(val) = row.try_get::<i64, _>(idx) {
                Ok(Value::I64(val))
            } else if let Ok(val) = row.try_get::<i32, _>(idx) {
                Ok(Value::I64(val as i64))
            } else if let Ok(val) = row.try_get::<i16, _>(idx) {
                Ok(Value::I64(val as i64))
            } else if let Ok(val) = row.try_get::<i8, _>(idx) {
                Ok(Value::I64(val as i64))
            } else if let Ok(val) = row.try_get::<u64, _>(idx) {
                Ok(Value::I64(val as i64))
            } else {
                Err(Error::row_decode_msg(format!(
                    "Failed to decode integer type: {}",
                    type_name
                )))
            }
        }

        "FLOAT" | "DOUBLE" | "REAL" | "FLOAT UNSIGNED" | "DOUBLE UNSIGNED" => {
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

        "DECIMAL" | "NUMERIC" => row
            .try_get::<rust_decimal::Decimal, _>(idx)
            .map(Value::Decimal)
            .map_err(|e| Error::row_decode(e, "Failed to decode DECIMAL")),

        "DATE" => row
            .try_get::<chrono::NaiveDate, _>(idx)
            .map(|d| {
                Value::DateTime(
                    d.and_hms_opt(0, 0, 0)
                        .expect("midnight (0, 0, 0) is always a valid time"),
                )
            })
            .map_err(|e| Error::row_decode(e, "Failed to decode DATE")),

        "DATETIME" | "TIMESTAMP" => row
            .try_get::<chrono::NaiveDateTime, _>(idx)
            .map(Value::DateTime)
            .map_err(|e| Error::row_decode(e, "Failed to decode DATETIME")),

        "TIME" => row
            .try_get::<chrono::NaiveTime, _>(idx)
            .map(|t| Value::String(t.to_string()))
            .map_err(|e| Error::row_decode(e, "Failed to decode TIME")),

        "VARCHAR" | "TEXT" | "CHAR" | "LONGTEXT" | "MEDIUMTEXT" | "TINYTEXT" | "ENUM" | "SET" => {
            row.try_get::<String, _>(idx)
                .map(Value::String)
                .map_err(|e| Error::row_decode(e, "Failed to decode string"))
        }

        "JSON" => row
            .try_get::<String, _>(idx)
            .map_err(|e| Error::row_decode(e, "Failed to decode JSON"))
            .and_then(|s| {
                serde_json::from_str(&s)
                    .map(Value::Json)
                    .map_err(|e| Error::row_decode_msg(format!("Failed to parse JSON: {}", e)))
            }),

        "BLOB" | "LONGBLOB" | "MEDIUMBLOB" | "TINYBLOB" | "VARBINARY" | "BINARY" => row
            .try_get::<Vec<u8>, _>(idx)
            .map(Value::Bytes)
            .map_err(|e| Error::row_decode(e, "Failed to decode bytes")),

        _ => {
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
    }
}
