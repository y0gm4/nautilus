//! Shared value conversion and helper utilities.
//!
//! This module houses functions for converting between JSON values and the
//! internal [`Value`] type, as well as small helpers used across the engine
//! (e.g. case conversion, row serialisation).

use std::collections::BTreeMap;
use std::str::FromStr;

use uuid::Uuid;

use nautilus_connector::Row;
use nautilus_core::Value;
use nautilus_protocol::ProtocolError;
use nautilus_schema::ir::{ResolvedFieldType, ScalarType};

/// Schema-aware coercion hint for a single projected column.
///
/// These hints are applied above the raw connector decoders when the engine
/// knows the expected schema type for each selected column but the backend row
/// metadata is too weak to recover it reliably (notably SQLite and some MySQL
/// text-affinity types).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueHint {
    /// Parse textual / numeric values into [`Value::Decimal`].
    Decimal,
    /// Parse textual values into [`Value::DateTime`].
    DateTime,
    /// Parse JSON text (or wrap scalar backend values) into [`Value::Json`].
    Json,
    /// Parse textual values into [`Value::Uuid`].
    Uuid,
    /// Wrap textual values as [`Value::Geometry`].
    Geometry,
    /// Wrap textual values as [`Value::Geography`].
    Geography,
}

/// Convert a JSON value to a [`Value`] for use in queries.
///
/// Handles UUID auto-detection, int32/int64 discrimination, arrays, and objects.
pub fn json_to_value(json: &serde_json::Value) -> Result<Value, ProtocolError> {
    match json {
        serde_json::Value::Null => Ok(Value::Null),
        serde_json::Value::Bool(b) => Ok(Value::Bool(*b)),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                if i >= i32::MIN as i64 && i <= i32::MAX as i64 {
                    Ok(Value::I32(i as i32))
                } else {
                    Ok(Value::I64(i))
                }
            } else if let Some(f) = n.as_f64() {
                Ok(Value::F64(f))
            } else {
                Err(ProtocolError::InvalidParams("Invalid number".to_string()))
            }
        }
        serde_json::Value::String(s) => {
            // Auto-detect UUID-format strings so they bind correctly as uuid::Uuid
            // against UUID columns (required for PostgreSQL prepared statements).
            if let Ok(u) = Uuid::parse_str(s) {
                Ok(Value::Uuid(u))
            } else {
                Ok(Value::String(s.clone()))
            }
        }
        serde_json::Value::Array(arr) => {
            let values: Result<Vec<Value>, _> = arr.iter().map(json_to_value).collect();
            Ok(Value::Array(values?))
        }
        serde_json::Value::Object(_) => Ok(Value::Json(json.clone())),
    }
}

/// Convert a JSON value to a [`Value`], using schema field-type context to
/// produce [`Value::Enum`] for enum-typed fields and [`Value::DateTime`] for
/// datetime-typed fields.
///
/// For PostgreSQL, enum columns require an explicit `::type_name` cast in
/// parameterised queries.  Wrapping the value in `Value::Enum` lets the
/// dialect layer inject that cast automatically; all other backends treat it
/// identically to `Value::String`.
pub fn json_to_value_field(
    json: &serde_json::Value,
    field_type: &ResolvedFieldType,
) -> Result<Value, ProtocolError> {
    if let ResolvedFieldType::Enum { enum_name } = field_type {
        match json {
            serde_json::Value::Null => return Ok(Value::Null),
            serde_json::Value::String(s) => {
                return Ok(Value::Enum {
                    value: s.clone(),
                    type_name: enum_name.to_lowercase(),
                });
            }
            _ => {} // fall through to the generic converter below
        }
    }
    // For DateTime fields, parse ISO-8601 / RFC-3339 strings into
    // Value::DateTime so the connector can bind them with the correct
    // PostgreSQL OID instead of sending an untyped text value.
    if let ResolvedFieldType::Scalar(ScalarType::DateTime) = field_type {
        if let serde_json::Value::String(s) = json {
            if let Some(dt) = parse_datetime_string(s) {
                return Ok(Value::DateTime(dt));
            }
        }
    }
    if let ResolvedFieldType::Scalar(ScalarType::Hstore) = field_type {
        return json_to_hstore_value(json);
    }
    if let ResolvedFieldType::Scalar(ScalarType::Vector { dimension }) = field_type {
        return json_to_vector_value(json, *dimension);
    }
    if let ResolvedFieldType::Scalar(ScalarType::Geometry) = field_type {
        return json_to_geometry_value(json);
    }
    if let ResolvedFieldType::Scalar(ScalarType::Geography) = field_type {
        return json_to_geography_value(json);
    }
    json_to_value(json)
}

/// Normalize a vector of rows using per-column schema hints.
///
/// This is intentionally applied only by modeled query paths. Raw SQL methods
/// keep returning the connector-decoded values without additional guessing.
pub fn normalize_rows_with_hints(
    rows: Vec<Row>,
    hints: &[Option<ValueHint>],
) -> Result<Vec<Row>, ProtocolError> {
    rows.into_iter()
        .map(|row| normalize_row_with_hints(row, hints))
        .collect()
}

/// Normalize a single row using per-column schema hints.
pub fn normalize_row_with_hints(
    row: Row,
    hints: &[Option<ValueHint>],
) -> Result<Row, ProtocolError> {
    if row.len() != hints.len() {
        return Err(ProtocolError::Internal(format!(
            "Schema-aware normalization expected {} projected columns, got {}",
            hints.len(),
            row.len()
        )));
    }

    if hints.iter().all(Option::is_none) {
        return Ok(row);
    }

    let mut normalized_row = Row::with_capacity(row.len());
    for (idx, ((name, value), hint)) in row
        .into_columns_iter()
        .zip(hints.iter().copied())
        .enumerate()
    {
        let normalized = match hint {
            Some(hint) => normalize_value_with_hint(&name, idx, value, hint)?,
            None => value,
        };
        normalized_row.push_column(name, normalized);
    }

    Ok(normalized_row)
}

/// Newtype wrapper used by [`rows_to_raw_json`] to serialize a [`Row`] as a
/// JSON object without cloning any column name strings.
struct RowRef<'a>(&'a Row);

impl serde::Serialize for RowRef<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(Some(self.0.len()))?;
        for (name, value) in self.0.iter() {
            map.serialize_entry(name, &value.to_json_plain())?;
        }
        map.end()
    }
}

/// Newtype wrapper used by [`rows_to_raw_json`] to serialize a slice of [`Row`]s
/// as a JSON array using the SIMD-accelerated `sonic-rs` serializer.
struct RowsRef<'a>(&'a [Row]);

impl serde::Serialize for RowsRef<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeSeq;
        let mut seq = serializer.serialize_seq(Some(self.0.len()))?;
        for row in self.0 {
            seq.serialize_element(&RowRef(row))?;
        }
        seq.end()
    }
}

/// Serialize database rows directly to a `Box<RawValue>` JSON array, bypassing
/// all intermediate `Map` / `Vec<JsonValue>` allocations.
///
/// Column names are written as `&str` references — no `.to_string()` cloning.
/// Uses SIMD-accelerated `sonic-rs` for the serialization pass.
pub fn rows_to_raw_json(rows: &[Row]) -> Result<Box<serde_json::value::RawValue>, ProtocolError> {
    let mut buf = Vec::with_capacity(rows.len().saturating_add(1) * 64);
    sonic_rs::to_writer(&mut buf, &RowsRef(rows))
        .map_err(|e| ProtocolError::Internal(format!("Serialize error: {}", e)))?;
    let s = String::from_utf8(buf)
        .map_err(|e| ProtocolError::Internal(format!("UTF-8 error: {}", e)))?;
    serde_json::value::RawValue::from_string(s)
        .map_err(|e| ProtocolError::Internal(format!("RawValue error: {}", e)))
}

/// Verify that the protocol version is within the supported range.
/// Accepts any version from MIN_PROTOCOL_VERSION up to PROTOCOL_VERSION (inclusive).
pub fn check_protocol_version(version: u32) -> Result<(), ProtocolError> {
    if !(nautilus_protocol::MIN_PROTOCOL_VERSION..=nautilus_protocol::PROTOCOL_VERSION)
        .contains(&version)
    {
        Err(ProtocolError::UnsupportedProtocolVersion {
            actual: version,
            expected: nautilus_protocol::PROTOCOL_VERSION,
        })
    } else {
        Ok(())
    }
}

/// Convert a camelCase or PascalCase string to snake_case.
pub fn to_snake_case(s: &str) -> String {
    let mut result = String::with_capacity(s.len() + 4);
    for (i, ch) in s.chars().enumerate() {
        if ch.is_uppercase() && i > 0 {
            result.push('_');
        }
        result.push(ch.to_lowercase().next().unwrap_or(ch));
    }
    result
}

fn parse_datetime_string(raw: &str) -> Option<chrono::NaiveDateTime> {
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(raw) {
        return Some(dt.naive_utc());
    }

    for fmt in [
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S",
    ] {
        if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(raw, fmt) {
            return Some(dt);
        }
    }

    None
}

fn json_to_hstore_value(json: &serde_json::Value) -> Result<Value, ProtocolError> {
    match json {
        serde_json::Value::Null => Ok(Value::Null),
        serde_json::Value::Object(object) => Ok(Value::Hstore(json_object_to_hstore(object)?)),
        serde_json::Value::Array(items) => {
            let mut values = Vec::with_capacity(items.len());
            for item in items {
                values.push(match item {
                    serde_json::Value::Null => Value::Null,
                    serde_json::Value::Object(object) => {
                        Value::Hstore(json_object_to_hstore(object)?)
                    }
                    other => {
                        return Err(ProtocolError::InvalidParams(format!(
                            "Hstore arrays must contain only objects or nulls, got {:?}",
                            other
                        )));
                    }
                });
            }
            Ok(Value::Array(values))
        }
        other => Err(ProtocolError::InvalidParams(format!(
            "Hstore values must be JSON objects with string or null values, got {:?}",
            other
        ))),
    }
}

fn json_object_to_hstore(
    object: &serde_json::Map<String, serde_json::Value>,
) -> Result<BTreeMap<String, Option<String>>, ProtocolError> {
    let mut decoded = BTreeMap::new();
    for (key, value) in object {
        let mapped = match value {
            serde_json::Value::String(item) => Some(item.clone()),
            serde_json::Value::Null => None,
            other => {
                return Err(ProtocolError::InvalidParams(format!(
                    "Hstore values must be strings or nulls; key {:?} received {:?}",
                    key, other
                )));
            }
        };
        decoded.insert(key.clone(), mapped);
    }
    Ok(decoded)
}

fn json_to_vector_value(json: &serde_json::Value, dimension: u32) -> Result<Value, ProtocolError> {
    match json {
        serde_json::Value::Null => Ok(Value::Null),
        serde_json::Value::Array(items) => {
            if items.len() != dimension as usize {
                return Err(ProtocolError::InvalidParams(format!(
                    "Vector value has {} dimensions but schema requires {}",
                    items.len(),
                    dimension
                )));
            }
            let mut values = Vec::with_capacity(items.len());
            for (idx, item) in items.iter().enumerate() {
                let Some(value) = item.as_f64() else {
                    return Err(ProtocolError::InvalidParams(format!(
                        "Vector values must be arrays of finite numbers; element {} was {:?}",
                        idx, item
                    )));
                };
                if !value.is_finite() || value < f32::MIN as f64 || value > f32::MAX as f64 {
                    return Err(ProtocolError::InvalidParams(format!(
                        "Vector element {} is outside the finite f32 range: {}",
                        idx, value
                    )));
                }
                values.push(value as f32);
            }
            Ok(Value::Vector(values))
        }
        other => Err(ProtocolError::InvalidParams(format!(
            "Vector values must be arrays of finite numbers, got {:?}",
            other
        ))),
    }
}

fn json_to_geometry_value(json: &serde_json::Value) -> Result<Value, ProtocolError> {
    json_to_spatial_value(json, "Geometry", Value::Geometry)
}

fn json_to_geography_value(json: &serde_json::Value) -> Result<Value, ProtocolError> {
    json_to_spatial_value(json, "Geography", Value::Geography)
}

fn json_to_spatial_value(
    json: &serde_json::Value,
    type_name: &str,
    wrap: fn(String) -> Value,
) -> Result<Value, ProtocolError> {
    match json {
        serde_json::Value::Null => Ok(Value::Null),
        serde_json::Value::String(raw) => Ok(wrap(raw.clone())),
        serde_json::Value::Array(items) => {
            let mut values = Vec::with_capacity(items.len());
            for (idx, item) in items.iter().enumerate() {
                let Some(raw) = item.as_str() else {
                    return Err(ProtocolError::InvalidParams(format!(
                        "{} arrays must contain only strings; element {} was {:?}",
                        type_name, idx, item
                    )));
                };
                values.push(wrap(raw.to_string()));
            }
            Ok(Value::Array(values))
        }
        other => Err(ProtocolError::InvalidParams(format!(
            "{} values must be strings containing WKT/EWKT or EWKB hex, got {:?}",
            type_name, other
        ))),
    }
}

fn normalize_value_with_hint(
    column: &str,
    index: usize,
    value: Value,
    hint: ValueHint,
) -> Result<Value, ProtocolError> {
    if matches!(value, Value::Null) {
        return Ok(Value::Null);
    }

    match hint {
        ValueHint::Decimal => normalize_decimal_value(column, index, value),
        ValueHint::DateTime => normalize_datetime_value(column, index, value),
        ValueHint::Json => normalize_json_value(column, index, value),
        ValueHint::Uuid => normalize_uuid_value(column, index, value),
        ValueHint::Geometry => normalize_geometry_value(column, index, value),
        ValueHint::Geography => normalize_geography_value(column, index, value),
    }
}

fn normalize_decimal_value(
    column: &str,
    index: usize,
    value: Value,
) -> Result<Value, ProtocolError> {
    match value {
        Value::Decimal(decimal) => Ok(Value::Decimal(decimal)),
        Value::I32(n) => parse_decimal(column, index, &n.to_string()),
        Value::I64(n) => parse_decimal(column, index, &n.to_string()),
        Value::F64(n) if n.is_finite() => parse_decimal(column, index, &n.to_string()),
        Value::String(raw) => parse_decimal(column, index, &raw),
        other => Err(invalid_hint_value(column, index, ValueHint::Decimal, other)),
    }
}

fn normalize_datetime_value(
    column: &str,
    index: usize,
    value: Value,
) -> Result<Value, ProtocolError> {
    match value {
        Value::DateTime(dt) => Ok(Value::DateTime(dt)),
        Value::String(raw) => parse_datetime_string(&raw)
            .map(Value::DateTime)
            .ok_or_else(|| invalid_hint_parse(column, index, ValueHint::DateTime, raw)),
        other => Err(invalid_hint_value(
            column,
            index,
            ValueHint::DateTime,
            other,
        )),
    }
}

fn normalize_json_value(column: &str, index: usize, value: Value) -> Result<Value, ProtocolError> {
    match value {
        Value::Json(json) => Ok(Value::Json(json)),
        Value::String(raw) => serde_json::from_str::<serde_json::Value>(&raw)
            .map(Value::Json)
            .map_err(|_| invalid_hint_parse(column, index, ValueHint::Json, raw)),
        other => Ok(Value::Json(other.to_json_plain())),
    }
}

fn normalize_uuid_value(column: &str, index: usize, value: Value) -> Result<Value, ProtocolError> {
    match value {
        Value::Uuid(uuid) => Ok(Value::Uuid(uuid)),
        Value::String(raw) => Uuid::parse_str(&raw)
            .map(Value::Uuid)
            .map_err(|_| invalid_hint_parse(column, index, ValueHint::Uuid, raw)),
        other => Err(invalid_hint_value(column, index, ValueHint::Uuid, other)),
    }
}

fn normalize_geometry_value(
    column: &str,
    index: usize,
    value: Value,
) -> Result<Value, ProtocolError> {
    match value {
        Value::Geometry(raw) => Ok(Value::Geometry(raw)),
        Value::String(raw) => Ok(Value::Geometry(raw)),
        other => Err(invalid_hint_value(
            column,
            index,
            ValueHint::Geometry,
            other,
        )),
    }
}

fn normalize_geography_value(
    column: &str,
    index: usize,
    value: Value,
) -> Result<Value, ProtocolError> {
    match value {
        Value::Geography(raw) => Ok(Value::Geography(raw)),
        Value::String(raw) => Ok(Value::Geography(raw)),
        other => Err(invalid_hint_value(
            column,
            index,
            ValueHint::Geography,
            other,
        )),
    }
}

fn parse_decimal(column: &str, index: usize, raw: &str) -> Result<Value, ProtocolError> {
    rust_decimal::Decimal::from_str(raw)
        .map(Value::Decimal)
        .map_err(|_| invalid_hint_parse(column, index, ValueHint::Decimal, raw.to_string()))
}

fn invalid_hint_parse(
    column: &str,
    index: usize,
    hint: ValueHint,
    raw: impl Into<String>,
) -> ProtocolError {
    ProtocolError::DatabaseExecution(format!(
        "Failed to normalize column '{}' at position {} as {} from value {:?}",
        column,
        index,
        hint_name(hint),
        raw.into()
    ))
}

fn invalid_hint_value(column: &str, index: usize, hint: ValueHint, value: Value) -> ProtocolError {
    ProtocolError::DatabaseExecution(format!(
        "Failed to normalize column '{}' at position {} as {} from incompatible value {:?}",
        column,
        index,
        hint_name(hint),
        value
    ))
}

fn hint_name(hint: ValueHint) -> &'static str {
    match hint {
        ValueHint::Decimal => "Decimal",
        ValueHint::DateTime => "DateTime",
        ValueHint::Json => "Json",
        ValueHint::Uuid => "Uuid",
        ValueHint::Geometry => "Geometry",
        ValueHint::Geography => "Geography",
    }
}
