//! Database value types.

use std::collections::BTreeMap;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Database value type.
///
/// Implements custom JSON serialization for cross-language compatibility:
/// - `Decimal` -> string (avoid precision loss)
/// - `DateTime` -> RFC3339 string
/// - `Uuid` -> hyphenated lowercase string
/// - `Bytes` -> base64 string
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    /// NULL value.
    Null,
    /// Boolean.
    Bool(bool),
    /// 32-bit integer.
    I32(i32),
    /// 64-bit integer.
    I64(i64),
    /// 64-bit float.
    F64(f64),
    /// Decimal number with arbitrary precision.
    Decimal(rust_decimal::Decimal),
    /// Date and time (without timezone).
    DateTime(chrono::NaiveDateTime),
    /// UUID.
    Uuid(uuid::Uuid),
    /// JSON value.
    Json(serde_json::Value),
    /// PostgreSQL hstore key/value map.
    Hstore(BTreeMap<String, Option<String>>),
    /// PostgreSQL pgvector dense embedding vector.
    Vector(Vec<f32>),
    /// String.
    String(String),
    /// Byte array.
    Bytes(Vec<u8>),
    /// Array of values (PostgreSQL native arrays).
    Array(Vec<Value>),
    /// 2D array of values (PostgreSQL multi-dimensional arrays).
    Array2D(Vec<Vec<Value>>),
    /// A database enum value with its PostgreSQL type name.
    ///
    /// Carries the variant string (e.g. `"ADMIN"`) together with the
    /// lowercase PG type name (e.g. `"role"`) so that the PostgreSQL
    /// dialect can emit the required explicit cast (`$1::role`).
    /// All other backends treat this identically to `Value::String`.
    Enum {
        /// The enum variant string sent to / received from the DB.
        value: String,
        /// Lowercase PostgreSQL type name (e.g. `"role"`, `"poststatus"`).
        type_name: String,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
enum SerdeValue {
    Null,
    Bool(bool),
    I32(i32),
    I64(i64),
    F64(f64),
    Decimal(String),
    DateTime(String),
    Uuid(String),
    Json(serde_json::Value),
    Hstore(BTreeMap<String, Option<String>>),
    Vector(Vec<f32>),
    String(String),
    Bytes(String),
    Array(Vec<Value>),
    Array2D(Vec<Vec<Value>>),
    Enum { value: String, type_name: String },
}

fn format_datetime(value: chrono::NaiveDateTime) -> String {
    value.format("%Y-%m-%dT%H:%M:%S%.fZ").to_string()
}

fn parse_datetime_string(raw: &str) -> std::result::Result<chrono::NaiveDateTime, String> {
    chrono::DateTime::parse_from_rfc3339(raw)
        .map(|value| value.naive_utc())
        .or_else(|_| chrono::NaiveDateTime::parse_from_str(raw, "%Y-%m-%dT%H:%M:%S%.f"))
        .or_else(|_| chrono::NaiveDateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S%.f"))
        .map_err(|_| format!("invalid datetime '{}'", raw))
}

impl From<&Value> for SerdeValue {
    fn from(value: &Value) -> Self {
        match value {
            Value::Null => SerdeValue::Null,
            Value::Bool(v) => SerdeValue::Bool(*v),
            Value::I32(v) => SerdeValue::I32(*v),
            Value::I64(v) => SerdeValue::I64(*v),
            Value::F64(v) => SerdeValue::F64(*v),
            Value::Decimal(v) => SerdeValue::Decimal(v.to_string()),
            Value::DateTime(v) => SerdeValue::DateTime(format_datetime(*v)),
            Value::Uuid(v) => SerdeValue::Uuid(v.to_string()),
            Value::Json(v) => SerdeValue::Json(v.clone()),
            Value::Hstore(v) => SerdeValue::Hstore(v.clone()),
            Value::Vector(v) => SerdeValue::Vector(v.clone()),
            Value::String(v) => SerdeValue::String(v.clone()),
            Value::Bytes(v) => {
                use base64::Engine;
                SerdeValue::Bytes(base64::engine::general_purpose::STANDARD.encode(v))
            }
            Value::Array(v) => SerdeValue::Array(v.clone()),
            Value::Array2D(v) => SerdeValue::Array2D(v.clone()),
            Value::Enum { value, type_name } => SerdeValue::Enum {
                value: value.clone(),
                type_name: type_name.clone(),
            },
        }
    }
}

impl TryFrom<SerdeValue> for Value {
    type Error = String;

    fn try_from(value: SerdeValue) -> std::result::Result<Self, Self::Error> {
        match value {
            SerdeValue::Null => Ok(Value::Null),
            SerdeValue::Bool(v) => Ok(Value::Bool(v)),
            SerdeValue::I32(v) => Ok(Value::I32(v)),
            SerdeValue::I64(v) => Ok(Value::I64(v)),
            SerdeValue::F64(v) => Ok(Value::F64(v)),
            SerdeValue::Decimal(raw) => rust_decimal::Decimal::from_str(&raw)
                .map(Value::Decimal)
                .map_err(|e| format!("invalid decimal '{}': {}", raw, e)),
            SerdeValue::DateTime(raw) => parse_datetime_string(&raw).map(Value::DateTime),
            SerdeValue::Uuid(raw) => uuid::Uuid::parse_str(&raw)
                .map(Value::Uuid)
                .map_err(|e| format!("invalid uuid '{}': {}", raw, e)),
            SerdeValue::Json(v) => Ok(Value::Json(v)),
            SerdeValue::Hstore(v) => Ok(Value::Hstore(v)),
            SerdeValue::Vector(v) => Ok(Value::Vector(v)),
            SerdeValue::String(v) => Ok(Value::String(v)),
            SerdeValue::Bytes(raw) => {
                use base64::Engine;
                base64::engine::general_purpose::STANDARD
                    .decode(raw.as_bytes())
                    .map(Value::Bytes)
                    .map_err(|e| format!("invalid base64 bytes '{}': {}", raw, e))
            }
            SerdeValue::Array(v) => Ok(Value::Array(v)),
            SerdeValue::Array2D(v) => Ok(Value::Array2D(v)),
            SerdeValue::Enum { value, type_name } => Ok(Value::Enum { value, type_name }),
        }
    }
}

impl From<bool> for Value {
    fn from(v: bool) -> Self {
        Value::Bool(v)
    }
}

impl From<i32> for Value {
    fn from(v: i32) -> Self {
        Value::I32(v)
    }
}

impl From<i64> for Value {
    fn from(v: i64) -> Self {
        Value::I64(v)
    }
}

impl From<f64> for Value {
    fn from(v: f64) -> Self {
        Value::F64(v)
    }
}

impl From<f32> for Value {
    fn from(v: f32) -> Self {
        Value::F64(v as f64)
    }
}

impl From<rust_decimal::Decimal> for Value {
    fn from(v: rust_decimal::Decimal) -> Self {
        Value::Decimal(v)
    }
}

impl From<chrono::NaiveDateTime> for Value {
    fn from(v: chrono::NaiveDateTime) -> Self {
        Value::DateTime(v)
    }
}

impl From<uuid::Uuid> for Value {
    fn from(v: uuid::Uuid) -> Self {
        Value::Uuid(v)
    }
}

impl From<serde_json::Value> for Value {
    fn from(v: serde_json::Value) -> Self {
        Value::Json(v)
    }
}

impl From<BTreeMap<String, Option<String>>> for Value {
    fn from(v: BTreeMap<String, Option<String>>) -> Self {
        Value::Hstore(v)
    }
}

impl From<Vec<f32>> for Value {
    fn from(v: Vec<f32>) -> Self {
        Value::Vector(v)
    }
}

impl From<String> for Value {
    fn from(v: String) -> Self {
        Value::String(v)
    }
}

impl From<&str> for Value {
    fn from(v: &str) -> Self {
        Value::String(v.to_string())
    }
}

impl From<Vec<u8>> for Value {
    fn from(v: Vec<u8>) -> Self {
        Value::Bytes(v)
    }
}

// Array conversions — generated for all scalar types that map cleanly to Value
// via `Into<Value>`. `Vec<u8>` is intentionally excluded: it maps to
// `Value::Bytes`, not `Value::Array`.
macro_rules! impl_vec_from {
    ($($t:ty),* $(,)?) => {
        $(
            impl From<Vec<$t>> for Value {
                fn from(v: Vec<$t>) -> Self {
                    Value::Array(v.into_iter().map(|x| x.into()).collect())
                }
            }

            impl From<Vec<Vec<$t>>> for Value {
                fn from(v: Vec<Vec<$t>>) -> Self {
                    Value::Array2D(
                        v.into_iter()
                            .map(|row| row.into_iter().map(|x| x.into()).collect())
                            .collect(),
                    )
                }
            }
        )*
    };
}

impl_vec_from!(
    i32,
    i64,
    f64,
    bool,
    String,
    BTreeMap<String, Option<String>>,
    rust_decimal::Decimal,
    uuid::Uuid,
    chrono::NaiveDateTime,
    serde_json::Value,
);

// Option<T> conversions — map None -> Value::Null.
// `Option<&str>` is kept manual because `&str` requires an explicit `.to_string()`
// and does not implement `Into<Value>` through the generic `v.into()` path.
macro_rules! impl_option_from {
    ($($t:ty),* $(,)?) => {
        $(
            impl From<Option<$t>> for Value {
                fn from(v: Option<$t>) -> Self {
                    v.map(|x| x.into()).unwrap_or(Value::Null)
                }
            }
        )*
    };
}

impl_option_from!(
    bool,
    i32,
    i64,
    f64,
    String,
    Vec<f32>,
    BTreeMap<String, Option<String>>,
    rust_decimal::Decimal,
    uuid::Uuid,
    chrono::NaiveDateTime,
);

impl From<Option<&str>> for Value {
    fn from(v: Option<&str>) -> Self {
        v.map(|s| Value::String(s.to_string()))
            .unwrap_or(Value::Null)
    }
}

impl Value {
    /// Convert this value into the plain JSON shape used on transport/wire paths.
    ///
    /// Unlike the serde representation of [`Value`] itself, this helper
    /// intentionally mirrors the historic untagged encoding used by the engine
    /// and generated raw-query helpers.
    pub fn to_json_plain(&self) -> serde_json::Value {
        match self {
            Value::Null => serde_json::Value::Null,
            Value::Bool(v) => serde_json::Value::Bool(*v),
            Value::I32(v) => serde_json::Value::Number((*v).into()),
            Value::I64(v) => serde_json::Value::Number((*v).into()),
            Value::F64(v) => serde_json::Number::from_f64(*v)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null),
            Value::Decimal(v) => serde_json::Value::String(v.to_string()),
            Value::DateTime(v) => serde_json::Value::String(format_datetime(*v)),
            Value::Uuid(v) => serde_json::Value::String(v.to_string()),
            Value::Json(v) => v.clone(),
            Value::Hstore(v) => serde_json::Value::Object(
                v.iter()
                    .map(|(key, value)| {
                        (
                            key.clone(),
                            value
                                .as_ref()
                                .map(|item| serde_json::Value::String(item.clone()))
                                .unwrap_or(serde_json::Value::Null),
                        )
                    })
                    .collect(),
            ),
            Value::Vector(v) => serde_json::Value::Array(
                v.iter()
                    .map(|item| {
                        serde_json::Number::from_f64(*item as f64)
                            .map(serde_json::Value::Number)
                            .unwrap_or(serde_json::Value::Null)
                    })
                    .collect(),
            ),
            Value::String(v) => serde_json::Value::String(v.clone()),
            Value::Bytes(v) => {
                use base64::Engine;
                serde_json::Value::String(base64::engine::general_purpose::STANDARD.encode(v))
            }
            Value::Array(v) => {
                serde_json::Value::Array(v.iter().map(Value::to_json_plain).collect())
            }
            Value::Array2D(v) => serde_json::Value::Array(
                v.iter()
                    .map(|row| {
                        serde_json::Value::Array(row.iter().map(Value::to_json_plain).collect())
                    })
                    .collect(),
            ),
            Value::Enum { value, .. } => serde_json::Value::String(value.clone()),
        }
    }
}

impl Serialize for Value {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        SerdeValue::from(self).serialize(serializer)
    }
}

/// Deserializes a [`Value`] from the tagged serde representation emitted by
/// [`Serialize`] for [`Value`].
impl<'de> Deserialize<'de> for Value {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let tagged = SerdeValue::deserialize(deserializer)?;
        Value::try_from(tagged).map_err(serde::de::Error::custom)
    }
}

/// Convert a `&serde_json::Value` reference to a [`Value`].
///
/// This is the canonical JSON->Value conversion used throughout the crate.
/// It is `pub(crate)` so that other modules (e.g. `column.rs`) can reuse it
/// without duplicating the logic.
///
/// Numbers are coerced to `I32` before `I64` when they fit, then `F64`.
/// Arrays of arrays are **not** auto-promoted to `Array2D` here; that
/// promotion happens in the connector stream decoders where full schema
/// knowledge is available.
pub(crate) fn json_to_value_ref(json: &serde_json::Value) -> Value {
    match json {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                if i >= i32::MIN as i64 && i <= i32::MAX as i64 {
                    Value::I32(i as i32)
                } else {
                    Value::I64(i)
                }
            } else if let Some(f) = n.as_f64() {
                Value::F64(f)
            } else {
                Value::String(n.to_string())
            }
        }
        serde_json::Value::String(s) => Value::String(s.clone()),
        serde_json::Value::Array(arr) => Value::Array(arr.iter().map(json_to_value_ref).collect()),
        serde_json::Value::Object(_) => Value::Json(json.clone()),
    }
}

#[cfg(test)]
mod tests {
    use core::f64;
    use std::collections::BTreeMap;

    use super::*;

    #[test]
    fn test_value_variants() {
        assert_eq!(Value::Null, Value::Null);
        assert_eq!(Value::Bool(true), Value::from(true));
        assert_eq!(Value::I32(42), Value::from(42i32));
        assert_eq!(Value::I64(42), Value::from(42i64));
        assert_eq!(Value::F64(2.5), Value::from(2.5f64));
        assert_eq!(Value::String("hello".to_string()), Value::from("hello"));
        assert_eq!(Value::Bytes(vec![1, 2, 3]), Value::from(vec![1u8, 2, 3]));

        use rust_decimal::Decimal;
        let dec = Decimal::new(12345, 2);
        assert_eq!(Value::Decimal(dec), Value::from(dec));

        use chrono::NaiveDate;
        let dt = NaiveDate::from_ymd_opt(2024, 1, 1)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap();
        assert_eq!(Value::DateTime(dt), Value::from(dt));

        use uuid::Uuid;
        let id = Uuid::nil();
        assert_eq!(Value::Uuid(id), Value::from(id));

        use serde_json::json;
        let j = json!({"key": "value"});
        assert_eq!(Value::Json(j.clone()), Value::from(j));

        let hstore = BTreeMap::from([
            ("display_name".to_string(), Some("Bob".to_string())),
            ("nickname".to_string(), None),
        ]);
        assert_eq!(Value::Hstore(hstore.clone()), Value::from(hstore));

        assert_eq!(
            Value::Vector(vec![0.1, 0.2]),
            Value::from(vec![0.1f32, 0.2])
        );
    }

    #[test]
    fn test_value_to_json_plain_primitives() {
        assert_eq!(Value::Null.to_json_plain(), serde_json::Value::Null);
        assert_eq!(
            Value::Bool(true).to_json_plain(),
            serde_json::Value::Bool(true)
        );
        assert_eq!(Value::I32(42).to_json_plain().as_i64(), Some(42));
        assert_eq!(
            Value::I64(9007199254740991).to_json_plain().as_i64(),
            Some(9007199254740991)
        );
        assert_eq!(
            Value::F64(f64::consts::PI).to_json_plain().as_f64(),
            Some(f64::consts::PI)
        );
        assert_eq!(
            Value::String("hello world".to_string())
                .to_json_plain()
                .as_str(),
            Some("hello world")
        );
    }

    #[test]
    fn test_value_to_json_plain_special_scalars() {
        use rust_decimal::Decimal;
        let dec = Decimal::new(12345, 2);
        use chrono::NaiveDate;
        let dt = NaiveDate::from_ymd_opt(2026, 2, 18)
            .unwrap()
            .and_hms_opt(10, 30, 45)
            .unwrap();
        use uuid::Uuid;
        let id = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        assert_eq!(Value::Decimal(dec).to_json_plain().as_str(), Some("123.45"));
        assert!(Value::DateTime(dt)
            .to_json_plain()
            .as_str()
            .unwrap()
            .starts_with("2026-02-18T10:30:45"));
        assert_eq!(
            Value::Uuid(id).to_json_plain().as_str(),
            Some("550e8400-e29b-41d4-a716-446655440000")
        );
        assert_eq!(
            Value::Bytes(vec![72, 101, 108, 108, 111])
                .to_json_plain()
                .as_str(),
            Some("SGVsbG8=")
        );
    }

    #[test]
    fn test_value_to_json_plain_json_and_arrays() {
        use serde_json::json;
        let object = json!({"name": "Alice", "age": 30});
        assert_eq!(Value::Json(object.clone()).to_json_plain(), object);

        let value = Value::Array(vec![
            Value::String("a".to_string()),
            Value::String("b".to_string()),
            Value::String("c".to_string()),
        ]);

        let json = value.to_json_plain();
        assert_eq!(json[0].as_str(), Some("a"));
        assert_eq!(json[1].as_str(), Some("b"));
        assert_eq!(json[2].as_str(), Some("c"));
    }

    #[test]
    fn test_value_to_json_plain_hstore() {
        let value = Value::Hstore(BTreeMap::from([
            ("display_name".to_string(), Some("Bob".to_string())),
            ("nickname".to_string(), None),
        ]));

        assert_eq!(
            value.to_json_plain(),
            serde_json::json!({
                "display_name": "Bob",
                "nickname": null
            })
        );
    }

    #[test]
    fn test_value_to_json_plain_vector() {
        let json = Value::Vector(vec![1.0, 2.5, 3.25]).to_json_plain();
        assert_eq!(json, serde_json::json!([1.0, 2.5, 3.25]));
    }

    #[test]
    fn test_value_plain_json_array2d_roundtrip_stays_untyped_without_schema() {
        let value = Value::Array2D(vec![
            vec![Value::I32(1), Value::I32(2)],
            vec![Value::I32(3), Value::I32(4)],
        ]);

        let json = value.to_json_plain();
        assert_eq!(json[0][0].as_i64(), Some(1));
        assert_eq!(json[0][1].as_i64(), Some(2));
        assert_eq!(json[1][0].as_i64(), Some(3));
        assert_eq!(json[1][1].as_i64(), Some(4));

        // Deserialization: without schema context the `Array2D` heuristic is
        // intentionally absent from `json_to_value_ref`. A nested JSON array
        // round-trips as `Array(Array(_))`. Promotion to `Array2D` is the
        // connector stream's responsibility.
        let expected = Value::Array(vec![
            Value::Array(vec![Value::I32(1), Value::I32(2)]),
            Value::Array(vec![Value::I32(3), Value::I32(4)]),
        ]);
        assert_eq!(json_to_value_ref(&json), expected);
    }

    #[test]
    fn test_tagged_serde_shape_is_explicit() {
        let value = Value::Decimal(rust_decimal::Decimal::new(12345, 2));
        let json = serde_json::to_value(&value).unwrap();

        assert_eq!(
            json,
            serde_json::json!({
                "type": "decimal",
                "value": "123.45"
            })
        );
    }

    #[test]
    fn test_tagged_serde_round_trip_preserves_typed_variants() {
        use chrono::NaiveDate;
        use serde_json::json;
        use uuid::Uuid;

        let values = vec![
            Value::Null,
            Value::Bool(false),
            Value::I32(-42),
            Value::I64(9007199254740991), // Large I64 beyond i32 range
            Value::F64(f64::consts::E),
            Value::Decimal(rust_decimal::Decimal::new(314, 2)),
            Value::DateTime(
                NaiveDate::from_ymd_opt(2026, 2, 18)
                    .unwrap()
                    .and_hms_opt(10, 30, 45)
                    .unwrap(),
            ),
            Value::Uuid(Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap()),
            Value::Bytes(vec![1, 2, 3, 4]),
            Value::Json(json!({"ok": true})),
            Value::Hstore(BTreeMap::from([
                ("display_name".to_string(), Some("Bob".to_string())),
                ("nickname".to_string(), None),
            ])),
            Value::Vector(vec![1.0, 2.0, 3.5]),
            Value::String("test".to_string()),
            Value::Array(vec![Value::I32(1), Value::I32(2)]),
            Value::Array2D(vec![vec![Value::I32(1), Value::I32(2)]]),
            Value::Enum {
                value: "ADMIN".to_string(),
                type_name: "role".to_string(),
            },
        ];

        for value in values {
            let json = serde_json::to_value(&value).unwrap();
            let deserialized: Value = serde_json::from_value(json).unwrap();
            assert_eq!(deserialized, value);
        }
    }
}
