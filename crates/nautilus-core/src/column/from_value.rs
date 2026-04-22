//! `FromValue` trait and standard scalar/collection implementations.

use std::collections::BTreeMap;

use crate::error::Result;
use crate::value::Value;

/// Trait for converting database values to Rust types.
///
/// This trait enables type-safe decoding of individual column values
/// during row deserialization in the selection API.
pub trait FromValue: Sized {
    /// Convert a Value reference to this type.
    ///
    /// Returns an error if the value is NULL, has the wrong type,
    /// or cannot be converted.
    fn from_value(value: &Value) -> Result<Self>;

    /// Convert an owned Value to this type, avoiding clones when possible.
    ///
    /// The default implementation delegates to `from_value`. Types that hold
    /// heap data (String, Json, Bytes) override this to take ownership
    /// instead of cloning.
    fn from_value_owned(value: Value) -> Result<Self> {
        Self::from_value(&value)
    }
}

impl FromValue for i64 {
    fn from_value(value: &Value) -> Result<Self> {
        match value {
            Value::I64(v) => Ok(*v),
            Value::Null => Err(crate::Error::TypeError("NULL value for i64".to_string())),
            _ => Err(crate::Error::TypeError(format!(
                "expected i64, got {:?}",
                value
            ))),
        }
    }
}

impl FromValue for i32 {
    fn from_value(value: &Value) -> Result<Self> {
        match value {
            Value::I32(v) => Ok(*v),
            Value::I64(v) => (*v).try_into().map_err(|_| {
                crate::Error::TypeError(format!("i64 value {} doesn't fit in i32", v))
            }),
            Value::Null => Err(crate::Error::TypeError("NULL value for i32".to_string())),
            _ => Err(crate::Error::TypeError(format!(
                "expected i32, got {:?}",
                value
            ))),
        }
    }
}

impl FromValue for String {
    fn from_value(value: &Value) -> Result<Self> {
        match value {
            Value::String(v) => Ok(v.clone()),
            Value::Null => Err(crate::Error::TypeError("NULL value for String".to_string())),
            _ => Err(crate::Error::TypeError(format!(
                "expected String, got {:?}",
                value
            ))),
        }
    }

    fn from_value_owned(value: Value) -> Result<Self> {
        match value {
            Value::String(v) => Ok(v),
            Value::Null => Err(crate::Error::TypeError("NULL value for String".to_string())),
            other => Err(crate::Error::TypeError(format!(
                "expected String, got {:?}",
                other
            ))),
        }
    }
}

impl FromValue for bool {
    fn from_value(value: &Value) -> Result<Self> {
        match value {
            Value::Bool(v) => Ok(*v),
            Value::I32(v) => match *v {
                0 => Ok(false),
                1 => Ok(true),
                other => Err(crate::Error::TypeError(format!(
                    "expected bool-compatible i32 (0 or 1), got {}",
                    other
                ))),
            },
            Value::I64(v) => match *v {
                0 => Ok(false),
                1 => Ok(true),
                other => Err(crate::Error::TypeError(format!(
                    "expected bool-compatible i64 (0 or 1), got {}",
                    other
                ))),
            },
            Value::Null => Err(crate::Error::TypeError("NULL value for bool".to_string())),
            _ => Err(crate::Error::TypeError(format!(
                "expected bool, got {:?}",
                value
            ))),
        }
    }
}

impl FromValue for f64 {
    fn from_value(value: &Value) -> Result<Self> {
        match value {
            Value::F64(v) => Ok(*v),
            Value::Null => Err(crate::Error::TypeError("NULL value for f64".to_string())),
            _ => Err(crate::Error::TypeError(format!(
                "expected f64, got {:?}",
                value
            ))),
        }
    }
}

impl FromValue for rust_decimal::Decimal {
    fn from_value(value: &Value) -> Result<Self> {
        match value {
            Value::Decimal(v) => Ok(*v),
            Value::String(v) => v.parse::<rust_decimal::Decimal>().map_err(|e| {
                crate::Error::TypeError(format!(
                    "failed to parse Decimal from string {:?}: {}",
                    v, e
                ))
            }),
            Value::Null => Err(crate::Error::TypeError(
                "NULL value for Decimal".to_string(),
            )),
            _ => Err(crate::Error::TypeError(format!(
                "expected Decimal, got {:?}",
                value
            ))),
        }
    }
}

impl FromValue for chrono::NaiveDateTime {
    fn from_value(value: &Value) -> Result<Self> {
        match value {
            Value::DateTime(v) => Ok(*v),
            Value::String(v) => chrono::DateTime::parse_from_rfc3339(v)
                .map(|dt| dt.naive_utc())
                .or_else(|_| chrono::NaiveDateTime::parse_from_str(v, "%Y-%m-%dT%H:%M:%S%.f"))
                .or_else(|_| chrono::NaiveDateTime::parse_from_str(v, "%Y-%m-%d %H:%M:%S%.f"))
                .map_err(|_| {
                    crate::Error::TypeError(format!("failed to parse DateTime from string {:?}", v))
                }),
            Value::Null => Err(crate::Error::TypeError(
                "NULL value for DateTime".to_string(),
            )),
            _ => Err(crate::Error::TypeError(format!(
                "expected DateTime, got {:?}",
                value
            ))),
        }
    }
}

impl FromValue for uuid::Uuid {
    fn from_value(value: &Value) -> Result<Self> {
        match value {
            Value::Uuid(v) => Ok(*v),
            Value::String(v) => uuid::Uuid::parse_str(v).map_err(|e| {
                crate::Error::TypeError(format!("failed to parse Uuid from string {:?}: {}", v, e))
            }),
            Value::Null => Err(crate::Error::TypeError("NULL value for Uuid".to_string())),
            _ => Err(crate::Error::TypeError(format!(
                "expected Uuid, got {:?}",
                value
            ))),
        }
    }
}

impl FromValue for serde_json::Value {
    fn from_value(value: &Value) -> Result<Self> {
        match value {
            Value::Null => Err(crate::Error::TypeError("NULL value for Json".to_string())),
            other => Ok(other.to_json_plain()),
        }
    }

    fn from_value_owned(value: Value) -> Result<Self> {
        match value {
            Value::Null => Err(crate::Error::TypeError("NULL value for Json".to_string())),
            other => Ok(other.to_json_plain()),
        }
    }
}

impl FromValue for BTreeMap<String, Option<String>> {
    fn from_value(value: &Value) -> Result<Self> {
        match value {
            Value::Hstore(map) => Ok(map.clone()),
            Value::Json(serde_json::Value::Object(map)) => decode_hstore_json_object(map),
            Value::Null => Err(crate::Error::TypeError("NULL value for Hstore".to_string())),
            other => Err(crate::Error::TypeError(format!(
                "expected Hstore or Json object, got {:?}",
                other
            ))),
        }
    }

    fn from_value_owned(value: Value) -> Result<Self> {
        match value {
            Value::Hstore(map) => Ok(map),
            Value::Json(serde_json::Value::Object(map)) => decode_hstore_json_object(&map),
            Value::Null => Err(crate::Error::TypeError("NULL value for Hstore".to_string())),
            other => Err(crate::Error::TypeError(format!(
                "expected Hstore or Json object, got {:?}",
                other
            ))),
        }
    }
}

impl FromValue for Vec<u8> {
    fn from_value(value: &Value) -> Result<Self> {
        match value {
            Value::Bytes(v) => Ok(v.clone()),
            Value::Null => Err(crate::Error::TypeError("NULL value for Bytes".to_string())),
            _ => Err(crate::Error::TypeError(format!(
                "expected Bytes, got {:?}",
                value
            ))),
        }
    }

    fn from_value_owned(value: Value) -> Result<Self> {
        match value {
            Value::Bytes(v) => Ok(v),
            Value::Null => Err(crate::Error::TypeError("NULL value for Bytes".to_string())),
            other => Err(crate::Error::TypeError(format!(
                "expected Bytes, got {:?}",
                other
            ))),
        }
    }
}

impl<T: FromValue> FromValue for Option<T> {
    fn from_value(value: &Value) -> Result<Self> {
        match value {
            Value::Null => Ok(None),
            _ => T::from_value(value).map(Some),
        }
    }

    fn from_value_owned(value: Value) -> Result<Self> {
        match value {
            Value::Null => Ok(None),
            _ => T::from_value_owned(value).map(Some),
        }
    }
}

/// Generates `FromValue` implementations for `Vec<T>` and `Vec<Vec<T>>` for
/// scalar types that already implement `FromValue`.
///
/// Both variants support:
/// - Native PostgreSQL array values (`Value::Array` / `Value::Array2D`)
/// - JSON-encoded arrays from MySQL and SQLite (`Value::Json`)
macro_rules! impl_vec_from_value {
    ($T:ty) => {
        impl FromValue for Vec<$T> {
            fn from_value(value: &Value) -> Result<Self> {
                match value {
                    Value::Array(items) => items.iter().map(<$T>::from_value).collect(),
                    Value::Json(json_value) => decode_json_array(json_value, <$T>::from_value),
                    Value::Null => Err(crate::Error::TypeError(
                        concat!("NULL value for Vec<", stringify!($T), ">").to_string(),
                    )),
                    _ => Err(crate::Error::TypeError(format!(
                        concat!(
                            "expected Array or Json for Vec<",
                            stringify!($T),
                            ">, got {:?}"
                        ),
                        value
                    ))),
                }
            }
        }

        impl FromValue for Vec<Vec<$T>> {
            fn from_value(value: &Value) -> Result<Self> {
                match value {
                    Value::Array2D(rows) => rows
                        .iter()
                        .map(|row| row.iter().map(<$T>::from_value).collect())
                        .collect(),
                    Value::Json(json_value) => decode_json_2d_array(json_value, <$T>::from_value),
                    Value::Null => Err(crate::Error::TypeError(
                        concat!("NULL value for Vec<Vec<", stringify!($T), ">>").to_string(),
                    )),
                    _ => Err(crate::Error::TypeError(format!(
                        concat!(
                            "expected Array2D or Json for Vec<Vec<",
                            stringify!($T),
                            ">>, got {:?}"
                        ),
                        value
                    ))),
                }
            }
        }
    };
}

impl_vec_from_value!(String);
impl_vec_from_value!(i32);
impl_vec_from_value!(i64);
impl_vec_from_value!(f64);
impl_vec_from_value!(bool);
impl_vec_from_value!(rust_decimal::Decimal);
impl_vec_from_value!(chrono::NaiveDateTime);
impl_vec_from_value!(uuid::Uuid);
impl_vec_from_value!(serde_json::Value);
impl_vec_from_value!(BTreeMap<String, Option<String>>);

fn decode_json_array<T, F>(json_value: &serde_json::Value, decoder: F) -> Result<Vec<T>>
where
    F: Fn(&Value) -> Result<T>,
{
    if let serde_json::Value::Array(arr) = json_value {
        let mut result = Vec::with_capacity(arr.len());
        for json_item in arr {
            let value = crate::value::json_to_value_ref(json_item);
            result.push(decoder(&value)?);
        }
        Ok(result)
    } else {
        Err(crate::Error::TypeError(format!(
            "expected JSON array, got {:?}",
            json_value
        )))
    }
}

fn decode_json_2d_array<T, F>(json_value: &serde_json::Value, decoder: F) -> Result<Vec<Vec<T>>>
where
    F: Fn(&Value) -> Result<T>,
{
    if let serde_json::Value::Array(outer_arr) = json_value {
        let mut result = Vec::with_capacity(outer_arr.len());
        for json_row in outer_arr {
            if let serde_json::Value::Array(inner_arr) = json_row {
                let mut row = Vec::with_capacity(inner_arr.len());
                for json_item in inner_arr {
                    let value = crate::value::json_to_value_ref(json_item);
                    row.push(decoder(&value)?);
                }
                result.push(row);
            } else {
                return Err(crate::Error::TypeError(format!(
                    "expected inner JSON array, got {:?}",
                    json_row
                )));
            }
        }
        Ok(result)
    } else {
        Err(crate::Error::TypeError(format!(
            "expected JSON 2D array, got {:?}",
            json_value
        )))
    }
}

fn decode_hstore_json_object(
    object: &serde_json::Map<String, serde_json::Value>,
) -> Result<BTreeMap<String, Option<String>>> {
    let mut decoded = BTreeMap::new();
    for (key, value) in object {
        let mapped = match value {
            serde_json::Value::String(item) => Some(item.clone()),
            serde_json::Value::Null => None,
            other => {
                return Err(crate::Error::TypeError(format!(
                    "expected Hstore JSON value to be string or null for key {:?}, got {:?}",
                    key, other
                )));
            }
        };
        decoded.insert(key.clone(), mapped);
    }
    Ok(decoded)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::FromValue;
    use crate::Value;

    #[test]
    fn decimal_uuid_datetime_and_json_arrays_decode_from_json_storage() {
        let decimals = Value::Json(serde_json::json!(["12.34", "56.78"]));
        let uuids = Value::Json(serde_json::json!([
            "550e8400-e29b-41d4-a716-446655440000",
            "123e4567-e89b-12d3-a456-426614174000"
        ]));
        let datetimes = Value::Json(serde_json::json!([
            "2026-02-18T10:30:45Z",
            "2026-02-19T11:31:46Z"
        ]));
        let jsons = Value::Json(serde_json::json!([
            {"name": "Alice"},
            42,
            true
        ]));

        let decimals: Vec<rust_decimal::Decimal> = Vec::from_value(&decimals).unwrap();
        let uuids: Vec<uuid::Uuid> = Vec::from_value(&uuids).unwrap();
        let datetimes: Vec<chrono::NaiveDateTime> = Vec::from_value(&datetimes).unwrap();
        let jsons: Vec<serde_json::Value> = Vec::from_value(&jsons).unwrap();

        assert_eq!(decimals.len(), 2);
        assert_eq!(uuids.len(), 2);
        assert_eq!(datetimes.len(), 2);
        assert_eq!(
            jsons,
            vec![
                serde_json::json!({"name": "Alice"}),
                serde_json::json!(42),
                serde_json::json!(true),
            ]
        );
    }

    #[test]
    fn uuid_and_decimal_scalars_accept_string_values() {
        let uuid = uuid::Uuid::from_value(&Value::String(
            "550e8400-e29b-41d4-a716-446655440000".to_string(),
        ))
        .unwrap();
        let decimal =
            rust_decimal::Decimal::from_value(&Value::String("12.34".to_string())).unwrap();

        assert_eq!(uuid.to_string(), "550e8400-e29b-41d4-a716-446655440000");
        assert_eq!(decimal.to_string(), "12.34");
    }

    #[test]
    fn hstore_scalar_accepts_native_and_json_object_values() {
        let native = Value::Hstore(BTreeMap::from([
            ("display_name".to_string(), Some("Bob".to_string())),
            ("nickname".to_string(), None),
        ]));
        let json = Value::Json(serde_json::json!({
            "display_name": "Bob",
            "nickname": null
        }));

        let native_map = BTreeMap::<String, Option<String>>::from_value(&native).unwrap();
        let json_map = BTreeMap::<String, Option<String>>::from_value(&json).unwrap();

        assert_eq!(native_map, json_map);
        assert_eq!(native_map["display_name"], Some("Bob".to_string()));
        assert_eq!(native_map["nickname"], None);
    }

    #[test]
    fn hstore_arrays_decode_from_json_storage() {
        let json = Value::Json(serde_json::json!([
            {"display_name": "Bob", "nickname": null},
            {"display_name": "OpenAI", "nickname": "oai"}
        ]));

        let decoded: Vec<BTreeMap<String, Option<String>>> = Vec::from_value(&json).unwrap();

        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0]["display_name"], Some("Bob".to_string()));
        assert_eq!(decoded[0]["nickname"], None);
        assert_eq!(decoded[1]["nickname"], Some("oai".to_string()));
    }
}
