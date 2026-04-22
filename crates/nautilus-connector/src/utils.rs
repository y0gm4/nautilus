//! Shared utilities for database value conversion.

use nautilus_core::Value;

/// Convert a [`Value`] to a [`serde_json::Value`] for JSON-based parameter binding.
///
/// Used by all three database backends when binding `Array` and `Array2D` parameters.
/// The function is recursive so nested arrays are handled correctly.
pub(crate) fn value_to_json(v: &Value) -> serde_json::Value {
    match v {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::I32(i) => serde_json::Value::Number((*i).into()),
        Value::I64(i) => serde_json::Value::Number((*i).into()),
        Value::F64(f) => serde_json::Value::Number(
            serde_json::Number::from_f64(*f).unwrap_or(serde_json::Number::from(0)),
        ),
        Value::Decimal(d) => serde_json::Value::String(d.to_string()),
        Value::DateTime(dt) => serde_json::Value::String(dt.to_string()),
        Value::Uuid(u) => serde_json::Value::String(u.to_string()),
        Value::Json(j) => j.clone(),
        Value::Hstore(map) => serde_json::Value::Object(
            map.iter()
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
        Value::String(s) => serde_json::Value::String(s.clone()),
        Value::Bytes(b) => {
            serde_json::Value::String(b.iter().map(|byte| format!("{:02x}", byte)).collect())
        }
        Value::Array(items) => serde_json::Value::Array(items.iter().map(value_to_json).collect()),
        Value::Array2D(rows) => serde_json::Value::Array(
            rows.iter()
                .map(|row| serde_json::Value::Array(row.iter().map(value_to_json).collect()))
                .collect(),
        ),
        Value::Enum { value, .. } => serde_json::Value::String(value.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_value_to_json_primitives() {
        assert_eq!(value_to_json(&Value::Null), serde_json::Value::Null);
        assert_eq!(
            value_to_json(&Value::Bool(true)),
            serde_json::Value::Bool(true)
        );
        assert_eq!(value_to_json(&Value::I32(42)), serde_json::json!(42));
        assert_eq!(value_to_json(&Value::I64(99)), serde_json::json!(99));
        assert_eq!(value_to_json(&Value::F64(1.5)), serde_json::json!(1.5));
        assert_eq!(
            value_to_json(&Value::String("hi".to_string())),
            serde_json::Value::String("hi".to_string()),
        );
    }

    #[test]
    fn test_value_to_json_array() {
        let arr = Value::Array(vec![Value::I64(1), Value::I64(2)]);
        assert_eq!(value_to_json(&arr), serde_json::json!([1, 2]));
    }

    #[test]
    fn test_value_to_json_array2d() {
        let arr2d = Value::Array2D(vec![
            vec![Value::I64(1), Value::I64(2)],
            vec![Value::I64(3), Value::I64(4)],
        ]);
        assert_eq!(value_to_json(&arr2d), serde_json::json!([[1, 2], [3, 4]]));
    }

    #[test]
    fn test_value_to_json_nested_json() {
        let inner = serde_json::json!({"a": 1});
        let v = Value::Json(inner.clone());
        assert_eq!(value_to_json(&v), inner);
    }

    #[test]
    fn test_value_to_json_hstore() {
        let value = Value::Hstore(std::collections::BTreeMap::from([
            ("display_name".to_string(), Some("Bob".to_string())),
            ("nickname".to_string(), None),
        ]));
        assert_eq!(
            value_to_json(&value),
            serde_json::json!({
                "display_name": "Bob",
                "nickname": null
            })
        );
    }
}
