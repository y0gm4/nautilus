use nautilus_connector::Row;
use nautilus_core::Value;
use nautilus_engine::conversion::{
    check_protocol_version, json_to_value, json_to_value_field, normalize_row_with_hints,
    normalize_rows_with_hints, rows_to_raw_json, to_snake_case, ValueHint,
};
use nautilus_schema::ir::{ResolvedFieldType, ScalarType};
use serde_json::json;

#[test]
fn snake_case_basic_camel() {
    assert_eq!(to_snake_case("createdAt"), "created_at");
}

#[test]
fn snake_case_already_snake() {
    assert_eq!(to_snake_case("already_snake"), "already_snake");
}

#[test]
fn snake_case_pascal() {
    assert_eq!(to_snake_case("PascalCaseWord"), "pascal_case_word");
}

#[test]
fn snake_case_single_word() {
    assert_eq!(to_snake_case("name"), "name");
}

#[test]
fn snake_case_empty() {
    assert_eq!(to_snake_case(""), "");
}

#[test]
fn snake_case_uppercase_initial() {
    assert_eq!(to_snake_case("ID"), "i_d");
}

#[test]
fn json_to_value_null() {
    let val = json_to_value(&json!(null)).unwrap();
    assert_eq!(val, Value::Null);
}

#[test]
fn json_to_value_bool_true() {
    let val = json_to_value(&json!(true)).unwrap();
    assert_eq!(val, Value::Bool(true));
}

#[test]
fn json_to_value_bool_false() {
    let val = json_to_value(&json!(false)).unwrap();
    assert_eq!(val, Value::Bool(false));
}

#[test]
fn json_to_value_i32_range() {
    let val = json_to_value(&json!(42)).unwrap();
    assert_eq!(val, Value::I32(42));
}

#[test]
fn json_to_value_i64_range() {
    let big = i64::MAX;
    let val = json_to_value(&json!(big)).unwrap();
    assert_eq!(val, Value::I64(big));
}

#[test]
fn json_to_value_float() {
    let val = json_to_value(&json!(core::f64::consts::PI)).unwrap();
    assert_eq!(val, Value::F64(core::f64::consts::PI));
}

#[test]
fn json_to_value_string() {
    let val = json_to_value(&json!("hello")).unwrap();
    assert_eq!(val, Value::String("hello".to_string()));
}

#[test]
fn json_to_value_uuid_string() {
    let uuid_str = "550e8400-e29b-41d4-a716-446655440000";
    let val = json_to_value(&json!(uuid_str)).unwrap();
    let expected = uuid::Uuid::parse_str(uuid_str).unwrap();
    assert_eq!(val, Value::Uuid(expected));
}

#[test]
fn json_to_value_array() {
    let val = json_to_value(&json!([1, 2, 3])).unwrap();
    assert_eq!(
        val,
        Value::Array(vec![Value::I32(1), Value::I32(2), Value::I32(3)])
    );
}

#[test]
fn json_to_value_object() {
    let obj = json!({"key": "value"});
    let val = json_to_value(&obj).unwrap();
    assert_eq!(val, Value::Json(obj));
}

#[test]
fn json_to_value_field_hstore_object() {
    let val = json_to_value_field(
        &json!({"display_name": "Bob", "nickname": null}),
        &ResolvedFieldType::Scalar(ScalarType::Hstore),
    )
    .unwrap();
    assert_eq!(
        val,
        Value::Hstore(std::collections::BTreeMap::from([
            ("display_name".to_string(), Some("Bob".to_string())),
            ("nickname".to_string(), None),
        ]))
    );
}

#[test]
fn json_to_value_field_hstore_array_of_objects() {
    let val = json_to_value_field(
        &json!([
            {"display_name": "Bob", "nickname": null},
            {"display_name": "OpenAI", "nickname": "oai"}
        ]),
        &ResolvedFieldType::Scalar(ScalarType::Hstore),
    )
    .unwrap();
    assert_eq!(
        val,
        Value::Array(vec![
            Value::Hstore(std::collections::BTreeMap::from([
                ("display_name".to_string(), Some("Bob".to_string())),
                ("nickname".to_string(), None),
            ])),
            Value::Hstore(std::collections::BTreeMap::from([
                ("display_name".to_string(), Some("OpenAI".to_string())),
                ("nickname".to_string(), Some("oai".to_string())),
            ])),
        ])
    );
}

#[test]
fn json_to_value_field_hstore_rejects_non_string_values() {
    let err = json_to_value_field(
        &json!({"display_name": 42}),
        &ResolvedFieldType::Scalar(ScalarType::Hstore),
    )
    .unwrap_err();
    assert!(matches!(
        err,
        nautilus_protocol::ProtocolError::InvalidParams(_)
    ));
    assert!(err.to_string().contains("strings or nulls"));
}

#[test]
fn rows_to_raw_json_empty() {
    let raw = rows_to_raw_json(&[]).unwrap();
    let parsed: Vec<serde_json::Value> = serde_json::from_str(raw.get()).unwrap();
    assert!(parsed.is_empty());
}

#[test]
fn rows_to_raw_json_single_row() {
    let row = Row::new(vec![
        ("id".to_string(), Value::I64(1)),
        ("name".to_string(), Value::String("Alice".to_string())),
    ]);
    let raw = rows_to_raw_json(&[row]).unwrap();
    let parsed: Vec<serde_json::Value> = serde_json::from_str(raw.get()).unwrap();
    assert_eq!(parsed.len(), 1);
    assert_eq!(parsed[0]["id"], json!(1));
    assert_eq!(parsed[0]["name"], json!("Alice"));
}

#[test]
fn rows_to_raw_json_multiple_rows() {
    let rows = vec![
        Row::new(vec![("x".to_string(), Value::I32(10))]),
        Row::new(vec![("x".to_string(), Value::I32(20))]),
    ];
    let raw = rows_to_raw_json(&rows).unwrap();
    let parsed: Vec<serde_json::Value> = serde_json::from_str(raw.get()).unwrap();
    assert_eq!(parsed.len(), 2);
    assert_eq!(parsed[0]["x"], json!(10));
    assert_eq!(parsed[1]["x"], json!(20));
}

#[test]
fn rows_to_raw_json_uses_plain_json_for_enum_values() {
    let row = Row::new(vec![(
        "role".to_string(),
        Value::Enum {
            value: "USER".to_string(),
            type_name: "role".to_string(),
        },
    )]);
    let raw = rows_to_raw_json(&[row]).unwrap();
    let parsed: Vec<serde_json::Value> = serde_json::from_str(raw.get()).unwrap();
    assert_eq!(parsed, vec![json!({ "role": "USER" })]);
}

#[test]
fn normalize_row_with_hints_parses_schema_aware_values() {
    let row = Row::new(vec![
        ("price".to_string(), Value::F64(12.34)),
        (
            "profile".to_string(),
            Value::String(r#"{"name":"Alice","active":true}"#.to_string()),
        ),
        (
            "external_id".to_string(),
            Value::String("550e8400-e29b-41d4-a716-446655440000".to_string()),
        ),
        (
            "created_at".to_string(),
            Value::String("2026-03-30T12:34:56Z".to_string()),
        ),
    ]);

    let normalized = normalize_row_with_hints(
        row,
        &[
            Some(ValueHint::Decimal),
            Some(ValueHint::Json),
            Some(ValueHint::Uuid),
            Some(ValueHint::DateTime),
        ],
    )
    .unwrap();

    assert_eq!(
        normalized.get("price"),
        Some(&Value::Decimal(rust_decimal::Decimal::new(1234, 2)))
    );
    assert_eq!(
        normalized.get("profile"),
        Some(&Value::Json(json!({"name":"Alice","active":true})))
    );
    assert_eq!(
        normalized.get("external_id"),
        Some(&Value::Uuid(
            uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap()
        ))
    );
    assert_eq!(
        normalized.get("created_at"),
        Some(&Value::DateTime(
            chrono::NaiveDate::from_ymd_opt(2026, 3, 30)
                .unwrap()
                .and_hms_opt(12, 34, 56)
                .unwrap()
        ))
    );
}

#[test]
fn normalize_rows_with_hints_rejects_invalid_json() {
    let rows = vec![Row::new(vec![(
        "profile".to_string(),
        Value::String("not-json".to_string()),
    )])];

    let err = normalize_rows_with_hints(rows, &[Some(ValueHint::Json)]).unwrap_err();
    assert!(matches!(
        err,
        nautilus_protocol::ProtocolError::DatabaseExecution(_)
    ));
    assert!(err.to_string().contains("profile"));
}

#[test]
fn protocol_version_ok() {
    assert!(check_protocol_version(nautilus_protocol::PROTOCOL_VERSION).is_ok());
}

#[test]
fn protocol_version_v1_accepted() {
    assert!(check_protocol_version(1).is_ok());
}

#[test]
fn protocol_version_mismatch() {
    assert!(check_protocol_version(nautilus_protocol::PROTOCOL_VERSION + 1).is_err());
}

#[test]
fn protocol_version_zero_rejected() {
    assert!(check_protocol_version(0).is_err());
}
