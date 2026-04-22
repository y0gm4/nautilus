//! Unit-style integration tests for type_helpers (Rust) and python::type_mapper.

use std::collections::HashMap;

use nautilus_codegen::{
    backend::LanguageBackend,
    java::backend::JavaBackend,
    python::type_mapper::{
        field_to_python_type, get_base_python_type, get_default_value,
        get_filter_operators_for_field, get_filter_operators_for_scalar,
        is_auto_generated as py_is_auto_generated, scalar_to_python_type,
    },
    type_helpers::{field_to_rust_type, is_auto_generated},
};
use nautilus_schema::ir::{
    DefaultValue, EnumIr, FieldIr, FunctionCall, RelationIr, ResolvedFieldType, ScalarType,
};
use nautilus_schema::Span;

fn no_span() -> Span {
    Span::new(0, 0)
}

fn scalar_field(scalar: ScalarType, required: bool, array: bool) -> FieldIr {
    FieldIr {
        logical_name: "field".to_string(),
        db_name: "field".to_string(),
        field_type: ResolvedFieldType::Scalar(scalar),
        is_required: required,
        is_array: array,
        storage_strategy: None,
        default_value: None,
        is_unique: false,
        is_updated_at: false,
        computed: None,
        check: None,
        span: no_span(),
    }
}

fn autoincrement_field() -> FieldIr {
    FieldIr {
        logical_name: "id".to_string(),
        db_name: "id".to_string(),
        field_type: ResolvedFieldType::Scalar(ScalarType::Int),
        is_required: true,
        is_array: false,
        storage_strategy: None,
        default_value: Some(DefaultValue::Function(FunctionCall {
            name: "autoincrement".to_string(),
            args: vec![],
        })),
        is_unique: false,
        is_updated_at: false,
        computed: None,
        check: None,
        span: no_span(),
    }
}

fn uuid_field() -> FieldIr {
    FieldIr {
        logical_name: "id".to_string(),
        db_name: "id".to_string(),
        field_type: ResolvedFieldType::Scalar(ScalarType::Uuid),
        is_required: true,
        is_array: false,
        storage_strategy: None,
        default_value: Some(DefaultValue::Function(FunctionCall {
            name: "uuid".to_string(),
            args: vec![],
        })),
        is_unique: false,
        is_updated_at: false,
        computed: None,
        check: None,
        span: no_span(),
    }
}

fn now_field() -> FieldIr {
    FieldIr {
        logical_name: "createdAt".to_string(),
        db_name: "created_at".to_string(),
        field_type: ResolvedFieldType::Scalar(ScalarType::DateTime),
        is_required: true,
        is_array: false,
        storage_strategy: None,
        default_value: Some(DefaultValue::Function(FunctionCall {
            name: "now".to_string(),
            args: vec![],
        })),
        is_unique: false,
        is_updated_at: false,
        computed: None,
        check: None,
        span: no_span(),
    }
}

fn relation_field(
    target: &str,
    array: bool,
    fields: Vec<String>,
    references: Vec<String>,
) -> FieldIr {
    FieldIr {
        logical_name: "rel".to_string(),
        db_name: "rel".to_string(),
        field_type: ResolvedFieldType::Relation(RelationIr {
            name: None,
            target_model: target.to_string(),
            fields,
            references,
            on_delete: None,
            on_update: None,
        }),
        is_required: true,
        is_array: array,
        storage_strategy: None,
        default_value: None,
        is_unique: false,
        is_updated_at: false,
        computed: None,
        check: None,
        span: no_span(),
    }
}

fn empty_enums() -> HashMap<String, EnumIr> {
    HashMap::new()
}

#[test]
fn test_rust_scalar_required_string() {
    let f = scalar_field(ScalarType::String, true, false);
    assert_eq!(field_to_rust_type(&f), "String");
}

#[test]
fn test_rust_scalar_optional_string() {
    let f = scalar_field(ScalarType::String, false, false);
    assert_eq!(field_to_rust_type(&f), "Option<String>");
}

#[test]
fn test_rust_scalar_array_string() {
    let f = scalar_field(ScalarType::String, true, true);
    assert_eq!(field_to_rust_type(&f), "Vec<String>");
}

#[test]
fn test_rust_scalar_int() {
    let f = scalar_field(ScalarType::Int, true, false);
    assert_eq!(field_to_rust_type(&f), "i32");
}

#[test]
fn test_rust_scalar_bigint() {
    let f = scalar_field(ScalarType::BigInt, true, false);
    assert_eq!(field_to_rust_type(&f), "i64");
}

#[test]
fn test_rust_scalar_float() {
    let f = scalar_field(ScalarType::Float, true, false);
    assert_eq!(field_to_rust_type(&f), "f64");
}

#[test]
fn test_rust_scalar_boolean() {
    let f = scalar_field(ScalarType::Boolean, true, false);
    assert_eq!(field_to_rust_type(&f), "bool");
}

#[test]
fn test_rust_scalar_datetime() {
    let f = scalar_field(ScalarType::DateTime, true, false);
    assert_eq!(field_to_rust_type(&f), "chrono::NaiveDateTime");
}

#[test]
fn test_rust_scalar_uuid() {
    let f = scalar_field(ScalarType::Uuid, true, false);
    assert_eq!(field_to_rust_type(&f), "uuid::Uuid");
}

#[test]
fn test_rust_extension_backed_postgres_scalars_map_to_string() {
    for scalar in [ScalarType::Citext, ScalarType::Ltree] {
        let f = scalar_field(scalar, true, false);
        assert_eq!(field_to_rust_type(&f), "String");
    }
}

#[test]
fn test_rust_hstore_maps_to_btree_map() {
    let f = scalar_field(ScalarType::Hstore, true, false);
    assert_eq!(
        field_to_rust_type(&f),
        "std::collections::BTreeMap<String, Option<String>>"
    );
}

#[test]
fn test_rust_scalar_decimal() {
    let f = scalar_field(
        ScalarType::Decimal {
            precision: 10,
            scale: 2,
        },
        true,
        false,
    );
    assert_eq!(field_to_rust_type(&f), "rust_decimal::Decimal");
}

#[test]
fn test_rust_scalar_bytes() {
    let f = scalar_field(ScalarType::Bytes, true, false);
    assert_eq!(field_to_rust_type(&f), "Vec<u8>");
}

#[test]
fn test_rust_scalar_json() {
    let f = scalar_field(ScalarType::Json, true, false);
    assert_eq!(field_to_rust_type(&f), "serde_json::Value");
}

#[test]
fn test_rust_enum_type() {
    let mut enums = HashMap::new();
    enums.insert(
        "Role".to_string(),
        EnumIr {
            logical_name: "Role".to_string(),
            variants: vec!["USER".to_string(), "ADMIN".to_string()],
            span: no_span(),
        },
    );
    let field = FieldIr {
        logical_name: "role".to_string(),
        db_name: "role".to_string(),
        field_type: ResolvedFieldType::Enum {
            enum_name: "Role".to_string(),
        },
        is_required: true,
        is_array: false,
        storage_strategy: None,
        default_value: None,
        is_unique: false,
        is_updated_at: false,
        computed: None,
        check: None,
        span: no_span(),
    };
    assert_eq!(field_to_rust_type(&field), "Role");
}

#[test]
fn test_rust_relation_single() {
    let f = relation_field("Post", false, vec!["postId".into()], vec!["id".into()]);
    assert_eq!(field_to_rust_type(&f), "Option<Box<Post>>");
}

#[test]
fn test_rust_relation_array() {
    let f = relation_field("Post", true, vec![], vec![]);
    assert_eq!(field_to_rust_type(&f), "Vec<Post>");
}

#[test]
fn test_is_auto_generated_autoincrement() {
    assert!(is_auto_generated(&autoincrement_field()));
}

#[test]
fn test_is_auto_generated_uuid() {
    assert!(is_auto_generated(&uuid_field()));
}

#[test]
fn test_is_auto_generated_now_is_false() {
    // Rust codegen does NOT treat now() as auto-generated (callers may override)
    assert!(!is_auto_generated(&now_field()));
}

#[test]
fn test_is_auto_generated_plain_field_is_false() {
    assert!(!is_auto_generated(&scalar_field(
        ScalarType::String,
        true,
        false
    )));
}

#[test]
fn test_python_scalar_string() {
    assert_eq!(scalar_to_python_type(&ScalarType::String), "str");
}

#[test]
fn test_python_scalar_int() {
    assert_eq!(scalar_to_python_type(&ScalarType::Int), "int");
}

#[test]
fn test_python_scalar_bigint() {
    assert_eq!(scalar_to_python_type(&ScalarType::BigInt), "int");
}

#[test]
fn test_python_scalar_float() {
    assert_eq!(scalar_to_python_type(&ScalarType::Float), "float");
}

#[test]
fn test_python_scalar_decimal() {
    assert_eq!(
        scalar_to_python_type(&ScalarType::Decimal {
            precision: 10,
            scale: 2
        }),
        "Decimal"
    );
}

#[test]
fn test_python_scalar_boolean() {
    assert_eq!(scalar_to_python_type(&ScalarType::Boolean), "bool");
}

#[test]
fn test_python_scalar_datetime() {
    assert_eq!(scalar_to_python_type(&ScalarType::DateTime), "datetime");
}

#[test]
fn test_python_scalar_bytes() {
    assert_eq!(scalar_to_python_type(&ScalarType::Bytes), "bytes");
}

#[test]
fn test_python_scalar_json() {
    assert_eq!(scalar_to_python_type(&ScalarType::Json), "JsonValue");
}

#[test]
fn test_python_scalar_jsonb() {
    assert_eq!(scalar_to_python_type(&ScalarType::Jsonb), "JsonValue");
}

#[test]
fn test_python_scalar_uuid() {
    assert_eq!(scalar_to_python_type(&ScalarType::Uuid), "UUID");
}

#[test]
fn test_python_extension_backed_postgres_scalars_map_to_str() {
    assert_eq!(scalar_to_python_type(&ScalarType::Citext), "str");
    assert_eq!(scalar_to_python_type(&ScalarType::Ltree), "str");
}

#[test]
fn test_python_hstore_maps_to_dict_of_optional_strings() {
    assert_eq!(scalar_to_python_type(&ScalarType::Hstore), "HstoreValue");
}

#[test]
fn test_java_hstore_uses_json_support_runtime_type() {
    assert_eq!(
        JavaBackend.scalar_to_type(&ScalarType::Hstore),
        "JsonSupport.Hstore"
    );
}

#[test]
fn test_python_field_required() {
    let f = scalar_field(ScalarType::String, true, false);
    assert_eq!(field_to_python_type(&f, &empty_enums()), "str");
}

#[test]
fn test_python_field_optional() {
    let f = scalar_field(ScalarType::String, false, false);
    assert_eq!(field_to_python_type(&f, &empty_enums()), "Optional[str]");
}

#[test]
fn test_python_field_list() {
    let f = scalar_field(ScalarType::String, true, true);
    assert_eq!(field_to_python_type(&f, &empty_enums()), "List[str]");
}

#[test]
fn test_python_relation_single_is_optional() {
    let f = relation_field("User", false, vec!["userId".into()], vec!["id".into()]);
    // Single non-array relations are always Optional for lazy-loading
    assert!(field_to_python_type(&f, &empty_enums()).starts_with("Optional["));
}

#[test]
fn test_python_relation_array_is_list() {
    let f = relation_field("Post", true, vec![], vec![]);
    assert!(field_to_python_type(&f, &empty_enums()).starts_with("List["));
}

#[test]
fn test_py_is_auto_generated_autoincrement() {
    assert!(py_is_auto_generated(&autoincrement_field()));
}

#[test]
fn test_py_is_auto_generated_uuid() {
    assert!(py_is_auto_generated(&uuid_field()));
}

#[test]
fn test_py_is_auto_generated_now() {
    // Python codegen DOES treat now() as auto-generated (unlike Rust)
    assert!(py_is_auto_generated(&now_field()));
}

#[test]
fn test_get_default_value_string_literal() {
    let mut f = scalar_field(ScalarType::String, true, false);
    f.default_value = Some(DefaultValue::String("hello".to_string()));
    assert_eq!(get_default_value(&f), Some("\"hello\"".to_string()));
}

#[test]
fn test_get_default_value_number_literal() {
    let mut f = scalar_field(ScalarType::Int, true, false);
    f.default_value = Some(DefaultValue::Number("42".to_string()));
    assert_eq!(get_default_value(&f), Some("42".to_string()));
}

#[test]
fn test_get_default_value_boolean_true() {
    let mut f = scalar_field(ScalarType::Boolean, true, false);
    f.default_value = Some(DefaultValue::Boolean(true));
    assert_eq!(get_default_value(&f), Some("True".to_string()));
}

#[test]
fn test_get_default_value_boolean_false() {
    let mut f = scalar_field(ScalarType::Boolean, true, false);
    f.default_value = Some(DefaultValue::Boolean(false));
    assert_eq!(get_default_value(&f), Some("False".to_string()));
}

#[test]
fn test_get_default_value_now_fn_returns_none_literal() {
    assert_eq!(get_default_value(&now_field()), Some("None".to_string()));
}

#[test]
fn test_get_default_value_optional_field_with_no_default() {
    let f = scalar_field(ScalarType::String, false, false);
    // Optional field with no @default -> Python default is None
    assert_eq!(get_default_value(&f), Some("None".to_string()));
}

#[test]
fn test_get_default_value_array_field_default_factory() {
    let f = scalar_field(ScalarType::String, true, true);
    // Array fields -> Field(default_factory=list)
    assert_eq!(
        get_default_value(&f),
        Some("Field(default_factory=list)".to_string())
    );
}

#[test]
fn test_get_default_value_required_no_default_is_none() {
    let f = scalar_field(ScalarType::String, true, false);
    assert_eq!(get_default_value(&f), None);
}

/// String gets contains / startswith / endswith / in / not_in / not
#[test]
fn test_filter_operators_for_string() {
    let ops = get_filter_operators_for_scalar(&ScalarType::String);
    let suffixes: Vec<&str> = ops.iter().map(|o| o.suffix.as_str()).collect();
    assert!(
        suffixes.contains(&"contains"),
        "missing contains: {suffixes:?}"
    );
    assert!(
        suffixes.contains(&"startswith"),
        "missing startswith: {suffixes:?}"
    );
    assert!(
        suffixes.contains(&"endswith"),
        "missing endswith: {suffixes:?}"
    );
    assert!(suffixes.contains(&"in"), "missing in: {suffixes:?}");
    assert!(suffixes.contains(&"not_in"), "missing not_in: {suffixes:?}");
    assert!(suffixes.contains(&"not"), "missing not: {suffixes:?}");
}

#[test]
fn test_filter_operators_for_citext_match_string_shape() {
    let ops = get_filter_operators_for_scalar(&ScalarType::Citext);
    let suffixes: Vec<&str> = ops.iter().map(|o| o.suffix.as_str()).collect();
    for expected in ["contains", "startswith", "endswith", "in", "not_in", "not"] {
        assert!(
            suffixes.contains(&expected),
            "missing {expected}: {suffixes:?}"
        );
    }
}

#[test]
fn test_filter_operators_for_hstore_only_expose_not() {
    let ops = get_filter_operators_for_scalar(&ScalarType::Hstore);
    let suffixes: Vec<&str> = ops.iter().map(|o| o.suffix.as_str()).collect();
    assert_eq!(suffixes, vec!["not"]);
}

/// Int gets lt / lte / gt / gte / in / not_in / not
#[test]
fn test_filter_operators_for_int() {
    let ops = get_filter_operators_for_scalar(&ScalarType::Int);
    let suffixes: Vec<&str> = ops.iter().map(|o| o.suffix.as_str()).collect();
    for expected in ["lt", "lte", "gt", "gte", "in", "not_in", "not"] {
        assert!(
            suffixes.contains(&expected),
            "missing {expected}: {suffixes:?}"
        );
    }
    // Int operators use "int" as the Python type for comparisons
    let lt = ops.iter().find(|o| o.suffix == "lt").unwrap();
    assert_eq!(lt.type_name, "int");
}

/// Float gets numeric operators with "float" type
#[test]
fn test_filter_operators_for_float() {
    let ops = get_filter_operators_for_scalar(&ScalarType::Float);
    let lt = ops.iter().find(|o| o.suffix == "lt").expect("missing lt");
    assert_eq!(lt.type_name, "float");
}

/// Boolean has no range operators — only the implicit equality plus `not`
#[test]
fn test_filter_operators_for_boolean_has_only_not() {
    let ops = get_filter_operators_for_scalar(&ScalarType::Boolean);
    let suffixes: Vec<&str> = ops.iter().map(|o| o.suffix.as_str()).collect();
    // No range operators for booleans
    assert!(
        !suffixes.contains(&"lt"),
        "unexpected lt for boolean: {suffixes:?}"
    );
    assert!(
        suffixes.contains(&"not"),
        "missing not for boolean: {suffixes:?}"
    );
}

/// Enum field gets in / not_in / not
#[test]
fn test_filter_operators_for_enum_field() {
    let mut enums = HashMap::new();
    enums.insert(
        "Role".to_string(),
        EnumIr {
            logical_name: "Role".to_string(),
            variants: vec!["USER".to_string(), "ADMIN".to_string()],
            span: no_span(),
        },
    );
    let field = FieldIr {
        logical_name: "role".to_string(),
        db_name: "role".to_string(),
        field_type: ResolvedFieldType::Enum {
            enum_name: "Role".to_string(),
        },
        is_required: true,
        is_array: false,
        storage_strategy: None,
        default_value: None,
        is_unique: false,
        is_updated_at: false,
        computed: None,
        check: None,
        span: no_span(),
    };
    let ops = get_filter_operators_for_field(&field, &enums);
    let suffixes: Vec<&str> = ops.iter().map(|o| o.suffix.as_str()).collect();
    assert!(suffixes.contains(&"in"), "missing in: {suffixes:?}");
    assert!(suffixes.contains(&"not_in"), "missing not_in: {suffixes:?}");
    assert!(suffixes.contains(&"not"), "missing not: {suffixes:?}");
    // Enum types are used, not "str"
    let in_op = ops.iter().find(|o| o.suffix == "in").unwrap();
    assert_eq!(in_op.type_name, "List[Role]");
}

/// Optional field gets an additional is_null operator
#[test]
fn test_filter_operators_optional_field_has_is_null() {
    let f = scalar_field(ScalarType::String, false, false); // is_required = false
    let ops = get_filter_operators_for_field(&f, &empty_enums());
    let suffixes: Vec<&str> = ops.iter().map(|o| o.suffix.as_str()).collect();
    assert!(
        suffixes.contains(&"is_null"),
        "missing is_null for optional field: {suffixes:?}"
    );
}

/// Required field does NOT get is_null
#[test]
fn test_filter_operators_required_field_no_is_null() {
    let f = scalar_field(ScalarType::String, true, false);
    let ops = get_filter_operators_for_field(&f, &empty_enums());
    let suffixes: Vec<&str> = ops.iter().map(|o| o.suffix.as_str()).collect();
    assert!(
        !suffixes.contains(&"is_null"),
        "unexpected is_null for required field: {suffixes:?}"
    );
}

#[test]
fn test_get_base_python_type_scalar() {
    let f = scalar_field(ScalarType::Int, true, false);
    assert_eq!(get_base_python_type(&f, &empty_enums()), "int");
}

#[test]
fn test_get_base_python_type_ignores_optional_wrapper() {
    // base type should be the raw Python type, not Optional[T]
    let f = scalar_field(ScalarType::String, false, false);
    assert_eq!(get_base_python_type(&f, &empty_enums()), "str");
}

#[test]
fn test_get_base_python_type_ignores_list_wrapper() {
    let f = scalar_field(ScalarType::Boolean, true, true);
    assert_eq!(get_base_python_type(&f, &empty_enums()), "bool");
}

#[test]
fn test_get_base_python_type_enum() {
    let mut enums = HashMap::new();
    enums.insert(
        "Status".to_string(),
        EnumIr {
            logical_name: "Status".to_string(),
            variants: vec!["A".to_string()],
            span: no_span(),
        },
    );
    let field = FieldIr {
        logical_name: "status".to_string(),
        db_name: "status".to_string(),
        field_type: ResolvedFieldType::Enum {
            enum_name: "Status".to_string(),
        },
        is_required: true,
        is_array: false,
        storage_strategy: None,
        default_value: None,
        is_unique: false,
        is_updated_at: false,
        computed: None,
        check: None,
        span: no_span(),
    };
    assert_eq!(get_base_python_type(&field, &enums), "Status");
}

#[test]
fn test_get_base_python_type_relation_returns_target_model() {
    let f = relation_field("Order", false, vec!["orderId".into()], vec!["id".into()]);
    // Base type for a relation is the target model name (no Optional/List wrapper)
    assert_eq!(get_base_python_type(&f, &empty_enums()), "Order");
}
