//! Java type-mapping helpers.

use nautilus_schema::ir::{
    CompositeFieldIr, DefaultValue, EnumIr, FieldIr, ResolvedFieldType, ScalarType,
};
use std::collections::{BTreeMap, BTreeSet, HashMap};

use crate::backend::LanguageBackend;
use crate::java::backend::JavaBackend;

fn backend() -> JavaBackend {
    JavaBackend
}

fn resolved_base_type(
    resolved: &ResolvedFieldType,
    root_package: &str,
    current_model_or_type: &str,
) -> (String, BTreeSet<String>) {
    let mut imports = BTreeSet::new();

    match resolved {
        ResolvedFieldType::Scalar(scalar) => {
            let ty = backend().scalar_to_type(scalar).to_string();
            imports.extend(imports_for_scalar(scalar));
            (ty, imports)
        }
        ResolvedFieldType::Enum { enum_name } => {
            if enum_name != current_model_or_type {
                imports.insert(format!("{root_package}.enums.{enum_name}"));
            }
            (enum_name.clone(), imports)
        }
        ResolvedFieldType::CompositeType { type_name } => {
            if type_name != current_model_or_type {
                imports.insert(format!("{root_package}.types.{type_name}"));
            }
            (type_name.clone(), imports)
        }
        ResolvedFieldType::Relation(rel) => {
            if rel.target_model != current_model_or_type {
                imports.insert(format!("{root_package}.model.{}", rel.target_model));
            }
            (rel.target_model.clone(), imports)
        }
    }
}

pub fn field_base_type(
    field: &FieldIr,
    root_package: &str,
    current_model: &str,
) -> (String, BTreeSet<String>) {
    resolved_base_type(&field.field_type, root_package, current_model)
}

pub fn field_to_java_type(
    field: &FieldIr,
    root_package: &str,
    current_model: &str,
) -> (String, BTreeSet<String>) {
    let (base, mut imports) = field_base_type(field, root_package, current_model);
    if field.is_array {
        imports.insert("java.util.List".to_string());
        (format!("List<{base}>"), imports)
    } else {
        (base, imports)
    }
}

pub fn composite_field_to_java_type(
    field: &CompositeFieldIr,
    root_package: &str,
    current_type: &str,
) -> (String, BTreeSet<String>) {
    let (base, mut imports) = resolved_base_type(&field.field_type, root_package, current_type);
    if field.is_array {
        imports.insert("java.util.List".to_string());
        (format!("List<{base}>"), imports)
    } else {
        (base, imports)
    }
}

pub fn imports_for_scalar(scalar: &ScalarType) -> BTreeSet<String> {
    let mut imports = BTreeSet::new();
    match scalar {
        ScalarType::Decimal { .. } => {
            imports.insert("java.math.BigDecimal".to_string());
        }
        ScalarType::DateTime => {
            imports.insert("java.time.OffsetDateTime".to_string());
        }
        ScalarType::Json | ScalarType::Jsonb => {
            imports.insert("com.fasterxml.jackson.databind.JsonNode".to_string());
        }
        ScalarType::Uuid => {
            imports.insert("java.util.UUID".to_string());
        }
        _ => {}
    }
    imports
}

pub fn is_auto_generated(field: &FieldIr) -> bool {
    if field.computed.is_some() || field.is_updated_at {
        return true;
    }

    matches!(
        field.default_value,
        Some(DefaultValue::Function(ref call))
            if call.name == "autoincrement" || call.name == "uuid" || call.name == "now"
    )
}

pub fn is_writable_on_create(field: &FieldIr) -> bool {
    !is_auto_generated(field) && field.computed.is_none()
}

pub fn is_writable_on_update(field: &FieldIr, primary_key_fields: &[&str]) -> bool {
    if field.computed.is_some() || field.is_updated_at {
        return false;
    }

    let is_auto_pk =
        is_auto_generated(field) && primary_key_fields.contains(&field.logical_name.as_str());
    !is_auto_pk
}

pub fn is_numeric_field(field: &FieldIr) -> bool {
    matches!(
        field.field_type,
        ResolvedFieldType::Scalar(ScalarType::Int)
            | ResolvedFieldType::Scalar(ScalarType::BigInt)
            | ResolvedFieldType::Scalar(ScalarType::Float)
            | ResolvedFieldType::Scalar(ScalarType::Decimal { .. })
    )
}

pub fn is_orderable_field(field: &FieldIr) -> bool {
    !matches!(
        field.field_type,
        ResolvedFieldType::Scalar(ScalarType::Boolean)
            | ResolvedFieldType::Scalar(ScalarType::Json)
            | ResolvedFieldType::Scalar(ScalarType::Jsonb)
            | ResolvedFieldType::Scalar(ScalarType::Hstore)
            | ResolvedFieldType::Scalar(ScalarType::Bytes)
    )
}

pub fn filter_operators_for_field(
    field: &FieldIr,
    enums: &BTreeMap<String, EnumIr>,
) -> Vec<(String, String)> {
    let enums = enums
        .iter()
        .map(|(name, item)| (name.clone(), item.clone()))
        .collect::<HashMap<_, _>>();
    backend()
        .get_filter_operators_for_field(field, &enums)
        .into_iter()
        .map(|op| (op.suffix, op.type_name))
        .collect()
}
