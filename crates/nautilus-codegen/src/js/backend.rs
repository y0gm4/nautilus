//! JavaScript/TypeScript-specific implementation of the shared language backend.

use nautilus_schema::ir::ScalarType;

use crate::backend::LanguageBackend;

/// TypeScript / JavaScript language backend.
pub struct JsBackend;

impl LanguageBackend for JsBackend {
    fn scalar_to_type(&self, scalar: &ScalarType) -> &'static str {
        match scalar {
            ScalarType::String => "string",
            ScalarType::Int => "number",
            ScalarType::BigInt => "number",
            ScalarType::Float => "number",
            ScalarType::Decimal { .. } => "string", // preserve precision; no decimal.js dependency
            ScalarType::Boolean => "boolean",
            ScalarType::DateTime => "Date",
            ScalarType::Bytes => "Buffer",
            ScalarType::Json => "JsonValue",
            ScalarType::Uuid => "string",
            ScalarType::Citext | ScalarType::Ltree => "string",
            ScalarType::Hstore => "HstoreValue",
            ScalarType::Vector { .. } => "number[]",
            ScalarType::Jsonb => "JsonValue",
            ScalarType::Xml | ScalarType::Char { .. } | ScalarType::VarChar { .. } => "string",
        }
    }

    fn array_type(&self, inner: &str) -> String {
        format!("{}[]", inner)
    }

    fn not_in_suffix(&self) -> &'static str {
        "notIn"
    }

    fn startswith_suffix(&self) -> &'static str {
        "startsWith"
    }

    fn endswith_suffix(&self) -> &'static str {
        "endsWith"
    }

    fn null_suffix(&self) -> &'static str {
        "isNull"
    }

    fn null_literal(&self) -> &'static str {
        "null"
    }

    fn true_literal(&self) -> &'static str {
        "true"
    }

    fn false_literal(&self) -> &'static str {
        "false"
    }

    fn string_literal(&self, s: &str) -> String {
        format!("'{}'", s)
    }

    fn empty_array_literal(&self) -> &'static str {
        "[]"
    }

    fn enum_variant_literal(&self, variant: &str) -> String {
        format!("'{}'", variant)
    }

    fn relation_type(&self, target_model: &str) -> String {
        format!("{}Model", target_model)
    }
}
