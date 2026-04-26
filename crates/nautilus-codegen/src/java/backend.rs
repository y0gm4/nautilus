//! Java codegen backend helpers.

use crate::backend::LanguageBackend;
use nautilus_schema::ir::ScalarType;

/// Language backend for Java code generation.
#[derive(Debug, Clone, Copy, Default)]
pub struct JavaBackend;

impl LanguageBackend for JavaBackend {
    fn scalar_to_type(&self, scalar: &ScalarType) -> &'static str {
        match scalar {
            ScalarType::String
            | ScalarType::Citext
            | ScalarType::Ltree
            | ScalarType::Xml
            | ScalarType::Char { .. }
            | ScalarType::VarChar { .. } => "String",
            ScalarType::Hstore => "JsonSupport.Hstore",
            ScalarType::Vector { .. } => "List<Float>",
            ScalarType::Boolean => "Boolean",
            ScalarType::Int => "Integer",
            ScalarType::BigInt => "Long",
            ScalarType::Float => "Double",
            ScalarType::Decimal { .. } => "BigDecimal",
            ScalarType::DateTime => "OffsetDateTime",
            ScalarType::Bytes => "byte[]",
            ScalarType::Json | ScalarType::Jsonb => "JsonNode",
            ScalarType::Uuid => "UUID",
        }
    }

    fn array_type(&self, inner: &str) -> String {
        format!("List<{inner}>")
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
        format!("{s:?}")
    }

    fn empty_array_literal(&self) -> &'static str {
        "List.of()"
    }
}
