//! Python-specific implementation of the shared language backend.

use nautilus_schema::ir::ScalarType;

use crate::backend::LanguageBackend;

/// Python language backend.
pub struct PythonBackend;

impl LanguageBackend for PythonBackend {
    fn scalar_to_type(&self, scalar: &ScalarType) -> &'static str {
        match scalar {
            ScalarType::String => "str",
            ScalarType::Int => "int",
            ScalarType::BigInt => "int",
            ScalarType::Float => "float",
            ScalarType::Decimal { .. } => "Decimal",
            ScalarType::Boolean => "bool",
            ScalarType::DateTime => "datetime",
            ScalarType::Bytes => "bytes",
            ScalarType::Json => "JsonValue",
            ScalarType::Uuid => "UUID",
            ScalarType::Citext | ScalarType::Ltree => "str",
            ScalarType::Hstore => "HstoreValue",
            ScalarType::Vector { .. } => "List[float]",
            ScalarType::Jsonb => "JsonValue",
            ScalarType::Xml | ScalarType::Char { .. } | ScalarType::VarChar { .. } => "str",
        }
    }

    fn array_type(&self, inner: &str) -> String {
        format!("List[{}]", inner)
    }

    fn not_in_suffix(&self) -> &'static str {
        "not_in"
    }

    fn startswith_suffix(&self) -> &'static str {
        "startswith"
    }

    fn endswith_suffix(&self) -> &'static str {
        "endswith"
    }

    fn null_suffix(&self) -> &'static str {
        "is_null"
    }

    fn null_literal(&self) -> &'static str {
        "None"
    }

    fn true_literal(&self) -> &'static str {
        "True"
    }

    fn false_literal(&self) -> &'static str {
        "False"
    }

    fn string_literal(&self, s: &str) -> String {
        format!("\"{}\"", s)
    }

    fn empty_array_literal(&self) -> &'static str {
        "Field(default_factory=list)"
    }
}
