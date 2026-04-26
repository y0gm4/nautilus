//! Language-backend abstraction for Nautilus code generation.
//!
//! Concrete backends live in their language-specific modules (for example
//! `crate::python::backend` and `crate::js::backend`). This module keeps only
//! the shared trait, data structures, and default logic used by all language
//! backends.

use nautilus_schema::ir::{
    DefaultValue, EnumIr, FieldIr, FunctionCall, ResolvedFieldType, ScalarType,
};
use serde::Serialize;
use std::collections::HashMap;

/// A filter operator entry produced by a [`LanguageBackend`].
///
/// The `type_name` field holds the target-language type as a string (e.g.
/// `"str"`, `"List[int]"`, `"string[]"`).  Generator code converts this into a
/// template-context struct whose field may be named differently
/// (`python_type`, `ts_type`, …).
#[derive(Debug, Clone, Serialize)]
pub struct FilterOperator {
    pub suffix: String,
    pub type_name: String,
}

/// Common interface for language-specific code generation backends.
///
/// ## Abstract methods
/// Backends must implement the four type-mapping primitives and the four
/// operator-naming conventions.
///
/// ## Default methods
/// Everything else — `is_auto_generated`, the numeric-operator helper, and the
/// full filter-operator builders — is provided as a default implementation that
/// composes the abstract methods.  Backends only override these defaults when
/// their language genuinely diverges from the shared logic.
pub trait LanguageBackend {
    /// Maps a Nautilus scalar type to the target language's type name.
    fn scalar_to_type(&self, scalar: &ScalarType) -> &'static str;

    /// Wraps a type name in the language's array/list syntax.
    ///
    /// Examples: `"List[T]"` (Python) vs `"T[]"` (TypeScript).
    fn array_type(&self, inner: &str) -> String;

    /// Suffix for the "not in collection" operator.
    ///
    /// Python: `"not_in"` — TypeScript: `"notIn"`
    fn not_in_suffix(&self) -> &'static str;

    /// Suffix for the "starts with" string operator.
    ///
    /// Python: `"startswith"` — TypeScript: `"startsWith"`
    fn startswith_suffix(&self) -> &'static str;

    /// Suffix for the "ends with" string operator.
    ///
    /// Python: `"endswith"` — TypeScript: `"endsWith"`
    fn endswith_suffix(&self) -> &'static str;

    /// Suffix for the null-check operator.
    ///
    /// Python: `"is_null"` — TypeScript: `"isNull"`
    fn null_suffix(&self) -> &'static str;

    /// Returns `true` for fields whose values are supplied automatically by the
    /// database: `autoincrement()`, `uuid()`, or `now()`.
    ///
    /// This implementation is identical for Python and TypeScript.  The Rust
    /// backend intentionally differs (it exposes `now()` fields as writable),
    /// which is why it lives in `type_helpers.rs` and does not use this trait.
    fn is_auto_generated(&self, field: &FieldIr) -> bool {
        if field.computed.is_some() {
            return true;
        }
        if let Some(default) = &field.default_value {
            matches!(
                default,
                DefaultValue::Function(f)
                    if f.name == "autoincrement" || f.name == "uuid" || f.name == "now"
            )
        } else {
            false
        }
    }

    /// Returns the standard comparison operators (`lt`, `lte`, `gt`, `gte`,
    /// `in`, `not_in`/`notIn`) for a numeric-like type.
    fn numeric_operators(&self, type_name: &str) -> Vec<FilterOperator> {
        let arr = self.array_type(type_name);
        vec![
            FilterOperator {
                suffix: "lt".to_string(),
                type_name: type_name.to_string(),
            },
            FilterOperator {
                suffix: "lte".to_string(),
                type_name: type_name.to_string(),
            },
            FilterOperator {
                suffix: "gt".to_string(),
                type_name: type_name.to_string(),
            },
            FilterOperator {
                suffix: "gte".to_string(),
                type_name: type_name.to_string(),
            },
            FilterOperator {
                suffix: "in".to_string(),
                type_name: arr.clone(),
            },
            FilterOperator {
                suffix: self.not_in_suffix().to_string(),
                type_name: arr,
            },
        ]
    }

    /// Returns the filter operators available for a given scalar type.
    fn get_filter_operators_for_scalar(&self, scalar: &ScalarType) -> Vec<FilterOperator> {
        let mut ops: Vec<FilterOperator> = Vec::new();

        match scalar {
            ScalarType::String | ScalarType::Citext | ScalarType::Ltree => {
                let str_t = self.scalar_to_type(&ScalarType::String);
                let arr = self.array_type(str_t);
                ops.push(FilterOperator {
                    suffix: "contains".to_string(),
                    type_name: str_t.to_string(),
                });
                ops.push(FilterOperator {
                    suffix: self.startswith_suffix().to_string(),
                    type_name: str_t.to_string(),
                });
                ops.push(FilterOperator {
                    suffix: self.endswith_suffix().to_string(),
                    type_name: str_t.to_string(),
                });
                ops.push(FilterOperator {
                    suffix: "in".to_string(),
                    type_name: arr.clone(),
                });
                ops.push(FilterOperator {
                    suffix: self.not_in_suffix().to_string(),
                    type_name: arr,
                });
            }
            ScalarType::Hstore | ScalarType::Vector { .. } => {}
            ScalarType::Int | ScalarType::BigInt => {
                ops.extend(self.numeric_operators(self.scalar_to_type(scalar)));
            }
            ScalarType::Float => {
                ops.extend(self.numeric_operators(self.scalar_to_type(scalar)));
            }
            ScalarType::Decimal { .. } => {
                ops.extend(self.numeric_operators(self.scalar_to_type(scalar)));
            }
            ScalarType::DateTime => {
                ops.extend(self.numeric_operators(self.scalar_to_type(scalar)));
            }
            ScalarType::Uuid => {
                let uuid_t = self.scalar_to_type(&ScalarType::Uuid);
                let arr = self.array_type(uuid_t);
                ops.push(FilterOperator {
                    suffix: "in".to_string(),
                    type_name: arr.clone(),
                });
                ops.push(FilterOperator {
                    suffix: self.not_in_suffix().to_string(),
                    type_name: arr,
                });
            }
            ScalarType::Xml | ScalarType::Char { .. } | ScalarType::VarChar { .. } => {
                let str_t = self.scalar_to_type(scalar);
                let arr = self.array_type(str_t);
                ops.push(FilterOperator {
                    suffix: "contains".to_string(),
                    type_name: str_t.to_string(),
                });
                ops.push(FilterOperator {
                    suffix: self.startswith_suffix().to_string(),
                    type_name: str_t.to_string(),
                });
                ops.push(FilterOperator {
                    suffix: self.endswith_suffix().to_string(),
                    type_name: str_t.to_string(),
                });
                ops.push(FilterOperator {
                    suffix: "in".to_string(),
                    type_name: arr.clone(),
                });
                ops.push(FilterOperator {
                    suffix: self.not_in_suffix().to_string(),
                    type_name: arr,
                });
            }
            // Boolean, Bytes, Json, Jsonb, Vector: only equality via the direct field value.
            ScalarType::Boolean | ScalarType::Bytes | ScalarType::Json | ScalarType::Jsonb => {}
        }

        // `not` is supported for all scalar types.
        ops.push(FilterOperator {
            suffix: "not".to_string(),
            type_name: self.scalar_to_type(scalar).to_string(),
        });

        ops
    }

    /// Returns filter operators for a field, considering its resolved type
    /// (scalar, enum, or relation).
    fn get_filter_operators_for_field(
        &self,
        field: &FieldIr,
        enums: &HashMap<String, EnumIr>,
    ) -> Vec<FilterOperator> {
        let mut ops: Vec<FilterOperator> = Vec::new();

        match &field.field_type {
            ResolvedFieldType::Scalar(scalar) => {
                ops = self.get_filter_operators_for_scalar(scalar);
            }
            ResolvedFieldType::Enum { enum_name } => {
                let enum_type = if enums.contains_key(enum_name) {
                    enum_name.clone()
                } else {
                    // Fall back to the language's string type.
                    self.scalar_to_type(&ScalarType::String).to_string()
                };
                let arr = self.array_type(&enum_type);
                ops.push(FilterOperator {
                    suffix: "in".to_string(),
                    type_name: arr.clone(),
                });
                ops.push(FilterOperator {
                    suffix: self.not_in_suffix().to_string(),
                    type_name: arr,
                });
                ops.push(FilterOperator {
                    suffix: "not".to_string(),
                    type_name: enum_type,
                });
            }
            ResolvedFieldType::Relation(_) | ResolvedFieldType::CompositeType { .. } => {
                // Relations and composite types are not filterable via scalar operators.
            }
        }

        // Null-check operator for optional / auto-generated fields.
        if !field.is_required || self.is_auto_generated(field) {
            ops.push(FilterOperator {
                suffix: self.null_suffix().to_string(),
                type_name: self.scalar_to_type(&ScalarType::Boolean).to_string(),
            });
        }

        ops
    }

    /// The null literal in this language (Python: `"None"`, TS: `"null"`).
    fn null_literal(&self) -> &'static str;

    /// The boolean true literal (Python: `"True"`, TS: `"true"`).
    fn true_literal(&self) -> &'static str;

    /// The boolean false literal (Python: `"False"`, TS: `"false"`).
    fn false_literal(&self) -> &'static str;

    /// Format a string literal (Python: `"\"hello\""`, TS: `"'hello'"`).
    fn string_literal(&self, s: &str) -> String;

    /// The empty-array factory expression (Python: `"Field(default_factory=list)"`, TS: `"[]"`).
    fn empty_array_literal(&self) -> &'static str;

    /// Format an enum variant as a default value (Python: unquoted, TS: single-quoted).
    fn enum_variant_literal(&self, variant: &str) -> String {
        variant.to_string()
    }

    /// Resolves the base type name for a relation field.
    ///
    /// Python uses the model name directly; TypeScript appends `Model`.
    fn relation_type(&self, target_model: &str) -> String {
        target_model.to_string()
    }

    /// Returns the bare base type for a field without array or optional wrappers.
    fn get_base_type(&self, field: &FieldIr, enums: &HashMap<String, EnumIr>) -> String {
        match &field.field_type {
            ResolvedFieldType::Scalar(scalar) => self.scalar_to_type(scalar).to_string(),
            ResolvedFieldType::Enum { enum_name } => {
                if enums.contains_key(enum_name) {
                    enum_name.clone()
                } else {
                    self.scalar_to_type(&ScalarType::String).to_string()
                }
            }
            ResolvedFieldType::CompositeType { type_name } => type_name.clone(),
            ResolvedFieldType::Relation(rel) => self.relation_type(&rel.target_model),
        }
    }

    /// Returns the default value expression for a field, or `None` if no default.
    fn get_default_value(&self, field: &FieldIr) -> Option<String> {
        if let Some(default) = &field.default_value {
            match default {
                DefaultValue::Function(FunctionCall { name, .. })
                    if matches!(name.as_str(), "now" | "uuid" | "autoincrement") =>
                {
                    return Some(self.null_literal().to_string());
                }
                DefaultValue::Function(_) => return None,
                DefaultValue::String(s) => return Some(self.string_literal(s)),
                DefaultValue::Number(n) => return Some(n.clone()),
                DefaultValue::Boolean(b) => {
                    return Some(if *b {
                        self.true_literal().to_string()
                    } else {
                        self.false_literal().to_string()
                    });
                }
                DefaultValue::EnumVariant(v) => return Some(self.enum_variant_literal(v)),
            }
        }

        if field.is_array {
            Some(self.empty_array_literal().to_string())
        } else if !field.is_required {
            Some(self.null_literal().to_string())
        } else {
            None
        }
    }
}
