//! Cross-backend helpers for pgvector metadata collected from a [`ModelIr`].
//!
//! Every codegen backend (Python, JS, Java) needs to know two things about
//! each model: whether it has any `Vector(...)` field at all, and the names
//! of those fields (for `findMany({ nearest: ... })` API surface). Before
//! this module that logic was reimplemented per backend with the same
//! `matches!(field.field_type, ResolvedFieldType::Scalar(ScalarType::Vector { .. }))`
//! predicate sprinkled in three places. Centralising it here keeps a single
//! definition of "what counts as a vector field" — extending the predicate
//! later (e.g. to recognise PostGIS geometry columns) becomes a one-line
//! change in one file.

use nautilus_schema::ir::ModelIr;

/// Pre-computed pgvector metadata for a single model.
///
/// Inserted into the Tera context under the keys `has_vector_fields` and
/// `vector_field_names` — the existing template variable names are
/// preserved so no template touches are required.
#[derive(Debug, Clone, Default)]
pub struct VectorMeta {
    /// `true` when the model has at least one `Vector(...)` field.
    pub has_vector: bool,
    /// Logical names of every `Vector(...)` field, in declaration order.
    pub field_names: Vec<String>,
}

impl VectorMeta {
    /// Collects pgvector metadata from a model's field list.
    pub fn from_model(model: &ModelIr) -> Self {
        let field_names: Vec<String> = model
            .vector_field_names()
            .map(|name| name.to_string())
            .collect();
        Self {
            has_vector: !field_names.is_empty(),
            field_names,
        }
    }
}
