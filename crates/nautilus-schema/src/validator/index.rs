//! Validation + assembly of [`IndexKind`] from `@@index(...)` AST inputs.
//!
//! This module is the single source of truth for the rule "given the raw
//! `@@index(...)` arguments and the indexed field's type, what is the
//! resulting [`IndexKind`] and what diagnostics should we report?".
//! The validator uses it to surface errors; the IR builder uses it to
//! produce the final [`IndexIr::kind`] and ignores the diagnostics.

use crate::ast::{FieldType, Ident};
use crate::error::SchemaError;
use crate::ir::DatabaseProvider;
use crate::ir::{
    parse_index_type_tag, IndexKind, IndexTypeTag, PgvectorIndex, PgvectorIndexOptions,
    PgvectorMethod, PgvectorOpClass, ALL_INDEX_TYPE_NAMES,
};
use crate::span::Span;

/// Raw inputs parsed from a `ModelAttribute::Index` attribute.
pub(super) struct RawIndexArgs<'a> {
    pub fields: &'a [Ident],
    pub index_type: Option<&'a Ident>,
    pub opclass: Option<&'a Ident>,
    pub m: Option<u32>,
    pub ef_construction: Option<u32>,
    pub lists: Option<u32>,
    pub model_span: Span,
}

/// Builds an [`IndexKind`] from the raw `@@index(...)` arguments and the
/// resolved type of the indexed field (for the "Vector field required"
/// check on pgvector indexes).
///
/// Returns the kind paired with every diagnostic the inputs produced. The
/// validator publishes the diagnostics; the IR builder discards them.
pub(super) fn build_index_kind(
    args: &RawIndexArgs<'_>,
    provider: Option<DatabaseProvider>,
    indexed_field_type: Option<&FieldType>,
    model_name: &str,
) -> (IndexKind, Vec<SchemaError>) {
    let mut diagnostics = Vec::new();

    let mut tag: Option<(IndexTypeTag, Span)> = None;
    if let Some(type_ident) = args.index_type {
        match parse_index_type_tag(&type_ident.value) {
            Some(t) => tag = Some((t, type_ident.span)),
            None => diagnostics.push(SchemaError::Validation(
                format!(
                    "Unknown index type '{}'. Valid types are: {}",
                    type_ident.value,
                    ALL_INDEX_TYPE_NAMES.join(", ")
                ),
                type_ident.span,
            )),
        }
    }

    if let Some((t, span)) = tag {
        if let Some(prov) = provider {
            let kind_for_check = match t {
                IndexTypeTag::Basic(b) => IndexKind::Basic(b),
                IndexTypeTag::Pgvector(method) => IndexKind::Pgvector(PgvectorIndex {
                    method,
                    opclass: None,
                    options: PgvectorIndexOptions::default(),
                }),
            };
            if !kind_for_check.supported_by(prov) {
                diagnostics.push(SchemaError::Validation(
                    format!(
                        "Index type '{}' is not supported by provider '{}' (supported by: {})",
                        kind_for_check.as_type_str().unwrap_or(""),
                        prov.as_str(),
                        kind_for_check.supported_providers()
                    ),
                    span,
                ));
            }
        }
    }

    let parsed_opclass = args.opclass.and_then(|opclass_ident| {
        match opclass_ident.value.parse::<PgvectorOpClass>() {
            Ok(value) => Some(value),
            Err(_) => {
                diagnostics.push(SchemaError::Validation(
                    format!(
                        "Unknown pgvector opclass '{}'. Valid values are: vector_l2_ops, vector_ip_ops, vector_cosine_ops",
                        opclass_ident.value
                    ),
                    opclass_ident.span,
                ));
                None
            }
        }
    });

    let pgvector_method = match tag {
        Some((IndexTypeTag::Pgvector(method), _)) => Some(method),
        _ => None,
    };

    let any_pgvector_arg = pgvector_method.is_some()
        || parsed_opclass.is_some()
        || args.m.is_some()
        || args.ef_construction.is_some()
        || args.lists.is_some();

    if !any_pgvector_arg {
        let kind = match tag {
            None => IndexKind::Default,
            Some((IndexTypeTag::Basic(b), _)) => IndexKind::Basic(b),
            Some((IndexTypeTag::Pgvector(_), _)) => unreachable!("handled above"),
        };
        return (kind, diagnostics);
    }

    let primary_span = args
        .fields
        .first()
        .map(|f| f.span)
        .unwrap_or(args.model_span);

    if args.fields.len() != 1 {
        diagnostics.push(SchemaError::Validation(
            "pgvector indexes require exactly one indexed field".to_string(),
            primary_span,
        ));
        return (
            build_pgvector_kind(
                pgvector_method,
                parsed_opclass,
                args,
                &mut diagnostics,
                primary_span,
            ),
            diagnostics,
        );
    }

    let field_ident = &args.fields[0];

    if let Some(ft) = indexed_field_type {
        if !matches!(ft, FieldType::Vector { .. }) {
            diagnostics.push(SchemaError::Validation(
                format!(
                    "pgvector indexes require a Vector field, but '{}.{}' is '{}'",
                    model_name, field_ident.value, ft
                ),
                field_ident.span,
            ));
        }
    }

    if pgvector_method.is_none() {
        diagnostics.push(SchemaError::Validation(
            "pgvector index arguments require `type: Hnsw` or `type: Ivfflat`".to_string(),
            field_ident.span,
        ));
    }

    if pgvector_method.is_some() && parsed_opclass.is_none() {
        diagnostics.push(SchemaError::Validation(
            "pgvector indexes require an explicit `opclass:` argument (`vector_l2_ops`, `vector_ip_ops`, or `vector_cosine_ops`)"
                .to_string(),
            field_ident.span,
        ));
    }

    if let Some(value) = args.m {
        if value == 0 {
            diagnostics.push(SchemaError::Validation(
                "`m` must be greater than 0".to_string(),
                field_ident.span,
            ));
        }
        if pgvector_method != Some(PgvectorMethod::Hnsw) {
            diagnostics.push(SchemaError::Validation(
                "`m` is only supported for `type: Hnsw`".to_string(),
                field_ident.span,
            ));
        }
    }

    if let Some(value) = args.ef_construction {
        if value == 0 {
            diagnostics.push(SchemaError::Validation(
                "`ef_construction` must be greater than 0".to_string(),
                field_ident.span,
            ));
        }
        if pgvector_method != Some(PgvectorMethod::Hnsw) {
            diagnostics.push(SchemaError::Validation(
                "`ef_construction` is only supported for `type: Hnsw`".to_string(),
                field_ident.span,
            ));
        }
    }

    if let Some(value) = args.lists {
        if value == 0 {
            diagnostics.push(SchemaError::Validation(
                "`lists` must be greater than 0".to_string(),
                field_ident.span,
            ));
        }
        if pgvector_method != Some(PgvectorMethod::Ivfflat) {
            diagnostics.push(SchemaError::Validation(
                "`lists` is only supported for `type: Ivfflat`".to_string(),
                field_ident.span,
            ));
        }
    }

    let kind = build_pgvector_kind(
        pgvector_method,
        parsed_opclass,
        args,
        &mut diagnostics,
        field_ident.span,
    );
    (kind, diagnostics)
}

/// Assembles the best [`IndexKind`] given a (possibly inconsistent) set of
/// pgvector inputs. When `method` is missing the function falls back to
/// `IndexKind::Default` so the IR remains constructible even in error
/// scenarios — diagnostics already explain the issue to the user.
fn build_pgvector_kind(
    method: Option<PgvectorMethod>,
    opclass: Option<PgvectorOpClass>,
    args: &RawIndexArgs<'_>,
    _diagnostics: &mut Vec<SchemaError>,
    _fallback_span: Span,
) -> IndexKind {
    let Some(method) = method else {
        return IndexKind::Default;
    };

    let options = match method {
        PgvectorMethod::Hnsw => PgvectorIndexOptions {
            m: args.m,
            ef_construction: args.ef_construction,
            lists: None,
        },
        PgvectorMethod::Ivfflat => PgvectorIndexOptions {
            m: None,
            ef_construction: None,
            lists: args.lists,
        },
    };

    IndexKind::Pgvector(PgvectorIndex {
        method,
        opclass,
        options,
    })
}
