//! pgvector-specific helpers for `CREATE INDEX` rendering.
//!
//! Lives next to [`super::ProviderStrategy`] but stays isolated so that
//! adding another extension family (PostGIS, full-text-ranked, ...) is a
//! sibling-module addition instead of a sprawling `if pgvector { ... }`
//! branch in the main provider code.

use nautilus_schema::ir::{PgvectorIndex, PgvectorIndexOptions, PgvectorMethod};

/// Renders the pgvector `WITH (...)` clause for a `CREATE INDEX` statement.
///
/// Returns an empty string when no options are set. The order of the
/// emitted parameters is **fixed** (`m`, `ef_construction`, `lists`) so the
/// generated DDL is deterministic across runs — important for snapshot
/// tests and for the diff round-trip invariant.
pub(super) fn with_clause(method: PgvectorMethod, options: &PgvectorIndexOptions) -> String {
    let mut parts = Vec::with_capacity(3);
    match method {
        PgvectorMethod::Hnsw => {
            if let Some(value) = options.m {
                parts.push(format!("m = {}", value));
            }
            if let Some(value) = options.ef_construction {
                parts.push(format!("ef_construction = {}", value));
            }
        }
        PgvectorMethod::Ivfflat => {
            if let Some(value) = options.lists {
                parts.push(format!("lists = {}", value));
            }
        }
    }

    if parts.is_empty() {
        String::new()
    } else {
        format!(" WITH ({})", parts.join(", "))
    }
}

/// Returns the operator-class suffix that must be appended to the first
/// indexed column for a pgvector index, or `None` when no opclass is set.
pub(super) fn opclass_suffix(index: &PgvectorIndex) -> Option<&'static str> {
    index.opclass.map(|c| c.as_str())
}

#[cfg(test)]
mod tests {
    use super::*;
    use nautilus_schema::ir::PgvectorOpClass;

    #[test]
    fn hnsw_with_full_options_emits_deterministic_clause() {
        let opts = PgvectorIndexOptions {
            m: Some(16),
            ef_construction: Some(64),
            lists: None,
        };
        assert_eq!(
            with_clause(PgvectorMethod::Hnsw, &opts),
            " WITH (m = 16, ef_construction = 64)"
        );
    }

    #[test]
    fn ivfflat_emits_lists_only() {
        let opts = PgvectorIndexOptions {
            m: None,
            ef_construction: None,
            lists: Some(100),
        };
        assert_eq!(
            with_clause(PgvectorMethod::Ivfflat, &opts),
            " WITH (lists = 100)"
        );
    }

    #[test]
    fn empty_options_emit_no_clause() {
        let opts = PgvectorIndexOptions::default();
        assert_eq!(with_clause(PgvectorMethod::Hnsw, &opts), "");
        assert_eq!(with_clause(PgvectorMethod::Ivfflat, &opts), "");
    }

    #[test]
    fn opclass_suffix_returns_canonical_name() {
        let idx = PgvectorIndex {
            method: PgvectorMethod::Hnsw,
            opclass: Some(PgvectorOpClass::CosineOps),
            options: PgvectorIndexOptions::default(),
        };
        assert_eq!(opclass_suffix(&idx), Some("vector_cosine_ops"));
    }
}
