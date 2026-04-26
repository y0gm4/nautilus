//! Structured argument types for query operations.

use std::collections::HashMap;

use crate::{Expr, OrderBy, Value};

/// Similarity metric used for pgvector nearest-neighbor queries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum VectorMetric {
    /// Euclidean / L2 distance (`<->`, `vector_l2_ops`).
    L2,
    /// Maximum inner product (`<#>`, `vector_ip_ops`).
    InnerProduct,
    /// Cosine distance (`<=>`, `vector_cosine_ops`).
    Cosine,
}

impl VectorMetric {
    /// Wire-format string used by the engine JSON protocol.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::L2 => "l2",
            Self::InnerProduct => "innerProduct",
            Self::Cosine => "cosine",
        }
    }
}

/// Nearest-neighbor search specification for a pgvector field.
#[derive(Debug, Clone, PartialEq)]
pub struct VectorNearest {
    /// Logical field name of the vector column to search against.
    pub field: String,
    /// Query embedding to compare against the stored vectors.
    pub query: Vec<f32>,
    /// Distance metric to use for ordering.
    pub metric: VectorMetric,
}

/// Arguments for eagerly loading a single relation in a query.
///
/// ```text
/// include: { posts: { where: { published: true } } }
/// ```
#[derive(Debug, Default, Clone)]
pub struct IncludeRelation {
    /// Optional filter to apply to the included child records.
    pub where_: Option<Expr>,
    /// ORDER BY clauses to apply to the included child records.
    pub order_by: Vec<OrderBy>,
    /// Maximum number of child rows to include.
    pub take: Option<i32>,
    /// Number of child rows to skip.
    pub skip: Option<u32>,
    /// Optional cursor for child pagination.
    pub cursor: Option<HashMap<String, Value>>,
    /// Columns to deduplicate child rows on.
    pub distinct: Vec<String>,
    /// Nested relations to include under this child relation.
    pub include: HashMap<String, IncludeRelation>,
}

impl IncludeRelation {
    /// Create a plain include with no child filter.
    pub fn plain() -> Self {
        Self::default()
    }

    /// Create an include with a child filter.
    pub fn with_filter(filter: Expr) -> Self {
        Self {
            where_: Some(filter),
            ..Self::default()
        }
    }

    /// Append an ORDER BY clause for the included child records.
    pub fn with_order_by(mut self, order: OrderBy) -> Self {
        self.order_by.push(order);
        self
    }

    /// Set the child LIMIT.
    pub fn with_take(mut self, take: i32) -> Self {
        self.take = Some(take);
        self
    }

    /// Set the child OFFSET.
    pub fn with_skip(mut self, skip: u32) -> Self {
        self.skip = Some(skip);
        self
    }

    /// Set the child cursor.
    pub fn with_cursor(mut self, cursor: HashMap<String, Value>) -> Self {
        self.cursor = Some(cursor);
        self
    }

    /// Set child DISTINCT columns.
    pub fn with_distinct(mut self, distinct: Vec<String>) -> Self {
        self.distinct = distinct;
        self
    }

    /// Add a nested relation include.
    pub fn with_include(mut self, relation: impl Into<String>, include: IncludeRelation) -> Self {
        self.include.insert(relation.into(), include);
        self
    }
}

/// Arguments accepted by `find_unique` and `find_unique_or_throw` delegate methods.
///
/// Uses a required `where_` filter (no ordering/pagination — implicit LIMIT 1).
///
/// # Example
/// ```rust,ignore
/// let args = FindUniqueArgs::new(
///     User::columns().email.eq("alice@example.com"),
/// );
/// let user = client.user.find_unique(args).await?;
/// ```
#[derive(Debug, Clone)]
pub struct FindUniqueArgs {
    /// WHERE filter expression (required — must reference a unique/PK field).
    pub where_: Expr,
    /// Projection: only return the specified fields.
    ///
    /// If empty, all columns are returned. When specified, PK columns are
    /// always included regardless. Cannot be used together with `include`.
    pub select: HashMap<String, bool>,
    /// Relations to eager-load for the matching record.
    ///
    /// Cannot be used together with `select`.
    pub include: HashMap<String, IncludeRelation>,
}

impl FindUniqueArgs {
    /// Construct with a required filter expression.
    pub fn new(filter: Expr) -> Self {
        FindUniqueArgs {
            where_: filter,
            select: HashMap::new(),
            include: HashMap::new(),
        }
    }

    /// Add a relation include.
    pub fn with_include(mut self, relation: impl Into<String>, include: IncludeRelation) -> Self {
        self.include.insert(relation.into(), include);
        self
    }

    /// Select a scalar field.
    pub fn with_select(mut self, field: impl Into<String>) -> Self {
        self.select.insert(field.into(), true);
        self
    }
}

/// Arguments accepted by `find_many` and `find_first` delegate methods.
///
/// All fields are optional and default to "no constraint".
///
/// # Example
/// ```rust,ignore
/// let args = FindManyArgs {
///     where_: Some(User::columns().email.eq("alice@example.com")),
///     take: Some(10),
///     ..Default::default()
/// };
/// let users = client.user.find_many(args).await?;
/// ```
#[derive(Debug, Default, Clone)]
pub struct FindManyArgs {
    /// Optional WHERE filter expression.
    pub where_: Option<Expr>,
    /// ORDER BY clauses (applied in order).
    pub order_by: Vec<OrderBy>,
    /// Maximum number of rows to return (LIMIT).
    ///
    /// Positive values paginate **forward**; negative values paginate
    /// **backward** (reverses the result set in application code after
    /// flipping `ORDER BY` directions — no DB-specific SQL needed).
    /// Only meaningful when `cursor` is also set.
    pub take: Option<i32>,
    /// Number of rows to skip (OFFSET), applied relative to the cursor position
    /// when `cursor` is set, or from the start of the result set otherwise.
    pub skip: Option<u32>,
    /// Relations to eager-load, with optional per-relation filters.
    ///
    /// Key is the relation field name (e.g. `"posts"`), value controls
    /// how that relation is included (filter, etc.).
    ///
    /// Cannot be used together with `select`.
    pub include: HashMap<String, IncludeRelation>,
    /// Projection: only return the specified scalar fields.
    ///
    /// Key is the field name (logical name), value must be `true` to include
    /// the field. PK fields are always returned regardless. When empty, all
    /// columns are returned.
    ///
    /// Cannot be used together with `include`.
    pub select: HashMap<String, bool>,
    /// Cursor for stable (keyset) pagination.
    ///
    /// A map of **primary-key field name -> value** that identifies the record
    /// from which the page should start.  When
    /// combined with `take` / `skip`, they are applied relative to this anchor
    /// record rather than from the absolute start of the table.
    ///
    /// All primary-key fields of the model must be present in the map.
    pub cursor: Option<HashMap<String, Value>>,
    /// Columns to deduplicate on (SELECT DISTINCT / DISTINCT ON).
    ///
    /// Specifying one or more field names activates column-level deduplication:
    /// - **Postgres**: rendered as `SELECT DISTINCT ON (col, ...)` with those
    ///   columns automatically prepended to `ORDER BY` as required by Postgres.
    /// - **SQLite / MySQL**: rendered as plain `SELECT DISTINCT` (full-row
    ///   deduplication — most effective when combined with `select` projection).
    ///
    /// When empty (the default), no deduplication is applied.
    pub distinct: Vec<String>,
    /// Optional pgvector nearest-neighbor search.
    ///
    /// When set, the engine orders the result set by vector distance on the
    /// specified field. Callers must also provide a positive `take` so the
    /// query remains bounded.
    pub nearest: Option<VectorNearest>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn include_relation_builder_methods_populate_all_fields() {
        let cursor = HashMap::from([("id".to_string(), Value::I64(5))]);
        let include =
            IncludeRelation::with_filter(Expr::column("posts__published").eq(Expr::param(true)))
                .with_order_by(OrderBy::desc("posts__created_at"))
                .with_take(10)
                .with_skip(2)
                .with_cursor(cursor.clone())
                .with_distinct(vec!["title".to_string()])
                .with_include("comments", IncludeRelation::plain());

        assert!(include.where_.is_some());
        assert_eq!(include.order_by.len(), 1);
        assert_eq!(include.take, Some(10));
        assert_eq!(include.skip, Some(2));
        assert_eq!(include.cursor, Some(cursor));
        assert_eq!(include.distinct, vec!["title"]);
        assert!(include.include.contains_key("comments"));
    }

    #[test]
    fn find_unique_args_new_starts_without_projection_or_includes() {
        let args = FindUniqueArgs::new(Expr::column("users__id").eq(Expr::param(1i64)));

        assert!(args.select.is_empty());
        assert!(args.include.is_empty());
    }

    #[test]
    fn vector_metric_strings_match_protocol_shape() {
        assert_eq!(VectorMetric::L2.as_str(), "l2");
        assert_eq!(VectorMetric::InnerProduct.as_str(), "innerProduct");
        assert_eq!(VectorMetric::Cosine.as_str(), "cosine");
    }
}
