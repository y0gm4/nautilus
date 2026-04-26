//! Expression AST for building WHERE clauses and filters.

use crate::select::Select;
use crate::value::Value;

/// Internal expression function marker rendered as pgvector `<->`.
pub const VECTOR_L2_DISTANCE_FUNCTION: &str = "__nautilus_vector_l2_distance";
/// Internal expression function marker rendered as pgvector `<#>`.
pub const VECTOR_INNER_PRODUCT_FUNCTION: &str = "__nautilus_vector_inner_product";
/// Internal expression function marker rendered as pgvector `<=>`.
pub const VECTOR_COSINE_DISTANCE_FUNCTION: &str = "__nautilus_vector_cosine_distance";

/// Relation filter operator used by generated relation helpers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelationFilterOp {
    /// At least one related row matches the child filter.
    Some,
    /// No related row matches the child filter.
    None,
    /// Every related row matches the child filter.
    Every,
}

/// Metadata needed to render or serialize a relation predicate.
#[derive(Debug, Clone, PartialEq)]
pub struct RelationFilter {
    /// Logical relation field name on the parent model.
    pub field: String,
    /// Database table name of the parent model.
    pub parent_table: String,
    /// Database table name of the related child model.
    pub target_table: String,
    /// Child-side foreign-key column name.
    pub fk_db: String,
    /// Parent-side referenced key column name.
    pub pk_db: String,
    /// Child filter to apply inside the relation predicate.
    pub filter: Box<Expr>,
}

/// Binary operators for expressions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BinaryOp {
    /// Equality (`=`).
    Eq,
    /// Not equal (`!=`).
    Ne,
    /// Less than (`<`).
    Lt,
    /// Less than or equal (`<=`).
    Le,
    /// Greater than (`>`).
    Gt,
    /// Greater than or equal (`>=`).
    Ge,
    /// Logical AND.
    And,
    /// Logical OR.
    Or,
    /// LIKE pattern matching.
    Like,
    /// Array contains (`@>` in PostgreSQL).
    ArrayContains,
    /// Array is contained by (`<@` in PostgreSQL).
    ArrayContainedBy,
    /// Array overlaps (`&&` in PostgreSQL).
    ArrayOverlaps,
    /// IN list membership.
    In,
    /// NOT IN list membership.
    NotIn,
}

/// Expression node for WHERE clauses and filters.
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    /// Column reference.
    Column(String),
    /// Parameter placeholder.
    Param(Value),
    /// Binary operation.
    Binary {
        /// Left operand.
        left: Box<Expr>,
        /// Operator.
        op: BinaryOp,
        /// Right operand.
        right: Box<Expr>,
    },
    /// Logical NOT.
    Not(Box<Expr>),
    /// Function call (e.g., json_agg, COALESCE).
    FunctionCall {
        /// Function name.
        name: String,
        /// Function arguments.
        args: Vec<Expr>,
    },
    /// SQL FILTER clause for aggregate functions (PostgreSQL).
    Filter {
        /// The aggregation expression.
        expr: Box<Expr>,
        /// The filter predicate.
        predicate: Box<Expr>,
    },
    /// EXISTS subquery predicate — compiles to `EXISTS (SELECT ...)`.
    Exists(Box<Select>),
    /// NOT EXISTS subquery predicate — compiles to `NOT EXISTS (SELECT ...)`.
    NotExists(Box<Select>),
    /// Relation predicate (`some` / `none` / `every`) with explicit relation metadata.
    Relation {
        /// Which relation operator to apply.
        op: RelationFilterOp,
        /// Relation metadata and nested child filter.
        relation: Box<RelationFilter>,
    },
    /// Scalar subquery — compiles to `(SELECT ...)`.
    ///
    /// The inner SELECT must return exactly one row and one column.  Used for
    /// correlated aggregate sub‑queries (e.g. relation includes) that must not
    /// produce a cartesian product when two or more relations are joined.
    ScalarSubquery(Box<Select>),
    /// IS NULL check — compiles to `expr IS NULL`.
    IsNull(Box<Expr>),
    /// IS NOT NULL check — compiles to `expr IS NOT NULL`.
    IsNotNull(Box<Expr>),
    /// A raw SQL string literal emitted verbatim (no parameter binding).
    ///
    /// Use this sparingly — only for values that must appear as SQL literals
    /// rather than positional parameters (e.g. keys in `json_build_object`).
    /// Never pass untrusted user input through `Literal`.
    Literal(String),
    /// An ordered list of expressions for use in IN / NOT IN clauses.
    ///
    /// Rendered as a comma-separated sequence; the surrounding parentheses are
    /// added by the IN/NOT IN rendering path in each dialect.
    List(Vec<Expr>),
    /// CASE WHEN … THEN … ELSE NULL END.
    CaseWhen {
        /// The condition.
        condition: Box<Expr>,
        /// The THEN result.
        then: Box<Expr>,
    },
    /// SQL wildcard `*` — used inside aggregate functions like `COUNT(*)`.
    Star,
}

impl Expr {
    /// Creates a column reference.
    pub fn column(name: impl Into<String>) -> Self {
        Expr::Column(name.into())
    }

    /// Creates a parameter placeholder.
    pub fn param(value: impl Into<Value>) -> Self {
        Expr::Param(value.into())
    }

    /// Creates an equality comparison (`=`).
    #[must_use]
    pub fn eq(self, other: Expr) -> Self {
        Expr::Binary {
            left: Box::new(self),
            op: BinaryOp::Eq,
            right: Box::new(other),
        }
    }

    /// Creates a not-equal comparison (`!=`).
    #[must_use]
    pub fn ne(self, other: Expr) -> Self {
        Expr::Binary {
            left: Box::new(self),
            op: BinaryOp::Ne,
            right: Box::new(other),
        }
    }

    /// Creates a less-than comparison (`<`).
    #[must_use]
    pub fn lt(self, other: Expr) -> Self {
        Expr::Binary {
            left: Box::new(self),
            op: BinaryOp::Lt,
            right: Box::new(other),
        }
    }

    /// Creates a less-than-or-equal comparison (`<=`).
    #[must_use]
    pub fn le(self, other: Expr) -> Self {
        Expr::Binary {
            left: Box::new(self),
            op: BinaryOp::Le,
            right: Box::new(other),
        }
    }

    /// Creates a greater-than comparison (`>`).
    #[must_use]
    pub fn gt(self, other: Expr) -> Self {
        Expr::Binary {
            left: Box::new(self),
            op: BinaryOp::Gt,
            right: Box::new(other),
        }
    }

    /// Creates a greater-than-or-equal comparison (`>=`).
    #[must_use]
    pub fn ge(self, other: Expr) -> Self {
        Expr::Binary {
            left: Box::new(self),
            op: BinaryOp::Ge,
            right: Box::new(other),
        }
    }

    /// Creates a logical AND.
    #[must_use]
    pub fn and(self, other: Expr) -> Self {
        Expr::Binary {
            left: Box::new(self),
            op: BinaryOp::And,
            right: Box::new(other),
        }
    }

    /// Creates a logical OR.
    #[must_use]
    pub fn or(self, other: Expr) -> Self {
        Expr::Binary {
            left: Box::new(self),
            op: BinaryOp::Or,
            right: Box::new(other),
        }
    }

    /// Creates a LIKE pattern match.
    #[must_use]
    pub fn like(self, pattern: Expr) -> Self {
        Expr::Binary {
            left: Box::new(self),
            op: BinaryOp::Like,
            right: Box::new(pattern),
        }
    }

    /// Creates an IN list membership check.
    #[must_use]
    pub fn in_list(self, exprs: Vec<Expr>) -> Self {
        Expr::Binary {
            left: Box::new(self),
            op: BinaryOp::In,
            right: Box::new(Expr::List(exprs)),
        }
    }

    /// Creates a NOT IN list membership check.
    #[must_use]
    pub fn not_in_list(self, exprs: Vec<Expr>) -> Self {
        Expr::Binary {
            left: Box::new(self),
            op: BinaryOp::NotIn,
            right: Box::new(Expr::List(exprs)),
        }
    }

    /// Creates a function call expression.
    pub fn function_call(name: impl Into<String>, args: Vec<Expr>) -> Self {
        Expr::FunctionCall {
            name: name.into(),
            args,
        }
    }

    /// Creates an internal vector-distance expression for pgvector ordering.
    pub fn vector_distance(metric: crate::args::VectorMetric, left: Expr, right: Expr) -> Self {
        let function = match metric {
            crate::args::VectorMetric::L2 => VECTOR_L2_DISTANCE_FUNCTION,
            crate::args::VectorMetric::InnerProduct => VECTOR_INNER_PRODUCT_FUNCTION,
            crate::args::VectorMetric::Cosine => VECTOR_COSINE_DISTANCE_FUNCTION,
        };
        Expr::function_call(function, vec![left, right])
    }

    /// Creates a json_agg() aggregate function.
    pub fn json_agg(expr: Expr) -> Self {
        Expr::FunctionCall {
            name: "json_agg".to_string(),
            args: vec![expr],
        }
    }

    /// Creates a json_build_object() function with key-value pairs.
    ///
    /// Keys are emitted as SQL string literals (not bound parameters) because
    /// `json_build_object` requires literal key names in all supported dialects.
    ///
    /// # Safety
    ///
    /// Keys must be static/compile-time strings. Never pass untrusted input as
    /// a key — use [`Expr::param`] for that and handle it in application logic.
    pub fn json_build_object(pairs: Vec<(String, Expr)>) -> Self {
        let args: Vec<Expr> = pairs
            .into_iter()
            .flat_map(|(key, value)| vec![Expr::Literal(key), value])
            .collect();

        Expr::FunctionCall {
            name: "json_build_object".to_string(),
            args,
        }
    }

    /// Creates a COALESCE() function to return first non-NULL value.
    pub fn coalesce(exprs: Vec<Expr>) -> Self {
        Expr::FunctionCall {
            name: "COALESCE".to_string(),
            args: exprs,
        }
    }

    /// Creates an IS NOT NULL check — compiles to `expr IS NOT NULL`.
    #[must_use]
    pub fn is_not_null(self) -> Self {
        Expr::IsNotNull(Box::new(self))
    }

    /// Creates an IS NULL check — compiles to `expr IS NULL`.
    #[must_use]
    pub fn is_null(self) -> Self {
        Expr::IsNull(Box::new(self))
    }

    /// Adds a FILTER clause to an aggregate expression (PostgreSQL).
    #[must_use]
    pub fn filter(self, predicate: Expr) -> Self {
        Expr::Filter {
            expr: Box::new(self),
            predicate: Box::new(predicate),
        }
    }

    /// Creates an EXISTS subquery predicate.
    pub fn exists(subquery: Select) -> Self {
        Expr::Exists(Box::new(subquery))
    }

    /// Creates a NOT EXISTS subquery predicate.
    pub fn not_exists(subquery: Select) -> Self {
        Expr::NotExists(Box::new(subquery))
    }

    /// Creates a relation `some` predicate.
    pub fn relation_some(
        field: impl Into<String>,
        parent_table: impl Into<String>,
        target_table: impl Into<String>,
        fk_db: impl Into<String>,
        pk_db: impl Into<String>,
        filter: Expr,
    ) -> Self {
        Expr::Relation {
            op: RelationFilterOp::Some,
            relation: Box::new(RelationFilter {
                field: field.into(),
                parent_table: parent_table.into(),
                target_table: target_table.into(),
                fk_db: fk_db.into(),
                pk_db: pk_db.into(),
                filter: Box::new(filter),
            }),
        }
    }

    /// Creates a relation `none` predicate.
    pub fn relation_none(
        field: impl Into<String>,
        parent_table: impl Into<String>,
        target_table: impl Into<String>,
        fk_db: impl Into<String>,
        pk_db: impl Into<String>,
        filter: Expr,
    ) -> Self {
        Expr::Relation {
            op: RelationFilterOp::None,
            relation: Box::new(RelationFilter {
                field: field.into(),
                parent_table: parent_table.into(),
                target_table: target_table.into(),
                fk_db: fk_db.into(),
                pk_db: pk_db.into(),
                filter: Box::new(filter),
            }),
        }
    }

    /// Creates a relation `every` predicate.
    pub fn relation_every(
        field: impl Into<String>,
        parent_table: impl Into<String>,
        target_table: impl Into<String>,
        fk_db: impl Into<String>,
        pk_db: impl Into<String>,
        filter: Expr,
    ) -> Self {
        Expr::Relation {
            op: RelationFilterOp::Every,
            relation: Box::new(RelationFilter {
                field: field.into(),
                parent_table: parent_table.into(),
                target_table: target_table.into(),
                fk_db: fk_db.into(),
                pk_db: pk_db.into(),
                filter: Box::new(filter),
            }),
        }
    }

    /// Creates a scalar subquery expression `(SELECT ...)`.
    ///
    /// The inner SELECT must return exactly one column and at most one row.
    pub fn scalar_subquery(subquery: Select) -> Self {
        Expr::ScalarSubquery(Box::new(subquery))
    }

    /// Creates a `CASE WHEN condition THEN result ELSE NULL END` expression.
    pub fn case_when(condition: Expr, then: Expr) -> Self {
        Expr::CaseWhen {
            condition: Box::new(condition),
            then: Box::new(then),
        }
    }

    /// Creates the SQL wildcard `*` (for use in `COUNT(*)`).
    pub fn star() -> Self {
        Expr::Star
    }
}

/// Implements the `!` operator for expressions, producing a SQL `NOT` clause.
impl std::ops::Not for Expr {
    type Output = Self;

    fn not(self) -> Self::Output {
        Expr::Not(Box::new(self))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_column_expr() {
        let expr = Expr::column("email");
        match expr {
            Expr::Column(name) => assert_eq!(name, "email"),
            _ => panic!("Expected Column variant"),
        }
    }

    #[test]
    fn test_param_expr() {
        let expr = Expr::param(42i64);
        match expr {
            Expr::Param(Value::I64(42)) => {}
            _ => panic!("Expected Param with I64(42)"),
        }
    }

    #[test]
    fn test_binary_ops() {
        let col = Expr::column("age");
        let val = Expr::param(18i64);

        let expr = col.ge(val);
        match expr {
            Expr::Binary { op, .. } => assert_eq!(op, BinaryOp::Ge),
            _ => panic!("Expected Binary expression"),
        }
    }

    #[test]
    fn test_complex_expr() {
        let expr = Expr::column("age")
            .ge(Expr::param(18i64))
            .and(Expr::column("email").like(Expr::param("%@gmail.com")));

        match expr {
            Expr::Binary { op, .. } => assert_eq!(op, BinaryOp::And),
            _ => panic!("Expected Binary AND expression"),
        }
    }

    #[test]
    fn test_not_expr() {
        let expr = !Expr::column("active").eq(Expr::param(true));
        match expr {
            Expr::Not(_) => {}
            _ => panic!("Expected Not expression"),
        }
    }

    #[test]
    fn test_in_list() {
        let expr = Expr::column("status").in_list(vec![
            Expr::param(Value::String("active".to_string())),
            Expr::param(Value::String("pending".to_string())),
        ]);
        match expr {
            Expr::Binary { op, .. } => assert_eq!(op, BinaryOp::In),
            _ => panic!("Expected Binary IN expression"),
        }
    }

    #[test]
    fn test_not_in_list() {
        let expr = Expr::column("role").not_in_list(vec![
            Expr::param(Value::String("admin".to_string())),
            Expr::param(Value::String("superuser".to_string())),
        ]);
        match expr {
            Expr::Binary { op, .. } => assert_eq!(op, BinaryOp::NotIn),
            _ => panic!("Expected Binary NOT IN expression"),
        }
    }

    #[test]
    fn test_relation_predicate() {
        let expr = Expr::relation_some(
            "posts",
            "users",
            "posts",
            "author_id",
            "id",
            Expr::column("posts__published").eq(Expr::param(true)),
        );
        match expr {
            Expr::Relation { op, relation } => {
                assert_eq!(op, RelationFilterOp::Some);
                assert_eq!(relation.field, "posts");
                assert_eq!(relation.parent_table, "users");
                assert_eq!(relation.target_table, "posts");
                assert_eq!(relation.fk_db, "author_id");
                assert_eq!(relation.pk_db, "id");
            }
            _ => panic!("Expected relation predicate"),
        }
    }
}
