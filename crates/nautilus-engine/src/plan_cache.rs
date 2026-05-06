//! Cached query plans for the hot read paths.
//!
//! When the typed `findUnique` path receives a request whose argument shape
//! has already been seen (same model, same projection, same flat AND chain of
//! `Column = Param` predicates), we reuse the previously rendered SQL text and
//! the precomputed scalar value hints. Only the parameter values are bound
//! per call, skipping the AST build, the filter qualification clone and the
//! dialect render entirely.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use nautilus_core::{BinaryOp, Expr, Value};

use crate::conversion::ValueHint;

/// Cache key for `findUnique` plans.
///
/// Two requests share a plan when they target the same model, request the same
/// resolved projection (selected logical fields plus implicit primary keys),
/// and produce the same ordered list of qualified filter columns.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct FindUniquePlanKey {
    pub(crate) model_db_name: String,
    pub(crate) selected_logical_fields: Vec<String>,
    pub(crate) filter_columns: Vec<String>,
}

/// SQL plan reusable across calls with the same [`FindUniquePlanKey`].
#[derive(Debug)]
pub(crate) struct CachedFindUniquePlan {
    pub(crate) sql_text: String,
    pub(crate) row_hints: Vec<Option<ValueHint>>,
}

/// Process-wide read-plan cache held by `EngineState`.
#[derive(Debug, Default)]
pub(crate) struct PlanCache {
    find_unique: RwLock<HashMap<FindUniquePlanKey, Arc<CachedFindUniquePlan>>>,
}

impl PlanCache {
    pub(crate) fn get_find_unique(
        &self,
        key: &FindUniquePlanKey,
    ) -> Option<Arc<CachedFindUniquePlan>> {
        self.find_unique.read().ok()?.get(key).cloned()
    }

    pub(crate) fn insert_find_unique(
        &self,
        key: FindUniquePlanKey,
        plan: Arc<CachedFindUniquePlan>,
    ) {
        if let Ok(mut guard) = self.find_unique.write() {
            guard.entry(key).or_insert(plan);
        }
    }

    #[cfg(test)]
    pub(crate) fn find_unique_len(&self) -> usize {
        self.find_unique.read().map(|g| g.len()).unwrap_or(0)
    }
}

/// Borrowed shape extracted from a cacheable `findUnique` filter expression.
pub(crate) struct EqFilterShape<'a> {
    pub(crate) columns: Vec<&'a str>,
    pub(crate) values: Vec<&'a Value>,
}

/// Detect whether `expr` is a flat AND chain of `Column = Param` predicates
/// (or a single equality), returning the columns and parameter values in
/// rendering order. Returns `None` for any other shape so the caller falls
/// back to the general path.
pub(crate) fn extract_simple_eq_filter(expr: &Expr) -> Option<EqFilterShape<'_>> {
    let mut columns = Vec::new();
    let mut values = Vec::new();
    walk_eq_chain(expr, &mut columns, &mut values).then_some(EqFilterShape { columns, values })
}

fn walk_eq_chain<'a>(
    expr: &'a Expr,
    columns: &mut Vec<&'a str>,
    values: &mut Vec<&'a Value>,
) -> bool {
    match expr {
        Expr::Binary {
            left,
            op: BinaryOp::Eq,
            right,
        } => match (left.as_ref(), right.as_ref()) {
            (Expr::Column(col), Expr::Param(val)) => {
                columns.push(col.as_str());
                values.push(val);
                true
            }
            _ => false,
        },
        Expr::Binary {
            left,
            op: BinaryOp::And,
            right,
        } => walk_eq_chain(left, columns, values) && walk_eq_chain(right, columns, values),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(name: &str) -> Expr {
        Expr::Column(name.to_string())
    }

    fn param(value: i64) -> Expr {
        Expr::Param(Value::I64(value))
    }

    #[test]
    fn extracts_single_eq() {
        let expr = col("users__id").eq(param(7));
        let shape = extract_simple_eq_filter(&expr).expect("should extract shape");
        assert_eq!(shape.columns, vec!["users__id"]);
        assert_eq!(shape.values, vec![&Value::I64(7)]);
    }

    #[test]
    fn extracts_and_chain_in_input_order() {
        let expr = col("posts__tenant")
            .eq(param(1))
            .and(col("posts__id").eq(param(99)));
        let shape = extract_simple_eq_filter(&expr).expect("should extract shape");
        assert_eq!(shape.columns, vec!["posts__tenant", "posts__id"]);
        assert_eq!(shape.values, vec![&Value::I64(1), &Value::I64(99)]);
    }

    #[test]
    fn rejects_non_equality_operators() {
        let expr = col("users__id").gt(param(5));
        assert!(extract_simple_eq_filter(&expr).is_none());
    }

    #[test]
    fn rejects_or_chains() {
        let expr = col("users__id")
            .eq(param(1))
            .or(col("users__id").eq(param(2)));
        assert!(extract_simple_eq_filter(&expr).is_none());
    }

    #[test]
    fn rejects_param_on_left() {
        let expr = Expr::Binary {
            left: Box::new(param(1)),
            op: BinaryOp::Eq,
            right: Box::new(col("users__id")),
        };
        assert!(extract_simple_eq_filter(&expr).is_none());
    }

    #[test]
    fn cache_returns_inserted_plan() {
        let cache = PlanCache::default();
        let key = FindUniquePlanKey {
            model_db_name: "User".to_string(),
            selected_logical_fields: vec!["id".to_string(), "name".to_string()],
            filter_columns: vec!["users__id".to_string()],
        };
        let plan = Arc::new(CachedFindUniquePlan {
            sql_text: "SELECT 1".to_string(),
            row_hints: vec![None, None],
        });
        cache.insert_find_unique(key.clone(), Arc::clone(&plan));
        let got = cache.get_find_unique(&key).expect("plan should be cached");
        assert!(Arc::ptr_eq(&plan, &got));
        assert_eq!(cache.find_unique_len(), 1);
    }
}
