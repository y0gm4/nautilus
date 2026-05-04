//! Conservative capacity estimates for SQL rendering buffers.
//!
//! This module walks the query AST before rendering so each dialect can
//! preallocate the `String` used for SQL text and the `Vec<Value>` used for
//! bound parameters.
//!
//! The estimates are intentionally conservative: they aim to reduce avoidable
//! reallocations in hot paths, not to predict the exact final size. Rendering
//! remains correct even when the estimate is imperfect, because the underlying
//! buffers can still grow normally if needed.

use nautilus_core::{Delete, Insert, Select, Update, Value};

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct RenderEstimate {
    pub(crate) sql_capacity: usize,
    pub(crate) params_capacity: usize,
}

impl RenderEstimate {
    pub(crate) const fn new(sql_capacity: usize, params_capacity: usize) -> Self {
        Self {
            sql_capacity,
            params_capacity,
        }
    }

    pub(crate) fn add_sql(&mut self, sql_capacity: usize) {
        self.sql_capacity += sql_capacity;
    }

    pub(crate) fn add_param(&mut self, should_bind: bool) {
        self.params_capacity += usize::from(should_bind);
    }

    pub(crate) fn merge(&mut self, other: Self) {
        self.sql_capacity += other.sql_capacity;
        self.params_capacity += other.params_capacity;
    }
}

/// Estimate the render-time capacity for a SELECT query.
pub(crate) fn estimate_select_render(select: &Select) -> RenderEstimate {
    let mut estimate = RenderEstimate::new(32 + estimate_identifier_len(&select.table), 0);

    if select.items.is_empty() && select.joins.iter().all(|join| join.items.is_empty()) {
        estimate.add_sql(1);
    } else {
        for item in &select.items {
            estimate.merge(estimate_select_item(item));
            estimate.add_sql(2);
        }
        for join in &select.joins {
            for item in &join.items {
                estimate.merge(estimate_select_item(item));
                estimate.add_sql(2);
            }
        }
    }

    if !select.distinct.is_empty() {
        estimate.add_sql(16);
        for column in &select.distinct {
            estimate.add_sql(estimate_identifier_reference_len(column) + 2);
        }
    }

    for join in &select.joins {
        estimate.add_sql(24 + estimate_identifier_len(&join.table));
        estimate.merge(estimate_expr_render(&join.on));
    }

    if let Some(filter) = select.filter.as_ref() {
        estimate.add_sql(7);
        estimate.merge(estimate_expr_render(filter));
    }

    if !select.group_by.is_empty() {
        estimate.add_sql(10);
        for column in &select.group_by {
            estimate.add_sql(estimate_qualified_identifier_len(&column.table, &column.name) + 2);
        }
    }

    if let Some(having) = select.having.as_ref() {
        estimate.add_sql(8);
        estimate.merge(estimate_expr_render(having));
    }

    if !select.order_by_items.is_empty() {
        estimate.add_sql(10);
        for item in &select.order_by_items {
            estimate.merge(estimate_order_by_item(item));
            estimate.add_sql(5);
        }
    } else {
        if !select.order_by.is_empty() || !select.order_by_exprs.is_empty() {
            estimate.add_sql(10);
        }
        for order in &select.order_by {
            estimate.add_sql(estimate_identifier_reference_len(&order.column) + 5);
        }
        for (expr, _) in &select.order_by_exprs {
            estimate.merge(estimate_expr_render(expr));
            estimate.add_sql(5);
        }
    }

    if let Some(take) = select.take {
        estimate.add_sql(7 + decimal_len_u64(u64::from(take.unsigned_abs())));
    } else if select.skip.is_some() {
        estimate.add_sql(27);
    }

    if let Some(skip) = select.skip {
        estimate.add_sql(8 + decimal_len_u64(u64::from(skip)));
    }

    estimate
}

/// Estimate the render-time capacity for an INSERT query.
pub(crate) fn estimate_insert_render(insert: &Insert) -> RenderEstimate {
    let mut estimate = RenderEstimate::new(32 + estimate_identifier_len(&insert.table), 0);

    for column in &insert.columns {
        estimate.add_sql(estimate_identifier_len(&column.name) + 2);
    }

    for row in &insert.values {
        estimate.add_sql(4);
        for value in row {
            if matches!(value, Value::Null) {
                estimate.add_sql(4);
            } else {
                estimate.add_sql(8);
                estimate.add_param(true);
                if let Value::Enum { type_name, .. } = value {
                    estimate.add_sql(2 + type_name.len());
                }
            }
        }
    }

    if !insert.returning.is_empty() {
        estimate.add_sql(12);
        for column in &insert.returning {
            estimate.add_sql(
                estimate_qualified_identifier_len(&column.table, &column.name)
                    + estimate_column_alias_len(column)
                    + 6,
            );
        }
    }

    estimate
}

/// Estimate the render-time capacity for an UPDATE query.
pub(crate) fn estimate_update_render(update: &Update) -> RenderEstimate {
    let mut estimate = RenderEstimate::new(24 + estimate_identifier_len(&update.table), 0);

    for (column, value) in &update.assignments {
        estimate.add_sql(estimate_identifier_len(&column.name) + 6);
        if matches!(value, Value::Null) {
            estimate.add_sql(4);
        } else {
            estimate.add_sql(8);
            estimate.add_param(true);
            if let Value::Enum { type_name, .. } = value {
                estimate.add_sql(2 + type_name.len());
            }
        }
    }

    if let Some(filter) = update.filter.as_ref() {
        estimate.add_sql(7);
        estimate.merge(estimate_expr_render(filter));
    }

    if !update.returning.is_empty() {
        estimate.add_sql(12);
        for column in &update.returning {
            estimate.add_sql(
                estimate_qualified_identifier_len(&column.table, &column.name)
                    + estimate_column_alias_len(column)
                    + 6,
            );
        }
    }

    estimate
}

/// Estimate the render-time capacity for a DELETE query.
pub(crate) fn estimate_delete_render(delete: &Delete) -> RenderEstimate {
    let mut estimate = RenderEstimate::new(24 + estimate_identifier_len(&delete.table), 0);

    if let Some(filter) = delete.filter.as_ref() {
        estimate.add_sql(7);
        estimate.merge(estimate_expr_render(filter));
    }

    if !delete.returning.is_empty() {
        estimate.add_sql(12);
        for column in &delete.returning {
            estimate.add_sql(
                estimate_qualified_identifier_len(&column.table, &column.name)
                    + estimate_column_alias_len(column)
                    + 6,
            );
        }
    }

    estimate
}

fn estimate_select_item(item: &nautilus_core::SelectItem) -> RenderEstimate {
    match item {
        nautilus_core::SelectItem::Column(column) => RenderEstimate::new(
            estimate_qualified_identifier_len(&column.table, &column.name)
                + estimate_column_alias_len(column)
                + 6,
            0,
        ),
        nautilus_core::SelectItem::Computed { expr, alias } => {
            let mut estimate = RenderEstimate::new(estimate_identifier_len(alias) + 6, 0);
            estimate.merge(estimate_expr_render(expr));
            estimate
        }
    }
}

fn estimate_order_by_item(item: &nautilus_core::OrderByItem) -> RenderEstimate {
    match item {
        nautilus_core::OrderByItem::Column(order) => {
            RenderEstimate::new(estimate_identifier_reference_len(&order.column) + 5, 0)
        }
        nautilus_core::OrderByItem::Expr(expr, _) => {
            let mut estimate = estimate_expr_render(expr);
            estimate.add_sql(5);
            estimate
        }
    }
}

fn estimate_expr_render(expr: &nautilus_core::Expr) -> RenderEstimate {
    use nautilus_core::Expr;

    match expr {
        Expr::Column(name) => RenderEstimate::new(estimate_identifier_reference_len(name), 0),
        Expr::Param(value) => {
            if matches!(value, Value::Null) {
                RenderEstimate::new(4, 0)
            } else {
                RenderEstimate::new(8, 1)
            }
        }
        Expr::Binary { left, right, .. } => {
            let mut estimate = RenderEstimate::new(8, 0);
            estimate.merge(estimate_expr_render(left));
            estimate.merge(estimate_expr_render(right));
            estimate
        }
        Expr::Not(inner) => {
            let mut estimate = RenderEstimate::new(6, 0);
            estimate.merge(estimate_expr_render(inner));
            estimate
        }
        Expr::FunctionCall { name, args } => {
            let mut estimate = RenderEstimate::new(name.len() + 2, 0);
            for arg in args {
                estimate.merge(estimate_expr_render(arg));
                estimate.add_sql(2);
            }
            estimate
        }
        Expr::Filter { expr, predicate } => {
            let mut estimate = RenderEstimate::new(18, 0);
            estimate.merge(estimate_expr_render(expr));
            estimate.merge(estimate_expr_render(predicate));
            estimate
        }
        Expr::Exists(select) | Expr::NotExists(select) | Expr::ScalarSubquery(select) => {
            let mut estimate = RenderEstimate::new(12, 0);
            estimate.merge(estimate_select_render(select));
            estimate
        }
        Expr::Relation { relation, .. } => {
            let mut estimate =
                RenderEstimate::new(40 + estimate_identifier_len(&relation.target_table), 0);
            estimate.add_sql(estimate_qualified_identifier_len(
                &relation.target_table,
                &relation.fk_db,
            ));
            estimate.add_sql(estimate_qualified_identifier_len(
                &relation.parent_table,
                &relation.pk_db,
            ));
            estimate.merge(estimate_expr_render(&relation.filter));
            estimate
        }
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
            let mut estimate = RenderEstimate::new(12, 0);
            estimate.merge(estimate_expr_render(inner));
            estimate
        }
        Expr::Literal(value) => RenderEstimate::new(value.len() * 2 + 2, 0),
        Expr::List(values) => {
            let mut estimate = RenderEstimate::default();
            for value in values {
                estimate.merge(estimate_expr_render(value));
                estimate.add_sql(2);
            }
            estimate
        }
        Expr::CaseWhen { condition, then } => {
            let mut estimate = RenderEstimate::new(28, 0);
            estimate.merge(estimate_expr_render(condition));
            estimate.merge(estimate_expr_render(then));
            estimate
        }
        Expr::Star => RenderEstimate::new(1, 0),
    }
}

fn estimate_identifier_reference_len(name: &str) -> usize {
    if let Some((table, column)) = name.split_once("__") {
        estimate_qualified_identifier_len(table, column)
    } else {
        estimate_identifier_len(name)
    }
}

fn estimate_qualified_identifier_len(table: &str, column: &str) -> usize {
    estimate_identifier_len(table) + 1 + estimate_identifier_len(column)
}

fn estimate_column_alias_len(column: &nautilus_core::ColumnMarker) -> usize {
    estimate_identifier_len(&column.table) + estimate_identifier_len(&column.name) + 2
}

fn estimate_identifier_len(name: &str) -> usize {
    name.len() + 4
}

fn decimal_len_u64(mut value: u64) -> usize {
    let mut digits = 1;
    while value >= 10 {
        value /= 10;
        digits += 1;
    }
    digits
}
