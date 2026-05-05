//! SQL dialect renderers for Nautilus ORM.

#![warn(missing_docs)]
#![forbid(unsafe_code)]

// These macros accept identifier parameters (`$quote`, `$render_expr`) so that
// each dialect module supplies only the logic that differs between dialects.
// Free identifiers in macro bodies (types, constants) are resolved at the
// *definition site* (here in lib.rs), so the required types must be imported
// below.  Identifier parameters (`$quote:expr`, `$render_expr:ident`) are
// substituted textually at the call site, which is the intended behaviour.

/// Append `RETURNING col1 AS alias1, ...` when `$returning` is non-empty.
macro_rules! render_returning {
    ($ctx:expr, $returning:expr, $quote:expr) => {{
        if !$returning.is_empty() {
            $ctx.sql.push_str(" RETURNING ");
            for (i, col) in $returning.iter().enumerate() {
                if i > 0 {
                    $ctx.sql.push_str(", ");
                }
                crate::push_qualified_identifier(&mut $ctx.sql, &col.table, &col.name, $quote);
                $ctx.sql.push_str(" AS ");
                crate::push_column_alias(&mut $ctx.sql, col, $quote);
            }
        }
    }};
}

/// Render the full body of an INSERT statement into `$ctx`.
///
/// `$supports_returning`: when `false` the RETURNING clause is omitted (MySQL).
macro_rules! render_insert_body {
    ($ctx:expr, $insert:expr, $quote:expr, $supports_returning:expr, $supports_enum_cast:expr) => {{
        $ctx.sql.push_str("INSERT INTO ");
        crate::push_quoted_identifier(&mut $ctx.sql, &$insert.table, $quote);

        $ctx.sql.push_str(" (");
        for (i, col) in $insert.columns.iter().enumerate() {
            if i > 0 {
                $ctx.sql.push_str(", ");
            }
            crate::push_quoted_identifier(&mut $ctx.sql, &col.name, $quote);
        }
        $ctx.sql.push(')');

        $ctx.sql.push_str(" VALUES ");
        for (row_idx, row) in $insert.values.iter().enumerate() {
            if row_idx > 0 {
                $ctx.sql.push_str(", ");
            }
            $ctx.sql.push('(');
            for (val_idx, value) in row.iter().enumerate() {
                if val_idx > 0 {
                    $ctx.sql.push_str(", ");
                }
                if matches!(value, nautilus_core::Value::Null) {
                    $ctx.sql.push_str("NULL");
                } else {
                    $ctx.push_param(value.clone());
                    if $supports_enum_cast {
                        if let nautilus_core::Value::Enum { type_name, .. } = value {
                            $ctx.sql.push_str("::");
                            $ctx.sql.push_str(type_name);
                        }
                    }
                }
            }
            $ctx.sql.push(')');
        }

        if $supports_returning {
            render_returning!($ctx, $insert.returning, $quote);
        }
    }};
}

/// Render the full body of an UPDATE statement into `$ctx`.
///
/// `$render_expr`: the dialect-local expression renderer.
/// `$supports_returning`: when `false` the RETURNING clause is omitted (MySQL).
macro_rules! render_update_body {
    ($ctx:expr, $update:expr, $quote:expr, $render_expr:ident, $supports_returning:expr, $supports_enum_cast:expr) => {{
        $ctx.sql.push_str("UPDATE ");
        crate::push_quoted_identifier(&mut $ctx.sql, &$update.table, $quote);

        $ctx.sql.push_str(" SET ");
        for (i, (col, value)) in $update.assignments.iter().enumerate() {
            if i > 0 {
                $ctx.sql.push_str(", ");
            }
            crate::push_quoted_identifier(&mut $ctx.sql, &col.name, $quote);
            $ctx.sql.push_str(" = ");
            if matches!(value, nautilus_core::Value::Null) {
                $ctx.sql.push_str("NULL");
            } else {
                $ctx.push_param(value.clone());
                if $supports_enum_cast {
                    if let nautilus_core::Value::Enum { type_name, .. } = value {
                        $ctx.sql.push_str("::");
                        $ctx.sql.push_str(type_name);
                    }
                }
            }
        }

        if let Some(ref filter) = $update.filter {
            $ctx.sql.push_str(" WHERE ");
            $render_expr($ctx, filter);
        }

        if $supports_returning {
            render_returning!($ctx, $update.returning, $quote);
        }
    }};
}

/// Render the full body of a DELETE statement into `$ctx`.
///
/// `$render_expr`: the dialect-local expression renderer.
/// `$supports_returning`: when `false` the RETURNING clause is omitted (MySQL).
macro_rules! render_delete_body {
    ($ctx:expr, $delete:expr, $quote:expr, $render_expr:ident, $supports_returning:expr) => {{
        $ctx.sql.push_str("DELETE FROM ");
        crate::push_quoted_identifier(&mut $ctx.sql, &$delete.table, $quote);

        if let Some(ref filter) = $delete.filter {
            $ctx.sql.push_str(" WHERE ");
            $render_expr($ctx, filter);
        }

        if $supports_returning {
            render_returning!($ctx, $delete.returning, $quote);
        }
    }};
}

/// Render the full body of a SELECT statement into `$ctx`.
///
/// - `$distinct_on`: `true` for PostgreSQL-style `DISTINCT ON (cols)`;
///   `false` emits plain `SELECT DISTINCT`.
/// - `$mysql_limit_hack`: `true` inserts a synthetic `LIMIT 18446744073709551615`
///   when only OFFSET is present (required by MySQL).
/// - `$render_expr`: the dialect-local expression renderer.
macro_rules! render_select_body_core {
    (
        $ctx:expr, $select:expr,
        $quote:expr, $render_expr:ident,
        $distinct_on:expr, $mysql_limit_hack:expr
    ) => {{
        $ctx.sql.push_str("SELECT ");

        // DISTINCT handling: Postgres supports DISTINCT ON (cols);
        // other dialects support only full-row SELECT DISTINCT.
        if !$select.distinct.is_empty() {
            if $distinct_on {
                $ctx.sql.push_str("DISTINCT ON (");
                for (i, col) in $select.distinct.iter().enumerate() {
                    if i > 0 {
                        $ctx.sql.push_str(", ");
                    }
                    crate::push_identifier_reference(&mut $ctx.sql, col, $quote);
                }
                $ctx.sql.push_str(") ");
            } else {
                $ctx.sql.push_str("DISTINCT ");
            }
        }

        let has_items =
            !$select.items.is_empty() || $select.joins.iter().any(|join| !join.items.is_empty());

        if !has_items {
            $ctx.sql.push('*');
        } else {
            let mut first = true;
            for item in &$select.items {
                if !first {
                    $ctx.sql.push_str(", ");
                }
                first = false;
                match item {
                    nautilus_core::SelectItem::Column(col) => {
                        crate::push_qualified_identifier(
                            &mut $ctx.sql,
                            &col.table,
                            &col.name,
                            $quote,
                        );
                        $ctx.sql.push_str(" AS ");
                        crate::push_column_alias(&mut $ctx.sql, col, $quote);
                    }
                    nautilus_core::SelectItem::Computed { expr, alias } => {
                        $ctx.sql.push('(');
                        $render_expr($ctx, expr);
                        $ctx.sql.push(')');
                        $ctx.sql.push_str(" AS ");
                        crate::push_quoted_identifier(&mut $ctx.sql, alias, $quote);
                    }
                }
            }
            for join in &$select.joins {
                for item in &join.items {
                    if !first {
                        $ctx.sql.push_str(", ");
                    }
                    first = false;
                    match item {
                        nautilus_core::SelectItem::Column(col) => {
                            crate::push_qualified_identifier(
                                &mut $ctx.sql,
                                &col.table,
                                &col.name,
                                $quote,
                            );
                            $ctx.sql.push_str(" AS ");
                            crate::push_column_alias(&mut $ctx.sql, col, $quote);
                        }
                        nautilus_core::SelectItem::Computed { expr, alias } => {
                            $ctx.sql.push('(');
                            $render_expr($ctx, expr);
                            $ctx.sql.push(')');
                            $ctx.sql.push_str(" AS ");
                            crate::push_quoted_identifier(&mut $ctx.sql, alias, $quote);
                        }
                    }
                }
            }
        }

        $ctx.sql.push_str(" FROM ");
        crate::push_quoted_identifier(&mut $ctx.sql, &$select.table, $quote);

        for join in &$select.joins {
            match join.join_type {
                nautilus_core::JoinType::Inner => $ctx.sql.push_str(" INNER JOIN "),
                nautilus_core::JoinType::Left => $ctx.sql.push_str(" LEFT JOIN "),
            }
            crate::push_quoted_identifier(&mut $ctx.sql, &join.table, $quote);
            $ctx.sql.push_str(" ON ");
            $render_expr($ctx, &join.on);
        }

        if let Some(ref filter) = $select.filter {
            $ctx.sql.push_str(" WHERE ");
            $render_expr($ctx, filter);
        }

        if !$select.group_by.is_empty() {
            $ctx.sql.push_str(" GROUP BY ");
            for (i, col) in $select.group_by.iter().enumerate() {
                if i > 0 {
                    $ctx.sql.push_str(", ");
                }
                crate::push_qualified_identifier(&mut $ctx.sql, &col.table, &col.name, $quote);
            }
        }

        if let Some(ref having) = $select.having {
            $ctx.sql.push_str(" HAVING ");
            $render_expr($ctx, having);
        }

        let has_order_items = !$select.order_by_items.is_empty();
        let has_col_order = !$select.order_by.is_empty();
        let has_expr_order = !$select.order_by_exprs.is_empty();
        if has_order_items || has_col_order || has_expr_order {
            $ctx.sql.push_str(" ORDER BY ");
            let mut first = true;
            if has_order_items {
                for item in &$select.order_by_items {
                    if !first {
                        $ctx.sql.push_str(", ");
                    }
                    first = false;
                    match item {
                        nautilus_core::OrderByItem::Column(order) => {
                            crate::push_identifier_reference(&mut $ctx.sql, &order.column, $quote);
                            match order.direction {
                                nautilus_core::OrderDir::Asc => $ctx.sql.push_str(" ASC"),
                                nautilus_core::OrderDir::Desc => $ctx.sql.push_str(" DESC"),
                            }
                        }
                        nautilus_core::OrderByItem::Expr(expr, dir) => {
                            $render_expr($ctx, expr);
                            match dir {
                                nautilus_core::OrderDir::Asc => $ctx.sql.push_str(" ASC"),
                                nautilus_core::OrderDir::Desc => $ctx.sql.push_str(" DESC"),
                            }
                        }
                    }
                }
            } else {
                for order in &$select.order_by {
                    if !first {
                        $ctx.sql.push_str(", ");
                    }
                    first = false;
                    crate::push_identifier_reference(&mut $ctx.sql, &order.column, $quote);
                    match order.direction {
                        nautilus_core::OrderDir::Asc => $ctx.sql.push_str(" ASC"),
                        nautilus_core::OrderDir::Desc => $ctx.sql.push_str(" DESC"),
                    }
                }
                for (expr, dir) in &$select.order_by_exprs {
                    if !first {
                        $ctx.sql.push_str(", ");
                    }
                    first = false;
                    $render_expr($ctx, expr);
                    match dir {
                        nautilus_core::OrderDir::Asc => $ctx.sql.push_str(" ASC"),
                        nautilus_core::OrderDir::Desc => $ctx.sql.push_str(" DESC"),
                    }
                }
            }
        }

        // MySQL requires LIMIT whenever OFFSET is present; emit a synthetic max value.
        if let Some(take) = $select.take {
            $ctx.sql.push_str(" LIMIT ");
            crate::push_u32(&mut $ctx.sql, take.unsigned_abs());
        } else if $mysql_limit_hack && $select.skip.is_some() {
            $ctx.sql.push_str(" LIMIT 18446744073709551615");
        }

        if let Some(skip) = $select.skip {
            $ctx.sql.push_str(" OFFSET ");
            crate::push_u32(&mut $ctx.sql, skip);
        }
    }};
}

/// Render the `Expr` variants that are **identical** across all SQL dialect renderers.
///
/// Eight variants (`Column`, `Not`, `Exists`, `NotExists`, `ScalarSubquery`,
/// `IsNull`, `IsNotNull`, `Literal`) have the same rendering logic in every
/// dialect — the only structural difference is which function is called to
/// quote identifiers and which function recurses for sub-expressions.
///
/// The four dialect-specific variants (`Param`, `Binary`, `FunctionCall`,
/// `Filter`) are provided by the caller as a block of match arms in
/// `{ $($specific:tt)* }` and are appended after the shared arms.
///
/// Parameters:
/// - `$ctx`: `&mut RenderContext` — mutable render context
/// - `$expr`: `&Expr` — the expression to render
/// - `$quote`: local identifier-quoting function
/// - `$render_expr`: dialect-local recursive expression renderer
/// - `$render_select_body`: dialect-local subquery renderer
/// - `{ $($specific:tt)* }`: match arms for dialect-specific variants
macro_rules! render_expr_common {
    (
        $ctx:expr, $expr:expr,
        $quote:expr, $render_expr:ident, $render_select_body:ident,
        { $($specific:tt)* }
    ) => {
        match $expr {
            // Split "table__column" into a qualified identifier pair; otherwise
            // render as a single unqualified identifier.
            nautilus_core::Expr::Column(name) => {
                crate::push_identifier_reference(&mut $ctx.sql, name, $quote);
            }
            nautilus_core::Expr::Not(inner) => {
                $ctx.sql.push_str("NOT (");
                $render_expr($ctx, inner);
                $ctx.sql.push(')');
            }
            nautilus_core::Expr::Exists(subquery) => {
                $ctx.sql.push_str("EXISTS (");
                $render_select_body($ctx, subquery);
                $ctx.sql.push(')');
            }
            nautilus_core::Expr::NotExists(subquery) => {
                $ctx.sql.push_str("NOT EXISTS (");
                $render_select_body($ctx, subquery);
                $ctx.sql.push(')');
            }
            nautilus_core::Expr::Relation { op, relation } => {
                let is_exists = matches!(op, nautilus_core::expr::RelationFilterOp::Some);
                if is_exists {
                    $ctx.sql.push_str("EXISTS (SELECT * FROM ");
                } else {
                    $ctx.sql.push_str("NOT EXISTS (SELECT * FROM ");
                }
                crate::push_quoted_identifier(&mut $ctx.sql, &relation.target_table, $quote);
                $ctx.sql.push_str(" WHERE ");
                crate::push_qualified_identifier(
                    &mut $ctx.sql,
                    &relation.target_table,
                    &relation.fk_db,
                    $quote,
                );
                $ctx.sql.push_str(" = ");
                crate::push_qualified_identifier(
                    &mut $ctx.sql,
                    &relation.parent_table,
                    &relation.pk_db,
                    $quote,
                );
                $ctx.sql.push_str(" AND ");
                if matches!(op, nautilus_core::expr::RelationFilterOp::Every) {
                    $ctx.sql.push_str("NOT (");
                    $render_expr($ctx, &relation.filter);
                    $ctx.sql.push(')');
                } else {
                    $render_expr($ctx, &relation.filter);
                }
                $ctx.sql.push(')');
            }
            nautilus_core::Expr::ScalarSubquery(subquery) => {
                $ctx.sql.push('(');
                $render_select_body($ctx, subquery);
                $ctx.sql.push(')');
            }
            nautilus_core::Expr::IsNull(inner) => {
                $ctx.sql.push('(');
                $render_expr($ctx, inner);
                $ctx.sql.push_str(" IS NULL)");
            }
            nautilus_core::Expr::IsNotNull(inner) => {
                $ctx.sql.push('(');
                $render_expr($ctx, inner);
                $ctx.sql.push_str(" IS NOT NULL)");
            }
            // Emit as a single-quoted SQL string literal with internal
            // single-quotes escaped by doubling.
            // Must only be called with trusted, static strings.
            nautilus_core::Expr::Literal(s) => {
                crate::push_sql_string_literal(&mut $ctx.sql, s);
            }
            nautilus_core::Expr::List(exprs) => {
                for (i, e) in exprs.iter().enumerate() {
                    if i > 0 { $ctx.sql.push_str(", "); }
                    $render_expr($ctx, e);
                }
            }
            nautilus_core::Expr::CaseWhen { condition, then } => {
                $ctx.sql.push_str("CASE WHEN ");
                $render_expr($ctx, condition);
                $ctx.sql.push_str(" THEN ");
                $render_expr($ctx, then);
                $ctx.sql.push_str(" ELSE NULL END");
            }
            nautilus_core::Expr::Star => {
                $ctx.sql.push('*');
            }
            $($specific)*
        }
    };
}

/// Mutable/owned variant of [`render_returning!`] used by `render_*_owned`.
macro_rules! render_returning_mut {
    ($ctx:expr, $returning:expr, $quote:expr) => {{
        if !$returning.is_empty() {
            $ctx.sql.push_str(" RETURNING ");
            for (i, col) in $returning.iter().enumerate() {
                if i > 0 {
                    $ctx.sql.push_str(", ");
                }
                crate::push_qualified_identifier(&mut $ctx.sql, &col.table, &col.name, $quote);
                $ctx.sql.push_str(" AS ");
                crate::push_column_alias(&mut $ctx.sql, col, $quote);
            }
        }
    }};
}

/// Mutable/owned variant of [`render_insert_body!`] used by `render_*_owned`.
macro_rules! render_insert_body_mut {
    ($ctx:expr, $insert:expr, $quote:expr, $supports_returning:expr, $supports_enum_cast:expr) => {{
        $ctx.sql.push_str("INSERT INTO ");
        crate::push_quoted_identifier(&mut $ctx.sql, &$insert.table, $quote);

        $ctx.sql.push_str(" (");
        for (i, col) in $insert.columns.iter().enumerate() {
            if i > 0 {
                $ctx.sql.push_str(", ");
            }
            crate::push_quoted_identifier(&mut $ctx.sql, &col.name, $quote);
        }
        $ctx.sql.push(')');

        $ctx.sql.push_str(" VALUES ");
        for (row_idx, row) in $insert.values.iter_mut().enumerate() {
            if row_idx > 0 {
                $ctx.sql.push_str(", ");
            }
            $ctx.sql.push('(');
            for (val_idx, value) in row.iter_mut().enumerate() {
                if val_idx > 0 {
                    $ctx.sql.push_str(", ");
                }
                if matches!(value, nautilus_core::Value::Null) {
                    $ctx.sql.push_str("NULL");
                } else {
                    let enum_type_name = if $supports_enum_cast {
                        if let nautilus_core::Value::Enum { type_name, .. } = value {
                            Some(type_name.clone())
                        } else {
                            None
                        }
                    } else {
                        None
                    };
                    $ctx.take_param(value);
                    if let Some(type_name) = enum_type_name.as_deref() {
                        $ctx.sql.push_str("::");
                        $ctx.sql.push_str(type_name);
                    }
                }
            }
            $ctx.sql.push(')');
        }

        if $supports_returning {
            render_returning_mut!($ctx, $insert.returning, $quote);
        }
    }};
}

/// Mutable/owned variant of [`render_update_body!`] used by `render_*_owned`.
macro_rules! render_update_body_mut {
    ($ctx:expr, $update:expr, $quote:expr, $render_expr:ident, $supports_returning:expr, $supports_enum_cast:expr) => {{
        $ctx.sql.push_str("UPDATE ");
        crate::push_quoted_identifier(&mut $ctx.sql, &$update.table, $quote);

        $ctx.sql.push_str(" SET ");
        for (i, (col, value)) in $update.assignments.iter_mut().enumerate() {
            if i > 0 {
                $ctx.sql.push_str(", ");
            }
            crate::push_quoted_identifier(&mut $ctx.sql, &col.name, $quote);
            $ctx.sql.push_str(" = ");
            if matches!(value, nautilus_core::Value::Null) {
                $ctx.sql.push_str("NULL");
            } else {
                let enum_type_name = if $supports_enum_cast {
                    if let nautilus_core::Value::Enum { type_name, .. } = value {
                        Some(type_name.clone())
                    } else {
                        None
                    }
                } else {
                    None
                };
                $ctx.take_param(value);
                if let Some(type_name) = enum_type_name.as_deref() {
                    $ctx.sql.push_str("::");
                    $ctx.sql.push_str(type_name);
                }
            }
        }

        if let Some(filter) = $update.filter.as_mut() {
            $ctx.sql.push_str(" WHERE ");
            $render_expr($ctx, filter);
        }

        if $supports_returning {
            render_returning_mut!($ctx, $update.returning, $quote);
        }
    }};
}

/// Mutable/owned variant of [`render_delete_body!`] used by `render_*_owned`.
macro_rules! render_delete_body_mut {
    ($ctx:expr, $delete:expr, $quote:expr, $render_expr:ident, $supports_returning:expr) => {{
        $ctx.sql.push_str("DELETE FROM ");
        crate::push_quoted_identifier(&mut $ctx.sql, &$delete.table, $quote);

        if let Some(filter) = $delete.filter.as_mut() {
            $ctx.sql.push_str(" WHERE ");
            $render_expr($ctx, filter);
        }

        if $supports_returning {
            render_returning_mut!($ctx, $delete.returning, $quote);
        }
    }};
}

/// Mutable/owned variant of [`render_select_body_core!`] used by `render_*_owned`.
macro_rules! render_select_body_core_mut {
    (
        $ctx:expr, $select:expr,
        $quote:expr, $render_expr:ident,
        $distinct_on:expr, $mysql_limit_hack:expr
    ) => {{
        $ctx.sql.push_str("SELECT ");

        if !$select.distinct.is_empty() {
            if $distinct_on {
                $ctx.sql.push_str("DISTINCT ON (");
                for (i, col) in $select.distinct.iter().enumerate() {
                    if i > 0 {
                        $ctx.sql.push_str(", ");
                    }
                    crate::push_identifier_reference(&mut $ctx.sql, col, $quote);
                }
                $ctx.sql.push_str(") ");
            } else {
                $ctx.sql.push_str("DISTINCT ");
            }
        }

        let has_items =
            !$select.items.is_empty() || $select.joins.iter().any(|join| !join.items.is_empty());

        if !has_items {
            $ctx.sql.push('*');
        } else {
            let mut first = true;
            for item in $select.items.iter_mut() {
                if !first {
                    $ctx.sql.push_str(", ");
                }
                first = false;
                match item {
                    nautilus_core::SelectItem::Column(col) => {
                        crate::push_qualified_identifier(
                            &mut $ctx.sql,
                            &col.table,
                            &col.name,
                            $quote,
                        );
                        $ctx.sql.push_str(" AS ");
                        crate::push_column_alias(&mut $ctx.sql, col, $quote);
                    }
                    nautilus_core::SelectItem::Computed { expr, alias } => {
                        $ctx.sql.push('(');
                        $render_expr($ctx, expr);
                        $ctx.sql.push(')');
                        $ctx.sql.push_str(" AS ");
                        crate::push_quoted_identifier(&mut $ctx.sql, alias, $quote);
                    }
                }
            }
            for join in $select.joins.iter_mut() {
                for item in join.items.iter_mut() {
                    if !first {
                        $ctx.sql.push_str(", ");
                    }
                    first = false;
                    match item {
                        nautilus_core::SelectItem::Column(col) => {
                            crate::push_qualified_identifier(
                                &mut $ctx.sql,
                                &col.table,
                                &col.name,
                                $quote,
                            );
                            $ctx.sql.push_str(" AS ");
                            crate::push_column_alias(&mut $ctx.sql, col, $quote);
                        }
                        nautilus_core::SelectItem::Computed { expr, alias } => {
                            $ctx.sql.push('(');
                            $render_expr($ctx, expr);
                            $ctx.sql.push(')');
                            $ctx.sql.push_str(" AS ");
                            crate::push_quoted_identifier(&mut $ctx.sql, alias, $quote);
                        }
                    }
                }
            }
        }

        $ctx.sql.push_str(" FROM ");
        crate::push_quoted_identifier(&mut $ctx.sql, &$select.table, $quote);

        for join in $select.joins.iter_mut() {
            match join.join_type {
                nautilus_core::JoinType::Inner => $ctx.sql.push_str(" INNER JOIN "),
                nautilus_core::JoinType::Left => $ctx.sql.push_str(" LEFT JOIN "),
            }
            crate::push_quoted_identifier(&mut $ctx.sql, &join.table, $quote);
            $ctx.sql.push_str(" ON ");
            $render_expr($ctx, &mut join.on);
        }

        if let Some(filter) = $select.filter.as_mut() {
            $ctx.sql.push_str(" WHERE ");
            $render_expr($ctx, filter);
        }

        if !$select.group_by.is_empty() {
            $ctx.sql.push_str(" GROUP BY ");
            for (i, col) in $select.group_by.iter().enumerate() {
                if i > 0 {
                    $ctx.sql.push_str(", ");
                }
                crate::push_qualified_identifier(&mut $ctx.sql, &col.table, &col.name, $quote);
            }
        }

        if let Some(having) = $select.having.as_mut() {
            $ctx.sql.push_str(" HAVING ");
            $render_expr($ctx, having);
        }

        let has_order_items = !$select.order_by_items.is_empty();
        let has_col_order = !$select.order_by.is_empty();
        let has_expr_order = !$select.order_by_exprs.is_empty();
        if has_order_items || has_col_order || has_expr_order {
            $ctx.sql.push_str(" ORDER BY ");
            let mut first = true;
            if has_order_items {
                for item in $select.order_by_items.iter_mut() {
                    if !first {
                        $ctx.sql.push_str(", ");
                    }
                    first = false;
                    match item {
                        nautilus_core::OrderByItem::Column(order) => {
                            crate::push_identifier_reference(&mut $ctx.sql, &order.column, $quote);
                            match order.direction {
                                nautilus_core::OrderDir::Asc => $ctx.sql.push_str(" ASC"),
                                nautilus_core::OrderDir::Desc => $ctx.sql.push_str(" DESC"),
                            }
                        }
                        nautilus_core::OrderByItem::Expr(expr, dir) => {
                            $render_expr($ctx, expr);
                            match *dir {
                                nautilus_core::OrderDir::Asc => $ctx.sql.push_str(" ASC"),
                                nautilus_core::OrderDir::Desc => $ctx.sql.push_str(" DESC"),
                            }
                        }
                    }
                }
            } else {
                for order in $select.order_by.iter() {
                    if !first {
                        $ctx.sql.push_str(", ");
                    }
                    first = false;
                    crate::push_identifier_reference(&mut $ctx.sql, &order.column, $quote);
                    match order.direction {
                        nautilus_core::OrderDir::Asc => $ctx.sql.push_str(" ASC"),
                        nautilus_core::OrderDir::Desc => $ctx.sql.push_str(" DESC"),
                    }
                }
                for (expr, dir) in $select.order_by_exprs.iter_mut() {
                    if !first {
                        $ctx.sql.push_str(", ");
                    }
                    first = false;
                    $render_expr($ctx, expr);
                    match *dir {
                        nautilus_core::OrderDir::Asc => $ctx.sql.push_str(" ASC"),
                        nautilus_core::OrderDir::Desc => $ctx.sql.push_str(" DESC"),
                    }
                }
            }
        }

        if let Some(take) = $select.take {
            $ctx.sql.push_str(" LIMIT ");
            crate::push_u32(&mut $ctx.sql, take.unsigned_abs());
        } else if $mysql_limit_hack && $select.skip.is_some() {
            $ctx.sql.push_str(" LIMIT 18446744073709551615");
        }

        if let Some(skip) = $select.skip {
            $ctx.sql.push_str(" OFFSET ");
            crate::push_u32(&mut $ctx.sql, skip);
        }
    }};
}

/// Mutable/owned variant of [`render_expr_common!`] used by `render_*_owned`.
macro_rules! render_expr_common_mut {
    (
        $ctx:expr, $expr:expr,
        $quote:expr, $render_expr:ident, $render_select_body:ident,
        { $($specific:tt)* }
    ) => {
        match $expr {
            nautilus_core::Expr::Column(name) => {
                crate::push_identifier_reference(&mut $ctx.sql, name, $quote);
            }
            nautilus_core::Expr::Not(inner) => {
                $ctx.sql.push_str("NOT (");
                $render_expr($ctx, inner.as_mut());
                $ctx.sql.push(')');
            }
            nautilus_core::Expr::Exists(subquery) => {
                $ctx.sql.push_str("EXISTS (");
                $render_select_body($ctx, subquery.as_mut());
                $ctx.sql.push(')');
            }
            nautilus_core::Expr::NotExists(subquery) => {
                $ctx.sql.push_str("NOT EXISTS (");
                $render_select_body($ctx, subquery.as_mut());
                $ctx.sql.push(')');
            }
            nautilus_core::Expr::Relation { op, relation } => {
                let is_exists = matches!(*op, nautilus_core::expr::RelationFilterOp::Some);
                if is_exists {
                    $ctx.sql.push_str("EXISTS (SELECT * FROM ");
                } else {
                    $ctx.sql.push_str("NOT EXISTS (SELECT * FROM ");
                }
                crate::push_quoted_identifier(&mut $ctx.sql, &relation.target_table, $quote);
                $ctx.sql.push_str(" WHERE ");
                crate::push_qualified_identifier(
                    &mut $ctx.sql,
                    &relation.target_table,
                    &relation.fk_db,
                    $quote,
                );
                $ctx.sql.push_str(" = ");
                crate::push_qualified_identifier(
                    &mut $ctx.sql,
                    &relation.parent_table,
                    &relation.pk_db,
                    $quote,
                );
                $ctx.sql.push_str(" AND ");
                if matches!(*op, nautilus_core::expr::RelationFilterOp::Every) {
                    $ctx.sql.push_str("NOT (");
                    $render_expr($ctx, relation.filter.as_mut());
                    $ctx.sql.push(')');
                } else {
                    $render_expr($ctx, relation.filter.as_mut());
                }
                $ctx.sql.push(')');
            }
            nautilus_core::Expr::ScalarSubquery(subquery) => {
                $ctx.sql.push('(');
                $render_select_body($ctx, subquery.as_mut());
                $ctx.sql.push(')');
            }
            nautilus_core::Expr::IsNull(inner) => {
                $ctx.sql.push('(');
                $render_expr($ctx, inner.as_mut());
                $ctx.sql.push_str(" IS NULL)");
            }
            nautilus_core::Expr::IsNotNull(inner) => {
                $ctx.sql.push('(');
                $render_expr($ctx, inner.as_mut());
                $ctx.sql.push_str(" IS NOT NULL)");
            }
            nautilus_core::Expr::Literal(s) => {
                crate::push_sql_string_literal(&mut $ctx.sql, s);
            }
            nautilus_core::Expr::List(exprs) => {
                for (i, e) in exprs.iter_mut().enumerate() {
                    if i > 0 {
                        $ctx.sql.push_str(", ");
                    }
                    $render_expr($ctx, e);
                }
            }
            nautilus_core::Expr::CaseWhen { condition, then } => {
                $ctx.sql.push_str("CASE WHEN ");
                $render_expr($ctx, condition.as_mut());
                $ctx.sql.push_str(" THEN ");
                $render_expr($ctx, then.as_mut());
                $ctx.sql.push_str(" ELSE NULL END");
            }
            nautilus_core::Expr::Star => {
                $ctx.sql.push('*');
            }
            $($specific)*
        }
    };
}

mod mysql;
mod postgres;
mod render_estimate;
mod sqlite;

pub use mysql::MysqlDialect;
pub use postgres::PostgresDialect;
pub use sqlite::SqliteDialect;

use nautilus_core::{Delete, Insert, Result, Select, Update, Value};
pub(crate) use render_estimate::{
    estimate_delete_render, estimate_insert_render, estimate_select_render, estimate_update_render,
    RenderEstimate,
};

/// SQL query with bound parameters.
///
/// Separates the SQL text from parameter values for use with prepared statements.
#[derive(Debug, Clone, PartialEq)]
#[must_use]
pub struct Sql {
    /// The SQL query text with parameter placeholders.
    pub text: String,
    /// The parameter values to bind to the query.
    pub params: Vec<Value>,
}

/// Trait for SQL dialect renderers.
///
/// Allows rendering AST queries into dialect-specific SQL strings.
pub trait Dialect {
    /// Whether this dialect natively supports the RETURNING clause
    /// on INSERT, UPDATE, and DELETE statements.
    ///
    /// Dialects that return `false` (e.g. MySQL) will have RETURNING
    /// emulated at the connector layer via separate queries.
    fn supports_returning(&self) -> bool {
        true
    }

    /// Render a SELECT query into SQL.
    fn render_select(&self, select: &Select) -> Result<Sql>;

    /// Render an owned SELECT query into SQL, allowing dialects to move bound
    /// values out of the AST instead of cloning them.
    fn render_select_owned(&self, select: Select) -> Result<Sql> {
        self.render_select(&select)
    }

    /// Render an INSERT query into SQL.
    fn render_insert(&self, insert: &Insert) -> Result<Sql>;

    /// Render an owned INSERT query into SQL, allowing dialects to move bound
    /// values out of the AST instead of cloning them.
    fn render_insert_owned(&self, insert: Insert) -> Result<Sql> {
        self.render_insert(&insert)
    }

    /// Render an UPDATE query into SQL.
    fn render_update(&self, update: &Update) -> Result<Sql>;

    /// Render an owned UPDATE query into SQL, allowing dialects to move bound
    /// values out of the AST instead of cloning them.
    fn render_update_owned(&self, update: Update) -> Result<Sql> {
        self.render_update(&update)
    }

    /// Render a DELETE query into SQL.
    fn render_delete(&self, delete: &Delete) -> Result<Sql>;

    /// Render an owned DELETE query into SQL, allowing dialects to move bound
    /// values out of the AST instead of cloning them.
    fn render_delete_owned(&self, delete: Delete) -> Result<Sql> {
        self.render_delete(&delete)
    }
}

fn push_escaped_identifier(sql: &mut String, name: &str, quote: char) {
    for ch in name.chars() {
        if ch == quote {
            sql.push(quote);
        }
        sql.push(ch);
    }
}

/// Quote a SQL identifier directly into the SQL buffer.
pub(crate) fn push_quoted_identifier(sql: &mut String, name: &str, quote: char) {
    sql.push(quote);
    push_escaped_identifier(sql, name, quote);
    sql.push(quote);
}

/// Quote multiple identifier segments as a single identifier directly into the SQL buffer.
pub(crate) fn push_quoted_identifier_segments(sql: &mut String, segments: &[&str], quote: char) {
    sql.push(quote);
    for segment in segments {
        push_escaped_identifier(sql, segment, quote);
    }
    sql.push(quote);
}

/// Render `table.column` directly into the SQL buffer.
pub(crate) fn push_qualified_identifier(sql: &mut String, table: &str, column: &str, quote: char) {
    push_quoted_identifier(sql, table, quote);
    sql.push('.');
    push_quoted_identifier(sql, column, quote);
}

/// Render a join-safe `table__column` alias directly into the SQL buffer.
pub(crate) fn push_column_alias(
    sql: &mut String,
    column: &nautilus_core::ColumnMarker,
    quote: char,
) {
    push_quoted_identifier_segments(
        sql,
        &[column.table.as_ref(), "__", column.name.as_ref()],
        quote,
    );
}

/// Render an identifier reference that may use the `table__column` shorthand.
///
/// The split happens only on the first `__`, so mapped column names like
/// `users__profile__slug` still render as `users.profile__slug`.
pub(crate) fn push_identifier_reference(sql: &mut String, name: &str, quote: char) {
    if let Some((table, column)) = name.split_once("__") {
        push_qualified_identifier(sql, table, column, quote);
    } else {
        push_quoted_identifier(sql, name, quote);
    }
}

/// Render a single-quoted SQL string literal directly into the SQL buffer.
pub(crate) fn push_sql_string_literal(sql: &mut String, value: &str) {
    sql.push('\'');
    for ch in value.chars() {
        if ch == '\'' {
            sql.push('\'');
        }
        sql.push(ch);
    }
    sql.push('\'');
}

fn push_u64(sql: &mut String, mut value: u64) {
    let mut digits = [0_u8; 20];
    let mut idx = digits.len();

    loop {
        idx -= 1;
        digits[idx] = b'0' + (value % 10) as u8;
        value /= 10;
        if value == 0 {
            break;
        }
    }

    for digit in &digits[idx..] {
        sql.push(char::from(*digit));
    }
}

/// Append a `u32` value directly into the SQL buffer.
pub(crate) fn push_u32(sql: &mut String, value: u32) {
    push_u64(sql, u64::from(value));
}

/// Append a `usize` value directly into the SQL buffer.
pub(crate) fn push_usize(sql: &mut String, value: usize) {
    push_u64(sql, value as u64);
}

/// Return the SQL operator keyword for a standard scalar binary operation.
///
/// Call only for the nine scalar operators (Eq through Like).  Composite cases
/// (IN/NOT IN, array operators) must be handled separately by each dialect before
/// delegating to this helper.
#[inline]
pub(crate) fn binary_op_sql(op: &nautilus_core::BinaryOp) -> &'static str {
    match op {
        nautilus_core::BinaryOp::Eq => "=",
        nautilus_core::BinaryOp::Ne => "!=",
        nautilus_core::BinaryOp::Lt => "<",
        nautilus_core::BinaryOp::Le => "<=",
        nautilus_core::BinaryOp::Gt => ">",
        nautilus_core::BinaryOp::Ge => ">=",
        nautilus_core::BinaryOp::And => "AND",
        nautilus_core::BinaryOp::Or => "OR",
        nautilus_core::BinaryOp::Like => "LIKE",
        nautilus_core::BinaryOp::ArrayContains
        | nautilus_core::BinaryOp::ArrayContainedBy
        | nautilus_core::BinaryOp::ArrayOverlaps
        | nautilus_core::BinaryOp::In
        | nautilus_core::BinaryOp::NotIn => {
            unreachable!(
                "binary_op_sql: operator {:?} must be handled by dialect-specific code",
                op
            )
        }
    }
}
