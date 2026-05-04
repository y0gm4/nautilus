//! MySQL SQL dialect renderer.

use crate::{Dialect, Sql};
use nautilus_core::{BinaryOp, Delete, Error, Expr, Insert, Result, Select, Update, Value};

/// MySQL SQL dialect renderer.
#[derive(Debug, Clone, Copy)]
pub struct MysqlDialect;

/// Renders query ASTs into MySQL-compatible SQL with `?` placeholders
/// and backtick-quoted identifiers.
impl Dialect for MysqlDialect {
    fn supports_returning(&self) -> bool {
        false
    }

    fn render_select(&self, select: &Select) -> Result<Sql> {
        let mut ctx = RenderContext::with_estimate(crate::estimate_select_render(select));
        render_select_body_core!(&mut ctx, select, '`', render_expr, false, true);
        ctx.finish()
    }

    fn render_insert(&self, insert: &Insert) -> Result<Sql> {
        let mut ctx = RenderContext::with_estimate(crate::estimate_insert_render(insert));
        render_insert_body!(&mut ctx, insert, '`', false, false);
        ctx.finish()
    }

    fn render_update(&self, update: &Update) -> Result<Sql> {
        let mut ctx = RenderContext::with_estimate(crate::estimate_update_render(update));
        render_update_body!(&mut ctx, update, '`', render_expr, false, false);
        ctx.finish()
    }

    fn render_delete(&self, delete: &Delete) -> Result<Sql> {
        let mut ctx = RenderContext::with_estimate(crate::estimate_delete_render(delete));
        render_delete_body!(&mut ctx, delete, '`', render_expr, false);
        ctx.finish()
    }
}

struct RenderContext {
    sql: String,
    params: Vec<Value>,
    error: Option<Error>,
}

impl RenderContext {
    fn with_estimate(estimate: crate::RenderEstimate) -> Self {
        Self {
            sql: String::with_capacity(estimate.sql_capacity),
            params: Vec::with_capacity(estimate.params_capacity),
            error: None,
        }
    }

    fn push_param(&mut self, value: Value) {
        self.params.push(value);
        self.sql.push('?');
    }

    fn fail(&mut self, message: impl Into<String>) {
        if self.error.is_none() {
            self.error = Some(Error::InvalidQuery(message.into()));
        }
    }

    fn finish(self) -> Result<Sql> {
        if let Some(err) = self.error {
            return Err(err);
        }

        Ok(Sql {
            text: self.sql,
            params: self.params,
        })
    }
}

fn render_select_body(ctx: &mut RenderContext, select: &crate::Select) {
    render_select_body_core!(ctx, select, '`', render_expr, false, true);
}

fn mysql_function_name(name: &str) -> &str {
    match name {
        "json_agg" => "JSON_ARRAYAGG",
        "json_build_object" => "JSON_OBJECT",
        _ => name,
    }
}

fn render_case_filtered_aggregate(
    ctx: &mut RenderContext,
    fn_name: &str,
    arg: &Expr,
    predicate: &Expr,
) {
    ctx.sql.push_str(fn_name);
    ctx.sql.push_str("(CASE WHEN ");
    render_expr(ctx, predicate);
    ctx.sql.push_str(" THEN ");
    render_expr(ctx, arg);
    ctx.sql.push_str(" ELSE NULL END)");
}

fn render_filter(ctx: &mut RenderContext, expr: &Expr, predicate: &Expr) {
    let Expr::FunctionCall { name, args } = expr else {
        ctx.fail("MysqlDialect can only emulate FILTER for aggregate function calls");
        return;
    };

    let upper = name.to_ascii_uppercase();
    match (upper.as_str(), args.as_slice()) {
        ("COUNT", [Expr::Star]) => {
            ctx.sql.push_str("COUNT(CASE WHEN ");
            render_expr(ctx, predicate);
            ctx.sql.push_str(" THEN 1 ELSE NULL END)");
        }
        ("COUNT", [arg]) | ("SUM", [arg]) | ("AVG", [arg]) | ("MIN", [arg]) | ("MAX", [arg]) => {
            render_case_filtered_aggregate(ctx, upper.as_str(), arg, predicate);
        }
        ("JSON_AGG", [_]) => {
            ctx.fail(
                "MysqlDialect cannot emulate FILTER for json_agg without changing JSON null semantics",
            );
        }
        (_, []) => {
            ctx.fail(format!(
                "MysqlDialect cannot emulate FILTER for function '{}' with zero arguments",
                name
            ));
        }
        _ => {
            ctx.fail(format!(
                "MysqlDialect cannot emulate FILTER for function '{}' with {} arguments",
                name,
                args.len()
            ));
        }
    }
}

fn render_expr(ctx: &mut RenderContext, expr: &Expr) {
    if ctx.error.is_some() {
        return;
    }

    render_expr_common!(ctx, expr, '`', render_expr, render_select_body, {
        Expr::Param(value) => {
            if matches!(value, Value::Null) {
                ctx.sql.push_str("NULL");
            } else {
                ctx.push_param(value.clone());
            }
        }
        Expr::Binary { left, op, right } => {
            if matches!(op, BinaryOp::In | BinaryOp::NotIn) {
                ctx.sql.push('(');
                render_expr(ctx, left);
                ctx.sql.push(' ');
                ctx.sql.push_str(if matches!(op, BinaryOp::In) { "IN" } else { "NOT IN" });
                ctx.sql.push_str(" (");
                if let Expr::List(exprs) = right.as_ref() {
                    for (i, e) in exprs.iter().enumerate() {
                        if i > 0 { ctx.sql.push_str(", "); }
                        render_expr(ctx, e);
                    }
                } else {
                    render_expr(ctx, right);
                }
                ctx.sql.push(')');
                ctx.sql.push(')');
            } else if matches!(op, BinaryOp::ArrayContains | BinaryOp::ArrayContainedBy | BinaryOp::ArrayOverlaps) {
                // Array operators emulated via MySQL JSON functions.
                // Arrays are bound as JSON strings by the connector layer.
                match op {
                    BinaryOp::ArrayContains => {
                        // col @> rhs: col contains every element of rhs.
                        // JSON_CONTAINS(target, candidate) returns 1 when the candidate is a subset of the target.
                        ctx.sql.push_str("JSON_CONTAINS(");
                        render_expr(ctx, left);
                        ctx.sql.push_str(", ");
                        render_expr(ctx, right);
                        ctx.sql.push(')');
                    }
                    BinaryOp::ArrayContainedBy => {
                        // col <@ rhs: rhs contains every element of col.
                        ctx.sql.push_str("JSON_CONTAINS(");
                        render_expr(ctx, right);
                        ctx.sql.push_str(", ");
                        render_expr(ctx, left);
                        ctx.sql.push(')');
                    }
                    BinaryOp::ArrayOverlaps => {
                        ctx.fail(
                            "MysqlDialect does not render ArrayOverlaps generically because JSON_OVERLAPS is unavailable on some supported MySQL-family backends",
                        );
                    }
                    _ => unreachable!(),
                }
            } else {
                ctx.sql.push('(');
                render_expr(ctx, left);
                ctx.sql.push(' ');
                ctx.sql.push_str(crate::binary_op_sql(op));
                ctx.sql.push(' ');
                render_expr(ctx, right);
                ctx.sql.push(')');
            }
        }
        Expr::FunctionCall { name, args } => {
            let mysql_name = mysql_function_name(name);
            ctx.sql.push_str(mysql_name);
            ctx.sql.push('(');
            for (i, arg) in args.iter().enumerate() {
                if i > 0 { ctx.sql.push_str(", "); }
                render_expr(ctx, arg);
            }
            ctx.sql.push(')');
        }
        Expr::Filter { expr, predicate } => {
            render_filter(ctx, expr, predicate);
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn quote_identifier(name: &str) -> String {
        let mut sql = String::new();
        crate::push_quoted_identifier(&mut sql, name, '`');
        sql
    }

    #[test]
    fn test_quote_identifier() {
        assert_eq!(quote_identifier("users"), "`users`");
        assert_eq!(quote_identifier("email"), "`email`");
        assert_eq!(quote_identifier("foo`bar"), "`foo``bar`");
        assert_eq!(quote_identifier("a`b`c"), "`a``b``c`");
    }

    #[test]
    fn test_skip_without_take() {
        let dialect = MysqlDialect;
        let select = Select::from_table("users").skip(20).build().unwrap();
        let sql = dialect.render_select(&select).unwrap();

        assert_eq!(
            sql.text,
            "SELECT * FROM `users` LIMIT 18446744073709551615 OFFSET 20"
        );
        assert!(sql.params.is_empty());
    }

    #[test]
    fn test_insert_returning_is_omitted() {
        let dialect = MysqlDialect;
        let insert = Insert::into_table("users")
            .column(nautilus_core::ColumnMarker::new("users", "email"))
            .values(vec![Value::String("alice@example.com".to_string())])
            .returning(vec![
                nautilus_core::ColumnMarker::new("users", "id"),
                nautilus_core::ColumnMarker::new("users", "email"),
            ])
            .build()
            .unwrap();
        let sql = dialect.render_insert(&insert).unwrap();

        assert_eq!(sql.text, "INSERT INTO `users` (`email`) VALUES (?)");
        assert!(!sql.text.contains("RETURNING"));
    }

    #[test]
    fn test_update_returning_is_omitted() {
        let dialect = MysqlDialect;
        let update = Update::table("users")
            .set(
                nautilus_core::ColumnMarker::new("users", "email"),
                Value::String("new@example.com".to_string()),
            )
            .filter(Expr::column("id").eq(Expr::param(Value::I64(1))))
            .returning(vec![
                nautilus_core::ColumnMarker::new("users", "id"),
                nautilus_core::ColumnMarker::new("users", "email"),
            ])
            .build()
            .unwrap();
        let sql = dialect.render_update(&update).unwrap();

        assert_eq!(sql.text, "UPDATE `users` SET `email` = ? WHERE (`id` = ?)");
        assert!(!sql.text.contains("RETURNING"));
    }

    #[test]
    fn test_delete_returning_is_omitted() {
        let dialect = MysqlDialect;
        let delete = Delete::from_table("users")
            .filter(Expr::column("id").eq(Expr::param(Value::I64(1))))
            .returning(vec![
                nautilus_core::ColumnMarker::new("users", "id"),
                nautilus_core::ColumnMarker::new("users", "email"),
            ])
            .build()
            .unwrap();
        let sql = dialect.render_delete(&delete).unwrap();

        assert_eq!(sql.text, "DELETE FROM `users` WHERE (`id` = ?)");
        assert!(!sql.text.contains("RETURNING"));
    }

    #[test]
    fn test_filter_count_star_is_emulated() {
        let dialect = MysqlDialect;
        let select = Select::from_table("users")
            .computed(
                Expr::function_call("COUNT", vec![Expr::star()])
                    .filter(Expr::column("active").eq(Expr::param(Value::Bool(true)))),
                "active_count",
            )
            .build()
            .unwrap();

        let sql = dialect.render_select(&select).unwrap();

        assert_eq!(
            sql.text,
            "SELECT (COUNT(CASE WHEN (`active` = ?) THEN 1 ELSE NULL END)) AS `active_count` FROM `users`"
        );
        assert_eq!(sql.params, vec![Value::Bool(true)]);
    }

    #[test]
    fn test_filter_single_arg_aggregate_is_emulated() {
        let dialect = MysqlDialect;
        let select = Select::from_table("users")
            .computed(
                Expr::function_call("SUM", vec![Expr::column("score")])
                    .filter(Expr::column("active").eq(Expr::param(Value::Bool(true)))),
                "active_score",
            )
            .build()
            .unwrap();

        let sql = dialect.render_select(&select).unwrap();

        assert_eq!(
            sql.text,
            "SELECT (SUM(CASE WHEN (`active` = ?) THEN `score` ELSE NULL END)) AS `active_score` FROM `users`"
        );
        assert_eq!(sql.params, vec![Value::Bool(true)]);
    }

    #[test]
    fn test_filter_multi_arg_function_is_rejected() {
        let dialect = MysqlDialect;
        let select = Select::from_table("users")
            .computed(
                Expr::function_call(
                    "json_build_object",
                    vec![Expr::Literal("score".to_string()), Expr::column("score")],
                )
                .filter(Expr::column("active").eq(Expr::param(Value::Bool(true)))),
                "payload",
            )
            .build()
            .unwrap();

        let err = dialect.render_select(&select).unwrap_err();
        assert!(err
            .to_string()
            .contains("cannot emulate FILTER for function 'json_build_object'"));
    }

    #[test]
    fn test_array_overlaps_is_rejected() {
        let dialect = MysqlDialect;
        let expr = Expr::Binary {
            left: Box::new(Expr::column("posts__tags")),
            op: BinaryOp::ArrayOverlaps,
            right: Box::new(Expr::param(Value::Array(vec![Value::String(
                "rust".to_string(),
            )]))),
        };
        let select = Select::from_table("posts").filter(expr).build().unwrap();

        let err = dialect.render_select(&select).unwrap_err();
        assert!(err.to_string().contains("ArrayOverlaps generically"));
    }
}
