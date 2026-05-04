//! SQLite SQL dialect renderer.

use crate::{Dialect, Sql};
use nautilus_core::{BinaryOp, Delete, Expr, Insert, Result, Select, Update, Value};

/// SQLite SQL dialect renderer.
#[derive(Debug, Clone, Copy)]
pub struct SqliteDialect;

/// Renders query ASTs into SQLite-compatible SQL with `?` placeholders
/// and double-quoted identifiers.
impl Dialect for SqliteDialect {
    fn render_select(&self, select: &Select) -> Result<Sql> {
        let mut ctx = RenderContext::with_estimate(crate::estimate_select_render(select));
        render_select_body_core!(&mut ctx, select, '"', render_expr, false, false);
        Ok(Sql {
            text: ctx.sql,
            params: ctx.params,
        })
    }

    fn render_insert(&self, insert: &Insert) -> Result<Sql> {
        let mut ctx = RenderContext::with_estimate(crate::estimate_insert_render(insert));
        render_insert_body!(&mut ctx, insert, '"', true, false);
        Ok(Sql {
            text: ctx.sql,
            params: ctx.params,
        })
    }

    fn render_update(&self, update: &Update) -> Result<Sql> {
        let mut ctx = RenderContext::with_estimate(crate::estimate_update_render(update));
        render_update_body!(&mut ctx, update, '"', render_expr, true, false);
        Ok(Sql {
            text: ctx.sql,
            params: ctx.params,
        })
    }

    fn render_delete(&self, delete: &Delete) -> Result<Sql> {
        let mut ctx = RenderContext::with_estimate(crate::estimate_delete_render(delete));
        render_delete_body!(&mut ctx, delete, '"', render_expr, true);
        Ok(Sql {
            text: ctx.sql,
            params: ctx.params,
        })
    }
}

struct RenderContext {
    sql: String,
    params: Vec<Value>,
}

impl RenderContext {
    fn with_estimate(estimate: crate::RenderEstimate) -> Self {
        Self {
            sql: String::with_capacity(estimate.sql_capacity),
            params: Vec::with_capacity(estimate.params_capacity),
        }
    }

    fn push_param(&mut self, value: Value) {
        self.params.push(value);
        self.sql.push('?');
    }
}

fn render_select_body(ctx: &mut RenderContext, select: &crate::Select) {
    render_select_body_core!(ctx, select, '"', render_expr, false, false);
}

fn render_expr(ctx: &mut RenderContext, expr: &Expr) {
    render_expr_common!(ctx, expr, '"', render_expr, render_select_body, {
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
                // Array operators emulated via SQLite JSON functions.
                // Arrays are bound as JSON strings by the connector layer; json_each unpacks them.
                match op {
                    BinaryOp::ArrayContains => {
                        // col @> rhs: every element of rhs exists in col.
                        ctx.sql.push_str("NOT EXISTS (SELECT 1 FROM json_each(");
                        render_expr(ctx, right);
                        ctx.sql.push_str(") AS _rhs WHERE NOT EXISTS (SELECT 1 FROM json_each(");
                        render_expr(ctx, left);
                        ctx.sql.push_str(") AS _col WHERE _col.value IS _rhs.value))");
                    }
                    BinaryOp::ArrayContainedBy => {
                        // col <@ rhs: every element of col exists in rhs.
                        ctx.sql.push_str("NOT EXISTS (SELECT 1 FROM json_each(");
                        render_expr(ctx, left);
                        ctx.sql.push_str(") AS _col WHERE NOT EXISTS (SELECT 1 FROM json_each(");
                        render_expr(ctx, right);
                        ctx.sql.push_str(") AS _rhs WHERE _col.value IS _rhs.value))");
                    }
                    BinaryOp::ArrayOverlaps => {
                        // col && rhs: at least one element in common.
                        ctx.sql.push_str("EXISTS (SELECT 1 FROM json_each(");
                        render_expr(ctx, left);
                        ctx.sql.push_str(") AS _col WHERE EXISTS (SELECT 1 FROM json_each(");
                        render_expr(ctx, right);
                        ctx.sql.push_str(") AS _rhs WHERE _col.value IS _rhs.value))");
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
            let sqlite_name = match name.as_str() {
                "json_agg" => "json_group_array",
                "json_build_object" => "json_object",
                _ => name,
            };
            ctx.sql.push_str(sqlite_name);
            ctx.sql.push('(');
            for (i, arg) in args.iter().enumerate() {
                if i > 0 { ctx.sql.push_str(", "); }
                render_expr(ctx, arg);
            }
            ctx.sql.push(')');
        }
        Expr::Filter { expr, predicate } => {
            render_expr(ctx, expr);
            ctx.sql.push_str(" FILTER (WHERE ");
            render_expr(ctx, predicate);
            ctx.sql.push(')');
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn quote_identifier(name: &str) -> String {
        let mut sql = String::new();
        crate::push_quoted_identifier(&mut sql, name, '"');
        sql
    }

    #[test]
    fn test_quote_identifier() {
        assert_eq!(quote_identifier("users"), "\"users\"");
        assert_eq!(quote_identifier("email"), "\"email\"");
        assert_eq!(quote_identifier("foo\"bar"), "\"foo\"\"bar\"");
        assert_eq!(quote_identifier("a\"b\"c"), "\"a\"\"b\"\"c\"");
    }

    #[test]
    fn test_array_contains_operator() {
        let dialect = SqliteDialect;
        let expr = Expr::Binary {
            left: Box::new(Expr::column("posts__tags")),
            op: BinaryOp::ArrayContains,
            right: Box::new(Expr::param(Value::Array(vec![Value::String(
                "rust".to_string(),
            )]))),
        };
        let select = Select::from_table("posts").filter(expr).build().unwrap();
        let sql = dialect.render_select(&select).unwrap();

        assert_eq!(
            sql.text,
            "SELECT * FROM \"posts\" WHERE NOT EXISTS (SELECT 1 FROM json_each(?) AS _rhs WHERE NOT EXISTS (SELECT 1 FROM json_each(\"posts\".\"tags\") AS _col WHERE _col.value IS _rhs.value))"
        );
        assert_eq!(sql.params.len(), 1);
        match &sql.params[0] {
            Value::Array(arr) => {
                assert_eq!(arr.len(), 1);
                assert_eq!(arr[0], Value::String("rust".to_string()));
            }
            _ => panic!("Expected Array value"),
        }
    }

    #[test]
    fn test_array_contained_by_operator() {
        let dialect = SqliteDialect;
        let expr = Expr::Binary {
            left: Box::new(Expr::column("posts__tags")),
            op: BinaryOp::ArrayContainedBy,
            right: Box::new(Expr::param(Value::Array(vec![
                Value::String("rust".to_string()),
                Value::String("go".to_string()),
            ]))),
        };
        let select = Select::from_table("posts").filter(expr).build().unwrap();
        let sql = dialect.render_select(&select).unwrap();

        assert_eq!(
            sql.text,
            "SELECT * FROM \"posts\" WHERE NOT EXISTS (SELECT 1 FROM json_each(\"posts\".\"tags\") AS _col WHERE NOT EXISTS (SELECT 1 FROM json_each(?) AS _rhs WHERE _col.value IS _rhs.value))"
        );
        assert_eq!(sql.params.len(), 1);
        match &sql.params[0] {
            Value::Array(arr) => {
                assert_eq!(arr.len(), 2);
                assert_eq!(arr[0], Value::String("rust".to_string()));
                assert_eq!(arr[1], Value::String("go".to_string()));
            }
            _ => panic!("Expected Array value"),
        }
    }

    #[test]
    fn test_array_overlaps_operator() {
        let dialect = SqliteDialect;
        let expr = Expr::Binary {
            left: Box::new(Expr::column("posts__tags")),
            op: BinaryOp::ArrayOverlaps,
            right: Box::new(Expr::param(Value::Array(vec![
                Value::String("rust".to_string()),
                Value::String("python".to_string()),
            ]))),
        };
        let select = Select::from_table("posts").filter(expr).build().unwrap();
        let sql = dialect.render_select(&select).unwrap();

        assert_eq!(
            sql.text,
            "SELECT * FROM \"posts\" WHERE EXISTS (SELECT 1 FROM json_each(\"posts\".\"tags\") AS _col WHERE EXISTS (SELECT 1 FROM json_each(?) AS _rhs WHERE _col.value IS _rhs.value))"
        );
        assert_eq!(sql.params.len(), 1);
        match &sql.params[0] {
            Value::Array(arr) => {
                assert_eq!(arr.len(), 2);
                assert_eq!(arr[0], Value::String("rust".to_string()));
                assert_eq!(arr[1], Value::String("python".to_string()));
            }
            _ => panic!("Expected Array value"),
        }
    }
}
