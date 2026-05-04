//! PostgreSQL SQL dialect renderer.

use crate::{Dialect, Sql};
use nautilus_core::{BinaryOp, Delete, Expr, Insert, Result, Select, Update, Value};

/// PostgreSQL SQL dialect renderer.
///
/// Uses `$1, $2, ...` numbered parameter placeholders and double-quoted identifiers.
/// Supports `RETURNING`, `DISTINCT ON`, PostgreSQL array operators, UUID type casts,
/// and `FILTER (WHERE ...)` on aggregates.
#[derive(Debug, Clone, Copy)]
pub struct PostgresDialect;

impl Dialect for PostgresDialect {
    fn render_select(&self, select: &Select) -> Result<Sql> {
        let mut ctx = RenderContext::with_estimate(crate::estimate_select_render(select));
        render_select_body_core!(&mut ctx, select, '"', render_expr, true, false);
        Ok(Sql {
            text: ctx.sql,
            params: ctx.params,
        })
    }

    fn render_insert(&self, insert: &Insert) -> Result<Sql> {
        let mut ctx = RenderContext::with_estimate(crate::estimate_insert_render(insert));
        render_insert_body!(&mut ctx, insert, '"', true, true);
        Ok(Sql {
            text: ctx.sql,
            params: ctx.params,
        })
    }

    fn render_update(&self, update: &Update) -> Result<Sql> {
        let mut ctx = RenderContext::with_estimate(crate::estimate_update_render(update));
        render_update_body!(&mut ctx, update, '"', render_expr, true, true);
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
        self.sql.push('$');
        crate::push_usize(&mut self.sql, self.params.len());
    }
}

fn render_select_body(ctx: &mut RenderContext, select: &crate::Select) {
    render_select_body_core!(ctx, select, '"', render_expr, true, false);
}

fn render_expr(ctx: &mut RenderContext, expr: &Expr) {
    render_expr_common!(ctx, expr, '"', render_expr, render_select_body, {
        Expr::Param(value) => {
            // NULL is emitted literally; PostgreSQL cannot implicitly resolve a
            // typed NULL sent as an unknown OID via the binary protocol.
            if matches!(value, Value::Null) {
                ctx.sql.push_str("NULL");
            } else {
                ctx.push_param(value.clone());
                // PostgreSQL needs an explicit cast when the driver sends an unknown OID.
                if matches!(value, Value::Uuid(_)) {
                    ctx.sql.push_str("::uuid");
                } else if matches!(value, Value::Json(_)) {
                    ctx.sql.push_str("::json");
                } else if matches!(value, Value::Vector(_)) {
                    ctx.sql.push_str("::vector");
                } else if matches!(value, Value::Geometry(_)) {
                    ctx.sql.push_str("::geometry");
                } else if matches!(value, Value::Geography(_)) {
                    ctx.sql.push_str("::geography");
                } else if is_homogeneous_geometry_array(value) {
                    ctx.sql.push_str("::geometry[]");
                } else if is_homogeneous_geography_array(value) {
                    ctx.sql.push_str("::geography[]");
                } else if let Value::Enum { type_name, .. } = value {
                    ctx.sql.push_str("::");
                    ctx.sql.push_str(type_name);
                }
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
            } else {
                ctx.sql.push('(');
                render_expr(ctx, left);
                ctx.sql.push(' ');
                ctx.sql.push_str(match op {
                    BinaryOp::ArrayContains => "@>",
                    BinaryOp::ArrayContainedBy => "<@",
                    BinaryOp::ArrayOverlaps => "&&",
                    _ => crate::binary_op_sql(op),
                });
                ctx.sql.push(' ');
                render_expr(ctx, right);
                ctx.sql.push(')');
            }
        }
        Expr::FunctionCall { name, args } => {
            if args.len() == 2 {
                let op = match name.as_str() {
                    nautilus_core::expr::VECTOR_L2_DISTANCE_FUNCTION => Some("<->"),
                    nautilus_core::expr::VECTOR_INNER_PRODUCT_FUNCTION => Some("<#>"),
                    nautilus_core::expr::VECTOR_COSINE_DISTANCE_FUNCTION => Some("<=>"),
                    _ => None,
                };
                if let Some(op) = op {
                    ctx.sql.push('(');
                    render_expr(ctx, &args[0]);
                    ctx.sql.push(' ');
                    ctx.sql.push_str(op);
                    ctx.sql.push(' ');
                    render_expr(ctx, &args[1]);
                    ctx.sql.push(')');
                    return;
                }
            }
            ctx.sql.push_str(name);
            ctx.sql.push('(');
            for (i, arg) in args.iter().enumerate() {
                if i > 0 { ctx.sql.push_str(", "); }
                render_expr(ctx, arg);
            }
            ctx.sql.push(')');
        }
        Expr::Filter { expr, predicate } => {
            // Native PostgreSQL FILTER clause (supported since pg 9.4).
            render_expr(ctx, expr);
            ctx.sql.push_str(" FILTER (WHERE ");
            render_expr(ctx, predicate);
            ctx.sql.push(')');
        }
    });
}

fn is_homogeneous_geometry_array(value: &Value) -> bool {
    matches!(
        value,
        Value::Array(items) if !items.is_empty() && items.iter().all(|item| matches!(item, Value::Geometry(_)))
    )
}

fn is_homogeneous_geography_array(value: &Value) -> bool {
    matches!(
        value,
        Value::Array(items) if !items.is_empty() && items.iter().all(|item| matches!(item, Value::Geography(_)))
    )
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
        let dialect = PostgresDialect;
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
            "SELECT * FROM \"posts\" WHERE (\"posts\".\"tags\" @> $1)"
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
        let dialect = PostgresDialect;
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
            "SELECT * FROM \"posts\" WHERE (\"posts\".\"tags\" <@ $1)"
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
        let dialect = PostgresDialect;
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
            "SELECT * FROM \"posts\" WHERE (\"posts\".\"tags\" && $1)"
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

    #[test]
    fn test_array_operators_with_integers() {
        let dialect = PostgresDialect;
        let expr = Expr::Binary {
            left: Box::new(Expr::column("posts__scores")),
            op: BinaryOp::ArrayContains,
            right: Box::new(Expr::param(Value::Array(vec![
                Value::I32(100),
                Value::I32(200),
            ]))),
        };
        let select = Select::from_table("posts").filter(expr).build().unwrap();
        let sql = dialect.render_select(&select).unwrap();

        assert_eq!(
            sql.text,
            "SELECT * FROM \"posts\" WHERE (\"posts\".\"scores\" @> $1)"
        );
        assert_eq!(sql.params.len(), 1);
        match &sql.params[0] {
            Value::Array(arr) => {
                assert_eq!(arr.len(), 2);
                assert_eq!(arr[0], Value::I32(100));
                assert_eq!(arr[1], Value::I32(200));
            }
            _ => panic!("Expected Array value"),
        }
    }

    #[test]
    fn vector_params_are_cast_to_pgvector_type() {
        let dialect = PostgresDialect;
        let select = Select::from_table("embeddings")
            .filter(
                Expr::column("embeddings__vector")
                    .eq(Expr::param(Value::Vector(vec![1.0, 2.0, 3.0]))),
            )
            .build()
            .unwrap();
        let sql = dialect.render_select(&select).unwrap();

        assert_eq!(
            sql.text,
            "SELECT * FROM \"embeddings\" WHERE (\"embeddings\".\"vector\" = $1::vector)"
        );
        assert_eq!(sql.params, vec![Value::Vector(vec![1.0, 2.0, 3.0])]);
    }

    #[test]
    fn postgis_params_are_cast_to_spatial_types() {
        let dialect = PostgresDialect;
        let select = Select::from_table("places")
            .filter(
                Expr::column("places__geom")
                    .eq(Expr::param(Value::Geometry("POINT(1 2)".to_string()))),
            )
            .build()
            .unwrap();
        let sql = dialect.render_select(&select).unwrap();

        assert_eq!(
            sql.text,
            "SELECT * FROM \"places\" WHERE (\"places\".\"geom\" = $1::geometry)"
        );
        assert_eq!(sql.params, vec![Value::Geometry("POINT(1 2)".to_string())]);

        let select = Select::from_table("places")
            .filter(
                Expr::column("places__geog")
                    .eq(Expr::param(Value::Geography("POINT(1 2)".to_string()))),
            )
            .build()
            .unwrap();
        let sql = dialect.render_select(&select).unwrap();

        assert_eq!(
            sql.text,
            "SELECT * FROM \"places\" WHERE (\"places\".\"geog\" = $1::geography)"
        );
        assert_eq!(sql.params, vec![Value::Geography("POINT(1 2)".to_string())]);
    }

    #[test]
    fn vector_distance_ordering_uses_pgvector_operator() {
        let dialect = PostgresDialect;
        let select = Select::from_table("embeddings")
            .order_by_expr(
                Expr::vector_distance(
                    nautilus_core::VectorMetric::Cosine,
                    Expr::column("embeddings__vector"),
                    Expr::param(Value::Vector(vec![1.0, 2.0, 3.0])),
                ),
                nautilus_core::OrderDir::Asc,
            )
            .take(5)
            .build()
            .unwrap();
        let sql = dialect.render_select(&select).unwrap();

        assert_eq!(
            sql.text,
            "SELECT * FROM \"embeddings\" ORDER BY (\"embeddings\".\"vector\" <=> $1::vector) ASC LIMIT 5"
        );
        assert_eq!(sql.params, vec![Value::Vector(vec![1.0, 2.0, 3.0])]);
    }
}
