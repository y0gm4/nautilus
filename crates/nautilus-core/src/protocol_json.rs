//! Convert typed Rust query arguments into the JSON shape consumed by the engine.

use std::collections::HashMap;

use serde_json::{Map as JsonMap, Value as JsonValue};

use crate::expr::RelationFilterOp;
use crate::{
    BinaryOp, Error, Expr, FindManyArgs, IncludeRelation, OrderBy, OrderDir, Result, VectorNearest,
};

/// Convert [`FindManyArgs`] into the same JSON payload shape used by thin clients.
///
/// This helper is intentionally conservative: if it encounters an expression
/// that cannot yet be represented in the engine wire format, it returns
/// [`Error::InvalidQuery`] so callers can decide whether to fail or to fall
/// back to a local execution path.
pub fn find_many_args_to_protocol_json(args: &FindManyArgs) -> Result<JsonValue> {
    let mut result = JsonMap::new();

    if let Some(where_) = &args.where_ {
        result.insert("where".to_string(), expr_to_filter_json(where_)?);
    }

    if !args.order_by.is_empty() {
        result.insert(
            "orderBy".to_string(),
            JsonValue::Array(
                args.order_by
                    .iter()
                    .map(order_by_to_json)
                    .collect::<Result<Vec<_>>>()?,
            ),
        );
    }

    if let Some(take) = args.take {
        result.insert("take".to_string(), JsonValue::from(take));
    }

    if let Some(skip) = args.skip {
        result.insert("skip".to_string(), JsonValue::from(skip));
    }

    if !args.include.is_empty() {
        result.insert("include".to_string(), include_map_to_json(&args.include)?);
    }

    if !args.select.is_empty() {
        let mut select = JsonMap::new();
        for (field, enabled) in &args.select {
            select.insert(field.clone(), JsonValue::Bool(*enabled));
        }
        result.insert("select".to_string(), JsonValue::Object(select));
    }

    if let Some(cursor) = &args.cursor {
        let mut wire_cursor = JsonMap::new();
        for (field, value) in cursor {
            wire_cursor.insert(strip_column_qualifier(field), value.to_json_plain());
        }
        result.insert("cursor".to_string(), JsonValue::Object(wire_cursor));
    }

    if !args.distinct.is_empty() {
        result.insert(
            "distinct".to_string(),
            JsonValue::Array(
                args.distinct
                    .iter()
                    .map(|field| JsonValue::String(strip_column_qualifier(field)))
                    .collect(),
            ),
        );
    }

    if let Some(nearest) = &args.nearest {
        result.insert("nearest".to_string(), nearest_to_json(nearest));
    }

    Ok(JsonValue::Object(result))
}

/// Convert a single Rust filter expression into the engine wire-format `"where"` object.
pub fn where_expr_to_protocol_json(expr: &Expr) -> Result<JsonValue> {
    expr_to_filter_json(expr)
}

fn include_map_to_json(include: &HashMap<String, IncludeRelation>) -> Result<JsonValue> {
    let mut result = JsonMap::new();
    for (field, relation) in include {
        result.insert(field.clone(), include_relation_to_json(relation)?);
    }
    Ok(JsonValue::Object(result))
}

fn include_relation_to_json(include: &IncludeRelation) -> Result<JsonValue> {
    let mut result = JsonMap::new();

    if let Some(where_) = &include.where_ {
        result.insert("where".to_string(), expr_to_filter_json(where_)?);
    }

    if !include.order_by.is_empty() {
        result.insert(
            "orderBy".to_string(),
            JsonValue::Array(
                include
                    .order_by
                    .iter()
                    .map(order_by_to_json)
                    .collect::<Result<Vec<_>>>()?,
            ),
        );
    }

    if let Some(take) = include.take {
        result.insert("take".to_string(), JsonValue::from(take));
    }

    if let Some(skip) = include.skip {
        result.insert("skip".to_string(), JsonValue::from(skip));
    }

    if let Some(cursor) = &include.cursor {
        let mut wire_cursor = JsonMap::new();
        for (field, value) in cursor {
            wire_cursor.insert(strip_column_qualifier(field), value.to_json_plain());
        }
        result.insert("cursor".to_string(), JsonValue::Object(wire_cursor));
    }

    if !include.distinct.is_empty() {
        result.insert(
            "distinct".to_string(),
            JsonValue::Array(
                include
                    .distinct
                    .iter()
                    .map(|field| JsonValue::String(strip_column_qualifier(field)))
                    .collect(),
            ),
        );
    }

    if !include.include.is_empty() {
        result.insert(
            "include".to_string(),
            include_map_to_json(&include.include)?,
        );
    }

    Ok(JsonValue::Object(result))
}

fn order_by_to_json(order: &OrderBy) -> Result<JsonValue> {
    let mut result = JsonMap::new();
    result.insert(
        strip_column_qualifier(&order.column),
        JsonValue::String(match order.direction {
            OrderDir::Asc => "asc".to_string(),
            OrderDir::Desc => "desc".to_string(),
        }),
    );
    Ok(JsonValue::Object(result))
}

fn nearest_to_json(nearest: &VectorNearest) -> JsonValue {
    let mut result = JsonMap::new();
    result.insert(
        "field".to_string(),
        JsonValue::String(strip_column_qualifier(&nearest.field)),
    );
    result.insert(
        "query".to_string(),
        JsonValue::Array(
            nearest
                .query
                .iter()
                .map(|value| JsonValue::from(*value as f64))
                .collect(),
        ),
    );
    result.insert(
        "metric".to_string(),
        JsonValue::String(nearest.metric.as_str().to_string()),
    );
    JsonValue::Object(result)
}

fn expr_to_filter_json(expr: &Expr) -> Result<JsonValue> {
    match expr {
        Expr::Binary {
            left,
            op: BinaryOp::And,
            right,
        } => logical_expr_to_json("AND", left, right),
        Expr::Binary {
            left,
            op: BinaryOp::Or,
            right,
        } => logical_expr_to_json("OR", left, right),
        Expr::Not(inner) => {
            let mut result = JsonMap::new();
            result.insert("NOT".to_string(), expr_to_filter_json(inner)?);
            Ok(JsonValue::Object(result))
        }
        Expr::Relation { op, relation } => {
            relation_predicate_to_json(&relation.field, *op, relation.filter.as_ref())
        }
        Expr::Binary { left, op, right } => field_predicate_to_json(left, op, right),
        Expr::IsNull(inner) => null_predicate_to_json(inner, true),
        Expr::IsNotNull(inner) => null_predicate_to_json(inner, false),
        other => Err(Error::InvalidQuery(format!(
            "query cannot be serialized to engine JSON: unsupported expression {:?}",
            other
        ))),
    }
}

fn relation_predicate_to_json(
    field: &str,
    op: RelationFilterOp,
    filter: &Expr,
) -> Result<JsonValue> {
    let mut relation_spec = JsonMap::new();
    relation_spec.insert(
        match op {
            RelationFilterOp::Some => "some".to_string(),
            RelationFilterOp::None => "none".to_string(),
            RelationFilterOp::Every => "every".to_string(),
        },
        expr_to_filter_json(filter)?,
    );

    let mut result = JsonMap::new();
    result.insert(field.to_string(), JsonValue::Object(relation_spec));
    Ok(JsonValue::Object(result))
}

fn logical_expr_to_json(name: &str, left: &Expr, right: &Expr) -> Result<JsonValue> {
    let mut items = Vec::new();
    collect_logical_operands(name, left, &mut items)?;
    collect_logical_operands(name, right, &mut items)?;

    let mut result = JsonMap::new();
    result.insert(name.to_string(), JsonValue::Array(items));
    Ok(JsonValue::Object(result))
}

fn collect_logical_operands(name: &str, expr: &Expr, out: &mut Vec<JsonValue>) -> Result<()> {
    match (name, expr) {
        (
            "AND",
            Expr::Binary {
                left,
                op: BinaryOp::And,
                right,
            },
        ) => {
            collect_logical_operands(name, left, out)?;
            collect_logical_operands(name, right, out)?;
            Ok(())
        }
        (
            "OR",
            Expr::Binary {
                left,
                op: BinaryOp::Or,
                right,
            },
        ) => {
            collect_logical_operands(name, left, out)?;
            collect_logical_operands(name, right, out)?;
            Ok(())
        }
        _ => {
            out.push(expr_to_filter_json(expr)?);
            Ok(())
        }
    }
}

fn field_predicate_to_json(left: &Expr, op: &BinaryOp, right: &Expr) -> Result<JsonValue> {
    let field = match left {
        Expr::Column(name) => strip_column_qualifier(name),
        other => {
            return Err(Error::InvalidQuery(format!(
                "query cannot be serialized to engine JSON: unsupported field operand {:?}",
                other
            )));
        }
    };

    let (operator, value) = match op {
        BinaryOp::Eq => (None, expr_value_to_json(right)?),
        BinaryOp::Ne => (Some("ne"), expr_value_to_json(right)?),
        BinaryOp::Lt => (Some("lt"), expr_value_to_json(right)?),
        BinaryOp::Le => (Some("lte"), expr_value_to_json(right)?),
        BinaryOp::Gt => (Some("gt"), expr_value_to_json(right)?),
        BinaryOp::Ge => (Some("gte"), expr_value_to_json(right)?),
        BinaryOp::Like => like_operator_and_value(right)?,
        BinaryOp::In => (Some("in"), list_expr_to_json_array(right)?),
        BinaryOp::NotIn => (Some("notIn"), list_expr_to_json_array(right)?),
        other => {
            return Err(Error::InvalidQuery(format!(
                "query cannot be serialized to engine JSON: unsupported binary op {:?}",
                other
            )));
        }
    };

    let mut result = JsonMap::new();
    match operator {
        None => {
            result.insert(field, value);
        }
        Some(op_name) => {
            let mut operators = JsonMap::new();
            operators.insert(op_name.to_string(), value);
            result.insert(field, JsonValue::Object(operators));
        }
    }

    Ok(JsonValue::Object(result))
}

fn null_predicate_to_json(inner: &Expr, is_null: bool) -> Result<JsonValue> {
    let field = match inner {
        Expr::Column(name) => strip_column_qualifier(name),
        other => {
            return Err(Error::InvalidQuery(format!(
                "query cannot be serialized to engine JSON: unsupported null predicate {:?}",
                other
            )));
        }
    };

    let mut operators = JsonMap::new();
    operators.insert("isNull".to_string(), JsonValue::Bool(is_null));

    let mut result = JsonMap::new();
    result.insert(field, JsonValue::Object(operators));
    Ok(JsonValue::Object(result))
}

fn like_operator_and_value(expr: &Expr) -> Result<(Option<&'static str>, JsonValue)> {
    let value = match expr {
        Expr::Param(value) => value.to_json_plain(),
        other => {
            return Err(Error::InvalidQuery(format!(
                "query cannot be serialized to engine JSON: unsupported LIKE operand {:?}",
                other
            )));
        }
    };

    let Some(pattern) = value.as_str() else {
        return Ok((Some("like"), value));
    };

    if pattern.starts_with('%') && pattern.ends_with('%') && pattern.len() >= 2 {
        return Ok((
            Some("contains"),
            JsonValue::String(pattern[1..pattern.len() - 1].to_string()),
        ));
    }

    if let Some(stripped) = pattern.strip_prefix('%') {
        return Ok((Some("endsWith"), JsonValue::String(stripped.to_string())));
    }

    if let Some(stripped) = pattern.strip_suffix('%') {
        return Ok((Some("startsWith"), JsonValue::String(stripped.to_string())));
    }

    Ok((Some("like"), JsonValue::String(pattern.to_string())))
}

fn list_expr_to_json_array(expr: &Expr) -> Result<JsonValue> {
    let Expr::List(items) = expr else {
        return Err(Error::InvalidQuery(format!(
            "query cannot be serialized to engine JSON: unsupported list operand {:?}",
            expr
        )));
    };

    Ok(JsonValue::Array(
        items
            .iter()
            .map(expr_value_to_json)
            .collect::<Result<Vec<_>>>()?,
    ))
}

fn expr_value_to_json(expr: &Expr) -> Result<JsonValue> {
    match expr {
        Expr::Param(value) => Ok(value.to_json_plain()),
        other => Err(Error::InvalidQuery(format!(
            "query cannot be serialized to engine JSON: unsupported value expression {:?}",
            other
        ))),
    }
}

fn strip_column_qualifier(name: &str) -> String {
    name.split_once("__")
        .map(|(_, column)| column.to_string())
        .unwrap_or_else(|| name.to_string())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use serde_json::json;

    use super::*;
    use crate::{Column, Value, VectorMetric, VectorNearest};

    #[test]
    fn find_many_args_serializes_supported_filters_and_includes() {
        let args = FindManyArgs {
            where_: Some(
                Column::<String>::new("Entry", "slug")
                    .contains("rust-entry-")
                    .and(Expr::column("Entry__id").gt(Expr::param(2))),
            ),
            order_by: vec![Column::<i32>::new("Entry", "id").asc()],
            take: Some(2),
            skip: Some(1),
            include: HashMap::from([(
                "author".to_string(),
                IncludeRelation::with_filter(
                    Column::<String>::new("User", "email").eq("a@example.com"),
                )
                .with_order_by(Column::<i32>::new("User", "id").desc())
                .with_take(1)
                .with_include("posts", IncludeRelation::plain()),
            )]),
            select: HashMap::from([("id".to_string(), true), ("slug".to_string(), true)]),
            cursor: Some(HashMap::from([("id".to_string(), Value::I32(10))])),
            distinct: vec!["Entry__slug".to_string()],
            nearest: Some(VectorNearest {
                field: "Entry__embedding".to_string(),
                query: vec![1.0, 2.0, 3.0],
                metric: VectorMetric::Cosine,
            }),
        };

        let json = find_many_args_to_protocol_json(&args).expect("serialization should succeed");

        assert_eq!(
            json,
            json!({
                "where": {
                    "AND": [
                        { "slug": { "contains": "rust-entry-" } },
                        { "id": { "gt": 2 } }
                    ]
                },
                "orderBy": [{ "id": "asc" }],
                "take": 2,
                "skip": 1,
                "include": {
                    "author": {
                        "where": { "email": "a@example.com" },
                        "orderBy": [{ "id": "desc" }],
                        "take": 1,
                        "include": {
                            "posts": {}
                        }
                    }
                },
                "select": {
                    "id": true,
                    "slug": true
                },
                "cursor": {
                    "id": 10
                },
                "distinct": ["slug"],
                "nearest": {
                    "field": "embedding",
                    "query": [1.0, 2.0, 3.0],
                    "metric": "cosine"
                }
            })
        );
    }

    #[test]
    fn unsupported_expression_returns_invalid_query() {
        let args = FindManyArgs {
            where_: Some(Expr::exists(
                crate::Select::from_table("Post")
                    .filter(Expr::column("Post__user_id").eq(Expr::column("User__id")))
                    .build()
                    .expect("valid select"),
            )),
            ..Default::default()
        };

        let err = find_many_args_to_protocol_json(&args).expect_err("exists is not serializable");
        assert!(matches!(err, Error::InvalidQuery(_)));
    }

    #[test]
    fn relation_predicates_serialize_to_relation_where_objects() {
        let args = FindManyArgs {
            where_: Some(
                Expr::relation_some(
                    "posts",
                    "User",
                    "Post",
                    "user_id",
                    "id",
                    crate::Column::<String>::new("Post", "title")
                        .contains("rust")
                        .and(Expr::relation_none(
                            "comments",
                            "Post",
                            "Comment",
                            "post_id",
                            "id",
                            crate::Column::<bool>::new("Comment", "flagged").eq(false),
                        )),
                )
                .and(Expr::relation_every(
                    "posts",
                    "User",
                    "Post",
                    "user_id",
                    "id",
                    crate::Column::<bool>::new("Post", "published").eq(true),
                )),
            ),
            ..Default::default()
        };

        let json =
            find_many_args_to_protocol_json(&args).expect("relation filters should serialize");

        assert_eq!(
            json,
            json!({
                "where": {
                    "AND": [
                        {
                            "posts": {
                                "some": {
                                    "AND": [
                                        { "title": { "contains": "rust" } },
                                        { "comments": { "none": { "flagged": false } } }
                                    ]
                                }
                            }
                        },
                        {
                            "posts": {
                                "every": {
                                    "published": true
                                }
                            }
                        }
                    ]
                }
            })
        );
    }
}
