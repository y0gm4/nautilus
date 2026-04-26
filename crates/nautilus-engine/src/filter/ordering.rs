use super::where_filter::{combine_conditions, parse_field_operators};
use super::*;

pub(super) fn parse_order_by(
    order_value: &JsonValue,
    field_types: Option<&FieldTypeMap>,
) -> Result<Vec<OrderBy>, ProtocolError> {
    let order_array = order_value
        .as_array()
        .ok_or_else(|| ProtocolError::InvalidFilter("orderBy must be an array".to_string()))?;

    let mut result = Vec::new();

    for item in order_array {
        let obj = item.as_object().ok_or_else(|| {
            ProtocolError::InvalidFilter("orderBy items must be objects".to_string())
        })?;

        for (field, direction) in obj {
            let dir_str = direction.as_str().ok_or_else(|| {
                ProtocolError::InvalidFilter("orderBy direction must be a string".to_string())
            })?;

            let dir = match dir_str.to_lowercase().as_str() {
                "asc" => OrderDir::Asc,
                "desc" => OrderDir::Desc,
                _ => {
                    return Err(ProtocolError::InvalidFilter(format!(
                        "Invalid order direction: {}",
                        dir_str
                    )));
                }
            };

            if field_types
                .and_then(|types| types.get(field))
                .is_some_and(|field_type| {
                    matches!(
                        field_type,
                        ResolvedFieldType::Scalar(ScalarType::Vector { .. })
                    )
                })
            {
                return Err(ProtocolError::InvalidFilter(format!(
                    "Vector field '{}' cannot be used with classic orderBy; use a vector similarity search API instead",
                    field
                )));
            }

            result.push(OrderBy {
                column: field.clone(),
                direction: dir,
            });
        }
    }

    Ok(result)
}

pub(crate) enum GroupByOrderItem {
    Column(OrderBy),
    Expr(Expr, OrderDir),
}

pub(crate) fn parse_group_by_order_by(
    order_value: &JsonValue,
    table: &str,
    logical_to_db: &HashMap<String, String>,
) -> Result<Vec<GroupByOrderItem>, ProtocolError> {
    let order_array = order_value
        .as_array()
        .ok_or_else(|| ProtocolError::InvalidFilter("orderBy must be an array".to_string()))?;

    let mut orders = Vec::new();

    for item in order_array {
        let obj = item.as_object().ok_or_else(|| {
            ProtocolError::InvalidFilter("orderBy items must be objects".to_string())
        })?;

        for (key, value) in obj {
            match key.as_str() {
                "_count" | "_avg" | "_sum" | "_min" | "_max" => {
                    let agg_fn = match key.as_str() {
                        "_count" => "COUNT",
                        "_avg" => "AVG",
                        "_sum" => "SUM",
                        "_min" => "MIN",
                        _ => "MAX",
                    };
                    let inner = value.as_object().ok_or_else(|| {
                        ProtocolError::InvalidFilter(format!(
                            "{} orderBy value must be an object",
                            key
                        ))
                    })?;
                    for (field, dir_val) in inner {
                        let dir_str = dir_val.as_str().ok_or_else(|| {
                            ProtocolError::InvalidFilter(
                                "orderBy direction must be a string".to_string(),
                            )
                        })?;
                        let dir = parse_order_dir(dir_str)?;
                        let agg_arg = if field == "_all" {
                            Expr::Star
                        } else {
                            let db_col = logical_to_db
                                .get(field.as_str())
                                .cloned()
                                .unwrap_or_else(|| field.clone());
                            Expr::Column(format!("{}__{}", table, db_col))
                        };
                        let agg_expr = Expr::function_call(agg_fn, vec![agg_arg]);
                        orders.push(GroupByOrderItem::Expr(agg_expr, dir));
                    }
                }
                _ => {
                    let dir_str = value.as_str().ok_or_else(|| {
                        ProtocolError::InvalidFilter(
                            "orderBy direction must be a string".to_string(),
                        )
                    })?;
                    let dir = parse_order_dir(dir_str)?;
                    let db_col = logical_to_db
                        .get(key.as_str())
                        .cloned()
                        .unwrap_or_else(|| key.clone());
                    let qualified = format!("{}__{}", table, db_col);
                    orders.push(GroupByOrderItem::Column(OrderBy::new(qualified, dir)));
                }
            }
        }
    }

    Ok(orders)
}

pub(super) fn parse_order_dir(s: &str) -> Result<OrderDir, ProtocolError> {
    match s.to_lowercase().as_str() {
        "asc" => Ok(OrderDir::Asc),
        "desc" => Ok(OrderDir::Desc),
        _ => Err(ProtocolError::InvalidFilter(format!(
            "Invalid order direction: {}",
            s
        ))),
    }
}

pub(crate) fn parse_having(
    having_value: &JsonValue,
    table: &str,
    logical_to_db: &HashMap<String, String>,
) -> Result<Expr, ProtocolError> {
    let obj = having_value
        .as_object()
        .ok_or_else(|| ProtocolError::InvalidFilter("having must be an object".to_string()))?;

    let mut conditions = Vec::new();

    for (agg_key, fields_val) in obj {
        let agg_fn = match agg_key.as_str() {
            "_count" => "COUNT",
            "_avg" => "AVG",
            "_sum" => "SUM",
            "_min" => "MIN",
            "_max" => "MAX",
            other => {
                return Err(ProtocolError::InvalidFilter(format!(
                    "Unknown having aggregate key: {}",
                    other
                )));
            }
        };

        let fields_obj = fields_val.as_object().ok_or_else(|| {
            ProtocolError::InvalidFilter(format!("having.{} must be an object", agg_key))
        })?;

        for (field, filter_val) in fields_obj {
            let agg_arg = if field == "_all" {
                Expr::Star
            } else {
                let db_col = logical_to_db
                    .get(field.as_str())
                    .cloned()
                    .unwrap_or_else(|| field.clone());
                Expr::Column(format!("{}__{}", table, db_col))
            };
            let agg_expr = Expr::function_call(agg_fn, vec![agg_arg]);

            let filter_obj = filter_val.as_object().ok_or_else(|| {
                ProtocolError::InvalidFilter(format!(
                    "having.{}.{} must be an operator object",
                    agg_key, field
                ))
            })?;
            let cond = parse_field_operators(agg_expr, filter_obj, None)?;
            conditions.push(cond);
        }
    }

    combine_conditions(conditions, BinaryOp::And)
}

pub(super) fn parse_int(value: &JsonValue, field_name: &str) -> Result<u32, ProtocolError> {
    value
        .as_u64()
        .and_then(|n| u32::try_from(n).ok())
        .ok_or_else(|| {
            ProtocolError::InvalidParams(format!("{} must be a non-negative integer", field_name))
        })
}

pub(super) fn parse_signed_int(value: &JsonValue, field_name: &str) -> Result<i64, ProtocolError> {
    value
        .as_i64()
        .ok_or_else(|| ProtocolError::InvalidParams(format!("{} must be an integer", field_name)))
}
