use super::context::relation_filter_context;
use super::*;

pub(crate) fn parse_where_filter(
    where_value: &JsonValue,
    relations: &RelationMap,
    field_types: &FieldTypeMap,
    models: Option<&HashMap<String, ModelIr>>,
) -> Result<Expr, ProtocolError> {
    let where_obj = where_value
        .as_object()
        .ok_or_else(|| ProtocolError::InvalidFilter("where must be an object".to_string()))?;

    if where_obj.is_empty() {
        return Err(ProtocolError::InvalidFilter(
            "where cannot be empty".to_string(),
        ));
    }

    parse_filter_object(where_obj, relations, field_types, models)
}

pub(super) fn parse_filter_object(
    obj: &serde_json::Map<String, JsonValue>,
    relations: &RelationMap,
    field_types: &FieldTypeMap,
    models: Option<&HashMap<String, ModelIr>>,
) -> Result<Expr, ProtocolError> {
    let mut conditions = Vec::new();

    if let Some(and_value) = obj.get("AND") {
        conditions.push(parse_and_operator(
            and_value,
            relations,
            field_types,
            models,
        )?);
    }
    if let Some(or_value) = obj.get("OR") {
        conditions.push(parse_or_operator(or_value, relations, field_types, models)?);
    }
    if let Some(not_value) = obj.get("NOT") {
        conditions.push(parse_not_operator(
            not_value,
            relations,
            field_types,
            models,
        )?);
    }

    for (field, value) in obj {
        if matches!(field.as_str(), "AND" | "OR" | "NOT") {
            continue;
        }
        let condition = if let Some(rel_info) = relations.get(field) {
            parse_relation_filter(rel_info, value, models)?
        } else {
            parse_field_condition(field, value, field_types)?
        };
        conditions.push(condition);
    }

    if conditions.is_empty() {
        return Err(ProtocolError::InvalidFilter(
            "No valid conditions found".to_string(),
        ));
    }

    combine_conditions(conditions, BinaryOp::And)
}

pub(super) fn parse_relation_filter(
    rel: &RelationInfo,
    value: &JsonValue,
    models: Option<&HashMap<String, ModelIr>>,
) -> Result<Expr, ProtocolError> {
    let spec = value.as_object().ok_or_else(|| {
        ProtocolError::InvalidFilter(
            "Relation filter must be an object with some/none/every".to_string(),
        )
    })?;

    let mut conditions = Vec::new();
    let empty_map = RelationMap::new();
    let empty_field_types = FieldTypeMap::new();
    let child_ctx = relation_filter_context(rel, models)?;

    let join_cond = || {
        Expr::column(format!("{}__{}", rel.target_table, rel.fk_db))
            .eq(Expr::column(format!("{}__{}", rel.parent_table, rel.pk_db)))
    };

    let parse_child_filter = |child_value: &JsonValue| -> Result<Expr, ProtocolError> {
        if let Some((child_relations, child_field_types, child_logical_to_db)) = &child_ctx {
            let parsed =
                parse_where_filter(child_value, child_relations, child_field_types, models)?;
            Ok(qualify_filter_columns(
                parsed,
                &rel.target_table,
                child_logical_to_db,
            ))
        } else {
            parse_where_filter(child_value, &empty_map, &empty_field_types, None)
        }
    };

    if let Some(some_val) = spec.get("some") {
        let child_filter = parse_child_filter(some_val)?;
        let subquery = Select::from_table(&rel.target_table)
            .filter(join_cond().and(child_filter))
            .build()
            .map_err(|e| ProtocolError::QueryPlanning(e.to_string()))?;
        conditions.push(Expr::exists(subquery));
    }

    if let Some(none_val) = spec.get("none") {
        let child_filter = parse_child_filter(none_val)?;
        let subquery = Select::from_table(&rel.target_table)
            .filter(join_cond().and(child_filter))
            .build()
            .map_err(|e| ProtocolError::QueryPlanning(e.to_string()))?;
        conditions.push(Expr::not_exists(subquery));
    }

    if let Some(every_val) = spec.get("every") {
        let child_filter = parse_child_filter(every_val)?;
        let subquery = Select::from_table(&rel.target_table)
            .filter(join_cond().and(!child_filter))
            .build()
            .map_err(|e| ProtocolError::QueryPlanning(e.to_string()))?;
        conditions.push(Expr::not_exists(subquery));
    }

    if conditions.is_empty() {
        return Err(ProtocolError::InvalidFilter(
            "Relation filter must have at least one of: some, none, every".to_string(),
        ));
    }

    combine_conditions(conditions, BinaryOp::And)
}

pub(super) fn parse_field_condition(
    field: &str,
    value: &JsonValue,
    field_types: &FieldTypeMap,
) -> Result<Expr, ProtocolError> {
    let field_expr = Expr::Column(field.to_string());
    let field_type = field_types.get(field);

    if let Some(operator_obj) = value.as_object() {
        parse_field_operators(field_expr, operator_obj, field_type)
    } else {
        let converted = match field_type {
            Some(ft) => json_to_value_field(value, ft)?,
            None => json_to_value(value)?,
        };
        let value_expr = Expr::Param(converted);
        Ok(field_expr.eq(value_expr))
    }
}

pub(super) fn parse_field_operators(
    field_expr: Expr,
    operators: &serde_json::Map<String, JsonValue>,
    field_type: Option<&ResolvedFieldType>,
) -> Result<Expr, ProtocolError> {
    let mut conditions = Vec::new();

    let convert = |v: &JsonValue| -> Result<Value, ProtocolError> {
        match field_type {
            Some(ft) => json_to_value_field(v, ft),
            None => json_to_value(v),
        }
    };

    for (op, value) in operators {
        if is_vector_field(field_type)
            && !matches!(op.as_str(), "eq" | "ne" | "not" | "isNull" | "isNotNull")
        {
            return Err(ProtocolError::InvalidFilter(format!(
                "Operator '{}' is not supported for Vector fields; use equality/null filters or vector similarity search",
                op
            )));
        }

        let condition = match op.as_str() {
            "eq" => {
                let val = Expr::Param(convert(value)?);
                field_expr.clone().eq(val)
            }
            "ne" | "not" => {
                let val = Expr::Param(convert(value)?);
                field_expr.clone().ne(val)
            }
            "gt" => {
                let val = Expr::Param(convert(value)?);
                field_expr.clone().gt(val)
            }
            "gte" => {
                let val = Expr::Param(convert(value)?);
                field_expr.clone().ge(val)
            }
            "lt" => {
                let val = Expr::Param(convert(value)?);
                field_expr.clone().lt(val)
            }
            "lte" => {
                let val = Expr::Param(convert(value)?);
                field_expr.clone().le(val)
            }
            "contains" => {
                let s = value.as_str().ok_or_else(|| {
                    ProtocolError::InvalidFilter("contains value must be a string".to_string())
                })?;
                let pattern = format!("%{}%", s);
                field_expr.clone().like(Expr::Param(Value::String(pattern)))
            }
            "startsWith" => {
                let s = value.as_str().ok_or_else(|| {
                    ProtocolError::InvalidFilter("startsWith value must be a string".to_string())
                })?;
                let pattern = format!("{}%", s);
                field_expr.clone().like(Expr::Param(Value::String(pattern)))
            }
            "endsWith" => {
                let s = value.as_str().ok_or_else(|| {
                    ProtocolError::InvalidFilter("endsWith value must be a string".to_string())
                })?;
                let pattern = format!("%{}", s);
                field_expr.clone().like(Expr::Param(Value::String(pattern)))
            }
            "like" => {
                let pattern = value
                    .as_str()
                    .ok_or_else(|| {
                        ProtocolError::InvalidFilter("like value must be a string".to_string())
                    })?
                    .to_string();
                field_expr.clone().like(Expr::Param(Value::String(pattern)))
            }
            "isNull" => {
                let is_null = value.as_bool().ok_or_else(|| {
                    ProtocolError::InvalidFilter("isNull value must be a boolean".to_string())
                })?;
                if is_null {
                    field_expr.clone().is_null()
                } else {
                    field_expr.clone().is_not_null()
                }
            }
            "isNotNull" => {
                let is_not_null = value.as_bool().ok_or_else(|| {
                    ProtocolError::InvalidFilter("isNotNull value must be a boolean".to_string())
                })?;
                if is_not_null {
                    field_expr.clone().is_not_null()
                } else {
                    field_expr.clone().is_null()
                }
            }
            "in" => {
                let arr = value.as_array().ok_or_else(|| {
                    ProtocolError::InvalidFilter("in value must be an array".to_string())
                })?;
                let exprs: Result<Vec<Expr>, _> =
                    arr.iter().map(|v| convert(v).map(Expr::Param)).collect();
                field_expr.clone().in_list(exprs?)
            }
            "notIn" => {
                let arr = value.as_array().ok_or_else(|| {
                    ProtocolError::InvalidFilter("notIn value must be an array".to_string())
                })?;
                let exprs: Result<Vec<Expr>, _> =
                    arr.iter().map(|v| convert(v).map(Expr::Param)).collect();
                field_expr.clone().not_in_list(exprs?)
            }
            _ => {
                return Err(ProtocolError::InvalidFilter(format!(
                    "Unknown operator: {}",
                    op
                )));
            }
        };

        conditions.push(condition);
    }

    if conditions.is_empty() {
        return Err(ProtocolError::InvalidFilter(
            "No valid operators found".to_string(),
        ));
    }

    combine_conditions(conditions, BinaryOp::And)
}

fn is_vector_field(field_type: Option<&ResolvedFieldType>) -> bool {
    matches!(field_type, Some(ResolvedFieldType::Scalar(s)) if s.is_vector())
}

pub(super) fn parse_and_operator(
    value: &JsonValue,
    relations: &RelationMap,
    field_types: &FieldTypeMap,
    models: Option<&HashMap<String, ModelIr>>,
) -> Result<Expr, ProtocolError> {
    let arr = value
        .as_array()
        .ok_or_else(|| ProtocolError::InvalidFilter("AND must be an array".to_string()))?;

    if arr.is_empty() {
        return Err(ProtocolError::InvalidFilter(
            "AND array cannot be empty".to_string(),
        ));
    }

    let mut conditions = Vec::new();
    for item in arr {
        let obj = item.as_object().ok_or_else(|| {
            ProtocolError::InvalidFilter("AND array items must be objects".to_string())
        })?;
        conditions.push(parse_filter_object(obj, relations, field_types, models)?);
    }

    combine_conditions(conditions, BinaryOp::And)
}

pub(super) fn parse_or_operator(
    value: &JsonValue,
    relations: &RelationMap,
    field_types: &FieldTypeMap,
    models: Option<&HashMap<String, ModelIr>>,
) -> Result<Expr, ProtocolError> {
    let arr = value
        .as_array()
        .ok_or_else(|| ProtocolError::InvalidFilter("OR must be an array".to_string()))?;

    if arr.is_empty() {
        return Err(ProtocolError::InvalidFilter(
            "OR array cannot be empty".to_string(),
        ));
    }

    let mut conditions = Vec::new();
    for item in arr {
        let obj = item.as_object().ok_or_else(|| {
            ProtocolError::InvalidFilter("OR array items must be objects".to_string())
        })?;
        conditions.push(parse_filter_object(obj, relations, field_types, models)?);
    }

    combine_conditions(conditions, BinaryOp::Or)
}

pub(super) fn parse_not_operator(
    value: &JsonValue,
    relations: &RelationMap,
    field_types: &FieldTypeMap,
    models: Option<&HashMap<String, ModelIr>>,
) -> Result<Expr, ProtocolError> {
    if let Some(obj) = value.as_object() {
        let inner = parse_filter_object(obj, relations, field_types, models)?;
        Ok(!inner)
    } else if let Some(arr) = value.as_array() {
        if arr.is_empty() {
            return Err(ProtocolError::InvalidFilter(
                "NOT array cannot be empty".to_string(),
            ));
        }

        let mut conditions = Vec::new();
        for item in arr {
            let obj = item.as_object().ok_or_else(|| {
                ProtocolError::InvalidFilter("NOT array items must be objects".to_string())
            })?;
            conditions.push(parse_filter_object(obj, relations, field_types, models)?);
        }

        let combined = combine_conditions(conditions, BinaryOp::And)?;
        Ok(!combined)
    } else {
        Err(ProtocolError::InvalidFilter(
            "NOT must be an object or array".to_string(),
        ))
    }
}

pub(super) fn combine_conditions(
    conditions: Vec<Expr>,
    op: BinaryOp,
) -> Result<Expr, ProtocolError> {
    if conditions.is_empty() {
        return Err(ProtocolError::InvalidFilter(
            "Cannot combine empty conditions".to_string(),
        ));
    }

    if conditions.len() == 1 {
        Ok(conditions.into_iter().next().unwrap())
    } else {
        let mut result = conditions[0].clone();
        for cond in &conditions[1..] {
            result = match op {
                BinaryOp::And => result.and(cond.clone()),
                BinaryOp::Or => result.or(cond.clone()),
                _ => unreachable!("combine_conditions only supports And/Or"),
            };
        }
        Ok(result)
    }
}

pub(crate) fn qualify_filter_columns(
    expr: Expr,
    table: &str,
    logical_to_db: &HashMap<String, String>,
) -> Expr {
    match expr {
        Expr::Column(name) if !name.contains("__") => {
            let db_col = logical_to_db
                .get(&name)
                .cloned()
                .unwrap_or_else(|| name.clone());
            Expr::Column(format!("{}__{}", table, db_col))
        }
        Expr::Binary { left, op, right } => Expr::Binary {
            left: Box::new(qualify_filter_columns(*left, table, logical_to_db)),
            op,
            right: Box::new(qualify_filter_columns(*right, table, logical_to_db)),
        },
        Expr::Not(inner) => Expr::Not(Box::new(qualify_filter_columns(
            *inner,
            table,
            logical_to_db,
        ))),
        Expr::IsNull(inner) => Expr::IsNull(Box::new(qualify_filter_columns(
            *inner,
            table,
            logical_to_db,
        ))),
        Expr::IsNotNull(inner) => Expr::IsNotNull(Box::new(qualify_filter_columns(
            *inner,
            table,
            logical_to_db,
        ))),
        Expr::Filter { expr, predicate } => Expr::Filter {
            expr: Box::new(qualify_filter_columns(*expr, table, logical_to_db)),
            predicate: Box::new(qualify_filter_columns(*predicate, table, logical_to_db)),
        },
        Expr::FunctionCall { name, args } => Expr::FunctionCall {
            name,
            args: args
                .into_iter()
                .map(|a| qualify_filter_columns(a, table, logical_to_db))
                .collect(),
        },
        other => other,
    }
}
