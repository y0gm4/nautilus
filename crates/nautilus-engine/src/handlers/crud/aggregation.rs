use super::common::wrap_data_result;
use super::*;

fn collect_agg_fields(model: &ModelIr, value: &serde_json::Value) -> Vec<String> {
    if value.as_bool() == Some(true) {
        model
            .scalar_fields()
            .map(|field| field.logical_name.clone())
            .collect()
    } else if let Some(obj) = value.as_object() {
        obj.iter()
            .filter(|(_, flag)| flag.as_bool() == Some(true))
            .map(|(field, _)| field.clone())
            .collect()
    } else {
        vec![]
    }
}

pub(super) async fn execute_group_by_rows(
    state: &EngineState,
    params: GroupByParams,
) -> Result<Vec<Row>, ProtocolError> {
    use nautilus_core::ColumnMarker;

    check_protocol_version(params.protocol_version)?;
    let tx_id = params.transaction_id;

    let model = get_model_or_error(state, &params.model)?;
    let metadata = state.model_metadata(model);
    let field_type_map = metadata.field_types();
    let logical_to_db = metadata.logical_to_db();
    let db_to_logical = metadata.db_to_logical();
    let args = params.args.as_ref();

    let by_fields: Vec<String> = args
        .and_then(|value| value.get("by"))
        .and_then(|value| value.as_array())
        .map(|array| {
            array
                .iter()
                .filter_map(|value| value.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default();

    if by_fields.is_empty() {
        return Err(ProtocolError::InvalidParams(
            "groupBy requires at least one field in `by`".to_string(),
        ));
    }

    let qualified_filter = args
        .and_then(|value| value.get("where"))
        .map(|where_val| {
            crate::filter::parse_where_filter(
                where_val,
                &crate::filter::RelationMap::new(),
                field_type_map,
                crate::filter::SchemaContext::none(),
            )
            .map(|expr| qualify_filter_columns(expr, &model.db_name, logical_to_db))
        })
        .transpose()?;

    let having_expr = args
        .and_then(|value| value.get("having"))
        .map(|value| parse_having(value, &model.db_name, logical_to_db))
        .transpose()?;

    let take_val = args
        .and_then(|value| value.get("take"))
        .and_then(|value| value.as_i64())
        .map(|value| value as i32);
    let skip_val = args
        .and_then(|value| value.get("skip"))
        .and_then(|value| value.as_u64())
        .map(|value| value as u32);

    let mut aggregate_items: Vec<(String, Expr)> = Vec::new();

    if let Some(count_val) = args.and_then(|value| value.get("count")) {
        if count_val.as_bool() == Some(true) {
            aggregate_items.push((
                "_count___all".to_string(),
                Expr::function_call("COUNT", vec![Expr::Star]),
            ));
        } else if let Some(obj) = count_val.as_object() {
            for (field, flag) in obj {
                if flag.as_bool() != Some(true) {
                    continue;
                }
                if field == "_all" {
                    aggregate_items.push((
                        "_count___all".to_string(),
                        Expr::function_call("COUNT", vec![Expr::Star]),
                    ));
                } else {
                    let db_col = logical_to_db
                        .get(field.as_str())
                        .cloned()
                        .unwrap_or_else(|| field.clone());
                    aggregate_items.push((
                        format!("_count__{}", field),
                        Expr::function_call(
                            "COUNT",
                            vec![Expr::Column(format!("{}__{}", model.db_name, db_col))],
                        ),
                    ));
                }
            }
        }
    }

    for (agg_key, agg_fn) in [
        ("avg", "AVG"),
        ("sum", "SUM"),
        ("min", "MIN"),
        ("max", "MAX"),
    ] {
        if let Some(agg_val) = args.and_then(|value| value.get(agg_key)) {
            for field in collect_agg_fields(model, agg_val) {
                let db_col = logical_to_db
                    .get(field.as_str())
                    .cloned()
                    .unwrap_or_else(|| field.clone());
                aggregate_items.push((
                    format!("_{}_{}", agg_key, field),
                    Expr::function_call(
                        agg_fn,
                        vec![Expr::Column(format!("{}__{}", model.db_name, db_col))],
                    ),
                ));
            }
        }
    }

    let group_orders = args
        .and_then(|value| value.get("orderBy"))
        .map(|value| parse_group_by_order_by(value, &model.db_name, logical_to_db))
        .transpose()?
        .unwrap_or_default();

    let order_by_columns = group_orders
        .iter()
        .filter(|order| matches!(order, crate::filter::GroupByOrderItem::Column(_)))
        .count();
    let order_by_exprs = group_orders.len() - order_by_columns;
    let mut builder = Select::from_table(&model.db_name).with_capacity(SelectCapacity {
        items: by_fields.len() + aggregate_items.len(),
        order_by_columns,
        order_by_exprs,
        group_by: by_fields.len(),
        ..SelectCapacity::default()
    });

    for field_name in &by_fields {
        let db_col = logical_to_db
            .get(field_name.as_str())
            .cloned()
            .unwrap_or_else(|| field_name.clone());
        let marker = ColumnMarker::new(&model.db_name, &db_col);
        builder = builder.item(SelectItem::from(marker.clone()));
        builder = builder.group_by_column(marker);
    }

    for (alias, expr) in aggregate_items {
        builder = builder.item(SelectItem::computed(expr, alias));
    }

    if let Some(filter) = qualified_filter {
        builder = builder.filter(filter);
    }

    if let Some(having) = having_expr {
        builder = builder.having(having);
    }

    for order in group_orders {
        builder = match order {
            crate::filter::GroupByOrderItem::Column(order) => {
                builder.order_by(order.column, order.direction)
            }
            crate::filter::GroupByOrderItem::Expr(expr, dir) => builder.order_by_expr(expr, dir),
        };
    }

    if let Some(value) = take_val {
        builder = builder.take(value);
    }
    if let Some(value) = skip_val {
        builder = builder.skip(value);
    }

    let select = builder.build().map_err(|e| {
        ProtocolError::QueryPlanning(format!("Failed to build groupBy query: {}", e))
    })?;
    let sql = state.dialect.render_select_owned(select).map_err(|e| {
        ProtocolError::QueryPlanning(format!("Failed to render groupBy query: {}", e))
    })?;

    let rows = state
        .execute_query_on(&sql, "GroupBy", tx_id.as_deref())
        .await?;

    let mut shaped_rows = Vec::with_capacity(rows.len());
    for row in rows {
        let mut columns = Vec::with_capacity(row.len());
        let mut count_map = serde_json::Map::new();
        let mut avg_map = serde_json::Map::new();
        let mut sum_map = serde_json::Map::new();
        let mut min_map = serde_json::Map::new();
        let mut max_map = serde_json::Map::new();

        for (col_name, value) in row.into_columns() {
            if let Some(rest) = col_name.strip_prefix("_count__") {
                let key = if rest == "_all" { "_all" } else { rest };
                count_map.insert(key.to_string(), value.to_json_plain());
            } else if let Some(rest) = col_name.strip_prefix("_avg_") {
                avg_map.insert(rest.to_string(), value.to_json_plain());
            } else if let Some(rest) = col_name.strip_prefix("_sum_") {
                sum_map.insert(rest.to_string(), value.to_json_plain());
            } else if let Some(rest) = col_name.strip_prefix("_min_") {
                min_map.insert(rest.to_string(), value.to_json_plain());
            } else if let Some(rest) = col_name.strip_prefix("_max_") {
                max_map.insert(rest.to_string(), value.to_json_plain());
            } else {
                let field_key = col_name
                    .split_once("__")
                    .map(|(_, col_part)| col_part)
                    .unwrap_or(col_name.as_str());
                let field_key = db_to_logical
                    .get(field_key)
                    .cloned()
                    .unwrap_or_else(|| field_key.to_string());
                columns.push((field_key, value));
            }
        }

        if !count_map.is_empty() {
            columns.push((
                "_count".to_string(),
                Value::Json(JsonValue::Object(count_map)),
            ));
        }
        if !avg_map.is_empty() {
            columns.push(("_avg".to_string(), Value::Json(JsonValue::Object(avg_map))));
        }
        if !sum_map.is_empty() {
            columns.push(("_sum".to_string(), Value::Json(JsonValue::Object(sum_map))));
        }
        if !min_map.is_empty() {
            columns.push(("_min".to_string(), Value::Json(JsonValue::Object(min_map))));
        }
        if !max_map.is_empty() {
            columns.push(("_max".to_string(), Value::Json(JsonValue::Object(max_map))));
        }

        shaped_rows.push(Row::new(columns));
    }

    Ok(shaped_rows)
}

/// Handle `query.groupBy`.
pub(super) async fn handle_group_by(
    state: &EngineState,
    request: RpcRequest,
) -> Result<Box<serde_json::value::RawValue>, ProtocolError> {
    let params: GroupByParams = serde_json::from_value(request.params)
        .map_err(|e| ProtocolError::InvalidParams(format!("Invalid groupBy params: {}", e)))?;
    let rows = execute_group_by_rows(state, params).await?;
    wrap_data_result(&rows, "groupBy result")
}

pub(super) async fn handle_group_by_embedded(
    state: &EngineState,
    request: RpcRequest,
) -> Result<Vec<Row>, ProtocolError> {
    let params: GroupByParams = serde_json::from_value(request.params)
        .map_err(|e| ProtocolError::InvalidParams(format!("Invalid groupBy params: {}", e)))?;
    execute_group_by_rows(state, params).await
}
