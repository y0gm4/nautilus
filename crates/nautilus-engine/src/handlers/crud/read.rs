use super::common::{
    parse_and_qualify_model_filter, qualify_model_filter, wrap_data_result, wrap_result,
};
use super::*;

fn include_alias(field_name: &str) -> String {
    format!("{}_json", field_name)
}

fn row_to_json_value(model: &ModelIr, row: &Row) -> JsonValue {
    let mut obj = serde_json::Map::with_capacity(row.len());
    for field in model.scalar_fields() {
        let alias = format!("{}__{}", model.db_name, field.db_name);
        if let Some(value) = row.get(&alias) {
            obj.insert(field.logical_name.clone(), value.to_json_plain());
        }
    }
    for (name, value) in row.iter() {
        if name.ends_with("_json") {
            obj.insert(name.to_string(), value.to_json_plain());
        }
    }
    JsonValue::Object(obj)
}

async fn load_relation_include_value(
    state: &EngineState,
    parent_row: &Row,
    rel_info: &RelationInfo,
    include_node: &IncludeNode,
    tx_id: Option<&str>,
) -> Result<Value, ProtocolError> {
    let parent_join_column = format!("{}__{}", rel_info.parent_table, rel_info.pk_db);
    let Some(parent_value) = parent_row.get(&parent_join_column).cloned() else {
        return Ok(if rel_info.is_array {
            Value::Json(serde_json::json!([]))
        } else {
            Value::Null
        });
    };

    let target_model = state
        .models
        .get(&rel_info.target_logical_name)
        .ok_or_else(|| {
            ProtocolError::QueryPlanning(format!(
                "Model '{}' not found",
                rel_info.target_logical_name
            ))
        })?;

    let join_filter = Expr::column(format!("{}__{}", rel_info.target_table, rel_info.fk_db))
        .eq(Expr::param(parent_value));
    let filter = Some(if let Some(child_filter) = include_node.filter.clone() {
        join_filter.and(child_filter)
    } else {
        join_filter
    });

    let query_args = QueryArgs {
        filter,
        order_by: include_node.order_by.clone(),
        take: if rel_info.is_array {
            include_node.take
        } else {
            include_node.take.or(Some(1))
        },
        skip: include_node.skip,
        include: include_node.nested.clone(),
        select: std::collections::HashSet::new(),
        cursor: None,
        backward: false,
        distinct: vec![],
        nearest: None,
    };

    let child_rows = Box::pin(execute_find_many_rows(
        state,
        target_model,
        query_args,
        tx_id,
    ))
    .await?;
    if rel_info.is_array {
        Ok(Value::Json(JsonValue::Array(
            child_rows
                .iter()
                .map(|row| row_to_json_value(target_model, row))
                .collect(),
        )))
    } else {
        Ok(child_rows
            .first()
            .map(|row| Value::Json(row_to_json_value(target_model, row)))
            .unwrap_or(Value::Null))
    }
}

async fn hydrate_rows_with_includes(
    state: &EngineState,
    model: &ModelIr,
    rows: Vec<Row>,
    includes: &std::collections::HashMap<String, IncludeNode>,
    tx_id: Option<&str>,
) -> Result<Vec<Row>, ProtocolError> {
    if rows.is_empty() || includes.is_empty() {
        return Ok(rows);
    }

    let relation_map = state.relation_map_for_model(model)?;
    let mut hydrated = Vec::with_capacity(rows.len());

    for row in rows {
        let mut columns = row.columns().to_vec();
        for (field_name, include_node) in includes {
            let Some(rel_info) = relation_map.get(field_name) else {
                continue;
            };
            let include_value =
                load_relation_include_value(state, &row, rel_info, include_node, tx_id).await?;
            columns.push((include_alias(field_name), include_value));
        }
        hydrated.push(Row::new(columns));
    }

    Ok(hydrated)
}

async fn execute_find_many_rows(
    state: &EngineState,
    model: &ModelIr,
    query_args: QueryArgs,
    tx_id: Option<&str>,
) -> Result<Vec<Row>, ProtocolError> {
    let QueryArgs {
        filter,
        order_by,
        take,
        skip,
        include,
        select,
        cursor,
        backward,
        distinct,
        nearest,
    } = query_args;

    let metadata = state.model_metadata(model);
    let logical_to_db = metadata.logical_to_db();
    let qualified_filter =
        filter.map(|expr| qualify_filter_columns(expr, &model.db_name, logical_to_db));
    let pk_fields = metadata.primary_key_fields();

    let mut builder = Select::from_table(&model.db_name).with_capacity(SelectCapacity {
        items: metadata.scalar_fields().len(),
        order_by_columns: order_by.len() + distinct.len() + pk_fields.len(),
        order_by_exprs: usize::from(nearest.is_some()),
        distinct: distinct.len(),
        ..SelectCapacity::default()
    });
    let mut row_hints = Vec::new();

    for field in metadata.scalar_fields() {
        if !select.is_empty()
            && !select.contains(field.logical_name())
            && !pk_fields
                .iter()
                .any(|pk_field| pk_field.logical_name() == field.logical_name())
        {
            continue;
        }
        builder = builder.item(SelectItem::from(field.marker().clone()));
        row_hints.push(field.hint());
    }

    let combined_filter = if let Some(ref cursor_map) = cursor {
        let pk_refs: Vec<(&str, &str)> = pk_fields
            .iter()
            .map(|field| (field.logical_name(), field.qualified_column()))
            .collect();

        let cursor_pred = build_cursor_predicate(&pk_refs, cursor_map, backward)
            .map_err(|e| ProtocolError::InvalidParams(format!("Invalid cursor: {}", e)))?;

        let existing_order_cols: std::collections::HashSet<&str> =
            order_by.iter().map(|order| order.column.as_str()).collect();
        for pk_field in pk_fields {
            if !existing_order_cols.contains(pk_field.db_name()) {
                let dir = if backward {
                    OrderDir::Desc
                } else {
                    OrderDir::Asc
                };
                builder = builder.order_by(pk_field.db_name().to_string(), dir);
            }
        }

        Some(match qualified_filter {
            Some(existing) => existing.and(cursor_pred),
            None => cursor_pred,
        })
    } else {
        qualified_filter
    };

    if let Some(filter_expr) = combined_filter {
        builder = builder.filter(filter_expr);
    }

    if let Some(nearest) = nearest {
        let db_col = logical_to_db
            .get(nearest.field.as_str())
            .cloned()
            .unwrap_or(nearest.field);
        let distance_expr = Expr::vector_distance(
            nearest.metric,
            Expr::column(format!("{}__{}", model.db_name, db_col)),
            Expr::param(Value::Vector(nearest.query)),
        );
        builder = builder.order_by_expr(distance_expr, OrderDir::Asc);
    }

    if !distinct.is_empty() {
        let existing_order_cols: std::collections::HashSet<&str> =
            order_by.iter().map(|order| order.column.as_str()).collect();
        for column in &distinct {
            let db_col = logical_to_db
                .get(column.as_str())
                .cloned()
                .unwrap_or_else(|| column.clone());
            if !existing_order_cols.contains(db_col.as_str()) {
                let dir = if backward {
                    OrderDir::Desc
                } else {
                    OrderDir::Asc
                };
                builder = builder.order_by(db_col, dir);
            }
        }
    }

    for order in order_by {
        let dir = if backward {
            match order.direction {
                OrderDir::Asc => OrderDir::Desc,
                OrderDir::Desc => OrderDir::Asc,
            }
        } else {
            order.direction
        };
        let db_col = logical_to_db
            .get(&order.column)
            .cloned()
            .unwrap_or(order.column);
        builder = builder.order_by(db_col, dir);
    }

    if let Some(take) = take {
        builder = builder.take(take);
    }
    if let Some(skip) = skip {
        builder = builder.skip(skip);
    }
    if !distinct.is_empty() {
        let distinct_db: Vec<String> = distinct
            .iter()
            .map(|column| {
                logical_to_db
                    .get(column.as_str())
                    .cloned()
                    .unwrap_or_else(|| column.clone())
            })
            .collect();
        builder = builder.distinct(distinct_db);
    }

    let select = builder
        .build()
        .map_err(|e| ProtocolError::QueryPlanning(format!("Failed to build query: {}", e)))?;

    let sql = state
        .dialect
        .render_select_owned(select)
        .map_err(|e| ProtocolError::QueryPlanning(format!("Failed to render SQL: {}", e)))?;

    let mut rows = normalize_rows_with_hints(
        state.execute_query_on(&sql, "Query", tx_id).await?,
        &row_hints,
    )?;

    if backward {
        rows.reverse();
    }

    hydrate_rows_with_includes(state, model, rows, &include, tx_id).await
}

/// Handle `query.findMany`.
///
/// Builds a SELECT for the requested model, applying optional `where`, `orderBy`,
/// `take`, `skip`, `cursor`, `distinct`, `select`, and `include` arguments.
/// Relation includes are hydrated after the parent rows load so child ordering
/// and pagination execute on the related query before JSON serialization.
/// Returns `QueryResult { data: [...] }`. Supports transactional execution via `transactionId`.
pub(super) async fn handle_find_many(
    state: &EngineState,
    request: RpcRequest,
    sender: Option<mpsc::Sender<RpcResponse>>,
) -> Result<Box<serde_json::value::RawValue>, ProtocolError> {
    let params: FindManyParams = serde_json::from_value(request.params)
        .map_err(|e| ProtocolError::InvalidParams(format!("Invalid findMany params: {}", e)))?;

    check_protocol_version(params.protocol_version)?;
    let tx_id = params.transaction_id;
    let chunk_size = params.chunk_size;

    let model = get_model_or_error(state, &params.model)?;

    let metadata = state.model_metadata(model);
    let relation_map = state.relation_map_for_model(model)?;
    let query_args = QueryArgs::parse_with_context(
        params.args,
        relation_map,
        metadata.field_types(),
        crate::filter::SchemaContext::with_state(state),
    )?;
    let rows = execute_find_many_rows(state, model, query_args, tx_id.as_deref()).await?;

    if let (Some(size), Some(channel)) = (chunk_size, sender) {
        let size = size.max(1);
        let id = request.id.clone();
        let mut chunks = rows.chunks(size).peekable();

        if chunks.peek().is_some() {
            while let Some(chunk) = chunks.next() {
                let is_last = chunks.peek().is_none();
                let data_raw = rows_to_raw_json(chunk)?;
                let raw =
                    wrap_result(format!("{{\"data\":{}}}", data_raw.get()), "findMany chunk")?;
                if is_last {
                    return Ok(raw);
                }
                channel
                    .send(ok_partial(id.clone(), raw))
                    .await
                    .map_err(|_| {
                        ProtocolError::Internal(
                            "Channel closed during chunked response".to_string(),
                        )
                    })?;
            }
        }
    }

    wrap_data_result(&rows, "findMany result")
}

/// Handle `query.findFirst` and delegate to [`handle_find_many`] with `take=1`.
pub(super) async fn handle_find_first(
    state: &EngineState,
    request: RpcRequest,
) -> Result<Box<serde_json::value::RawValue>, ProtocolError> {
    let params: FindFirstParams = serde_json::from_value(request.params.clone())
        .map_err(|e| ProtocolError::InvalidParams(format!("Invalid findFirst params: {}", e)))?;

    check_protocol_version(params.protocol_version)?;

    let find_many_params = FindManyParams {
        protocol_version: params.protocol_version,
        model: params.model,
        args: params
            .args
            .map(|mut value| {
                if let serde_json::Value::Object(ref mut map) = value {
                    map.insert("take".into(), serde_json::json!(1));
                }
                value
            })
            .or_else(|| Some(serde_json::json!({ "take": 1 }))),
        transaction_id: params.transaction_id,
        chunk_size: None,
    };

    let find_many_request = RpcRequest {
        jsonrpc: "2.0".to_string(),
        id: request.id,
        method: "query.findMany".to_string(),
        params: serde_json::to_value(find_many_params)
            .map_err(|e| ProtocolError::Internal(format!("Failed to serialize params: {}", e)))?,
    };
    handle_find_many(state, find_many_request, None).await
}

/// Handle `query.findUnique`.
///
/// Builds a SELECT with the provided unique filter and `LIMIT 1`. Does not support
/// relation includes or cursor pagination.
pub(super) async fn handle_find_unique(
    state: &EngineState,
    request: RpcRequest,
) -> Result<Box<serde_json::value::RawValue>, ProtocolError> {
    let params: FindUniqueParams = serde_json::from_value(request.params)
        .map_err(|e| ProtocolError::InvalidParams(format!("Invalid findUnique params: {}", e)))?;

    check_protocol_version(params.protocol_version)?;
    let tx_id = params.transaction_id;

    let model = get_model_or_error(state, &params.model)?;
    let metadata = state.model_metadata(model);
    let qualified_filter = parse_and_qualify_model_filter(
        model,
        &params.filter,
        metadata.field_types(),
        metadata.logical_to_db(),
    )?;

    let mut builder = Select::from_table(&model.db_name).with_capacity(SelectCapacity {
        items: metadata.scalar_markers().len(),
        ..SelectCapacity::default()
    });
    for marker in metadata.scalar_markers() {
        builder = builder.item(SelectItem::from(marker.clone()));
    }

    builder = builder.filter(qualified_filter);

    let select = builder
        .take(1)
        .build()
        .map_err(|e| ProtocolError::QueryPlanning(format!("Failed to build query: {}", e)))?;

    let sql = state
        .dialect
        .render_select_owned(select)
        .map_err(|e| ProtocolError::QueryPlanning(format!("Failed to render SQL: {}", e)))?;

    let rows = normalize_rows_with_hints(
        state
            .execute_query_on(&sql, "Query", tx_id.as_deref())
            .await?,
        metadata.scalar_hints(),
    )?;
    wrap_data_result(&rows, "findUnique result")
}

/// Handle `query.findUniqueOrThrow`.
pub(super) async fn handle_find_unique_or_throw(
    state: &EngineState,
    request: RpcRequest,
) -> Result<Box<serde_json::value::RawValue>, ProtocolError> {
    let raw = handle_find_unique(state, request).await?;
    let parsed: serde_json::Value = serde_json::from_str(raw.get())
        .map_err(|e| ProtocolError::Internal(format!("Failed to parse result: {}", e)))?;
    let is_empty = parsed
        .get("data")
        .and_then(|value| value.as_array())
        .is_none_or(|array| array.is_empty());
    if is_empty {
        return Err(ProtocolError::RecordNotFound(
            "findUniqueOrThrow: no record found matching the given filter".to_string(),
        ));
    }
    Ok(raw)
}

/// Handle `query.findFirstOrThrow`.
pub(super) async fn handle_find_first_or_throw(
    state: &EngineState,
    request: RpcRequest,
) -> Result<Box<serde_json::value::RawValue>, ProtocolError> {
    let raw = handle_find_first(state, request).await?;
    let parsed: serde_json::Value = serde_json::from_str(raw.get())
        .map_err(|e| ProtocolError::Internal(format!("Failed to parse result: {}", e)))?;
    let is_empty = parsed
        .get("data")
        .and_then(|value| value.as_array())
        .is_none_or(|array| array.is_empty());
    if is_empty {
        return Err(ProtocolError::RecordNotFound(
            "findFirstOrThrow: no record found matching the given filter".to_string(),
        ));
    }
    Ok(raw)
}

/// Handle `query.count`.
///
/// When `take` and/or `skip` are provided, the count is performed over the paginated window.
pub(super) async fn handle_count(
    state: &EngineState,
    request: RpcRequest,
) -> Result<Box<serde_json::value::RawValue>, ProtocolError> {
    let params: CountParams = serde_json::from_value(request.params)
        .map_err(|e| ProtocolError::InvalidParams(format!("Invalid count params: {}", e)))?;

    check_protocol_version(params.protocol_version)?;
    let tx_id = params.transaction_id;

    let model = get_model_or_error(state, &params.model)?;
    let metadata = state.model_metadata(model);
    let query_args = QueryArgs::parse_typed(params.args, metadata.field_types())?;
    let qualified_filter = qualify_model_filter(model, metadata.logical_to_db(), query_args.filter);

    let has_pagination = query_args.take.is_some() || query_args.skip.is_some();

    let sql: Sql = if has_pagination {
        let mut inner = Select::from_table(&model.db_name)
            .with_capacity(SelectCapacity {
                items: 1,
                ..SelectCapacity::default()
            })
            .item(SelectItem::computed(Expr::param(Value::I32(1)), "_1"));
        if let Some(filter) = qualified_filter {
            inner = inner.filter(filter);
        }
        if let Some(take) = query_args.take {
            inner = inner.take(take);
        }
        if let Some(skip) = query_args.skip {
            inner = inner.skip(skip);
        }
        let inner_built = inner.build().map_err(|e| {
            ProtocolError::QueryPlanning(format!("Failed to build inner count query: {}", e))
        })?;
        let inner_rendered = state
            .dialect
            .render_select_owned(inner_built)
            .map_err(|e| {
                ProtocolError::QueryPlanning(format!("Failed to render inner count query: {}", e))
            })?;
        Sql {
            text: format!("SELECT COUNT(*) FROM ({}) AS _cntq", inner_rendered.text),
            params: inner_rendered.params,
        }
    } else {
        let mut builder = Select::from_table(&model.db_name)
            .with_capacity(SelectCapacity {
                items: 1,
                ..SelectCapacity::default()
            })
            .item(SelectItem::computed(
                Expr::function_call("COUNT", vec![Expr::star()]),
                "count",
            ));
        if let Some(filter) = qualified_filter {
            builder = builder.filter(filter);
        }
        let select = builder.build().map_err(|e| {
            ProtocolError::QueryPlanning(format!("Failed to build count query: {}", e))
        })?;
        state.dialect.render_select_owned(select).map_err(|e| {
            ProtocolError::QueryPlanning(format!("Failed to render count query: {}", e))
        })?
    };

    let rows = state
        .execute_query_on(&sql, "Count", tx_id.as_deref())
        .await?;
    let count: i64 = rows
        .first()
        .and_then(|row| row.get_by_pos(0))
        .map(|value| match value {
            Value::I64(n) => *n,
            Value::I32(n) => *n as i64,
            _ => 0,
        })
        .unwrap_or(0);

    wrap_result(format!("{{\"count\":{}}}", count), "count result")
}
