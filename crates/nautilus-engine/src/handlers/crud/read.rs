use super::common::{
    parse_and_qualify_model_filter, qualify_model_filter, wrap_count_result, wrap_data_result,
};
use super::include::hydrate_rows_with_includes;
use super::*;

pub(super) async fn execute_find_many_rows(
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

pub(super) async fn execute_find_many_params(
    state: &EngineState,
    params: FindManyParams,
) -> Result<Vec<Row>, ProtocolError> {
    check_protocol_version(params.protocol_version)?;
    let tx_id = params.transaction_id;

    let model = get_model_or_error(state, &params.model)?;
    let metadata = state.model_metadata(model);
    let relation_map = state.relation_map_for_model(model)?;
    let query_args = QueryArgs::parse_with_context(
        params.args,
        relation_map,
        metadata.field_types(),
        crate::filter::SchemaContext::with_state(state),
    )?;

    execute_find_many_rows(state, model, query_args, tx_id.as_deref()).await
}

pub(super) async fn execute_find_many_typed(
    state: &EngineState,
    model_name: &str,
    args: &nautilus_core::FindManyArgs,
    transaction_id: Option<&str>,
) -> Result<Vec<Row>, ProtocolError> {
    let model = get_model_or_error(state, model_name)?;
    let metadata = state.model_metadata(model);
    let query_args = QueryArgs::from_find_many_args(args, metadata.field_types())?;

    execute_find_many_rows(state, model, query_args, transaction_id).await
}

fn build_find_unique_sql(
    state: &EngineState,
    model: &ModelIr,
    qualified_filter: Expr,
    selected_fields: &std::collections::HashSet<&str>,
) -> Result<(Sql, Vec<Option<ValueHint>>), ProtocolError> {
    let metadata = state.model_metadata(model);
    let pk_fields = metadata.primary_key_fields();

    let mut builder = Select::from_table(&model.db_name).with_capacity(SelectCapacity {
        items: metadata.scalar_fields().len(),
        ..SelectCapacity::default()
    });
    let mut row_hints = Vec::new();

    for field in metadata.scalar_fields() {
        if !selected_fields.is_empty()
            && !selected_fields.contains(field.logical_name())
            && !pk_fields
                .iter()
                .any(|pk_field| pk_field.logical_name() == field.logical_name())
        {
            continue;
        }

        builder = builder.item(SelectItem::from(field.marker().clone()));
        row_hints.push(field.hint());
    }

    let select = builder
        .filter(qualified_filter)
        .take(1)
        .build()
        .map_err(|e| ProtocolError::QueryPlanning(format!("Failed to build query: {}", e)))?;

    let sql = state
        .dialect
        .render_select_owned(select)
        .map_err(|e| ProtocolError::QueryPlanning(format!("Failed to render SQL: {}", e)))?;

    Ok((sql, row_hints))
}

async fn execute_find_unique_rows(
    state: &EngineState,
    model: &ModelIr,
    qualified_filter: Expr,
    selected_fields: &std::collections::HashSet<&str>,
    tx_id: Option<&str>,
) -> Result<Vec<Row>, ProtocolError> {
    let (sql, row_hints) = build_find_unique_sql(state, model, qualified_filter, selected_fields)?;
    normalize_rows_with_hints(
        state.execute_query_on(&sql, "Query", tx_id).await?,
        &row_hints,
    )
}

/// Build the [`FindUniquePlanKey`] for a request matched by [`extract_simple_eq_filter`].
///
/// The resolved projection is canonicalised (selected fields plus implicit PK
/// fields, sorted) so semantically equivalent inputs share a cache entry.
fn find_unique_plan_key(
    model: &ModelIr,
    metadata: &crate::metadata::ModelMetadata,
    selected_fields: &std::collections::HashSet<&str>,
    shape: &crate::plan_cache::EqFilterShape<'_>,
) -> crate::plan_cache::FindUniquePlanKey {
    let resolved: Vec<String> = if selected_fields.is_empty() {
        Vec::new()
    } else {
        let mut combined: Vec<String> = selected_fields.iter().map(|s| s.to_string()).collect();
        for pk in metadata.primary_key_fields() {
            let logical = pk.logical_name();
            if !selected_fields.contains(logical) {
                combined.push(logical.to_string());
            }
        }
        combined.sort();
        combined.dedup();
        combined
    };

    crate::plan_cache::FindUniquePlanKey {
        model_db_name: model.db_name.clone(),
        selected_logical_fields: resolved,
        filter_columns: shape.columns.iter().map(|s| s.to_string()).collect(),
    }
}

pub(super) async fn execute_find_unique_typed(
    state: &EngineState,
    model_name: &str,
    args: &nautilus_core::FindUniqueArgs,
    transaction_id: Option<&str>,
) -> Result<Vec<Row>, ProtocolError> {
    if !args.include.is_empty() {
        return execute_find_many_typed(
            state,
            model_name,
            &nautilus_core::FindManyArgs {
                where_: Some(args.where_.clone()),
                take: Some(1),
                select: args.select.clone(),
                include: args.include.clone(),
                ..Default::default()
            },
            transaction_id,
        )
        .await;
    }

    let model = get_model_or_error(state, model_name)?;
    let metadata = state.model_metadata(model);
    let selected_fields: std::collections::HashSet<&str> = args
        .select
        .iter()
        .filter_map(|(field, enabled)| enabled.then_some(field.as_str()))
        .collect();

    // Plan-cache fast path: only available when the filter is a flat AND chain
    // of `Column = Param` predicates so we can replay the rendered SQL by
    // re-binding parameter values without rebuilding the AST.
    if let Some(shape) = crate::plan_cache::extract_simple_eq_filter(&args.where_) {
        let cache_key = find_unique_plan_key(model, metadata, &selected_fields, &shape);
        if let Some(plan) = state.plan_cache().get_find_unique(&cache_key) {
            let sql = Sql {
                text: plan.sql_text.clone(),
                params: shape.values.iter().map(|v| (*v).clone()).collect(),
            };
            return normalize_rows_with_hints(
                state
                    .execute_query_on(&sql, "Query", transaction_id)
                    .await?,
                &plan.row_hints,
            );
        }

        let qualified_filter = qualify_filter_columns(
            args.where_.clone(),
            &model.db_name,
            metadata.logical_to_db(),
        );
        let (sql, row_hints) =
            build_find_unique_sql(state, model, qualified_filter, &selected_fields)?;
        state.plan_cache().insert_find_unique(
            cache_key,
            std::sync::Arc::new(crate::plan_cache::CachedFindUniquePlan {
                sql_text: sql.text.clone(),
                row_hints: row_hints.clone(),
            }),
        );
        return normalize_rows_with_hints(
            state
                .execute_query_on(&sql, "Query", transaction_id)
                .await?,
            &row_hints,
        );
    }

    let qualified_filter = qualify_filter_columns(
        args.where_.clone(),
        &model.db_name,
        metadata.logical_to_db(),
    );
    execute_find_unique_rows(
        state,
        model,
        qualified_filter,
        &selected_fields,
        transaction_id,
    )
    .await
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

    let chunk_size = params.chunk_size;
    let rows = execute_find_many_params(state, params).await?;

    if let (Some(size), Some(channel)) = (chunk_size, sender) {
        let size = size.max(1);
        let id = request.id.clone();
        let mut chunks = rows.chunks(size).peekable();

        if chunks.peek().is_some() {
            while let Some(chunk) = chunks.next() {
                let is_last = chunks.peek().is_none();
                let raw = wrap_data_result(chunk, "findMany chunk")?;
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

pub(super) async fn handle_find_many_embedded(
    state: &EngineState,
    request: RpcRequest,
) -> Result<Vec<Row>, ProtocolError> {
    let params: FindManyParams = serde_json::from_value(request.params)
        .map_err(|e| ProtocolError::InvalidParams(format!("Invalid findMany params: {}", e)))?;
    execute_find_many_params(state, params).await
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
    let rows = execute_find_unique_rows(
        state,
        model,
        qualified_filter,
        &std::collections::HashSet::new(),
        tx_id.as_deref(),
    )
    .await?;
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

    let count = execute_count_params(state, params).await?;
    wrap_count_result(count, "count result")
}

async fn execute_count_params(
    state: &EngineState,
    params: CountParams,
) -> Result<i64, ProtocolError> {
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

    Ok(count)
}

pub(super) async fn handle_count_embedded(
    state: &EngineState,
    request: RpcRequest,
) -> Result<i64, ProtocolError> {
    let params: CountParams = serde_json::from_value(request.params)
        .map_err(|e| ProtocolError::InvalidParams(format!("Invalid count params: {}", e)))?;
    execute_count_params(state, params).await
}

pub(super) async fn handle_count_typed(
    state: &EngineState,
    params: CountParams,
) -> Result<i64, ProtocolError> {
    execute_count_params(state, params).await
}
