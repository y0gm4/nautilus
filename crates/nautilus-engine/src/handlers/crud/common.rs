use super::*;

pub(super) fn model_logical_to_db(model: &ModelIr) -> std::collections::HashMap<String, String> {
    model
        .scalar_fields()
        .flat_map(|field| {
            let mut entries = vec![(field.logical_name.clone(), field.db_name.clone())];
            if field.db_name != field.logical_name {
                entries.push((field.db_name.clone(), field.db_name.clone()));
            }
            entries
        })
        .collect()
}

pub(super) fn model_db_to_logical(model: &ModelIr) -> std::collections::HashMap<String, String> {
    model
        .scalar_fields()
        .map(|field| (field.db_name.clone(), field.logical_name.clone()))
        .collect()
}

pub(super) fn qualify_model_filter(model: &ModelIr, filter: Option<Expr>) -> Option<Expr> {
    let logical_to_db = model_logical_to_db(model);
    filter.map(|expr| qualify_filter_columns(expr, &model.db_name, &logical_to_db))
}

fn protocol_filter_body(filter: &JsonValue) -> &JsonValue {
    filter
        .as_object()
        .and_then(|obj| (obj.len() == 1).then_some(obj))
        .and_then(|obj| obj.get("where"))
        .unwrap_or(filter)
}

pub(super) fn parse_optional_model_filter(
    model: &ModelIr,
    filter: &JsonValue,
    field_types: &crate::filter::FieldTypeMap,
) -> Result<Option<Expr>, ProtocolError> {
    let filter = protocol_filter_body(filter);
    let JsonValue::Object(filter_obj) = filter else {
        return Err(ProtocolError::InvalidFilter(
            "where must be an object".to_string(),
        ));
    };

    if filter_obj.is_empty() {
        return Ok(None);
    }

    let parsed = crate::filter::parse_where_filter(
        filter,
        &crate::filter::RelationMap::new(),
        field_types,
        None,
    )?;
    let logical_to_db = model_logical_to_db(model);
    Ok(Some(qualify_filter_columns(
        parsed,
        &model.db_name,
        &logical_to_db,
    )))
}

pub(super) fn parse_and_qualify_model_filter(
    model: &ModelIr,
    filter: &JsonValue,
    field_types: &crate::filter::FieldTypeMap,
) -> Result<Expr, ProtocolError> {
    parse_optional_model_filter(model, filter, field_types)?
        .ok_or_else(|| ProtocolError::InvalidFilter("where cannot be empty".to_string()))
}

pub(super) fn field_value_hint(field: &FieldIr) -> Option<ValueHint> {
    if field.is_array && field.storage_strategy == Some(StorageStrategy::Json) {
        return Some(ValueHint::Json);
    }

    match &field.field_type {
        ResolvedFieldType::Scalar(ScalarType::Decimal { .. }) => Some(ValueHint::Decimal),
        ResolvedFieldType::Scalar(ScalarType::DateTime) => Some(ValueHint::DateTime),
        ResolvedFieldType::Scalar(ScalarType::Json | ScalarType::Jsonb) => Some(ValueHint::Json),
        ResolvedFieldType::Scalar(ScalarType::Uuid) => Some(ValueHint::Uuid),
        ResolvedFieldType::CompositeType { .. }
            if field.storage_strategy == Some(StorageStrategy::Json) =>
        {
            Some(ValueHint::Json)
        }
        _ => None,
    }
}

pub(super) fn model_scalar_value_hints(model: &ModelIr) -> Vec<Option<ValueHint>> {
    model.scalar_fields().map(field_value_hint).collect()
}

pub(super) fn wrap_result(
    result_str: String,
    context: &str,
) -> Result<Box<serde_json::value::RawValue>, ProtocolError> {
    serde_json::value::RawValue::from_string(result_str)
        .map_err(|e| ProtocolError::Internal(format!("Failed to wrap {}: {}", context, e)))
}

pub(super) fn wrap_data_result(
    rows: &[Row],
    context: &str,
) -> Result<Box<serde_json::value::RawValue>, ProtocolError> {
    let data_raw = rows_to_raw_json(rows)?;
    wrap_result(format!("{{\"data\":{}}}", data_raw.get()), context)
}

pub(super) fn wrap_count_result(
    count: impl std::fmt::Display,
    context: &str,
) -> Result<Box<serde_json::value::RawValue>, ProtocolError> {
    wrap_result(format!("{{\"count\":{}}}", count), context)
}

pub(super) fn wrap_mutation_result(
    rows: &[Row],
    context: &str,
) -> Result<Box<serde_json::value::RawValue>, ProtocolError> {
    let data_raw = rows_to_raw_json(rows)?;
    wrap_result(
        format!("{{\"count\":{},\"data\":{}}}", rows.len(), data_raw.get()),
        context,
    )
}

/// Execute the SQL for a mutation and wrap the result.
///
/// When `return_data` is true, runs `execute_query_on` with the model's value
/// hints and returns `{count, data}`. Otherwise runs `execute_affected_on` and
/// returns `{count}`.
pub(super) async fn finish_mutation(
    state: &EngineState,
    sql: &Sql,
    exec_tag: &'static str,
    tx_id: Option<&str>,
    model: &ModelIr,
    return_data: bool,
    result_label: &str,
) -> Result<Box<serde_json::value::RawValue>, ProtocolError> {
    if return_data {
        let rows = normalize_rows_with_hints(
            state.execute_query_on(sql, exec_tag, tx_id).await?,
            &model_scalar_value_hints(model),
        )?;
        wrap_mutation_result(&rows, result_label)
    } else {
        let count = state.execute_affected_on(sql, exec_tag, tx_id).await?;
        wrap_count_result(count, result_label)
    }
}
