use super::*;

pub(super) fn qualify_model_filter(
    model: &ModelIr,
    logical_to_db: &std::collections::HashMap<String, String>,
    filter: Option<Expr>,
) -> Option<Expr> {
    filter.map(|expr| qualify_filter_columns(expr, &model.db_name, logical_to_db))
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
    logical_to_db: &std::collections::HashMap<String, String>,
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
        crate::filter::SchemaContext::none(),
    )?;
    Ok(Some(qualify_filter_columns(
        parsed,
        &model.db_name,
        logical_to_db,
    )))
}

pub(super) fn parse_and_qualify_model_filter(
    model: &ModelIr,
    filter: &JsonValue,
    field_types: &crate::filter::FieldTypeMap,
    logical_to_db: &std::collections::HashMap<String, String>,
) -> Result<Expr, ProtocolError> {
    parse_optional_model_filter(model, filter, field_types, logical_to_db)?
        .ok_or_else(|| ProtocolError::InvalidFilter("where cannot be empty".to_string()))
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
    scalar_hints: &[Option<ValueHint>],
    return_data: bool,
    result_label: &str,
) -> Result<Box<serde_json::value::RawValue>, ProtocolError> {
    if return_data {
        let rows = normalize_rows_with_hints(
            state.execute_query_on(sql, exec_tag, tx_id).await?,
            scalar_hints,
        )?;
        wrap_mutation_result(&rows, result_label)
    } else {
        let count = state.execute_affected_on(sql, exec_tag, tx_id).await?;
        wrap_count_result(count, result_label)
    }
}
