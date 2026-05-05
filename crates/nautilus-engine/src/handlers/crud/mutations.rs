use super::common::{finish_mutation, parse_optional_model_filter};
use super::*;

/// Parse `request.params` into the given type, check the protocol version,
/// and look up the target model. Returns `(params, model)` on success.
macro_rules! parse_params {
    ($state:expr, $request:expr, $ty:ty, $label:literal) => {{
        let params: $ty = serde_json::from_value($request.params).map_err(|e| {
            ProtocolError::InvalidParams(format!(concat!("Invalid ", $label, " params: {}"), e))
        })?;
        check_protocol_version(params.protocol_version)?;
        let model = get_model_or_error($state, &params.model)?;
        (params, model)
    }};
}

fn row_field_json<'a>(
    data_obj: &'a JsonMap<String, JsonValue>,
    field: &FieldIr,
) -> Option<&'a JsonValue> {
    data_obj
        .get(&field.logical_name)
        .or_else(|| data_obj.get(&field.db_name))
}

fn updated_at_now_value() -> Value {
    Value::DateTime(chrono::Utc::now().naive_utc())
}

#[derive(Clone, Copy)]
enum FieldInputMode {
    Create,
    Update,
}

fn field_input_value(
    data_obj: &JsonMap<String, JsonValue>,
    field: &FieldIr,
    mode: FieldInputMode,
) -> Result<Option<Value>, ProtocolError> {
    if field.is_updated_at {
        return match row_field_json(data_obj, field) {
            Some(json_val) if !json_val.is_null() => {
                Ok(Some(json_to_value_field(json_val, &field.field_type)?))
            }
            _ => Ok(Some(updated_at_now_value())),
        };
    }

    let Some(json_val) = row_field_json(data_obj, field) else {
        return Ok(None);
    };

    if matches!(mode, FieldInputMode::Create)
        && json_val.is_null()
        && matches!(&field.default_value, Some(DefaultValue::Function(_)))
    {
        return Ok(None);
    }

    Ok(Some(json_to_value_field(json_val, &field.field_type)?))
}

fn should_omit_server_default(json_val: &JsonValue, field: &FieldIr) -> bool {
    json_val.is_null() && matches!(&field.default_value, Some(DefaultValue::Function(_)))
}

fn create_many_effective_fields<'a>(
    model: &'a ModelIr,
    data_obj: &JsonMap<String, JsonValue>,
) -> Vec<&'a FieldIr> {
    model
        .fields
        .iter()
        .filter(|field| !matches!(field.field_type, ResolvedFieldType::Relation(_)))
        .filter(|field| {
            if field.is_updated_at {
                return true;
            }
            row_field_json(data_obj, field)
                .is_some_and(|json_val| !should_omit_server_default(json_val, field))
        })
        .collect()
}

/// Handle `query.create`.
pub(super) async fn handle_create(
    state: &EngineState,
    request: RpcRequest,
) -> Result<Box<serde_json::value::RawValue>, ProtocolError> {
    let (params, model) = parse_params!(state, request, CreateParams, "create");
    let tx_id = params.transaction_id;
    let metadata = state.model_metadata(model);

    let data_obj = params
        .data
        .as_object()
        .ok_or_else(|| ProtocolError::InvalidParams("data must be an object".to_string()))?;

    let mut builder = Insert::into_table(&model.db_name);
    let mut columns = Vec::new();
    let mut values = Vec::new();

    for field in &model.fields {
        if matches!(field.field_type, ResolvedFieldType::Relation(_)) {
            continue;
        }
        if let Some(value) = field_input_value(data_obj, field, FieldInputMode::Create)? {
            columns.push(field_marker(model, field));
            values.push(value);
        }
    }

    builder = builder.columns(columns).values(values);
    if params.return_data {
        builder = builder.returning(metadata.scalar_markers().to_vec());
    }

    let insert = builder
        .build()
        .map_err(|e| ProtocolError::QueryPlanning(format!("Failed to build insert: {}", e)))?;

    let sql = state
        .dialect
        .render_insert_owned(insert)
        .map_err(|e| ProtocolError::QueryPlanning(format!("Failed to render SQL: {}", e)))?;

    finish_mutation(
        state,
        &sql,
        "Insert",
        tx_id.as_deref(),
        metadata.scalar_hints(),
        params.return_data,
        "create result",
    )
    .await
}

/// Handle `query.createMany`.
pub(super) async fn handle_create_many(
    state: &EngineState,
    request: RpcRequest,
) -> Result<Box<serde_json::value::RawValue>, ProtocolError> {
    let (params, model) = parse_params!(state, request, CreateManyParams, "createMany");
    let tx_id = params.transaction_id;
    let metadata = state.model_metadata(model);

    if params.data.is_empty() {
        return Err(ProtocolError::InvalidParams(
            "data array cannot be empty".to_string(),
        ));
    }

    let first_obj = params.data[0]
        .as_object()
        .ok_or_else(|| ProtocolError::InvalidParams("data items must be objects".to_string()))?;

    let relevant_fields = create_many_effective_fields(model, first_obj);
    let expected_keys: Vec<&str> = relevant_fields
        .iter()
        .map(|field| field.logical_name.as_str())
        .collect();
    let expected_key_set: std::collections::HashSet<&str> = expected_keys.iter().copied().collect();

    let columns: Vec<_> = relevant_fields
        .iter()
        .map(|field| field_marker(model, field))
        .collect();

    let mut all_values = Vec::new();
    for (row_idx, json_value) in params.data.iter().enumerate() {
        let data_obj = json_value.as_object().ok_or_else(|| {
            ProtocolError::InvalidParams("data items must be objects".to_string())
        })?;

        let row_fields = create_many_effective_fields(model, data_obj);
        let row_keys: Vec<&str> = row_fields
            .iter()
            .map(|field| field.logical_name.as_str())
            .collect();

        if row_keys != expected_keys {
            let row_key_set: std::collections::HashSet<&str> = row_keys.iter().copied().collect();
            let missing: Vec<&str> = expected_keys
                .iter()
                .copied()
                .filter(|key| !row_key_set.contains(key))
                .collect();
            let extra: Vec<&str> = row_keys
                .iter()
                .copied()
                .filter(|key| !expected_key_set.contains(key))
                .collect();
            return Err(ProtocolError::InvalidParams(format!(
                "createMany rows must use the same key set after omitting server defaults; row {} differs from row 0 (missing: [{}], extra: [{}])",
                row_idx,
                missing.join(", "),
                extra.join(", "),
            )));
        }

        let mut row_values = Vec::new();
        for field in &relevant_fields {
            if let Some(value) = field_input_value(data_obj, field, FieldInputMode::Create)? {
                row_values.push(value);
            } else {
                row_values.push(Value::Null);
            }
        }
        all_values.push(row_values);
    }

    let mut builder = Insert::into_table(&model.db_name).columns(columns);
    for row in all_values {
        builder = builder.values(row);
    }
    if params.return_data {
        builder = builder.returning(metadata.scalar_markers().to_vec());
    }

    let insert = builder
        .build()
        .map_err(|e| ProtocolError::QueryPlanning(format!("Failed to build insert: {}", e)))?;

    let sql = state
        .dialect
        .render_insert_owned(insert)
        .map_err(|e| ProtocolError::QueryPlanning(format!("Failed to render SQL: {}", e)))?;

    finish_mutation(
        state,
        &sql,
        "Insert",
        tx_id.as_deref(),
        metadata.scalar_hints(),
        params.return_data,
        "createMany result",
    )
    .await
}

/// Handle `query.update`.
pub(super) async fn handle_update(
    state: &EngineState,
    request: RpcRequest,
) -> Result<Box<serde_json::value::RawValue>, ProtocolError> {
    let (params, model) = parse_params!(state, request, UpdateParams, "update");
    let tx_id = params.transaction_id;
    let metadata = state.model_metadata(model);

    let qualified_filter = parse_optional_model_filter(
        model,
        &params.filter,
        metadata.field_types(),
        metadata.logical_to_db(),
    )?;

    let data_obj = params
        .data
        .as_object()
        .ok_or_else(|| ProtocolError::InvalidParams("data must be an object".to_string()))?;

    let mut builder = Update::table(&model.db_name);

    for field in &model.fields {
        if matches!(field.field_type, ResolvedFieldType::Relation(_)) {
            continue;
        }
        if let Some(value) = field_input_value(data_obj, field, FieldInputMode::Update)? {
            builder = builder.set(field_marker(model, field), value);
        }
    }

    if let Some(filter) = qualified_filter {
        builder = builder.filter(filter);
    }

    if params.return_data {
        builder = builder.returning(metadata.scalar_markers().to_vec());
    }

    let update = builder
        .build()
        .map_err(|e| ProtocolError::QueryPlanning(format!("Failed to build update: {}", e)))?;

    let sql = state
        .dialect
        .render_update_owned(update)
        .map_err(|e| ProtocolError::QueryPlanning(format!("Failed to render SQL: {}", e)))?;

    finish_mutation(
        state,
        &sql,
        "Update",
        tx_id.as_deref(),
        metadata.scalar_hints(),
        params.return_data,
        "update result",
    )
    .await
}

/// Handle `query.delete`.
pub(super) async fn handle_delete(
    state: &EngineState,
    request: RpcRequest,
) -> Result<Box<serde_json::value::RawValue>, ProtocolError> {
    let (params, model) = parse_params!(state, request, DeleteParams, "delete");
    let tx_id = params.transaction_id;
    let metadata = state.model_metadata(model);

    let qualified_filter = parse_optional_model_filter(
        model,
        &params.filter,
        metadata.field_types(),
        metadata.logical_to_db(),
    )?;

    let mut builder = Delete::from_table(&model.db_name);
    if let Some(filter) = qualified_filter {
        builder = builder.filter(filter);
    }

    if params.return_data {
        builder = builder.returning(metadata.scalar_markers().to_vec());
    }

    let delete = builder
        .build()
        .map_err(|e| ProtocolError::QueryPlanning(format!("Failed to build delete: {}", e)))?;

    let sql = state
        .dialect
        .render_delete_owned(delete)
        .map_err(|e| ProtocolError::QueryPlanning(format!("Failed to render SQL: {}", e)))?;

    finish_mutation(
        state,
        &sql,
        "Delete",
        tx_id.as_deref(),
        metadata.scalar_hints(),
        params.return_data,
        "delete result",
    )
    .await
}
