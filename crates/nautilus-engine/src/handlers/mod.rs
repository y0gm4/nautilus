//! RPC request dispatch and shared handler helpers.
//!
//! This module contains:
//! - `handle_request` — top-level entry point
//! - `dispatch` — inner routing table (also used by `transaction_batch`)
//! - `handle_handshake` — engine.handshake handler
//! - Shared helpers used by `crud` and `transactions` submodules

use std::collections::HashMap;

use nautilus_core::ColumnMarker;
use nautilus_protocol::wire::{err, ok};
use nautilus_protocol::{
    CountParams, CreateManyParams, CreateParams, GroupByParams, HandshakeParams, HandshakeResult,
    ProtocolError, RpcError, RpcRequest, RpcResponse, SchemaValidateParams, SchemaValidateResult,
    UpdateParams, ENGINE_HANDSHAKE, PROTOCOL_VERSION, QUERY_COUNT, QUERY_CREATE, QUERY_CREATE_MANY,
    QUERY_DELETE, QUERY_FIND_FIRST, QUERY_FIND_FIRST_OR_THROW, QUERY_FIND_MANY, QUERY_FIND_UNIQUE,
    QUERY_FIND_UNIQUE_OR_THROW, QUERY_GROUP_BY, QUERY_RAW, QUERY_RAW_STMT, QUERY_UPDATE,
    SCHEMA_VALIDATE, TRANSACTION_BATCH, TRANSACTION_COMMIT, TRANSACTION_ROLLBACK,
    TRANSACTION_START,
};
use nautilus_schema::ir::{FieldIr, ModelIr, ResolvedFieldType};
use nautilus_schema::{analyze, Severity};
use tokio::sync::mpsc;

use crate::conversion::check_protocol_version;
use crate::conversion::to_snake_case;
use crate::filter::{RelationInfo, RelationMap};
use crate::state::EngineState;

mod crud;
mod transactions;

#[derive(Debug)]
pub enum EmbeddedResponse {
    Rows(Vec<nautilus_connector::Row>),
    Count(i64),
    Json(Box<serde_json::value::RawValue>),
}

/// Build a `ColumnMarker` for a scalar field.
pub(super) fn field_marker(model: &ModelIr, field: &FieldIr) -> ColumnMarker {
    ColumnMarker::new(&model.db_name, &field.db_name)
}

/// Build a map from logical field name -> resolved field type for a model.
/// Used by tests that exercise the filter parser in isolation.
#[cfg(test)]
pub(super) fn build_field_type_map(model: &ModelIr) -> crate::filter::FieldTypeMap {
    crate::metadata::build_field_type_map(model)
}

/// Look up a model by logical name, returning a typed error on miss.
pub(super) fn get_model_or_error<'a>(
    state: &'a EngineState,
    model_name: &str,
) -> Result<&'a ModelIr, ProtocolError> {
    state
        .models
        .get(model_name)
        .ok_or_else(|| ProtocolError::InvalidModel(format!("Model not found: {}", model_name)))
}

/// Build a `RelationMap` for the given model so that the filter parser can resolve
/// `some` / `none` / `every` predicates and `include` entries at runtime.
pub(super) fn build_relation_map(
    model: &ModelIr,
    models: &HashMap<String, ModelIr>,
) -> Result<RelationMap, ProtocolError> {
    let mut map = RelationMap::new();

    for field in model.relation_fields() {
        if let ResolvedFieldType::Relation(rel) = &field.field_type {
            let target_logical_name = rel.target_model.clone();

            if let Some(target_model) = models.get(&target_logical_name) {
                // Resolve (fk_db, pk_db) based on which side carries the FK.
                let (fk_db, pk_db) = if rel.fields.is_empty() {
                    // Array / many-side: FK is in the target model.
                    // Find the inverse relation (the FK side) in the target model.
                    let matching_name = rel.name.as_deref();
                    let candidates: Vec<_> = target_model
                        .relation_fields()
                        .filter_map(|candidate_field| {
                            let ResolvedFieldType::Relation(inv_rel) = &candidate_field.field_type
                            else {
                                return None;
                            };
                            if inv_rel.target_model != model.logical_name
                                || inv_rel.fields.is_empty()
                            {
                                return None;
                            }
                            if let Some(name) = matching_name {
                                if inv_rel.name.as_deref() != Some(name) {
                                    return None;
                                }
                            }
                            Some((candidate_field, inv_rel))
                        })
                        .collect();

                    let inverse = match candidates.len() {
                        0 if matching_name.is_some() => {
                            return Err(ProtocolError::QueryPlanning(format!(
                                "Relation '{}.{}' expects inverse relation name '{}' on model '{}', but no matching FK-side relation was found",
                                model.logical_name,
                                field.logical_name,
                                matching_name.unwrap_or_default(),
                                target_model.logical_name,
                            )));
                        }
                        0 => None,
                        1 => Some(candidates[0]),
                        _ => {
                            let relation_hint = matching_name
                                .map(|name| format!(" named '{}'", name))
                                .unwrap_or_default();
                            return Err(ProtocolError::QueryPlanning(format!(
                                "Relation '{}.{}' has ambiguous inverse relation{} on model '{}'",
                                model.logical_name,
                                field.logical_name,
                                relation_hint,
                                target_model.logical_name,
                            )));
                        }
                    };

                    if let Some((_, inv_rel)) = inverse {
                        let fk = inv_rel
                            .fields
                            .first()
                            .and_then(|name| {
                                target_model
                                    .fields
                                    .iter()
                                    .find(|f| &f.logical_name == name)
                                    .map(|f| f.db_name.clone())
                            })
                            .unwrap_or_default();
                        let pk = inv_rel
                            .references
                            .first()
                            .and_then(|name| {
                                model
                                    .fields
                                    .iter()
                                    .find(|f| &f.logical_name == name)
                                    .map(|f| f.db_name.clone())
                            })
                            .unwrap_or_default();
                        (fk, pk)
                    } else {
                        (String::new(), String::new())
                    }
                } else {
                    // FK-side: rel.fields = FK logical names in this model,
                    // rel.references = referenced (PK) logical names in target model.
                    // For EXISTS from this model into target:
                    //   EXISTS (SELECT * FROM target WHERE target.ref_col = this.fk_col)
                    let fk = rel
                        .references
                        .first()
                        .and_then(|name| {
                            target_model
                                .fields
                                .iter()
                                .find(|f| &f.logical_name == name)
                                .map(|f| f.db_name.clone())
                        })
                        .unwrap_or_default();
                    let pk = rel
                        .fields
                        .first()
                        .and_then(|name| {
                            model
                                .fields
                                .iter()
                                .find(|f| &f.logical_name == name)
                                .map(|f| f.db_name.clone())
                        })
                        .unwrap_or_default();
                    (fk, pk)
                };

                if !fk_db.is_empty() && !pk_db.is_empty() {
                    // Use snake_case logical name as the map key (matches JSON field names)
                    let field_key = to_snake_case(&field.logical_name);
                    map.insert(
                        field_key,
                        RelationInfo {
                            parent_table: model.db_name.clone(),
                            target_logical_name,
                            target_table: target_model.db_name.clone(),
                            fk_db,
                            pk_db,
                            is_array: field.is_array,
                        },
                    );
                }
            }
        }
    }

    Ok(map)
}

/// Dispatch RPC request to the appropriate handler.
///
/// `tx` is the response channel — forwarded to `handle_find_many` so that it can
/// emit partial (chunked) responses before returning the final chunk.
pub async fn handle_request(
    state: &EngineState,
    request: RpcRequest,
    tx: mpsc::Sender<RpcResponse>,
) -> RpcResponse {
    let id = request.id.clone();

    // Route findMany separately so we can pass the sender for chunked streaming.
    if request.method == QUERY_FIND_MANY {
        let result = crud::handle_find_many(state, request, Some(tx)).await;
        return response_from_result(id, result);
    }

    response_from_result(id, dispatch(state, request).await)
}

/// Handle an in-process request without allocating a response channel.
///
/// This is intended for embedded callers that only consume the final response
/// and do not use chunked `findMany` partials.
pub async fn handle_request_inline(state: &EngineState, request: RpcRequest) -> RpcResponse {
    let id = request.id.clone();
    response_from_result(id, dispatch(state, request).await)
}

/// Handle an in-process request and return typed rows/counts when possible.
///
/// This is intended for embedded Rust callers that can consume modeled results
/// directly without serializing them through the public JSON-RPC wire shape.
pub async fn handle_request_embedded(
    state: &EngineState,
    request: RpcRequest,
) -> Result<EmbeddedResponse, ProtocolError> {
    match request.method.as_str() {
        QUERY_FIND_MANY => crud::handle_find_many_embedded(state, request)
            .await
            .map(EmbeddedResponse::Rows),
        QUERY_CREATE => crud::handle_create_embedded(state, request)
            .await
            .map(EmbeddedResponse::Rows),
        QUERY_CREATE_MANY => crud::handle_create_many_embedded(state, request)
            .await
            .map(EmbeddedResponse::Rows),
        QUERY_UPDATE => crud::handle_update_embedded(state, request)
            .await
            .map(EmbeddedResponse::Rows),
        QUERY_COUNT => crud::handle_count_embedded(state, request)
            .await
            .map(EmbeddedResponse::Count),
        QUERY_GROUP_BY => crud::handle_group_by_embedded(state, request)
            .await
            .map(EmbeddedResponse::Rows),
        _ => dispatch(state, request).await.map(EmbeddedResponse::Json),
    }
}

/// Handle a typed Rust `findMany` request in-process without going through the
/// JSON-RPC envelope or engine JSON argument format.
pub async fn handle_find_many_typed(
    state: &EngineState,
    model_name: &str,
    args: &nautilus_core::FindManyArgs,
    transaction_id: Option<&str>,
) -> Result<Vec<nautilus_connector::Row>, ProtocolError> {
    crud::handle_find_many_typed(state, model_name, args, transaction_id).await
}

/// Handle a typed Rust `create` request in-process without an RPC envelope.
pub async fn handle_create_typed(
    state: &EngineState,
    params: CreateParams,
) -> Result<Vec<nautilus_connector::Row>, ProtocolError> {
    crud::handle_create_typed(state, params).await
}

/// Handle a typed Rust `createMany` request in-process without an RPC envelope.
pub async fn handle_create_many_typed(
    state: &EngineState,
    params: CreateManyParams,
) -> Result<Vec<nautilus_connector::Row>, ProtocolError> {
    crud::handle_create_many_typed(state, params).await
}

/// Handle a typed Rust `update` request in-process without an RPC envelope.
pub async fn handle_update_typed(
    state: &EngineState,
    params: UpdateParams,
) -> Result<Vec<nautilus_connector::Row>, ProtocolError> {
    crud::handle_update_typed(state, params).await
}

/// Handle a typed Rust `count` request in-process without an RPC envelope.
pub async fn handle_count_typed(
    state: &EngineState,
    params: CountParams,
) -> Result<i64, ProtocolError> {
    crud::handle_count_typed(state, params).await
}

/// Handle a typed Rust `groupBy` request in-process without an RPC envelope.
pub async fn handle_group_by_typed(
    state: &EngineState,
    params: GroupByParams,
) -> Result<Vec<nautilus_connector::Row>, ProtocolError> {
    crud::handle_group_by_typed(state, params).await
}

fn response_from_result(
    id: Option<nautilus_protocol::RpcId>,
    result: Result<Box<serde_json::value::RawValue>, ProtocolError>,
) -> RpcResponse {
    match result {
        Ok(value) => ok(id, value),
        Err(protocol_error) => {
            let rpc_error: RpcError = protocol_error.into();
            err(id, rpc_error.code, rpc_error.message, rpc_error.data)
        }
    }
}

/// Inner dispatch: route method name to handler, returning a raw Result.
///
/// Extracted so that [`transactions::handle_transaction_batch`] can re-use the
/// same routing table without constructing full RPC responses.
pub(super) async fn dispatch(
    state: &EngineState,
    request: RpcRequest,
) -> Result<Box<serde_json::value::RawValue>, ProtocolError> {
    match request.method.as_str() {
        ENGINE_HANDSHAKE => handle_handshake(state, request).await,
        SCHEMA_VALIDATE => handle_schema_validate(state, request).await,
        QUERY_FIND_MANY => crud::handle_find_many(state, request, None).await,
        QUERY_FIND_FIRST => crud::handle_find_first(state, request).await,
        QUERY_FIND_UNIQUE => crud::handle_find_unique(state, request).await,
        QUERY_FIND_UNIQUE_OR_THROW => crud::handle_find_unique_or_throw(state, request).await,
        QUERY_FIND_FIRST_OR_THROW => crud::handle_find_first_or_throw(state, request).await,
        QUERY_CREATE => crud::handle_create(state, request).await,
        QUERY_CREATE_MANY => crud::handle_create_many(state, request).await,
        QUERY_UPDATE => crud::handle_update(state, request).await,
        QUERY_DELETE => crud::handle_delete(state, request).await,
        QUERY_COUNT => crud::handle_count(state, request).await,
        QUERY_GROUP_BY => crud::handle_group_by(state, request).await,
        QUERY_RAW => crud::handle_raw_query(state, request).await,
        QUERY_RAW_STMT => crud::handle_raw_stmt_query(state, request).await,
        TRANSACTION_START => transactions::handle_transaction_start(state, request).await,
        TRANSACTION_COMMIT => transactions::handle_transaction_commit(state, request).await,
        TRANSACTION_ROLLBACK => transactions::handle_transaction_rollback(state, request).await,
        TRANSACTION_BATCH => transactions::handle_transaction_batch(state, request).await,
        _ => Err(ProtocolError::InvalidMethod(request.method)),
    }
}

/// Handle engine.handshake
async fn handle_handshake(
    _state: &EngineState,
    request: RpcRequest,
) -> Result<Box<serde_json::value::RawValue>, ProtocolError> {
    let params: HandshakeParams = serde_json::from_value(request.params)
        .map_err(|e| ProtocolError::InvalidParams(format!("Invalid handshake params: {}", e)))?;

    check_protocol_version(params.protocol_version)?;

    let result = HandshakeResult {
        engine_version: env!("CARGO_PKG_VERSION").to_string(),
        protocol_version: PROTOCOL_VERSION,
    };

    serialize_result(&result, "handshake result")
}

async fn handle_schema_validate(
    _state: &EngineState,
    request: RpcRequest,
) -> Result<Box<serde_json::value::RawValue>, ProtocolError> {
    let params: SchemaValidateParams = serde_json::from_value(request.params).map_err(|e| {
        ProtocolError::InvalidParams(format!("Invalid schema.validate params: {}", e))
    })?;

    check_protocol_version(params.protocol_version)?;

    let analysis = analyze(&params.schema);
    let errors: Vec<String> = analysis
        .diagnostics
        .into_iter()
        .filter(|diagnostic| diagnostic.severity == Severity::Error)
        .map(|diagnostic| diagnostic.message)
        .collect();

    let result = SchemaValidateResult {
        valid: errors.is_empty(),
        errors: (!errors.is_empty()).then_some(errors),
    };

    serialize_result(&result, "schema.validate result")
}

fn serialize_result<T: serde::Serialize>(
    result: &T,
    context: &str,
) -> Result<Box<serde_json::value::RawValue>, ProtocolError> {
    let s = sonic_rs::to_string(result)
        .map_err(|e| ProtocolError::Internal(format!("Failed to serialize {context}: {}", e)))?;
    serde_json::value::RawValue::from_string(s)
        .map_err(|e| ProtocolError::Internal(format!("Failed to wrap {context}: {}", e)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use nautilus_schema::ir::{PrimaryKeyIr, RelationIr, ScalarType};
    use nautilus_schema::{validate_schema_source, Span};
    use std::collections::HashMap;

    fn parse_ir(source: &str) -> nautilus_schema::ir::SchemaIr {
        validate_schema_source(source)
            .expect("validation failed")
            .ir
    }

    fn scalar_field(logical: &str, db: &str) -> FieldIr {
        FieldIr {
            logical_name: logical.to_string(),
            db_name: db.to_string(),
            field_type: ResolvedFieldType::Scalar(ScalarType::Int),
            is_required: true,
            is_array: false,
            storage_strategy: None,
            default_value: None,
            is_unique: false,
            is_updated_at: false,
            computed: None,
            check: None,
            span: Span::new(0, 0),
        }
    }

    fn relation_field(
        logical: &str,
        target_model: &str,
        fields: &[&str],
        references: &[&str],
        name: Option<&str>,
        is_array: bool,
    ) -> FieldIr {
        FieldIr {
            logical_name: logical.to_string(),
            db_name: logical.to_string(),
            field_type: ResolvedFieldType::Relation(RelationIr {
                name: name.map(str::to_string),
                target_model: target_model.to_string(),
                fields: fields.iter().map(|s| (*s).to_string()).collect(),
                references: references.iter().map(|s| (*s).to_string()).collect(),
                on_delete: None,
                on_update: None,
            }),
            is_required: !is_array,
            is_array,
            storage_strategy: None,
            default_value: None,
            is_unique: false,
            is_updated_at: false,
            computed: None,
            check: None,
            span: Span::new(0, 0),
        }
    }

    #[test]
    fn field_marker_builds_correct_marker() {
        let model = ModelIr {
            logical_name: "User".to_string(),
            db_name: "users".to_string(),
            fields: vec![],
            primary_key: PrimaryKeyIr::Single("id".to_string()),
            unique_constraints: vec![],
            indexes: vec![],
            check_constraints: vec![],
            span: Span::new(0, 0),
        };
        let field = FieldIr {
            logical_name: "id".to_string(),
            db_name: "id".to_string(),
            field_type: ResolvedFieldType::Scalar(ScalarType::Int),
            is_required: true,
            is_array: false,
            storage_strategy: None,
            default_value: None,
            is_unique: false,
            is_updated_at: false,
            computed: None,
            check: None,
            span: Span::new(0, 0),
        };
        let marker = field_marker(&model, &field);
        assert_eq!(marker.table, "users");
        assert_eq!(marker.name, "id");
    }

    #[test]
    fn build_relation_map_uses_relation_names_for_multiple_inverse_relations() {
        let schema = r#"
model User {
  id            Int    @id @default(autoincrement())
  authoredPosts Post[] @relation(name: "AuthoredPosts")
  reviewedPosts Post[] @relation(name: "ReviewedPosts")
}

model Post {
  id         Int  @id @default(autoincrement())
  authorId   Int  @map("author_id")
  reviewerId Int  @map("reviewer_id")
  author     User @relation(name: "AuthoredPosts", fields: [authorId], references: [id])
  reviewer   User @relation(name: "ReviewedPosts", fields: [reviewerId], references: [id])
}
"#;
        let ir = parse_ir(schema);
        let user_model = ir.models.get("User").expect("User model missing");
        let relation_map =
            build_relation_map(user_model, &ir.models).expect("relation map should build");

        let authored = relation_map
            .get("authored_posts")
            .expect("authored_posts relation missing");
        assert_eq!(authored.fk_db, "author_id");
        assert_eq!(authored.pk_db, "id");

        let reviewed = relation_map
            .get("reviewed_posts")
            .expect("reviewed_posts relation missing");
        assert_eq!(reviewed.fk_db, "reviewer_id");
        assert_eq!(reviewed.pk_db, "id");
    }

    #[test]
    fn build_relation_map_handles_named_self_relations() {
        let node_model = ModelIr {
            logical_name: "Node".to_string(),
            db_name: "nodes".to_string(),
            fields: vec![
                scalar_field("id", "id"),
                scalar_field("parentId", "parent_id"),
                relation_field(
                    "parent",
                    "Node",
                    &["parentId"],
                    &["id"],
                    Some("Tree"),
                    false,
                ),
                relation_field("children", "Node", &[], &[], Some("Tree"), true),
            ],
            primary_key: PrimaryKeyIr::Single("id".to_string()),
            unique_constraints: vec![],
            indexes: vec![],
            check_constraints: vec![],
            span: Span::new(0, 0),
        };
        let mut models = HashMap::new();
        models.insert(node_model.logical_name.clone(), node_model.clone());
        let relation_map =
            build_relation_map(&node_model, &models).expect("relation map should build");

        let children = relation_map
            .get("children")
            .expect("children relation missing");
        assert_eq!(children.fk_db, "parent_id");
        assert_eq!(children.pk_db, "id");
    }

    #[test]
    fn build_relation_map_rejects_ambiguous_array_inverses() {
        let user_model = ModelIr {
            logical_name: "User".to_string(),
            db_name: "users".to_string(),
            fields: vec![
                scalar_field("id", "id"),
                relation_field("posts", "Post", &[], &[], None, true),
            ],
            primary_key: PrimaryKeyIr::Single("id".to_string()),
            unique_constraints: vec![],
            indexes: vec![],
            check_constraints: vec![],
            span: Span::new(0, 0),
        };
        let post_model = ModelIr {
            logical_name: "Post".to_string(),
            db_name: "posts".to_string(),
            fields: vec![
                scalar_field("id", "id"),
                scalar_field("authorId", "author_id"),
                scalar_field("reviewerId", "reviewer_id"),
                relation_field("author", "User", &["authorId"], &["id"], None, false),
                relation_field("reviewer", "User", &["reviewerId"], &["id"], None, false),
            ],
            primary_key: PrimaryKeyIr::Single("id".to_string()),
            unique_constraints: vec![],
            indexes: vec![],
            check_constraints: vec![],
            span: Span::new(0, 0),
        };

        let mut models = HashMap::new();
        models.insert(user_model.logical_name.clone(), user_model.clone());
        models.insert(post_model.logical_name.clone(), post_model);

        match build_relation_map(&user_model, &models) {
            Err(ProtocolError::QueryPlanning(message)) => {
                assert!(
                    message.contains("ambiguous inverse relation"),
                    "unexpected error message: {message}"
                );
            }
            Ok(map) => panic!("expected ambiguous inverse relation error, got {map:?}"),
            Err(other) => panic!("unexpected error: {other:?}"),
        }
    }
}
