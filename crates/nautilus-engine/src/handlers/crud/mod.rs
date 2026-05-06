//! CRUD query handlers: findMany, findFirst, findUnique, create, createMany, update, delete
//! and their `*OrThrow` variants.

use nautilus_connector::Row;
use nautilus_core::{
    build_cursor_predicate, Delete, DeleteCapacity, Expr, Insert, InsertCapacity, OrderDir, Select,
    SelectCapacity, SelectItem, Update, UpdateCapacity, Value,
};
use nautilus_dialect::Sql;
use nautilus_protocol::wire::ok_partial;
use nautilus_protocol::{
    CountParams, CreateManyParams, CreateParams, DeleteParams, FindFirstParams, FindManyParams,
    FindUniqueParams, GroupByParams, ProtocolError, RpcRequest, RpcResponse, UpdateParams,
};
use nautilus_schema::ir::{DefaultValue, FieldIr, ModelIr, ResolvedFieldType};
use serde_json::{Map as JsonMap, Value as JsonValue};
use tokio::sync::mpsc;

use super::{field_marker, get_model_or_error};
use crate::conversion::{
    check_protocol_version, json_to_value, json_to_value_field, normalize_rows_with_hints,
    rows_to_raw_json, ValueHint,
};
use crate::filter::{parse_group_by_order_by, parse_having, qualify_filter_columns, QueryArgs};
use crate::state::EngineState;

mod aggregation;
mod common;
mod include;
mod mutations;
mod raw;
mod read;

pub(super) async fn handle_find_many(
    state: &EngineState,
    request: RpcRequest,
    sender: Option<mpsc::Sender<RpcResponse>>,
) -> Result<Box<serde_json::value::RawValue>, ProtocolError> {
    read::handle_find_many(state, request, sender).await
}

pub(super) async fn handle_find_many_embedded(
    state: &EngineState,
    request: RpcRequest,
) -> Result<Vec<Row>, ProtocolError> {
    read::handle_find_many_embedded(state, request).await
}

pub(super) async fn handle_find_many_typed(
    state: &EngineState,
    model_name: &str,
    args: &nautilus_core::FindManyArgs,
    transaction_id: Option<&str>,
) -> Result<Vec<Row>, ProtocolError> {
    read::execute_find_many_typed(state, model_name, args, transaction_id).await
}

pub(super) async fn handle_find_first(
    state: &EngineState,
    request: RpcRequest,
) -> Result<Box<serde_json::value::RawValue>, ProtocolError> {
    read::handle_find_first(state, request).await
}

pub(super) async fn handle_find_unique(
    state: &EngineState,
    request: RpcRequest,
) -> Result<Box<serde_json::value::RawValue>, ProtocolError> {
    read::handle_find_unique(state, request).await
}

pub(super) async fn handle_find_unique_typed(
    state: &EngineState,
    model_name: &str,
    args: &nautilus_core::FindUniqueArgs,
    transaction_id: Option<&str>,
) -> Result<Vec<Row>, ProtocolError> {
    read::execute_find_unique_typed(state, model_name, args, transaction_id).await
}

pub(super) async fn handle_find_unique_or_throw(
    state: &EngineState,
    request: RpcRequest,
) -> Result<Box<serde_json::value::RawValue>, ProtocolError> {
    read::handle_find_unique_or_throw(state, request).await
}

pub(super) async fn handle_find_first_or_throw(
    state: &EngineState,
    request: RpcRequest,
) -> Result<Box<serde_json::value::RawValue>, ProtocolError> {
    read::handle_find_first_or_throw(state, request).await
}

pub(super) async fn handle_create(
    state: &EngineState,
    request: RpcRequest,
) -> Result<Box<serde_json::value::RawValue>, ProtocolError> {
    mutations::handle_create(state, request).await
}

pub(super) async fn handle_create_embedded(
    state: &EngineState,
    request: RpcRequest,
) -> Result<Vec<Row>, ProtocolError> {
    mutations::handle_create_embedded(state, request).await
}

pub(super) async fn handle_create_typed(
    state: &EngineState,
    params: CreateParams,
) -> Result<Vec<Row>, ProtocolError> {
    mutations::handle_create_typed(state, params).await
}

pub(super) async fn handle_create_many(
    state: &EngineState,
    request: RpcRequest,
) -> Result<Box<serde_json::value::RawValue>, ProtocolError> {
    mutations::handle_create_many(state, request).await
}

pub(super) async fn handle_create_many_embedded(
    state: &EngineState,
    request: RpcRequest,
) -> Result<Vec<Row>, ProtocolError> {
    mutations::handle_create_many_embedded(state, request).await
}

pub(super) async fn handle_create_many_typed(
    state: &EngineState,
    params: CreateManyParams,
) -> Result<Vec<Row>, ProtocolError> {
    mutations::handle_create_many_typed(state, params).await
}

pub(super) async fn handle_update(
    state: &EngineState,
    request: RpcRequest,
) -> Result<Box<serde_json::value::RawValue>, ProtocolError> {
    mutations::handle_update(state, request).await
}

pub(super) async fn handle_update_embedded(
    state: &EngineState,
    request: RpcRequest,
) -> Result<Vec<Row>, ProtocolError> {
    mutations::handle_update_embedded(state, request).await
}

pub(super) async fn handle_update_typed(
    state: &EngineState,
    params: UpdateParams,
) -> Result<Vec<Row>, ProtocolError> {
    mutations::handle_update_typed(state, params).await
}

pub(super) async fn handle_delete(
    state: &EngineState,
    request: RpcRequest,
) -> Result<Box<serde_json::value::RawValue>, ProtocolError> {
    mutations::handle_delete(state, request).await
}

pub(super) async fn handle_count(
    state: &EngineState,
    request: RpcRequest,
) -> Result<Box<serde_json::value::RawValue>, ProtocolError> {
    read::handle_count(state, request).await
}

pub(super) async fn handle_count_embedded(
    state: &EngineState,
    request: RpcRequest,
) -> Result<i64, ProtocolError> {
    read::handle_count_embedded(state, request).await
}

pub(super) async fn handle_count_typed(
    state: &EngineState,
    params: CountParams,
) -> Result<i64, ProtocolError> {
    read::handle_count_typed(state, params).await
}

pub(super) async fn handle_group_by(
    state: &EngineState,
    request: RpcRequest,
) -> Result<Box<serde_json::value::RawValue>, ProtocolError> {
    aggregation::handle_group_by(state, request).await
}

pub(super) async fn handle_group_by_embedded(
    state: &EngineState,
    request: RpcRequest,
) -> Result<Vec<Row>, ProtocolError> {
    aggregation::handle_group_by_embedded(state, request).await
}

pub(super) async fn handle_group_by_typed(
    state: &EngineState,
    params: GroupByParams,
) -> Result<Vec<Row>, ProtocolError> {
    aggregation::handle_group_by_typed(state, params).await
}

pub(super) async fn handle_raw_query(
    state: &EngineState,
    request: RpcRequest,
) -> Result<Box<serde_json::value::RawValue>, ProtocolError> {
    raw::handle_raw_query(state, request).await
}

pub(super) async fn handle_raw_stmt_query(
    state: &EngineState,
    request: RpcRequest,
) -> Result<Box<serde_json::value::RawValue>, ProtocolError> {
    raw::handle_raw_stmt_query(state, request).await
}
