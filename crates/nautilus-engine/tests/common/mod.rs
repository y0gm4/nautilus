use nautilus_engine::{handlers, EngineState};
use nautilus_migrate::{DatabaseProvider, DdlGenerator};
use nautilus_protocol::{RpcRequest, RpcResponse};
use nautilus_schema::validate_schema_source;
use tempfile::TempDir;

pub fn parse_ir(source: &str) -> nautilus_schema::ir::SchemaIr {
    validate_schema_source(source)
        .expect("validation failed")
        .ir
}

fn sqlite_db_url(prefix: &str) -> (String, TempDir) {
    let dir = tempfile::Builder::new()
        .prefix(prefix)
        .tempdir()
        .expect("failed to create sqlite test directory");

    let path = dir.path().join("test.db");
    std::fs::File::create(&path).expect("failed to create sqlite test file");
    let url = format!("sqlite:///{}", path.to_string_lossy().replace('\\', "/"));
    (url, dir)
}

pub async fn sqlite_state(prefix: &str, schema_source: &str) -> (EngineState, TempDir) {
    let schema = parse_ir(schema_source);
    let (database_url, temp_dir) = sqlite_db_url(prefix);
    let state = EngineState::new(schema.clone(), database_url, None)
        .await
        .expect("failed to create engine state");

    let ddl = DdlGenerator::new(DatabaseProvider::Sqlite)
        .generate_create_tables(&schema)
        .expect("failed to build ddl");
    state
        .execute_ddl_sql(ddl)
        .await
        .expect("failed to apply ddl");

    (state, temp_dir)
}

#[allow(dead_code)]
pub async fn call_rpc_response(
    state: &EngineState,
    method: &str,
    params: serde_json::Value,
) -> RpcResponse {
    handlers::handle_request_inline(
        state,
        RpcRequest {
            jsonrpc: "2.0".to_string(),
            id: None,
            method: method.to_string(),
            params,
        },
    )
    .await
}

#[allow(dead_code)]
pub async fn call_rpc_json(
    state: &EngineState,
    method: &str,
    params: serde_json::Value,
) -> serde_json::Value {
    let response = call_rpc_response(state, method, params).await;
    if let Some(error) = response.error {
        panic!("RPC {method} failed ({}): {}", error.code, error.message);
    }

    serde_json::from_str(response.result.expect("missing rpc result").get())
        .expect("failed to parse rpc result")
}
