use nautilus_engine::{handlers, EngineState};
use nautilus_protocol::{RpcId, RpcRequest, PROTOCOL_VERSION, SCHEMA_VALIDATE};
use nautilus_schema::validate_schema_source;
use serde_json::json;
use tempfile::TempDir;

fn parse_ir(source: &str) -> nautilus_schema::ir::SchemaIr {
    validate_schema_source(source)
        .expect("validation failed")
        .ir
}

fn test_db_url() -> (String, TempDir) {
    let dir = tempfile::Builder::new()
        .prefix("schema-validate-tests")
        .tempdir()
        .expect("failed to create sqlite test directory");

    let path = dir.path().join("test.db");
    std::fs::File::create(&path).expect("failed to create sqlite test file");
    let url = format!("sqlite:///{}", path.to_string_lossy().replace('\\', "/"));
    (url, dir)
}

async fn sqlite_state() -> (EngineState, TempDir) {
    let schema = parse_ir(
        r#"
datasource db {
  provider = "sqlite"
  url      = "sqlite::memory:"
}

model Existing {
  id Int @id @default(autoincrement())
}
"#,
    );
    let (database_url, temp_dir) = test_db_url();
    let state = EngineState::new(schema, database_url, None)
        .await
        .expect("failed to create engine state");
    (state, temp_dir)
}

async fn call_schema_validate(state: &EngineState, schema: &str) -> nautilus_protocol::RpcResponse {
    handlers::handle_request_inline(
        state,
        RpcRequest {
            jsonrpc: "2.0".to_string(),
            id: Some(RpcId::Number(1)),
            method: SCHEMA_VALIDATE.to_string(),
            params: json!({
                "protocolVersion": PROTOCOL_VERSION,
                "schema": schema,
            }),
        },
    )
    .await
}

#[tokio::test]
async fn schema_validate_returns_valid_true_for_valid_schema() {
    let (state, temp_dir) = sqlite_state().await;

    let response = call_schema_validate(
        &state,
        r#"
model User {
  id    Int    @id
  email String @unique
}
"#,
    )
    .await;

    assert!(
        response.error.is_none(),
        "unexpected rpc error: {response:?}"
    );
    let payload: serde_json::Value =
        serde_json::from_str(response.result.expect("missing rpc result").get())
            .expect("failed to parse rpc result");
    assert_eq!(payload["valid"], json!(true));
    assert!(payload.get("errors").is_none() || payload["errors"].is_null());

    drop(state);
    drop(temp_dir);
}

#[tokio::test]
async fn schema_validate_returns_errors_for_invalid_schema() {
    let (state, temp_dir) = sqlite_state().await;

    let response = call_schema_validate(
        &state,
        r#"
model User {
  id     Int @id
  friend Foo
}
"#,
    )
    .await;

    assert!(
        response.error.is_none(),
        "unexpected rpc error: {response:?}"
    );
    let payload: serde_json::Value =
        serde_json::from_str(response.result.expect("missing rpc result").get())
            .expect("failed to parse rpc result");
    assert_eq!(payload["valid"], json!(false));
    let errors = payload["errors"]
        .as_array()
        .expect("invalid schema should return errors");
    assert!(
        errors.iter().any(|item| item
            .as_str()
            .is_some_and(|message| message.contains("Unknown type"))),
        "expected unknown type error, got: {errors:?}"
    );

    drop(state);
    drop(temp_dir);
}
