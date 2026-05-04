mod common;

use common::sqlite_state;
use std::time::Duration;

use nautilus_core::Value;
use nautilus_dialect::Sql;
use nautilus_engine::{handlers, EngineState};
use nautilus_protocol::{
    error::ERR_TRANSACTION_TIMEOUT, RpcRequest, PROTOCOL_VERSION, TRANSACTION_COMMIT,
};

fn schema_source() -> &'static str {
    r#"
datasource db {
  provider = "sqlite"
  url      = "sqlite::memory:"
}

model User {
  id   Int    @id @default(autoincrement())
  name String
}
"#
}

fn insert_user_sql(name: &str) -> Sql {
    Sql {
        text: r#"INSERT INTO "User" ("name") VALUES (?)"#.to_string(),
        params: vec![Value::String(name.to_string())],
    }
}

async fn count_users(state: &EngineState) -> usize {
    let sql = Sql {
        text: r#"SELECT "id" FROM "User""#.to_string(),
        params: vec![],
    };
    state
        .execute_query_on(&sql, "count users", None)
        .await
        .expect("count query should succeed")
        .len()
}

#[tokio::test]
async fn transaction_commit_rpc_preserves_timeout_error_code() {
    let (state, temp_dir) = sqlite_state("transaction-timeout-tests", schema_source()).await;
    let tx_id = "rpc-timeout".to_string();

    state
        .begin_transaction(tx_id.clone(), Duration::from_millis(10), None)
        .await
        .expect("transaction should start");
    state
        .execute_affected_on(&insert_user_sql("Alice"), "insert user", Some(&tx_id))
        .await
        .expect("insert inside tx should succeed");

    tokio::time::sleep(Duration::from_millis(30)).await;

    let response = handlers::handle_request_inline(
        &state,
        RpcRequest {
            jsonrpc: "2.0".to_string(),
            id: None,
            method: TRANSACTION_COMMIT.to_string(),
            params: serde_json::json!({
                "protocolVersion": PROTOCOL_VERSION,
                "id": tx_id
            }),
        },
    )
    .await;

    let error = response.error.expect("commit should fail");
    assert_eq!(error.code, ERR_TRANSACTION_TIMEOUT);
    assert!(error.message.contains("timed out"));
    assert_eq!(count_users(&state).await, 0);

    drop(state);
    drop(temp_dir);
}
