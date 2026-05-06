//! JSON-RPC 2.0 request loop over stdin/stdout.
//!
//! Reads newline-delimited JSON-RPC requests from stdin, spawns a Tokio task
//! per request for concurrent handling, and writes responses through a
//! dedicated writer task. Handler panics are caught via `catch_unwind` and
//! converted into JSON-RPC internal-error responses so the client never hangs.

use std::sync::Arc;
use std::time::Duration;
use tokio::io::{self as tokio_io, AsyncBufReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;

use nautilus_protocol::wire::err;
use nautilus_protocol::{RpcRequest, RpcResponse};

use crate::handlers;
use crate::state::EngineState;

use futures::FutureExt;
use std::panic::AssertUnwindSafe;

const TRANSACTION_REAPER_INTERVAL: Duration = Duration::from_millis(250);

fn spawn_transaction_reaper(state: Arc<EngineState>) -> JoinHandle<()> {
    spawn_transaction_reaper_with_interval(state, TRANSACTION_REAPER_INTERVAL)
}

fn spawn_transaction_reaper_with_interval(
    state: Arc<EngineState>,
    interval: Duration,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            ticker.tick().await;
            state.reap_expired_transactions().await;
        }
    })
}

/// Run the main request loop: read JSON-RPC requests from stdin, dispatch handlers, write responses to stdout
pub async fn run_request_loop(state: EngineState) -> Result<(), Box<dyn std::error::Error>> {
    let state = Arc::new(state);
    let reaper_task = spawn_transaction_reaper(Arc::clone(&state));
    let stdin = tokio_io::stdin();
    let mut reader = tokio_io::BufReader::new(stdin);
    let stdout = tokio_io::stdout();

    let (tx, mut rx) = mpsc::channel::<RpcResponse>(100);

    let writer_task = tokio::spawn(async move {
        // Buffered writer amortizes syscalls; we flush after each drained batch
        // so chunked findMany partials still reach the client promptly.
        let mut stdout = tokio_io::BufWriter::with_capacity(64 * 1024, stdout);
        let mut batch: Vec<RpcResponse> = Vec::with_capacity(32);
        let mut serialized: Vec<u8> = Vec::with_capacity(8 * 1024);

        loop {
            let received = rx.recv_many(&mut batch, 32).await;
            if received == 0 {
                break;
            }

            let mut write_failed = false;
            for response in batch.drain(..) {
                serialized.clear();
                if let Err(e) = serde_json::to_writer(&mut serialized, &response) {
                    eprintln!("[engine] Failed to serialize response: {}", e);
                    serialized.clear();
                    let fallback = err(
                        response.id.clone(),
                        -32603,
                        format!("Failed to serialize response: {}", e),
                        None,
                    );
                    if serde_json::to_writer(&mut serialized, &fallback).is_err() {
                        continue; // truly unrecoverable
                    }
                }
                serialized.push(b'\n');
                if let Err(e) = stdout.write_all(&serialized).await {
                    eprintln!("[engine] Failed to write response: {}", e);
                    write_failed = true;
                    break;
                }
            }

            if write_failed {
                break;
            }

            if let Err(e) = stdout.flush().await {
                eprintln!("[engine] Failed to flush stdout: {}", e);
                break;
            }
        }
    });

    let mut line = String::new();

    loop {
        line.clear();

        match reader.read_line(&mut line).await {
            Ok(0) => {
                eprintln!("[engine] Received EOF, shutting down");
                break;
            }
            Ok(_) => {
                let line_trimmed = line.trim();
                if line_trimmed.is_empty() {
                    continue;
                }

                let request: RpcRequest = match serde_json::from_str(line_trimmed) {
                    Ok(req) => req,
                    Err(e) => {
                        eprintln!("[engine] JSON parse error: {}", e);
                        let response = err(None, -32700, "Parse error".to_string(), None);
                        let _ = tx.send(response).await;
                        continue;
                    }
                };

                if request.jsonrpc != "2.0" {
                    let response = err(
                        request.id.clone(),
                        -32600,
                        "Invalid Request: jsonrpc must be '2.0'".to_string(),
                        None,
                    );
                    let _ = tx.send(response).await;
                    continue;
                }

                let state_ref = Arc::clone(&state);
                let tx_clone = tx.clone();

                tokio::spawn(async move {
                    let request_id = request.id.clone();
                    let response = AssertUnwindSafe(handlers::handle_request(
                        &state_ref,
                        request,
                        tx_clone.clone(),
                    ))
                    .catch_unwind()
                    .await
                    .unwrap_or_else(|panic_err| {
                        let msg = if let Some(s) = panic_err.downcast_ref::<&str>() {
                            format!("Internal engine panic: {}", s)
                        } else if let Some(s) = panic_err.downcast_ref::<String>() {
                            format!("Internal engine panic: {}", s)
                        } else {
                            "Internal engine panic (unknown)".to_string()
                        };
                        eprintln!("[engine] Handler panicked: {}", msg);
                        err(request_id, -32603, msg, None)
                    });
                    let _ = tx_clone.send(response).await;
                });
            }
            Err(e) => {
                eprintln!("[engine] Read error: {}", e);
                break;
            }
        }
    }

    drop(tx);

    reaper_task.abort();
    let _ = reaper_task.await;
    let _ = writer_task.await;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    use nautilus_core::Value;
    use nautilus_dialect::Sql;
    use nautilus_migrate::{DatabaseProvider, DdlGenerator};
    use nautilus_schema::validate_schema_source;
    use tempfile::TempDir;

    fn parse_ir(source: &str) -> nautilus_schema::ir::SchemaIr {
        validate_schema_source(source)
            .expect("validation failed")
            .ir
    }

    fn test_db_url() -> (String, TempDir) {
        let dir = tempfile::Builder::new()
            .prefix("transaction-timeout-transport-tests")
            .tempdir()
            .expect("failed to create sqlite test directory");

        let path = dir.path().join("test.db");
        fs::File::create(&path).expect("failed to create sqlite test file");
        let url = format!("sqlite:///{}", path.to_string_lossy().replace('\\', "/"));
        (url, dir)
    }

    async fn sqlite_state(schema_source: &str) -> (Arc<EngineState>, TempDir) {
        let schema = parse_ir(schema_source);
        let (database_url, temp_dir) = test_db_url();
        let state = Arc::new(
            EngineState::new(schema.clone(), database_url, None)
                .await
                .expect("failed to create engine state"),
        );

        let ddl = DdlGenerator::new(DatabaseProvider::Sqlite)
            .generate_create_tables(&schema)
            .expect("failed to build ddl");
        state
            .execute_ddl_sql(ddl)
            .await
            .expect("failed to apply ddl");

        (state, temp_dir)
    }

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
    async fn spawned_reaper_expires_idle_transactions() {
        let (state, temp_dir) = sqlite_state(schema_source()).await;
        let tx_id = "background-reaper-timeout".to_string();

        state
            .begin_transaction(tx_id.clone(), Duration::from_millis(10), None)
            .await
            .expect("transaction should start");
        state
            .execute_affected_on(&insert_user_sql("Alice"), "insert user", Some(&tx_id))
            .await
            .expect("insert inside tx should succeed");

        let reaper =
            spawn_transaction_reaper_with_interval(Arc::clone(&state), Duration::from_millis(5));

        tokio::time::sleep(Duration::from_millis(40)).await;

        let err = state
            .commit_transaction(&tx_id)
            .await
            .expect_err("background reaper should expire the idle tx");
        assert!(matches!(
            err,
            nautilus_protocol::ProtocolError::TransactionTimeout(_)
        ));
        assert_eq!(count_users(&state).await, 0);

        reaper.abort();
        let _ = reaper.await;

        drop(state);
        drop(temp_dir);
    }
}
