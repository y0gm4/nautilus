mod common;

use common::{call_rpc_json, sqlite_state};
use nautilus_engine::handlers;
use nautilus_protocol::{
    RpcId, RpcRequest, RpcResponse, PROTOCOL_VERSION, QUERY_CREATE, QUERY_FIND_MANY,
};
use serde_json::json;
use tokio::sync::mpsc;

fn parse_result(response: RpcResponse) -> serde_json::Value {
    if let Some(error) = response.error {
        panic!(
            "streaming response failed ({}): {}",
            error.code, error.message
        );
    }

    serde_json::from_str(response.result.expect("missing rpc result").get())
        .expect("failed to parse rpc result")
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

fn schema_with_relation_source() -> &'static str {
    r#"
datasource db {
  provider = "sqlite"
  url      = "sqlite::memory:"
}

model User {
  id    Int    @id @default(autoincrement())
  name  String
  posts Post[]
}

model Post {
  id      Int    @id @default(autoincrement())
  title   String
  user_id Int
  user    User   @relation(fields: [user_id], references: [id], onDelete: Cascade)
}
"#
}

#[tokio::test]
async fn find_many_chunk_size_emits_partial_responses_in_order() {
    let (state, temp_dir) = sqlite_state("streaming-find-many-tests", schema_source()).await;

    for name in ["Alice", "Bob", "Cara"] {
        let _ = call_rpc_json(
            &state,
            QUERY_CREATE,
            json!({
                "protocolVersion": PROTOCOL_VERSION,
                "model": "User",
                "data": {
                    "name": name
                }
            }),
        )
        .await;
    }

    let (tx, mut rx) = mpsc::channel(8);
    let final_response = handlers::handle_request(
        &state,
        RpcRequest {
            jsonrpc: "2.0".to_string(),
            id: Some(RpcId::String("chunked-find-many".to_string())),
            method: QUERY_FIND_MANY.to_string(),
            params: json!({
                "protocolVersion": PROTOCOL_VERSION,
                "model": "User",
                "args": {
                    "orderBy": [
                        { "id": "asc" }
                    ]
                },
                "chunkSize": 1
            }),
        },
        tx,
    )
    .await;

    let first_partial = rx.recv().await.expect("missing first partial response");
    let second_partial = rx.recv().await.expect("missing second partial response");
    let final_json = parse_result(final_response);
    let first_json = parse_result(first_partial.clone());
    let second_json = parse_result(second_partial.clone());

    assert_eq!(first_partial.partial, Some(true));
    assert_eq!(second_partial.partial, Some(true));
    assert_eq!(
        first_partial.id,
        Some(RpcId::String("chunked-find-many".to_string()))
    );
    assert_eq!(
        second_partial.id,
        Some(RpcId::String("chunked-find-many".to_string()))
    );

    assert_eq!(
        first_json["data"]
            .as_array()
            .expect("first chunk rows")
            .len(),
        1
    );
    assert_eq!(
        second_json["data"]
            .as_array()
            .expect("second chunk rows")
            .len(),
        1
    );
    assert_eq!(
        final_json["data"]
            .as_array()
            .expect("final chunk rows")
            .len(),
        1
    );

    assert_eq!(first_json["data"][0]["User__name"], json!("Alice"));
    assert_eq!(second_json["data"][0]["User__name"], json!("Bob"));
    assert_eq!(final_json["data"][0]["User__name"], json!("Cara"));

    drop(state);
    drop(temp_dir);
}

/// When `chunk_size` evenly divides the row count, the engine still returns the
/// last filled chunk as the *final* (non-partial) reply, never an empty trailer.
/// This pins the boundary behaviour of `stream_find_many_chunked` because the
/// streaming path can't peek ahead of the underlying `RowStream`.
#[tokio::test]
async fn find_many_chunk_size_aligned_to_row_count_ends_with_full_final_chunk() {
    let (state, temp_dir) =
        sqlite_state("streaming-find-many-aligned-tests", schema_source()).await;

    for name in ["Alice", "Bob", "Cara", "Dora"] {
        let _ = call_rpc_json(
            &state,
            QUERY_CREATE,
            json!({
                "protocolVersion": PROTOCOL_VERSION,
                "model": "User",
                "data": { "name": name }
            }),
        )
        .await;
    }

    let (tx, mut rx) = mpsc::channel(8);
    let final_response = handlers::handle_request(
        &state,
        RpcRequest {
            jsonrpc: "2.0".to_string(),
            id: Some(RpcId::String("aligned".to_string())),
            method: QUERY_FIND_MANY.to_string(),
            params: json!({
                "protocolVersion": PROTOCOL_VERSION,
                "model": "User",
                "args": { "orderBy": [{ "id": "asc" }] },
                "chunkSize": 2,
            }),
        },
        tx,
    )
    .await;

    let partial = rx.recv().await.expect("missing partial response");
    assert_eq!(partial.partial, Some(true));
    let partial_json = parse_result(partial);
    let final_json = parse_result(final_response);

    assert_eq!(partial_json["data"].as_array().unwrap().len(), 2);
    assert_eq!(final_json["data"].as_array().unwrap().len(), 2);
    assert_eq!(partial_json["data"][0]["User__name"], json!("Alice"));
    assert_eq!(partial_json["data"][1]["User__name"], json!("Bob"));
    assert_eq!(final_json["data"][0]["User__name"], json!("Cara"));
    assert_eq!(final_json["data"][1]["User__name"], json!("Dora"));

    // No further partials.
    assert!(rx.try_recv().is_err());

    drop(state);
    drop(temp_dir);
}

/// Backward pagination cannot stream because the row order is reversed in
/// memory after the fetch. The engine must fall back to the buffered chunking
/// path and still emit chunked responses correctly.
#[tokio::test]
async fn find_many_with_backward_pagination_falls_back_to_buffered_chunking() {
    let (state, temp_dir) =
        sqlite_state("streaming-find-many-backward-tests", schema_source()).await;

    for name in ["Alice", "Bob", "Cara"] {
        let _ = call_rpc_json(
            &state,
            QUERY_CREATE,
            json!({
                "protocolVersion": PROTOCOL_VERSION,
                "model": "User",
                "data": { "name": name }
            }),
        )
        .await;
    }

    let (tx, mut rx) = mpsc::channel(8);
    let final_response = handlers::handle_request(
        &state,
        RpcRequest {
            jsonrpc: "2.0".to_string(),
            id: Some(RpcId::String("backward".to_string())),
            method: QUERY_FIND_MANY.to_string(),
            params: json!({
                "protocolVersion": PROTOCOL_VERSION,
                "model": "User",
                "args": {
                    "orderBy": [{ "id": "asc" }],
                    "take": -3
                },
                "chunkSize": 1,
            }),
        },
        tx,
    )
    .await;

    let mut partials = Vec::new();
    while let Ok(msg) = rx.try_recv() {
        partials.push(msg);
    }

    let final_json = parse_result(final_response);
    assert_eq!(partials.len(), 2, "expected 2 partial chunks");
    let first = parse_result(partials.remove(0));
    let second = parse_result(partials.remove(0));

    // Backward pagination fetches DESC then reverses to restore natural order,
    // so the buffered chunking path emits Alice → Bob → Cara even though the
    // query is logically a backward window.
    assert_eq!(first["data"][0]["User__name"], json!("Alice"));
    assert_eq!(second["data"][0]["User__name"], json!("Bob"));
    assert_eq!(final_json["data"][0]["User__name"], json!("Cara"));

    drop(state);
    drop(temp_dir);
}

/// If the response channel is closed mid-stream (consumer disconnect), the
/// streaming path must surface an internal error. Dropping the underlying
/// `RowStream` (when this function returns) triggers the worker drain so the
/// connection returns to the pool clean.
#[tokio::test]
async fn find_many_streaming_propagates_consumer_disconnect_as_error() {
    let (state, temp_dir) = sqlite_state("streaming-find-many-cancel-tests", schema_source()).await;

    for name in ["Alice", "Bob", "Cara", "Dora", "Eve"] {
        let _ = call_rpc_json(
            &state,
            QUERY_CREATE,
            json!({
                "protocolVersion": PROTOCOL_VERSION,
                "model": "User",
                "data": { "name": name }
            }),
        )
        .await;
    }

    // Capacity 0 with the receiver dropped immediately: the very first
    // partial-frame send will fail.
    let (tx, rx) = mpsc::channel(1);
    drop(rx);

    let response = handlers::handle_request(
        &state,
        RpcRequest {
            jsonrpc: "2.0".to_string(),
            id: Some(RpcId::String("cancel".to_string())),
            method: QUERY_FIND_MANY.to_string(),
            params: json!({
                "protocolVersion": PROTOCOL_VERSION,
                "model": "User",
                "args": { "orderBy": [{ "id": "asc" }] },
                "chunkSize": 1,
            }),
        },
        tx,
    )
    .await;

    let err = response.error.expect("expected an error response");
    assert!(
        err.message.contains("Channel closed"),
        "expected Channel-closed message, got: {}",
        err.message
    );

    // After the error, subsequent queries on the same state must succeed —
    // proves the worker drained the connection before releasing it.
    let follow_up = call_rpc_json(
        &state,
        QUERY_FIND_MANY,
        json!({
            "protocolVersion": PROTOCOL_VERSION,
            "model": "User",
            "args": { "orderBy": [{ "id": "asc" }], "take": 1 }
        }),
    )
    .await;
    assert_eq!(follow_up["data"].as_array().unwrap().len(), 1);

    drop(state);
    drop(temp_dir);
}

/// `include` requires hydrating relations after the parent rows load (the
/// child query is keyed on the parent PKs), so the engine cannot stream parent
/// rows row-by-row. The streaming dispatcher must fall back to the buffered
/// chunking path while still respecting `chunkSize` on the wire.
#[tokio::test]
async fn find_many_with_include_falls_back_to_buffered_chunking() {
    let (state, temp_dir) = sqlite_state(
        "streaming-find-many-include-tests",
        schema_with_relation_source(),
    )
    .await;

    for name in ["Alice", "Bob"] {
        let user = call_rpc_json(
            &state,
            QUERY_CREATE,
            json!({
                "protocolVersion": PROTOCOL_VERSION,
                "model": "User",
                "data": { "name": name }
            }),
        )
        .await;
        let user_id = user["data"][0]["User__id"].as_i64().unwrap();
        let _ = call_rpc_json(
            &state,
            QUERY_CREATE,
            json!({
                "protocolVersion": PROTOCOL_VERSION,
                "model": "Post",
                "data": { "title": format!("{}-post", name), "user_id": user_id }
            }),
        )
        .await;
    }

    let (tx, mut rx) = mpsc::channel(8);
    let final_response = handlers::handle_request(
        &state,
        RpcRequest {
            jsonrpc: "2.0".to_string(),
            id: Some(RpcId::String("include".to_string())),
            method: QUERY_FIND_MANY.to_string(),
            params: json!({
                "protocolVersion": PROTOCOL_VERSION,
                "model": "User",
                "args": {
                    "orderBy": [{ "id": "asc" }],
                    "include": { "posts": true }
                },
                "chunkSize": 1,
            }),
        },
        tx,
    )
    .await;

    let partial = rx.recv().await.expect("missing partial response");
    assert_eq!(partial.partial, Some(true));
    let partial_json = parse_result(partial);
    let final_json = parse_result(final_response);

    assert_eq!(partial_json["data"][0]["User__name"], json!("Alice"));
    assert_eq!(final_json["data"][0]["User__name"], json!("Bob"));

    // Hydrated relations must be present on both rows — buffered fallback
    // ran the include batch query before chunking the rows out.
    assert!(partial_json["data"][0]
        .get("posts_json")
        .map(|value| !value.is_null())
        .unwrap_or(false));
    assert!(final_json["data"][0]
        .get("posts_json")
        .map(|value| !value.is_null())
        .unwrap_or(false));

    drop(state);
    drop(temp_dir);
}

/// Larger dataset + early break: simulates a client that consumes only the
/// first chunk and disconnects. The streaming path must surface the disconnect
/// as an error and the worker must drain the underlying sqlx stream so the
/// next query on the same state still works.
#[tokio::test]
async fn find_many_streaming_early_break_releases_connection() {
    let (state, temp_dir) =
        sqlite_state("streaming-find-many-early-break-tests", schema_source()).await;

    for i in 0..200 {
        let _ = call_rpc_json(
            &state,
            QUERY_CREATE,
            json!({
                "protocolVersion": PROTOCOL_VERSION,
                "model": "User",
                "data": { "name": format!("User-{:03}", i) }
            }),
        )
        .await;
    }

    // Capacity 1: the engine fills the first partial frame, sends it, and
    // continues filling chunks. We pull only the first frame and then drop
    // the receiver, simulating a client that stopped reading after a single
    // chunk. The engine's next send fails, which trips the error path and
    // (crucially) drops the underlying `RowStream`, kicking off the worker
    // drain that releases the connection clean.
    let (tx, mut rx) = mpsc::channel(1);

    let request = RpcRequest {
        jsonrpc: "2.0".to_string(),
        id: Some(RpcId::String("early-break".to_string())),
        method: QUERY_FIND_MANY.to_string(),
        params: json!({
            "protocolVersion": PROTOCOL_VERSION,
            "model": "User",
            "args": { "orderBy": [{ "id": "asc" }] },
            "chunkSize": 10,
        }),
    };

    let engine_fut = handlers::handle_request(&state, request, tx);
    let consumer_fut = async {
        let first = rx.recv().await.expect("missing first partial");
        drop(rx);
        first
    };

    let (response, first) = tokio::join!(engine_fut, consumer_fut);
    assert_eq!(first.partial, Some(true));
    let err = response.error.expect("expected error after early break");
    assert!(err.message.contains("Channel closed"));

    // Pool is still usable: the worker drained the underlying stream before
    // releasing the connection, even though the engine bailed out early.
    let follow_up = call_rpc_json(
        &state,
        QUERY_FIND_MANY,
        json!({
            "protocolVersion": PROTOCOL_VERSION,
            "model": "User",
            "args": { "orderBy": [{ "id": "asc" }], "take": 5 }
        }),
    )
    .await;
    assert_eq!(follow_up["data"].as_array().unwrap().len(), 5);

    drop(state);
    drop(temp_dir);
}
