//! # Nautilus Protocol
//!
//! JSON-RPC 2.0 protocol for multi-language Nautilus clients.
//!
//! This crate defines the stable wire format for communication between:
//! - Language-specific clients (JavaScript, Python, etc.)
//! - The Nautilus engine (Rust binary running over stdin/stdout)
//!
//! ## Protocol Overview
//!
//! - **Transport**: Line-delimited JSON over stdin/stdout
//! - **Format**: JSON-RPC 2.0
//! - **Versioning**: Protocol version included in every request
//!
//! ## Example Usage
//!
//! ```rust
//! use nautilus_protocol::*;
//! use serde_json::json;
//! let request = RpcRequest {
//!     jsonrpc: "2.0".to_string(),
//!     id: Some(RpcId::Number(1)),
//!     method: ENGINE_HANDSHAKE.to_string(),
//!     params: serde_json::to_value(HandshakeParams {
//!         protocol_version: PROTOCOL_VERSION,
//!         client_name: Some("nautilus-js".to_string()),
//!         client_version: Some("0.1.0".to_string()),
//!     }).unwrap(),
//! };
//! let json = serde_json::to_string(&request).unwrap();
//! ```

#![forbid(unsafe_code)]

pub mod error;
pub mod methods;
pub mod version;
pub mod wire;

pub use error::{BatchOperationErrorData, ProtocolError, ProtocolErrorCause, Result};
pub use methods::{
    BatchOperation, CountParams, CreateManyParams, CreateParams, DeleteParams,
    FindFirstOrThrowParams, FindFirstParams, FindManyParams, FindUniqueOrThrowParams,
    FindUniqueParams, GroupByParams, HandshakeParams, HandshakeResult, IsolationLevel,
    MutationResult, QueryResult, RawQueryParams, RawStmtQueryParams, RequestCancelParams,
    RequestCancelResult, SchemaValidateParams, SchemaValidateResult, TransactionBatchParams,
    TransactionBatchResult, TransactionCommitParams, TransactionCommitResult,
    TransactionRollbackParams, TransactionRollbackResult, TransactionStartParams,
    TransactionStartResult, UpdateParams, ENGINE_HANDSHAKE, QUERY_COUNT, QUERY_CREATE,
    QUERY_CREATE_MANY, QUERY_DELETE, QUERY_FIND_FIRST, QUERY_FIND_FIRST_OR_THROW, QUERY_FIND_MANY,
    QUERY_FIND_UNIQUE, QUERY_FIND_UNIQUE_OR_THROW, QUERY_GROUP_BY, QUERY_RAW, QUERY_RAW_STMT,
    QUERY_UPDATE, REQUEST_CANCEL, SCHEMA_VALIDATE, TRANSACTION_BATCH, TRANSACTION_COMMIT,
    TRANSACTION_ROLLBACK, TRANSACTION_START,
};
pub use version::{ProtocolVersion, MIN_PROTOCOL_VERSION, PROTOCOL_VERSION};
pub use wire::{RpcError, RpcId, RpcRequest, RpcResponse};
