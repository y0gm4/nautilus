//! Nautilus protocol method definitions.
//!
//! This module defines stable method names and their request/response payloads.

use crate::wire::RpcId;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use serde_json::Value;

fn default_true() -> bool {
    true
}

pub const ENGINE_HANDSHAKE: &str = "engine.handshake";
pub const QUERY_FIND_MANY: &str = "query.findMany";
pub const QUERY_FIND_FIRST: &str = "query.findFirst";
pub const QUERY_FIND_UNIQUE: &str = "query.findUnique";
pub const QUERY_FIND_UNIQUE_OR_THROW: &str = "query.findUniqueOrThrow";
pub const QUERY_FIND_FIRST_OR_THROW: &str = "query.findFirstOrThrow";
pub const QUERY_CREATE: &str = "query.create";
pub const QUERY_CREATE_MANY: &str = "query.createMany";
pub const QUERY_UPDATE: &str = "query.update";
pub const QUERY_DELETE: &str = "query.delete";
pub const QUERY_COUNT: &str = "query.count";
/// Group records and compute aggregates (COUNT, AVG, SUM, MIN, MAX).
pub const QUERY_GROUP_BY: &str = "query.groupBy";
/// Method name for schema validation.
pub const SCHEMA_VALIDATE: &str = "schema.validate";
/// Cancel an in-flight request by id.
pub const REQUEST_CANCEL: &str = "request.cancel";

/// Start a new interactive transaction.
pub const TRANSACTION_START: &str = "transaction.start";
/// Commit an interactive transaction.
pub const TRANSACTION_COMMIT: &str = "transaction.commit";
/// Rollback an interactive transaction.
pub const TRANSACTION_ROLLBACK: &str = "transaction.rollback";
/// Execute a batch of operations atomically in a single transaction.
pub const TRANSACTION_BATCH: &str = "transaction.batch";

/// Handshake request parameters.
///
/// The handshake must be the first request sent by a client to validate
/// protocol compatibility.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HandshakeParams {
    /// Protocol version the client is using.
    pub protocol_version: u32,

    /// Optional client name (e.g., "nautilus-js").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_name: Option<String>,

    /// Optional client version (e.g., "0.1.0").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_version: Option<String>,
}

/// Handshake response.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HandshakeResult {
    /// Engine version.
    pub engine_version: String,

    /// Protocol version the engine supports.
    pub protocol_version: u32,
}

/// Cancel-request parameters.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RequestCancelParams {
    /// Protocol version (required in all requests).
    pub protocol_version: u32,

    /// Identifier of the in-flight request to cancel.
    pub request_id: RpcId,
}

/// Cancel-request result.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RequestCancelResult {
    /// True when a live request was found and aborted.
    pub cancelled: bool,
}

/// Find many request parameters.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FindManyParams {
    /// Protocol version (required in all requests).
    pub protocol_version: u32,

    /// Model name (e.g., "User", "Post").
    pub model: String,

    /// Query arguments (filters, ordering, pagination, etc.).
    /// Structure is flexible and parsed by the engine.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub args: Option<Value>,

    /// Optional transaction ID — if present, this query runs inside the given transaction.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transaction_id: Option<String>,

    /// Optional chunk size for streaming large result sets.
    /// When set, the engine emits multiple partial responses of at most `chunk_size` rows each.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chunk_size: Option<usize>,
}

/// Find first request parameters (same shape as FindMany — optional full args).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FindFirstParams {
    pub protocol_version: u32,
    pub model: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub args: Option<Value>,
    /// Optional transaction ID.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transaction_id: Option<String>,
}

/// Find unique request parameters.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FindUniqueParams {
    pub protocol_version: u32,
    pub model: String,
    pub filter: Value,
    /// Optional transaction ID.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transaction_id: Option<String>,
}

/// Find unique or throw request parameters (same shape as FindUnique).
pub type FindUniqueOrThrowParams = FindUniqueParams;

/// Find first or throw request parameters (same shape as FindFirst).
pub type FindFirstOrThrowParams = FindFirstParams;

/// Create request parameters.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateParams {
    pub protocol_version: u32,
    pub model: String,
    pub data: Value,
    /// Optional transaction ID.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transaction_id: Option<String>,
    /// Whether to return the created row(s). Defaults to `true`.
    #[serde(default = "default_true")]
    pub return_data: bool,
}

/// Create many request parameters.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateManyParams {
    pub protocol_version: u32,
    pub model: String,
    pub data: Vec<Value>,
    /// Optional transaction ID.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transaction_id: Option<String>,
    /// Whether to return the created row(s). Defaults to `true`.
    #[serde(default = "default_true")]
    pub return_data: bool,
}

/// Update request parameters.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateParams {
    pub protocol_version: u32,
    pub model: String,
    pub filter: Value,
    pub data: Value,
    /// Optional transaction ID.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transaction_id: Option<String>,
    /// Whether to return the updated row(s). Defaults to `true`.
    #[serde(default = "default_true")]
    pub return_data: bool,
}

/// Delete request parameters.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeleteParams {
    pub protocol_version: u32,
    pub model: String,
    pub filter: Value,
    /// Optional transaction ID.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transaction_id: Option<String>,
    /// Whether to return the deleted row(s). Defaults to `true`.
    #[serde(default = "default_true")]
    pub return_data: bool,
}

/// Count request parameters.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CountParams {
    pub protocol_version: u32,
    pub model: String,
    /// Optional query arguments (where, take, skip).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub args: Option<Value>,
    /// Optional transaction ID.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transaction_id: Option<String>,
}

/// Group-by request parameters.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GroupByParams {
    pub protocol_version: u32,
    pub model: String,
    /// Query arguments: by, where, having, take, skip, orderBy, count, avg, sum, min, max.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub args: Option<Value>,
    /// Optional transaction ID.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transaction_id: Option<String>,
}

/// Query result containing data rows.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResult {
    /// Result data as JSON objects.
    pub data: Vec<Value>,
}

/// Mutation result with count of affected rows.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MutationResult {
    /// Number of rows affected.
    pub count: usize,

    /// Optional returning data for mutations that support RETURNING.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Vec<Value>>,
}

/// Method name for executing a raw SQL query (no parameter binding).
pub const QUERY_RAW: &str = "query.rawQuery";
/// Method name for executing a raw prepared-statement query (with bound params).
pub const QUERY_RAW_STMT: &str = "query.rawStmtQuery";

/// Raw SQL query request parameters.
///
/// Execute the SQL string as-is against the database and return the result rows
/// as generic JSON objects.  No parameter binding is performed — embed literal
/// values directly in the SQL string or use [`RawStmtQueryParams`] instead.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RawQueryParams {
    /// Protocol version (required in all requests).
    pub protocol_version: u32,
    /// Raw SQL string to execute.
    pub sql: String,
    /// Optional transaction ID.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transaction_id: Option<String>,
}

/// Raw prepared-statement query request parameters.
///
/// Execute the SQL string with bound parameters and return the result rows as
/// generic JSON objects.  Use `$1`, `$2`, … (PostgreSQL) or `?` (MySQL /
/// SQLite) as placeholders; parameters are bound in the order they appear in
/// the `params` array.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RawStmtQueryParams {
    /// Protocol version (required in all requests).
    pub protocol_version: u32,
    /// Raw SQL string containing parameter placeholders.
    pub sql: String,
    /// Ordered list of parameter values to bind.
    #[serde(default)]
    pub params: Vec<Value>,
    /// Optional transaction ID.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transaction_id: Option<String>,
}

/// Schema validation request parameters.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SchemaValidateParams {
    pub protocol_version: u32,
    pub schema: String,
}

/// Schema validation result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaValidateResult {
    /// Whether the schema is valid.
    pub valid: bool,

    /// Validation errors if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub errors: Option<Vec<String>>,
}

/// Transaction isolation level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum IsolationLevel {
    /// Read uncommitted — allows dirty reads.
    ReadUncommitted,
    /// Read committed — default for most databases.
    ReadCommitted,
    /// Repeatable read — prevents non-repeatable reads.
    RepeatableRead,
    /// Serializable — strictest isolation level.
    Serializable,
}

impl IsolationLevel {
    /// Returns the SQL representation (e.g., `"READ COMMITTED"`).
    pub fn as_sql(&self) -> &'static str {
        match self {
            IsolationLevel::ReadUncommitted => "READ UNCOMMITTED",
            IsolationLevel::ReadCommitted => "READ COMMITTED",
            IsolationLevel::RepeatableRead => "REPEATABLE READ",
            IsolationLevel::Serializable => "SERIALIZABLE",
        }
    }
}

/// Start a new interactive transaction.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransactionStartParams {
    pub protocol_version: u32,
    /// Maximum duration in milliseconds before the transaction is automatically
    /// rolled back. Defaults to 5000 (5 seconds) if omitted.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
    /// Optional isolation level override.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub isolation_level: Option<IsolationLevel>,
}

/// Result of starting a transaction.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransactionStartResult {
    /// Unique transaction identifier. Pass this as `transactionId` in
    /// subsequent query requests.
    pub id: String,
}

/// Commit an interactive transaction.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransactionCommitParams {
    pub protocol_version: u32,
    /// Transaction ID returned by `transaction.start`.
    pub id: String,
}

/// Result of committing a transaction (empty on success).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionCommitResult {}

/// Rollback an interactive transaction.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransactionRollbackParams {
    pub protocol_version: u32,
    /// Transaction ID returned by `transaction.start`.
    pub id: String,
}

/// Result of rolling back a transaction (empty on success).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionRollbackResult {}

/// A single operation inside a batch transaction.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BatchOperation {
    /// JSON-RPC method name (e.g., `"query.create"`).
    pub method: String,
    /// Method-specific params (same shape as the standalone request).
    pub params: Value,
}

/// Execute multiple operations atomically in one transaction.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransactionBatchParams {
    pub protocol_version: u32,
    /// Ordered list of operations to execute.
    pub operations: Vec<BatchOperation>,
    /// Optional isolation level for the batch transaction.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub isolation_level: Option<IsolationLevel>,
    /// Optional timeout in milliseconds (default: 5000).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
}

/// Result of a batch transaction.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionBatchResult {
    /// One result per operation, in the same order as the input.
    pub results: Vec<Box<RawValue>>,
}
