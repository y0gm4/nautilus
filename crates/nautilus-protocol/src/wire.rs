//! JSON-RPC 2.0 wire types and helpers.
//!
//! This module defines the base JSON-RPC 2.0 protocol structures.

use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use serde_json::Value;

/// JSON-RPC 2.0 request identifier.
///
/// Can be a number, string, or null. The spec allows clients to omit the id
/// for notifications (requests that don't expect a response).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RpcId {
    Number(i64),
    String(String),
    Null,
}

/// JSON-RPC 2.0 request.
///
/// # Example
///
/// ```json
/// {
///   "jsonrpc": "2.0",
///   "id": 1,
///   "method": "engine.handshake",
///   "params": { "protocolVersion": 1 }
/// }
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcRequest {
    pub jsonrpc: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<RpcId>,
    pub method: String,
    pub params: Value,
}

/// JSON-RPC 2.0 response.
///
/// Either contains a `result` (success) or an `error` (failure), but never both.
///
/// # Example Success
///
/// ```json
/// {
///   "jsonrpc": "2.0",
///   "id": 1,
///   "result": { "engineVersion": "0.1.0" }
/// }
/// ```
///
/// # Example Error
///
/// ```json
/// {
///   "jsonrpc": "2.0",
///   "id": 1,
///   "error": {
///     "code": -32601,
///     "message": "Method not found"
///   }
/// }
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcResponse {
    pub jsonrpc: String,
    pub id: Option<RpcId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Box<RawValue>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<RpcError>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partial: Option<bool>,
}

/// JSON-RPC 2.0 error object.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcError {
    pub code: i32,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

pub const PARSE_ERROR: i32 = -32700;
pub const INVALID_REQUEST: i32 = -32600;
pub const METHOD_NOT_FOUND: i32 = -32601;
pub const INVALID_PARAMS: i32 = -32602;
pub const INTERNAL_ERROR: i32 = -32603;

/// Create a successful JSON-RPC response.
pub fn ok(id: Option<RpcId>, result: Box<RawValue>) -> RpcResponse {
    RpcResponse {
        jsonrpc: "2.0".to_string(),
        id,
        result: Some(result),
        error: None,
        partial: None,
    }
}

/// Create an error JSON-RPC response.
pub fn err(id: Option<RpcId>, code: i32, message: String, data: Option<Value>) -> RpcResponse {
    RpcResponse {
        jsonrpc: "2.0".to_string(),
        id,
        result: None,
        error: Some(RpcError {
            code,
            message,
            data,
        }),
        partial: None,
    }
}

/// Create a partial (chunked) JSON-RPC response. Used for streaming large result sets.
pub fn ok_partial(id: Option<RpcId>, result: Box<RawValue>) -> RpcResponse {
    RpcResponse {
        jsonrpc: "2.0".to_string(),
        id,
        result: Some(result),
        error: None,
        partial: Some(true),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_rpc_id_serialization() {
        let id = RpcId::Number(42);
        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(json, "42");
        let parsed: RpcId = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, id);

        let id = RpcId::String("abc-123".to_string());
        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(json, r#""abc-123""#);
        let parsed: RpcId = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, id);

        let id = RpcId::Null;
        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(json, "null");
        let parsed: RpcId = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, id);
    }

    #[test]
    fn test_request_serialization() {
        let request = RpcRequest {
            jsonrpc: "2.0".to_string(),
            id: Some(RpcId::Number(1)),
            method: "test.method".to_string(),
            params: json!({"key": "value"}),
        };

        let json = serde_json::to_value(&request).unwrap();
        assert_eq!(json["jsonrpc"], "2.0");
        assert_eq!(json["id"], 1);
        assert_eq!(json["method"], "test.method");
        assert_eq!(json["params"]["key"], "value");

        let parsed: RpcRequest = serde_json::from_value(json).unwrap();
        assert_eq!(parsed.jsonrpc, "2.0");
        assert_eq!(parsed.id, Some(RpcId::Number(1)));
        assert_eq!(parsed.method, "test.method");
    }

    #[test]
    fn test_response_ok() {
        let raw = serde_json::value::to_raw_value(&json!({"status": "ok"})).unwrap();
        let response = ok(Some(RpcId::Number(1)), raw);

        assert_eq!(response.jsonrpc, "2.0");
        assert_eq!(response.id, Some(RpcId::Number(1)));
        assert!(response.result.is_some());
        assert!(response.error.is_none());

        let serialized = serde_json::to_string(&response).unwrap();
        let json: serde_json::Value = serde_json::from_str(&serialized).unwrap();
        assert_eq!(json["result"]["status"], "ok");
        assert!(json.get("error").is_none());
    }

    #[test]
    fn test_response_error() {
        let response = err(
            Some(RpcId::Number(2)),
            METHOD_NOT_FOUND,
            "Method not found".to_string(),
            Some(json!({"method": "unknown.method"})),
        );

        assert_eq!(response.jsonrpc, "2.0");
        assert_eq!(response.id, Some(RpcId::Number(2)));
        assert!(response.result.is_none());
        assert!(response.error.is_some());

        let error = response.error.unwrap();
        assert_eq!(error.code, METHOD_NOT_FOUND);
        assert_eq!(error.message, "Method not found");
        assert_eq!(error.data.unwrap()["method"], "unknown.method");
    }

    #[test]
    fn test_notification_no_id() {
        let request = RpcRequest {
            jsonrpc: "2.0".to_string(),
            id: None,
            method: "notification".to_string(),
            params: json!(null),
        };

        let json = serde_json::to_value(&request).unwrap();
        assert!(json["id"].is_null() || !json.as_object().unwrap().contains_key("id"));
    }
}
