"""JSON-RPC protocol types and utilities."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, Union

PROTOCOL_VERSION = {{ protocol_version }}


@dataclass
class JsonRpcRequest:
    """JSON-RPC 2.0 request."""

    jsonrpc: str = "2.0"
    id: Optional[Union[int, str]] = None
    method: str = ""
    params: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        result: Dict[str, Any] = {
            "jsonrpc": self.jsonrpc,
            "method": self.method,
        }
        if self.id is not None:
            result["id"] = self.id
        if self.params is not None:
            result["params"] = self.params
        return result


@dataclass
class JsonRpcError:
    """JSON-RPC 2.0 error object."""

    code: int
    message: str
    data: Optional[Any] = None

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> JsonRpcError:
        """Parse from dictionary."""
        return cls(
            code=d["code"],
            message=d["message"],
            data=d.get("data"),
        )


@dataclass
class JsonRpcResponse:
    """JSON-RPC 2.0 response."""

    jsonrpc: str
    id: Optional[Union[int, str]]
    result: Optional[Any] = None
    error: Optional[JsonRpcError] = None
    partial: Optional[bool] = None

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> JsonRpcResponse:
        """Parse from dictionary."""
        error = None
        if "error" in d:
            error = JsonRpcError.from_dict(d["error"])

        return cls(
            jsonrpc=d["jsonrpc"],
            id=d.get("id"),
            result=d.get("result"),
            error=error,
            partial=d.get("partial"),
        )

    def unwrap(self) -> Any:
        """Extract result or raise error."""
        from ..errors.errors import error_from_code  # type: ignore

        if self.error:
            raise error_from_code(self.error.code, self.error.message, self.error.data)
        return self.result
