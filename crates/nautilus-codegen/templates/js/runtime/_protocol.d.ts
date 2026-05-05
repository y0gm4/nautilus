// Runtime file — do not edit manually.

export declare const PROTOCOL_VERSION: {{ protocol_version }};

export interface JsonRpcRequest {
  jsonrpc: '2.0';
  id?: number;
  method: string;
  params?: Record<string, unknown>;
}

export interface JsonRpcError {
  code: number;
  message: string;
  data?: unknown;
}

export interface JsonRpcResponse {
  jsonrpc: string;
  id?: number | null;
  result?: unknown;
  error?: JsonRpcError;
  /** True when this response is a partial chunk of a larger streamed result. */
  partial?: boolean;
}
