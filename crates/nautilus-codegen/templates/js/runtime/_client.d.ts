// Runtime file — do not edit manually.

import { IsolationLevel, TransactionClient } from './_transaction.js';
import type { EngineProcess, EnginePoolOptions } from './_engine.js';

export interface TransactionBatchOperation {
  method: string;
  params: Record<string, unknown>;
}

export interface NautilusClientOptions {
  migrate?: boolean;
  pool?: EnginePoolOptions;
}

export declare class NautilusClient {
  protected readonly engine: EngineProcess;
  _delegates: Record<string, unknown>;
  constructor(schemaPath: string, options?: NautilusClientOptions);
  connect(): Promise<void>;
  disconnect(): Promise<void>;
  _rpc(method: string, params: Record<string, unknown>): Promise<unknown>;
  _streamRpc(
    method: string,
    params: Record<string, unknown>,
    timeoutMs?: number,
  ): AsyncIterable<unknown>;
  protected _startTransaction(timeoutMs?: number, isolationLevel?: IsolationLevel): Promise<string>;
  protected _commitTransaction(txId: string): Promise<void>;
  protected _rollbackTransaction(txId: string): Promise<void>;
  protected _runTransactionCallback<T>(
    fn: (tx: TransactionClient) => Promise<T>,
    options?: { timeout?: number; isolationLevel?: IsolationLevel },
  ): Promise<T>;
  protected _runTransactionBatch(
    operations: TransactionBatchOperation[],
    options?: { timeout?: number; isolationLevel?: IsolationLevel },
  ): Promise<unknown[]>;
}
