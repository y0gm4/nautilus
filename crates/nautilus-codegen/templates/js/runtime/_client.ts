// Runtime file — do not edit manually.

import * as readline from 'readline';

const RPC_TIMEOUT_MS        = 30_000;
const DEFAULT_TX_TIMEOUT_MS =  5_000;
const STREAM_END            = Symbol('nautilus.stream.end');
import { EngineProcess, type EnginePoolOptions } from './_engine';
import { PROTOCOL_VERSION, type JsonRpcRequest, type JsonRpcResponse } from './_protocol';
import { IsolationLevel, TransactionClient } from './_transaction';
import { errorFromCode, HandshakeError, ProtocolError } from './_errors';

export interface TransactionBatchOperation {
  method: string;
  params: Record<string, unknown>;
}

export interface NautilusClientOptions {
  migrate?: boolean;
  pool?: EnginePoolOptions;
}

interface PendingRequest {
  resolve: (value: unknown) => void;
  reject:  (error: Error)   => void;
}

type StreamQueueItem = unknown | Error | typeof STREAM_END;

interface StreamWaiter {
  resolve: (value: StreamQueueItem) => void;
  timer: ReturnType<typeof setTimeout>;
}

interface PendingStreamRequest {
  items: StreamQueueItem[];
  waiters: StreamWaiter[];
  closed: boolean;
}

/**
 * Base Nautilus client.
 *
 * Manages the engine subprocess lifecycle, multiplexes JSON-RPC
 * requests over the engine's stdin/stdout pipes, and provides the
 * transaction API that the generated `Nautilus` class builds on.
 *
 * The generated subclass adds typed delegate properties (`user`,
 * `post`, …) on top of this base.
 */
export class NautilusClient {
  protected readonly engine: EngineProcess;

  private nextId      = 0;
  private readonly pending     = new Map<number, PendingRequest>();
  private readonly partialData = new Map<number, unknown[]>();
  private readonly streams = new Map<number, PendingStreamRequest>();
  private rl: readline.Interface | null = null;

  /**
   * Named model delegates registered by the generated subclass.
   * TransactionClient reads this map to clone delegates bound to the
   * transaction's RPC channel.
   */
  _delegates: Record<string, unknown> = {};

  constructor(
    private readonly schemaPath: string,
    options?: NautilusClientOptions,
  ) {
    this.engine = new EngineProcess(
      undefined,
      options?.migrate ?? false,
      options?.pool,
    );
  }

  /** Connect to the engine and perform the protocol handshake. */
  async connect(): Promise<void> {
    if (this.engine.isRunning()) return;

    this.engine.spawn(this.schemaPath);
    this._startReading();
    await this._handshake();

    // Best-effort cleanup on unexpected process exit.
    process.once('exit', () => { this.engine.terminate().catch(() => {}); });
  }

  /** Disconnect from the engine and clean up resources. */
  async disconnect(): Promise<void> {
    this.rl?.close();
    this.rl = null;

    await this.engine.terminate();

    const err = new ProtocolError('Client disconnected');
    for (const { reject } of this.pending.values()) reject(err);
    this.pending.clear();
    this.partialData.clear();
    this._failStreams(err);
  }

  /**
   * Execute a JSON-RPC call and return the unwrapped result.
   *
   * Assigns a unique ID, writes `JSON.stringify(req) + "\n"` to stdin,
   * and returns a Promise that is resolved/rejected by the readline reader
   * when the matching response arrives.
   */
  async _rpc(method: string, params: Record<string, unknown>, timeoutMs = RPC_TIMEOUT_MS): Promise<unknown> {
    if (!this.engine.isRunning()) {
      throw new ProtocolError('Engine is not running. Call connect() first.');
    }

    const id = ++this.nextId;

    return new Promise<unknown>((resolve, reject) => {
      const timer = setTimeout(() => {
        if (this.pending.delete(id)) {
          reject(new ProtocolError(`Request ${id} timed out`));
        }
      }, timeoutMs);

      this.pending.set(id, {
        resolve: (v) => { clearTimeout(timer); resolve(v); },
        reject:  (e) => { clearTimeout(timer); reject(e);  },
      });

      this._writeRequest({ jsonrpc: '2.0', id, method, params }).catch((err) => {
        clearTimeout(timer);
        this.pending.delete(id);
        reject(err);
      });
    });
  }

  async *_streamRpc(
    method: string,
    params: Record<string, unknown>,
    timeoutMs = RPC_TIMEOUT_MS,
  ): AsyncGenerator<unknown, void, unknown> {
    if (!this.engine.isRunning()) {
      throw new ProtocolError('Engine is not running. Call connect() first.');
    }

    const id = ++this.nextId;
    this.streams.set(id, { items: [], waiters: [], closed: false });
    let completed = false;

    try {
      await this._writeRequest({ jsonrpc: '2.0', id, method, params });

      while (true) {
        const item = await this._readStreamItem(id, timeoutMs);
        if (item === STREAM_END) {
          completed = true;
          break;
        }
        if (item instanceof Error) throw item;
        yield item;
      }
    } finally {
      this._clearStream(id);
      this.partialData.delete(id);
      if (!completed) {
        await this._cancelRequest(id);
      }
    }
  }

  /** JSON replacer that serialises special JS types the engine understands. */
  private _jsonReplacer(_key: string, value: unknown): unknown {
    if (value instanceof Date)   return value.toISOString();
    if (value instanceof Buffer) return value.toString('base64');
    return value;
  }

  private async _writeRequest(request: JsonRpcRequest): Promise<void> {
    const stdin = this.engine.stdin;
    if (!stdin) {
      throw new ProtocolError('Engine is not running. Call connect() first.');
    }

    const payload = JSON.stringify(request, this._jsonReplacer) + '\n';

    await new Promise<void>((resolve, reject) => {
      stdin.write(payload, (err) => {
        if (err) {
          reject(new ProtocolError(`Write failed: ${err.message}`));
        } else {
          resolve();
        }
      });
    });
  }

  private async _cancelRequest(requestId: number): Promise<void> {
    try {
      await this._writeRequest({
        jsonrpc: '2.0',
        method: 'request.cancel',
        params: {
          protocolVersion: PROTOCOL_VERSION,
          requestId,
        },
      });
    } catch {
      // Best effort: the engine may already be gone.
    }
  }

  private _pushStreamItem(id: number, item: StreamQueueItem): void {
    const stream = this.streams.get(id);
    if (!stream) return;

    const waiter = stream.waiters.shift();
    if (waiter) {
      clearTimeout(waiter.timer);
      waiter.resolve(item);
      return;
    }

    stream.items.push(item);
  }

  private _finishStream(id: number, item: StreamQueueItem): void {
    const stream = this.streams.get(id);
    if (!stream || stream.closed) return;

    this._pushStreamItem(id, item);
    this._closeStream(id);
  }

  private _closeStream(id: number): void {
    const stream = this.streams.get(id);
    if (!stream || stream.closed) return;

    stream.closed = true;
    this._pushStreamItem(id, STREAM_END);
  }

  private _failStreams(error: Error): void {
    for (const [id, stream] of this.streams.entries()) {
      if (stream.closed) continue;
      stream.closed = true;
      this._pushStreamItem(id, error);
      this._pushStreamItem(id, STREAM_END);
    }
  }

  private _clearStream(id: number): void {
    const stream = this.streams.get(id);
    if (!stream) return;

    for (const waiter of stream.waiters) {
      clearTimeout(waiter.timer);
    }

    this.streams.delete(id);
  }

  private _readStreamItem(id: number, timeoutMs: number): Promise<StreamQueueItem> {
    const stream = this.streams.get(id);
    if (!stream) {
      return Promise.resolve(STREAM_END);
    }
    if (stream.items.length > 0) {
      return Promise.resolve(stream.items.shift()!);
    }

    return new Promise<StreamQueueItem>((resolve, reject) => {
      const activeStream = this.streams.get(id);
      if (!activeStream) {
        resolve(STREAM_END);
        return;
      }

      const waiter: StreamWaiter = {
        resolve,
        timer: setTimeout(() => {
          const index = activeStream.waiters.indexOf(waiter);
          if (index >= 0) {
            activeStream.waiters.splice(index, 1);
          }
          reject(new ProtocolError(`Request ${id} timed out`));
        }, timeoutMs),
      };

      activeStream.waiters.push(waiter);
    });
  }

  /**
   * Start the background readline reader that processes engine stdout.
   * Each line is a complete JSON-RPC response (or a partial chunk).
   */
  private _startReading(): void {
    const stdout = this.engine.stdout!;
    this.rl = readline.createInterface({ input: stdout, crlfDelay: Infinity });

    this.rl.on('line', (line: string) => {
      const trimmed = line.trim();
      if (!trimmed) return;

      let response: JsonRpcResponse;
      try {
        response = JSON.parse(trimmed) as JsonRpcResponse;
      } catch {
        console.error('[nautilus-js] Failed to parse response:', trimmed);
        return;
      }

      const id = response.id as number | undefined;
      if (id == null) return;

      const stream = this.streams.get(id);
      if (stream) {
        if (response.error) {
          this._finishStream(
            id,
            errorFromCode(
              response.error.code,
              response.error.message,
              response.error.data,
            ),
          );
        } else {
          this._pushStreamItem(id, response.result);
          if (response.partial !== true) {
            this._closeStream(id);
          }
        }
        return;
      }

      const pending = this.pending.get(id);
      if (!pending) return;

      if (response.partial === true) {
        // Accumulate partial chunk data arrays.
        const chunkData = ((response.result as Record<string, unknown> | undefined)?.['data'] as unknown[] | undefined) ?? [];
        if (!this.partialData.has(id)) this.partialData.set(id, []);
        this.partialData.get(id)!.push(...chunkData);
      } else {
        // Final (or non-chunked) response.
        this.pending.delete(id);

        if (response.error) {
          this.partialData.delete(id);
          pending.reject(errorFromCode(
            response.error.code,
            response.error.message,
            response.error.data,
          ));
        } else {
          let result = response.result as Record<string, unknown> | undefined;

          // Merge accumulated partial chunks into the final result.
          if (this.partialData.has(id)) {
            const accumulated = this.partialData.get(id)!;
            this.partialData.delete(id);
            if (result && Array.isArray(result['data'])) {
              result = { ...result, data: [...accumulated, ...(result['data'] as unknown[])] };
            } else {
              result = { ...(result ?? {}), data: accumulated };
            }
          }

          pending.resolve(result);
        }
      }
    });

    this.rl.on('close', () => {
      // Engine exited — reject all in-flight requests with a diagnostic message.
      const stderr = this.engine.getStderrOutput().trim();
      const msg    = stderr
        ? `Engine process exited unexpectedly.\nDetails: ${stderr}`
        : 'Engine process exited unexpectedly (no output on stderr).';
      const err = new ProtocolError(msg);
      for (const { reject } of this.pending.values()) reject(err);
      this.pending.clear();
      this.partialData.clear();
      this._failStreams(err);
    });
  }

  private async _handshake(): Promise<void> {
    let response: Record<string, unknown>;
    try {
      response = (await this._rpc('engine.handshake', {
        protocolVersion: PROTOCOL_VERSION,
        clientName:      'nautilus-js',
        clientVersion:   '0.1.0',
      })) as Record<string, unknown>;
    } catch (e) {
      await this.disconnect();
      throw new HandshakeError(`Handshake failed: ${e}`);
    }

    const v = response?.['protocolVersion'] as number | undefined;
    if (v !== PROTOCOL_VERSION) {
      await this.disconnect();
      throw new HandshakeError(
        `Protocol version mismatch: engine uses ${v}, client expects ${PROTOCOL_VERSION}`,
      );
    }
  }

  protected async _startTransaction(
    timeoutMs      = DEFAULT_TX_TIMEOUT_MS,
    isolationLevel?: IsolationLevel,
  ): Promise<string> {
    const params: Record<string, unknown> = { protocolVersion: PROTOCOL_VERSION, timeoutMs };
    if (isolationLevel != null) params['isolationLevel'] = isolationLevel;
    const result = (await this._rpc('transaction.start', params)) as Record<string, unknown>;
    return result['id'] as string;
  }

  protected async _commitTransaction(txId: string): Promise<void> {
    await this._rpc('transaction.commit', { protocolVersion: PROTOCOL_VERSION, id: txId });
  }

  protected async _rollbackTransaction(txId: string): Promise<void> {
    try {
      await this._rpc('transaction.rollback', { protocolVersion: PROTOCOL_VERSION, id: txId });
    } catch { /* best-effort */ }
  }

  protected async _runTransactionBatch(
    operations: TransactionBatchOperation[],
    options?: { timeout?: number; isolationLevel?: IsolationLevel },
  ): Promise<unknown[]> {
    const params: Record<string, unknown> = {
      protocolVersion: PROTOCOL_VERSION,
      operations,
      timeoutMs: options?.timeout ?? DEFAULT_TX_TIMEOUT_MS,
    };
    if (options?.isolationLevel != null) params['isolationLevel'] = options.isolationLevel;

    const result = (await this._rpc('transaction.batch', params)) as Record<string, unknown>;
    return Array.isArray(result['results']) ? (result['results'] as unknown[]) : [];
  }

  /**
   * Execute `fn` inside a server-side transaction.
   * Commits on success; rolls back and re-throws on any error.
   */
  protected async _runTransactionCallback<T>(
    fn:       (tx: TransactionClient) => Promise<T>,
    options?: { timeout?: number; isolationLevel?: IsolationLevel },
  ): Promise<T> {
    const txId = await this._startTransaction(
      options?.timeout ?? DEFAULT_TX_TIMEOUT_MS,
      options?.isolationLevel,
    );
    const tx = new TransactionClient(this, txId);
    try {
      const result = await fn(tx);
      await this._commitTransaction(txId);
      return result;
    } catch (e) {
      await this._rollbackTransaction(txId);
      throw e;
    }
  }
}
