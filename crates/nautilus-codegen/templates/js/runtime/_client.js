// Runtime file — do not edit manually.

import * as readline from 'readline';
import { PROTOCOL_VERSION } from './_protocol.js';
import { EngineProcess } from './_engine.js';
import { IsolationLevel, TransactionClient } from './_transaction.js';
import { errorFromCode, HandshakeError, ProtocolError } from './_errors.js';

const RPC_TIMEOUT_MS = 30000;
const STREAM_END = Symbol('nautilus.stream.end');

export class NautilusClient {
  constructor(schemaPath, options) {
    this.schemaPath = schemaPath;
    this.engine = new EngineProcess(
      undefined,
      options?.migrate ?? false,
      options?.pool,
    );
    this.nextId = 0;
    this.pending = new Map();
    this.partialData = new Map();
    this.streams = new Map();
    this.rl = null;
    this._delegates = {};
  }

  async connect() {
    if (this.engine.isRunning()) return;

    this.engine.spawn(this.schemaPath);
    this._startReading();
    await this._handshake();

    process.once('exit', () => { this.engine.terminate().catch(() => {}); });
  }

  async disconnect() {
    this.rl?.close();
    this.rl = null;

    await this.engine.terminate();

    const err = new ProtocolError('Client disconnected');
    for (const { reject } of this.pending.values()) reject(err);
    this.pending.clear();
    this.partialData.clear();
    this._failStreams(err);
  }

  async _rpc(method, params) {
    if (!this.engine.isRunning()) {
      throw new ProtocolError('Engine is not running. Call connect() first.');
    }

    const id = ++this.nextId;

    return new Promise((resolve, reject) => {
      this.pending.set(id, { resolve, reject });

      this._writeRequest({ jsonrpc: '2.0', id, method, params }).catch((err) => {
        this.pending.delete(id);
        reject(err);
      });
    });
  }

  async *_streamRpc(method, params, timeoutMs = RPC_TIMEOUT_MS) {
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

  _jsonReplacer(_key, value) {
    if (value instanceof Date)   return value.toISOString();
    if (value instanceof Buffer) return value.toString('base64');
    return value;
  }

  async _writeRequest(request) {
    const stdin = this.engine.stdin;
    if (!stdin) {
      throw new ProtocolError('Engine is not running. Call connect() first.');
    }

    const payload = JSON.stringify(request, this._jsonReplacer) + '\n';

    await new Promise((resolve, reject) => {
      stdin.write(payload, (err) => {
        if (err) {
          reject(new ProtocolError(`Write failed: ${err.message}`));
        } else {
          resolve();
        }
      });
    });
  }

  async _cancelRequest(requestId) {
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

  _pushStreamItem(id, item) {
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

  _finishStream(id, item) {
    const stream = this.streams.get(id);
    if (!stream || stream.closed) return;

    this._pushStreamItem(id, item);
    this._closeStream(id);
  }

  _closeStream(id) {
    const stream = this.streams.get(id);
    if (!stream || stream.closed) return;

    stream.closed = true;
    this._pushStreamItem(id, STREAM_END);
  }

  _failStreams(error) {
    for (const [id, stream] of this.streams.entries()) {
      if (stream.closed) continue;
      stream.closed = true;
      this._pushStreamItem(id, error);
      this._pushStreamItem(id, STREAM_END);
    }
  }

  _clearStream(id) {
    const stream = this.streams.get(id);
    if (!stream) return;

    for (const waiter of stream.waiters) {
      clearTimeout(waiter.timer);
    }

    this.streams.delete(id);
  }

  _readStreamItem(id, timeoutMs) {
    const stream = this.streams.get(id);
    if (!stream) {
      return Promise.resolve(STREAM_END);
    }
    if (stream.items.length > 0) {
      return Promise.resolve(stream.items.shift());
    }

    return new Promise((resolve, reject) => {
      const activeStream = this.streams.get(id);
      if (!activeStream) {
        resolve(STREAM_END);
        return;
      }

      const waiter = {
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

  _startReading() {
    const stdout = this.engine.stdout;
    this.rl = readline.createInterface({ input: stdout, crlfDelay: Infinity });

    this.rl.on('line', (line) => {
      const trimmed = line.trim();
      if (!trimmed) return;

      let response;
      try {
        response = JSON.parse(trimmed);
      } catch {
        console.error('[nautilus-js] Failed to parse response:', trimmed);
        return;
      }

      const id = response.id;
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
        const chunkData = (response.result?.['data']) ?? [];
        if (!this.partialData.has(id)) this.partialData.set(id, []);
        this.partialData.get(id).push(...chunkData);
      } else {
        this.pending.delete(id);

        if (response.error) {
          this.partialData.delete(id);
          pending.reject(errorFromCode(
            response.error.code,
            response.error.message,
            response.error.data,
          ));
        } else {
          let result = response.result;

          if (this.partialData.has(id)) {
            const accumulated = this.partialData.get(id);
            this.partialData.delete(id);
            if (result && Array.isArray(result['data'])) {
              result = { ...result, data: [...accumulated, ...result['data']] };
            } else {
              result = { ...(result ?? {}), data: accumulated };
            }
          }

          pending.resolve(result);
        }
      }
    });

    this.rl.on('close', () => {
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

  async _handshake() {
    let response;
    try {
      response = await this._rpc('engine.handshake', {
        protocolVersion: PROTOCOL_VERSION,
        clientName:      'nautilus-js',
        clientVersion:   '0.1.0',
      });
    } catch (e) {
      await this.disconnect();
      throw new HandshakeError(`Handshake failed: ${e}`);
    }

    const v = response?.['protocolVersion'];
    if (v !== PROTOCOL_VERSION) {
      await this.disconnect();
      throw new HandshakeError(
        `Protocol version mismatch: engine uses ${v}, client expects ${PROTOCOL_VERSION}`,
      );
    }
  }

  async _startTransaction(timeoutMs = 5000, isolationLevel) {
    const params = { protocolVersion: PROTOCOL_VERSION, timeoutMs };
    if (isolationLevel != null) params['isolationLevel'] = isolationLevel;
    const result = await this._rpc('transaction.start', params);
    return result['id'];
  }

  async _commitTransaction(txId) {
    await this._rpc('transaction.commit', { protocolVersion: PROTOCOL_VERSION, id: txId });
  }

  async _rollbackTransaction(txId) {
    try {
      await this._rpc('transaction.rollback', { protocolVersion: PROTOCOL_VERSION, id: txId });
    } catch { /* best-effort */ }
  }

  async _runTransactionBatch(operations, options) {
    const params = {
      protocolVersion: PROTOCOL_VERSION,
      operations,
      timeoutMs: options?.timeout ?? 5000,
    };
    if (options?.isolationLevel != null) params['isolationLevel'] = options.isolationLevel;

    const result = await this._rpc('transaction.batch', params);
    return Array.isArray(result?.['results']) ? result['results'] : [];
  }

  async _runTransactionCallback(fn, options) {
    const txId = await this._startTransaction(
      options?.timeout ?? 5000,
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
