// Runtime file — do not edit manually.

import { Writable, Readable } from 'stream';

export interface EnginePoolOptions {
  maxConnections?: number;
  minConnections?: number;
  acquireTimeoutMs?: number;
  idleTimeoutMs?: number | null;
  testBeforeAcquire?: boolean;
  statementCacheCapacity?: number;
}

export declare class EngineProcess {
  constructor(enginePath?: string, migrate?: boolean, poolOptions?: EnginePoolOptions);
  spawn(schemaPath: string): void;
  get stdin(): Writable | null;
  get stdout(): Readable | null;
  isRunning(): boolean;
  getStderrOutput(): string;
  terminate(): Promise<void>;
}
