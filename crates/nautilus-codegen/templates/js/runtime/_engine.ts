// Runtime file — do not edit manually.

import * as cp   from 'child_process';
import * as fs   from 'fs';
import * as path from 'path';
import { Writable, Readable } from 'stream';

const BINARY_NAME        = process.platform === 'win32' ? 'nautilus.exe'        : 'nautilus';
const LEGACY_BINARY_NAME = process.platform === 'win32' ? 'nautilus-engine.exe' : 'nautilus-engine';
const NPM_PACKAGE        = 'nautilus-orm';
const NPX_BIN            = process.platform === 'win32' ? 'npx.cmd' : 'npx';
const NPM_BIN            = process.platform === 'win32' ? 'npm.cmd' : 'npm';

type ResolvedEngine =
  | { kind: 'binary'; path: string }
  | { kind: 'npx' };

export interface EnginePoolOptions {
  maxConnections?: number;
  minConnections?: number;
  acquireTimeoutMs?: number;
  idleTimeoutMs?: number | null;
  testBeforeAcquire?: boolean;
}

/**
 * Manages the `nautilus engine serve` subprocess.
 *
 * The engine reads JSON-RPC requests from stdin (newline-delimited) and writes
 * JSON-RPC responses to stdout (also newline-delimited).
 *
 * Stderr is drained into an internal buffer to prevent pipe deadlock on
 * Windows and to provide diagnostic output when the process exits unexpectedly.
 */
export class EngineProcess {
  private proc: cp.ChildProcess | null = null;
  private stderrChunks: Buffer[] = [];

  constructor(
    private readonly enginePath?: string,
    private readonly migrate: boolean = false,
    private readonly poolOptions: EnginePoolOptions = {},
  ) {}

  // Public interface

  /**
   * Spawn the engine process.
   *
   * Loads `.env` file (walks up from schema dir, then CWD —
   * and the Python client behaviour), then executes the nautilus binary.
   */
  spawn(schemaPath: string): void {
    if (this.proc) {
      throw new Error('Engine process is already running');
    }

    this.stderrChunks = [];
    this._loadDotenv(schemaPath);

    const serveArgs = [
      'engine', 'serve',
      '--schema', schemaPath,
      ...(this.migrate ? ['--migrate'] : []),
      ...this._poolArgs(),
    ];

    let command: string;
    let args: string[];

    if (this.enginePath) {
      const isLegacy = path.basename(this.enginePath).startsWith('nautilus-engine');
      command = this.enginePath;
      args = isLegacy
        ? ['--schema', schemaPath, ...(this.migrate ? ['--migrate'] : [])]
        : serveArgs;
    } else {
      const resolved = this._findEngine();
      if (resolved.kind === 'npx') {
        command = NPX_BIN;
        args    = [NPM_PACKAGE, ...serveArgs];
      } else {
        const isLegacy = path.basename(resolved.path).startsWith('nautilus-engine');
        command = resolved.path;
        args = isLegacy
          ? ['--schema', schemaPath, ...(this.migrate ? ['--migrate'] : [])]
          : serveArgs;
      }
    }

    this.proc = cp.spawn(command, args, {
      stdio: ['pipe', 'pipe', 'pipe'],
    });

    // Drain stderr to prevent pipe deadlock.
    this.proc.stderr!.on('data', (chunk: Buffer) => {
      this.stderrChunks.push(chunk);
    });
  }

  private _poolArgs(): string[] {
    const args: string[] = [];

    if (this.poolOptions.maxConnections != null) {
      args.push('--max-connections', String(this.poolOptions.maxConnections));
    }
    if (this.poolOptions.minConnections != null) {
      args.push('--min-connections', String(this.poolOptions.minConnections));
    }
    if (this.poolOptions.acquireTimeoutMs != null) {
      args.push('--acquire-timeout-ms', String(this.poolOptions.acquireTimeoutMs));
    }

    if (Object.prototype.hasOwnProperty.call(this.poolOptions, 'idleTimeoutMs')) {
      const idleTimeoutMs = this.poolOptions.idleTimeoutMs;
      if (idleTimeoutMs === null) {
        args.push('--disable-idle-timeout');
      } else if (idleTimeoutMs != null) {
        args.push('--idle-timeout-ms', String(idleTimeoutMs));
      }
    }

    if (this.poolOptions.testBeforeAcquire != null) {
      args.push(
        '--test-before-acquire',
        String(this.poolOptions.testBeforeAcquire),
      );
    }

    return args;
  }

  get stdin(): Writable | null {
    return this.proc?.stdin ?? null;
  }

  get stdout(): Readable | null {
    return this.proc?.stdout ?? null;
  }

  isRunning(): boolean {
    return this.proc !== null && this.proc.exitCode === null && !this.proc.killed;
  }

  getStderrOutput(): string {
    return Buffer.concat(this.stderrChunks).toString('utf8');
  }

  /**
   * Gracefully terminate the engine:
   *   1. Close stdin (signals EOF to the engine)
   *   2. Send SIGTERM and wait up to 5 s
   *   3. Force-kill with SIGKILL if still running
   */
  async terminate(): Promise<void> {
    const proc = this.proc;
    if (!proc) return;
    this.proc = null;

    return new Promise<void>((resolve) => {
      // Already dead.
      if (proc.exitCode !== null || proc.killed) {
        resolve();
        return;
      }

      const cleanup = () => { clearTimeout(timer); resolve(); };
      proc.once('exit', cleanup);
      proc.once('error', cleanup);

      // Close stdin to let the engine shut down cleanly.
      try { proc.stdin?.end(); } catch { /* ignore */ }

      // Signal after a brief pause.
      const timer = setTimeout(() => {
        try { proc.kill('SIGTERM'); } catch { /* ignore */ }

        // Force-kill if still alive after 5 s.
        const forceTimer = setTimeout(() => {
          try { proc.kill('SIGKILL'); } catch { /* ignore */ }
        }, 5000);
        proc.once('exit', () => clearTimeout(forceTimer));
      }, 100);
    });
  }

  // Private helpers

  /**
   * Walk up from the schema directory (and then from CWD) looking for a
   * `.env` file.  Reads the first one found and injects `KEY=VALUE` pairs
   * into `process.env`, without overwriting existing variables.
   *
   * This mirrors the behaviour of the Python `_engine.py`.
   */
  private _loadDotenv(schemaPath: string): void {
    const dirs: string[] = [];
    const seen = new Set<string>();

    // Walk up from schema directory.
    let dir = path.resolve(path.dirname(schemaPath));
    while (true) {
      if (!seen.has(dir)) { dirs.push(dir); seen.add(dir); }
      const parent = path.dirname(dir);
      if (parent === dir) break;
      dir = parent;
    }

    // Also check CWD.
    const cwd = process.cwd();
    if (!seen.has(cwd)) dirs.push(cwd);

    for (const d of dirs) {
      const envPath = path.join(d, '.env');
      if (!fs.existsSync(envPath)) continue;

      let content: string;
      try { content = fs.readFileSync(envPath, 'utf8'); } catch { continue; }

      for (const line of content.split('\n')) {
        const trimmed = line.trim();
        if (!trimmed || trimmed.startsWith('#')) continue;
        const eqIdx = trimmed.indexOf('=');
        if (eqIdx < 1) continue;

        const key   = trimmed.slice(0, eqIdx).trim();
        let   value = trimmed.slice(eqIdx + 1).trim();

        // Strip surrounding quotes.
        if (
          value.length >= 2 &&
          ((value[0] === '"'  && value[value.length - 1] === '"') ||
           (value[0] === "'"  && value[value.length - 1] === "'"))
        ) {
          value = value.slice(1, -1);
        }

        if (key && !(key in process.env)) {
          process.env[key] = value;
        }
      }

      break; // Use only the first .env found.
    }
  }

  /**
   * Locate the `nautilus` (or `nautilus-engine`) binary.
   *
   * Search order:
   * 1. System PATH (`nautilus` then `nautilus-engine`).
   * 2. `node_modules/.bin/nautilus` — walks up from CWD (local npm install).
   * 3. `npx nautilus-orm` — if the package is installed globally or locally.
   */
  private _findEngine(): ResolvedEngine {
    for (const name of [BINARY_NAME, LEGACY_BINARY_NAME]) {
      const found = this._which(name);
      if (found) return { kind: 'binary', path: found };
    }

    const localBin = this._findInNodeModules();
    if (localBin) return { kind: 'binary', path: localBin };

    if (this._isInstalledViaNpm()) return { kind: 'npx' };

    throw new Error(
      `nautilus binary not found.\n` +
      `Install it with: npm install nautilus-orm  (or -g for global)\n` +
      `Or: cargo install nautilus-cli`,
    );
  }

  /**
   * Walk up from CWD looking for `node_modules/.bin/nautilus[.exe]`
   * (covers local npm installs).
   */
  private _findInNodeModules(): string | null {
    let dir = process.cwd();
    while (true) {
      const candidate = path.join(dir, 'node_modules', '.bin', BINARY_NAME);
      try {
        fs.accessSync(candidate, fs.constants.X_OK);
        return candidate;
      } catch { /* not found */ }
      const parent = path.dirname(dir);
      if (parent === dir) break;
      dir = parent;
    }
    return null;
  }

  /**
   * Returns `true` when `nautilus-orm` is listed as an installed npm package
   * (local or global). Uses `npm ls` — no subprocess is spawned if npm is
   * absent.
   */
  private _isInstalledViaNpm(): boolean {
    try {
      for (const extra of [[], ['-g']] as string[][]) {
        const result = cp.spawnSync(
          NPM_BIN,
          ['ls', '--depth=0', '--json', NPM_PACKAGE, ...extra],
          { encoding: 'utf8', timeout: 5000 },
        );
        if (result.status === 0 && result.stdout) {
          const data = JSON.parse(result.stdout) as { dependencies?: Record<string, unknown> };
          if (data.dependencies && NPM_PACKAGE in data.dependencies) return true;
        }
      }
    } catch { /* npm not available */ }
    return false;
  }

  private _which(name: string): string | null {
    const envPath = process.env['PATH'] ?? '';
    const sep     = process.platform === 'win32' ? ';' : ':';
    for (const dir of envPath.split(sep)) {
      if (!dir) continue;
      const candidate = path.join(dir, name);
      try {
        fs.accessSync(candidate, fs.constants.X_OK);
        return candidate;
      } catch { /* not found or not executable */ }
    }
    return null;
  }
}
