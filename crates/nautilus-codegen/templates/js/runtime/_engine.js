// Runtime file — do not edit manually.

import * as cp   from 'child_process';
import * as fs   from 'fs';
import * as path from 'path';

const BINARY_NAME        = process.platform === 'win32' ? 'nautilus.exe'        : 'nautilus';
const LEGACY_BINARY_NAME = process.platform === 'win32' ? 'nautilus-engine.exe' : 'nautilus-engine';

export class EngineProcess {
  constructor(enginePath, migrate = false, poolOptions = {}) {
    this.enginePath = enginePath;
    this.migrate = migrate;
    this.poolOptions = poolOptions;
    this.proc = null;
    this.stderrChunks = [];
  }

  spawn(schemaPath) {
    if (this.proc) {
      throw new Error('Engine process is already running');
    }

    this.stderrChunks = [];
    this._loadDotenv(schemaPath);

    const resolved = this.enginePath ?? this._findEngine(schemaPath);
    const isLegacy = path.basename(resolved).startsWith('nautilus-engine');
    const poolArgs = this._poolArgs();

    const args = isLegacy
      ? ['--schema', schemaPath, ...(this.migrate ? ['--migrate'] : []), ...poolArgs]
      : ['engine', 'serve', '--schema', schemaPath, ...(this.migrate ? ['--migrate'] : []), ...poolArgs];

    this.proc = cp.spawn(resolved, args, {
      stdio: ['pipe', 'pipe', 'pipe'],
    });

    this.proc.stderr.on('data', (chunk) => {
      this.stderrChunks.push(chunk);
    });
  }

  _poolArgs() {
    const args = [];

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
      args.push('--test-before-acquire', String(this.poolOptions.testBeforeAcquire));
    }

    return args;
  }

  get stdin() {
    return this.proc?.stdin ?? null;
  }

  get stdout() {
    return this.proc?.stdout ?? null;
  }

  isRunning() {
    return this.proc !== null && this.proc.exitCode === null && !this.proc.killed;
  }

  getStderrOutput() {
    return Buffer.concat(this.stderrChunks).toString('utf8');
  }

  async terminate() {
    const proc = this.proc;
    if (!proc) return;
    this.proc = null;

    return new Promise((resolve) => {
      if (proc.exitCode !== null || proc.killed) {
        resolve();
        return;
      }

      const cleanup = () => { clearTimeout(timer); resolve(); };
      proc.once('exit', cleanup);
      proc.once('error', cleanup);

      try { proc.stdin?.end(); } catch { /* ignore */ }

      const timer = setTimeout(() => {
        try { proc.kill('SIGTERM'); } catch { /* ignore */ }

        const forceTimer = setTimeout(() => {
          try { proc.kill('SIGKILL'); } catch { /* ignore */ }
        }, 5000);
        proc.once('exit', () => clearTimeout(forceTimer));
      }, 100);
    });
  }

  _loadDotenv(schemaPath) {
    const dirs = [];
    const seen = new Set();

    let dir = path.resolve(path.dirname(schemaPath));
    while (true) {
      if (!seen.has(dir)) { dirs.push(dir); seen.add(dir); }
      const parent = path.dirname(dir);
      if (parent === dir) break;
      dir = parent;
    }

    const cwd = process.cwd();
    if (!seen.has(cwd)) dirs.push(cwd);

    for (const d of dirs) {
      const envPath = path.join(d, '.env');
      if (!fs.existsSync(envPath)) continue;

      let content;
      try { content = fs.readFileSync(envPath, 'utf8'); } catch { continue; }

      for (const line of content.split('\n')) {
        const trimmed = line.trim();
        if (!trimmed || trimmed.startsWith('#')) continue;
        const eqIdx = trimmed.indexOf('=');
        if (eqIdx < 1) continue;

        const key   = trimmed.slice(0, eqIdx).trim();
        let   value = trimmed.slice(eqIdx + 1).trim();

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

      break;
    }
  }

  _findEngine(schemaPath) {
    const local = this._findWorkspaceBinary(schemaPath);
    if (local) return local;

    for (const name of [BINARY_NAME, LEGACY_BINARY_NAME]) {
      const found = this._which(name);
      if (found) return found;
    }
    throw new Error(
      `nautilus binary not found in PATH.\n` +
      `Install it with: cargo install nautilus-cli\n` +
      `Or add the compiled binary to your PATH before running nautilus generate.`,
    );
  }

  _findWorkspaceBinary(schemaPath) {
    for (const root of this._searchRoots(schemaPath)) {
      for (const buildDir of ['debug', 'release']) {
        for (const name of [BINARY_NAME, LEGACY_BINARY_NAME]) {
          const candidate = path.join(root, 'target', buildDir, name);
          try {
            fs.accessSync(candidate, fs.constants.X_OK);
            return candidate;
          } catch { /* keep searching */ }
        }
      }
    }
    return null;
  }

  _searchRoots(schemaPath) {
    const roots = [];
    const seen = new Set();

    if (schemaPath) {
      let dir = path.resolve(path.dirname(schemaPath));
      while (true) {
        if (!seen.has(dir)) {
          roots.push(dir);
          seen.add(dir);
        }
        const parent = path.dirname(dir);
        if (parent === dir) break;
        dir = parent;
      }
    }

    const cwd = process.cwd();
    if (!seen.has(cwd)) {
      roots.push(cwd);
    }

    return roots;
  }

  _which(name) {
    // On Windows, delegate to `where.exe` so that the system PATH (not the
    // bash-mangled PATH exposed by Git Bash) is searched correctly.
    if (process.platform === 'win32') {
      try {
        const result = cp.spawnSync('where.exe', [name], { encoding: 'utf8' });
        if (result.status === 0 && result.stdout) {
          const first = result.stdout.trim().split(/\r?\n/)[0];
          if (first) return first;
        }
      } catch { /* where.exe not available — fall through to manual search */ }
    }

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
