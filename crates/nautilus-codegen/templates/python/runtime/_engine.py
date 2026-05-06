"""Engine process management with auto-download from GitHub releases."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
import os
import platform
import signal
import shutil
import sys
import urllib.request
from pathlib import Path
from typing import Optional

# Target engine version (should match the Rust workspace version)
NAUTILUS_ENGINE_VERSION = "0.1.0"
GITHUB_REPO = "nautilus-orm/nautilus"

_BINARY_NAME = "nautilus.exe" if platform.system() == "Windows" else "nautilus"
# Legacy binary name kept for backward compatibility
_LEGACY_BINARY_NAME = "nautilus-engine.exe" if platform.system() == "Windows" else "nautilus-engine"


@dataclass(frozen=True)
class EnginePoolOptions:
    """Engine-level connection-pool overrides for subprocess clients."""

    max_connections: Optional[int] = None
    min_connections: Optional[int] = None
    acquire_timeout_ms: Optional[int] = None
    idle_timeout_ms: Optional[int] = None
    disable_idle_timeout: bool = False
    test_before_acquire: Optional[bool] = None
    statement_cache_capacity: Optional[int] = None

    def to_cli_args(self) -> list[str]:
        if self.disable_idle_timeout and self.idle_timeout_ms is not None:
            raise ValueError(
                "idle_timeout_ms and disable_idle_timeout cannot be set together"
            )

        args: list[str] = []
        if self.max_connections is not None:
            args.extend(["--max-connections", str(self.max_connections)])
        if self.min_connections is not None:
            args.extend(["--min-connections", str(self.min_connections)])
        if self.acquire_timeout_ms is not None:
            args.extend(["--acquire-timeout-ms", str(self.acquire_timeout_ms)])
        if self.disable_idle_timeout:
            args.append("--disable-idle-timeout")
        elif self.idle_timeout_ms is not None:
            args.extend(["--idle-timeout-ms", str(self.idle_timeout_ms)])
        if self.test_before_acquire is not None:
            args.extend(
                ["--test-before-acquire", "true" if self.test_before_acquire else "false"]
            )
        if self.statement_cache_capacity is not None:
            args.extend(
                ["--statement-cache-capacity", str(self.statement_cache_capacity)]
            )
        return args


class EngineProcess:
    """Manages the nautilus engine subprocess via asyncio."""

    def __init__(
        self,
        engine_path: Optional[str] = None,
        migrate: bool = False,
        pool_options: Optional[EnginePoolOptions] = None,
    ) -> None:
        """Initialize engine process manager.

        Args:
            engine_path: Path to the 'nautilus' binary. If None, will auto-detect or download.
            migrate: If True, pass --migrate flag to run DDL migrations on startup.
            pool_options: Optional engine-level pool overrides.
        """
        self._resolved = engine_path
        self._is_legacy = bool(
            engine_path and os.path.basename(engine_path).startswith("nautilus-engine")
        )
        self.migrate = migrate
        self.pool_options = pool_options or EnginePoolOptions()
        self._process: Optional[asyncio.subprocess.Process] = None
        self._stderr_drain_task: Optional[asyncio.Task] = None
        self._stderr_buffer: list = []
        # Kept separately so the atexit handler can kill the process with
        # os.kill() without touching any asyncio transport.
        self._pid: Optional[int] = None

    async def spawn(self, schema_path: str) -> None:
        """Spawn the engine process with the given schema.

        Args:
            schema_path: Path to the Nautilus schema file.
        """
        if self._process:
            raise RuntimeError("Engine process already running")
        self._stderr_buffer = []

        self._load_dotenv(schema_path)

        if self._resolved is None:
            self._resolved = self._find_or_download_engine(schema_path)
            self._is_legacy = os.path.basename(self._resolved).startswith("nautilus-engine")

        pool_args = self.pool_options.to_cli_args()
        if self._is_legacy:
            cmd = [self._resolved, "--schema", schema_path]
            if self.migrate:
                cmd.append("--migrate")
            cmd.extend(pool_args)
        else:
            cmd = [self._resolved, "engine", "serve", "--schema", schema_path]
            if self.migrate:
                cmd.append("--migrate")
            cmd.extend(pool_args)

        self._process = await asyncio.create_subprocess_exec(
            *cmd,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            limit=16 * 1024 * 1024,  # 16 MB — prevents LimitOverrunError on large responses
        )
        self._pid = self._process.pid

        # Start a background task that continuously drains the engine's stderr
        # so the OS pipe buffer never fills up.  On Windows the default pipe
        # buffer is 64 KB; if eprintln! in the Rust engine writes more than
        # that without anyone reading the other end, the write blocks and the
        # engine deadlocks.
        self._stderr_drain_task = asyncio.ensure_future(self._drain_stderr())

    async def terminate(self) -> None:
        """Terminate the engine process and close all asyncio pipe transports
        while the event loop is still open.

        On Windows (ProactorEventLoop), every pipe spawned with
        create_subprocess_exec gets its own _ProactorBasePipeTransport.
        If those transports are not explicitly closed *before* the event loop
        shuts down, their __del__ methods call loop.call_soon() on an already-
        closed loop, producing the 'RuntimeError: Event loop is closed' noise.

        The correct sequence is:
          close stdin -> signal process -> wait for exit -> close all three pipe
          transports explicitly -> allow the loop to drain their callbacks.
        """
        if not self._process:
            return

        process = self._process
        self._process = None
        self._pid = None

        # Cancel the stderr drain task.
        if self._stderr_drain_task and not self._stderr_drain_task.done():
            self._stderr_drain_task.cancel()
            try:
                await self._stderr_drain_task
            except (asyncio.CancelledError, Exception):
                pass
        self._stderr_drain_task = None

        # 1. Close stdin so the engine receives EOF and can exit cleanly.
        if process.stdin and not process.stdin.is_closing():
            process.stdin.close()
            try:
                await process.stdin.wait_closed()
            except Exception:
                pass

        # 2. Ask the process to exit.
        try:
            process.terminate()
        except ProcessLookupError:
            pass  # already dead

        # 3. Wait for the process to actually exit (with a fallback kill).
        try:
            await asyncio.wait_for(process.wait(), timeout=5.0)
        except asyncio.TimeoutError:
            try:
                process.kill()
            except ProcessLookupError:
                pass
            await process.wait()

        # 4. Explicitly close stdout and stderr read-pipe transports.
        #    These are _ProactorReadPipeTransport objects bound to the original
        #    event loop.  Closing them here (while the loop is still open)
        #    prevents BaseSubprocessTransport.__del__ from attempting the same
        #    close after the loop has shut down.
        for stream in (process.stdout, process.stderr):
            if stream is None:
                continue
            transport = getattr(stream, "_transport", None)
            if transport is not None and not transport.is_closing():
                transport.close()

        # 5. Close the SubprocessTransport itself.
        sub_transport = getattr(process, "_transport", None)
        if sub_transport is not None and not sub_transport.is_closing():
            sub_transport.close()

        # 6. Give the loop several iterations to process all close callbacks
        #    so everything is fully torn down before the caller returns.
        for _ in range(5):
            await asyncio.sleep(0)

    def is_running(self) -> bool:
        """Check if engine process is running."""
        return self._process is not None and self._process.returncode is None

    async def _drain_stderr(self) -> None:
        """Read engine stderr into a buffer so the OS pipe never fills up.

        Without this, long-running sessions with verbose engine logging will
        fill the 64 KB Windows pipe buffer, causing the engine to block on
        ``eprintln!`` and deadlock.  The buffered content is available via
        ``get_stderr_output()`` for diagnostics when the engine exits.
        """
        try:
            reader = self._process.stderr if self._process else None
            if reader is None:
                return
            while True:
                chunk = await reader.read(65536)
                if not chunk:
                    break  # EOF — engine exited
                self._stderr_buffer.append(chunk)
        except (asyncio.CancelledError, Exception):
            pass

    def get_stderr_output(self) -> str:
        """Return all stderr output collected since the last ``spawn()`` call."""
        return b"".join(self._stderr_buffer).decode("utf-8", errors="replace")

    @staticmethod
    def _load_dotenv(schema_path: str) -> None:
        """Load a .env file and populate os.environ (never overwrites existing vars).

        Search order (first file found wins):
          1. Walk up from the directory that contains the schema file (closest first).
          2. Current working directory (if not already covered above).

        A .env in a parent directory of the schema 
        is found even when the schema itself lives in a subdirectory.

        Only plain KEY=VALUE syntax is supported (no variable expansion).
        Lines starting with '#' and blank lines are ignored.
        Values may be optionally quoted with single or double quotes.
        """
        seen: set = set()
        search_dirs: list = []

        d = Path(schema_path).resolve().parent
        while True:
            if d not in seen:
                search_dirs.append(d)
                seen.add(d)
            parent = d.parent
            if parent == d:
                break
            d = parent

        cwd = Path.cwd()
        if cwd not in seen:
            search_dirs.append(cwd)

        dotenv_path: Optional[Path] = None
        for directory in search_dirs:
            candidate = directory / ".env"
            if candidate.is_file():
                dotenv_path = candidate
                break

        if dotenv_path is None:
            return

        try:
            with dotenv_path.open(encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue
                    if "=" not in line:
                        continue
                    key, _, value = line.partition("=")
                    key = key.strip()
                    value = value.strip()
                    if len(value) >= 2 and value[0] in ('"', "'") and value[0] == value[-1]:
                        value = value[1:-1]
                    if key and key not in os.environ:
                        os.environ[key] = value
        except OSError:
            pass  # Unreadable .env — silently skip

    @property
    def stdin(self) -> Optional[asyncio.StreamWriter]:
        """Engine process stdin stream writer."""
        return self._process.stdin if self._process else None

    @property
    def stdout(self) -> Optional[asyncio.StreamReader]:
        """Engine process stdout stream reader."""
        return self._process.stdout if self._process else None

    @property
    def stderr(self) -> Optional[asyncio.StreamReader]:
        """Engine process stderr stream reader."""
        return self._process.stderr if self._process else None

    def _find_or_download_engine(self, schema_path: Optional[str] = None) -> str:
        """Find the engine binary near the workspace, in PATH or cache, or download it from GitHub."""
        local_binary = self._find_workspace_binary(schema_path)
        if local_binary:
            return local_binary

        path_binary = shutil.which(_BINARY_NAME)
        if path_binary:
            return path_binary

        legacy_binary = shutil.which(_LEGACY_BINARY_NAME)
        if legacy_binary:
            return legacy_binary

        cache_dir = self._get_cache_dir()
        cached_binary = cache_dir / _BINARY_NAME
        if cached_binary.exists():
            return str(cached_binary)

        print(f"Downloading nautilus v{NAUTILUS_ENGINE_VERSION}...")
        self._download_engine(cache_dir)

        if not cached_binary.exists():
            raise FileNotFoundError(
                f"Could not find or download the nautilus binary.\n"
                f"Install it manually with: cargo install nautilus-cli\n"
                f"or add the compiled binary to your PATH."
            )

        return str(cached_binary)

    def _find_workspace_binary(self, schema_path: Optional[str]) -> Optional[str]:
        """Look for a locally built binary under target/{debug,release} near the schema/workspace."""
        for root in self._search_roots(schema_path):
            for build_dir in ("debug", "release"):
                for binary_name in (_BINARY_NAME, _LEGACY_BINARY_NAME):
                    candidate = root / "target" / build_dir / binary_name
                    if candidate.is_file() and os.access(candidate, os.X_OK):
                        return str(candidate)
        return None

    @staticmethod
    def _search_roots(schema_path: Optional[str]) -> list[Path]:
        """Search the schema directory ancestry first, then the current working directory."""
        roots: list[Path] = []
        seen: set[Path] = set()

        if schema_path:
            current = Path(schema_path).resolve().parent
            while True:
                if current not in seen:
                    roots.append(current)
                    seen.add(current)
                parent = current.parent
                if parent == current:
                    break
                current = parent

        cwd = Path.cwd()
        if cwd not in seen:
            roots.append(cwd)

        return roots

    def _get_cache_dir(self) -> Path:
        """Return the platform-specific cache directory."""
        if platform.system() == "Windows":
            cache_base = Path(os.environ.get("LOCALAPPDATA", str(Path.home() / ".nautilus")))
        else:
            cache_base = Path.home() / ".nautilus"

        cache_dir = cache_base / "bin" / NAUTILUS_ENGINE_VERSION
        cache_dir.mkdir(parents=True, exist_ok=True)
        return cache_dir

    def _download_engine(self, cache_dir: Path) -> None:
        """Download the unified nautilus binary from GitHub releases."""
        system = platform.system()
        machine = platform.machine().lower()

        if system == "Windows":
            platform_suffix = "x86_64-pc-windows-msvc.exe"
        elif system == "Darwin":
            platform_suffix = "x86_64-apple-darwin" if machine == "x86_64" else "aarch64-apple-darwin"
        elif system == "Linux":
            platform_suffix = "x86_64-unknown-linux-gnu"
        else:
            raise RuntimeError(f"Unsupported platform: {system}")

        url = (
            f"https://github.com/{GITHUB_REPO}/releases/download/"
            f"v{NAUTILUS_ENGINE_VERSION}/nautilus-{platform_suffix}"
        )

        target_path = cache_dir / _BINARY_NAME

        try:
            print(f"Downloading from {url}...")
            urllib.request.urlretrieve(url, target_path)
            if system != "Windows":
                os.chmod(target_path, 0o755)
            print(f"Downloaded to {target_path}")
        except Exception as e:
            print(f"Warning: Auto-download failed: {e}", file=sys.stderr)
            print("Please install manually: cargo install nautilus-cli", file=sys.stderr)
            raise
