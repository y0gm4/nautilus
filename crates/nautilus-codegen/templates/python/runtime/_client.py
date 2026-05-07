"""Main Nautilus client with JSON-RPC communication."""

from __future__ import annotations

import asyncio
import atexit
import concurrent.futures
import json
import os
import threading
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Any, AsyncIterator, Dict, Generic, Optional, TypeVar
from uuid import UUID

C = TypeVar("C")

_RPC_TIMEOUT_S: float = 30.0
_STDERR_DRAIN_TIMEOUT_S: float = 1.0
_SYNC_CONNECT_TIMEOUT_S: int = 35
_SYNC_LOOP_JOIN_TIMEOUT_S: int = 10
_STREAM_END = object()

from .engine import EnginePoolOptions, EngineProcess  # type: ignore
from ..errors.errors import HandshakeError, ProtocolError, TransactionError, TransactionTimeoutError  # type: ignore
from .protocol import PROTOCOL_VERSION, JsonRpcRequest, JsonRpcResponse  # type: ignore
from .transaction import IsolationLevel, TransactionClient  # type: ignore


def _build_engine_error(stderr: str, schema_path: str) -> str:
    """Build a human-readable error message when the engine process exits unexpectedly."""
    if stderr:
        stderr_lower = stderr.lower()
        if "database_url" in stderr_lower or "environment variable" in stderr_lower:
            return (
                "Engine failed to start: DATABASE_URL is not set or invalid.\n"
                "Add DATABASE_URL to your .env file or set it as an environment variable.\n"
                f"Details: {stderr}"
            )
        if "connection refused" in stderr_lower or "could not connect" in stderr_lower:
            return (
                "Engine could not connect to the database.\n"
                "Make sure your database is running and DATABASE_URL is correct.\n"
                f"Details: {stderr}"
            )
        if "no such file" in stderr_lower or "not found" in stderr_lower:
            return (
                "Engine failed to start: a required file was not found.\n"
                f"Details: {stderr}"
            )
        return f"Engine process exited unexpectedly.\nDetails: {stderr}"

    if not os.path.isfile(schema_path):
        return (
            f"Engine failed to start: schema file not found at:\n"
            f"  {schema_path}\n"
            "Re-run 'nautilus generate' from the directory containing your schema file."
        )
    if not os.environ.get("DATABASE_URL"):
        return (
            "Engine failed to start: DATABASE_URL is not set.\n"
            "Add DATABASE_URL to your .env file or set it as an environment variable."
        )
    return "Engine process exited unexpectedly (no output on stderr)."


def _json_default(obj: Any) -> Any:
    """JSON serializer for types not handled by the stdlib encoder."""
    if isinstance(obj, UUID):
        return str(obj)
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return str(obj)
    if isinstance(obj, Enum):
        return obj.value
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


_GLOBAL_INSTANCE: Optional[NautilusClient] = None
_auto_register_lock = threading.Lock()


class NautilusClient:
    """Nautilus database client.

    Manages engine process lifecycle, multiplexes JSON-RPC requests,
    and provides the base RPC layer for generated model delegates.
    """

    def __init__(
        self,
        schema_path: str,
        engine_path: Optional[str] = None,
        migrate: bool = False,
        auto_register: bool = False,
        pool_options: Optional[EnginePoolOptions] = None,
    ) -> None:
        """Initialize the Nautilus client.

        Args:
            schema_path: Path to the Nautilus schema file.
            engine_path: Optional path to nautilus-engine binary.
            migrate: If True, run DDL migrations on engine startup.
            auto_register: If True, register this instance globally for model.nautilus access.
            pool_options: Optional engine-level pool overrides for the subprocess.
        """
        self.schema_path = schema_path
        self.engine = EngineProcess(engine_path, migrate=migrate, pool_options=pool_options)

        self._request_id = 0
        self._pending: Dict[int, asyncio.Future] = {}
        self._partial_data: Dict[int, list] = {}
        self._stream_queues: Dict[int, asyncio.Queue[Any]] = {}
        self._reader_task: Optional[asyncio.Task] = None
        self._writer_lock = asyncio.Lock()
        self._handshake_done = False
        self._delegates: Dict[str, Any] = {}
        self._auto_registered = False
        self._atexit_handler = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._loop_close_original = None
        self._loop_close_patch = None

        self._sync_loop: Optional[asyncio.AbstractEventLoop] = None
        self._sync_thread: Optional[threading.Thread] = None
        self._sync_loop_lock = threading.Lock()

        if auto_register:
            self._set_as_global_instance()

    async def __aenter__(self) -> NautilusClient:
        """Async context manager entry."""
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit."""
        await self.disconnect()
        if self._auto_registered:
            self._clear_global_instance()

    async def connect(self) -> None:
        """Connect to the engine and perform handshake."""
        if self.engine.is_running():
            return

        await self.engine.spawn(self.schema_path)
        self._reader_task = asyncio.create_task(self._read_responses())
        await self._handshake()

        # Primary cleanup: patch loop.close() so that if the caller returns
        # from asyncio.run() without calling disconnect(), we still run the
        # full async teardown (cancel reader task, close all pipe transports)
        # while the loop is stopped-but-not-yet-closed.  That is the only
        # window in which asyncio transports can be closed cleanly.
        loop = asyncio.get_running_loop()
        self._loop = loop
        self._loop_close_original = loop.close

        def _ensure_disconnected_then_close():
            # Capture and restore original close immediately to be re-entrance safe.
            # Use a local variable so that disconnect() nullifying
            # self._loop_close_original doesn't affect the call below.
            original_close = self._loop_close_original
            loop.close = original_close  # type: ignore[method-assign]
            if self.engine.is_running():
                try:
                    loop.run_until_complete(self.disconnect())
                except Exception:
                    # Last resort: at least kill the OS process.
                    pid = self.engine._pid
                    if pid is not None:
                        import signal as _signal
                        try:
                            os.kill(pid, _signal.SIGTERM)
                        except OSError:
                            pass
            original_close()

        self._loop_close_patch = _ensure_disconnected_then_close
        loop.close = _ensure_disconnected_then_close  # type: ignore[method-assign]

        # Fallback atexit: covers edge-cases where the loop is destroyed
        # externally (e.g. SIGKILL, test runners that reuse loops, etc.).
        # At that point we can only kill the OS process; transports are gone.
        def _atexit_cleanup():
            pid = self.engine._pid
            if pid is not None:
                import signal as _signal
                try:
                    os.kill(pid, _signal.SIGTERM)
                except OSError:
                    pass  # process already dead

        self._atexit_handler = _atexit_cleanup
        atexit.register(self._atexit_handler)

    async def disconnect(self) -> None:
        """Disconnect from the engine."""
        # Restore loop.close() patch if it is still in place (i.e. we are
        # being called explicitly before the loop closes).
        if (
            self._loop is not None
            and self._loop_close_patch is not None
            and getattr(self._loop, "close", None) is self._loop_close_patch
        ):
            self._loop.close = self._loop_close_original  # type: ignore[method-assign]
        self._loop_close_patch = None
        self._loop_close_original = None

        # Unregister the atexit handler - we're disconnecting cleanly.
        if self._atexit_handler is not None:
            atexit.unregister(self._atexit_handler)
            self._atexit_handler = None

        if self._reader_task:
            self._reader_task.cancel()
            try:
                await self._reader_task
            except asyncio.CancelledError:
                pass

        await self.engine.terminate()

        for future in self._pending.values():
            if not future.done():
                future.cancel()
        self._pending.clear()
        self._partial_data.clear()
        self._fail_streams(ProtocolError("Client disconnected"))

        if self._auto_registered:
            self._clear_global_instance()

    async def _handshake(self) -> None:
        """Perform protocol handshake with engine."""
        try:
            response = await self._rpc("engine.handshake", {
                "protocolVersion": PROTOCOL_VERSION,
                "clientName": "nautilus-py",
                "clientVersion": "0.1.0",
            })

            protocol_version = response.get("protocolVersion")
            if protocol_version != PROTOCOL_VERSION:
                raise HandshakeError(
                    f"Protocol version mismatch: engine uses {protocol_version}, "
                    f"client expects {PROTOCOL_VERSION}"
                )

            self._handshake_done = True

        except Exception as e:
            await self.disconnect()
            raise HandshakeError(f"Handshake failed: {e}") from e

    async def _rpc(self, method: str, params: Dict[str, Any]) -> Any:
        """Execute a JSON-RPC call."""
        request_id = self._next_request_id()

        request = JsonRpcRequest(
            id=request_id,
            method=method,
            params=params,
        )

        future: asyncio.Future = asyncio.Future()
        self._pending[request_id] = future

        try:
            await self._write_request(request)
            response = await asyncio.wait_for(future, timeout=_RPC_TIMEOUT_S)
            return response

        except asyncio.TimeoutError:
            raise ProtocolError(f"Request {request_id} timed out")
        finally:
            self._pending.pop(request_id, None)

    def _next_request_id(self) -> int:
        """Allocate a fresh JSON-RPC request id."""
        self._request_id += 1
        return self._request_id

    def _fail_streams(self, error: Exception) -> None:
        """Wake all active streaming iterators with the same terminal error."""
        for queue in self._stream_queues.values():
            queue.put_nowait(error)
            queue.put_nowait(_STREAM_END)
        self._stream_queues.clear()

    async def _cancel_request(self, request_id: int) -> None:
        """Best-effort cancellation for an in-flight streaming request."""
        try:
            await self._write_request(
                JsonRpcRequest(
                    method="request.cancel",
                    params={
                        "protocolVersion": PROTOCOL_VERSION,
                        "requestId": request_id,
                    },
                )
            )
        except Exception:
            pass

    async def _stream_rpc(
        self,
        method: str,
        params: Dict[str, Any],
    ) -> AsyncIterator[Any]:
        """Execute a JSON-RPC call and yield chunked results as they arrive."""
        request_id = self._next_request_id()
        request = JsonRpcRequest(
            id=request_id,
            method=method,
            params=params,
        )
        queue: asyncio.Queue[Any] = asyncio.Queue()
        self._stream_queues[request_id] = queue
        completed = False

        try:
            await self._write_request(request)

            while True:
                try:
                    item = await asyncio.wait_for(queue.get(), timeout=_RPC_TIMEOUT_S)
                except asyncio.TimeoutError as exc:
                    raise ProtocolError(f"Request {request_id} timed out") from exc

                if item is _STREAM_END:
                    completed = True
                    break
                if isinstance(item, Exception):
                    raise item

                yield item
        finally:
            self._stream_queues.pop(request_id, None)
            self._partial_data.pop(request_id, None)
            if not completed:
                await self._cancel_request(request_id)

    async def _write_request(self, request: JsonRpcRequest) -> None:
        """Write a JSON-RPC request to engine stdin."""
        stdin = self.engine.stdin
        if not stdin:
            raise ProtocolError("Engine process not running")

        request_json = json.dumps(request.to_dict(), default=_json_default)
        line = (request_json + "\n").encode("utf-8")

        async with self._writer_lock:
            stdin.write(line)
            await stdin.drain()

    async def _read_responses(self) -> None:
        """Background task that reads responses from engine stdout."""
        stdout = self.engine.stdout
        if not stdout:
            return

        try:
            while True:
                line_bytes = await stdout.readline()

                if not line_bytes:
                    # Engine process exited.  Wait briefly for the stderr drain
                    # task to finish collecting output, then build a diagnostic
                    # message.  We must NOT read from engine.stderr here because
                    # _drain_stderr is already reading from the same stream —
                    # a concurrent read raises "read() called while another
                    # coroutine is already waiting for incoming data".
                    drain_task = self.engine._stderr_drain_task
                    if drain_task and not drain_task.done():
                        try:
                            await asyncio.wait_for(asyncio.shield(drain_task), timeout=_STDERR_DRAIN_TIMEOUT_S)
                        except (asyncio.TimeoutError, Exception):
                            pass
                    stderr_output = self.engine.get_stderr_output().strip()
                    error_msg = _build_engine_error(stderr_output, self.schema_path)
                    for future in self._pending.values():
                        if not future.done():
                            future.set_exception(ProtocolError(error_msg))
                    self._fail_streams(ProtocolError(error_msg))
                    break

                line = line_bytes.decode("utf-8").strip()
                if not line:
                    continue

                try:
                    response_dict = json.loads(line)
                    response = JsonRpcResponse.from_dict(response_dict)

                    if response.id is not None:
                        req_id = response.id
                        stream_queue = self._stream_queues.get(req_id)
                        if stream_queue is not None:
                            try:
                                result = response.unwrap()
                                await stream_queue.put(result)
                            except Exception as e:
                                self._stream_queues.pop(req_id, None)
                                await stream_queue.put(e)
                                await stream_queue.put(_STREAM_END)
                                continue

                            if response.partial is not True:
                                self._stream_queues.pop(req_id, None)
                                await stream_queue.put(_STREAM_END)
                            continue

                        future = self._pending.get(req_id)
                        if future and not future.done():
                            if response.partial is True:
                                chunk_data = (response.result or {}).get("data", [])
                                self._partial_data.setdefault(req_id, []).extend(chunk_data)
                            else:
                                try:
                                    result = response.unwrap()
                                    if req_id in self._partial_data:
                                        accumulated = self._partial_data.pop(req_id)
                                        if isinstance(result, dict) and "data" in result:
                                            result = {**result, "data": accumulated + result["data"]}
                                    future.set_result(result)
                                except Exception as e:
                                    self._partial_data.pop(req_id, None)
                                    future.set_exception(e)

                except json.JSONDecodeError as e:
                    print(f"Failed to parse response: {line}", e)
                except Exception as e:
                    print(f"Error processing response: {e}")

        except asyncio.CancelledError:
            raise  # disconnect() is responsible for terminating the engine
        except Exception as e:
            print(f"Reader task error: {e}")
            for future in self._pending.values():
                if not future.done():
                    future.set_exception(ProtocolError(f"Reader task failed: {e}"))
            self._fail_streams(ProtocolError(f"Reader task failed: {e}"))

    def transaction(
        self,
        callback=None,
        *,
        timeout_ms: int = 5000,
        isolation_level: Optional[IsolationLevel] = None,
    ):
        """Run operations inside a database transaction.

        Can be used as an **async context manager** *or* as a **callback-style
        transaction**.

        Context-manager (interactive) usage::

            async with client.transaction() as tx:
                user = await tx.user.create({"name": "Alice"})
                await tx.post.create({"title": "Hello", "authorId": user.id})

        Callback usage::

            async def work(tx):
                user = await tx.user.create({"name": "Bob"})
                return user

            user = await client.transaction(work, timeout_ms=10000)

        Args:
            callback: Optional async callable receiving a ``TransactionClient``.
                      If provided the transaction is committed on success and
                      rolled back on error; the return value of *callback* is
                      returned.
            timeout_ms: Server-side transaction timeout (default 5 000 ms).
            isolation_level: Optional SQL isolation level.

        Returns:
            When used with *callback*: an awaitable coroutine.
            When used as a context manager: an ``_AsyncTransactionContext``.
        """
        if callback is not None:
            return self._run_transaction_callback(
                callback, timeout_ms=timeout_ms, isolation_level=isolation_level
            )
        return _AsyncTransactionContext(
            self, timeout_ms=timeout_ms, isolation_level=isolation_level
        )

    async def _start_transaction(
        self,
        timeout_ms: int = 5000,
        isolation_level: Optional[IsolationLevel] = None,
    ) -> str:
        """Begin a server-side transaction and return its id."""
        params: Dict[str, Any] = {"protocolVersion": PROTOCOL_VERSION, "timeoutMs": timeout_ms}
        if isolation_level is not None:
            params["isolationLevel"] = isolation_level.value
        result = await self._rpc("transaction.start", params)
        return result["id"]

    async def _commit_transaction(self, tx_id: str) -> None:
        await self._rpc("transaction.commit", {"protocolVersion": PROTOCOL_VERSION, "id": tx_id})

    async def _rollback_transaction(self, tx_id: str) -> None:
        try:
            await self._rpc(
                "transaction.rollback",
                {"protocolVersion": PROTOCOL_VERSION, "id": tx_id},
            )
        except Exception:
            pass  # best-effort rollback

    async def _run_transaction_callback(
        self,
        callback,
        *,
        timeout_ms: int = 5000,
        isolation_level: Optional[IsolationLevel] = None,
    ):
        tx_id = await self._start_transaction(
            timeout_ms=timeout_ms, isolation_level=isolation_level
        )
        tx = TransactionClient(self, tx_id)
        try:
            result = await callback(tx)
        except Exception:
            await self._rollback_transaction(tx_id)
            raise
        await self._commit_transaction(tx_id)
        return result

    async def transaction_batch(
        self,
        operations: list,
        *,
        timeout_ms: int = 5000,
        isolation_level: Optional[IsolationLevel] = None,
    ) -> list:
        """Execute a list of operations atomically (batch transaction).

        Each element in *operations* is a dict ``{"method": "...", "params": {...}}``.

        Example::

            results = await client.transaction_batch([
                {"method": "query.create", "params": {
                    "protocolVersion": PROTOCOL_VERSION, "model": "User",
                    "data": {"name": "Alice"}}},
                {"method": "query.create", "params": {
                    "protocolVersion": PROTOCOL_VERSION, "model": "Post",
                    "data": {"title": "Hello"}}},
            ])

        Args:
            operations: List of RPC operations.
            timeout_ms: Server-side timeout.
            isolation_level: Optional isolation level.

        Returns:
            A list of result dicts, one per operation.
        """
        params: Dict[str, Any] = {
            "protocolVersion": PROTOCOL_VERSION,
            "operations": operations,
            "timeoutMs": timeout_ms,
        }
        if isolation_level is not None:
            params["isolationLevel"] = isolation_level.value
        result = await self._rpc("transaction.batch", params)
        return result.get("results", [])

    # These methods allow calling the async engine from synchronous code without
    # requiring the caller to manage an event loop.  They are used by the
    # generated sync delegates (interface = "sync") and by the sync context
    # manager (__enter__ / __exit__) on the generated Nautilus class.
    #
    # Implementation: a single background thread runs a dedicated asyncio event
    # loop for the lifetime of the client session.  Sync callers submit
    # coroutines to that loop via asyncio.run_coroutine_threadsafe(), which is
    # safe to call from *any* context — including from within another running
    # event loop (e.g. asyncio.run(main())) or a plain synchronous thread.

    def _ensure_sync_loop(self) -> asyncio.AbstractEventLoop:
        """Return the background event loop, starting it if necessary."""
        with self._sync_loop_lock:
            if self._sync_loop is None or not self._sync_loop.is_running():
                loop = asyncio.new_event_loop()
                thread = threading.Thread(
                    target=loop.run_forever,
                    daemon=True,
                    name="nautilus-sync-loop",
                )
                thread.start()
                self._sync_loop = loop
                self._sync_thread = thread
            return self._sync_loop

    def _sync_rpc(self, method: str, params: Dict[str, Any]) -> Any:
        """Synchronous RPC call — safe to call from any calling context.

        Submits the async ``_rpc`` coroutine to the background event loop and
        blocks the calling thread until the result is ready.
        """
        loop = self._ensure_sync_loop()
        future = asyncio.run_coroutine_threadsafe(self._rpc(method, params), loop)
        return future.result()

    def _sync_connect(self) -> None:
        """Synchronous connect — starts the background loop then connects."""
        loop = self._ensure_sync_loop()
        future = asyncio.run_coroutine_threadsafe(NautilusClient.connect(self), loop)
        try:
            future.result(timeout=_SYNC_CONNECT_TIMEOUT_S)
        except concurrent.futures.TimeoutError:
            future.cancel()
            raise TimeoutError(f"Engine connection timed out after {_SYNC_CONNECT_TIMEOUT_S} seconds")

    def _sync_disconnect(self) -> None:
        """Synchronous disconnect — disconnects and shuts down the background loop."""
        if self._sync_loop is None:
            return
        loop = self._sync_loop
        future = asyncio.run_coroutine_threadsafe(NautilusClient.disconnect(self), loop)
        future.result()
        loop.call_soon_threadsafe(loop.stop)
        if self._sync_thread is not None:
            self._sync_thread.join(timeout=_SYNC_LOOP_JOIN_TIMEOUT_S)
        self._sync_loop = None
        self._sync_thread = None

    def sync_transaction(
        self,
        callback=None,
        *,
        timeout_ms: int = 5000,
        isolation_level: Optional[IsolationLevel] = None,
    ):
        """Synchronous transaction — context-manager or callback style.

        Context-manager usage::

            with client.sync_transaction() as tx:
                user = tx.user.create({"name": "Alice"})
                tx.post.create({"title": "Hello", "authorId": user.id})

        Callback usage::

            def work(tx):
                return tx.user.create({"name": "Bob"})

            user = client.sync_transaction(work)
        """
        if callback is not None:
            return self._sync_run_transaction_callback(
                callback, timeout_ms=timeout_ms, isolation_level=isolation_level
            )
        return _SyncTransactionContext(
            self, timeout_ms=timeout_ms, isolation_level=isolation_level
        )

    def _sync_start_transaction(
        self,
        timeout_ms: int = 5000,
        isolation_level: Optional[IsolationLevel] = None,
    ) -> str:
        loop = self._ensure_sync_loop()
        future = asyncio.run_coroutine_threadsafe(
            self._start_transaction(timeout_ms, isolation_level), loop
        )
        return future.result()

    def _sync_commit_transaction(self, tx_id: str) -> None:
        loop = self._ensure_sync_loop()
        future = asyncio.run_coroutine_threadsafe(
            self._commit_transaction(tx_id), loop
        )
        future.result()

    def _sync_rollback_transaction(self, tx_id: str) -> None:
        loop = self._ensure_sync_loop()
        future = asyncio.run_coroutine_threadsafe(
            self._rollback_transaction(tx_id), loop
        )
        try:
            future.result()
        except Exception:
            pass

    def _sync_run_transaction_callback(
        self,
        callback,
        *,
        timeout_ms: int = 5000,
        isolation_level: Optional[IsolationLevel] = None,
    ):
        tx_id = self._sync_start_transaction(timeout_ms, isolation_level)
        tx = TransactionClient(self, tx_id)
        try:
            result = callback(tx)
        except Exception:
            self._sync_rollback_transaction(tx_id)
            raise
        self._sync_commit_transaction(tx_id)
        return result

    def sync_transaction_batch(
        self,
        operations: list,
        *,
        timeout_ms: int = 5000,
        isolation_level: Optional[IsolationLevel] = None,
    ) -> list:
        """Synchronous batch transaction."""
        loop = self._ensure_sync_loop()
        future = asyncio.run_coroutine_threadsafe(
            self.transaction_batch(
                operations,
                timeout_ms=timeout_ms,
                isolation_level=isolation_level,
            ),
            loop,
        )
        return future.result()

    def register_delegate(self, name: str, delegate: Any) -> None:
        """Register a model delegate.
        
        Args:
            name: The model name (snake_case).
            delegate: The delegate instance.
        """
        self._delegates[name] = delegate

    def get_delegate(self, name: str) -> Any:
        """Get a registered model delegate.
        
        Args:
            name: The model name (snake_case).
            
        Returns:
            The delegate instance.
            
        Raises:
            KeyError: If the delegate is not registered.
        """
        return self._delegates[name]

    def _set_as_global_instance(self) -> None:
        """Register this instance as the global Nautilus instance."""
        global _GLOBAL_INSTANCE
        with _auto_register_lock:
            if _GLOBAL_INSTANCE is not None:
                raise RuntimeError(
                    "A Nautilus instance with auto_register=True already exists. "
                    "Only one auto-registered instance is allowed at a time."
                )
            _GLOBAL_INSTANCE = self
            self._auto_registered = True

    def _clear_global_instance(self) -> None:
        """Clear the global instance if this is it."""
        global _GLOBAL_INSTANCE
        with _auto_register_lock:
            if _GLOBAL_INSTANCE is self:
                _GLOBAL_INSTANCE = None
                self._auto_registered = False

    @staticmethod
    def get_global_instance() -> Optional[NautilusClient]:
        """Get the globally registered Nautilus instance.
        
        Returns:
            The global instance, or None if no instance is registered.
        """
        return _GLOBAL_INSTANCE


class _AsyncTransactionContext(Generic[C]):
    """Async context manager returned by ``client.transaction()``."""

    def __init__(
        self,
        client: NautilusClient,
        *,
        timeout_ms: int = 5000,
        isolation_level: Optional[IsolationLevel] = None,
    ) -> None:
        self._client = client
        self._timeout_ms = timeout_ms
        self._isolation_level = isolation_level
        self._tx_id: Optional[str] = None
        self._tx: Optional[TransactionClient] = None

    async def __aenter__(self) -> C:  # type: ignore[override]
        self._tx_id = await self._client._start_transaction(
            self._timeout_ms, self._isolation_level
        )
        self._tx = TransactionClient(self._client, self._tx_id)
        return self._tx

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._tx_id is None:
            return
        if exc_type is not None:
            await self._client._rollback_transaction(self._tx_id)
        else:
            await self._client._commit_transaction(self._tx_id)
        self._tx_id = None


class _SyncTransactionContext(Generic[C]):
    """Sync context manager returned by ``client.sync_transaction()``."""

    def __init__(
        self,
        client: NautilusClient,
        *,
        timeout_ms: int = 5000,
        isolation_level: Optional[IsolationLevel] = None,
    ) -> None:
        self._client = client
        self._timeout_ms = timeout_ms
        self._isolation_level = isolation_level
        self._tx_id: Optional[str] = None
        self._tx: Optional[TransactionClient] = None

    def __enter__(self) -> C:  # type: ignore[override]
        self._tx_id = self._client._sync_start_transaction(
            self._timeout_ms, self._isolation_level
        )
        self._tx = TransactionClient(self._client, self._tx_id)
        return self._tx

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._tx_id is None:
            return
        if exc_type is not None:
            self._client._sync_rollback_transaction(self._tx_id)
        else:
            self._client._sync_commit_transaction(self._tx_id)
        self._tx_id = None
