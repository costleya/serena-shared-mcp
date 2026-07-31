"""Stateful stdio-to-HTTP MCP transport relay."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum, auto
from typing import Any

import anyio
from anyio.to_thread import run_sync as run_sync_in_worker_thread
from mcp.client.streamable_http import streamable_http_client
from mcp.shared.message import SessionMessage

from .protocol import (
    ActivityLease,
    adapt_client_message,
    message_id,
    message_method,
    replay_initialization,
)


class _ConnectionExit(Enum):
    BACKEND_CLOSED = auto()
    INPUT_CLOSED = auto()


@dataclass(frozen=True)
class _Reconnect:
    message: SessionMessage


type _ConnectionResult = _ConnectionExit | _Reconnect


@dataclass
class _ConnectionAttempt:
    backend_exited: bool = False
    input_exited: bool = False
    reconnect_item: SessionMessage | None = None


class StdioHttpBridge:
    """Own the state and lifecycle of a reconnecting stdio-to-HTTP relay."""

    def __init__(
        self,
        stdio_read: Any,
        stdio_write: Any,
        endpoint_provider: Callable[[], tuple[str, int]],
        identity_provider: Callable[[], tuple[str, int] | None],
        lease: ActivityLease,
    ) -> None:
        self._stdio_read = stdio_read
        self._stdio_write = stdio_write
        self._endpoint_provider = endpoint_provider
        self._identity_provider = identity_provider
        self._lease = lease
        self._initialize: SessionMessage | None = None
        self._initialized: SessionMessage | None = None
        self._pending: set[int | str] = set()
        self._input_closed = False

    async def run(self) -> None:
        queued_send, queued_receive = anyio.create_memory_object_stream[SessionMessage](
            100
        )
        async with anyio.create_task_group() as group:
            group.start_soon(self._read_stdio, queued_send)
            group.start_soon(self._relay_connections, queued_receive)

    async def _read_stdio(self, queued_send: Any) -> None:
        try:
            async for item in self._stdio_read:
                if isinstance(item, Exception):
                    raise item
                method = message_method(item)
                if method in ("initialize", "server/discover"):
                    self._initialize = item
                elif method == "notifications/initialized":
                    self._initialized = item
                request_id = message_id(item)
                if method is not None and request_id is not None:
                    self._lease.request_started()
                    self._pending.add(request_id)
                await queued_send.send(item)
        finally:
            self._input_closed = True
            await queued_send.aclose()

    async def _relay_connections(self, queued_receive: Any) -> None:
        async with queued_receive:
            first = await self._receive_next(queued_receive)
            if first is None:
                return

            first_connection = True
            while True:
                result = await self._run_connection(
                    first,
                    queued_receive,
                    first_connection=first_connection,
                )
                first_connection = False

                if result is _ConnectionExit.INPUT_CLOSED:
                    return

                if isinstance(result, _Reconnect):
                    first = result.message
                    continue

                if self._pending:
                    raise RuntimeError(
                        "Serena disconnected while requests were in flight."
                    )

                first = await self._receive_next(queued_receive)
                if first is None:
                    return

    async def _receive_next(self, queued_receive: Any) -> SessionMessage | None:
        try:
            return await queued_receive.receive()
        except anyio.EndOfStream:
            return None

    async def _run_connection(
        self,
        first: SessionMessage,
        queued_receive: Any,
        *,
        first_connection: bool,
    ) -> _ConnectionResult:
        endpoint, generation = await run_sync_in_worker_thread(
            self._endpoint_provider
        )
        attempt = _ConnectionAttempt()

        try:
            async with streamable_http_client(
                endpoint, terminate_on_close=True
            ) as streams:
                http_read, http_write = streams
                if not first_connection:
                    if self._initialize is None:
                        raise RuntimeError(
                            "Cannot restart Serena before MCP initialization."
                        )
                    await replay_initialization(
                        http_read,
                        http_write,
                        self._initialize,
                        self._initialized,
                    )
                await http_write.send(adapt_client_message(first))

                async with anyio.create_task_group() as group:
                    group.start_soon(
                        self._client_to_http,
                        group,
                        attempt,
                        queued_receive,
                        endpoint,
                        generation,
                        http_write,
                    )
                    group.start_soon(
                        self._http_to_client,
                        group,
                        attempt,
                        http_read,
                        self._stdio_write,
                    )
        except Exception:
            if self._pending or not attempt.backend_exited:
                raise

        if self._input_closed or attempt.input_exited:
            return _ConnectionExit.INPUT_CLOSED
        if attempt.reconnect_item is not None:
            return _Reconnect(attempt.reconnect_item)
        return _ConnectionExit.BACKEND_CLOSED

    async def _client_to_http(
        self,
        group: Any,
        attempt: _ConnectionAttempt,
        queued_receive: Any,
        endpoint: str,
        generation: int,
        http_write: Any,
    ) -> None:
        try:
            async for item in queued_receive:
                current = await run_sync_in_worker_thread(self._identity_provider)
                if current != (endpoint, generation):
                    attempt.reconnect_item = item
                    group.cancel_scope.cancel()
                    return
                await http_write.send(adapt_client_message(item))
            attempt.input_exited = True
        finally:
            group.cancel_scope.cancel()

    async def _http_to_client(
        self,
        group: Any,
        attempt: _ConnectionAttempt,
        http_read: Any,
        stdio_write: Any,
    ) -> None:
        try:
            async for item in http_read:
                if isinstance(item, Exception):
                    raise item
                response_id = message_id(item)
                if response_id in self._pending:
                    self._pending.remove(response_id)
                    self._lease.request_finished()
                await stdio_write.send(item)
        finally:
            attempt.backend_exited = True
            group.cancel_scope.cancel()
