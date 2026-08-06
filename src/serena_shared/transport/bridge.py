"""Stateful stdio-to-HTTP MCP transport relay."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any

import anyio
from anyio.to_thread import run_sync as run_sync_in_worker_thread
from mcp.client.streamable_http import streamable_http_client
from mcp.shared.message import SessionMessage
from serena_shared.runtime.models import BackendIdentity

from .protocol import (
    ActivityLease,
    activate_project,
    adapt_client_message,
    message_id,
    message_metadata,
    message_method,
    replay_initialization,
    transport_barrier_request,
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
    forwarded: set[int | str] = field(default_factory=lambda: set[int | str]())
    submitted_messages: int = 0
    completed_messages: int = 0
    barrier_requested: bool = False
    barrier_id: str | None = None


class StdioHttpBridge:
    """Own the state and lifecycle of a reconnecting stdio-to-HTTP relay."""

    def __init__(
        self,
        stdio_read: Any,
        stdio_write: Any,
        endpoint_provider: Callable[[], BackendIdentity],
        identity_provider: Callable[[], BackendIdentity | None],
        lease: ActivityLease,
        project: str | None = None,
    ) -> None:
        self._stdio_read = stdio_read
        self._stdio_write = stdio_write
        self._endpoint_provider = endpoint_provider
        self._identity_provider = identity_provider
        self._lease = lease
        self._project = project
        self._initialize: SessionMessage | None = None
        self._initialized: SessionMessage | None = None
        self._activated_identity: BackendIdentity | None = None
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
        identity = await run_sync_in_worker_thread(self._endpoint_provider)
        endpoint = identity[0]
        if self._requires_activation(first, identity):
            return await self._activate_project(first, identity)
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
                attempt.submitted_messages += 1
                request_id = message_id(first)
                if request_id is not None:
                    attempt.forwarded.add(request_id)
                else:
                    attempt.completed_messages += 1

                async with anyio.create_task_group() as group:
                    group.start_soon(
                        self._client_to_http,
                        group,
                        attempt,
                        queued_receive,
                        identity,
                        http_write,
                    )
                    group.start_soon(
                        self._http_to_client,
                        group,
                        attempt,
                        http_read,
                        http_write,
                        self._stdio_write,
                    )
        except Exception:
            if self._pending or not attempt.backend_exited:
                raise

        if attempt.reconnect_item is not None:
            if (
                attempt.submitted_messages != attempt.completed_messages
                or attempt.barrier_requested
            ):
                raise RuntimeError("Serena disconnected while requests were in flight.")
            return _Reconnect(attempt.reconnect_item)
        if self._input_closed or attempt.input_exited:
            return _ConnectionExit.INPUT_CLOSED
        return _ConnectionExit.BACKEND_CLOSED

    def _requires_activation(self, item: SessionMessage, identity: BackendIdentity) -> bool:
        return (
            self._project is not None
            and message_method(item) == "tools/call"
            and self._activated_identity != identity
        )

    async def _activate_project(
        self,
        item: SessionMessage,
        identity: BackendIdentity,
    ) -> _ConnectionResult:
        if self._initialize is None:
            raise RuntimeError("Cannot activate Serena before MCP initialization.")
        project = self._project
        if project is None:
            return _Reconnect(item)
        current = await run_sync_in_worker_thread(self._identity_provider)
        if current != identity:
            return _Reconnect(item)
        try:
            async with streamable_http_client(identity[0], terminate_on_close=True) as streams:
                http_read, http_write = streams
                await replay_initialization(
                    http_read,
                    http_write,
                    self._initialize,
                    self._initialized,
                )
                await activate_project(
                    http_read,
                http_write,
                project,
                message_metadata(self._initialize),
                )
        except Exception:
            current = await run_sync_in_worker_thread(self._identity_provider)
            if current != identity:
                return _Reconnect(item)
            raise
        current = await run_sync_in_worker_thread(self._identity_provider)
        if current != identity:
            return _Reconnect(item)
        self._activated_identity = identity
        return _Reconnect(item)

    async def _client_to_http(
        self,
        group: Any,
        attempt: _ConnectionAttempt,
        queued_receive: Any,
        identity: BackendIdentity,
        http_write: Any,
    ) -> None:
        try:
            async for item in queued_receive:
                current = await run_sync_in_worker_thread(self._identity_provider)
                if current != identity:
                    attempt.reconnect_item = item
                    group.cancel_scope.cancel()
                    return
                if self._requires_activation(item, identity):
                    attempt.reconnect_item = item
                    attempt.barrier_requested = True
                    if not attempt.forwarded:
                        await self._queue_transport_barrier(attempt, http_write)
                    return
                await http_write.send(adapt_client_message(item))
                attempt.submitted_messages += 1
                request_id = message_id(item)
                if request_id is not None:
                    attempt.forwarded.add(request_id)
                else:
                    attempt.completed_messages += 1
            attempt.input_exited = True
        finally:
            if attempt.reconnect_item is None:
                group.cancel_scope.cancel()

    async def _queue_transport_barrier(
        self,
        attempt: _ConnectionAttempt,
        http_write: Any,
    ) -> None:
        if attempt.barrier_id is not None:
            return
        request_id, request = transport_barrier_request(
            message_metadata(self._initialize) if self._initialize is not None else None
        )
        attempt.barrier_id = request_id
        await http_write.send(request)

    async def _http_to_client(
        self,
        group: Any,
        attempt: _ConnectionAttempt,
        http_read: Any,
        http_write: Any,
        stdio_write: Any,
    ) -> None:
        try:
            async for item in http_read:
                if isinstance(item, Exception):
                    raise item
                response_id = message_id(item)
                if response_id == attempt.barrier_id:
                    attempt.barrier_id = None
                    attempt.barrier_requested = False
                    group.cancel_scope.cancel()
                    return
                if response_id in attempt.forwarded:
                    attempt.forwarded.remove(response_id)
                    attempt.completed_messages += 1
                if response_id in self._pending:
                    self._pending.remove(response_id)
                    self._lease.request_finished()
                await stdio_write.send(item)
                if (
                    attempt.reconnect_item is not None
                    and attempt.barrier_requested
                    and not attempt.forwarded
                ):
                    await self._queue_transport_barrier(attempt, http_write)
        finally:
            attempt.backend_exited = True
            group.cancel_scope.cancel()
