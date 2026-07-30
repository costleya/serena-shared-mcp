"""MCP v2 health probing and transparent stdio-to-HTTP transport relay."""

from __future__ import annotations

import uuid
from collections.abc import Callable
from typing import Any, Protocol, cast

import anyio
from mcp import Client
from mcp.client.streamable_http import streamable_http_client
from mcp.server.stdio import stdio_server
from mcp.shared.inbound import (
    MCP_METHOD_HEADER,
    MCP_NAME_HEADER,
    MCP_PROTOCOL_VERSION_HEADER,
    NAME_BEARING_METHODS,
    encode_header_value,
)
from mcp.shared.message import ClientMessageMetadata, SessionMessage
from anyio.to_thread import run_sync as run_sync_in_worker_thread

PROTOCOL_VERSION_META_KEY = "io.modelcontextprotocol/protocolVersion"


async def probe_mcp_async(endpoint: str, timeout: float = 3.0) -> bool:
    """Negotiate with an MCP endpoint using v2 discovery and legacy fallback."""
    try:
        with anyio.fail_after(timeout):
            async with Client(endpoint):
                return True
    except Exception:
        return False


def probe_mcp(endpoint: str, timeout: float = 3.0) -> bool:
    return anyio.run(probe_mcp_async, endpoint, timeout)


def adapt_client_message(session_message: SessionMessage) -> SessionMessage:
    """Attach required HTTP routing headers to a modern stdio request.

    Stdio has no transport headers. MCP 2026-07-28 carries its protocol version
    in the request envelope as well, which lets a transparent bridge recreate
    the mandatory Streamable HTTP routing headers without terminating MCP.
    """
    message = session_message.message
    method = getattr(message, "method", None)
    if not isinstance(method, str):
        return session_message
    data: dict[str, Any] = message.model_dump(
        by_alias=True, mode="json", exclude_unset=True
    )
    params = data.get("params")
    if not isinstance(params, dict):
        return session_message
    typed_params = cast(dict[str, Any], params)
    meta = typed_params.get("_meta")
    if not isinstance(meta, dict):
        return session_message
    typed_meta = cast(dict[str, Any], meta)
    protocol_version = typed_meta.get(PROTOCOL_VERSION_META_KEY)
    if not isinstance(protocol_version, str):
        return session_message
    headers = {
        MCP_PROTOCOL_VERSION_HEADER: protocol_version,
        MCP_METHOD_HEADER: method,
    }
    name_key = NAME_BEARING_METHODS.get(method)
    name = typed_params.get(name_key) if name_key else None
    if isinstance(name, str):
        headers[MCP_NAME_HEADER] = encode_header_value(name)
    return SessionMessage(message, ClientMessageMetadata(headers=headers))


class ActivityLease(Protocol):
    def request_started(self) -> None: ...

    def request_finished(self) -> None: ...


def message_method(item: SessionMessage) -> str | None:
    method = getattr(item.message, "method", None)
    return method if isinstance(method, str) else None


def message_id(item: SessionMessage) -> int | str | None:
    value = getattr(item.message, "id", None)
    return value if isinstance(value, (int, str)) else None


async def replay_initialization(
    http_read: Any,
    http_write: Any,
    initialize: SessionMessage,
    initialized: SessionMessage | None,
) -> None:
    replay_id = f"serena-shared-replay-{uuid.uuid4().hex}"
    replay_message = initialize.message.model_copy(update={"id": replay_id})
    await http_write.send(
        adapt_client_message(SessionMessage(replay_message, initialize.metadata))
    )
    while True:
        response = await http_read.receive()
        if isinstance(response, Exception):
            raise response
        if message_id(response) == replay_id:
            break
    if initialized is not None:
        await http_write.send(adapt_client_message(initialized))


async def bridge_stdio_async(
    endpoint_provider: Callable[[], tuple[str, int]],
    identity_provider: Callable[[], tuple[str, int] | None],
    lease: ActivityLease,
) -> None:
    """Relay stdio while reconnecting an automatically reaped HTTP backend."""
    async with stdio_server() as (stdio_read, stdio_write):
        queued_send, queued_receive = anyio.create_memory_object_stream[SessionMessage](100)
        initialize: SessionMessage | None = None
        initialized: SessionMessage | None = None
        pending: set[int | str] = set()
        stdio_closed = False

        async def read_stdio() -> None:
            nonlocal initialize, initialized, stdio_closed
            try:
                async for item in stdio_read:
                    if isinstance(item, Exception):
                        raise item
                    method = message_method(item)
                    if method in ("initialize", "server/discover"):
                        initialize = item
                    elif method == "notifications/initialized":
                        initialized = item
                    request_id = message_id(item)
                    if method is not None and request_id is not None:
                        lease.request_started()
                        pending.add(request_id)
                    await queued_send.send(item)
            finally:
                stdio_closed = True
                await queued_send.aclose()

        async def relay_connections() -> None:
            first_connection = True
            async with queued_receive:
                try:
                    first = await queued_receive.receive()
                except anyio.EndOfStream:
                    return
                while True:
                    endpoint, generation = await run_sync_in_worker_thread(
                        endpoint_provider
                    )
                    backend_closed = False
                    client_closed = False
                    reconnect_item: SessionMessage | None = None
                    try:
                        async with streamable_http_client(
                            endpoint, terminate_on_close=True
                        ) as streams:
                            http_read, http_write = streams
                            if not first_connection:
                                if initialize is None:
                                    raise RuntimeError(
                                        "Cannot restart Serena before MCP initialization."
                                    )
                                await replay_initialization(
                                    http_read, http_write, initialize, initialized
                                )
                            first_connection = False
                            await http_write.send(adapt_client_message(first))

                            async with anyio.create_task_group() as group:
                                async def client_to_http() -> None:
                                    nonlocal client_closed, reconnect_item
                                    try:
                                        async for item in queued_receive:
                                            current = await run_sync_in_worker_thread(
                                                identity_provider
                                            )
                                            if current != (endpoint, generation):
                                                reconnect_item = item
                                                group.cancel_scope.cancel()
                                                return
                                            await http_write.send(adapt_client_message(item))
                                        client_closed = True
                                    finally:
                                        group.cancel_scope.cancel()

                                async def http_to_client() -> None:
                                    nonlocal backend_closed
                                    try:
                                        async for item in http_read:
                                            if isinstance(item, Exception):
                                                raise item
                                            response_id = message_id(item)
                                            if response_id in pending:
                                                pending.remove(response_id)
                                                lease.request_finished()
                                            await stdio_write.send(item)
                                    finally:
                                        backend_closed = True
                                        group.cancel_scope.cancel()

                                group.start_soon(client_to_http)
                                group.start_soon(http_to_client)
                    except Exception:
                        if pending or not backend_closed:
                            raise
                    if client_closed or stdio_closed:
                        return
                    if reconnect_item is not None:
                        first = reconnect_item
                        continue
                    if pending:
                        raise RuntimeError(
                            "Serena disconnected while requests were in flight."
                        )
                    try:
                        first = await queued_receive.receive()
                    except anyio.EndOfStream:
                        return

        async with anyio.create_task_group() as group:
            group.start_soon(read_stdio)
            group.start_soon(relay_connections)


def bridge_stdio(
    endpoint_provider: Callable[[], tuple[str, int]],
    identity_provider: Callable[[], tuple[str, int] | None],
    lease: ActivityLease,
) -> None:
    try:
        anyio.run(
            bridge_stdio_async, endpoint_provider, identity_provider, lease
        )
    except* Exception as group:
        def messages_for(error: BaseException) -> list[str]:
            if isinstance(error, BaseExceptionGroup):
                typed_group = cast(BaseExceptionGroup[BaseException], error)
                return [
                    message
                    for item in typed_group.exceptions
                    for message in messages_for(item)
                ]
            return [str(error)] if str(error) else []

        messages = "; ".join(messages_for(group))
        raise RuntimeError(messages or "MCP transport bridge failed.") from group
