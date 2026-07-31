"""MCP v2 health probing and transparent stdio-to-HTTP transport relay."""

from __future__ import annotations

import uuid
from collections.abc import Callable, Mapping
from typing import Any, Protocol, cast

from serena_shared._typing import is_json_object

import anyio
from mcp import Client
from mcp.server.stdio import stdio_server
from mcp.shared.inbound import (
    MCP_METHOD_HEADER,
    MCP_NAME_HEADER,
    MCP_PROTOCOL_VERSION_HEADER,
    NAME_BEARING_METHODS,
    encode_header_value,
)
from mcp.shared.message import ClientMessageMetadata, SessionMessage

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


def _message_params(message: Any) -> Mapping[str, object] | None:
    data: dict[str, Any] = message.model_dump(
        by_alias=True, mode="json", exclude_unset=True
    )
    params = data.get("params")
    if not is_json_object(params):
        return None
    return params


def _client_message_headers(
    session_message: SessionMessage,
) -> dict[str, str] | None:
    message = session_message.message
    method = getattr(message, "method", None)
    if not isinstance(method, str):
        return None

    params = _message_params(message)
    if params is None:
        return None

    meta = params.get("_meta")
    if not is_json_object(meta):
        return None

    protocol_version = meta.get(PROTOCOL_VERSION_META_KEY, None)
    if not isinstance(protocol_version, str):
        return None

    headers: dict[str, str] = {
        MCP_PROTOCOL_VERSION_HEADER: protocol_version,
        MCP_METHOD_HEADER: method,
    }

    name_key = NAME_BEARING_METHODS.get(method)
    name = params.get(name_key) if name_key is not None else None
    if isinstance(name, str):
        headers[MCP_NAME_HEADER] = encode_header_value(name)

    return headers


def adapt_client_message(session_message: SessionMessage) -> SessionMessage:
    """Attach required HTTP routing headers to a modern stdio request."""
    headers = _client_message_headers(session_message)
    if headers is None:
        return session_message
    return SessionMessage(
        session_message.message,
        ClientMessageMetadata(headers=headers),
    )


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
    from .bridge import StdioHttpBridge

    async with stdio_server() as (stdio_read, stdio_write):
        bridge = StdioHttpBridge(
            stdio_read, stdio_write, endpoint_provider, identity_provider, lease
        )
        await bridge.run()


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
