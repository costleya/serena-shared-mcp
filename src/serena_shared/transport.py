"""MCP v2 health probing and transparent stdio-to-HTTP transport relay."""

from __future__ import annotations

from collections.abc import AsyncIterable
from typing import Any, cast

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


async def _copy_messages(
    source: AsyncIterable[SessionMessage | Exception],
    destination: Any,
    *,
    adapt: bool,
) -> None:
    async for item in source:
        if isinstance(item, Exception):
            raise item
        await destination.send(adapt_client_message(item) if adapt else item)


async def bridge_stdio_async(endpoint: str) -> None:
    """Relay one stdio connection to one independent upstream HTTP connection."""
    async with stdio_server() as (stdio_read, stdio_write):
        async with streamable_http_client(endpoint, terminate_on_close=True) as streams:
            http_read, http_write = streams
            async with anyio.create_task_group() as group:
                async def run_copy(source: Any, destination: Any, *, adapt: bool) -> None:
                    try:
                        await _copy_messages(source, destination, adapt=adapt)
                    finally:
                        group.cancel_scope.cancel()

                async def client_to_http() -> None:
                    await run_copy(stdio_read, http_write, adapt=True)

                async def http_to_client() -> None:
                    await run_copy(http_read, stdio_write, adapt=False)

                group.start_soon(client_to_http)
                group.start_soon(http_to_client)


def bridge_stdio(endpoint: str) -> None:
    try:
        anyio.run(bridge_stdio_async, endpoint)
    except* Exception as group:
        messages = "; ".join(str(error) for error in group.exceptions if str(error))
        raise RuntimeError(messages or "MCP transport bridge failed.") from group
