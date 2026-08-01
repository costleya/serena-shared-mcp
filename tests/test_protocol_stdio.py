from __future__ import annotations

from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import asynccontextmanager
from typing import Any, cast

import anyio

from mcp.server.stdio import stdio_server as real_stdio_server

import serena_shared.transport.protocol as protocol


class _EofInput:
    def __aiter__(self) -> AsyncIterator[str]:
        return self

    async def __anext__(self) -> str:
        raise StopAsyncIteration


class _Output:
    async def write(self, value: str) -> int:
        return len(value)

    async def flush(self) -> None:
        return None


class _Lease:
    def request_started(self) -> None:
        pass

    def request_finished(self) -> None:
        pass


def test_bridge_stdio_async_exits_after_real_stdio_input_eof(
    monkeypatch: Any,
) -> None:
    """A clean stdio EOF must let the real MCP stdio context shut down."""

    @asynccontextmanager
    async def injected_stdio_server() -> AsyncGenerator[tuple[Any, Any], None]:
        async with real_stdio_server(
            stdin=cast(Any, _EofInput()),
            stdout=cast(Any, _Output()),
        ) as streams:
            yield streams

    monkeypatch.setattr(protocol, "stdio_server", injected_stdio_server)

    async def exercise() -> None:
        with anyio.fail_after(1):
            await protocol.bridge_stdio_async(
                lambda: ("http://localhost", 1),
                lambda: ("http://localhost", 1),
                _Lease(),
            )

    anyio.run(exercise)
