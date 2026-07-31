from __future__ import annotations

from contextlib import asynccontextmanager
from collections.abc import AsyncGenerator
from typing import Any

# These tests intentionally exercise the bridge's private relay seams.
# pyright: reportPrivateUsage=false

import anyio
import pytest
from mcp.shared.message import SessionMessage
from mcp_types import JSONRPCRequest

from serena_shared.transport import bridge as bridge_module
from serena_shared.transport.protocol import adapt_client_message


class _Lease:
    def request_started(self) -> None:
        pass

    def request_finished(self) -> None:
        pass


class _EmptyReader:
    def __aiter__(self) -> Any:
        return self

    async def __anext__(self) -> Any:
        raise StopAsyncIteration


class _ErrorReader:
    def __init__(self, error: Exception) -> None:
        self._error = error

    def __aiter__(self) -> Any:
        return self

    async def __anext__(self) -> Any:
        raise self._error


class _Write:
    def __init__(self) -> None:
        self.sent: list[SessionMessage] = []

    async def send(self, item: SessionMessage) -> None:
        self.sent.append(item)


def _bridge() -> bridge_module.StdioHttpBridge:
    return bridge_module.StdioHttpBridge(
        _EmptyReader(),
        _Write(),
        lambda: ("http://localhost", 1),
        lambda: ("http://localhost", 1),
        _Lease(),
    )


def _message(identifier: int) -> SessionMessage:
    return SessionMessage(
        JSONRPCRequest(jsonrpc="2.0", id=identifier, method="tools/call", params={})
    )


def test_legacy_message_passes_without_transport_metadata() -> None:
    request = JSONRPCRequest(jsonrpc="2.0", id=1, method="initialize", params={})
    original = SessionMessage(request)
    assert adapt_client_message(original) is original


def test_modern_message_gains_required_http_headers() -> None:
    request = JSONRPCRequest(
        jsonrpc="2.0",
        id=2,
        method="tools/call",
        params={
            "name": "symbols/lookup",
            "arguments": {},
            "_meta": {
                "io.modelcontextprotocol/protocolVersion": "2026-07-28",
                "io.modelcontextprotocol/clientCapabilities": {},
            },
        },
    )
    adapted = adapt_client_message(SessionMessage(request))
    assert adapted.metadata is not None
    assert getattr(adapted.metadata, "headers", None) == {
        "mcp-protocol-version": "2026-07-28",
        "mcp-method": "tools/call",
        "mcp-name": "symbols/lookup",
    }
    assert adapted.message is request


def test_relay_uses_next_queued_message_after_backend_closes() -> None:
    first = _message(1)
    second = _message(2)
    bridge = _bridge()
    calls: list[SessionMessage] = []

    async def exercise() -> None:
        send, receive = anyio.create_memory_object_stream[SessionMessage](10)
        await send.send(first)
        await send.send(second)
        await send.aclose()

        async def run_connection(
            first: SessionMessage,
            queued_receive: Any,
            *,
            first_connection: bool,
        ) -> bridge_module._ConnectionResult:
            del queued_receive, first_connection
            calls.append(first)
            return bridge_module._ConnectionExit.BACKEND_CLOSED

        bridge._run_connection = run_connection
        await bridge._relay_connections(receive)

    anyio.run(exercise)
    assert calls == [first, second]


def test_relay_raises_when_backend_closes_with_pending_requests() -> None:
    bridge = _bridge()
    bridge._pending.add(1)

    async def exercise() -> None:
        send, receive = anyio.create_memory_object_stream[SessionMessage](1)
        await send.send(_message(1))
        await send.aclose()

        async def run_connection(
            first: SessionMessage,
            queued_receive: Any,
            *,
            first_connection: bool,
        ) -> bridge_module._ConnectionResult:
            del first, queued_receive, first_connection
            return bridge_module._ConnectionExit.BACKEND_CLOSED

        bridge._run_connection = run_connection
        with pytest.raises(RuntimeError, match="requests were in flight"):
            await bridge._relay_connections(receive)

    anyio.run(exercise)


def test_reconnect_retries_held_message_before_later_queued_messages() -> None:
    first = _message(1)
    held = _message(2)
    later = _message(3)
    bridge = _bridge()
    calls: list[SessionMessage] = []

    async def exercise() -> None:
        send, receive = anyio.create_memory_object_stream[SessionMessage](10)
        await send.send(first)
        await send.send(held)
        await send.send(later)
        await send.aclose()

        async def run_connection(
            first: SessionMessage,
            queued_receive: Any,
            *,
            first_connection: bool,
        ) -> bridge_module._ConnectionResult:
            calls.append(first)
            if len(calls) == 1:
                assert first_connection
                del queued_receive
                consumed = await receive.receive()
                assert consumed is held
                return bridge_module._Reconnect(held)
            assert not first_connection
            return bridge_module._ConnectionExit.BACKEND_CLOSED

        bridge._run_connection = run_connection
        await bridge._relay_connections(receive)

    anyio.run(exercise)
    assert calls == [first, held, later]


def test_closed_input_terminates_relay_cleanly() -> None:
    bridge = _bridge()
    calls: list[SessionMessage] = []

    async def exercise() -> None:
        send, receive = anyio.create_memory_object_stream[SessionMessage](1)
        await send.aclose()

        async def run_connection(
            first: SessionMessage,
            queued_receive: Any,
            *,
            first_connection: bool,
        ) -> bridge_module._ConnectionResult:
            del first, queued_receive, first_connection
            raise AssertionError("closed input should not start a connection")

        bridge._run_connection = run_connection
        await bridge._relay_connections(receive)

    anyio.run(exercise)
    assert calls == []


def test_reconnect_before_initialization_raises() -> None:
    bridge = _bridge()
    write = _Write()

    @asynccontextmanager
    async def fake_client(
        endpoint: str, *, terminate_on_close: bool
    ) -> AsyncGenerator[tuple[Any, Any], None]:
        del endpoint, terminate_on_close
        yield _EmptyReader(), write

    async def exercise() -> None:
        with pytest.raises(RuntimeError, match="before MCP initialization"):
            await bridge._run_connection(
                _message(1), _EmptyReader(), first_connection=False
            )

    original = bridge_module.streamable_http_client
    bridge_module.streamable_http_client = fake_client
    try:
        anyio.run(exercise)
    finally:
        bridge_module.streamable_http_client = original


def test_setup_error_propagates() -> None:
    bridge = _bridge()

    @asynccontextmanager
    async def fake_client(
        endpoint: str, *, terminate_on_close: bool
    ) -> AsyncGenerator[tuple[Any, Any], None]:
        del endpoint, terminate_on_close
        raise ValueError("setup failed")
        if False:  # pragma: no cover
            yield _EmptyReader(), _Write()

    async def exercise() -> None:
        send, receive = anyio.create_memory_object_stream[SessionMessage](1)
        await send.aclose()
        with pytest.raises(ValueError, match="setup failed"):
            await bridge._run_connection(
                _message(1), receive, first_connection=True
            )

    original = bridge_module.streamable_http_client
    bridge_module.streamable_http_client = fake_client
    try:
        anyio.run(exercise)
    finally:
        bridge_module.streamable_http_client = original


def test_relay_error_propagates() -> None:
    bridge = _bridge()

    async def exercise() -> None:
        send, receive = anyio.create_memory_object_stream[SessionMessage](1)
        await send.send(_message(1))
        await send.aclose()

        async def run_connection(
            first: SessionMessage,
            queued_receive: Any,
            *,
            first_connection: bool,
        ) -> bridge_module._ConnectionResult:
            del first, queued_receive, first_connection
            raise ValueError("relay failed")

        bridge._run_connection = run_connection
        with pytest.raises(ValueError, match="relay failed"):
            await bridge._relay_connections(receive)

    anyio.run(exercise)


def test_backend_error_with_pending_request_propagates() -> None:
    bridge = _bridge()
    bridge._pending.add(1)
    write = _Write()

    @asynccontextmanager
    async def fake_client(
        endpoint: str, *, terminate_on_close: bool
    ) -> AsyncGenerator[tuple[Any, Any], None]:
        del endpoint, terminate_on_close
        yield _ErrorReader(ValueError("backend failed")), write

    async def exercise() -> None:
        send, receive = anyio.create_memory_object_stream[SessionMessage](1)
        await send.aclose()
        with pytest.raises(ExceptionGroup) as caught:
            await bridge._run_connection(
                _message(1), receive, first_connection=True
            )
        assert any(
            isinstance(error, ValueError)
            for error in caught.value.exceptions
        )

    original = bridge_module.streamable_http_client
    bridge_module.streamable_http_client = fake_client
    try:
        anyio.run(exercise)
    finally:
        bridge_module.streamable_http_client = original


def test_input_close_wins_when_backend_exits_before_relay_marks_input() -> None:
    bridge = _bridge()
    request = _message(1)
    write = _Write()

    async def stdio_input() -> Any:
        yield request

    bridge._stdio_read = stdio_input()

    @asynccontextmanager
    async def fake_client(
        endpoint: str, *, terminate_on_close: bool
    ) -> AsyncGenerator[tuple[Any, Any], None]:
        del endpoint, terminate_on_close
        yield _EmptyReader(), write

    async def exercise() -> bridge_module._ConnectionResult:
        send, receive = anyio.create_memory_object_stream[SessionMessage](1)
        await bridge._read_stdio(send)
        first = await receive.receive()
        assert first is request
        assert bridge._pending == {1}
        return await bridge._run_connection(
            first, receive, first_connection=True
        )

    original = bridge_module.streamable_http_client
    bridge_module.streamable_http_client = fake_client
    try:
        result = anyio.run(exercise)
    finally:
        bridge_module.streamable_http_client = original

    assert result is bridge_module._ConnectionExit.INPUT_CLOSED
