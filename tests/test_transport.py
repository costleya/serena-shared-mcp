from __future__ import annotations

from contextlib import asynccontextmanager
from collections.abc import AsyncGenerator
from typing import Any, cast

# These tests intentionally exercise the bridge's private relay seams.
# pyright: reportPrivateUsage=false

import anyio
import pytest
from mcp.shared.message import SessionMessage
from mcp_types import JSONRPCNotification, JSONRPCRequest, JSONRPCResponse

from serena_shared.transport import bridge as bridge_module
from serena_shared.transport.protocol import adapt_client_message


class _Lease:
    def request_started(self) -> None:
        pass

    def request_finished(self) -> None:
        pass


class _CountingLease(_Lease):
    def __init__(self) -> None:
        self.started = 0
        self.finished = 0

    def request_started(self) -> None:
        self.started += 1

    def request_finished(self) -> None:
        self.finished += 1


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


class _DelayedWrite(_Write):
    def __init__(self) -> None:
        super().__init__()
        self.started = anyio.Event()
        self.release = anyio.Event()
        self.barrier_sent = anyio.Event()

    async def send(self, item: SessionMessage) -> None:
        self.started.set()
        await self.release.wait()
        await super().send(item)
        if getattr(item.message, "method", None) == "ping":
            self.barrier_sent.set()


class _BarrierRead:
    def __init__(self, write: _DelayedWrite) -> None:
        self._write = write
        self._returned = False

    def __aiter__(self) -> Any:
        return self

    async def __anext__(self) -> SessionMessage:
        if self._returned:
            raise StopAsyncIteration
        await self._write.barrier_sent.wait()
        barrier = cast(Any, self._write.sent[-1].message)
        self._returned = True
        return SessionMessage(
            JSONRPCResponse(jsonrpc="2.0", id=barrier.id, result={})
        )


class _ActivationRead:
    def __init__(self, write: _Write, *, error: bool = False) -> None:
        self._write = write
        self._error = error

    async def receive(self) -> SessionMessage:
        request = cast(Any, self._write.sent[-1].message)
        result: dict[str, object] = {"isError": True} if self._error else {}
        return SessionMessage(
            JSONRPCResponse(jsonrpc="2.0", id=request.id, result=result)
        )

    def __aiter__(self) -> Any:
        return self

    async def __anext__(self) -> SessionMessage:
        raise StopAsyncIteration


class _CancelScope:
    def __init__(self) -> None:
        self.cancelled = False

    def cancel(self) -> None:
        self.cancelled = True


class _TaskGroup:
    def __init__(self) -> None:
        self.cancel_scope = _CancelScope()


def _identity(pid: int = 1, endpoint: str = "http://localhost") -> tuple[str, int, str, str, str | None]:
    return (endpoint, pid, "started", "serena start-mcp-server", "/checkout/project")


def _bridge() -> bridge_module.StdioHttpBridge:
    return bridge_module.StdioHttpBridge(
        _EmptyReader(),
        _Write(),
        lambda: _identity(),
        lambda: _identity(),
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


def test_client_requests_are_leased_without_counting_notifications() -> None:
    lease = _CountingLease()
    bridge = bridge_module.StdioHttpBridge(
        _EmptyReader(),
        _Write(),
        lambda: _identity(),
        lambda: _identity(),
        lease,
    )
    initialize = SessionMessage(
        JSONRPCRequest(jsonrpc="2.0", id="initialize", method="initialize", params={})
    )
    initialized = SessionMessage(
        JSONRPCNotification(
            jsonrpc="2.0", method="notifications/initialized", params={}
        )
    )
    tool_call = _message(7)

    async def input_messages() -> Any:
        yield initialize
        yield initialized
        yield tool_call

    bridge._stdio_read = input_messages()

    async def exercise() -> None:
        send, receive = anyio.create_memory_object_stream[SessionMessage](3)
        await bridge._read_stdio(send)
        assert [await receive.receive() for _ in range(3)] == [
            initialize,
            initialized,
            tool_call,
        ]

    anyio.run(exercise)
    assert lease.started == 2
    assert bridge._pending == {"initialize", 7}


def _initialize_messages() -> tuple[SessionMessage, SessionMessage]:
    return (
        SessionMessage(
            JSONRPCRequest(
                jsonrpc="2.0",
                id="init",
                method="initialize",
                params={
                    "_meta": {
                        "io.modelcontextprotocol/protocolVersion": "2026-07-28",
                        "io.modelcontextprotocol/clientCapabilities": {},
                    }
                },
            )
        ),
        SessionMessage(
            JSONRPCNotification(
                jsonrpc="2.0", method="notifications/initialized", params={}
            )
        ),
    )


def test_discovery_and_tools_list_do_not_require_activation() -> None:
    bridge = _bridge()
    initialize, _ = _initialize_messages()
    assert not bridge._requires_activation(initialize, _identity(11))
    assert not bridge._requires_activation(
        SessionMessage(
            JSONRPCRequest(jsonrpc="2.0", id=1, method="tools/list", params={})
        ),
        _identity(11),
    )


def test_first_tool_call_activates_once_per_backend_generation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    initialize, initialized = _initialize_messages()
    tool_call = _message(9)
    writes: list[_Write] = []

    @asynccontextmanager
    async def fake_client(
        endpoint: str, *, terminate_on_close: bool
    ) -> AsyncGenerator[tuple[_ActivationRead, _Write], None]:
        del endpoint, terminate_on_close
        write = _Write()
        writes.append(write)
        yield _ActivationRead(write), write

    async def exercise() -> None:
        bridge = bridge_module.StdioHttpBridge(
            _EmptyReader(), _Write(), lambda: _identity(101),
            lambda: _identity(101), _Lease(), "/checkout/project"
        )
        bridge._initialize = initialize
        bridge._initialized = initialized
        assert bridge._requires_activation(tool_call, _identity(101))
        result = await bridge._activate_project(tool_call, _identity(101))
        assert isinstance(result, bridge_module._Reconnect)
        assert result.message is tool_call
        assert not bridge._requires_activation(tool_call, _identity(101))
        assert bridge._requires_activation(tool_call, _identity(101, "http://other"))

    monkeypatch.setattr(bridge_module, "streamable_http_client", fake_client)
    anyio.run(exercise)
    assert len(writes) == 1
    sent = writes[0].sent
    assert [getattr(item.message, "method", None) for item in sent] == [
        "initialize", "notifications/initialized", "tools/call"
    ]
    activation = cast(Any, sent[-1].message)
    assert activation.id.startswith("serena-shared-activate-")
    assert activation.params["name"] == "activate_project"
    assert activation.params["arguments"] == {"project": "/checkout/project"}
    metadata = cast(Any, sent[-1].metadata)
    assert metadata is not None
    assert metadata.headers == {
        "mcp-protocol-version": "2026-07-28",
        "mcp-method": "tools/call",
        "mcp-name": "activate_project",
    }


def test_activation_uses_fresh_connection_then_forwards_tool_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    initialize, initialized = _initialize_messages()
    tool_call = _message(10)
    writes: list[_Write] = []

    @asynccontextmanager
    async def fake_client(
        endpoint: str, *, terminate_on_close: bool
    ) -> AsyncGenerator[tuple[_ActivationRead, _Write], None]:
        del endpoint, terminate_on_close
        write = _Write()
        writes.append(write)
        yield _ActivationRead(write), write

    async def exercise() -> None:
        bridge = bridge_module.StdioHttpBridge(
            _EmptyReader(), _Write(), lambda: _identity(101),
            lambda: _identity(101), _Lease(), "/checkout/project"
        )
        bridge._initialize = initialize
        bridge._initialized = initialized
        await bridge._activate_project(tool_call, _identity(101))
        send, receive = anyio.create_memory_object_stream[SessionMessage](1)
        await send.aclose()
        result = await bridge._run_connection(tool_call, receive, first_connection=False)
        assert result is bridge_module._ConnectionExit.BACKEND_CLOSED

    monkeypatch.setattr(bridge_module, "streamable_http_client", fake_client)
    anyio.run(exercise)
    assert len(writes) == 2
    assert writes[1].sent[-1] is tool_call


def test_activation_errors_propagate_and_hide_internal_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    initialize, initialized = _initialize_messages()
    lease = _CountingLease()
    client_output = _Write()
    writes: list[_Write] = []

    @asynccontextmanager
    async def fake_client(
        endpoint: str, *, terminate_on_close: bool
    ) -> AsyncGenerator[tuple[_ActivationRead, _Write], None]:
        del endpoint, terminate_on_close
        write = _Write()
        writes.append(write)
        yield _ActivationRead(write, error=True), write

    async def exercise() -> None:
        bridge = bridge_module.StdioHttpBridge(
            _EmptyReader(), client_output, lambda: _identity(),
            lambda: _identity(101), lease, "/checkout/project"
        )
        bridge._initialize = initialize
        bridge._initialized = initialized
        with pytest.raises(RuntimeError, match="could not activate"):
            await bridge._activate_project(_message(12), _identity(101))

    monkeypatch.setattr(bridge_module, "streamable_http_client", fake_client)
    anyio.run(exercise)
    assert client_output.sent == []
    assert lease.started == 0
    assert lease.finished == 0
    activation = cast(Any, writes[0].sent[-1].message)
    assert activation.id.startswith("serena-shared-activate-")


def test_activation_failure_after_identity_replacement_reconnects(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    initialize, initialized = _initialize_messages()
    current_identity = [_identity(101)]

    @asynccontextmanager
    async def fake_client(
        endpoint: str, *, terminate_on_close: bool
    ) -> AsyncGenerator[tuple[_ActivationRead, _Write], None]:
        del endpoint, terminate_on_close
        write = _Write()
        yield _ActivationRead(write), write

    async def failing_activation(*_args: Any) -> None:
        current_identity[0] = _identity(102)
        raise RuntimeError("replaced backend closed activation stream")

    async def exercise() -> bridge_module._ConnectionResult:
        bridge = bridge_module.StdioHttpBridge(
            _EmptyReader(), _Write(), lambda: _identity(101),
            lambda: current_identity[0], _Lease(), "/checkout/project"
        )
        bridge._initialize = initialize
        bridge._initialized = initialized
        result = await bridge._activate_project(
            _message(13), _identity(101)
        )
        assert isinstance(result, bridge_module._Reconnect)
        return result

    monkeypatch.setattr(bridge_module, "streamable_http_client", fake_client)
    monkeypatch.setattr(bridge_module, "activate_project", failing_activation)
    result = anyio.run(exercise)
    reconnect = cast(bridge_module._Reconnect, result)
    assert cast(Any, reconnect.message.message).id == 13


def test_forwarded_request_completes_before_queued_tool_activation() -> None:
    lease = _CountingLease()
    bridge = bridge_module.StdioHttpBridge(
        _EmptyReader(), _Write(), lambda: _identity(101),
        lambda: _identity(101), lease, "/checkout/project"
    )
    forwarded = SessionMessage(
        JSONRPCNotification(
            jsonrpc="2.0", method="notifications/initialized", params={}
        )
    )
    tool_call = _message(22)
    http_write = _Write()
    attempt = bridge_module._ConnectionAttempt()
    group = _TaskGroup()

    async def exercise() -> None:
        send, receive = anyio.create_memory_object_stream[SessionMessage](2)
        await send.send(forwarded)
        await send.send(tool_call)
        await send.aclose()
        await bridge._client_to_http(
            group, attempt, receive, _identity(101), http_write
        )

    anyio.run(exercise)
    assert [getattr(cast(Any, item.message), "method", None) for item in http_write.sent] == [
        "notifications/initialized", "ping"
    ]
    assert attempt.reconnect_item is tool_call
    assert attempt.forwarded == set()
    assert attempt.barrier_requested
    assert attempt.barrier_id is not None

    # The response/lease completion path is exercised independently by the
    # client-request accounting regression above; this assertion ensures the
    # activation trigger cannot overtake the already-forwarded request.
    assert lease.finished == 0
    assert bridge._pending == set()


def test_delayed_notification_send_completes_before_activation_reconnect() -> None:
    bridge = bridge_module.StdioHttpBridge(
        _EmptyReader(), _Write(), lambda: _identity(101),
        lambda: _identity(101), _CountingLease(), "/checkout/project"
    )
    notification = SessionMessage(
        JSONRPCNotification(
            jsonrpc="2.0", method="notifications/initialized", params={}
        )
    )
    tool_call = _message(23)
    http_write = _DelayedWrite()
    attempt = bridge_module._ConnectionAttempt()
    client_output = _Write()

    async def exercise() -> None:
        send, receive = anyio.create_memory_object_stream[SessionMessage](2)
        await send.send(notification)
        await send.send(tool_call)
        await send.aclose()
        async with anyio.create_task_group() as task_group:
            task_group.start_soon(
                bridge._client_to_http,
                task_group,
                attempt,
                receive,
                _identity(101),
                http_write,
            )
            task_group.start_soon(
                bridge._http_to_client,
                task_group,
                attempt,
                _BarrierRead(http_write),
                http_write,
                client_output,
            )
            await http_write.started.wait()
            assert attempt.reconnect_item is None
            assert http_write.sent == []
            http_write.release.set()

    anyio.run(exercise)
    assert [getattr(cast(Any, item.message), "method", None) for item in http_write.sent] == [
        "notifications/initialized", "ping"
    ]
    assert attempt.reconnect_item is tool_call
    assert attempt.barrier_id is None
    assert client_output.sent == []


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
