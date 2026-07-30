from __future__ import annotations

from mcp.shared.message import SessionMessage
from mcp_types import JSONRPCRequest

from serena_shared.transport import adapt_client_message


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
