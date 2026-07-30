from __future__ import annotations

import json
import os
import stat
import subprocess
import sys
from pathlib import Path

import anyio
import pytest
from mcp import Client, StdioServerParameters
from mcp.client.stdio import stdio_client


pytestmark = pytest.mark.skipif(
    os.environ.get("SERENA_SHARED_INTEGRATION") != "1",
    reason="requires process inspection and local sockets",
)


FAKE_SERENA = """#!{python}
import sys
from mcp.server import MCPServer

server = MCPServer("Fake Serena", log_level="ERROR")

@server.tool()
def echo(value: str) -> str:
    return value

args = sys.argv
port = int(args[args.index("--port") + 1])
server.run(
    "streamable-http",
    host="127.0.0.1",
    port=port,
    json_response=True,
    stateless_http=True,
)
"""

LEGACY_FAKE_SERENA = """#!{python}
import sys
from mcp.server.fastmcp import FastMCP

args = sys.argv
port = int(args[args.index("--port") + 1])
server = FastMCP(
    "Fake Legacy Serena",
    log_level="ERROR",
    host="127.0.0.1",
    port=port,
    json_response=True,
)

@server.tool()
def echo(value: str) -> str:
    return value

server.run("streamable-http")
"""


def exercise_bridge(tmp_path: Path, fake_source: str, python: str) -> None:
    checkout = tmp_path / "checkout"
    binary = tmp_path / "bin"
    checkout.mkdir()
    binary.mkdir()
    subprocess.run(["git", "init", "-q"], cwd=checkout, check=True)
    fake_serena = binary / "serena"
    fake_serena.write_text(fake_source.format(python=python))
    fake_serena.chmod(fake_serena.stat().st_mode | stat.S_IXUSR)
    environment = {**os.environ, "PATH": f"{binary}{os.pathsep}{os.environ['PATH']}"}
    params = StdioServerParameters(
        command=sys.executable,
        args=["-m", "serena_shared.cli", "proxy"],
        cwd=checkout,
        env=environment,
    )

    async def call(value: str) -> str:
        async with Client(stdio_client(params), read_timeout_seconds=10) as client:
            result = await client.call_tool("echo", {"value": value})
            assert result.structured_content is not None
            return str(result.structured_content["result"])

    first = anyio.run(call, "first")
    status_one = json.loads(
        subprocess.run(
            [sys.executable, "-m", "serena_shared.cli", "status"],
            cwd=checkout,
            env=environment,
            text=True,
            capture_output=True,
            check=True,
        ).stdout
    )
    second = anyio.run(call, "second")
    status_two = json.loads(
        subprocess.run(
            [sys.executable, "-m", "serena_shared.cli", "status"],
            cwd=checkout,
            env=environment,
            text=True,
            capture_output=True,
            check=True,
        ).stdout
    )
    assert (first, second) == ("first", "second")
    assert status_one["healthy"] and status_two["healthy"]
    assert status_one["record"]["pid"] == status_two["record"]["pid"]

    stopped = subprocess.run(
        [sys.executable, "-m", "serena_shared.cli", "stop"],
        cwd=checkout,
        env=environment,
        text=True,
        capture_output=True,
        check=True,
    )
    assert "Stopped shared Serena" in stopped.stdout
    final_status = json.loads(
        subprocess.run(
            [sys.executable, "-m", "serena_shared.cli", "status"],
            cwd=checkout,
            env=environment,
            text=True,
            capture_output=True,
            check=True,
        ).stdout
    )
    assert final_status["record"] is None


def test_two_stdio_clients_reuse_one_modern_http_backend(tmp_path: Path) -> None:
    exercise_bridge(tmp_path, FAKE_SERENA, sys.executable)


def test_v2_bridge_falls_back_to_legacy_http_backend(tmp_path: Path) -> None:
    legacy_python = os.environ.get("SERENA_SHARED_LEGACY_PYTHON")
    if not legacy_python:
        pytest.skip("SERENA_SHARED_LEGACY_PYTHON is not configured")
    exercise_bridge(tmp_path, LEGACY_FAKE_SERENA, legacy_python)
