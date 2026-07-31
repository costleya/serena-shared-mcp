from __future__ import annotations

import json
import os
import stat
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import anyio
import pytest
from mcp import Client, StdioServerParameters
from mcp.client.stdio import stdio_client

from serena_shared.runtime import lifecycle, process
from serena_shared.runtime import leases as runtime_leases


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

    def read_status() -> dict[str, Any]:
        return json.loads(
            subprocess.run(
                [sys.executable, "-m", "serena_shared.cli", "status"],
                cwd=checkout,
                env=environment,
                text=True,
                capture_output=True,
                check=True,
            ).stdout
        )

    async def call_from_two_clients() -> None:
        async with Client(stdio_client(params), read_timeout_seconds=10) as first_client:
            first = await first_client.call_tool("echo", {"value": "first"})
            status_one = read_status()
            async with Client(stdio_client(params), read_timeout_seconds=10) as second_client:
                second = await second_client.call_tool("echo", {"value": "second"})
                status_two = read_status()
                assert first.structured_content == {"result": "first"}
                assert second.structured_content == {"result": "second"}
                assert status_one["healthy"] and status_two["healthy"]
                first_record = status_one["record"]
                second_record = status_two["record"]
                assert isinstance(first_record, dict) and isinstance(second_record, dict)
                assert first_record["pid"] == second_record["pid"]

    anyio.run(call_from_two_clients)

    deadline = time.monotonic() + 10
    final_status: dict[str, Any] = {}
    while time.monotonic() < deadline:
        final_status = read_status()
        if final_status["record"] is None:
            break
        time.sleep(0.2)
    assert final_status["record"] is None


def test_two_stdio_clients_reuse_one_modern_http_backend(tmp_path: Path) -> None:
    exercise_bridge(tmp_path, FAKE_SERENA, sys.executable)


def test_open_stdio_client_restarts_idle_backend_transparently(tmp_path: Path) -> None:
    checkout = tmp_path / "checkout"
    binary = tmp_path / "bin"
    checkout.mkdir()
    binary.mkdir()
    subprocess.run(["git", "init", "-q"], cwd=checkout, check=True)
    fake_serena = binary / "serena"
    fake_serena.write_text(FAKE_SERENA.format(python=sys.executable))
    fake_serena.chmod(fake_serena.stat().st_mode | stat.S_IXUSR)
    environment = {**os.environ, "PATH": f"{binary}{os.pathsep}{os.environ['PATH']}"}
    params = StdioServerParameters(
        command=sys.executable,
        args=[
            "-m",
            "serena_shared.cli",
            "proxy",
            "--idle-timeout-minutes",
            "1",
        ],
        cwd=checkout,
        env=environment,
    )

    async def exercise_restart() -> None:
        async with Client(stdio_client(params), read_timeout_seconds=15) as client:
            first = await client.call_tool("echo", {"value": "before"})
            resolved = lifecycle.resolve_checkout(checkout)
            paths = lifecycle.paths_for_checkout(
                resolved.common_git_dir, resolved.root
            )
            original = process.read_json(paths.record)
            assert original is not None
            with process.file_lock(paths.startup_lock):
                leases = runtime_leases.lease_paths(paths)
                assert len(leases) == 1
                lease = process.read_json(leases[0])
                assert lease is not None
                process.write_json_atomically(
                    leases[0], {**lease, "lastActivityAt": time.time() - 120}
                )
            deadline = time.monotonic() + 20
            while paths.record.exists() and time.monotonic() < deadline:
                await anyio.sleep(0.2)
            assert not paths.record.exists()
            await anyio.sleep(0.2)
            second = await client.call_tool("echo", {"value": "after"})
            restarted = process.read_json(paths.record)
            assert restarted is not None
            assert first.structured_content == {"result": "before"}
            assert second.structured_content == {"result": "after"}
            assert restarted["pid"] != original["pid"]

    anyio.run(exercise_restart)


def test_v2_bridge_falls_back_to_legacy_http_backend(tmp_path: Path) -> None:
    legacy_python = os.environ.get("SERENA_SHARED_LEGACY_PYTHON")
    if not legacy_python:
        pytest.skip("SERENA_SHARED_LEGACY_PYTHON is not configured")
    exercise_bridge(tmp_path, LEGACY_FAKE_SERENA, legacy_python)
