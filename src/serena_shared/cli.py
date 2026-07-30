"""Command-line interface for the shared Serena MCP bridge."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from pathlib import Path

from . import __version__
from .lifecycle import (
    collect_stale_servers,
    ensure_shared_serena,
    status,
    stop_current_checkout,
)
from .transport import bridge_stdio, probe_mcp


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(
        prog="serena-shared",
        description="Share one Serena Streamable HTTP server per Git checkout.",
    )
    value.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    value.add_argument(
        "command",
        nargs="?",
        default="proxy",
        choices=("proxy", "status", "stop", "gc"),
    )
    return value


def run(argv: Sequence[str] | None = None, cwd: Path | str | None = None) -> int:
    command = parser().parse_args(argv).command
    if command == "proxy":
        record = ensure_shared_serena(probe_mcp, cwd)
        bridge_stdio(record["endpoint"])
        return 0
    if command == "status":
        _, output = status(probe_mcp, cwd)
        print(json.dumps(output, indent=2))
        return 0
    if command == "stop":
        _, message = stop_current_checkout(cwd)
        print(message)
        return 0
    print(json.dumps(collect_stale_servers(cwd), indent=2))
    return 0


def main() -> None:
    try:
        raise SystemExit(run())
    except (OSError, RuntimeError, TimeoutError, ValueError) as error:
        print(str(error), file=sys.stderr)
        raise SystemExit(1) from error


if __name__ == "__main__":
    main()
