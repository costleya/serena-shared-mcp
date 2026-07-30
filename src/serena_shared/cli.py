"""Command-line interface for the shared Serena MCP bridge."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from pathlib import Path

from . import __version__
from .lifecycle import (
    backend_identity,
    create_proxy_lease,
    ensure_backend,
    run_watchdog,
    status,
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
        choices=("proxy", "status"),
    )
    value.add_argument(
        "--idle-timeout-minutes",
        type=positive_integer,
        default=15,
        metavar="MINUTES",
        help="stop the Serena backend after this many idle minutes (default: 15)",
    )
    return value


def positive_integer(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("must be a positive integer") from error
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return parsed


def run(argv: Sequence[str] | None = None, cwd: Path | str | None = None) -> int:
    arguments = parser().parse_args(argv)
    command = arguments.command
    if command == "proxy":
        lease = create_proxy_lease(arguments.idle_timeout_minutes, cwd)
        try:
            def endpoint() -> tuple[str, int]:
                record = ensure_backend(lease.checkout, lease.paths, probe_mcp)
                return str(record["endpoint"]), int(record["pid"])

            def identity() -> tuple[str, int] | None:
                return backend_identity(lease.paths, lease.checkout.root)

            bridge_stdio(endpoint, identity, lease)
        finally:
            lease.close()
        return 0
    if command == "status":
        _, output = status(probe_mcp, cwd)
        print(json.dumps(output, indent=2))
        return 0
    return 0


def main() -> None:
    try:
        if len(sys.argv) == 4 and sys.argv[1] == "__watchdog":
            run_watchdog(sys.argv[2], sys.argv[3])
            raise SystemExit(0)
        raise SystemExit(run())
    except (OSError, RuntimeError, TimeoutError, ValueError) as error:
        print(str(error), file=sys.stderr)
        raise SystemExit(1) from error


if __name__ == "__main__":
    main()
