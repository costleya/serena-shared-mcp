"""Command-line interface for the shared Serena MCP bridge."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from typing import cast
from pathlib import Path

from . import __version__
from .runtime.lifecycle import (
    backend_identity,
    create_proxy_lease,
    ensure_backend,
    profile_from_key,
    run_watchdog,
    status,
)
from .runtime.models import BackendIdentity
from .transport.protocol import bridge_stdio, probe_mcp


class _ArgumentParser(argparse.ArgumentParser):
    _blocked_serena_options = frozenset({
        "--project",
        "--project-file",
        "--project-from-cwd",
        "--transport",
        "--host",
        "--port",
    })
    _proxy_options = frozenset({
        "--profile-key",
        "--idle-timeout-minutes",
        "--respect-ignored-paths",
    })

    def parse_args(  # pyright: ignore[reportIncompatibleMethodOverride]
        self,
        args: Sequence[str] | None = None,
        namespace: argparse.Namespace | None = None,
    ) -> argparse.Namespace:
        values = tuple(sys.argv[1:] if args is None else args)
        try:
            separator = values.index("--")
        except ValueError:
            wrapper_args = values
            serena_args: list[str] = []
            explicit_tail = False
        else:
            wrapper_args = values[:separator]
            serena_args = list(values[separator + 1:])
            explicit_tail = True

        result = super().parse_args(wrapper_args, namespace)
        if result is None:
            raise RuntimeError("Argument parsing unexpectedly returned no namespace.")
        if result.command == "status":
            if explicit_tail:
                self.error("status does not accept Serena arguments")
            if any(value.split("=", 1)[0] in self._proxy_options for value in wrapper_args):
                self.error("proxy runtime options are not valid with status")
        if result.profile_key == "":
            self.error("Profile key must not be empty.")
        for argument in serena_args:
            if argument.split("=", 1)[0] in self._blocked_serena_options:
                self.error(f"Serena option is controlled by serena-shared: {argument}")
        result.serena_args = serena_args
        return result


def parser() -> _ArgumentParser:
    value = _ArgumentParser(
        prog="serena-shared",
        description="Share one Serena Streamable HTTP server per Git checkout.",
        allow_abbrev=False,
    )
    value.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    value.add_argument(
        "command",
        nargs="?",
        default="proxy",
        choices=("proxy", "status"),
    )
    value.add_argument("--profile-key", metavar="KEY")
    value.add_argument(
        "--idle-timeout-minutes",
        type=positive_integer,
        default=5,
        metavar="MINUTES",
        help="stop the Serena backend after this many idle minutes (default: 5)",
    )
    value.add_argument(
        "--respect-ignored-paths",
        action="store_true",
        help="locally reject search_for_pattern requests that target ignored paths",
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
    serena_args = tuple(cast(list[str], vars(arguments)["serena_args"]))
    command = arguments.command
    if command == "proxy":
        profile = profile_from_key(arguments.profile_key)
        lease = create_proxy_lease(
            arguments.idle_timeout_minutes,
            profile,
            serena_args,
            cwd=cwd,
        )
        try:
            def endpoint() -> BackendIdentity:
                ensure_backend(
                    lease.checkout,
                    lease.paths,
                    probe_mcp,
                    lease.profile,
                    lease.serena_args,
                )
                current = backend_identity(
                    lease.paths, lease.checkout.root, lease.profile
                )
                if current is None:
                    raise RuntimeError("Shared Serena backend ownership could not be verified.")
                return current

            def identity() -> BackendIdentity | None:
                return backend_identity(lease.paths, lease.checkout.root, lease.profile)

            bridge_stdio(
                endpoint,
                identity,
                lease,
                project=str(lease.checkout.root),
                respect_ignored_paths=arguments.respect_ignored_paths,
            )
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
        if len(sys.argv) == 5 and sys.argv[1] == "__watchdog":
            run_watchdog(sys.argv[2], sys.argv[3], sys.argv[4])
            raise SystemExit(0)
        raise SystemExit(run())
    except (OSError, RuntimeError, TimeoutError, ValueError) as error:
        print(str(error), file=sys.stderr)
        raise SystemExit(1) from error


if __name__ == "__main__":
    main()
