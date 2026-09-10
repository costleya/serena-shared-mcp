"""Opt-in local enforcement of Serena's ignored-path matcher."""

from __future__ import annotations

import os.path
import shutil
import subprocess
from collections.abc import Callable, Mapping
from typing import Any

from mcp.shared.message import SessionMessage
from mcp.types import JSONRPCResponse

from serena_shared._typing import is_json_object

from .protocol import message_id, message_method

IGNORED_PATH_CHECK_TIMEOUT_SECONDS = 10.0


class IgnoredPathChecker:
    """Ask the installed Serena CLI whether a path is ignored for one project."""

    def __init__(
        self, project: str, ignored_path_checker: Callable[[str], bool] | None = None
    ) -> None:
        self._project = project
        self._ignored_path_checker = ignored_path_checker or self.is_ignored_path

    def rejection_for(self, item: SessionMessage) -> SessionMessage | None:
        """Return a local tool error for a request the opt-in guard rejects."""
        if message_method(item) != "tools/call":
            return None
        params = _message_params(item)
        if params is None or params.get("name") != "search_for_pattern":
            return None
        arguments = params.get("arguments")
        if not is_json_object(arguments):
            return None
        if arguments.get("skip_ignored_files") is False:
            return _tool_error(
                item,
                "The proxy rejected search_for_pattern because "
                "--respect-ignored-paths forbids skip_ignored_files=false. "
                "Omit skip_ignored_files or set it to true.",
            )
        relative_path = arguments.get("relative_path")
        if not isinstance(relative_path, str):
            return None
        normalized_path = os.path.normpath(
            os.path.join(self._project, relative_path.strip())
        )
        try:
            ignored = self._ignored_path_checker(normalized_path)
        except RuntimeError as error:
            return _tool_error(
                item,
                "The proxy could not verify whether "
                f"relative_path {relative_path!r} is ignored: {error} "
                "Refusing to forward search_for_pattern while "
                "--respect-ignored-paths is enabled.",
            )
        if ignored:
            return _tool_error(
                item,
                "The proxy rejected search_for_pattern because "
                f"relative_path {relative_path!r} is ignored. "
                "Choose a non-ignored relative_path or run without "
                "--respect-ignored-paths.",
            )
        return None

    def is_ignored_path(self, path: str) -> bool:
        """Return Serena's exact ignored-path result, rejecting ambiguous output."""
        executable = shutil.which("serena")
        if executable is None:
            raise RuntimeError("Serena is not installed or is not available on PATH.")
        try:
            completed = subprocess.run(
                [executable, "project", "is_ignored_path", path, self._project],
                capture_output=True,
                check=False,
                text=True,
                timeout=IGNORED_PATH_CHECK_TIMEOUT_SECONDS,
            )
        except (OSError, ValueError, subprocess.TimeoutExpired) as error:
            raise RuntimeError(f"Could not run Serena's ignored-path check: {error}") from error
        if completed.returncode != 0:
            detail = completed.stderr.strip() or completed.stdout.strip()
            raise RuntimeError(detail or f"Serena exited with status {completed.returncode}.")
        if _matches_ignored_output(completed.stdout):
            return True
        if _matches_not_ignored_output(completed.stdout):
            return False
        raise RuntimeError("Serena returned an unexpected ignored-path result.")


def _message_params(item: SessionMessage) -> Mapping[str, object] | None:
    data: dict[str, Any] = item.message.model_dump(
        by_alias=True, mode="json", exclude_unset=True
    )
    params = data.get("params")
    return params if is_json_object(params) else None


def _tool_error(item: SessionMessage, text: str) -> SessionMessage | None:
    request_id = message_id(item)
    if request_id is None:
        return None
    return SessionMessage(
        JSONRPCResponse(
            jsonrpc="2.0",
            id=request_id,
            result={"content": [{"type": "text", "text": text}], "isError": True},
        )
    )


def _matches_ignored_output(output: str) -> bool:
    return output.startswith("Path '") and output.endswith(
        "' IS ignored by the project configuration.\n"
    )


def _matches_not_ignored_output(output: str) -> bool:
    return output.startswith("Path '") and output.endswith(
        "' IS IS NOT ignored by the project configuration.\n"
    )
