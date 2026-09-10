from __future__ import annotations

import subprocess
from typing import Any

import pytest
from mcp.shared.message import SessionMessage
from mcp_types import JSONRPCRequest

from serena_shared.transport.ignored_paths import IgnoredPathChecker
from serena_shared.transport import ignored_paths


def _which(_name: str) -> str:
    return "/usr/bin/serena"


def test_serena_cli_ignored_path_output_is_parsed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[list[str]] = []

    def fake_run(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        calls.append(args)
        assert kwargs == {
            "capture_output": True,
            "check": False,
            "text": True,
            "timeout": 10,
        }
        return subprocess.CompletedProcess(
            args, 0, "Path '/checkout/ignored.txt' IS ignored by the project configuration.\n", ""
        )

    monkeypatch.setattr(ignored_paths.shutil, "which", _which)
    monkeypatch.setattr(ignored_paths.subprocess, "run", fake_run)

    assert IgnoredPathChecker("/checkout/project").is_ignored_path("/checkout/ignored.txt")
    assert calls == [[
        "/usr/bin/serena", "project", "is_ignored_path", "/checkout/ignored.txt", "/checkout/project"
    ]]


def test_serena_cli_not_ignored_output_is_parsed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(ignored_paths.shutil, "which", _which)

    def fake_run(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        del kwargs
        return subprocess.CompletedProcess(
            args,
            0,
            "Path '/checkout/file.py' IS IS NOT ignored by the project configuration.\n",
            "",
        )

    monkeypatch.setattr(
        ignored_paths.subprocess, "run", fake_run
    )
    assert not IgnoredPathChecker("/checkout/project").is_ignored_path("/checkout/file.py")


def test_serena_cli_failure_is_reported_as_actionable_local_error() -> None:
    def checker(_path: str) -> bool:
        raise RuntimeError("Serena exited with status 2.")

    request = SessionMessage(
        JSONRPCRequest(
            jsonrpc="2.0",
            id=41,
            method="tools/call",
            params={
                "name": "search_for_pattern",
                "arguments": {"relative_path": "ignored.txt"},
            },
        )
    )
    response = IgnoredPathChecker("/checkout/project", checker).rejection_for(request)
    assert response is not None
    payload = response.message.model_dump(mode="json")
    assert payload["result"]["isError"] is True
    assert "Serena exited with status 2" in str(payload)


def test_serena_cli_unexpected_output_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(ignored_paths.shutil, "which", _which)

    def fake_run(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        del kwargs
        return subprocess.CompletedProcess(args, 0, "ambiguous\n", "")

    monkeypatch.setattr(
        ignored_paths.subprocess, "run", fake_run
    )
    with pytest.raises(RuntimeError, match="unexpected ignored-path result"):
        IgnoredPathChecker("/checkout/project").is_ignored_path("/checkout/file.py")


def test_serena_cli_timeout_fails_closed_with_actionable_local_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(ignored_paths.shutil, "which", _which)

    def fake_run(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        del kwargs
        raise subprocess.TimeoutExpired(args, 10)

    monkeypatch.setattr(ignored_paths.subprocess, "run", fake_run)
    request = SessionMessage(
        JSONRPCRequest(
            jsonrpc="2.0",
            id=42,
            method="tools/call",
            params={
                "name": "search_for_pattern",
                "arguments": {"relative_path": "ignored.txt"},
            },
        )
    )

    response = IgnoredPathChecker("/checkout/project").rejection_for(request)
    assert response is not None
    payload = response.message.model_dump(mode="json")
    assert payload["result"]["isError"] is True
    assert "timed out" in str(payload).lower()
