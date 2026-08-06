from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any, cast

import pytest

from serena_shared import cli
from serena_shared.runtime import process
from serena_shared.runtime.models import (
    RuntimeProfile,
    is_runtime_profile_record,
    is_serena_record,
)


def _tail(namespace: argparse.Namespace) -> list[str]:
    """Return the parser's opaque Serena argument tail without naming its field."""
    lists = [
        cast(list[str], value)
        for value in vars(namespace).values()
        if isinstance(value, list)
    ]
    assert len(lists) == 1
    return lists[0]


def _serena_record() -> dict[str, object]:
    port = process.PORT_START
    return {
        "root": "/tmp/checkout",
        "pid": 123,
        "port": port,
        "endpoint": process.endpoint_for_port(port),
        "log": "/tmp/serena.log",
        "process": None,
        "fingerprintAvailable": False,
        "state": "ready",
        "profile": RuntimeProfile.resolve().as_record(),
    }


def test_runtime_profile_record_guard_rejects_malformed_records() -> None:
    digest = RuntimeProfile.resolve().digest

    assert not is_runtime_profile_record({"profileKey": "", "digest": digest})
    assert not is_runtime_profile_record({"digest": digest})


def test_serena_record_guard_validates_the_complete_structure() -> None:
    record = _serena_record()
    assert is_serena_record(record)
    assert process.is_registry_record_for_profile_digest(
        record,
        "/tmp/checkout",
        RuntimeProfile.resolve().digest,
    )

    missing_log = _serena_record()
    del missing_log["log"]
    assert not is_serena_record(missing_log)
    assert not process.is_registry_record_for_profile_digest(
        missing_log,
        "/tmp/checkout",
    )

    missing_process = _serena_record()
    del missing_process["process"]
    assert not is_serena_record(missing_process)

    invalid_watchdog = _serena_record()
    invalid_watchdog["watchdog"] = None
    assert not is_serena_record(invalid_watchdog)


def test_proxy_parser_exposes_only_wrapper_options_and_forwards_tail() -> None:
    arguments = cli.parser().parse_args(
        [
            "proxy",
            "--profile-key",
            "Dev",
            "--idle-timeout-minutes",
            "45",
            "--",
            "--log-level",
            "debug",
            "--feature=x",
            "value with spaces",
        ]
    )

    assert arguments.command == "proxy"
    assert arguments.profile_key == "Dev"
    assert arguments.idle_timeout_minutes == 45
    assert _tail(arguments) == ["--log-level", "debug", "--feature=x", "value with spaces"]


def test_proxy_parser_preserves_empty_explicit_tail() -> None:
    arguments = cli.parser().parse_args(["proxy", "--"])
    assert _tail(arguments) == []


@pytest.mark.parametrize(
    "argv",
    [
        ["proxy", "--profile-key", ""],
        ["proxy", "--profile-key="],
    ],
)
def test_empty_profile_key_is_rejected(argv: list[str]) -> None:
    with pytest.raises(SystemExit):
        cli.parser().parse_args(argv)


@pytest.mark.parametrize("value", ["Dev", "dev", "DEV", "日本語"])
def test_profile_key_is_opaque_and_case_sensitive(value: str) -> None:
    arguments = cli.parser().parse_args(["proxy", "--profile-key", value])
    assert arguments.profile_key == value


@pytest.mark.parametrize(
    ("option", "value"),
    [
        ("--profile", "dev"),
        ("--profile-k", "dev"),
        ("--idle", "10"),
        ("--idle-timeout", "10"),
        ("--idle-timeout-m", "10"),
    ],
)
@pytest.mark.parametrize("command", ["proxy", "status"])
def test_abbreviated_wrapper_options_are_rejected(
    command: str, option: str, value: str
) -> None:
    with pytest.raises(SystemExit):
        cli.parser().parse_args([command, option, value])


@pytest.mark.parametrize(
    "token",
    [
        "--project",
        "--project=/tmp/project",
        "--project-file",
        "--project-file=/tmp/project.yml",
        "--project-from-cwd",
        "--project-from-cwd=/tmp",
        "--transport",
        "--transport=streamable-http",
        "--host",
        "--host=127.0.0.1",
        "--port",
        "--port=9121",
    ],
)
def test_reserved_serena_control_flags_are_rejected_in_tail(token: str) -> None:
    with pytest.raises(SystemExit):
        cli.parser().parse_args(["proxy", "--", token])


@pytest.mark.parametrize("token", ["--log-level", "--context", "--mode", "--memories"])
def test_non_reserved_tail_tokens_remain_opaque(token: str) -> None:
    arguments = cli.parser().parse_args(["proxy", "--", token, "value"])
    assert _tail(arguments) == [token, "value"]


@pytest.mark.parametrize("argv", [["status", "--"], ["status", "--", "--log-level", "debug"]])
def test_status_rejects_an_explicit_tail(argv: list[str]) -> None:
    with pytest.raises(SystemExit):
        cli.parser().parse_args(argv)


@pytest.mark.parametrize("option", ["--profile-key", "--idle-timeout-minutes"])
def test_status_rejects_wrapper_options(option: str) -> None:
    value = "dev" if option == "--profile-key" else "10"
    with pytest.raises(SystemExit):
        cli.parser().parse_args(["status", option, value])


def test_serena_start_uses_fixed_managed_arguments_then_exact_tail(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    log = tmp_path / "serena.log"
    calls: list[tuple[list[str], dict[str, Any]]] = []

    class FakeProcess:
        pid = 123

    def fake_popen(args: list[str], **kwargs: Any) -> FakeProcess:
        calls.append((args, kwargs))
        return FakeProcess()

    def fake_which(_name: str) -> str:
        return "/usr/bin/serena"

    monkeypatch.setattr(process.shutil, "which", fake_which)
    monkeypatch.setattr(process.subprocess, "Popen", fake_popen)

    process.start_serena(checkout, 9121, log, ["--log-level", "debug", "--feature=x"])

    argv = calls[0][0]
    assert argv[:8] == [
        "/usr/bin/serena",
        "start-mcp-server",
        "--transport",
        "streamable-http",
        "--host",
        "127.0.0.1",
        "--port",
        "9121",
    ]
    assert argv[8:] == ["--log-level", "debug", "--feature=x"]
    assert "--project" not in argv
