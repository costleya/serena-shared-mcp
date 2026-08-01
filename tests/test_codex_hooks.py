"""Contract tests for the repository's Codex/context-mode configuration."""

from __future__ import annotations

import json
import os
import shutil
import signal
import stat
import subprocess
import tomllib
from pathlib import Path
from typing import cast

import pytest


ROOT = Path(__file__).parents[1]
CONFIG = ROOT / ".codex" / "config.toml"
HOOK = ROOT / ".codex" / "hooks" / "context-mode-hook.mjs"
HOOKS_JSON = ROOT / ".codex" / "hooks.json"
EXPECTED_PROFILES = {
    "code_reviewer",
    "repo_explorer",
    "repo_implementer",
    "researcher",
    "test_implementer",
    "test_runner",
}
EXPECTED_EVENTS = {
    "pretooluse",
    "posttooluse",
    "sessionstart",
    "precompact",
    "userpromptsubmit",
    "stop",
}


def _toml(path: Path) -> dict[str, object]:
    return tomllib.loads(path.read_text(encoding="utf-8"))


def _context_mode_commands(value: object) -> list[str]:
    if isinstance(value, dict):
        mapping = cast(dict[object, object], value)
        commands: list[str] = []
        command = mapping.get("command")
        if isinstance(command, str) and "context-mode" in command:
            commands.append(command)
        for child in mapping.values():
            commands.extend(_context_mode_commands(child))
        return commands
    if isinstance(value, list):
        values = cast(list[object], value)
        commands: list[str] = []
        for child in values:
            commands.extend(_context_mode_commands(child))
        return commands
    return []


def _fake_context_mode(tmp_path: Path) -> tuple[Path, Path]:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    invocation_log = tmp_path / "invocation.txt"
    fake = bin_dir / "context-mode"
    fake.write_text(
        "#!/bin/sh\n"
        'printf \'%s\\n\' "$@" > "$CONTEXT_MODE_INVOCATION_LOG"\n'
        "cat\n"
        "printf 'fake-context-mode-stderr' >&2\n"
        'exit "${CONTEXT_MODE_EXIT_CODE:-0}"\n',
        encoding="utf-8",
    )
    fake.chmod(fake.stat().st_mode | stat.S_IXUSR)
    return bin_dir, invocation_log


def _node_executable() -> str:
    node = shutil.which("node")
    assert node is not None
    return node


def test_codex_context_mode_configuration_is_complete() -> None:
    config = _toml(CONFIG)
    assert config["features"]["hooks"] is True  # type: ignore[index]
    context_mode = config["mcp_servers"]["context-mode"]  # type: ignore[index]
    assert context_mode["enabled"] is True  # type: ignore[index]
    assert context_mode["command"] == "context-mode"  # type: ignore[index]

    profiles = {path.stem: path for path in (ROOT / ".codex" / "agents").glob("*.toml")}
    assert set(profiles) == EXPECTED_PROFILES
    for profile_path in profiles.values():
        profile = _toml(profile_path)
        assert "features" not in profile or profile["features"].get("hooks") is not False  # type: ignore[union-attr]
        profile_context_mode = profile["mcp_servers"]["context-mode"]  # type: ignore[index]
        assert profile_context_mode["enabled"] is False  # type: ignore[index]
        assert profile_context_mode["command"] == "context-mode"  # type: ignore[index]


def test_context_mode_hook_skips_child_events_with_agent_id(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bin_dir, invocation_log = _fake_context_mode(tmp_path)
    monkeypatch.setenv("PATH", str(bin_dir) + os.pathsep + os.environ["PATH"])
    monkeypatch.setenv("CONTEXT_MODE_INVOCATION_LOG", str(invocation_log))

    for event in ("pretooluse", "posttooluse", "precompact", "userpromptsubmit"):
        result = subprocess.run(
            ["node", str(HOOK), event],
            input=b'{"agent_id":"child-1","event":"test"}',
            capture_output=True,
            check=False,
        )

        assert result.returncode == 0
        assert result.stdout == b""
        assert result.stderr == b""
    assert not invocation_log.exists()


@pytest.mark.parametrize(
    ("payload", "event"),
    [
        (b"{\"event\":\"missing-agent\"}", "posttooluse"),
        (b"{\"agent_id\":\"\"}", "precompact"),
        (b"not-json\n", "userpromptsubmit"),
        (b"session input", "sessionstart"),
        (b"stop input", "stop"),
    ],
)
def test_context_mode_hook_forwards_main_and_unidentified_events(
    tmp_path: Path, payload: bytes, event: str
) -> None:
    bin_dir, invocation_log = _fake_context_mode(tmp_path)
    environment = os.environ | {
        "PATH": str(bin_dir) + os.pathsep + os.environ["PATH"],
        "CONTEXT_MODE_INVOCATION_LOG": str(invocation_log),
        "CONTEXT_MODE_EXIT_CODE": "37",
    }
    result = subprocess.run(
        ["node", str(HOOK), event],
        input=payload,
        capture_output=True,
        env=environment,
        check=False,
    )

    assert result.returncode == 37
    assert result.stdout == payload
    assert result.stderr == b"fake-context-mode-stderr"
    assert invocation_log.read_text(encoding="utf-8").splitlines() == [
        "hook",
        "codex",
        event,
    ]


@pytest.mark.skipif(os.name == "nt", reason="signal semantics are POSIX-specific")
def test_context_mode_hook_sigterm_is_not_hidden_by_child_success(tmp_path: Path) -> None:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    fake = bin_dir / "context-mode"
    fake.write_text(
        "#!/bin/sh\n"
        "trap 'exit 0' TERM\n"
        "printf 'ready\\n'\n"
        "while :; do /bin/sleep 1; done\n",
        encoding="utf-8",
    )
    fake.chmod(fake.stat().st_mode | stat.S_IXUSR)
    environment = os.environ | {"PATH": str(bin_dir)}
    process = subprocess.Popen(
        [_node_executable(), str(HOOK), "posttooluse"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=environment,
    )
    try:
        assert process.stdin is not None
        process.stdin.write(b"main input")
        process.stdin.close()
        process.stdin = None
        assert process.stdout is not None
        assert process.stdout.readline() == b"ready\n"
        process.send_signal(signal.SIGTERM)
        stdout, stderr = process.communicate(timeout=5)
    finally:
        if process.poll() is None:
            process.kill()
            process.wait()

    assert process.returncode in {-signal.SIGTERM, 128 + signal.SIGTERM}
    assert stdout == b""
    assert stderr == b""


def test_context_mode_hook_reports_missing_context_mode(tmp_path: Path) -> None:
    empty_path = tmp_path / "empty-bin"
    empty_path.mkdir()
    environment = os.environ | {"PATH": str(empty_path)}

    result = subprocess.run(
        [_node_executable(), str(HOOK), "posttooluse"],
        input=b"main input",
        capture_output=True,
        env=environment,
        check=False,
    )

    assert result.returncode == 1
    assert result.stdout == b""
    error = result.stderr.decode(errors="replace").lower()
    assert "context-mode" in error
    assert "spawn" in error or "enoent" in error or "not found" in error


def test_hooks_json_routes_all_context_mode_events_through_wrapper() -> None:
    hooks = cast(dict[str, object], json.loads(HOOKS_JSON.read_text(encoding="utf-8")))
    commands = _context_mode_commands(hooks)
    assert len(commands) == len(EXPECTED_EVENTS)
    assert {command.rsplit(maxsplit=1)[-1] for command in commands} == EXPECTED_EVENTS
    assert all("context-mode-hook.mjs" in command for command in commands)
    assert all("node" in command for command in commands)

    hook_groups = cast(dict[str, object], hooks["hooks"])
    grepai_commands: list[str] = []
    for entries_value in hook_groups.values():
        entries = cast(list[object], entries_value)
        for entry_value in entries:
            entry = cast(dict[str, object], entry_value)
            hook_values = cast(list[object], entry["hooks"])
            for hook_value in hook_values:
                hook = cast(dict[str, object], hook_value)
                command = hook.get("command", "")
                if isinstance(command, str) and "grepai-reminder.mjs" in command:
                    grepai_commands.append(command)
    assert grepai_commands
