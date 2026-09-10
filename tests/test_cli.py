from __future__ import annotations

import json
import subprocess
from types import SimpleNamespace
from pathlib import Path

import pytest

from serena_shared import cli


def test_status_is_observational_and_exits_successfully(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    subprocess.run(["git", "init", "-q"], cwd=checkout, check=True)
    assert cli.run(["status"], checkout) == 0
    output = json.loads(capsys.readouterr().out)
    assert output["checkout"] == str(checkout.resolve())
    assert output["healthy"] is False


def test_version(capsys: pytest.CaptureFixture[str]) -> None:
    try:
        cli.run(["--version"])
    except SystemExit as error:
        assert error.code == 0
    assert "serena-shared 0.1.0" in capsys.readouterr().out


def test_proxy_timeout_defaults_to_five_minutes() -> None:
    arguments = cli.parser().parse_args(["proxy"])
    assert arguments.idle_timeout_minutes == 5


def test_respect_ignored_paths_is_opt_in() -> None:
    assert cli.parser().parse_args(["proxy"]).respect_ignored_paths is False
    assert (
        cli.parser().parse_args(["proxy", "--respect-ignored-paths"])
        .respect_ignored_paths
        is True
    )


def test_status_rejects_respect_ignored_paths() -> None:
    with pytest.raises(SystemExit):
        cli.parser().parse_args(["status", "--respect-ignored-paths"])


def test_proxy_passes_respect_ignored_paths_to_bridge(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    checkout = SimpleNamespace(root=tmp_path)
    lease = SimpleNamespace(
        checkout=checkout,
        paths=SimpleNamespace(),
        profile=object(),
        serena_args=(),
    )
    closed = False
    bridge_kwargs: dict[str, object] = {}

    def close() -> None:
        nonlocal closed
        closed = True

    lease.close = close
    def fake_create_proxy_lease(*args: object, **kwargs: object) -> SimpleNamespace:
        del args, kwargs
        return lease

    def fake_bridge_stdio(*args: object, **kwargs: object) -> None:
        del args
        bridge_kwargs.update(kwargs)

    monkeypatch.setattr(cli, "create_proxy_lease", fake_create_proxy_lease)
    monkeypatch.setattr(
        cli,
        "bridge_stdio",
        fake_bridge_stdio,
    )

    assert cli.run(["proxy", "--respect-ignored-paths"], tmp_path) == 0
    assert bridge_kwargs["respect_ignored_paths"] is True
    assert closed


def test_proxy_timeout_accepts_positive_integer_minutes() -> None:
    arguments = cli.parser().parse_args(["proxy", "--idle-timeout-minutes", "45"])
    assert arguments.idle_timeout_minutes == 45


@pytest.mark.parametrize("value", ["0", "-1", "1.5", "later"])
def test_proxy_timeout_rejects_invalid_minutes(value: str) -> None:
    with pytest.raises(SystemExit):
        cli.parser().parse_args(["proxy", "--idle-timeout-minutes", value])


@pytest.mark.parametrize("command", ["stop", "gc"])
def test_removed_maintenance_commands_are_rejected(command: str) -> None:
    with pytest.raises(SystemExit):
        cli.parser().parse_args([command])
