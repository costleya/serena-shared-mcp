from __future__ import annotations

import json
import subprocess
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


def test_proxy_timeout_defaults_to_fifteen_minutes() -> None:
    arguments = cli.parser().parse_args(["proxy"])
    assert arguments.idle_timeout_minutes == 15


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
