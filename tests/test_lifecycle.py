from __future__ import annotations


import subprocess
import threading
import time
from pathlib import Path
from typing import Any, NoReturn

import pytest

from serena_shared import lifecycle


def test_paths_are_worktree_specific_and_share_port_lock(tmp_path: Path) -> None:
    first = lifecycle.paths_for_checkout(tmp_path, "/checkout/one")
    second = lifecycle.paths_for_checkout(tmp_path, "/checkout/two")
    assert first.base_dir == tmp_path / "serena-shared"
    assert first.record != second.record
    assert first.port_lock == second.port_lock
    assert lifecycle.endpoint_for_port(9121) == "http://127.0.0.1:9121/mcp"
    assert lifecycle.orphan_record_path(first, 42).name.endswith(".42.orphan.json")


def test_resolve_checkout_distinguishes_linked_worktrees(tmp_path: Path) -> None:
    repository = tmp_path / "repository"
    worktree = tmp_path / "worktree"
    repository.mkdir()
    subprocess.run(["git", "init", "-q", "-b", "main"], cwd=repository, check=True)
    subprocess.run(["git", "config", "user.email", "test@example.com"], cwd=repository, check=True)
    subprocess.run(["git", "config", "user.name", "Test"], cwd=repository, check=True)
    (repository / "README").write_text("test\n")
    subprocess.run(["git", "add", "README"], cwd=repository, check=True)
    subprocess.run(["git", "commit", "-qm", "initial"], cwd=repository, check=True)
    subprocess.run(["git", "worktree", "add", "-q", "-b", "other", str(worktree)], cwd=repository, check=True)

    primary = lifecycle.resolve_checkout(repository)
    linked = lifecycle.resolve_checkout(worktree)
    assert primary.root != linked.root
    assert primary.common_git_dir == linked.common_git_dir
    assert lifecycle.paths_for_checkout(primary.common_git_dir, primary.root).record != lifecycle.paths_for_checkout(
        linked.common_git_dir, linked.root
    ).record


def test_atomic_json_replaces_complete_document(tmp_path: Path) -> None:
    target = tmp_path / "state" / "record.json"
    lifecycle.write_json_atomically(target, {"value": 1})
    lifecycle.write_json_atomically(target, {"value": 2, "complete": True})
    assert lifecycle.read_json(target) == {"value": 2, "complete": True}
    assert list(target.parent.glob(f".{target.name}.*")) == []


def test_flock_serializes_callers(tmp_path: Path) -> None:
    lock = tmp_path / "test.flock"
    active = 0
    maximum = 0

    def worker() -> None:
        nonlocal active, maximum
        with lifecycle.file_lock(lock):
            active += 1
            maximum = max(maximum, active)
            time.sleep(0.05)
            active -= 1

    threads = [threading.Thread(target=worker) for _ in range(3)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert maximum == 1


def test_process_fingerprint_rejects_non_serena_process(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_run(args: Any, **_kwargs: Any) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            args, 0, "123 Thu Jul 30 00:00:00 2026 sleep 1\n", ""
        )

    monkeypatch.setattr(
        lifecycle.subprocess,
        "run",
        fake_run,
    )
    fingerprint = lifecycle.process_fingerprint(123)
    assert fingerprint is not None
    record: dict[str, Any] = {
        "pid": fingerprint["pid"],
        "port": 9121,
        "root": "/checkout",
        "process": fingerprint,
    }
    assert not lifecycle.is_serena_fingerprint(fingerprint, record)


def test_process_liveness_reaps_owned_child(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_waitpid(pid: int, _options: int) -> tuple[int, int]:
        return pid, 0

    def fake_kill(_pid: int, _signal: int) -> NoReturn:
        pytest.fail("already reaped")

    monkeypatch.setattr(lifecycle.os, "waitpid", fake_waitpid)
    monkeypatch.setattr(lifecycle.os, "kill", fake_kill)
    assert not lifecycle.is_process_alive(123)


def test_start_reuses_healthy_backward_compatible_record(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    paths = lifecycle.paths_for_checkout(tmp_path / "git", checkout)
    process: dict[str, Any] = {
        "pid": 123,
        "startedAt": "Thu Jul 30 00:00:00 2026",
        "command": f"serena start-mcp-server --port 9123 --project {checkout}",
    }
    record: dict[str, Any] = {
        "root": str(checkout),
        "pid": 123,
        "port": 9123,
        "endpoint": lifecycle.endpoint_for_port(9123),
        "log": str(paths.log),
        "process": process,
        "fingerprintAvailable": True,
        "state": "active",
    }

    def process_is_alive(_pid: int) -> bool:
        return True

    def process_fingerprint(_pid: int) -> dict[str, Any]:
        return process

    def fail_start(*_args: Any) -> NoReturn:
        pytest.fail("must reuse")

    lifecycle.write_json_atomically(paths.record, record)
    monkeypatch.setattr(lifecycle, "is_process_alive", process_is_alive)
    monkeypatch.setattr(lifecycle, "process_fingerprint", process_fingerprint)
    monkeypatch.setattr(lifecycle, "start_serena", fail_start)
    assert lifecycle.start_shared_serena(checkout, tmp_path / "git", lambda endpoint: True) == record


def test_failed_startup_contains_verified_process(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    process: dict[str, Any] = {
        "pid": 321,
        "startedAt": "Thu Jul 30 00:00:00 2026",
        "command": f"serena start-mcp-server --port 9121 --project {checkout}",
    }

    def find_port(_attempted: set[int] | None = None) -> int:
        return 9121

    def start_process(*_args: Any) -> int:
        return 321

    def process_fingerprint(_pid: int) -> dict[str, Any]:
        return process

    def process_is_alive(_pid: int) -> bool:
        return True

    def terminate_process(_record: dict[str, Any]) -> bool:
        return True

    monkeypatch.setattr(lifecycle, "find_available_port", find_port)
    monkeypatch.setattr(lifecycle, "start_serena", start_process)
    monkeypatch.setattr(lifecycle, "process_fingerprint", process_fingerprint)
    monkeypatch.setattr(lifecycle, "is_process_alive", process_is_alive)
    monkeypatch.setattr(lifecycle, "terminate_verified_serena", terminate_process)
    monkeypatch.setattr(lifecycle, "STARTUP_TIMEOUT_SECONDS", 0)
    with pytest.raises(RuntimeError, match="contained"):
        lifecycle.start_shared_serena(checkout, tmp_path / "git", lambda endpoint: False)
    paths = lifecycle.paths_for_checkout(tmp_path / "git", checkout)
    assert lifecycle.read_json(paths.record) is None


def test_gc_removes_dead_and_retains_unverified_orphan(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    checkout = tmp_path / "checkout"
    git = checkout / ".git"
    git.mkdir(parents=True)
    paths = lifecycle.paths_for_checkout(git, checkout)
    dead = paths.base_dir / "dead.json"
    orphan = paths.base_dir / "orphan.json"
    lifecycle.write_json_atomically(dead, {"root": str(checkout), "pid": 1, "state": "active"})
    lifecycle.write_json_atomically(orphan, {"root": str(checkout), "pid": 2, "state": "orphan"})
    def resolve_checkout(_cwd: Path | str | None = None) -> lifecycle.Checkout:
        return lifecycle.Checkout(checkout, git)

    def process_is_alive(pid: int) -> bool:
        return pid == 2

    def is_owned_serena(_record: dict[str, Any]) -> bool:
        return False

    monkeypatch.setattr(lifecycle, "resolve_checkout", resolve_checkout)
    monkeypatch.setattr(lifecycle, "is_process_alive", process_is_alive)
    monkeypatch.setattr(lifecycle, "is_owned_serena", is_owned_serena)
    results = lifecycle.collect_stale_servers(checkout)
    assert not dead.exists()
    assert orphan.exists()
    assert {item.get("skipped") for item in results} == {None, "unverified-live-process"}


def test_status_shape_without_server(tmp_path: Path) -> None:
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    subprocess.run(["git", "init", "-q"], cwd=checkout, check=True)
    healthy, output = lifecycle.status(lambda endpoint: False, checkout)
    assert not healthy
    assert output == {"checkout": str(checkout.resolve()), "healthy": False, "record": None}
