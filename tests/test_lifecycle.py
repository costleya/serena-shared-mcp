from __future__ import annotations


import subprocess
import threading
import time
from pathlib import Path
from typing import Any, NoReturn, cast

import pytest

from serena_shared.runtime import lifecycle, models, process
from serena_shared.runtime import leases as runtime_leases


def default_profile() -> models.RuntimeProfile:
    return lifecycle.profile_from_key()


def _fingerprint(checkout: Path, pid: int, port: int) -> dict[str, Any]:
    return {
        "pid": pid,
        "startedAt": "Thu Jul 30 00:00:00 2026",
        "command": f"serena start-mcp-server --port {port} --project {checkout}",
    }


def _pending_record(
    checkout: Path,
    paths: Any,
    profile: models.RuntimeProfile,
    *,
    pid: int,
    port: int,
    fingerprint: dict[str, Any] | None,
) -> dict[str, Any]:
    return {
        "root": str(checkout),
        "pid": pid,
        "port": port,
        "endpoint": process.endpoint_for_port(port),
        "log": str(paths.log),
        "process": fingerprint,
        "fingerprintAvailable": fingerprint is not None,
        "state": "pending",
        "profile": profile.as_record(),
        "probeState": "not-started",
    }


def test_paths_are_worktree_specific_and_share_port_lock(tmp_path: Path) -> None:
    profile = default_profile()
    first = lifecycle.paths_for_checkout(tmp_path, "/checkout/one", profile=profile)
    second = lifecycle.paths_for_checkout(tmp_path, "/checkout/two", profile=profile)
    assert first.base_dir == tmp_path / "serena-shared"
    assert first.record != second.record
    assert first.port_lock == second.port_lock
    assert process.endpoint_for_port(9121) == "http://127.0.0.1:9121/mcp"
    assert first.lease("client").name.endswith(".client.lease.json")


def test_lifecycle_no_longer_reexports_runtime_helpers() -> None:
    removed_names = {
        "endpoint_for_port",
        "file_lock",
        "IdleState",
        "is_process_alive",
        "lease_paths",
        "read_json",
        "StatePaths",
        "write_json_atomically",
    }
    assert all(not hasattr(lifecycle, name) for name in removed_names)
    assert all(name not in lifecycle.__all__ for name in removed_names)
    assert hasattr(lifecycle, "paths_for_checkout")
    assert "paths_for_checkout" in lifecycle.__all__


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
    profile = default_profile()
    assert lifecycle.paths_for_checkout(primary.common_git_dir, primary.root, profile=profile).record != lifecycle.paths_for_checkout(
        linked.common_git_dir, linked.root, profile=profile
    ).record


def test_atomic_json_replaces_complete_document(tmp_path: Path) -> None:
    target = tmp_path / "state" / "record.json"
    process.write_json_atomically(target, {"value": 1})
    process.write_json_atomically(target, {"value": 2, "complete": True})
    assert process.read_json(target) == {"value": 2, "complete": True}
    assert list(target.parent.glob(f".{target.name}.*")) == []


def test_flock_serializes_callers(tmp_path: Path) -> None:
    lock = tmp_path / "test.flock"
    active = 0
    maximum = 0

    def worker() -> None:
        nonlocal active, maximum
        with process.file_lock(lock):
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
        process.subprocess,
        "run",
        fake_run,
    )
    fingerprint = process.process_fingerprint(123)
    assert fingerprint is not None
    record: dict[str, Any] = {
        "pid": fingerprint["pid"],
        "port": 9121,
        "root": "/checkout",
        "process": fingerprint,
    }
    assert not process.is_serena_fingerprint(fingerprint, record)


def test_process_liveness_reaps_owned_child(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_waitpid(pid: int, _options: int) -> tuple[int, int]:
        return pid, 0

    def fake_kill(_pid: int, _signal: int) -> NoReturn:
        pytest.fail("already reaped")

    monkeypatch.setattr(process.os, "waitpid", fake_waitpid)
    monkeypatch.setattr(process.os, "kill", fake_kill)
    assert not process.is_process_alive(123)


def test_process_liveness_treats_non_child_zombie_as_dead(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def non_child_waitpid(_pid: int, _options: int) -> NoReturn:
        raise ChildProcessError

    def permitted_kill(_pid: int, _signal: int) -> None:
        return None

    def zombie_status(args: Any, **_kwargs: Any) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(args, 0, "Z+\n", "")

    monkeypatch.setattr(process.os, "waitpid", non_child_waitpid)
    monkeypatch.setattr(process.os, "kill", permitted_kill)
    monkeypatch.setattr(process.subprocess, "run", zombie_status)
    assert not process.is_process_alive(123)


def test_start_reuses_healthy_profile_record(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    profile = default_profile()
    paths = lifecycle.paths_for_checkout(tmp_path / "git", checkout, profile=profile)
    fingerprint_record: dict[str, Any] = {
        "pid": 123,
        "startedAt": "Thu Jul 30 00:00:00 2026",
        "command": f"serena start-mcp-server --port 9123 --project {checkout}",
    }
    record: dict[str, Any] = {
        "root": str(checkout),
        "pid": 123,
        "port": 9123,
        "endpoint": process.endpoint_for_port(9123),
        "log": str(paths.log),
        "process": fingerprint_record,
        "fingerprintAvailable": True,
        "state": "active",
        "profile": profile.as_record(),
    }

    def process_is_alive(_pid: int) -> bool:
        return True

    def process_fingerprint(_pid: int) -> dict[str, Any]:
        return fingerprint_record

    def fail_start(*_args: Any) -> NoReturn:
        pytest.fail("must reuse")

    process.write_json_atomically(paths.record, record)
    monkeypatch.setattr(process, "is_process_alive", process_is_alive)
    monkeypatch.setattr(process, "process_fingerprint", process_fingerprint)
    monkeypatch.setattr(process, "start_serena", fail_start)
    assert process.start_shared_serena(
        checkout, tmp_path / "git", lambda endpoint: True, profile, []
    ) == record


def test_start_persists_pending_record_before_verification(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    profile = default_profile()
    paths = lifecycle.paths_for_checkout(tmp_path / "git", checkout, profile=profile)
    fingerprint = _fingerprint(checkout, 501, 9123)
    observed: dict[str, Any] = {}
    clock = iter((0.0, 0.0, 0.0, 0.0, 2.0))

    def find_port(_attempted: set[int] | None = None) -> int:
        return 9123

    def start_process(*_args: Any) -> int:
        return 501

    def process_is_alive(_pid: int) -> bool:
        return True

    def no_sleep(_seconds: float) -> None:
        return None

    def process_fingerprint(_pid: int) -> dict[str, Any]:
        observed.update(process.read_json(paths.record) or {})
        return fingerprint

    monkeypatch.setattr(process, "find_available_port", find_port)
    monkeypatch.setattr(process, "start_serena", start_process)
    monkeypatch.setattr(process, "is_process_alive", process_is_alive)
    monkeypatch.setattr(process, "STARTUP_TIMEOUT_SECONDS", 1)
    monkeypatch.setattr(process.time, "monotonic", lambda: next(clock))
    monkeypatch.setattr(process.time, "sleep", no_sleep)
    monkeypatch.setattr(process, "process_fingerprint", process_fingerprint)

    with pytest.raises(RuntimeError, match=r"pending.*probe failed"):
        process.start_shared_serena(
            checkout, tmp_path / "git", lambda _endpoint: False, profile, []
        )

    assert observed["pid"] == 501
    assert observed["state"] == "pending"
    assert observed["fingerprintAvailable"] is False


def test_start_adopts_verified_pending_record(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    profile = default_profile()
    paths = lifecycle.paths_for_checkout(tmp_path / "git", checkout, profile=profile)
    fingerprint = _fingerprint(checkout, 502, 9124)
    process.write_json_atomically(
        paths.record,
        _pending_record(
            checkout, paths, profile, pid=502, port=9124, fingerprint=None
        ),
    )

    def process_is_alive(_pid: int) -> bool:
        return True

    def process_fingerprint(_pid: int) -> dict[str, Any]:
        return fingerprint

    def fail_start(*_args: Any) -> NoReturn:
        pytest.fail("must adopt pending backend")

    monkeypatch.setattr(process, "is_process_alive", process_is_alive)
    monkeypatch.setattr(process, "process_fingerprint", process_fingerprint)
    monkeypatch.setattr(process, "start_serena", fail_start)

    result = process.start_shared_serena(
        checkout, tmp_path / "git", lambda _endpoint: True, profile, []
    )

    assert result["pid"] == 502
    assert result["state"] == "active"
    assert result["process"] == fingerprint
    assert result.get("probeState") == "succeeded"


def test_pending_launch_allows_fingerprint_to_stabilize_before_activation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    profile = default_profile()
    paths = lifecycle.paths_for_checkout(tmp_path / "git", checkout, profile=profile)
    pending = _pending_record(
        checkout, paths, profile, pid=506, port=9128, fingerprint=None
    )
    process.write_json_atomically(paths.record, pending)
    first = _fingerprint(checkout, 506, 9128)
    first["command"] = f"/tmp/bin/serena start-mcp-server --port 9128 --project {checkout}"
    second = _fingerprint(checkout, 506, 9128)
    second["command"] = (
        f"/usr/bin/python /tmp/bin/serena start-mcp-server "
        f"--port 9128 --project {checkout}"
    )
    fingerprints = iter((first, second))
    probes = iter((False, True))
    clock = iter((0.0, 0.0, 0.0))

    def process_is_alive(_pid: int) -> bool:
        return True

    def process_fingerprint(_pid: int) -> dict[str, Any]:
        return next(fingerprints)

    def probe(_endpoint: str) -> bool:
        return next(probes)

    def no_sleep(_seconds: float) -> None:
        return None

    monkeypatch.setattr(process, "is_process_alive", process_is_alive)
    monkeypatch.setattr(process, "process_fingerprint", process_fingerprint)
    monkeypatch.setattr(process, "STARTUP_TIMEOUT_SECONDS", 1)
    monkeypatch.setattr(process.time, "monotonic", lambda: next(clock))
    monkeypatch.setattr(process.time, "sleep", no_sleep)

    recovered = process.recover_pending_serena(
        paths.record, cast(models.SerenaRecord, pending), probe
    )

    assert recovered is not None
    assert recovered["state"] == "active"
    assert recovered["process"] == second
    assert recovered.get("probeState") == "succeeded"


def test_dead_pending_record_is_replaced_once_then_reused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    profile = default_profile()
    paths = lifecycle.paths_for_checkout(tmp_path / "git", checkout, profile=profile)
    process.write_json_atomically(
        paths.record,
        _pending_record(
            checkout, paths, profile, pid=503, port=9125, fingerprint=None
        ),
    )
    replacement = _fingerprint(checkout, 504, 9126)
    starts: list[int] = []

    def process_is_alive(pid: int) -> bool:
        return pid == 504

    def process_fingerprint(pid: int) -> dict[str, Any] | None:
        return replacement if pid == 504 else None

    def find_port(_attempted: set[int] | None = None) -> int:
        return 9126

    def start_process(*_args: Any) -> int:
        starts.append(1)
        return 504

    monkeypatch.setattr(process, "is_process_alive", process_is_alive)
    monkeypatch.setattr(process, "process_fingerprint", process_fingerprint)
    monkeypatch.setattr(process, "find_available_port", find_port)
    monkeypatch.setattr(process, "start_serena", start_process)

    first = process.start_shared_serena(
        checkout, tmp_path / "git", lambda _endpoint: True, profile, []
    )
    second = process.start_shared_serena(
        checkout, tmp_path / "git", lambda _endpoint: True, profile, []
    )

    assert first["pid"] == second["pid"] == 504
    assert starts == [1]


def test_live_unverified_pending_record_is_not_replaced_or_terminated(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    profile = default_profile()
    paths = lifecycle.paths_for_checkout(tmp_path / "git", checkout, profile=profile)
    unrelated: dict[str, Any] = {
        "pid": 505,
        "startedAt": "other",
        "command": "unrelated",
    }
    pending = _pending_record(
        checkout, paths, profile, pid=505, port=9127, fingerprint=unrelated
    )
    process.write_json_atomically(paths.record, pending)
    terminated: list[int] = []

    def process_is_alive(_pid: int) -> bool:
        return True

    def process_fingerprint(_pid: int) -> dict[str, Any]:
        return unrelated

    def terminate(record: dict[str, Any]) -> bool:
        terminated.append(record["pid"])
        return True

    def fail_start(*_args: Any) -> NoReturn:
        pytest.fail("must not replace unverified pending backend")

    monkeypatch.setattr(process, "is_process_alive", process_is_alive)
    monkeypatch.setattr(process, "process_fingerprint", process_fingerprint)
    monkeypatch.setattr(process, "terminate_verified_serena", terminate)
    monkeypatch.setattr(process, "start_serena", fail_start)

    with pytest.raises(RuntimeError, match=r"pending.*fingerprint-mismatch"):
        process.start_shared_serena(
            checkout, tmp_path / "git", lambda _endpoint: True, profile, []
        )

    assert terminated == []
    retained = process.read_json(paths.record)
    assert retained is not None
    assert retained["pid"] == 505
    assert retained["state"] == "pending"
    assert retained["probeState"] == "fingerprint-mismatch"


def test_process_start_uses_verified_serena_command(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    log = tmp_path / "serena.log"
    popen_calls = 0

    class FakeProcess:
        pid = 321

    def fake_popen(*_args: Any, **_kwargs: Any) -> FakeProcess:
        nonlocal popen_calls
        popen_calls += 1
        return FakeProcess()

    def fake_which(_name: str) -> str:
        return "/usr/bin/serena"

    monkeypatch.setattr(process.shutil, "which", fake_which)
    monkeypatch.setattr(process.subprocess, "Popen", fake_popen)

    assert process.start_serena(checkout, 9121, log, []) == 321
    assert popen_calls == 1
    assert log.exists()


def test_process_termination_kills_owned_process_without_lifecycle_forwarding(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    kill_calls = 0
    record = cast(
        process.SerenaRecord,
        {"pid": 321, "process": {"pid": 321, "startedAt": "now", "command": "serena"}},
    )

    def fake_kill(_pid: int, _signal: int) -> None:
        nonlocal kill_calls
        kill_calls += 1

    def process_is_owned(_record: process.SerenaRecord) -> bool:
        return True

    def process_is_alive(_pid: int) -> bool:
        return False

    monkeypatch.setattr(process, "is_owned_serena", process_is_owned)
    monkeypatch.setattr(process.os, "kill", fake_kill)
    monkeypatch.setattr(process, "is_process_alive", process_is_alive)

    assert process.terminate_verified_serena(record) is True
    assert kill_calls == 1


def test_failed_startup_preserves_verified_process_as_pending(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    profile = default_profile()
    paths = lifecycle.paths_for_checkout(tmp_path / "git", checkout, profile=profile)
    fingerprint_record = _fingerprint(checkout, 321, 9121)

    def find_port(_attempted: set[int] | None = None) -> int:
        return 9121

    def start_process(*_args: Any) -> int:
        return 321

    def process_fingerprint(_pid: int) -> dict[str, Any]:
        return fingerprint_record

    def process_is_alive(_pid: int) -> bool:
        return True

    monkeypatch.setattr(process, "find_available_port", find_port)
    monkeypatch.setattr(process, "start_serena", start_process)
    monkeypatch.setattr(process, "process_fingerprint", process_fingerprint)
    monkeypatch.setattr(process, "is_process_alive", process_is_alive)
    monkeypatch.setattr(process, "STARTUP_TIMEOUT_SECONDS", 0)

    with pytest.raises(RuntimeError, match=r"pending.*PID 321.*probe not-started"):
        process.start_shared_serena(
            checkout, tmp_path / "git", lambda _endpoint: False, profile, []
        )

    pending = process.read_json(paths.record)
    assert pending is not None
    assert pending["pid"] == 321
    assert pending["state"] == "pending"
    assert pending["fingerprintAvailable"] is False


def test_inspect_leases_removes_dead_and_retains_verified_proxy(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    checkout = tmp_path / "checkout"
    git = checkout / ".git"
    git.mkdir(parents=True)
    profile = default_profile()
    paths = lifecycle.paths_for_checkout(git, checkout, profile=profile)
    process_record = {"pid": 2, "startedAt": "now", "command": "serena-shared proxy"}
    dead = paths.lease("dead")
    live = paths.lease("live")
    base = {
        "root": str(checkout),
        "idleTimeoutMinutes": 15,
        "lastActivityAt": 100.0,
        "inFlight": 0,
    }
    process.write_json_atomically(dead, {**base, "pid": 1, "process": None})
    process.write_json_atomically(live, {**base, "pid": 2, "process": process_record})
    def fingerprint_for_live_proxy(pid: int) -> dict[str, int | str] | None:
        return process_record if pid == 2 else None

    monkeypatch.setattr(runtime_leases, "process_fingerprint", fingerprint_for_live_proxy)
    live_leases, removed = lifecycle.inspect_leases(paths, checkout)
    assert removed == 1
    assert not dead.exists()
    assert live.exists()
    assert live_leases == [{**base, "pid": 2, "process": process_record}]


def test_idle_shutdown_waits_for_every_timeout_and_in_flight_request() -> None:
    leases: list[models.IdleState] = [
        {"idleTimeoutMinutes": 15, "lastActivityAt": 0.0, "inFlight": 0},
        {"idleTimeoutMinutes": 30, "lastActivityAt": 0.0, "inFlight": 0},
    ]
    assert not lifecycle.idle_shutdown_due(leases, 29 * 60)
    assert lifecycle.idle_shutdown_due(leases, 30 * 60)
    leases[0]["inFlight"] = 1
    assert not lifecycle.idle_shutdown_due(leases, 60 * 60)


def test_watchdog_stops_verified_idle_backend(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    git = tmp_path / "git"
    profile = default_profile()
    paths = lifecycle.paths_for_checkout(git, checkout, profile=profile)
    record = {
        "root": str(checkout),
        "pid": 42,
        "port": 9121,
        "endpoint": process.endpoint_for_port(9121),
        "log": str(paths.log),
        "process": {"pid": 42, "startedAt": "1", "command": "serena"},
        "fingerprintAvailable": True,
        "state": "ready",
        "profile": profile.as_record(),
    }
    process.write_json_atomically(paths.record, record)
    def process_is_alive(_pid: int) -> bool:
        return True

    def terminate_record(value: dict[str, Any]) -> bool:
        return value == record

    monkeypatch.setattr(process, "is_process_alive", process_is_alive)
    monkeypatch.setattr(process, "terminate_verified_serena", terminate_record)
    lifecycle.run_watchdog(checkout, git, paths.key, poll_seconds=0)
    assert not paths.record.exists()


def test_watchdog_stops_backend_for_deleted_worktree(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    checkout = tmp_path / "deleted-checkout"
    git = tmp_path / "git"
    profile = default_profile()
    paths = lifecycle.paths_for_checkout(git, checkout, profile=profile)
    record = {
        "root": str(checkout),
        "pid": 42,
        "port": 9121,
        "endpoint": process.endpoint_for_port(9121),
        "log": str(paths.log),
        "process": {"pid": 42, "startedAt": "1", "command": "serena"},
        "fingerprintAvailable": True,
        "state": "ready",
        "profile": profile.as_record(),
    }
    process.write_json_atomically(paths.record, record)
    def process_is_alive(_pid: int) -> bool:
        return True

    def terminate_record(value: dict[str, Any]) -> bool:
        return value == record

    def active_lease(
        _paths: models.StatePaths,
        _checkout: Path,
        *,
        remove_stale: bool = True,
    ) -> tuple[list[dict[str, Any]], int]:
        del remove_stale
        return [
            {
                "idleTimeoutMinutes": 15,
                "lastActivityAt": time.time(),
                "inFlight": 1,
            }
        ], 0

    monkeypatch.setattr(process, "is_process_alive", process_is_alive)
    monkeypatch.setattr(process, "terminate_verified_serena", terminate_record)
    monkeypatch.setattr(
        lifecycle,
        "inspect_leases",
        active_lease,
    )
    lifecycle.run_watchdog(checkout, git, paths.key, poll_seconds=0)
    assert not paths.record.exists()


def test_status_shape_without_server(tmp_path: Path) -> None:
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    subprocess.run(["git", "init", "-q"], cwd=checkout, check=True)
    healthy, output = lifecycle.status(lambda endpoint: False, checkout)
    assert not healthy
    assert output["checkout"] == str(checkout.resolve())
    assert output["healthy"] is False
    assert isinstance(output["profiles"], list)
