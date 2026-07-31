"""Checkout resolution and orchestration for shared Serena lifecycle."""

from __future__ import annotations

import hashlib
import os
import subprocess
import time
from pathlib import Path
from typing import Any, Callable, Sequence

from . import leases as _leases
from . import models as _models
from . import process as _process

__all__ = [
    "backend_identity",
    "create_proxy_lease",
    "ensure_backend",
    "ensure_watchdog",
    "hash_checkout_path",
    "inspect_leases",
    "is_live_lease",
    "is_live_watchdog",
    "idle_shutdown_due",
    "paths_for_checkout",
    "profile_from_key",
    "resolve_checkout",
    "run_watchdog",
    "start_shared_serena",
    "status",
]


def _git(cwd: Path, *args: str) -> str:
    completed = subprocess.run(
        ["git", *args], cwd=cwd, text=True, capture_output=True, check=False
    )
    if completed.returncode:
        raise RuntimeError("Shared Serena must run from a Git checkout.")
    return completed.stdout.strip()


def resolve_checkout(cwd: Path | str | None = None) -> _models.Checkout:
    current = Path.cwd() if cwd is None else Path(cwd)
    root = Path(_git(current, "rev-parse", "--show-toplevel")).resolve()
    raw_common = Path(_git(current, "rev-parse", "--git-common-dir"))
    common = raw_common if raw_common.is_absolute() else current / raw_common
    return _models.Checkout(root=root, common_git_dir=common.resolve())


def hash_checkout_path(checkout: Path | str) -> str:
    return hashlib.sha256(os.fspath(checkout).encode()).hexdigest()


def paths_for_checkout(
    common_git_dir: Path | str,
    checkout: Path | str,
    *,
    profile: _models.RuntimeProfile,
) -> _models.StatePaths:
    common = Path(common_git_dir)
    checkout_key = hash_checkout_path(checkout)
    key = f"{checkout_key}.{profile.digest}"
    base = common / "serena-shared"
    return _models.StatePaths(
        base_dir=base,
        key=key,
        startup_lock=base / f"{key}.startup.flock",
        port_lock=base / "ports.flock",
        log=base / f"{key}.log",
        record=base / f"{key}.json",
    )


def _paths_for_key(common_git_dir: Path | str, key: str) -> _models.StatePaths:
    common = Path(common_git_dir)
    base = common / "serena-shared"
    return _models.StatePaths(
        base_dir=base,
        key=key,
        startup_lock=base / f"{key}.startup.flock",
        port_lock=base / "ports.flock",
        log=base / f"{key}.log",
        record=base / f"{key}.json",
    )


def profile_from_key(profile_key: str | None = None) -> _models.RuntimeProfile:
    return _models.RuntimeProfile.resolve(profile_key)


def start_shared_serena(
    checkout: Path,
    common_git_dir: Path,
    probe: Callable[[str], bool],
    profile: _models.RuntimeProfile,
    serena_args: Sequence[str],
) -> _models.SerenaRecord:
    return _process.start_shared_serena(
        checkout,
        common_git_dir,
        probe,
        profile,
        serena_args,
    )


def ensure_watchdog(
    checkout: _models.Checkout,
    paths: _models.StatePaths,
    profile: _models.RuntimeProfile,
) -> _models.SerenaRecord:
    with _process.file_lock(paths.startup_lock):
        record = _process.read_json(paths.record)
        if not _process.is_registry_record_for_profile(record, checkout.root, profile):
            raise RuntimeError(f"No verified shared Serena server exists for {checkout.root}.")
        if _process.is_live_watchdog(record.get("watchdog")):
            return record
        watchdog = _process.start_watchdog(checkout, paths)
        updated: _models.SerenaRecord = {**record, "watchdog": watchdog}
        _process.write_json_atomically(paths.record, updated)
        return updated


def is_live_watchdog(value: Any) -> bool:
    return _process.is_live_watchdog(value)


def ensure_backend(
    checkout: _models.Checkout,
    paths: _models.StatePaths,
    probe: Callable[[str], bool],
    profile: _models.RuntimeProfile,
    serena_args: Sequence[str],
) -> _models.SerenaRecord:
    record = start_shared_serena(
        checkout.root,
        checkout.common_git_dir,
        probe,
        profile,
        serena_args,
    )
    return ensure_watchdog(checkout, paths, profile) if not is_live_watchdog(record.get("watchdog")) else record


def backend_identity(
    paths: _models.StatePaths,
    checkout: Path,
    profile: _models.RuntimeProfile,
) -> tuple[str, int] | None:
    record = _process.read_json(paths.record)
    if not _process.is_registry_record_for_profile(record, checkout, profile):
        return None
    if not _process.is_owned_serena(record):
        return None
    return record["endpoint"], record["pid"]


def create_proxy_lease(
    idle_timeout_minutes: int,
    profile: _models.RuntimeProfile,
    serena_args: Sequence[str],
    *,
    cwd: Path | str | None = None,
) -> _leases.ProxyLease:
    return _leases.create_proxy_lease(
        idle_timeout_minutes,
        profile,
        serena_args,
        cwd=cwd,
    )


def is_live_lease(value: Any, checkout: Path | str) -> bool:
    return _leases.is_live_lease(value, checkout)


def inspect_leases(
    paths: _models.StatePaths,
    checkout: Path,
    *,
    remove_stale: bool = True,
) -> tuple[list[_models.LeaseRecord], int]:
    return _leases.inspect_leases(paths, checkout, remove_stale=remove_stale)


def idle_shutdown_due(leases: Sequence[_models.IdleState | _models.LeaseRecord], now: float) -> bool:
    return _leases.idle_shutdown_due(leases, now)


def run_watchdog(
    checkout_root: Path | str,
    common_git_dir: Path | str,
    profile_key: str,
    *,
    poll_seconds: float = 5.0,
) -> None:
    checkout = _models.Checkout(Path(checkout_root), Path(common_git_dir))
    paths = _paths_for_key(checkout.common_git_dir, profile_key)
    while True:
        with _process.file_lock(paths.startup_lock):
            record = _process.read_json(paths.record)
            profile_digest = profile_key.rsplit(".", 1)[-1]
            if not _process.is_registry_record_for_profile_digest(record, checkout.root, profile_digest):
                return
            if not _process.is_process_alive(record["pid"]):
                paths.record.unlink(missing_ok=True)
                return
            leases, _ = inspect_leases(paths, checkout.root)
            now = time.time()
            idle = idle_shutdown_due(leases, now)
            if (not checkout.root.exists() or idle) and _process.terminate_verified_serena(record):
                paths.record.unlink(missing_ok=True)
                return
        time.sleep(poll_seconds)


def _status_for_paths(
    checkout: _models.Checkout,
    paths: _models.StatePaths,
    profile_digest: str,
    probe: Callable[[str], bool],
) -> tuple[bool, _models.SerenaRecord | None, dict[str, Any]]:
    record = _process.read_json(paths.record)
    if not _process.is_registry_record_for_profile_digest(record, checkout.root, profile_digest):
        return False, None, {
            "liveProxies": 0, "inFlight": 0, "lastActivityAt": None,
            "nextShutdownAt": None, "watchdogHealthy": False,
        }
    healthy = bool(
        _process.is_process_alive(record["pid"])
        and _process.has_matching_record_fingerprint(record)
        and probe(record["endpoint"])
    )
    leases, _ = inspect_leases(paths, checkout.root, remove_stale=False)
    now = time.time()
    next_shutdown = (
        None if any(lease["inFlight"] for lease in leases)
        else max((lease["lastActivityAt"] + lease["idleTimeoutMinutes"] * 60 for lease in leases), default=None)
    )
    return healthy, record, {
        "liveProxies": len(leases),
        "inFlight": sum(lease["inFlight"] for lease in leases),
        "lastActivityAt": max((lease["lastActivityAt"] for lease in leases), default=None),
        "nextShutdownAt": next_shutdown if next_shutdown is None or next_shutdown > now else now,
        "watchdogHealthy": bool(is_live_watchdog(record.get("watchdog"))),
    }


def status(probe: Callable[[str], bool], cwd: Path | str | None = None) -> tuple[bool, dict[str, Any]]:
    checkout = resolve_checkout(cwd)
    checkout_key = hash_checkout_path(checkout.root)
    profiles: list[dict[str, Any]] = []
    base = Path(checkout.common_git_dir) / "serena-shared"
    for record_path in sorted(base.glob(f"{checkout_key}.*.json")):
        paths = _paths_for_key(checkout.common_git_dir, record_path.stem)
        profile_digest = record_path.stem.rsplit(".", 1)[-1]
        healthy, record, activity = _status_for_paths(checkout, paths, profile_digest, probe)
        if record is not None:
            profiles.append({
                "profileKey": record["profile"]["profileKey"],
                "healthy": healthy,
                "record": dict(record),
                "activity": activity,
            })
    healthy = bool(profiles) and all(profile["healthy"] for profile in profiles)
    return healthy, {
        "checkout": os.fspath(checkout.root),
        "healthy": healthy,
        "profiles": profiles,
    }
