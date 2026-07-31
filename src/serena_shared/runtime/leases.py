"""Proxy leases and idle-shutdown policy for shared Serena."""

from __future__ import annotations

import os
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence, TypeGuard

from serena_shared._typing import is_json_object

from .models import Checkout, IdleState, LeaseRecord, RuntimeProfile, StatePaths
from .process import (
    file_lock,
    is_same_process_fingerprint,
    process_fingerprint,
    read_json,
    write_json_atomically,
)


def lease_paths(paths: StatePaths) -> list[Path]:
    return sorted(paths.base_dir.glob(f"{paths.key}.*.lease.json"))


@dataclass(slots=True)
class ProxyLease:
    checkout: Checkout
    paths: StatePaths
    path: Path
    profile: RuntimeProfile
    serena_args: tuple[str, ...]

    def _update(self, *, started: bool = False, finished: bool = False) -> None:
        with file_lock(self.paths.startup_lock):
            record = read_json(self.path)
            if not is_json_object(record):
                raise RuntimeError("Shared Serena proxy lease disappeared.")
            in_flight = record.get("inFlight", 0)
            if not isinstance(in_flight, int):
                raise RuntimeError("Shared Serena proxy lease is invalid.")
            if started:
                in_flight += 1
            if finished:
                in_flight = max(0, in_flight - 1)
            write_json_atomically(self.path, {**record, "inFlight": in_flight, "lastActivityAt": time.time()})

    def request_started(self) -> None:
        self._update(started=True)

    def request_finished(self) -> None:
        self._update(finished=True)

    def close(self) -> None:
        with file_lock(self.paths.startup_lock):
            self.path.unlink(missing_ok=True)


def create_proxy_lease(
    idle_timeout_minutes: int,
    profile: RuntimeProfile,
    serena_args: Sequence[str],
    *,
    cwd: Path | str | None = None,
) -> ProxyLease:
    if idle_timeout_minutes <= 0:
        raise ValueError("Idle timeout minutes must be a positive integer.")
    from .lifecycle import paths_for_checkout, resolve_checkout

    checkout = resolve_checkout(cwd)
    paths = paths_for_checkout(checkout.common_git_dir, checkout.root, profile=profile)
    fingerprint = process_fingerprint(os.getpid())
    if fingerprint is None:
        raise RuntimeError("Shared Serena proxy fingerprint could not be verified.")
    path = paths.lease(uuid.uuid4().hex)
    with file_lock(paths.startup_lock):
        write_json_atomically(path, {
            "root": os.fspath(checkout.root), "pid": os.getpid(), "process": fingerprint,
            "idleTimeoutMinutes": idle_timeout_minutes, "lastActivityAt": time.time(), "inFlight": 0,
        })
    return ProxyLease(checkout, paths, path, profile, tuple(serena_args))


def is_live_lease(record: Any, checkout: Path | str) -> TypeGuard[LeaseRecord]:
    if not is_json_object(record):
        return False
    pid = record.get("pid")
    idle_timeout = record.get("idleTimeoutMinutes")
    last_activity = record.get("lastActivityAt")
    in_flight = record.get("inFlight")
    if (
        record.get("root") != os.fspath(checkout)
        or not isinstance(pid, int)
        or not isinstance(idle_timeout, int)
        or idle_timeout <= 0
        or not isinstance(last_activity, (int, float))
        or not isinstance(in_flight, int)
        or in_flight < 0
    ):
        return False
    actual = process_fingerprint(pid)
    return actual is not None and is_same_process_fingerprint(actual, record.get("process"))


def inspect_leases(paths: StatePaths, checkout: Path, *, remove_stale: bool = True) -> tuple[list[LeaseRecord], int]:
    live: list[LeaseRecord] = []
    removed = 0
    for path in lease_paths(paths):
        record = read_json(path)
        if is_live_lease(record, checkout):
            live.append(record)
        elif remove_stale:
            path.unlink(missing_ok=True)
            removed += 1
    return live, removed


def idle_shutdown_due(leases: Sequence[IdleState], now: float) -> bool:
    return not leases or all(
        lease["inFlight"] == 0
        and now - lease["lastActivityAt"] >= lease["idleTimeoutMinutes"] * 60
        for lease in leases
    )
