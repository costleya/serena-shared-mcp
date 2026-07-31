"""Proxy leases and idle-shutdown policy for shared Serena."""

from __future__ import annotations

import os
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence, TypeGuard

from .models import Checkout, IdleState, LeaseRecord, StatePaths
from .process import (
    file_lock,
    is_json_object,
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

    def _update(self, *, started: bool = False, finished: bool = False) -> None:
        with file_lock(self.paths.startup_lock):
            record = read_json(self.path)
            if not is_json_object(record):
                raise RuntimeError("Shared Serena proxy lease disappeared.")
            in_flight = int(record.get("inFlight", 0))
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


def create_proxy_lease(idle_timeout_minutes: int, cwd: Path | str | None = None) -> ProxyLease:
    if idle_timeout_minutes <= 0:
        raise ValueError("Idle timeout minutes must be a positive integer.")
    from .lifecycle import paths_for_checkout, resolve_checkout

    checkout = resolve_checkout(cwd)
    paths = paths_for_checkout(checkout.common_git_dir, checkout.root)
    fingerprint = process_fingerprint(os.getpid())
    if fingerprint is None:
        raise RuntimeError("Shared Serena proxy fingerprint could not be verified.")
    path = paths.lease(uuid.uuid4().hex)
    with file_lock(paths.startup_lock):
        write_json_atomically(path, {
            "root": os.fspath(checkout.root), "pid": os.getpid(), "process": fingerprint,
            "idleTimeoutMinutes": idle_timeout_minutes, "lastActivityAt": time.time(), "inFlight": 0,
        })
    return ProxyLease(checkout, paths, path)


def is_live_lease(record: Any, checkout: Path | str) -> TypeGuard[LeaseRecord]:
    if not (
        is_json_object(record)
        and record.get("root") == os.fspath(checkout)
        and isinstance(record.get("pid"), int)
        and isinstance(record.get("idleTimeoutMinutes"), int)
        and record["idleTimeoutMinutes"] > 0
        and isinstance(record.get("lastActivityAt"), (int, float))
        and isinstance(record.get("inFlight"), int)
        and record["inFlight"] >= 0
    ):
        return False
    actual = process_fingerprint(record["pid"])
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
