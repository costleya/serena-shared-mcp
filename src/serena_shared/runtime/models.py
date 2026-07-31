"""Domain models and persisted state records for shared Serena."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import NotRequired, TypedDict


@dataclass(frozen=True, slots=True)
class Checkout:
    root: Path
    common_git_dir: Path


@dataclass(frozen=True, slots=True)
class StatePaths:
    base_dir: Path
    key: str
    startup_lock: Path
    port_lock: Path
    log: Path
    record: Path

    def lease(self, lease_id: str) -> Path:
        return self.base_dir / f"{self.key}.{lease_id}.lease.json"


class ProcessFingerprint(TypedDict):
    pid: int
    startedAt: str
    command: str


class WatchdogRecord(TypedDict):
    pid: int
    process: ProcessFingerprint


class SerenaRecord(TypedDict):
    root: str
    pid: int
    port: int
    endpoint: str
    log: str
    process: ProcessFingerprint | None
    fingerprintAvailable: bool
    state: str
    watchdog: NotRequired[WatchdogRecord]


class IdleState(TypedDict):
    idleTimeoutMinutes: int
    lastActivityAt: float
    inFlight: int


class LeaseRecord(IdleState):
    root: str
    pid: int
    process: ProcessFingerprint
