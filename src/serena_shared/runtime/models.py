"""Domain models and persisted state records for shared Serena."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import NotRequired, TypeGuard, TypedDict

from serena_shared._typing import is_json_object


@dataclass(frozen=True, slots=True)
class Checkout:
    root: Path
    common_git_dir: Path


@dataclass(frozen=True, slots=True)
class RuntimeProfile:
    """The explicit key that identifies one shared backend."""

    profile_key: str | None = None

    @property
    def digest(self) -> str:
        encoded = json.dumps(self.profile_key, separators=(",", ":")).encode()
        return hashlib.sha256(encoded).hexdigest()

    def as_record(self) -> RuntimeProfileRecord:
        return {"profileKey": self.profile_key, "digest": self.digest}

    @classmethod
    def resolve(cls, profile_key: str | None = None) -> RuntimeProfile:
        if profile_key == "":
            raise ValueError("Profile key must not be empty.")
        return cls(profile_key)


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
    cwd: NotRequired[str]


type BackendIdentity = tuple[str, int, str, str, str | None]


def is_process_fingerprint(value: object) -> TypeGuard[ProcessFingerprint]:
    """Validate a persisted process fingerprint."""
    if not is_json_object(value):
        return False
    return (
        isinstance(value.get("pid"), int)
        and isinstance(value.get("startedAt"), str)
        and isinstance(value.get("command"), str)
        and ("cwd" not in value or isinstance(value.get("cwd"), str))
    )


class WatchdogRecord(TypedDict):
    pid: int
    process: ProcessFingerprint


def is_watchdog_record(value: object) -> TypeGuard[WatchdogRecord]:
    """Validate a persisted watchdog record."""
    if not is_json_object(value):
        return False
    return isinstance(value.get("pid"), int) and is_process_fingerprint(
        value.get("process")
    )


class RuntimeProfileRecord(TypedDict):
    profileKey: str | None
    digest: str


def is_runtime_profile_record(value: object) -> TypeGuard[RuntimeProfileRecord]:
    """Validate a persisted runtime profile without raising on malformed input."""
    if (
        not is_json_object(value)
        or "profileKey" not in value
        or "digest" not in value
    ):
        return False
    profile_key = value["profileKey"]
    digest = value["digest"]
    if profile_key == "" or (
        profile_key is not None and not isinstance(profile_key, str)
    ):
        return False
    return (
        isinstance(digest, str)
        and digest == RuntimeProfile.resolve(profile_key).digest
    )


class SerenaRecord(TypedDict):
    root: str
    pid: int
    port: int
    endpoint: str
    log: str
    process: ProcessFingerprint | None
    fingerprintAvailable: bool
    state: str
    profile: RuntimeProfileRecord
    probeState: NotRequired[str]
    watchdog: NotRequired[WatchdogRecord]


def is_serena_record(value: object) -> TypeGuard[SerenaRecord]:
    """Validate the complete persisted Serena record structure."""
    if not is_json_object(value) or "process" not in value:
        return False
    process = value["process"]
    watchdog = value.get("watchdog")
    probe_state = value.get("probeState")
    return (
        isinstance(value.get("root"), str)
        and isinstance(value.get("pid"), int)
        and isinstance(value.get("port"), int)
        and isinstance(value.get("endpoint"), str)
        and isinstance(value.get("log"), str)
        and (process is None or is_process_fingerprint(process))
        and isinstance(value.get("fingerprintAvailable"), bool)
        and isinstance(value.get("state"), str)
        and is_runtime_profile_record(value.get("profile"))
        and ("probeState" not in value or isinstance(probe_state, str))
        and ("watchdog" not in value or is_watchdog_record(watchdog))
    )


class IdleState(TypedDict):
    idleTimeoutMinutes: int
    lastActivityAt: float
    inFlight: int


class LeaseRecord(IdleState):
    root: str
    pid: int
    process: ProcessFingerprint
