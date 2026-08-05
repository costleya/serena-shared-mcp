"""Process ownership, state persistence, and Serena server management."""

from __future__ import annotations

import fcntl
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Callable, Generator, Mapping, Sequence, TypeGuard, cast

from serena_shared._typing import is_json_object

from .models import (
    Checkout,
    ProcessFingerprint,
    RuntimeProfile,
    SerenaRecord,
    StatePaths,
    WatchdogRecord,
    is_serena_record,
)

PORT_START = 9121
PORT_END = 9199
LOCK_WAIT_SECONDS = 30.0
STARTUP_TIMEOUT_SECONDS = 15.0
TERMINATION_TIMEOUT_SECONDS = 5.0
POLL_INTERVAL_SECONDS = 0.2


def endpoint_for_port(port: int) -> str:
    return f"http://127.0.0.1:{port}/mcp"


def read_json(path: Path) -> dict[str, Any] | None:
    try:
        return json.loads(path.read_text())
    except FileNotFoundError:
        return None


def write_json_atomically(path: Path, value: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary_path = Path(temporary)
    try:
        with os.fdopen(descriptor, "w") as stream:
            json.dump(value, stream, indent=2)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary_path, path)
    finally:
        temporary_path.unlink(missing_ok=True)


@contextmanager
def file_lock(path: Path, timeout: float = LOCK_WAIT_SECONDS) -> Generator[None, None, None]:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)
    deadline = time.monotonic() + timeout
    try:
        while True:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    raise TimeoutError(f"Timed out waiting for shared Serena lock: {path}")
                time.sleep(POLL_INTERVAL_SECONDS)
        yield
    finally:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


def process_fingerprint(pid: int) -> ProcessFingerprint | None:
    completed = subprocess.run(
        ["ps", "-ww", "-p", str(pid), "-o", "pid=", "-o", "lstart=", "-o", "command="],
        text=True,
        capture_output=True,
        check=False,
    )
    if completed.returncode or not completed.stdout.strip():
        return None
    parts = completed.stdout.strip().split(maxsplit=6)
    if len(parts) != 7 or not parts[0].isdigit():
        return None
    return {"pid": int(parts[0]), "startedAt": " ".join(parts[1:6]), "command": parts[6]}


def is_process_alive(pid: int) -> bool:
    try:
        reaped, _ = os.waitpid(pid, os.WNOHANG)
        if reaped == pid:
            return False
    except ChildProcessError:
        pass
    try:
        # Signal 0 only checks whether the process exists; it does not kill it.
        os.kill(pid, 0)
        completed = subprocess.run(
            ["ps", "-p", str(pid), "-o", "stat="],
            text=True,
            capture_output=True,
            check=False,
        )
        state = completed.stdout.strip()
        # On POSIX, "Z" is the ps state for a zombie process: it exists but is no longer running.
        return not state.startswith("Z") if completed.returncode == 0 and state else True
    except PermissionError:
        return True
    except (ProcessLookupError, ValueError, TypeError):
        return False


def is_same_process_fingerprint(actual: Any, expected: Any) -> bool:
    return bool(
        is_json_object(actual)
        and is_json_object(expected)
        and actual.get("pid") == expected.get("pid")
        and actual.get("startedAt") == expected.get("startedAt")
        and actual.get("command") == expected.get("command")
    )


def is_registry_record_for_profile_digest(
    record: object,
    checkout: Path | str,
    profile_digest: str | None = None,
) -> TypeGuard[SerenaRecord]:
    if not is_serena_record(record):
        return False
    port = record["port"]
    return (
        record["root"] == os.fspath(checkout)
        and PORT_START <= port <= PORT_END
        and record["endpoint"] == endpoint_for_port(port)
        and (
            profile_digest is None
            or record["profile"]["digest"] == profile_digest
        )
    )


def is_serena_fingerprint(fingerprint: Any, record: Any) -> bool:
    if not is_json_object(record) or not is_same_process_fingerprint(
        fingerprint, record.get("process")
    ):
        return False
    if not is_json_object(fingerprint):
        return False
    command = fingerprint.get("command")
    if not isinstance(command, str):
        return False
    return (
        "serena start-mcp-server" in command
        and f"--port {record.get('port')}" in command
        and f"--project {record.get('root')}" in command
    )


def has_matching_record_fingerprint(record: SerenaRecord) -> bool:
    actual = process_fingerprint(record["pid"])
    return actual is None or is_serena_fingerprint(actual, record)


def is_owned_serena(record: SerenaRecord) -> bool:
    return is_serena_fingerprint(process_fingerprint(record["pid"]), record)


def find_available_port(excluded: set[int] | None = None) -> int:
    excluded = excluded or set()
    for port in range(PORT_START, PORT_END + 1):
        if port in excluded:
            continue
        with socket.socket() as candidate:
            candidate.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                candidate.bind(("127.0.0.1", port))
            except OSError:
                continue
        return port
    raise RuntimeError(f"No shared Serena port is available in {PORT_START}-{PORT_END}.")


def create_serena_record(
    checkout: Path,
    endpoint: str,
    log: Path,
    pid: int,
    port: int,
    process: ProcessFingerprint | None,
    state: str,
    profile: RuntimeProfile,
    probe_state: str = "not-started",
) -> SerenaRecord:
    value: SerenaRecord = {
        "root": os.fspath(checkout), "pid": pid, "port": port, "endpoint": endpoint,
        "log": os.fspath(log), "process": process, "fingerprintAvailable": process is not None,
        "state": state, "profile": profile.as_record(), "probeState": probe_state,
    }
    return value


def pending_startup_diagnostic(record: SerenaRecord, probe_state: str) -> str:
    return (
        "Shared Serena startup remains pending "
        f"(PID {record["pid"]}, endpoint {record["endpoint"]}, "
        f"profile {record["profile"]["profileKey"]!r}, probe {probe_state})."
    )


def recover_pending_serena(
    record_path: Path,
    record: SerenaRecord,
    probe: Callable[[str], bool],
) -> SerenaRecord | None:
    """Wait for a pending launch and promote only a verified, probed backend."""
    deadline = time.monotonic() + STARTUP_TIMEOUT_SECONDS
    pending = record
    probe_state = pending.get("probeState", "not-started")
    while time.monotonic() < deadline:
        if not is_process_alive(pending["pid"]):
            return None
        fingerprint = process_fingerprint(pending["pid"])
        if fingerprint is None:
            probe_state = "fingerprint-unavailable"
        else:
            recorded_fingerprint = pending["process"]
            if recorded_fingerprint is not None and not is_same_process_fingerprint(
                fingerprint, recorded_fingerprint
            ):
                pending["probeState"] = "fingerprint-mismatch"
                write_json_atomically(record_path, pending)
                return None
            candidate = cast(
                SerenaRecord,
                {**pending, "process": fingerprint, "fingerprintAvailable": True},
            )
            if not is_serena_fingerprint(fingerprint, candidate):
                if recorded_fingerprint is not None:
                    pending["probeState"] = "fingerprint-mismatch"
                    write_json_atomically(record_path, pending)
                    return None
                probe_state = "fingerprint-unverified"
            else:
                if probe(candidate["endpoint"]):
                    active = cast(
                        SerenaRecord,
                        {**candidate, "state": "active", "probeState": "succeeded"},
                    )
                    write_json_atomically(record_path, active)
                    return active
                probe_state = "failed"
        pending = cast(
            SerenaRecord,
            {**pending, "state": "pending", "probeState": probe_state},
        )
        write_json_atomically(record_path, pending)
        time.sleep(POLL_INTERVAL_SECONDS)
    return None


def start_serena(
    checkout: Path,
    port: int,
    log: Path,
    serena_args: Sequence[str],
) -> int:
    executable = shutil.which("serena")
    if executable is None:
        raise RuntimeError("Serena is not installed or is not available on PATH.")
    log.parent.mkdir(parents=True, exist_ok=True)
    stream = log.open("a")
    try:
        child = subprocess.Popen(
            [
                executable,
                "start-mcp-server",
                "--transport",
                "streamable-http",
                "--host",
                "127.0.0.1",
                "--port",
                str(port),
                "--project",
                os.fspath(checkout),
                *serena_args,
            ],
            stdin=subprocess.DEVNULL,
            stdout=stream,
            stderr=subprocess.STDOUT,
            start_new_session=True,
            close_fds=True,
        )
        return child.pid
    finally:
        stream.close()


def terminate_verified_serena(record: SerenaRecord) -> bool:
    if not is_owned_serena(record):
        return False
    try:
        os.kill(record["pid"], signal.SIGTERM)
    except (ProcessLookupError, PermissionError):
        return False
    deadline = time.monotonic() + TERMINATION_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        if not is_process_alive(record["pid"]):
            return True
        if not is_same_process_fingerprint(process_fingerprint(record["pid"]), record["process"]):
            return False
        time.sleep(POLL_INTERVAL_SECONDS)
    return False


def is_registry_record_for_profile(record: Any, checkout: Path | str, profile: RuntimeProfile) -> TypeGuard[SerenaRecord]:
    return is_registry_record_for_profile_digest(record, checkout, profile.digest)


def start_shared_serena(
    checkout: Path,
    common_git_dir: Path,
    probe: Callable[[str], bool],
    profile: RuntimeProfile,
    serena_args: Sequence[str],
) -> SerenaRecord:
    from .lifecycle import paths_for_checkout

    paths = paths_for_checkout(common_git_dir, checkout, profile=profile)
    with file_lock(paths.startup_lock):
        existing = read_json(paths.record)
        if existing:
            if not is_registry_record_for_profile(existing, checkout, profile):
                paths.record.unlink(missing_ok=True)
            elif not is_process_alive(existing["pid"]):
                paths.record.unlink(missing_ok=True)
            elif existing.get("state") == "pending":
                recovered = recover_pending_serena(paths.record, existing, probe)
                if recovered is not None:
                    return recovered
                latest = read_json(paths.record)
                if is_registry_record_for_profile(latest, checkout, profile):
                    existing = latest
                if not is_process_alive(existing["pid"]):
                    paths.record.unlink(missing_ok=True)
                else:
                    probe_state = existing.get("probeState", "unverified")
                    raise RuntimeError(pending_startup_diagnostic(existing, probe_state))
            elif not is_owned_serena(existing):
                paths.record.unlink(missing_ok=True)
            elif probe(existing["endpoint"]):
                return existing
            else:
                if not terminate_verified_serena(existing):
                    raise RuntimeError(
                        "Refusing to replace an unverified live shared Serena process "
                        f"(PID {existing["pid"]}, endpoint {existing["endpoint"]}, "
                        f"profile {existing["profile"]["profileKey"]!r}, probe failed)."
                    )
                paths.record.unlink(missing_ok=True)

        with file_lock(paths.port_lock):
            attempted: set[int] = set()
            for _ in range(PORT_END - PORT_START + 1):
                port = find_available_port(attempted)
                attempted.add(port)
                endpoint = endpoint_for_port(port)
                pid = start_serena(checkout, port, paths.log, serena_args)
                pending = create_serena_record(
                    checkout, endpoint, paths.log, pid, port, None, "pending", profile,
                )
                # Persist before fingerprinting or probing so an interrupted cold start is recoverable.
                write_json_atomically(paths.record, pending)
                recovered = recover_pending_serena(paths.record, pending, probe)
                if recovered is not None:
                    return recovered
                latest = read_json(paths.record)
                if is_registry_record_for_profile(latest, checkout, profile):
                    pending = latest
                if not is_process_alive(pid):
                    paths.record.unlink(missing_ok=True)
                    continue
                probe_state = pending.get("probeState", "unverified")
                raise RuntimeError(pending_startup_diagnostic(pending, probe_state))
            raise RuntimeError(f"Shared Serena did not become healthy. See {paths.log}.")


def start_watchdog(checkout: Checkout, paths: StatePaths) -> WatchdogRecord:
    child = subprocess.Popen(
        [sys.executable, "-m", "serena_shared.cli", "__watchdog", os.fspath(checkout.root), os.fspath(checkout.common_git_dir), paths.key],
        stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        start_new_session=True, close_fds=True,
    )
    fingerprint = process_fingerprint(child.pid)
    if fingerprint is None:
        raise RuntimeError("Shared Serena watchdog fingerprint could not be verified.")
    return {"pid": child.pid, "process": fingerprint}


def is_live_watchdog(value: Any) -> bool:
    if not is_json_object(value):
        return False
    pid = value.get("pid")
    if not isinstance(pid, int):
        return False
    actual = process_fingerprint(pid)
    return actual is not None and is_same_process_fingerprint(actual, value.get("process"))
