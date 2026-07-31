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
from importlib.resources import files
from pathlib import Path
from typing import Any, Callable, Generator, Mapping, TypeGuard

from .models import (
    Checkout,
    ProcessFingerprint,
    SerenaRecord,
    StatePaths,
    WatchdogRecord,
)

PORT_START = 9121
PORT_END = 9199
LOCK_WAIT_SECONDS = 30.0
STARTUP_TIMEOUT_SECONDS = 15.0
TERMINATION_TIMEOUT_SECONDS = 5.0
POLL_INTERVAL_SECONDS = 0.2


def endpoint_for_port(port: int) -> str:
    return f"http://127.0.0.1:{port}/mcp"


def default_context_path() -> Path:
    return Path(str(files("serena_shared.resources").joinpath("serena.yml")))


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


def is_json_object(value: Any) -> TypeGuard[dict[str, Any]]:
    return isinstance(value, dict)


def is_same_process_fingerprint(actual: Any, expected: Any) -> bool:
    return bool(
        is_json_object(actual)
        and is_json_object(expected)
        and actual.get("pid") == expected.get("pid")
        and actual.get("startedAt") == expected.get("startedAt")
        and actual.get("command") == expected.get("command")
    )


def is_registry_record_for_checkout(record: Any, checkout: Path | str) -> TypeGuard[SerenaRecord]:
    return bool(
        is_json_object(record)
        and record.get("root") == os.fspath(checkout)
        and isinstance(record.get("pid"), int)
        and isinstance(record.get("port"), int)
        and PORT_START <= record["port"] <= PORT_END
        and record.get("endpoint") == endpoint_for_port(record["port"])
    )


def is_serena_fingerprint(fingerprint: Any, record: Any) -> bool:
    if not is_json_object(record) or not is_same_process_fingerprint(
        fingerprint, record.get("process")
    ):
        return False
    if not is_json_object(fingerprint) or not isinstance(fingerprint.get("command"), str):
        return False
    command = fingerprint["command"]
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


def create_serena_record(checkout: Path, endpoint: str, log: Path, pid: int, port: int, process: ProcessFingerprint | None, state: str) -> SerenaRecord:
    return {
        "root": os.fspath(checkout), "pid": pid, "port": port, "endpoint": endpoint,
        "log": os.fspath(log), "process": process, "fingerprintAvailable": process is not None,
        "state": state,
    }


def start_serena(checkout: Path, port: int, context: Path, log: Path) -> int:
    executable = shutil.which("serena")
    if executable is None:
        raise RuntimeError("Serena is not installed or is not available on PATH.")
    log.parent.mkdir(parents=True, exist_ok=True)
    stream = log.open("a")
    try:
        child = subprocess.Popen(
            [executable, "start-mcp-server", "--transport", "streamable-http", "--host", "127.0.0.1",
             "--port", str(port), "--project", os.fspath(checkout), "--context", os.fspath(context),
             "--mode", "one-shot", "--mode", "no-memories", "--enable-web-dashboard", "true",
             "--open-web-dashboard", "false"],
            stdin=subprocess.DEVNULL, stdout=stream, stderr=subprocess.STDOUT,
            start_new_session=True, close_fds=True,
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


def start_shared_serena(checkout: Path, common_git_dir: Path, probe: Callable[[str], bool], context: Path | None = None) -> SerenaRecord:
    from .lifecycle import paths_for_checkout

    paths = paths_for_checkout(common_git_dir, checkout)
    context = default_context_path() if context is None else context
    with file_lock(paths.startup_lock):
        existing = read_json(paths.record)
        if existing:
            if not is_registry_record_for_checkout(existing, checkout):
                existing_pid = existing.get("pid")
                if isinstance(existing_pid, int) and is_process_alive(existing_pid):
                    raise RuntimeError(f"Refusing to replace a live mismatched shared Serena record for {checkout}.")
                paths.record.unlink(missing_ok=True)
            elif is_process_alive(existing["pid"]):
                if existing.get("state") == "pending":
                    actual = process_fingerprint(existing["pid"])
                    candidate: SerenaRecord = existing if existing.get("process") else {**existing, "process": actual}
                    if ((actual and is_serena_fingerprint(actual, candidate)) or actual is None) and probe(existing["endpoint"]):
                        active: SerenaRecord = {**existing, "process": actual or existing.get("process"), "state": "active"}
                        write_json_atomically(paths.record, active)
                        return active
                    raise RuntimeError(f"Shared Serena startup remains pending verification for {checkout}.")
                if not has_matching_record_fingerprint(existing):
                    raise RuntimeError(f"Refusing to replace an unverified live shared Serena process for {checkout}.")
                if probe(existing["endpoint"]):
                    return existing
                if not terminate_verified_serena(existing):
                    raise RuntimeError(f"Refusing to replace an unverified live shared Serena process for {checkout}.")
                paths.record.unlink(missing_ok=True)
            else:
                paths.record.unlink(missing_ok=True)

        with file_lock(paths.port_lock):
            attempted: set[int] = set()
            for _ in range(PORT_END - PORT_START + 1):
                port = find_available_port(attempted)
                attempted.add(port)
                endpoint = endpoint_for_port(port)
                pid: int | None = None
                fingerprint: ProcessFingerprint | None = None
                try:
                    pid = start_serena(checkout, port, context, paths.log)
                    initial = process_fingerprint(pid)
                    if initial:
                        initial_record = create_serena_record(checkout, endpoint, paths.log, pid, port, initial, "pending")
                        if is_serena_fingerprint(initial, initial_record):
                            fingerprint = initial
                    deadline = time.monotonic() + STARTUP_TIMEOUT_SECONDS
                    while time.monotonic() < deadline:
                        if not is_process_alive(pid):
                            break
                        current = process_fingerprint(pid)
                        if current:
                            candidate = create_serena_record(checkout, endpoint, paths.log, pid, port, current, "pending")
                            if is_serena_fingerprint(current, candidate):
                                fingerprint = current
                            elif fingerprint:
                                break
                        if probe(endpoint):
                            if not fingerprint:
                                raise RuntimeError("Shared Serena process fingerprint could not be verified.")
                            record = create_serena_record(checkout, endpoint, paths.log, pid, port, fingerprint, "active")
                            write_json_atomically(paths.record, record)
                            return record
                        time.sleep(POLL_INTERVAL_SECONDS)
                    if not is_process_alive(pid):
                        continue
                    raise RuntimeError(f"Shared Serena startup could not be verified for {checkout}; leaving PID {pid} pending.")
                except Exception as error:
                    if pid is None:
                        raise
                    record = create_serena_record(checkout, endpoint, paths.log, pid, port, fingerprint, "pending")
                    if fingerprint and terminate_verified_serena(record):
                        raise RuntimeError(f"Shared Serena startup was contained after failure for {checkout}.") from error
                    if is_process_alive(pid):
                        write_json_atomically(paths.record, record)
                        raise RuntimeError(f"Shared Serena startup could not be verified for {checkout}; leaving PID {pid} pending.") from error
                    raise
            raise RuntimeError(f"Shared Serena did not become healthy. See {paths.log}.")


def start_watchdog(checkout: Checkout, paths: StatePaths) -> WatchdogRecord:
    child = subprocess.Popen(
        [sys.executable, "-m", "serena_shared.cli", "__watchdog", os.fspath(checkout.root), os.fspath(checkout.common_git_dir)],
        stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        start_new_session=True, close_fds=True,
    )
    fingerprint = process_fingerprint(child.pid)
    if fingerprint is None:
        raise RuntimeError("Shared Serena watchdog fingerprint could not be verified.")
    return {"pid": child.pid, "process": fingerprint}


def is_live_watchdog(value: Any) -> bool:
    if not is_json_object(value) or not isinstance(value.get("pid"), int):
        return False
    actual = process_fingerprint(value["pid"])
    return actual is not None and is_same_process_fingerprint(actual, value.get("process"))
