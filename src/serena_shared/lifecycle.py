"""Safe lifecycle management for one shared Serena server per Git checkout."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from importlib.resources import files
from pathlib import Path
from typing import Any, Callable, Generator, Mapping, NotRequired, Sequence, TypeGuard, TypedDict

PORT_START = 9121
PORT_END = 9199
LOCK_WAIT_SECONDS = 30.0
STARTUP_TIMEOUT_SECONDS = 15.0
TERMINATION_TIMEOUT_SECONDS = 5.0
POLL_INTERVAL_SECONDS = 0.2
WATCHDOG_POLL_SECONDS = 5.0


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


def hash_checkout_path(checkout: Path | str) -> str:
    return hashlib.sha256(os.fspath(checkout).encode()).hexdigest()


def endpoint_for_port(port: int) -> str:
    return f"http://127.0.0.1:{port}/mcp"


def paths_for_checkout(common_git_dir: Path | str, checkout: Path | str) -> StatePaths:
    common = Path(common_git_dir)
    key = hash_checkout_path(checkout)
    base = common / "serena-shared"
    return StatePaths(
        base_dir=base,
        key=key,
        startup_lock=base / f"{key}.startup.flock",
        port_lock=base / "ports.flock",
        log=base / f"{key}.log",
        record=base / f"{key}.json",
    )


def lease_paths(paths: StatePaths) -> list[Path]:
    return sorted(paths.base_dir.glob(f"{paths.key}.*.lease.json"))


def default_context_path() -> Path:
    return Path(str(files("serena_shared.resources").joinpath("serena.yml")))


def _git(cwd: Path, *args: str) -> str:
    completed = subprocess.run(
        ["git", *args], cwd=cwd, text=True, capture_output=True, check=False
    )
    if completed.returncode:
        raise RuntimeError("Shared Serena must run from a Git checkout.")
    return completed.stdout.strip()


def resolve_checkout(cwd: Path | str | None = None) -> Checkout:
    current = Path.cwd() if cwd is None else Path(cwd)
    root = Path(_git(current, "rev-parse", "--show-toplevel")).resolve()
    raw_common = Path(_git(current, "rev-parse", "--git-common-dir"))
    common = raw_common if raw_common.is_absolute() else current / raw_common
    return Checkout(root=root, common_git_dir=common.resolve())


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
    value = completed.stdout.strip()
    parts = value.split(maxsplit=6)
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
        os.kill(pid, 0)
        completed = subprocess.run(
            ["ps", "-p", str(pid), "-o", "stat="],
            text=True,
            capture_output=True,
            check=False,
        )
        state = completed.stdout.strip()
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


def is_registry_record_for_checkout(
    record: Any, checkout: Path | str
) -> TypeGuard[SerenaRecord]:
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
) -> SerenaRecord:
    return {
        "root": os.fspath(checkout),
        "pid": pid,
        "port": port,
        "endpoint": endpoint,
        "log": os.fspath(log),
        "process": process,
        "fingerprintAvailable": process is not None,
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
            [
                executable,
                "start-mcp-server",
                "--transport", "streamable-http",
                "--host", "127.0.0.1",
                "--port", str(port),
                "--project", os.fspath(checkout),
                "--context", os.fspath(context),
                "--mode", "one-shot",
                "--mode", "no-memories",
                "--enable-web-dashboard", "true",
                "--open-web-dashboard", "false",
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


def contain_spawned_serena(
    *, checkout: Path, endpoint: str, log: Path, paths: StatePaths, pid: int,
    port: int, process: ProcessFingerprint | None, record_path: Path, state: str
) -> tuple[bool, bool]:
    record = create_serena_record(checkout, endpoint, log, pid, port, process, state)
    if process and terminate_verified_serena(record):
        return True, False
    if is_process_alive(pid):
        write_json_atomically(record_path, record)
        return False, True
    return True, False


def start_shared_serena(
    checkout: Path,
    common_git_dir: Path,
    probe: Callable[[str], bool],
    context: Path | None = None,
) -> SerenaRecord:
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
                    candidate: SerenaRecord = (
                        existing if existing.get("process") else {**existing, "process": actual}
                    )
                    if ((actual and is_serena_fingerprint(actual, candidate)) or actual is None) and probe(existing["endpoint"]):
                        active: SerenaRecord = {
                            **existing,
                            "process": actual or existing.get("process"),
                            "state": "active",
                        }
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
                        initial_record = create_serena_record(
                            checkout, endpoint, paths.log, pid, port, initial, "pending"
                        )
                        if is_serena_fingerprint(initial, initial_record):
                            fingerprint = initial
                    deadline = time.monotonic() + STARTUP_TIMEOUT_SECONDS
                    while time.monotonic() < deadline:
                        if not is_process_alive(pid):
                            break
                        current = process_fingerprint(pid)
                        if current:
                            candidate = create_serena_record(
                                checkout, endpoint, paths.log, pid, port, current, "pending"
                            )
                            if is_serena_fingerprint(current, candidate):
                                fingerprint = current
                            elif fingerprint:
                                break
                        if probe(endpoint):
                            if not fingerprint:
                                raise RuntimeError("Shared Serena process fingerprint could not be verified.")
                            record = create_serena_record(
                                checkout, endpoint, paths.log, pid, port, fingerprint, "active"
                            )
                            write_json_atomically(paths.record, record)
                            return record
                        time.sleep(POLL_INTERVAL_SECONDS)
                    if not is_process_alive(pid):
                        continue
                    raise RuntimeError(
                        f"Shared Serena startup could not be verified for {checkout}; leaving PID {pid} pending."
                    )
                except Exception as error:
                    if pid is None:
                        raise
                    contained, persisted = contain_spawned_serena(
                        checkout=checkout,
                        endpoint=endpoint,
                        log=paths.log,
                        paths=paths,
                        pid=pid,
                        port=port,
                        process=fingerprint,
                        record_path=paths.record,
                        state="pending",
                    )
                    if persisted:
                        raise RuntimeError(
                            f"Shared Serena startup could not be verified for {checkout}; leaving PID {pid} pending."
                        ) from error
                    if contained:
                        raise RuntimeError(f"Shared Serena startup was contained after failure for {checkout}.") from error
                    raise
            raise RuntimeError(f"Shared Serena did not become healthy. See {paths.log}.")


def start_watchdog(checkout: Checkout, paths: StatePaths) -> WatchdogRecord:
    child = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "serena_shared.cli",
            "__watchdog",
            os.fspath(checkout.root),
            os.fspath(checkout.common_git_dir),
        ],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
        close_fds=True,
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


def ensure_watchdog(checkout: Checkout, paths: StatePaths) -> SerenaRecord:
    with file_lock(paths.startup_lock):
        record = read_json(paths.record)
        if not is_registry_record_for_checkout(record, checkout.root):
            raise RuntimeError(f"No verified shared Serena server exists for {checkout.root}.")
        if is_live_watchdog(record.get("watchdog")):
            return record
        watchdog = start_watchdog(checkout, paths)
        updated: SerenaRecord = {**record, "watchdog": watchdog}
        write_json_atomically(paths.record, updated)
        return updated


def ensure_backend(
    checkout: Checkout, paths: StatePaths, probe: Callable[[str], bool]
) -> SerenaRecord:
    record = start_shared_serena(
        checkout.root, checkout.common_git_dir, probe, default_context_path()
    )
    return ensure_watchdog(checkout, paths) if not is_live_watchdog(record.get("watchdog")) else record


def backend_identity(paths: StatePaths, checkout: Path) -> tuple[str, int] | None:
    record = read_json(paths.record)
    if not is_registry_record_for_checkout(record, checkout):
        return None
    if not is_process_alive(record["pid"]) or not has_matching_record_fingerprint(record):
        return None
    return record["endpoint"], record["pid"]


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
            write_json_atomically(
                self.path,
                {
                    **record,
                    "inFlight": in_flight,
                    "lastActivityAt": time.time(),
                },
            )

    def request_started(self) -> None:
        self._update(started=True)

    def request_finished(self) -> None:
        self._update(finished=True)

    def close(self) -> None:
        with file_lock(self.paths.startup_lock):
            self.path.unlink(missing_ok=True)


def create_proxy_lease(
    idle_timeout_minutes: int, cwd: Path | str | None = None
) -> ProxyLease:
    if idle_timeout_minutes <= 0:
        raise ValueError("Idle timeout minutes must be a positive integer.")
    checkout = resolve_checkout(cwd)
    paths = paths_for_checkout(checkout.common_git_dir, checkout.root)
    fingerprint = process_fingerprint(os.getpid())
    if fingerprint is None:
        raise RuntimeError("Shared Serena proxy fingerprint could not be verified.")
    path = paths.lease(uuid.uuid4().hex)
    with file_lock(paths.startup_lock):
        write_json_atomically(
            path,
            {
                "root": os.fspath(checkout.root),
                "pid": os.getpid(),
                "process": fingerprint,
                "idleTimeoutMinutes": idle_timeout_minutes,
                "lastActivityAt": time.time(),
                "inFlight": 0,
            },
        )
    return ProxyLease(checkout, paths, path)


def inspect_leases(
    paths: StatePaths, checkout: Path, *, remove_stale: bool = True
) -> tuple[list[LeaseRecord], int]:
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


def run_watchdog(
    checkout_root: Path | str,
    common_git_dir: Path | str,
    *,
    poll_seconds: float = WATCHDOG_POLL_SECONDS,
) -> None:
    checkout = Checkout(Path(checkout_root), Path(common_git_dir))
    paths = paths_for_checkout(checkout.common_git_dir, checkout.root)
    while True:
        with file_lock(paths.startup_lock):
            record = read_json(paths.record)
            if not is_registry_record_for_checkout(record, checkout.root):
                return
            if not is_process_alive(record["pid"]):
                paths.record.unlink(missing_ok=True)
                return
            leases, _ = inspect_leases(paths, checkout.root)
            now = time.time()
            root_exists = checkout.root.exists()
            idle = idle_shutdown_due(leases, now)
            if (not root_exists or idle) and terminate_verified_serena(record):
                paths.record.unlink(missing_ok=True)
                return
        time.sleep(poll_seconds)


def status(probe: Callable[[str], bool], cwd: Path | str | None = None) -> tuple[bool, dict[str, Any]]:
    checkout = resolve_checkout(cwd)
    record = read_json(paths_for_checkout(checkout.common_git_dir, checkout.root).record)
    healthy = False
    if is_registry_record_for_checkout(record, checkout.root):
        healthy = (
            is_process_alive(record["pid"])
            and has_matching_record_fingerprint(record)
            and probe(record["endpoint"])
        )
    paths = paths_for_checkout(checkout.common_git_dir, checkout.root)
    leases, _ = inspect_leases(paths, checkout.root, remove_stale=False)
    now = time.time()
    next_shutdown = (
        None
        if any(lease["inFlight"] for lease in leases)
        else max(
            (
                lease["lastActivityAt"] + lease["idleTimeoutMinutes"] * 60
                for lease in leases
            ),
            default=None,
        )
    )
    activity = {
        "liveProxies": len(leases),
        "inFlight": sum(lease["inFlight"] for lease in leases),
        "lastActivityAt": max((lease["lastActivityAt"] for lease in leases), default=None),
        "nextShutdownAt": next_shutdown if next_shutdown is None or next_shutdown > now else now,
        "watchdogHealthy": bool(record and is_live_watchdog(record.get("watchdog"))),
    }
    return healthy, {
        "checkout": os.fspath(checkout.root),
        "healthy": healthy,
        "record": record,
        "activity": activity,
    }
