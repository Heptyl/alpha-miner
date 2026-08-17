"""Atomic state persistence, redacted event logging, and the workspace run lock."""

from __future__ import annotations

import json
import os
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


class BrainLockedError(RuntimeError):
    """Raised when another live brain process owns the workspace lock."""


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"unable to read controller file {path.name}") from exc
    if not isinstance(value, dict):
        raise RuntimeError(f"controller file {path.name} is not a JSON object")
    return value


class EventLog:
    """Append fixed controller metadata; prompts and agent output are never accepted."""

    def __init__(self, path: Path) -> None:
        self.path = path

    def append(self, event: str, **metadata: str | int | bool | None) -> None:
        allowed = {"from_state", "to_state", "round", "role", "exit_code", "reason_code"}
        unexpected = set(metadata) - allowed
        if unexpected:
            raise ValueError(f"unsupported log metadata: {', '.join(sorted(unexpected))}")
        record = {"at": utc_now(), "event": event, **metadata}
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8", newline="\n") as stream:
            stream.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")


class RunLock:
    """Atomic per-workspace lock with conservative stale-process recovery."""

    def __init__(self, path: Path, run_id: str) -> None:
        self.path = path
        self.run_id = run_id
        self.token = uuid.uuid4().hex
        self._descriptor: int | None = None

    def acquire(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = json.dumps(
            {"pid": os.getpid(), "run_id": self.run_id, "token": self.token},
            sort_keys=True,
        ).encode("utf-8")
        for _attempt in range(2):
            try:
                descriptor = os.open(
                    self.path,
                    os.O_CREAT | os.O_EXCL | os.O_WRONLY,
                    0o600,
                )
            except FileExistsError:
                if not self._remove_stale_lock():
                    raise BrainLockedError("another brain run owns the workspace lock") from None
                continue
            os.write(descriptor, payload)
            os.fsync(descriptor)
            self._descriptor = descriptor
            return
        raise BrainLockedError("unable to acquire the workspace lock")

    def assert_owned(self) -> None:
        try:
            data = read_json(self.path)
        except RuntimeError as exc:
            raise BrainLockedError("workspace lock disappeared during the run") from exc
        if data.get("token") != self.token:
            raise BrainLockedError("workspace lock ownership changed during the run")

    def release(self) -> None:
        if self._descriptor is not None:
            os.close(self._descriptor)
            self._descriptor = None
        try:
            data = read_json(self.path)
        except RuntimeError:
            return
        if data.get("token") == self.token:
            self.path.unlink(missing_ok=True)

    def _remove_stale_lock(self) -> bool:
        try:
            data = read_json(self.path)
            pid = int(data["pid"])
        except (KeyError, TypeError, ValueError, RuntimeError):
            return False
        if _pid_is_alive(pid):
            return False
        try:
            self.path.unlink()
        except OSError:
            return False
        return True


def _pid_is_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    if os.name == "nt":
        import ctypes

        process_query_limited_information = 0x1000
        handle = ctypes.windll.kernel32.OpenProcess(  # type: ignore[attr-defined]
            process_query_limited_information,
            False,
            pid,
        )
        if handle:
            ctypes.windll.kernel32.CloseHandle(handle)  # type: ignore[attr-defined]
            return True
        error_access_denied = 5
        return ctypes.windll.kernel32.GetLastError() == error_access_denied  # type: ignore[attr-defined]
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True
