"""Read-only Git and file-state checks used around every agent run."""

from __future__ import annotations

import hashlib
import subprocess
from dataclasses import asdict, dataclass
from pathlib import Path, PurePosixPath


class GitGuardError(RuntimeError):
    """Raised when an agent violates a Git or worktree invariant."""


@dataclass(frozen=True)
class GitSnapshot:
    head: str
    status: dict[str, str]
    dirty_paths: tuple[str, ...]
    file_hashes: dict[str, str | None]

    def persisted(self) -> dict[str, object]:
        """Return a JSON-safe snapshot without absolute paths or file contents."""
        return asdict(self)


def capture_snapshot(root: Path) -> GitSnapshot:
    """Capture HEAD, porcelain status, and hashes for Git-visible worktree files."""
    head = _git(root, "rev-parse", "HEAD").strip()
    status = _parse_status(_git_bytes(root, "status", "--porcelain=v1", "-z", "-uall"))
    paths = _nul_paths(
        _git_bytes(root, "ls-files", "-z", "--cached", "--others", "--exclude-standard")
    )
    file_hashes = {path: _hash_path(root / Path(path)) for path in sorted(set(paths))}
    return GitSnapshot(
        head=head,
        status=status,
        dirty_paths=tuple(sorted(status)),
        file_hashes=file_hashes,
    )


def changed_paths(before: GitSnapshot, after: GitSnapshot) -> set[str]:
    """Return content, presence, or index/worktree-status changes between snapshots."""
    paths = set(before.file_hashes) | set(after.file_hashes) | set(before.status) | set(after.status)
    return {
        path
        for path in paths
        if before.file_hashes.get(path) != after.file_hashes.get(path)
        or before.status.get(path) != after.status.get(path)
    }


def assert_same_head(before: GitSnapshot, after: GitSnapshot) -> None:
    if before.head != after.head:
        raise GitGuardError("Git HEAD changed during an agent run")


def assert_paths_allowed(paths: set[str], allowed_patterns: tuple[str, ...]) -> None:
    outside = sorted(path for path in paths if not path_is_allowed(path, allowed_patterns))
    if outside:
        raise GitGuardError(f"changes exceed TASK.allowed_paths: {', '.join(outside)}")


def assert_protected_dirty_preserved(
    initial: GitSnapshot,
    current: GitSnapshot,
    allowed_patterns: tuple[str, ...],
) -> None:
    """Preserve pre-existing dirty files that are outside the accepted task scope."""
    protected = {
        path for path in initial.dirty_paths if not path_is_allowed(path, allowed_patterns)
    }
    missing_or_changed = sorted(
        path
        for path in protected
        if current.file_hashes.get(path) != initial.file_hashes.get(path)
        or current.status.get(path) != initial.status.get(path)
    )
    if missing_or_changed:
        raise GitGuardError(
            "pre-existing dirty worktree files changed or disappeared: "
            + ", ".join(missing_or_changed)
        )


def normalize_task_path(value: str) -> str:
    """Normalize and validate a task-controlled repository path or glob."""
    normalized = value.replace("\\", "/").strip()
    if not normalized or normalized.startswith(("/", "~")):
        raise GitGuardError(f"allowed path must be repository-relative: {value!r}")
    if len(normalized) >= 2 and normalized[1] == ":":
        raise GitGuardError(f"allowed path must be repository-relative: {value!r}")
    parts = normalized.split("/")
    if any(part in {"", ".", ".."} for part in parts):
        raise GitGuardError(f"allowed path contains an unsafe segment: {value!r}")
    if parts[0] in {".git", ".brain"}:
        raise GitGuardError(f"allowed path targets controller or Git internals: {value!r}")
    return normalized


def path_is_allowed(path: str, allowed_patterns: tuple[str, ...]) -> bool:
    normalized = path.replace("\\", "/")
    for pattern in allowed_patterns:
        if pattern.endswith("/**"):
            prefix = pattern[:-3].rstrip("/")
            if normalized == prefix or normalized.startswith(prefix + "/"):
                return True
        elif normalized == pattern:
            return True
        elif "*" in pattern or "?" in pattern or "[" in pattern:
            if PurePosixPath(normalized).match(pattern):
                return True
    return False


def _git(root: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=root,
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    if result.returncode != 0:
        raise GitGuardError(f"git {' '.join(args)} failed with exit code {result.returncode}")
    return result.stdout


def _git_bytes(root: Path, *args: str) -> bytes:
    result = subprocess.run(
        ["git", *args],
        cwd=root,
        check=False,
        capture_output=True,
    )
    if result.returncode != 0:
        raise GitGuardError(f"git {' '.join(args)} failed with exit code {result.returncode}")
    return result.stdout


def _nul_paths(raw: bytes) -> list[str]:
    return [item.decode("utf-8", errors="surrogateescape") for item in raw.split(b"\0") if item]


def _parse_status(raw: bytes) -> dict[str, str]:
    entries = raw.split(b"\0")
    status: dict[str, str] = {}
    index = 0
    while index < len(entries):
        entry = entries[index]
        index += 1
        if not entry:
            continue
        decoded = entry.decode("utf-8", errors="surrogateescape")
        code = decoded[:2]
        path = decoded[3:]
        status[path] = code
        if code[0] in {"R", "C"} and index < len(entries) and entries[index]:
            source = entries[index].decode("utf-8", errors="surrogateescape")
            index += 1
            status[source] = f"{code}:source"
    return status


def _hash_path(path: Path) -> str | None:
    if not path.is_file():
        return None
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
