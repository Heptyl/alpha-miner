"""Canonical identity contract for a closed market SQLite snapshot."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from collections.abc import Callable, Mapping
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

KEYS = frozenset({"schema_version", "source_snapshot_sha256", "size_bytes", "latest_trade_date", "published_at"})


def canonical_json(value: Mapping[str, Any]) -> str:
    return json.dumps(dict(value), ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def sidecar_path(database: Path) -> Path:
    return database.with_name(f"{database.name}.manifest.json")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def utc_now(clock: Callable[[], datetime]) -> str:
    value = clock()
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError("clock must return an aware datetime")
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds")


def inspect_database(path: Path) -> tuple[str, str]:
    connection = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True)
    try:
        check = str(connection.execute("PRAGMA quick_check").fetchone()[0])
        row = connection.execute("SELECT MAX(trade_date) FROM daily_price").fetchone()
    finally:
        connection.close()
    latest = row[0] if row else None
    if not isinstance(latest, str):
        raise ValueError("daily_price has no latest trade date")
    date.fromisoformat(latest)
    return check, latest


def build_manifest(path: Path, *, published_at: str) -> dict[str, Any]:
    check, latest = inspect_database(path)
    if check != "ok":
        raise RuntimeError(f"SQLite quick_check failed: {check}")
    value = {"schema_version": 1, "source_snapshot_sha256": sha256_file(path), "size_bytes": path.stat().st_size, "latest_trade_date": latest, "published_at": published_at}
    return validate_manifest(value)


def validate_manifest(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != KEYS or value["schema_version"] != 1:
        raise ValueError("manifest keys/schema do not match")
    digest = value["source_snapshot_sha256"]
    if not isinstance(digest, str) or len(digest) != 64 or set(digest) - set("0123456789abcdef"):
        raise ValueError("source_snapshot_sha256 must be lowercase SHA256")
    if isinstance(value["size_bytes"], bool) or not isinstance(value["size_bytes"], int) or value["size_bytes"] <= 0:
        raise ValueError("size_bytes must be positive")
    date.fromisoformat(value["latest_trade_date"])
    parsed = datetime.fromisoformat(value["published_at"].replace("Z", "+00:00"))
    if parsed.tzinfo is None or value["published_at"] != parsed.astimezone(timezone.utc).isoformat(timespec="microseconds"):
        raise ValueError("published_at must be canonical UTC")
    return dict(value)


def read_manifest(path: Path) -> dict[str, Any]:
    text = path.read_text(encoding="utf-8")
    value = validate_manifest(json.loads(text))
    if text != canonical_json(value):
        raise ValueError("manifest is not canonical JSON")
    return value


def validate_pair(database: Path, sidecar: Path) -> dict[str, Any]:
    if not database.is_file() or not sidecar.is_file():
        raise FileNotFoundError("database and manifest are both required")
    value = read_manifest(sidecar)
    check, latest = inspect_database(database)
    if check != "ok" or sha256_file(database) != value["source_snapshot_sha256"] or database.stat().st_size != value["size_bytes"] or latest != value["latest_trade_date"]:
        raise ValueError("database does not match manifest")
    return value


def fsync_path(path: Path) -> None:
    try:
        with path.open("r+b") as stream:
            stream.flush()
            os.fsync(stream.fileno())
        if os.name != "nt" and hasattr(os, "O_DIRECTORY"):
            descriptor = os.open(str(path.parent), os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
    except OSError as exc:
        raise RuntimeError(f"fsync failed for {path.name}") from exc
