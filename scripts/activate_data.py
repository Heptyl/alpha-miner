"""Activate a verified market snapshot without opening the research ledger."""

from __future__ import annotations

import os
import shutil
import sqlite3
import tempfile
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path

from src.data.snapshot_manifest import (
    build_manifest,
    canonical_json,
    fsync_path,
    inspect_database,
    read_manifest,
    sha256_file,
    sidecar_path,
    utc_now,
    validate_pair,
)

FORBIDDEN_TABLES = {"dataset_snapshots", "dataset_activations", "research_candidates", "research_evidence"}


def _temp(parent: Path, suffix: str) -> Path:
    descriptor, name = tempfile.mkstemp(dir=parent, suffix=suffix)
    os.close(descriptor)
    return Path(name)


def _assert_market_only(path: Path) -> None:
    connection = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True)
    try:
        tables = {str(row[0]) for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    finally:
        connection.close()
    if tables & FORBIDDEN_TABLES:
        raise ValueError("market database contains research ledger tables")


def _consume(incoming: Path) -> None:
    for path in (incoming, sidecar_path(incoming)):
        try:
            path.unlink()
        except FileNotFoundError:
            pass


def _same_identity(left: dict[str, object], right: dict[str, object]) -> bool:
    keys = ("source_snapshot_sha256", "size_bytes", "latest_trade_date")
    return all(left[key] == right[key] for key in keys)


def _remaining_identity(incoming: Path) -> dict[str, object]:
    if incoming.exists():
        _assert_market_only(incoming)
        check, latest = inspect_database(incoming)
        if check != "ok":
            raise ValueError("incomplete incoming database failed quick_check")
        return {
            "source_snapshot_sha256": sha256_file(incoming),
            "size_bytes": incoming.stat().st_size,
            "latest_trade_date": latest,
        }
    return read_manifest(sidecar_path(incoming))


def activate_snapshot(
    incoming: Path,
    destination: Path,
    previous: Path,
    *,
    clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    before_replace: Callable[[], None] | None = None,
) -> dict[str, object]:
    incoming, destination, previous = incoming.resolve(), destination.resolve(), previous.resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination_sidecar = sidecar_path(destination)

    # A valid active pair with the same market identity means both replaces
    # already completed.  Retry only the idempotent incoming cleanup; previous
    # must retain the old active identity.
    if destination.exists() and destination_sidecar.exists():
        try:
            active_manifest = validate_pair(destination, destination_sidecar)
        except Exception:
            active_manifest = None
        if active_manifest is not None:
            incoming_parts = (incoming.exists(), sidecar_path(incoming).exists())
            if any(incoming_parts):
                if all(incoming_parts):
                    manifest = validate_pair(incoming, sidecar_path(incoming))
                    _assert_market_only(incoming)
                else:
                    manifest = _remaining_identity(incoming)
                if _same_identity(active_manifest, manifest):
                    _consume(incoming)
                    return active_manifest
                if not all(incoming_parts):
                    raise ValueError("incomplete incoming does not match active database")

    manifest = validate_pair(incoming, sidecar_path(incoming))
    _assert_market_only(incoming)

    # Recovery state: DB replace completed but sidecar replace did not.  Only the
    # exact incoming byte identity may finish that interrupted transition.
    if destination.exists() and destination_sidecar.exists():
        try:
            validate_pair(destination, destination_sidecar)
        except Exception:
            if sha256_file(destination) != manifest["source_snapshot_sha256"]:
                raise ValueError("unknown active database/manifest mixed state")
            shutil.copy2(sidecar_path(incoming), destination_sidecar)
            fsync_path(destination_sidecar)
            validate_pair(destination, destination_sidecar)
            _consume(incoming)
            return manifest
    elif destination.exists():
        _assert_market_only(destination)
    elif destination_sidecar.exists():
        raise ValueError("active manifest exists without database")

    staged_db, staged_sidecar = _temp(destination.parent, ".activating.db"), _temp(destination.parent, ".activating.json")
    prior_manifest: dict[str, object] | None = None
    previous_db_temp: Path | None = None
    previous_sidecar_temp: Path | None = None
    try:
        shutil.copy2(incoming, staged_db)
        shutil.copy2(sidecar_path(incoming), staged_sidecar)
        if destination.exists():
            if destination_sidecar.exists():
                prior_manifest = validate_pair(destination, destination_sidecar)
            else:
                prior_manifest = build_manifest(destination, published_at=utc_now(clock))
            previous.parent.mkdir(parents=True, exist_ok=True)
            previous_db_temp = _temp(previous.parent, ".previous.db")
            previous_sidecar_temp = _temp(previous.parent, ".previous.json")
            shutil.copy2(destination, previous_db_temp)
            previous_sidecar_temp.write_text(canonical_json(prior_manifest), encoding="utf-8")
        if before_replace:
            before_replace()
        if previous_db_temp and previous_sidecar_temp:
            os.replace(previous_db_temp, previous)
            fsync_path(previous)
            os.replace(previous_sidecar_temp, sidecar_path(previous))
            fsync_path(sidecar_path(previous))
        os.replace(staged_db, destination)
        fsync_path(destination)
        os.replace(staged_sidecar, destination_sidecar)
        fsync_path(destination_sidecar)
        validate_pair(destination, destination_sidecar)
        _consume(incoming)
        return manifest
    finally:
        for path in (staged_db, staged_sidecar, previous_db_temp, previous_sidecar_temp):
            if path is not None and path.exists():
                path.unlink()


def main() -> None:
    root = Path.cwd().resolve()
    manifest = activate_snapshot(root / "incoming/alpha_miner.db", root / "data/alpha_miner.db", root / "data/alpha_miner.previous.db")
    print(f"Activated market snapshot: {manifest['source_snapshot_sha256']}")


if __name__ == "__main__":
    main()
