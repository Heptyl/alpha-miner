"""Publish one closed, byte-verifiable market SQLite snapshot."""

from __future__ import annotations

import argparse
import os
import sqlite3
import tempfile
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path

from src.data.snapshot_manifest import (
    build_manifest,
    canonical_json,
    fsync_path,
    sidecar_path,
    utc_now,
)


def _temp(parent: Path, suffix: str) -> Path:
    descriptor, name = tempfile.mkstemp(dir=parent, suffix=suffix)
    os.close(descriptor)
    Path(name).unlink()
    return Path(name)


def publish_snapshot(
    source: Path,
    target: Path,
    *,
    clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    before_replace: Callable[[], None] | None = None,
) -> dict[str, object]:
    source, target = source.resolve(), target.resolve()
    if not source.is_file():
        raise FileNotFoundError(source)
    target.parent.mkdir(parents=True, exist_ok=True)
    staged, staged_sidecar = _temp(target.parent, ".publishing.db"), _temp(target.parent, ".publishing.json")
    try:
        source_db = sqlite3.connect(f"file:{source.as_posix()}?mode=ro", uri=True)
        target_db = sqlite3.connect(staged)
        try:
            source_db.backup(target_db)
            schema = (Path(__file__).parents[1] / "src/data/schema.sql").read_text(encoding="utf-8")
            target_db.executescript(schema)
            target_db.commit()
        finally:
            target_db.close()
            source_db.close()
        manifest = build_manifest(staged, published_at=utc_now(clock))
        staged_sidecar.write_text(canonical_json(manifest), encoding="utf-8")
        fsync_path(staged)
        fsync_path(staged_sidecar)
        if before_replace:
            before_replace()
        os.replace(staged, target)
        fsync_path(target)
        os.replace(staged_sidecar, sidecar_path(target))
        fsync_path(sidecar_path(target))
        return manifest
    finally:
        for path in (staged, staged_sidecar):
            if path.exists():
                path.unlink()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", default="data/alpha_miner.db")
    parser.add_argument("--target", required=True)
    args = parser.parse_args()
    manifest = publish_snapshot(Path(args.source), Path(args.target))
    print(f"Published market snapshot: {manifest['source_snapshot_sha256']}")


if __name__ == "__main__":
    main()
