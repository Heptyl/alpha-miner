"""Pure-market publication and activation contracts."""

from __future__ import annotations

import os
import shutil
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest

import scripts.activate_data as activation
from scripts.activate_data import FORBIDDEN_TABLES, ActivationConflict, activate_snapshot
from scripts.publish_data import publish_snapshot
from src.data.snapshot_manifest import sha256_file, sidecar_path, validate_pair
from src.data.storage import Storage

NOW = datetime(2026, 8, 18, 8, tzinfo=timezone.utc)


def _market(path: Path, day: str, close: float = 10) -> None:
    Storage(str(path)).init_db()
    connection = sqlite3.connect(path)
    connection.execute(
        "INSERT INTO daily_price(stock_code,trade_date,close) VALUES('000001',?,?)",
        (day, close),
    )
    connection.commit()
    connection.close()


def _pair(tmp_path: Path, name: str, day: str, close: float = 10) -> Path:
    source, target = tmp_path / f"{name}-source.db", tmp_path / f"{name}.db"
    _market(source, day, close)
    publish_snapshot(source, target, clock=lambda: NOW)
    return target


def _identity(path: Path) -> tuple[str, int, int]:
    return sha256_file(path), path.stat().st_size, path.stat().st_mtime_ns


def test_publish_pair_is_exact_and_market_only(tmp_path):
    database = _pair(tmp_path, "published", "2026-08-17")
    manifest = validate_pair(database, sidecar_path(database))
    connection = sqlite3.connect(database)
    tables = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    connection.close()
    assert manifest["source_snapshot_sha256"] == sha256_file(database)
    assert not (tables & FORBIDDEN_TABLES)


@pytest.mark.parametrize("target", ["db", "manifest", "missing"])
def test_incoming_tamper_or_missing_sidecar_is_rejected(tmp_path, target):
    incoming = _pair(tmp_path, target, "2026-08-17")
    if target == "db":
        incoming.write_bytes(incoming.read_bytes() + b"x")
    elif target == "manifest":
        sidecar_path(incoming).write_text("{}", encoding="utf-8")
    else:
        sidecar_path(incoming).unlink()
    with pytest.raises(Exception):
        activate_snapshot(incoming, tmp_path / "active.db", tmp_path / "previous.db")


def test_activation_preserves_previous_exact_pair(tmp_path):
    first = _pair(tmp_path, "first", "2026-08-15")
    active, previous = tmp_path / "active.db", tmp_path / "previous.db"
    activate_snapshot(first, active, previous, clock=lambda: NOW)
    before = _identity(active), _identity(sidecar_path(active))
    second = _pair(tmp_path, "second", "2026-08-18")
    activate_snapshot(second, active, previous, clock=lambda: NOW)
    assert _identity(previous) == before[0]
    assert _identity(sidecar_path(previous))[:2] == before[1][:2]
    validate_pair(active, sidecar_path(active))
    validate_pair(previous, sidecar_path(previous))


def test_before_replace_preserves_active_and_incoming_for_retry(tmp_path):
    first = _pair(tmp_path, "first", "2026-08-15")
    active, previous = tmp_path / "active.db", tmp_path / "previous.db"
    activate_snapshot(first, active, previous)
    second = _pair(tmp_path, "second", "2026-08-18")
    identities = _identity(active), _identity(sidecar_path(active)), _identity(second), _identity(sidecar_path(second))

    with pytest.raises(RuntimeError):
        activate_snapshot(
            second,
            active,
            previous,
            before_replace=lambda: (_ for _ in ()).throw(RuntimeError("fault")),
        )
    assert identities == (_identity(active), _identity(sidecar_path(active)), _identity(second), _identity(sidecar_path(second)))
    activate_snapshot(second, active, previous)
    validate_pair(active, sidecar_path(active))


def test_expected_hash_is_rechecked_immediately_before_replace(tmp_path):
    first = _pair(tmp_path, "expected-first", "2026-08-15")
    active, previous = tmp_path / "active.db", tmp_path / "previous.db"
    activate_snapshot(first, active, previous)
    expected = sha256_file(active)
    second = _pair(tmp_path, "expected-second", "2026-08-18", 11)
    external = _pair(tmp_path, "external", "2026-08-18", 12)

    def replace_active():
        shutil.copy2(external, active)
        shutil.copy2(sidecar_path(external), sidecar_path(active))

    with pytest.raises(ActivationConflict, match="before activation replace"):
        activate_snapshot(
            second, active, previous,
            expected_current_hash=expected, before_replace=replace_active,
        )
    assert sha256_file(active) == sha256_file(external)


def test_db_replaced_manifest_old_finishes_only_exact_incoming(tmp_path, monkeypatch):
    first = _pair(tmp_path, "first", "2026-08-17", 10)
    active, previous = tmp_path / "active.db", tmp_path / "previous.db"
    activate_snapshot(first, active, previous)
    second = _pair(tmp_path, "second", "2026-08-17", 11)
    original_replace = os.replace

    def fail_manifest(source, destination):
        if Path(destination) == sidecar_path(active):
            raise OSError("manifest replace fault")
        original_replace(source, destination)

    monkeypatch.setattr(os, "replace", fail_manifest)
    with pytest.raises(OSError):
        activate_snapshot(second, active, previous)
    monkeypatch.setattr(os, "replace", original_replace)
    activate_snapshot(second, active, previous)
    validate_pair(active, sidecar_path(active))


def test_cleanup_retry_does_not_rewrite_previous_pair(tmp_path, monkeypatch):
    first = _pair(tmp_path, "cleanup-first", "2026-08-15", 10)
    active, previous = tmp_path / "active.db", tmp_path / "previous.db"
    activate_snapshot(first, active, previous)
    second = _pair(tmp_path, "cleanup-second", "2026-08-18", 11)
    original_consume = activation._consume
    monkeypatch.setattr(
        activation,
        "_consume",
        lambda _: (_ for _ in ()).throw(RuntimeError("cleanup fault")),
    )
    with pytest.raises(RuntimeError, match="cleanup fault"):
        activate_snapshot(second, active, previous)
    previous_identity = _identity(previous), _identity(sidecar_path(previous))
    monkeypatch.setattr(activation, "_consume", original_consume)

    activate_snapshot(second, active, previous)

    validate_pair(active, sidecar_path(active))
    assert not second.exists() and not sidecar_path(second).exists()
    assert previous_identity == (_identity(previous), _identity(sidecar_path(previous)))


@pytest.mark.parametrize("remaining", ["database", "manifest"])
def test_cleanup_retry_accepts_one_matching_incoming_part(tmp_path, monkeypatch, remaining):
    first = _pair(tmp_path, f"partial-{remaining}-first", "2026-08-15", 10)
    active, previous = tmp_path / "active.db", tmp_path / "previous.db"
    activate_snapshot(first, active, previous)
    second = _pair(tmp_path, f"partial-{remaining}-second", "2026-08-18", 11)
    original_consume = activation._consume
    monkeypatch.setattr(
        activation,
        "_consume",
        lambda _: (_ for _ in ()).throw(RuntimeError("cleanup fault")),
    )
    with pytest.raises(RuntimeError, match="cleanup fault"):
        activate_snapshot(second, active, previous)
    monkeypatch.setattr(activation, "_consume", original_consume)
    (sidecar_path(second) if remaining == "database" else second).unlink()

    activate_snapshot(second, active, previous)

    assert not second.exists() and not sidecar_path(second).exists()


def test_incomplete_unrelated_incoming_fails_closed(tmp_path):
    first = _pair(tmp_path, "incomplete-first", "2026-08-15", 10)
    active, previous = tmp_path / "active.db", tmp_path / "previous.db"
    activate_snapshot(first, active, previous)
    active_identity = _identity(active), _identity(sidecar_path(active))
    unrelated = _pair(tmp_path, "incomplete-unrelated", "2026-08-18", 11)
    sidecar_path(unrelated).unlink()

    with pytest.raises(ValueError, match="incomplete incoming"):
        activate_snapshot(unrelated, active, previous)

    assert active_identity == (_identity(active), _identity(sidecar_path(active)))


def test_unknown_mixed_state_and_research_tables_fail_closed(tmp_path):
    first = _pair(tmp_path, "first", "2026-08-15")
    active, previous = tmp_path / "active.db", tmp_path / "previous.db"
    activate_snapshot(first, active, previous)
    sidecar_path(active).write_text("{}", encoding="utf-8")
    unrelated = _pair(tmp_path, "unrelated", "2026-08-18")
    with pytest.raises(ValueError, match="unknown"):
        activate_snapshot(unrelated, active, previous)

    bad = _pair(tmp_path, "bad", "2026-08-18")
    connection = sqlite3.connect(bad)
    connection.execute("CREATE TABLE research_evidence(x)")
    connection.commit()
    connection.close()
    # Refreshing the manifest would make the bytes trusted-looking; table policy still rejects.
    from src.data.snapshot_manifest import build_manifest, canonical_json

    manifest = build_manifest(bad, published_at=NOW.isoformat(timespec="microseconds"))
    sidecar_path(bad).write_text(canonical_json(manifest), encoding="utf-8")
    with pytest.raises(ValueError, match="research ledger"):
        activate_snapshot(bad, tmp_path / "other.db", tmp_path / "other.previous.db")


def test_same_day_and_clock_rollback_use_byte_hash(tmp_path):
    active, previous = tmp_path / "active.db", tmp_path / "previous.db"
    first = _pair(tmp_path, "same-a", "2026-08-17", 10)
    activate_snapshot(first, active, previous, clock=lambda: NOW)
    first_hash = sha256_file(active)
    second = _pair(tmp_path, "same-b", "2026-08-17", 11)
    activate_snapshot(second, active, previous, clock=lambda: datetime(2020, 1, 1, tzinfo=timezone.utc))
    assert sha256_file(active) != first_hash
    validate_pair(active, sidecar_path(active))
