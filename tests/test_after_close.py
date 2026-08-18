"""Opt-in after-close publication and frozen projection tests."""

from __future__ import annotations

import json
import shutil
import sqlite3
from datetime import datetime
from pathlib import Path

import pytest
from click.testing import CliRunner

import cli.limit_up as limit_up_cli
import scripts.activate_data as activation
import src.mining.after_close as pipeline
from cli.limit_up import main
from scripts.activate_data import activate_snapshot as real_activate
from scripts.publish_data import publish_snapshot
from src.data.limit_up_history import CollectionCheck
from src.data.snapshot_manifest import sha256_file, sidecar_path, validate_pair
from src.data.storage import Storage
from src.data.watchlist_minutes import WatchlistCaptureResult
from src.mining.behavior_state import BehaviorStateSpec
from src.mining.experiments import (
    AttentionReaccelerationRule,
    FrozenPlayProjection,
    PlayGenome,
)

DAY = "2026-08-18"
DECISION = datetime.fromisoformat("2026-08-18T16:20:00+08:00")


def _active(tmp_path, close=10):
    tmp_path.mkdir(parents=True, exist_ok=True)
    source, active = tmp_path / "source.db", tmp_path / "data" / "alpha_miner.db"
    storage = Storage(str(source))
    storage.init_db()
    storage.execute_write(
        "INSERT INTO daily_price(stock_code,trade_date,close,snapshot_time) "
        "VALUES('000001','2026-08-17',?,'2026-08-17 16:00:00')",
        (close,),
    )
    publish_snapshot(source, active)
    return active


def _collect(day, storage):
    storage.execute_write(
        "INSERT OR REPLACE INTO daily_price"
        "(stock_code,trade_date,open,high,low,close,volume,amount,snapshot_time) "
        "VALUES('600001',?,9,10,9,10,100,1000,?)",
        (day, f"{day} 16:00:00"),
    )
    storage.execute_write(
        "INSERT OR REPLACE INTO zt_pool"
        "(stock_code,trade_date,name,industry,consecutive_zt,open_count,snapshot_time) "
        "VALUES('600001',?,'测试股','种植业',1,0,?)",
        (day, f"{day} 16:00:00"),
    )
    storage.execute_write(
        "INSERT OR REPLACE INTO limit_up_collection_runs"
        "(id,trade_date,attempted_at,price_rows,zt_rows,status,detail) "
        "VALUES(1,?,?,5000,50,'ok','closed')",
        (day, f"{day} 16:10:00"),
    )
    return {"daily_price": 1, "zt_pool": 1}, CollectionCheck(day, 5000, 50, "ok", "closed")


def _projection(dataset_hash, *, threshold=0.5, candidate="a"):
    rule = AttentionReaccelerationRule(
        min_total_attention=threshold,
        allowed_state_domains=tuple(sorted(AttentionReaccelerationRule().allowed_state_domains)),
    )
    genome = PlayGenome(
        "limited_attention_salience", "cascade_momentum", "THEORY_DERIVED",
        BehaviorStateSpec(), rule,
    )
    return FrozenPlayProjection(
        1, -1.0, candidate * 64, "b" * 64, dataset_hash, "d" * 64,
        genome.genome_hash, genome.execution_hash, genome.to_payload(),
        "DEVELOPMENT_CANDIDATE", "PAPER_ONLY", "HOLDOUT_NOT_OPENED", "NOT_ADMITTED",
    )


def _development(captured=None, *, threshold=0.5, candidate="a"):
    def develop(active, _temporary):
        dataset_hash = validate_pair(active, sidecar_path(active))["source_snapshot_sha256"]
        projection = _projection(dataset_hash, threshold=threshold, candidate=candidate)
        if captured is not None:
            captured.append(projection)
        return projection
    return develop


def _cards(active):
    connection = sqlite3.connect(f"file:{active.as_posix()}?mode=ro", uri=True)
    try:
        return connection.execute(
            "SELECT play_id,historical_evidence_json FROM play_cards ORDER BY play_id"
        ).fetchall()
    finally:
        connection.close()


def test_success_publishes_rank_one_identity_once(tmp_path, monkeypatch):
    active, captured = _active(tmp_path), []
    monkeypatch.setattr(pipeline, "_develop", _development(captured))
    result = pipeline.run_after_close(active, DAY, _collect, decision_clock=lambda: DECISION)

    projection = captured[0]
    assert result.paper_generated and result.projection == projection
    assert validate_pair(active, sidecar_path(active))["source_snapshot_sha256"] == sha256_file(active)
    cards = _cards(active)
    assert len(cards) == 1
    identity = json.loads(cards[0][1])["forward_plan"]["research_identity"]
    assert identity == projection.to_payload()
    assert set(identity) >= {
        "candidate_hash", "lineage_hash", "dataset_snapshot_hash",
        "search_family_hash", "genome_hash", "execution_hash",
    }


def test_research_failure_keeps_collected_canonical(tmp_path, monkeypatch):
    active = _active(tmp_path)
    monkeypatch.setattr(
        pipeline, "_develop", lambda *_: (_ for _ in ()).throw(RuntimeError("research failed"))
    )
    result = pipeline.run_after_close(active, DAY, _collect, decision_clock=lambda: DECISION)

    assert not result.paper_generated and result.warning.startswith("PAPER_NOT_GENERATED")
    assert sha256_file(active) == result.canonical_snapshot_hash
    assert _cards(active) == []
    validate_pair(active, sidecar_path(active))
def test_watch_capture_failure_is_best_effort_before_core_publish(tmp_path, monkeypatch):
    active = _active(tmp_path)
    monkeypatch.setattr(pipeline, "_develop", _development())
    monkeypatch.setattr(pipeline, "capture_watchlist_minutes",
                        lambda *_: (_ for _ in ()).throw(RuntimeError("watch source failed")))
    result = pipeline.run_after_close(active, DAY, _collect, decision_clock=lambda: DECISION)
    assert result.paper_generated
    assert result.watch_capture_status == "ERROR:RuntimeError:watch source failed"
    validate_pair(active, sidecar_path(active))
def test_watch_capture_runs_on_working_market_before_publish(tmp_path, monkeypatch):
    active, seen = _active(tmp_path), []
    monkeypatch.setattr(pipeline, "_develop", _development())
    def capture(working, preferences):
        seen.append((Path(working), Path(preferences)))
        Storage(str(working)).execute_write(
            "INSERT INTO daily_price(stock_code,trade_date,close,snapshot_time) VALUES"
            "('000735','2026-08-18',10,'2026-08-18 18:00:00')")
        return WatchlistCaptureResult(1, 1, 1, 0, "SUCCESS")
    monkeypatch.setattr(pipeline, "capture_watchlist_minutes", capture)
    result = pipeline.run_after_close(active, DAY, _collect, decision_clock=lambda: DECISION)
    assert result.paper_generated and result.watch_capture_status == "SUCCESS"
    assert seen[0][0] != active and seen[0][1] == active.parent / "user_preferences.db"
    connection = sqlite3.connect(active)
    assert connection.execute("SELECT COUNT(*) FROM daily_price WHERE stock_code='000735'").fetchone()[0] == 1
    connection.close()
def test_second_activation_failure_restores_canonical_one(tmp_path, monkeypatch):
    active = _active(tmp_path)
    monkeypatch.setattr(pipeline, "_develop", _development())
    calls = 0

    def fail_after_second(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            return real_activate(*args, **kwargs)
        original_replace = activation.os.replace
        def fail_manifest(source, target):
            if target == sidecar_path(active):
                raise RuntimeError("manifest replace interrupted")
            return original_replace(source, target)
        activation.os.replace = fail_manifest
        try:
            return real_activate(*args, **kwargs)
        finally:
            activation.os.replace = original_replace

    monkeypatch.setattr(pipeline, "activate_snapshot", fail_after_second)
    result = pipeline.run_after_close(active, DAY, _collect, decision_clock=lambda: DECISION)

    assert calls == 2 and not result.paper_generated
    assert sha256_file(active) == result.canonical_snapshot_hash
    assert _cards(active) == []
    validate_pair(active, sidecar_path(active))


def test_retry_is_idempotent_and_different_plan_cannot_overwrite(tmp_path, monkeypatch):
    active, captured = _active(tmp_path), []
    monkeypatch.setattr(pipeline, "_develop", _development(captured))
    one = pipeline.run_after_close(active, DAY, _collect, decision_clock=lambda: DECISION)
    assert one.paper_generated and len(_cards(active)) == 1
    first = captured[0]

    monkeypatch.setattr(pipeline, "_develop", _development(threshold=1.0, candidate="e"))
    result = pipeline.run_after_close(active, DAY, _collect, decision_clock=lambda: DECISION)
    assert not result.paper_generated and "cannot be replaced" in result.warning
    identity = json.loads(_cards(active)[0][1])["forward_plan"]["research_identity"]
    assert identity["candidate_hash"] == first.candidate_hash


def test_replace_after_development_is_a_hard_conflict(tmp_path, monkeypatch):
    active, replacement = _active(tmp_path), _active(tmp_path / "other", close=11)
    def replace_after_development(current, temporary):
        projection = _development()(current, temporary)
        shutil.copy2(replacement, current)
        shutil.copy2(sidecar_path(replacement), sidecar_path(current))
        return projection
    monkeypatch.setattr(pipeline, "_develop", replace_after_development)
    with pytest.raises(activation.ActivationConflict, match="not canonical#1"):
        pipeline.run_after_close(active, DAY, _collect, decision_clock=lambda: DECISION)
    assert sha256_file(active) == sha256_file(replacement)
    assert _cards(active) == []


def test_exclusive_lock_rejects_concurrent_worker(tmp_path):
    active = _active(tmp_path)
    lock = active.with_suffix(active.suffix + ".activation.lock")
    lock.touch()
    try:
        with pytest.raises(activation.ActivationConflict, match="already in progress"):
            pipeline.run_after_close(active, DAY, _collect, decision_clock=lambda: DECISION)
    finally:
        lock.unlink()


def test_replace_between_check_and_paper_copy_is_a_hard_conflict(tmp_path, monkeypatch):
    active, replacement = _active(tmp_path), _active(tmp_path / "other", close=11)
    monkeypatch.setattr(pipeline, "_develop", _development())
    original_copy, calls = pipeline._copy_canonical, 0

    def replace_before_copy(current, working, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 3:
            shutil.copy2(replacement, current)
            shutil.copy2(sidecar_path(replacement), sidecar_path(current))
        return original_copy(current, working, **kwargs)

    monkeypatch.setattr(pipeline, "_copy_canonical", replace_before_copy)
    with pytest.raises(activation.ActivationConflict, match="not canonical#1"):
        pipeline.run_after_close(active, DAY, _collect, decision_clock=lambda: DECISION)
    assert sha256_file(active) == sha256_file(replacement)
    assert _cards(active) == []


def test_replace_after_paper_copy_before_activation_is_a_hard_conflict(tmp_path, monkeypatch):
    active, replacement = _active(tmp_path), _active(tmp_path / "other", close=11)
    monkeypatch.setattr(pipeline, "_develop", _development())
    original_activate, calls = pipeline.activate_snapshot, 0

    def replace_before_second_activate(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 2:
            shutil.copy2(replacement, active)
            shutil.copy2(sidecar_path(replacement), sidecar_path(active))
        return original_activate(*args, **kwargs)

    monkeypatch.setattr(pipeline, "activate_snapshot", replace_before_second_activate)
    with pytest.raises(activation.ActivationConflict, match="expected hash"):
        pipeline.run_after_close(active, DAY, _collect, decision_clock=lambda: DECISION)
    assert sha256_file(active) == sha256_file(replacement)
    assert _cards(active) == []


def test_missing_canonical_manifest_fails_before_collection(tmp_path):
    active = tmp_path / "alpha_miner.db"
    Storage(str(active)).init_db()
    called = False

    def collect(*_):
        nonlocal called
        called = True

    with pytest.raises(Exception):
        pipeline.run_after_close(active, DAY, collect, decision_clock=lambda: DECISION)
    assert not called
def test_preferences_cannot_alias_active_before_collection_or_capture(tmp_path, monkeypatch):
    active = _active(tmp_path)
    monkeypatch.setattr(pipeline, "capture_watchlist_minutes",
                        lambda *_: pytest.fail("capture must not start for aliased preferences"))
    with pytest.raises(Exception, match="偏好库路径"):
        pipeline.run_after_close(active, DAY, lambda *_: pytest.fail("collection must not start"),
                                 decision_clock=lambda: DECISION, preferences_db=active)
def test_default_collect_does_not_enter_auto_pipeline(tmp_path, monkeypatch):
    database = tmp_path / "legacy.db"
    monkeypatch.delenv("ALPHA_MINER_AUTO_DEVELOPMENT", raising=False)
    monkeypatch.setattr(
        "cli.limit_up._collect_and_audit",
        lambda day, storage: ({}, CollectionCheck(day, 0, 0, "skipped", "weekend")),
    )
    monkeypatch.setattr(
        pipeline, "run_after_close", lambda *_: pytest.fail("auto pipeline was called")
    )
    result = CliRunner().invoke(main, ["collect", "--db", str(database)])
    assert result.exit_code == 0
    assert "auto development" not in result.output


def test_enabled_cli_uses_aware_shanghai_default_clock(tmp_path, monkeypatch):
    active = _active(tmp_path)
    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return DECISION.astimezone(tz) if tz else DECISION.replace(tzinfo=None)
    monkeypatch.setattr(pipeline, "datetime", FixedDateTime)
    monkeypatch.setattr(limit_up_cli, "datetime", FixedDateTime)
    monkeypatch.setattr(pipeline, "_develop", _development())
    monkeypatch.setattr(limit_up_cli, "_collect_and_audit", _collect)
    result = CliRunner().invoke(
        main, ["collect", "--db", str(active)], env={"ALPHA_MINER_AUTO_DEVELOPMENT": "1"}
    )
    assert result.exit_code == 0
    assert "PAPER card projected" in result.output
    generated_at = json.loads(_cards(active)[0][1])["forward_plan"]["generated_at"]
    assert generated_at.endswith("+08:00")
