"""End-to-end contract for the first unified executable-play adapter."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest

from src.data.snapshot_manifest import build_manifest, canonical_json, sidecar_path
from src.data.storage import Storage
from src.mining.evolution import EvolutionEngine
from src.mining.experiments import HOLDOUT_NOT_OPENED, DevelopmentEvidence, FrozenPartition
from src.mining.plays import theme_new_entrant_candidate
from src.mining.research_ledger import ResearchLedger

DATES = [f"2026-07-{day:02d}" for day in range(1, 13)]


def _active_market(
    root: Path, *, unfilled: bool = False, dates: list[str] | None = None
) -> Path:
    dates = dates or DATES
    root.mkdir(parents=True)
    market = root / "alpha_miner.db"
    storage = Storage(str(market))
    storage.init_db()
    for index, trade_date in enumerate(dates):
        storage.execute_write(
            "INSERT INTO daily_price(stock_code,trade_date,open,high,low,close,volume,"
            "snapshot_time) VALUES('CAL',?,10,11,9,10,100,?)",
            (trade_date, f"{trade_date} 16:00:00"),
        )
        storage.execute_write(
            "INSERT INTO limit_up_collection_runs(trade_date,attempted_at,price_rows,"
            "zt_rows,status,detail) VALUES(?,?,5000,50,'ok','')",
            (trade_date, f"{trade_date} 16:10:00"),
        )
        breadth = 2 if index % 2 == 0 else 3
        for number in range(breadth):
            storage.execute_write(
                "INSERT INTO zt_pool(stock_code,trade_date,name,industry,amount,"
                "snapshot_time) VALUES(?,?,?,'通用行业',100,?)",
                (f"600{index:02d}{number}", trade_date, f"涨停{number}", f"{trade_date} 16:01:00"),
            )
        if breadth == 3:
            code = f"601{index:03d}"
            storage.execute_write(
                "INSERT INTO strong_pool(stock_code,trade_date,name,industry,amount,"
                "snapshot_time) VALUES(?,?,?,'通用行业',999,?)",
                (code, trade_date, "新强势", f"{trade_date} 16:02:00"),
            )
            storage.execute_write(
                "INSERT INTO daily_price(stock_code,trade_date,open,high,low,close,volume,"
                "snapshot_time) VALUES(?,?,10,11,9,10,100,?)",
                (code, trade_date, f"{trade_date} 16:03:00"),
            )
            for offset, open_price in ((1, 10.0), (3, 11.0)):
                if index + offset < len(dates):
                    high = open_price if unfilled and offset == 1 else open_price + 1
                    low = high if unfilled and offset == 1 else open_price - 1
                    storage.execute_write(
                        "INSERT INTO daily_price(stock_code,trade_date,open,high,low,close,"
                        "volume,snapshot_time) VALUES(?,?,?,?,?,?,100,?)",
                        (
                            code,
                            dates[index + offset],
                            open_price,
                            high,
                            low,
                            open_price,
                            f"{dates[index + offset]} 16:03:00",
                        ),
                    )
    manifest = build_manifest(
        market, published_at="2026-08-18T01:00:00.000000+00:00"
    )
    sidecar_path(market).write_text(canonical_json(manifest), encoding="utf-8")
    return market


def _table_count(path: Path, table: str) -> int:
    connection = sqlite3.connect(path)
    try:
        return int(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
    finally:
        connection.close()


def test_complete_play_candidate_is_immutable_and_strict():
    partition = FrozenPartition(
        play_id="theme_new_entrant_diffusion_v1",
        dataset_snapshot_hash="a" * 64,
        audited_dates=("2026-07-01", "2026-07-02"),
        development_dates=(),
        embargo_dates=("2026-07-01",),
        reserved_dates=("2026-07-02",),
    )
    candidate = theme_new_entrant_candidate(partition)
    protocol = candidate.spec.protocol()
    assert set(protocol) == {
        "play_id",
        "behavior_hypothesis",
        "universe_rule",
        "decision_boundary",
        "prediction",
        "entry_rule",
        "exit_rule",
        "executability_rule",
        "invalidations",
        "market_regime",
        "development_protocol",
        "adapter_id",
    }
    with pytest.raises(FrozenInstanceError):
        candidate.candidate_name = "changed"  # type: ignore[misc]
    assert candidate.cost_model == {
        "commission_bps": None,
        "model_scope": "LUMP_SUM_PROXY_NOT_ITEMIZED",
        "round_trip_bps": 20.0,
        "slippage_bps": None,
        "stamp_tax_bps": None,
    }


def test_freezes_before_return_read_and_uses_only_bound_snapshot(tmp_path, monkeypatch):
    market = _active_market(tmp_path / "data")
    market_hash = hashlib.sha256(market.read_bytes()).hexdigest()
    original = EvolutionEngine._evaluate_play_adapter
    observed: dict[str, object] = {"frozen": [], "paths": []}

    def spy(engine, storage, candidate, **kwargs):
        ledger_path = market.parent / "research_ledger.db"
        observed["frozen"].append(_table_count(ledger_path, "research_candidates"))
        observed["paths"].append(storage.db_path)
        return original(engine, storage, candidate, **kwargs)

    monkeypatch.setattr(EvolutionEngine, "_evaluate_play_adapter", spy)
    engine = EvolutionEngine(db_path=str(market))
    evidence = engine.run(
        generations=3, population_size=4, resume=False, workers=2
    )[0]

    assert observed["frozen"] == [4] * 4 + [8] * 4 + [12] * 4
    assert len(set(observed["paths"])) == 1
    bound_path = observed["paths"][0]
    assert "research_snapshots" in str(bound_path)
    assert Path(str(bound_path)) != market
    assert evidence.holdout_status == HOLDOUT_NOT_OPENED
    assert evidence.completed_signal_days > 0
    assert engine.accepted == []
    assert engine.completed_generations == 3
    assert hashlib.sha256(market.read_bytes()).hexdigest() == market_hash
    assert _table_count(market.parent / "research_ledger.db", "research_evidence") == 12


def test_same_snapshot_and_search_family_are_idempotent_and_cost_is_fixed(
    tmp_path, monkeypatch
):
    market = _active_market(tmp_path / "data")
    monkeypatch.setattr(
        EvolutionEngine,
        "_run_factor_hypothesis_development",
        lambda *args, **kwargs: [],
    )
    EvolutionEngine(db_path=str(market)).run(total_cost_bps=20)
    EvolutionEngine(db_path=str(market)).run(total_cost_bps=20)
    ledger_path = market.parent / "research_ledger.db"
    assert _table_count(ledger_path, "research_candidates") == 1
    assert _table_count(ledger_path, "research_evidence") == 1

    with pytest.raises(ValueError, match="cost must match"):
        EvolutionEngine(db_path=str(market)).run(total_cost_bps=30)
    assert _table_count(ledger_path, "research_candidates") == 1
    assert _table_count(ledger_path, "research_evidence") == 1
    connection = sqlite3.connect(ledger_path)
    try:
        rows = connection.execute(
            "SELECT candidate_hash, protocol_json FROM research_candidates "
            "ORDER BY candidate_hash"
        ).fetchall()
    finally:
        connection.close()
    plans = [json.loads(row[1])["frozen_partition"] for row in rows]
    assert {plan["total_cost_bps"] for plan in plans} == {20.0}
    assert len({plan["partition_hash"] for plan in plans}) == 1
    assert len({row[0] for row in rows}) == 1


def test_unfilled_and_no_sample_are_append_only_negative_evidence(
    tmp_path, monkeypatch
):
    market = _active_market(tmp_path / "data", unfilled=True)
    monkeypatch.setattr(
        EvolutionEngine,
        "_run_factor_hypothesis_development",
        lambda *args, **kwargs: [],
    )
    result = EvolutionEngine(db_path=str(market)).run()[0]
    assert result.candidate_count > 0
    assert result.unfilled_count > 0
    assert result.completed_signal_days == 0
    assert result.mean_net_return_pct is None
    ledger_path = market.parent / "research_ledger.db"
    connection = sqlite3.connect(ledger_path)
    try:
        payload = json.loads(
            connection.execute(
                "SELECT payload_json FROM research_evidence"
            ).fetchone()[0]
        )
    finally:
        connection.close()
    assert payload["research_status"] == "DEVELOPMENT_ONLY"
    assert payload["holdout_status"] == "HOLDOUT_NOT_OPENED"
    assert payload["mean_net_return_pct"] is None


def test_public_run_never_opens_holdout(tmp_path, monkeypatch):
    market = _active_market(tmp_path / "data")
    monkeypatch.setattr(
        ResearchLedger,
        "open_holdout",
        lambda *args, **kwargs: pytest.fail("holdout must remain closed"),
    )
    monkeypatch.setattr(
        EvolutionEngine,
        "_run_factor_hypothesis_development",
        lambda *args, **kwargs: [],
    )
    monkeypatch.setattr(
        ResearchLedger,
        "append_holdout_result",
        lambda *args, **kwargs: pytest.fail("holdout result must remain closed"),
    )
    EvolutionEngine(db_path=str(market)).run()


def test_public_run_uses_play_budget_and_never_calls_factor_loop(tmp_path, monkeypatch):
    market = _active_market(tmp_path / "data")
    engine = EvolutionEngine(
        db_path=str(market),
        mining_log_path=str(tmp_path / "factor.jsonl"),
        state_path=str(tmp_path / "factor-state.json"),
    )
    observed = []
    monkeypatch.setattr(
        EvolutionEngine,
        "_run_factor_hypothesis_development",
        lambda *args, **kwargs: pytest.fail("factor loop must not consume play budget"),
    )
    monkeypatch.setattr(
        EvolutionEngine,
        "_evaluate_play_adapter",
        lambda engine, storage, candidate, **kwargs: (
            observed.append(storage.db_path) or DevelopmentEvidence(
                "2026-07-01", "2026-07-01", 0, 0, 0, 0, 0, 0,
                None, None, None, None, 20.0, {}, ("NO_SAMPLE",),
            )
        ),
    )
    engine.run(generations=1, population_size=1, resume=False, workers=1)
    assert observed == [engine.db_path]
    assert "research_snapshots" in engine.db_path
    assert engine.sandbox.db_path == engine.db_path
    assert engine.completed_generations == 1
    assert engine.development_candidates == []
    assert engine.play_development_candidates[0]["research_status"] == "DEVELOPMENT_CANDIDATE"
