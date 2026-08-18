"""One-shot holdout gate tests; all databases are synthetic and temporary."""

from __future__ import annotations

import sqlite3
from datetime import date, timedelta

import pytest

from src.mining.evolution import EvolutionEngine
from src.mining.experiments import FrozenPartition, canonical_mapping
from src.mining.plays import (
    summarize_theme_holdout_statistics,
    theme_new_entrant_candidate,
)
from src.mining.research_ledger import CandidateSpec, LineageRetired, ResearchLedger
from tests.test_unified_experiment import _active_market


def _dates(count: int) -> tuple[str, ...]:
    start = date(2025, 1, 1)
    return tuple((start + timedelta(days=index)).isoformat() for index in range(count))


def _partition(snapshot_hash: str, *, reserved: int = 40) -> FrozenPartition:
    audited = _dates(reserved + 4)
    return FrozenPartition(
        play_id="theme_new_entrant_diffusion_v1",
        dataset_snapshot_hash=snapshot_hash,
        audited_dates=audited,
        development_dates=audited[:1],
        embargo_dates=audited[1:4],
        reserved_dates=audited[4:],
    )


def _run_bound_engine(tmp_path, monkeypatch) -> EvolutionEngine:
    market = _active_market(tmp_path / "data")
    monkeypatch.setattr(
        EvolutionEngine,
        "_run_factor_hypothesis_development",
        lambda *args, **kwargs: [],
    )
    engine = EvolutionEngine(
        db_path=str(market),
        mining_log_path=str(tmp_path / "factor.jsonl"),
        state_path=str(tmp_path / "factor-state.json"),
    )
    engine.run()
    return engine


def _replace_with_h1_candidate(engine: EvolutionEngine, *, reserved: int = 40):
    ledger = engine._ledger
    bound = engine._bound_snapshot
    assert ledger is not None and bound is not None
    partition = _partition(bound.source_snapshot_sha256, reserved=reserved)
    candidate = theme_new_entrant_candidate(partition)
    implementation = engine._implementation_manifest(candidate)
    frozen = ledger.freeze_candidate(
        CandidateSpec(
            candidate_name=candidate.candidate_name,
            experiment_type="EXECUTABLE_PLAY",
            code_text=canonical_mapping(implementation, "implementation"),
            parameters=candidate.parameters,
            data_manifest={"frozen_partition": partition.to_dict()},
            cost_model=candidate.cost_model,
            protocol={
                **candidate.spec.protocol(),
                "frozen_partition": partition.to_dict(),
                "holdout_scope_hash": partition.holdout_scope_hash,
            },
        )
    )
    ledger.append_development_result(
        frozen.candidate_hash,
        {"research_status": "DEVELOPMENT_ONLY"},
        f"dev:{frozen.candidate_hash}",
    )
    engine._play_candidate = candidate
    engine._frozen_candidate = frozen
    return ledger, bound, candidate, frozen


def _metrics(value: float = 0.1, count: int = 40) -> dict:
    return summarize_theme_holdout_statistics(
        {
            "signal_days": count,
            "candidate_count": count + 5,
            "filled_count": count,
            "unfilled_count": 5,
            "fill_signal_days": count,
            "invalidations": {"ENTRY_NOT_EXECUTABLE": 5},
            "daily_returns": [value] * count,
        },
        _partition("a" * 64, reserved=40),
    )


def test_immature_check_never_opens_or_constructs_reserved_reader(tmp_path, monkeypatch):
    engine = _run_bound_engine(tmp_path, monkeypatch)
    _replace_with_h1_candidate(engine, reserved=39)
    monkeypatch.setattr(
        ResearchLedger,
        "open_holdout",
        lambda *args, **kwargs: pytest.fail("immature candidate must not open"),
    )
    result = engine.evaluate_holdout(
        authorize_once=True,
        _reader_factory=lambda path: pytest.fail("reserved values were read"),
    )
    assert result.status == "NOT_OPENED_IMMATURE"
    assert result.opened is False


def test_open_is_committed_before_first_reserved_reader_and_terminal_is_written(
    tmp_path, monkeypatch
):
    engine = _run_bound_engine(tmp_path, monkeypatch)
    ledger, _bound, _candidate, frozen = _replace_with_h1_candidate(engine)
    observed = []

    def reader_factory(_path):
        connection = sqlite3.connect(ledger._db_path)
        try:
            observed.append(
                    connection.execute(
                        "SELECT COUNT(*) FROM research_evidence "
                        "WHERE candidate_hash=? AND lineage_hash=? "
                        "AND event_type='HOLDOUT_OPENED'",
                        (frozen.candidate_hash, frozen.lineage_hash),
                    ).fetchone()[0]
            )
        finally:
            connection.close()
        return object()

    monkeypatch.setattr(
        "src.mining.evolution.evaluate_theme_new_entrant_holdout",
        lambda storage, candidate: _metrics(0.1),
    )
    result = engine.evaluate_holdout(
        authorize_once=True, _reader_factory=reader_factory
    )
    assert observed == [1]
    assert result.status == "REJECTED"
    connection = sqlite3.connect(ledger._db_path)
    try:
        holdout_events = connection.execute(
            "SELECT event_type FROM research_evidence "
            "WHERE candidate_hash=? AND lineage_hash=? "
            "AND event_type IN ('HOLDOUT_OPENED','HOLDOUT_RESULT','EVALUATION_ERROR') "
            "ORDER BY sequence_id",
            (frozen.candidate_hash, frozen.lineage_hash),
        ).fetchall()
    finally:
        connection.close()
    assert holdout_events == [("HOLDOUT_OPENED",), ("HOLDOUT_RESULT",)]


def test_ordinary_error_is_terminal_but_hard_crash_is_inconclusive(
    tmp_path, monkeypatch
):
    error_engine = _run_bound_engine(tmp_path / "error", monkeypatch)
    error_ledger, *_ = _replace_with_h1_candidate(error_engine)
    result = error_engine.evaluate_holdout(
        authorize_once=True,
        _reader_factory=lambda path: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    assert result.status == "EVALUATION_ERROR"
    connection = sqlite3.connect(error_ledger._db_path)
    try:
        assert connection.execute(
            "SELECT event_type FROM research_evidence ORDER BY sequence_id DESC LIMIT 1"
        ).fetchone()[0] == "EVALUATION_ERROR"
    finally:
        connection.close()

    crash_engine = _run_bound_engine(tmp_path / "crash", monkeypatch)
    crash_ledger, *_ = _replace_with_h1_candidate(crash_engine)
    with pytest.raises(KeyboardInterrupt):
        crash_engine.evaluate_holdout(
            authorize_once=True,
            _reader_factory=lambda path: (_ for _ in ()).throw(KeyboardInterrupt()),
        )
    with pytest.raises(LineageRetired):
        crash_engine.evaluate_holdout(authorize_once=True)
    connection = sqlite3.connect(crash_ledger._db_path)
    try:
        assert connection.execute(
            "SELECT event_type FROM research_evidence ORDER BY sequence_id DESC LIMIT 1"
        ).fetchone()[0] == "HOLDOUT_OPENED"
    finally:
        connection.close()


def test_retired_lineage_rejects_development_before_storage_read(tmp_path, monkeypatch):
    engine = _run_bound_engine(tmp_path, monkeypatch)
    _replace_with_h1_candidate(engine)
    with pytest.raises(KeyboardInterrupt):
        engine.evaluate_holdout(
            authorize_once=True,
            _reader_factory=lambda path: (_ for _ in ()).throw(KeyboardInterrupt()),
        )

    class ForbiddenStorage:
        def execute(self, *args, **kwargs):
            pytest.fail("retired development attempted a data read")

    with pytest.raises(RuntimeError, match="retired lineage"):
        engine._evaluate_play_adapter(ForbiddenStorage(), engine._play_candidate)


def test_bootstrap_and_admission_gates_are_deterministic():
    approved = _metrics(0.5, 40)
    repeated = _metrics(0.5, 40)
    assert approved == repeated
    assert approved["terminal_decision"] == "ADMISSION_APPROVED_PENDING_PUBLICATION"
    assert approved["multiplicity"] == {"family_size": 1, "rule": "HOLM_FAMILY_1"}
    assert approved["candidate_count"] == 45
    assert approved["unfilled_count"] == 5
    assert _metrics(0.1, 40)["terminal_decision"] == "REJECTED"
    assert _metrics(0.5, 39)["terminal_decision"] == "INSUFFICIENT_SAMPLE"
