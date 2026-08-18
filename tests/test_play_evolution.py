"""Bounded executable-play evolution and ledger-resume contracts."""

from __future__ import annotations

import json
import shutil
import sqlite3
import threading
from dataclasses import replace
from datetime import date, timedelta

import pytest

from src.data.snapshot_manifest import build_manifest, canonical_json, sidecar_path
from src.mining.behavior_state import BehaviorStateSpec
from src.mining.evolution import EvolutionEngine
from src.mining.experiments import (
    AttentionReaccelerationRule,
    DevelopmentEvidence,
    FrozenPartition,
    PlayGenome,
)
from src.mining.plays import ATTENTION_REACCELERATION_PLAY_ID, attention_genome_candidate
from src.mining.research_ledger import ResearchLedger
from tests.test_unified_experiment import _active_market


def _evidence(mean: float | None = 0.3, completed: int = 12) -> DevelopmentEvidence:
    return DevelopmentEvidence(
        coverage_start="2026-07-01",
        coverage_end="2026-07-10",
        signal_days=completed,
        candidate_count=max(1, completed),
        filled_count=completed,
        unfilled_count=0,
        completed_signal_days=completed,
        wins=completed if mean and mean > 0 else 0,
        mean_net_return_pct=mean,
        win_rate=1.0 if mean and mean > 0 else 0.0,
        profit_loss_ratio=0.0,
        max_drawdown_pct=0.0,
        total_cost_bps=20.0,
        invalidation_counts={},
        data_limitations=("SYNTHETIC_DEVELOPMENT",),
    )


def _rows(path, sql, params=()):
    connection = sqlite3.connect(path)
    try:
        return connection.execute(sql, params).fetchall()
    finally:
        connection.close()


def test_play_genome_is_deterministic_bounded_and_semantic():
    genome = PlayGenome(
        "limited_attention_salience",
        "salience_volume_reacceleration",
        "THEORY_DERIVED",
        BehaviorStateSpec(),
        replace(
            AttentionReaccelerationRule(),
            allowed_state_domains=(
                "industry_diffusion_non_limit",
                "post_limit_non_limit",
                "recent_limit_memory",
            ),
        ),
    )
    assert genome.genome_hash == genome.genome_hash
    mutated = genome.mutate("half_life_trade_days")
    assert mutated.genome_hash != genome.genome_hash
    assert mutated.behavior_spec.half_life_trade_days in {3.0, 5.0, 8.0}
    with pytest.raises(ValueError, match="preregistered"):
        replace(genome, behavior_spec=replace(genome.behavior_spec, half_life_trade_days=6)).validate()


def test_knowledge_provenance_merges_identical_execution():
    engine = EvolutionEngine()
    genomes = engine._knowledge_play_genomes()
    execution_hashes = [genome.execution_hash for genome in genomes]
    assert len(execution_hashes) == len(set(execution_hashes))
    assert any(len(genome.theory_provenance) > 1 for genome in genomes)
    merged = next(genome for genome in genomes if len(genome.theory_provenance) > 1)
    assert len(merged.to_payload()["theory_provenance"]) == len(merged.theory_provenance)


def test_generations_population_workers_freeze_before_parallel_read(tmp_path, monkeypatch):
    market = _active_market(tmp_path / "data")
    observed_paths = []
    thread_ids = set()
    first_generation = threading.Barrier(3)
    lock = threading.Lock()
    calls = 0

    def fake(engine, storage, candidate, **kwargs):
        nonlocal calls
        with lock:
            calls += 1
            current = calls
            thread_ids.add(threading.get_ident())
            observed_paths.append(storage.db_path)
        if current <= 3:
            first_generation.wait(timeout=5)
        ledger = engine._ledger
        assert ledger is not None
        history = ledger.load_development_history(
            "EVOLVED_EXECUTABLE_PLAY", engine._bound_snapshot.source_snapshot_sha256
        )
        assert len(history) == 3 * (((current - 1) // 3) + 1)
        assert sum(event is not None for _, event in history) < current
        return _evidence()

    monkeypatch.setattr(EvolutionEngine, "_evaluate_play_adapter", fake)
    engine = EvolutionEngine(db_path=str(market))
    engine.run(generations=2, population_size=3, resume=True, workers=3)

    assert calls == 6
    assert len(thread_ids) >= 3
    assert len(set(observed_paths)) == 1
    assert "research_snapshots" in observed_paths[0]
    assert engine.completed_generations == 2
    assert len(engine.play_development_candidates) == 6
    ledger_path = market.parent / "research_ledger.db"
    assert _rows(ledger_path, "SELECT COUNT(*) FROM research_candidates")[0][0] == 6
    assert _rows(ledger_path, "SELECT COUNT(*) FROM research_evidence")[0][0] == 6


def test_ranked_projection_matches_frozen_ledger_identity(tmp_path, monkeypatch):
    market = _active_market(tmp_path / "data")
    monkeypatch.setattr(EvolutionEngine, "_evaluate_play_adapter", lambda *a, **k: _evidence())
    engine = EvolutionEngine(db_path=str(market))
    engine.run(generations=1, population_size=2, resume=True, workers=1)
    projection = engine.ranked_play_projections(1)[0]

    row = _rows(
        market.parent / "research_ledger.db",
        "SELECT c.candidate_hash,c.lineage_hash,c.dataset_snapshot_hash,c.parameters_json,"
        "e.payload_json FROM research_candidates c JOIN research_evidence e "
        "USING(candidate_hash) WHERE c.candidate_hash=?",
        (projection.candidate_hash,),
    )[0]
    parameters = json.loads(row[3])
    assert row[:3] == (
        projection.candidate_hash, projection.lineage_hash, projection.dataset_snapshot_hash,
    )
    assert parameters["genome_hash"] == projection.genome_hash
    assert parameters["execution_hash"] == projection.execution_hash
    assert json.loads(row[4])["evolution"]["search_family_hash"] == projection.search_family_hash
    assert projection.to_payload()["holdout_status"] == "HOLDOUT_NOT_OPENED"
    assert _rows(
        market.parent / "research_ledger.db",
        "SELECT COUNT(*) FROM research_evidence WHERE event_type='HOLDOUT_OPENED'",
    )[0][0] == 0

    history = engine._ledger.load_development_history(
        "EVOLVED_EXECUTABLE_PLAY", engine._bound_snapshot.source_snapshot_sha256
    )
    monkeypatch.setattr(
        engine._ledger, "load_development_history", lambda *_: history + history[:1]
    )
    with pytest.raises(ValueError, match="invalid or duplicated"):
        engine.ranked_play_projections(1)


def test_partial_generation_resume_skips_completed_trials(tmp_path, monkeypatch):
    market = _active_market(tmp_path / "data")
    original = ResearchLedger.append_development_result
    written = 0

    def interrupt(ledger, *args, **kwargs):
        nonlocal written
        if written == 4:
            raise RuntimeError("simulated interruption")
        written += 1
        return original(ledger, *args, **kwargs)

    monkeypatch.setattr(EvolutionEngine, "_evaluate_play_adapter", lambda *a, **k: _evidence())
    monkeypatch.setattr(ResearchLedger, "append_development_result", interrupt)
    with pytest.raises(RuntimeError, match="interruption"):
        EvolutionEngine(db_path=str(market)).run(
            generations=2, population_size=3, resume=True, workers=1
        )
    payloads = _rows(
        market.parent / "research_ledger.db",
        "SELECT payload_json FROM research_evidence ORDER BY sequence_id",
    )
    assert [json.loads(row[0])["evolution"]["generation"] for row in payloads] == [1, 1, 1, 2]
    monkeypatch.setattr(ResearchLedger, "append_development_result", original)
    resumed_calls = 0

    def resumed(*args, **kwargs):
        nonlocal resumed_calls
        resumed_calls += 1
        return _evidence()

    monkeypatch.setattr(EvolutionEngine, "_evaluate_play_adapter", resumed)
    engine = EvolutionEngine(db_path=str(market))
    engine.run(generations=2, population_size=3, resume=True, workers=1)
    assert resumed_calls == 2
    assert engine.completed_generations == 2
    assert len(engine.play_development_candidates) == 6


def test_resume_rejects_budget_change_and_fresh_backdoor(tmp_path, monkeypatch):
    market = _active_market(tmp_path / "data")
    monkeypatch.setattr(EvolutionEngine, "_evaluate_play_adapter", lambda *a, **k: _evidence())
    EvolutionEngine(db_path=str(market)).run(
        generations=1, population_size=2, resume=True, workers=1
    )
    with pytest.raises(ValueError, match="protocol differs"):
        EvolutionEngine(db_path=str(market)).run(
            generations=2, population_size=2, resume=True, workers=1
        )
    with pytest.raises(ValueError, match="fresh search"):
        EvolutionEngine(db_path=str(market)).run(
            generations=1, population_size=2, resume=False, workers=1
        )


@pytest.mark.parametrize(
    ("old", "new"),
    [
        ("limited_attention_salience", "limited_attention_salience_v2"),
        ("salience_volume_reacceleration", "salience_volume_reacceleration_v2"),
        (
            "behavior_states: [attention_memory, diffusion, decay]",
            "behavior_states: [attention_memory]",
        ),
    ],
)
def test_resume_rejects_theory_or_seed_manifest_drift(tmp_path, monkeypatch, old, new):
    market = _active_market(tmp_path / "data")
    knowledge = tmp_path / "theories.yaml"
    source = EvolutionEngine().kb_path.read_text(encoding="utf-8")
    knowledge.write_text(source, encoding="utf-8")
    monkeypatch.setattr(EvolutionEngine, "_evaluate_play_adapter", lambda *a, **k: _evidence())
    EvolutionEngine(db_path=str(market), knowledge_path=str(knowledge)).run(
        generations=1, population_size=2, resume=True, workers=1
    )
    knowledge.write_text(
        source.replace(old, new, 1),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="protocol differs"):
        EvolutionEngine(db_path=str(market), knowledge_path=str(knowledge)).run(
            generations=1, population_size=2, resume=True, workers=1
        )


def test_failed_parent_produces_bounded_traceable_offspring(tmp_path, monkeypatch):
    market = _active_market(tmp_path / "data")
    monkeypatch.setattr(
        EvolutionEngine, "_evaluate_play_adapter", lambda *a, **k: _evidence(None, 0)
    )
    engine = EvolutionEngine(db_path=str(market))
    engine.run(generations=2, population_size=2, resume=True, workers=1)
    evolution = engine.play_development_candidates
    children = [item for item in evolution if item["generation"] == 2]
    assert len(children) == 2
    assert all(item["parent_candidate_hashes"] for item in children)
    assert all(item["mutation_reason"].startswith("DIRECTED_MUTATION:") for item in children)
    assert all(item["failure_family"] == "SMALL_SAMPLE" for item in evolution)
    assert all(item["holdout_status"] == "HOLDOUT_NOT_OPENED" for item in evolution)


def test_evolved_attention_is_rejected_before_holdout_open_or_reserved_read(
    tmp_path, monkeypatch
):
    market = _active_market(tmp_path / "data")
    monkeypatch.setattr(EvolutionEngine, "_evaluate_play_adapter", lambda *a, **k: _evidence())
    engine = EvolutionEngine(db_path=str(market))
    engine.run(generations=1, population_size=1, resume=True, workers=1)
    ledger = engine._ledger
    bound = engine._bound_snapshot
    assert ledger is not None and bound is not None
    frozen, event = ledger.load_development_history(
        "EVOLVED_EXECUTABLE_PLAY", bound.source_snapshot_sha256
    )[0]
    genome = engine._genome_from_payload(event.payload["evolution"]["genome"])
    dates = tuple((date(2026, 1, 1) + timedelta(days=index)).isoformat() for index in range(44))
    partition = FrozenPartition(
        ATTENTION_REACCELERATION_PLAY_ID,
        bound.source_snapshot_sha256,
        dates,
        dates[:1],
        dates[1:4],
        dates[4:],
    )
    engine._play_candidate = attention_genome_candidate(partition, genome)
    engine._frozen_candidate = frozen
    with pytest.raises(RuntimeError, match="HOLDOUT_NOT_OPENED.*unsupported"):
        engine.evaluate_holdout(
            authorize_once=True,
            _reader_factory=lambda _path: pytest.fail("reserved reader must remain unopened"),
        )
    assert _rows(
        market.parent / "research_ledger.db",
        "SELECT COUNT(*) FROM research_evidence WHERE event_type='HOLDOUT_OPENED'",
    )[0][0] == 0


def test_resume_reads_canonical_evidence_without_recalculation(tmp_path, monkeypatch):
    market = _active_market(tmp_path / "data")
    monkeypatch.setattr(EvolutionEngine, "_evaluate_play_adapter", lambda *a, **k: _evidence())
    first = EvolutionEngine(db_path=str(market))
    first.run(generations=1, population_size=2, resume=True, workers=1)
    expected = [item["genome_hash"] for item in first.play_development_candidates]
    monkeypatch.setattr(
        EvolutionEngine,
        "_evaluate_play_adapter",
        lambda *a, **k: pytest.fail("completed genome must not be reevaluated"),
    )
    resumed = EvolutionEngine(db_path=str(market))
    resumed.run(generations=1, population_size=2, resume=True, workers=1)
    assert [item["genome_hash"] for item in resumed.play_development_candidates] == expected
    assert resumed.completed_generations == 1

    invalid = _evidence().to_payload()
    invalid["candidate_count"] = -1
    with pytest.raises(ValueError, match="non-negative"):
        EvolutionEngine._evidence_from_payload(invalid)


def test_search_protocol_records_complete_auditable_why(tmp_path, monkeypatch):
    market = _active_market(tmp_path / "data")
    monkeypatch.setattr(EvolutionEngine, "_evaluate_play_adapter", lambda *a, **k: _evidence())
    EvolutionEngine(db_path=str(market)).run(
        generations=1, population_size=1, resume=True, workers=1
    )
    row = _rows(
        market.parent / "research_ledger.db",
        "SELECT protocol_json,payload_json FROM research_candidates "
        "JOIN research_evidence USING(candidate_hash)",
    )[0]
    protocol, payload = map(json.loads, row)
    family = protocol["search_family"]
    assert family["deterministic_seed"] == 20260818
    assert family["max_trials"] == family["family_size"] == 1
    assert family["search_axes"]["half_life_trade_days"] == [3.0, 5.0, 8.0]
    assert family["implementation"]["source_sha256"]
    expected = {
        "research_status": "DEVELOPMENT_CANDIDATE",
        "usage_status": "PAPER_ONLY",
        "holdout_status": "HOLDOUT_NOT_OPENED",
        "admission_status": "NOT_ADMITTED",
    }
    assert {key: payload["evolution"][key] for key in expected} == expected


def test_same_seed_and_snapshot_produce_same_genomes_and_fitness(tmp_path, monkeypatch):
    first_market = _active_market(tmp_path / "first")
    second_root = tmp_path / "second"
    second_root.mkdir()
    second_market = second_root / "alpha_miner.db"
    shutil.copy2(first_market, second_market)
    shutil.copy2(sidecar_path(first_market), sidecar_path(second_market))

    def deterministic(_engine, _storage, candidate, **_kwargs):
        value = int(candidate.parameters["execution_hash"][:6], 16) / 0xFFFFFF
        return _evidence(value)

    monkeypatch.setattr(EvolutionEngine, "_evaluate_play_adapter", deterministic)
    outputs = []
    for market in (first_market, second_market):
        engine = EvolutionEngine(db_path=str(market))
        engine.run(generations=2, population_size=3, resume=True, workers=2)
        outputs.append([
            (item["genome_hash"], item["fitness"], item["generation"])
            for item in engine.play_development_candidates
        ])
    assert outputs[0] == outputs[1]


def test_reserved_price_extremes_do_not_change_genome_fitness_or_top(tmp_path):
    normal = _active_market(tmp_path / "normal")
    extreme_root = tmp_path / "extreme"
    extreme_root.mkdir()
    extreme = extreme_root / "alpha_miner.db"
    shutil.copy2(normal, extreme)
    connection = sqlite3.connect(extreme)
    try:
        connection.execute(
            "UPDATE daily_price SET open=999999,close=999999 "
            "WHERE trade_date IN ('2026-07-10','2026-07-11','2026-07-12')"
        )
        connection.commit()
    finally:
        connection.close()
    manifest = build_manifest(extreme, published_at="2026-08-18T01:00:00.000000+00:00")
    sidecar_path(extreme).write_text(canonical_json(manifest), encoding="utf-8")

    outputs = []
    for market in (normal, extreme):
        engine = EvolutionEngine(db_path=str(market))
        engine.run(generations=1, population_size=2, resume=True, workers=1)
        outputs.append([
            (item["genome_hash"], item["fitness"])
            for item in engine.play_development_candidates
        ])
    assert outputs[0] == outputs[1]
