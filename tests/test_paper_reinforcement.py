"""Bounded FORWARD PAPER reinforcement and traceability contracts."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import date, timedelta
from types import SimpleNamespace

import pytest

import cli.mine as mine_cli
from src.data.storage import Storage
from src.mining.behavior_state import BehaviorStateSpec
from src.mining.evolution import EvolutionEngine
from src.mining.experiments import (
    AttentionReaccelerationRule,
    DevelopmentEvidence,
    PlayGenome,
)
from src.mining.playbook import PlayCard, freeze_forward_plan, save_play_card
from src.mining.plays import (
    ATTENTION_REACCELERATION_PLAY_ID,
    _paper_feedback,
    empty_paper_feedback,
    load_attention_paper_feedback,
)
from src.mining.research_ledger import (
    CandidateSpec,
    LedgerConflict,
    LedgerValidationError,
    ResearchLedger,
)
from tests.test_unified_experiment import _active_market


def _genome(**rule_changes) -> PlayGenome:
    base_rule = AttentionReaccelerationRule()
    rule_changes.setdefault(
        "allowed_state_domains", tuple(sorted(base_rule.allowed_state_domains))
    )
    return PlayGenome(
        "limited_attention_salience",
        "salience_volume_reacceleration",
        "THEORY_DERIVED",
        BehaviorStateSpec(),
        replace(base_rule, **rule_changes),
    )


def _evidence() -> DevelopmentEvidence:
    return DevelopmentEvidence(
        "2026-06-01", "2026-07-01", 10, 10, 10, 0, 10, 6,
        0.3, 0.6, 1.2, 2.0, 20.0, {}, ("SYNTHETIC",),
    )


def _ledger_candidate(ledger: ResearchLedger, suffix: str, execution_hash="a" * 64):
    return ledger.freeze_candidate(CandidateSpec(
        candidate_name=f"paper-{suffix}",
        experiment_type="EVOLVED_EXECUTABLE_PLAY",
        code_text=f"implementation-{suffix}",
        parameters={"suffix": suffix, "execution_hash": execution_hash},
        data_manifest={"partition": suffix},
        cost_model={"round_trip_bps": 20},
        protocol={"holdout_scope_hash": suffix[0] * 64},
    ))


def _consumption_payload(execution_hash: str, start_day: int = 1) -> dict:
    facts = [
        {
            "signal_trade_date": f"2026-06-{day:02d}",
            "plan_hash": f"{day:064x}",
            "results": [{"stock_code": "000001", "status": "COMPLETED",
                         "net_return_pct": 1.0}],
        }
        for day in range(start_day, start_day + 5)
    ]
    return {"evolution": {"forward_feedback": _paper_feedback(
        execution_hash, "2026-07-31", facts
    )}}


def _rehash_feedback(feedback):
    keys = ("execution_hash", "plan_hashes", "content_hash", "window_start",
            "window_end", "completed_signal_days")
    encoded = json.dumps({key: feedback[key] for key in keys}, sort_keys=True,
                         ensure_ascii=False, separators=(",", ":"), allow_nan=False)
    feedback["consumption_receipt_hash"] = hashlib.sha256(encoded.encode()).hexdigest()


def _save_card(
    storage: Storage,
    signal_date: str,
    genome: PlayGenome,
    outcomes: list[tuple[str, float | None]],
) -> PlayCard:
    generated_at = f"{signal_date}T16:00:00+08:00"
    envelope = {
        "play_id": ATTENTION_REACCELERATION_PLAY_ID,
        "play_name": "注意力再加速",
        "behavior_logic": "冻结行为状态后做FORWARD PAPER。",
        "signal_trade_date": signal_date,
        "generated_at": generated_at,
        "trigger_rule": "D+1开盘满足冻结条件时模拟成交。",
        "abandon_rule": "缺数据、不可成交或越界时保留负证据。",
        "exit_rule": "固定D+3开盘退出，扣20bp。",
        "admission_status": "NOT_ADMITTED",
    }
    candidates = []
    for index, (status, net_return) in enumerate(outcomes):
        code = f"000{index + 1:03d}"
        candidate = {
            "stock_code": code,
            "paper_status": status,
            "lifecycle_events": [
                {"status": "PLANNED", "recorded_at": generated_at, "reason": "frozen"},
                {"status": status, "recorded_at": generated_at, "reason": "settled"},
            ],
        }
        if status == "COMPLETED":
            candidate.update(
                entry_trade_date=signal_date,
                exit_trade_date=signal_date,
                entry_price=10.0,
                exit_price=10.0 * (1 + (float(net_return) + 0.2) / 100),
                total_cost_bps=20.0,
                net_return_pct=net_return,
            )
        candidates.append(candidate)
    plan, plan_hash = freeze_forward_plan(
        envelope,
        candidates,
        ("stock_code",),
        {
            "behavior_state_spec": genome.behavior_spec.to_payload(),
            "behavior_state_spec_hash": genome.behavior_spec.spec_hash,
            "rule": genome.rule.to_payload(),
            "rule_hash": genome.rule.rule_hash,
            "planned_entry_date": signal_date,
            "planned_exit_date": signal_date,
        },
    )
    card = PlayCard(
        **envelope,
        candidates=candidates,
        historical_evidence={"forward_plan": plan, "forward_plan_hash": plan_hash},
        paper_status="COMPLETED",
    )
    save_play_card(storage, card)
    return card


def test_feedback_exact_match_signal_day_weighting_negative_and_cap(tmp_path):
    storage = Storage(str(tmp_path / "market.db"))
    storage.init_db()
    genome = _genome()
    dates = [f"2026-08-{day:02d}" for day in range(10, 15)]
    _save_card(storage, dates[0], genome, [("COMPLETED", 10.0)] * 3)
    for signal_date in dates[1:]:
        _save_card(storage, signal_date, genome, [("COMPLETED", -1.0)])

    feedback = load_attention_paper_feedback(storage, dates[-1])[genome.execution_hash]
    assert feedback["completed_signal_days"] == 5
    assert feedback["status_counts"]["COMPLETED"] == 7
    assert feedback["mean_net_return_pct"] == pytest.approx(1.2)
    assert feedback["adjustment"] == pytest.approx(0.048)
    assert len(feedback["plan_hashes"]) == 5

    capped_storage = Storage(str(tmp_path / "capped.db"))
    capped_storage.init_db()
    for signal_date in dates:
        _save_card(capped_storage, signal_date, genome, [("COMPLETED", 100.0)])
    assert load_attention_paper_feedback(
        capped_storage, dates[-1]
    )[genome.execution_hash]["adjustment"] == 0.05


def test_incomplete_wrong_genome_and_damaged_cards_never_reinforce(tmp_path):
    storage = Storage(str(tmp_path / "market.db"))
    storage.init_db()
    genome = _genome()
    other = _genome(min_total_attention=1.0)
    for day in range(10, 14):
        _save_card(storage, f"2026-08-{day:02d}", genome, [("COMPLETED", 100.0)])
    _save_card(storage, "2026-08-14", other, [("COMPLETED", 100.0)])
    feedback = load_attention_paper_feedback(storage, "2026-08-14")
    assert feedback[genome.execution_hash]["status"] == "INSUFFICIENT"
    assert feedback[genome.execution_hash]["adjustment"] == 0
    assert other.execution_hash in feedback and genome.execution_hash != other.execution_hash
    assert empty_paper_feedback("a" * 64, "2026-08-14")["adjustment"] == 0

    storage.execute_write(
        "UPDATE play_cards SET historical_evidence_json='{}' WHERE signal_trade_date=?",
        ("2026-08-14",),
    )
    with pytest.raises((KeyError, ValueError)):
        load_attention_paper_feedback(storage, "2026-08-14")


def test_unsettled_and_execution_failures_are_counted_without_fake_reward(tmp_path):
    storage = Storage(str(tmp_path / "market.db"))
    storage.init_db()
    genome = _genome()
    _save_card(
        storage,
        "2026-08-14",
        genome,
        [("NOT_TRIGGERED", None), ("UNFILLED", None), ("UNFILLED", None),
         ("INVALID", None)],
    )
    feedback = load_attention_paper_feedback(storage, "2026-08-14")[genome.execution_hash]
    assert feedback["status_counts"] == {
        "COMPLETED": 0, "INVALID": 1, "NOT_TRIGGERED": 1, "UNFILLED": 2
    }
    assert feedback["status_signal_days"] == {
        "COMPLETED": 0, "INVALID": 1, "NOT_TRIGGERED": 0, "UNFILLED": 0
    }
    assert feedback["completed_signal_days"] == 0
    assert feedback["mean_net_return_pct"] is None and feedback["adjustment"] == 0


@pytest.mark.parametrize(
    ("signal_days", "expected_cap"), [(4, 0.0), (5, 0.05), (20, 0.10), (40, 0.20)]
)
def test_feedback_uses_preregistered_signal_day_caps(signal_days, expected_cap):
    start = date(2026, 1, 1)
    facts = [
        {
            "signal_trade_date": (start + timedelta(days=index)).isoformat(),
            "plan_hash": f"{index + 1:064x}",
            "results": [{"stock_code": "000001", "status": "COMPLETED",
                         "net_return_pct": 100.0}],
        }
        for index in range(signal_days)
    ]
    feedback = _paper_feedback("a" * 64, "2026-12-31", facts)
    assert feedback["adjustment"] == expected_cap
    assert feedback["status"] == (
        "INSUFFICIENT" if signal_days < 5 else "ADAPTIVE_DEVELOPMENT_FEEDBACK"
    )


def test_under_five_terminal_days_do_not_change_failure_family():
    evidence = replace(
        _evidence(), signal_days=40, candidate_count=40, filled_count=40,
        completed_signal_days=40,
    )
    facts = [
        {
            "signal_trade_date": f"2026-08-{day:02d}",
            "plan_hash": f"{day:064x}",
            "results": [{"stock_code": "000001", "status": "INVALID"}],
        }
        for day in range(10, 14)
    ]
    feedback = _paper_feedback("a" * 64, "2026-08-31", facts)
    family, _ = EvolutionEngine._failure_family(evidence, feedback)
    assert family == "WEAK_RISK_ADJUSTED_RETURN"


def test_lifecycle_after_cutoff_fails_closed(tmp_path):
    storage = Storage(str(tmp_path / "market.db"))
    storage.init_db()
    _save_card(storage, "2026-08-14", _genome(), [("COMPLETED", 1.0)])
    row = storage.execute("SELECT candidates_json FROM play_cards")[0]
    candidates = json.loads(row["candidates_json"])
    candidates[0]["lifecycle_events"][-1]["recorded_at"] = "2026-08-15T09:31:00+08:00"
    storage.execute_write(
        "UPDATE play_cards SET candidates_json=?", (json.dumps(candidates),)
    )
    with pytest.raises(ValueError, match="after cutoff"):
        load_attention_paper_feedback(storage, "2026-08-14")


@pytest.mark.parametrize("corruption", ["duplicate_code", "wrong_cost"])
def test_duplicate_stock_or_wrong_cost_fails_closed(tmp_path, corruption):
    storage = Storage(str(tmp_path / "market.db"))
    storage.init_db()
    _save_card(
        storage, "2026-08-14", _genome(),
        [("COMPLETED", 1.0), ("COMPLETED", -2.0)],
    )
    candidates = json.loads(storage.execute("SELECT candidates_json FROM play_cards")[0]["candidates_json"])
    if corruption == "duplicate_code":
        candidates[1]["stock_code"] = candidates[0]["stock_code"]
    else:
        candidates[0]["total_cost_bps"] = 19.0
    storage.execute_write(
        "UPDATE play_cards SET candidates_json=?", (json.dumps(candidates),)
    )
    with pytest.raises(ValueError):
        load_attention_paper_feedback(storage, "2026-08-14")


@pytest.mark.parametrize("tampered_return", [9.0, -9.0])
def test_tampered_completed_return_cannot_change_fitness(tmp_path, tampered_return):
    storage = Storage(str(tmp_path / "market.db"))
    storage.init_db()
    _save_card(storage, "2026-08-14", _genome(), [("COMPLETED", 1.0)])
    candidates = json.loads(storage.execute("SELECT candidates_json FROM play_cards")[0]["candidates_json"])
    candidates[0]["net_return_pct"] = tampered_return
    storage.execute_write(
        "UPDATE play_cards SET candidates_json=?", (json.dumps(candidates),)
    )
    with pytest.raises(ValueError, match="differs from frozen prices"):
        load_attention_paper_feedback(storage, "2026-08-14")


def test_resume_rejects_feedback_manifest_drift(tmp_path, monkeypatch):
    market = _active_market(tmp_path / "data")
    first = _paper_feedback("a" * 64, "2026-07-12", [])
    monkeypatch.setattr(
        "src.mining.evolution.load_attention_paper_feedback",
        lambda _storage, cutoff, _consumed: {
            first["execution_hash"]: {**first, "cutoff_trade_date": cutoff}
        },
    )
    monkeypatch.setattr(EvolutionEngine, "_evaluate_play_adapter", lambda *_a, **_k: _evidence())
    EvolutionEngine(db_path=str(market)).run(1, 1, True, 1)
    changed = {**first, "content_hash": "c" * 64}
    monkeypatch.setattr(
        "src.mining.evolution.load_attention_paper_feedback",
        lambda _storage, cutoff, _consumed: {
            changed["execution_hash"]: {**changed, "cutoff_trade_date": cutoff}
        },
    )
    with pytest.raises(ValueError, match="protocol differs"):
        EvolutionEngine(db_path=str(market)).run(1, 1, True, 1)


@pytest.mark.parametrize("corruption", ["receipt_hash", "execution_hash"])
def test_resume_revalidates_stored_feedback_before_parent_ranking(
    tmp_path, monkeypatch, corruption
):
    market = _active_market(tmp_path / "data")
    genome = _genome()
    monkeypatch.setattr(EvolutionEngine, "_knowledge_play_genomes", lambda _self: [genome])
    facts = [{"signal_trade_date": f"2026-06-{day:02d}", "plan_hash": f"{day:064x}",
              "results": [{"stock_code": "000001", "status": "COMPLETED",
                           "net_return_pct": 1.0}]} for day in range(1, 6)]
    monkeypatch.setattr(
        "src.mining.evolution.load_attention_paper_feedback",
        lambda _storage, cutoff, _consumed: {
            genome.execution_hash: _paper_feedback(genome.execution_hash, cutoff, facts)
        },
    )
    monkeypatch.setattr(EvolutionEngine, "_evaluate_play_adapter", lambda *_a, **_k: _evidence())
    EvolutionEngine(db_path=str(market)).run(1, 1, True, 1)
    ledger_path = market.parent / "research_ledger.db"
    with sqlite3.connect(ledger_path) as connection:
        event_count = connection.execute("SELECT COUNT(*) FROM research_evidence").fetchone()[0]
        row = connection.execute(
            "SELECT event_id,payload_json FROM research_evidence WHERE event_type='DEVELOPMENT_RESULT'"
        ).fetchone()
        payload = json.loads(row[1])
        feedback = payload["evolution"]["forward_feedback"]
        if corruption == "receipt_hash":
            feedback["consumption_receipt_hash"] = "c" * 64
        else:
            feedback["execution_hash"] = "f" * 64
            _rehash_feedback(feedback)
        encoded = json.dumps(payload, sort_keys=True, ensure_ascii=False,
                             separators=(",", ":"), allow_nan=False)
        connection.execute("DROP TRIGGER trg_research_evidence_no_update")
        connection.execute(
            "UPDATE research_evidence SET payload_json=?,payload_hash=? WHERE event_id=?",
            (encoded, hashlib.sha256(encoded.encode()).hexdigest(), row[0]),
        )
        connection.commit()
    ledger = ResearchLedger(market.parent)
    ledger.init_db()
    bound = ledger.bind_active_market()
    error = LedgerValidationError if corruption == "receipt_hash" else LedgerConflict
    with pytest.raises(error):
        ledger.load_development_history("EVOLVED_EXECUTABLE_PLAY", bound.source_snapshot_sha256)
    parent_calls = []
    monkeypatch.setattr(EvolutionEngine, "_play_parents", lambda *_a: parent_calls.append(1))
    with pytest.raises(error):
        EvolutionEngine(db_path=str(market)).run(1, 1, True, 1)
    assert parent_calls == []
    with sqlite3.connect(ledger_path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM research_evidence").fetchone()[0] == event_count


def test_ledger_atomically_rejects_cross_family_plan_reuse_and_backwards_window(tmp_path):
    market = _active_market(tmp_path / "data")
    ledger = ResearchLedger(market.parent)
    ledger.init_db()
    ledger.bind_active_market()
    execution_hash = "a" * 64
    first = _ledger_candidate(ledger, "a-first")
    second = _ledger_candidate(ledger, "b-second")
    payload = _consumption_payload(execution_hash, start_day=10)
    event = ledger.append_development_result(first.candidate_hash, payload, "consume-first")
    with pytest.raises(LedgerConflict, match="already consumed"):
        ledger.append_development_result(second.candidate_hash, payload, "consume-second")
    receipts = ledger.load_paper_feedback_consumption()
    assert receipts[execution_hash]["event_ids"] == (event.event_id,)

    backwards = _ledger_candidate(ledger, "c-backwards")
    earlier = _consumption_payload(execution_hash, start_day=1)
    with pytest.raises(LedgerConflict, match="window overlaps or moves backwards"):
        ledger.append_development_result(backwards.candidate_hash, earlier, "consume-backwards")


def test_ledger_rejects_fake_pending_and_cross_candidate_execution(tmp_path):
    market = _active_market(tmp_path / "data")
    ledger = ResearchLedger(market.parent)
    ledger.init_db()
    bound = ledger.bind_active_market()
    candidate = _ledger_candidate(ledger, "a-pending")
    pending = empty_paper_feedback("a" * 64, "2026-07-31")
    pending.update(completed_signal_days=5, adjustment=0.05,
                   status="ADAPTIVE_DEVELOPMENT_FEEDBACK")
    _rehash_feedback(pending)
    with pytest.raises(LedgerValidationError, match="pending PAPER feedback"):
        ledger.append_development_result(
            candidate.candidate_hash, {"evolution": {"forward_feedback": pending}}, "fake-pending"
        )
    other = _ledger_candidate(ledger, "b-identity")
    with pytest.raises(LedgerConflict, match="differs from frozen candidate"):
        ledger.append_development_result(
            other.candidate_hash, _consumption_payload("b" * 64), "wrong-execution"
        )
    history = ledger.load_development_history(
        "EVOLVED_EXECUTABLE_PLAY", bound.source_snapshot_sha256
    )
    assert history and all(event is None for _candidate, event in history)
    assert ledger.load_paper_feedback_consumption() == {}


def test_concurrent_feedback_consumption_grants_exactly_one_writer(tmp_path):
    market = _active_market(tmp_path / "data")
    ledger = ResearchLedger(market.parent)
    ledger.init_db()
    ledger.bind_active_market()
    candidates = [
        _ledger_candidate(ledger, f"{value}-parallel", "d" * 64)
        for value in ("a", "b")
    ]
    payload = _consumption_payload("d" * 64)

    def append(index):
        return ledger.append_development_result(
            candidates[index].candidate_hash, payload, f"parallel-{index}"
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(append, index) for index in range(2)]
    outcomes = []
    for future in futures:
        try:
            outcomes.append(future.result())
        except LedgerConflict:
            outcomes.append("CONFLICT")
    assert sum(item == "CONFLICT" for item in outcomes) == 1


def test_active_feedback_is_applied_once_and_labeled_adaptive(tmp_path, monkeypatch):
    market = _active_market(tmp_path / "data")
    genome = _genome()
    monkeypatch.setattr(EvolutionEngine, "_knowledge_play_genomes", lambda _self: [genome])

    def feedback_for_cutoff(_storage, cutoff, _consumed):
        facts = [
            {
                "signal_trade_date": f"2026-06-{day:02d}",
                "plan_hash": f"{day:064x}",
                "results": [{"stock_code": "000001", "status": "COMPLETED",
                             "net_return_pct": 100.0}],
            }
            for day in range(1, 6)
        ]
        return {genome.execution_hash: _paper_feedback(genome.execution_hash, cutoff, facts)}

    monkeypatch.setattr("src.mining.evolution.load_attention_paper_feedback", feedback_for_cutoff)
    monkeypatch.setattr(EvolutionEngine, "_evaluate_play_adapter", lambda *_a, **_k: _evidence())
    engine = EvolutionEngine(db_path=str(market))
    engine.run(1, 1, True, 1)
    item = engine.play_development_candidates[0]
    assert item["forward_feedback"]["status"] == "ADAPTIVE_DEVELOPMENT_FEEDBACK"
    assert item["forward_feedback"]["adjustment"] == 0.05
    assert item["fitness"] == pytest.approx(item["base_development_fitness"] + 0.05)
    assert item["why"]["feedback_scope"] == "EXECUTION_ONLY_NOT_THEORY_OR_LINEAGE_EVIDENCE"
    monkeypatch.setattr(
        EvolutionEngine, "_evaluate_play_adapter",
        lambda *_a, **_k: pytest.fail("same-family resume must not consume or evaluate twice"),
    )
    resumed = EvolutionEngine(db_path=str(market))
    resumed.run(1, 1, True, 1)
    assert resumed.play_development_candidates[0]["fitness"] == item["fitness"]


def test_no_paper_is_deterministic_and_why_is_complete(tmp_path, monkeypatch):
    market = _active_market(tmp_path / "data")
    monkeypatch.setattr(EvolutionEngine, "_evaluate_play_adapter", lambda *_a, **_k: _evidence())
    engine = EvolutionEngine(db_path=str(market))
    engine.run(1, 1, True, 1)
    item = engine.play_development_candidates[0]
    assert item["forward_feedback"]["status"] == "INSUFFICIENT"
    assert item["forward_feedback"]["adjustment"] == 0
    assert item["fitness"] == item["base_development_fitness"]
    assert set(item["why"]) >= {
        "theory_provenance", "state_domains", "thresholds", "parent_candidate_hashes",
        "mutation_reason", "failure_family", "dataset_snapshot_hash", "partition_hash",
        "search_family_hash", "development_metrics", "forward_feedback",
        "feedback_scope", "eliminated_reasons",
    }
    assert item["research_status"] == "DEVELOPMENT_CANDIDATE"
    assert item["holdout_status"] == "HOLDOUT_NOT_OPENED"
    assert item["admission_status"] == "NOT_ADMITTED"


def test_mine_output_limits_candidates_and_explains_why(monkeypatch, capsys):
    class FakeEngine:
        completed_generations = 1
        play_development_candidates = [
            {
                "execution_hash": str(index) * 64,
                "fitness": 0.1,
                "forward_feedback": {"status": "INSUFFICIENT", "adjustment": 0.0},
                "why": {"state_domains": ["recent_limit_memory"]},
                "mutation_reason": "KNOWLEDGE_SEED",
                "failure_family": "SMALL_SAMPLE",
            }
            for index in range(4)
        ]

        def __init__(self, **_kwargs):
            pass

        def run(self, **_kwargs):
            return [_evidence()]

    monkeypatch.setattr(mine_cli, "_build_llm_client", lambda: (None, None))
    monkeypatch.setattr(mine_cli, "EvolutionEngine", FakeEngine)
    mine_cli.cmd_evolve(SimpleNamespace(
        db="unused.db", log="unused.jsonl", generations=1, population=4, workers=1
    ))
    output = capsys.readouterr().out
    assert output.count("为什么:") == 3
    assert "forward_feedback=INSUFFICIENT/+0.0000" in output
    assert "3333333333" not in output
