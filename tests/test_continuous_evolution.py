"""持续进化闭环的回归测试。"""

import json
from datetime import datetime

import pandas as pd

from src.data.pit import PITMode, PointInTimeView
from src.data.storage import Storage
from src.mining.evolution import Candidate, EvolutionEngine


def _write_knowledge(path):
    path.write_text(
        """
theories:
  - id: test_theory
    testable_predictions:
      - id: seed_factor
        prediction: test signal
        factor_type: conditional
        conditions:
          - table: daily_price
            column: volume
            operator: ">"
            value: 100
""",
        encoding="utf-8",
    )


def _fake_rejected_evaluation(candidate):
    candidate.code = """import pandas as pd
def compute(universe, as_of, db):
    return pd.Series(1.0, index=universe)
"""
    candidate.evaluation = {
        "ic_mean": 0.02,
        "icir": 0.1,
        "win_rate": 0.4,
        "sample_per_day": 20,
        "total_days": 20,
        "ic_series": [],
    }
    candidate.accepted = False


def test_rejected_candidate_becomes_next_generation_and_resume_continues(tmp_path, monkeypatch):
    kb_path = tmp_path / "theories.yaml"
    log_path = tmp_path / "mining_log.jsonl"
    state_path = tmp_path / "state.json"
    db_path = tmp_path / "test.db"
    _write_knowledge(kb_path)
    db = Storage(str(db_path))
    db.init_db()

    engine = EvolutionEngine(
        db_path=str(db_path),
        knowledge_path=str(kb_path),
        mining_log_path=str(log_path),
        state_path=str(state_path),
    )
    monkeypatch.setattr(engine, "_evaluate", _fake_rejected_evaluation)
    engine.run(generations=1, population_size=2, resume=False)

    state = json.loads(state_path.read_text(encoding="utf-8"))
    assert state["completed_generations"] == 1
    assert state["frontier"], "失败诊断没有产生下一代"
    assert all(item["source"] == "mutation" for item in state["frontier"])

    resumed = EvolutionEngine(
        db_path=str(db_path),
        knowledge_path=str(kb_path),
        mining_log_path=str(log_path),
        state_path=str(state_path),
    )
    evaluated_names = []

    def record_evaluation(candidate):
        evaluated_names.append(candidate.name)
        _fake_rejected_evaluation(candidate)

    monkeypatch.setattr(resumed, "_evaluate", record_evaluation)
    resumed.run(generations=1, population_size=2, resume=True)

    assert resumed.completed_generations == 2
    assert evaluated_names
    assert "seed_factor" not in evaluated_names


def test_empty_frontier_restarts_from_historical_failure(tmp_path, monkeypatch):
    kb_path = tmp_path / "theories.yaml"
    log_path = tmp_path / "mining_log.jsonl"
    state_path = tmp_path / "state.json"
    db_path = tmp_path / "test.db"
    _write_knowledge(kb_path)
    Storage(str(db_path)).init_db()

    signature_engine = EvolutionEngine(
        db_path=str(db_path),
        knowledge_path=str(kb_path),
        mining_log_path=str(log_path),
        state_path=str(state_path),
    )
    historical = signature_engine._generate_from_knowledge()[0]
    _fake_rejected_evaluation(historical)
    historical.generation = 2
    log_path.write_text(json.dumps(historical.to_dict()) + "\n", encoding="utf-8")

    seed_signature = signature_engine._candidate_signature(historical)
    state_path.write_text(json.dumps({
        "completed_generations": 2,
        "seen_signatures": [seed_signature],
        "accepted": [],
        "frontier": [],
    }), encoding="utf-8")

    engine = EvolutionEngine(
        db_path=str(db_path),
        knowledge_path=str(kb_path),
        mining_log_path=str(log_path),
        state_path=str(state_path),
    )
    evaluated = []

    def record(candidate):
        evaluated.append(candidate)
        _fake_rejected_evaluation(candidate)

    monkeypatch.setattr(engine, "_evaluate", record)
    engine.run(generations=1, population_size=2)

    assert engine.completed_generations == 3
    assert evaluated
    assert all(candidate.source in {"mutation", "restart"} for candidate in evaluated)
    assert all(candidate.name != "seed_factor" for candidate in evaluated)


def test_conditional_template_compares_against_configured_threshold(tmp_path):
    db = Storage(str(tmp_path / "test.db"))
    db.init_db()
    snapshot = datetime(2024, 6, 14, 10)
    db.insert("daily_price", pd.DataFrame([
        {
            "stock_code": "000001", "trade_date": "2024-06-14",
            "open": 10, "high": 10, "low": 10, "close": 10,
            "volume": 50, "amount": 500, "turnover_rate": 1,
        },
        {
            "stock_code": "000002", "trade_date": "2024-06-14",
            "open": 10, "high": 10, "low": 10, "close": 10,
            "volume": 150, "amount": 1500, "turnover_rate": 1,
        },
    ]), snapshot_time=snapshot)

    engine = EvolutionEngine(
        db_path=str(tmp_path / "test.db"),
        mining_log_path=str(tmp_path / "log.jsonl"),
    )
    candidate = Candidate("threshold", "test", {
        "name": "threshold",
        "prediction": "volume threshold",
        "factor_type": "conditional",
        "conditions": [{
            "table": "daily_price",
            "column": "volume",
            "operator": ">",
            "value": 100,
        }],
    })
    code = engine._template_construct(candidate)
    compute = engine._extract_compute_fn(code)

    decision = datetime(2024, 6, 14, 15)
    pit = PointInTimeView(db, decision, PITMode.FORWARD)
    values = compute(["000001", "000002"], decision, pit)

    assert values["000001"] == 0.0
    assert values["000002"] == 1.0


def test_reverse_mutation_changes_executable_output(tmp_path):
    engine = EvolutionEngine(
        db_path=str(tmp_path / "test.db"),
        mining_log_path=str(tmp_path / "log.jsonl"),
    )
    parent_code = """def compute(universe, as_of, db):
    return pd.Series({code: i + 1 for i, code in enumerate(universe)}, dtype=float)
"""
    code = engine._wrap_mutation_code(parent_code, {"mutation_type": "reverse_direction"})
    compute = engine._extract_compute_fn(code)

    values = compute(["000001", "000002"], datetime(2024, 6, 14), None)

    assert values.tolist() == [-1.0, -2.0]
