"""P0 contracts: generated research is PIT-bound and development-only."""

from __future__ import annotations

import hashlib
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest

from src.data.pit import (
    PITMode,
    PointInTimeError,
    PointInTimeView,
    ResearchCodeError,
    compile_compute_source,
)
from src.data.storage import Storage
from src.mining.backtester import BacktestResult, FactorBacktester
from src.mining.evolution import Candidate, EvolutionEngine
from src.mining.limit_up_evolution import (
    GENE_FEATURES,
    LimitUpEvolutionEngine,
)
from src.mining.sandbox import Sandbox


def _storage(tmp_path: Path, name: str = "pit.db") -> Storage:
    storage = Storage(str(tmp_path / name))
    storage.init_db()
    return storage


def test_point_in_time_view_hides_late_availability_and_has_no_escape_surface(tmp_path):
    storage = _storage(tmp_path)
    rows = pd.DataFrame(
        [
            {
                "stock_code": "000001",
                "trade_date": "2026-08-17",
                "close": 10.0,
            },
            {
                "stock_code": "000002",
                "trade_date": "2026-08-17",
                "close": 20.0,
            },
        ]
    )
    storage.insert(
        "daily_price",
        rows.iloc[[0]],
        snapshot_time=datetime(2026, 8, 17, 9, 30),
    )
    storage.insert(
        "daily_price",
        rows.iloc[[1]],
        snapshot_time=datetime(2026, 8, 17, 11, 0),
    )
    decision = datetime(2026, 8, 17, 10, 0)
    forward = PointInTimeView(storage, decision, PITMode.FORWARD)

    visible = forward.query(
        "daily_price",
        decision,
        where="trade_date = ?",
        params=("2026-08-17",),
    )
    assert visible["stock_code"].tolist() == ["000001"]
    assert visible.attrs["pit_mode"] == "FORWARD"
    with pytest.raises(PointInTimeError, match="晚于"):
        forward.query("daily_price", decision + timedelta(minutes=1))
    with pytest.raises(PointInTimeError, match="没有可靠"):
        forward.query("concept_mapping", decision)
    for forbidden in (
        "execute",
        "_get_conn",
        "db_path",
        "insert",
        "execute_write",
        "init_db",
        "backtest_mode",
    ):
        with pytest.raises(AttributeError):
            getattr(forward, forbidden)

    retro = PointInTimeView(storage, decision, PITMode.RETRO_DEVELOPMENT)
    retro_rows = retro.query("daily_price", decision)
    assert set(retro_rows["stock_code"]) == {"000001", "000002"}
    assert retro_rows.attrs["research_label"] == "RETRO_DEVELOPMENT_ONLY"


@pytest.mark.parametrize(
    "statement",
    [
        "return db.execute('SELECT 1')",
        "return db._get_conn()",
        "return db.execute_write('DELETE FROM x')",
        "return db.insert('x', None)",
        "db.backtest_mode = True\n    return None",
        "return db.query('daily_price', as_of, bypass_snapshot=True)",
        "return open('x')",
        "return eval('1')",
        "return getattr(db, 'execute')('SELECT 1')",
    ],
)
def test_generated_compute_ast_rejects_storage_and_language_escapes(statement):
    code = f"def compute(universe, as_of, db):\n    {statement}\n"
    with pytest.raises(ResearchCodeError):
        compile_compute_source(code)


def test_generated_compute_ast_rejects_import_in_main_and_sandbox(tmp_path):
    code = "import os\ndef compute(universe, as_of, db):\n    return None\n"
    engine = EvolutionEngine(
        db_path=str(tmp_path / "empty.db"),
        mining_log_path=str(tmp_path / "log.jsonl"),
    )
    assert engine._extract_compute_fn(code) is None
    validated = Sandbox(str(tmp_path / "empty.db")).validate(code, "malicious")
    assert "error" in validated and "禁止import" in validated["error"]


class _SpyStorage(Storage):
    def __init__(self, db_path: str):
        super().__init__(db_path)
        self.calls: list[tuple[str, tuple]] = []

    def query(self, table, as_of, where="", params=(), **kwargs):
        self.calls.append((f"query:{table}:{where}", tuple(params)))
        return super().query(table, as_of, where=where, params=params, **kwargs)

    def execute(self, sql: str, params: tuple = ()):
        self.calls.append((sql, tuple(params)))
        return super().execute(sql, params)


def test_factor_backtester_never_reads_reserved_holdout_values(tmp_path):
    storage = _storage(tmp_path, "development.db")
    dates = [value.strftime("%Y-%m-%d") for value in pd.bdate_range("2026-04-01", periods=60)]
    rows = []
    for day_index, trade_date in enumerate(dates):
        for stock_index in range(15):
            rows.append(
                {
                    "stock_code": f"{stock_index + 1:06d}",
                    "trade_date": trade_date,
                    "open": 10 + stock_index,
                    "high": 11 + stock_index,
                    "low": 9 + stock_index,
                    "close": 10 + stock_index + day_index * (stock_index + 1) / 100,
                    "volume": 1000 + stock_index,
                    "amount": 100_000 + stock_index,
                }
            )
    storage.insert(
        "daily_price",
        pd.DataFrame(rows),
        snapshot_time=datetime(2026, 8, 1),
    )
    spy = _SpyStorage(storage.db_path)

    def compute(universe, as_of, db):
        frame = db.query(
            "daily_price",
            as_of,
            where="trade_date = ?",
            params=(as_of.strftime("%Y-%m-%d"),),
        )
        return frame.set_index("stock_code")["close"].reindex(universe)

    first = FactorBacktester(spy).run(compute, lookback_days=60)
    holdout_dates = set(first.reserved_holdout_dates)
    assert len(holdout_dates) == 12 and not first.holdout_opened
    for sql, params in spy.calls:
        if "SELECT DISTINCT trade_date" in sql:
            continue
        assert not any(str(value)[:10] in holdout_dates for value in params)

    placeholders = ",".join("?" for _ in holdout_dates)
    storage.execute_write(
        f"UPDATE daily_price SET close = close * 1000 WHERE trade_date IN ({placeholders})",
        tuple(sorted(holdout_dates)),
    )
    second = FactorBacktester(_SpyStorage(storage.db_path)).run(compute, lookback_days=60)
    assert first.to_dict() == second.to_dict()


def test_generic_evolution_marks_development_passed_but_never_accepted(tmp_path, monkeypatch):
    engine = EvolutionEngine(
        db_path=str(tmp_path / "empty.db"),
        mining_log_path=str(tmp_path / "mining.jsonl"),
    )
    candidate = Candidate(
        "strong_dev",
        "test",
        {},
        code="def compute(universe, as_of, db):\n    return pd.Series(1.0, index=universe)\n",
    )
    monkeypatch.setattr(engine.sandbox, "validate", lambda *args: {"validated": True})
    monkeypatch.setattr(
        FactorBacktester,
        "run",
        lambda *args, **kwargs: BacktestResult(
            "strong_dev", ic_mean=0.1, icir=1.0, win_rate=0.8, total_days=40
        ),
    )

    engine._evaluate(candidate)

    assert candidate.development_passed is True
    assert candidate.accepted is False
    assert not hasattr(engine, "candidate_pool")
    assert not hasattr(engine, "_stage_candidate")
    assert not (tmp_path / "pool.jsonl").exists()


def _limit_events(extreme_holdout: bool) -> tuple[pd.DataFrame, dict]:
    dates = [f"2026-07-{day:02d}" for day in range(1, 11)]
    rows = []
    for date_index, signal_date in enumerate(dates):
        for stock_index in range(4):
            holdout_return = 1_000_000.0 if extreme_holdout and date_index >= 8 else 1.0
            rows.append(
                {
                    "signal_date": signal_date,
                    "stock_code": f"{stock_index + 1:06d}",
                    "board_count": 1,
                    "open_count": 1,
                    "entry_gap": 0.0,
                    "unbuyable": False,
                    "return_1": holdout_return + stock_index / 10,
                    "drawdown_1": -1.0,
                    **{feature: stock_index / 4 for feature in GENE_FEATURES},
                }
            )
    development = dates[:8]
    summary = {
        "data_ready": True,
        "signal_dates": len(development),
        "development_date_values": development,
        "reserved_holdout_date_values": dates[8:],
        "development_date_hash": hashlib.sha256(
            "|".join(development).encode("utf-8")
        ).hexdigest(),
        "active_features": list(GENE_FEATURES),
        "holdout_opened": False,
    }
    return pd.DataFrame(rows), summary


def test_limit_up_holdout_extremes_do_not_change_fitness_best_or_hash(tmp_path, monkeypatch):
    normal_events, summary = _limit_events(False)
    extreme_events, extreme_summary = _limit_events(True)

    def run(events, state_name):
        engine = LimitUpEvolutionEngine(
            db_path=str(tmp_path / "limit.db"),
            state_path=str(tmp_path / state_name),
            min_market_rows=1,
            min_signal_dates=1,
        )
        monkeypatch.setattr(
            engine,
            "build_event_dataset",
            lambda: (events.copy(), dict(summary)),
        )
        return engine.run(generations=1, population_size=4)

    first = run(normal_events, "normal.json")
    summary = extreme_summary
    second = run(extreme_events, "extreme.json")

    assert first.best is not None and second.best is not None
    assert first.best.genome.name == second.best.genome.name
    assert first.best.fitness == second.best.fitness
    assert first.dataset_summary["development_date_hash"] == second.dataset_summary[
        "development_date_hash"
    ]
    assert all(not item.accepted for item in first.evaluations + second.evaluations)
    assert not hasattr(first.best, "test")
