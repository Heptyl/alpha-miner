"""策略进化器测试 — 真实临时数据库，无 mock。"""

import os
import tempfile
from datetime import datetime

import pandas as pd
import pytest

from src.data.storage import Storage
from src.strategy.evolver import StrategyEvolver, EvolveResult
from src.strategy.schema import (
    EntryRule, ExitRule, PositionRule, Strategy, StrategyReport,
)


@pytest.fixture
def db():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    store = Storage(db_path)
    store.init_db()
    _seed_data(store)
    yield store
    os.unlink(db_path)


def _seed_data(db: Storage):
    """3个交易日、3只股票的基础数据。"""
    import sqlite3
    conn = sqlite3.connect(db.db_path)
    snap = "2026-04-14 20:59:59"
    dates = ["2026-04-14", "2026-04-15", "2026-04-16"]
    stocks = {"000001": 10.0, "000002": 20.0, "000003": 15.0}

    for date in dates:
        idx = dates.index(date)
        for code, base in stocks.items():
            close = round(base * (1 + 0.01 * idx), 2)
            open_p = round(base * (1 + 0.005 * idx), 2)
            pre = round(base * (1 + 0.01 * (idx - 1)), 2) if idx > 0 else base
            conn.execute(
                "INSERT INTO daily_price (stock_code,trade_date,open,high,low,close,pre_close,volume,amount,turnover_rate,snapshot_time) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (code, date, open_p, round(close*1.02,2), round(close*0.98,2),
                 close, pre, 1000000, 10000000, 5.0, snap)
            )
        # zt_pool
        for code in ["000001", "000002"]:
            cb = 2 if code == "000001" and idx >= 1 else (1 if idx == 0 else 0)
            conn.execute(
                "INSERT INTO zt_pool (stock_code,trade_date,consecutive_zt,amount,snapshot_time) VALUES (?,?,?,?,?)",
                (code, date, cb, 50000000, snap)
            )
        # factors
        for code in stocks:
            cb = 2 if code == "000001" and idx >= 1 else (1 if code in ("000001","000002") and idx == 0 else 0)
            conn.execute(
                "INSERT INTO factor_values (factor_name,stock_code,trade_date,factor_value,snapshot_time) VALUES (?,?,?,?,?)",
                ("consecutive_board", code, date, float(cb), snap)
            )
            conn.execute(
                "INSERT INTO factor_values (factor_name,stock_code,trade_date,factor_value,snapshot_time) VALUES (?,?,?,?,?)",
                ("turnover_rank", code, date, 0.5, snap)
            )
    conn.commit()
    conn.close()


def _make_base() -> Strategy:
    return Strategy(
        name="base_test",
        description="evolver test",
        entry=EntryRule(conditions=[{"factor": "consecutive_board", "op": ">=", "value": 1}]),
        exit=ExitRule(take_profit_pct=5.0, stop_loss_pct=-3.0, max_hold_days=3),
        position=PositionRule(single_position_pct=20.0, max_holdings=3),
    )


class TestEvolveBasic:
    def test_returns_evolve_result(self, db):
        evolver = StrategyEvolver(db)
        result = evolver.evolve(_make_base(), "2026-04-14", "2026-04-16")
        assert isinstance(result, EvolveResult)
        assert isinstance(result.best_strategy, Strategy)
        assert isinstance(result.best_report, StrategyReport)

    def test_all_variants_counted(self, db):
        grid = {"take_profit_pct": [3.0, 5.0], "stop_loss_pct": [-2.0, -3.0]}
        evolver = StrategyEvolver(db)
        result = evolver.evolve(_make_base(), "2026-04-14", "2026-04-16", param_grid=grid)
        assert result.all_variants == 4  # 2 x 2

    def test_max_variants_limit(self, db):
        grid = {"take_profit_pct": [3.0, 5.0, 7.0], "stop_loss_pct": [-2.0, -3.0, -5.0]}
        evolver = StrategyEvolver(db)
        result = evolver.evolve(
            _make_base(), "2026-04-14", "2026-04-16",
            param_grid=grid, max_variants=2,
        )
        assert result.all_variants <= 2


class TestApplyParams:
    def test_apply_creates_new_strategy(self, db):
        evolver = StrategyEvolver(db)
        base = _make_base()
        variant = evolver._apply_params(base, {"take_profit_pct": 10.0, "max_hold_days": 5})
        assert variant.exit.take_profit_pct == 10.0
        assert variant.exit.max_hold_days == 5
        assert variant.parent == base.name
        assert variant.version == base.version + 1
        # 原策略不变
        assert base.exit.take_profit_pct == 5.0

    def test_name_increments_version(self, db):
        evolver = StrategyEvolver(db)
        base = _make_base()
        v1 = evolver._apply_params(base, {"take_profit_pct": 7.0})
        assert v1.name == "base_test_v2"


class TestDiffParams:
    def test_detects_changes(self, db):
        evolver = StrategyEvolver(db)
        base = _make_base()
        variant = evolver._apply_params(base, {"take_profit_pct": 10.0})
        diff = evolver._diff_params(base, variant)
        assert "take_profit_pct" in diff
        assert "5.0→10.0" in diff["take_profit_pct"]

    def test_no_changes(self, db):
        evolver = StrategyEvolver(db)
        base = _make_base()
        variant = evolver._apply_params(base, {})
        diff = evolver._diff_params(base, variant)
        assert diff == {}


class TestScoring:
    def test_empty_report_scores_low(self, db):
        evolver = StrategyEvolver(db)
        report = StrategyReport(
            strategy_name="empty", backtest_start="2026-01-01",
            backtest_end="2026-06-30", total_trades=0,
        )
        assert evolver._score(report, "sharpe") == -999.0

    def test_sharpe_objective(self, db):
        evolver = StrategyEvolver(db)
        report = StrategyReport(
            strategy_name="t", backtest_start="2026-01-01",
            backtest_end="2026-06-30", total_trades=10, sharpe_ratio=1.5,
        )
        assert evolver._score(report, "sharpe") == 1.5

    def test_composite_objective(self, db):
        evolver = StrategyEvolver(db)
        report = StrategyReport(
            strategy_name="t", backtest_start="2026-01-01",
            backtest_end="2026-06-30", total_trades=10,
            sharpe_ratio=2.0, win_rate=0.6, profit_loss_ratio=2.0,
        )
        score = evolver._score(report, "composite")
        assert 0 < score < 1


class TestImprovements:
    def test_top_k_improvements(self, db):
        grid = {"take_profit_pct": [3.0, 5.0, 7.0], "stop_loss_pct": [-2.0, -3.0, -5.0]}
        evolver = StrategyEvolver(db)
        result = evolver.evolve(
            _make_base(), "2026-04-14", "2026-04-16",
            param_grid=grid, top_k=2,
        )
        assert len(result.improvements) <= 2
        for imp in result.improvements:
            assert "rank" in imp
            assert "score" in imp
            assert "delta" in imp
