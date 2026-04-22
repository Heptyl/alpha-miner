"""策略持久化层测试。"""

import os
import tempfile

import pytest

from src.data.storage import Storage
from src.strategy.schema import (
    EntryRule, ExitRule, Strategy, StrategyReport, Trade,
)
from src.strategy.store import StrategyStore


@pytest.fixture
def store():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    db = Storage(db_path)
    db.init_db()
    s = StrategyStore(db)
    yield s
    os.unlink(db_path)


def _make_strategy(name="test_strat") -> Strategy:
    return Strategy(
        name=name,
        description="test",
        entry=EntryRule(conditions=[{"factor": "x", "op": ">", "value": 0}]),
        exit=ExitRule(take_profit_pct=5.0),
        tags=["tag1", "tag2"],
    )


def _make_report(name="test_strat") -> StrategyReport:
    return StrategyReport(
        strategy_name=name,
        backtest_start="2026-01-01",
        backtest_end="2026-03-31",
        total_trades=3,
        win_rate=0.67,
        total_return_pct=8.5,
        sharpe_ratio=1.2,
        max_drawdown_pct=3.0,
        profit_loss_ratio=2.1,
        trades=[
            Trade(strategy_name=name, stock_code="000001", entry_date="2026-01-05",
                  entry_price=10.0, exit_date="2026-01-07", exit_price=10.5,
                  return_pct=5.0, hold_days=2, exit_reason="take_profit:5.0%",
                  regime_at_entry="board_rally"),
            Trade(strategy_name=name, stock_code="000002", entry_date="2026-01-10",
                  entry_price=20.0, exit_date="2026-01-11", exit_price=19.0,
                  return_pct=-5.0, hold_days=1, exit_reason="stop_loss:-5.0%"),
            Trade(strategy_name=name, stock_code="000003", entry_date="2026-02-01",
                  entry_price=15.0, exit_date="2026-02-04", exit_price=16.0,
                  return_pct=6.67, hold_days=3, exit_reason="max_hold:3d"),
        ],
    )


class TestStrategyCRUD:
    def test_save_and_load(self, store):
        s = _make_strategy()
        store.save_strategy(s)
        loaded = store.load_strategy("test_strat")
        assert loaded is not None
        assert loaded.name == "test_strat"
        assert loaded.exit.take_profit_pct == 5.0
        assert loaded.tags == ["tag1", "tag2"]

    def test_update_existing(self, store):
        s = _make_strategy()
        store.save_strategy(s)
        s.exit.take_profit_pct = 10.0
        store.save_strategy(s)
        loaded = store.load_strategy("test_strat")
        assert loaded.exit.take_profit_pct == 10.0

    def test_list_strategies(self, store):
        store.save_strategy(_make_strategy("s1"))
        store.save_strategy(_make_strategy("s2"))
        result = store.list_strategies()
        assert len(result) == 2
        names = {r["name"] for r in result}
        assert names == {"s1", "s2"}

    def test_load_nonexistent(self, store):
        assert store.load_strategy("nope") is None

    def test_delete_cascade(self, store):
        store.save_strategy(_make_strategy())
        store.save_report(_make_report())
        store.delete_strategy("test_strat")
        assert store.load_strategy("test_strat") is None
        assert store.get_report_summary("test_strat") is None


class TestReportPersistence:
    def test_save_and_load_report(self, store):
        store.save_strategy(_make_strategy())
        report = _make_report()
        rid = store.save_report(report)
        assert rid > 0

        loaded = store.load_latest_report("test_strat")
        assert loaded is not None
        assert loaded.total_trades == 3
        assert loaded.win_rate == 0.67
        assert loaded.sharpe_ratio == 1.2
        assert len(loaded.trades) == 3

    def test_report_summary(self, store):
        store.save_strategy(_make_strategy())
        store.save_report(_make_report())
        summary = store.get_report_summary("test_strat")
        assert summary["total_trades"] == 3
        assert abs(summary["win_rate"] - 0.67) < 0.01

    def test_trades_persisted(self, store):
        store.save_strategy(_make_strategy())
        store.save_report(_make_report())
        trades = store.get_trades("test_strat")
        assert len(trades) == 3
        codes = {t["stock_code"] for t in trades}
        assert codes == {"000001", "000002", "000003"}

    def test_trades_date_filter(self, store):
        store.save_strategy(_make_strategy())
        store.save_report(_make_report())
        trades = store.get_trades("test_strat", start_date="2026-01-10", end_date="2026-02-01")
        assert len(trades) == 2

    def test_auto_create_strategy_on_report(self, store):
        """保存报告时自动创建占位策略。"""
        report = StrategyReport(
            strategy_name="auto_created", backtest_start="2026-01-01",
            backtest_end="2026-03-31", total_trades=1,
        )
        store.save_report(report)
        loaded = store.load_strategy("auto_created")
        assert loaded is not None
        assert loaded.name == "auto_created"
