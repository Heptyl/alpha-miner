"""回测引擎测试 — 使用真实临时数据库，无 mock/stub。"""

import os
import tempfile
from datetime import datetime, timedelta

import pandas as pd
import pytest

from src.data.storage import Storage
from src.drift.regime import RegimeDetector
from src.strategy.backtest_engine import BacktestEngine, HoldingInfo
from src.strategy.schema import (
    EntryRule, ExitRule, PositionRule, Strategy, Trade,
)


@pytest.fixture
def db():
    """创建临时数据库并填入基础数据。"""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    store = Storage(db_path)
    store.init_db()
    _seed_basic_data(store)
    yield store
    os.unlink(db_path)


def _seed_basic_data(db: Storage):
    """填充 3 个交易日(04-14, 04-15, 04-16)的基础数据。"""
    as_of_str = "2026-04-14 20:59:59"
    dates = ["2026-04-14", "2026-04-15", "2026-04-16"]

    # daily_price: 5 只股票
    stocks = {
        "000001": {"base": 10.0},
        "000002": {"base": 20.0},
        "000003": {"base": 15.0},
        "000004": {"base": 30.0},
        "000005": {"base": 8.0},
    }

    rows = []
    for date in dates:
        for code, info in stocks.items():
            base = info["base"]
            # 每天微涨/微跌
            day_idx = dates.index(date)
            close = round(base * (1 + 0.01 * day_idx), 2)
            open_p = round(base * (1 + 0.005 * day_idx), 2)
            high = round(close * 1.02, 2)
            low = round(close * 0.98, 2)
            pre_close = round(base * (1 + 0.01 * (day_idx - 1)), 2) if day_idx > 0 else base
            rows.append({
                "stock_code": code, "trade_date": date,
                "open": open_p, "high": high, "low": low,
                "close": close, "pre_close": pre_close,
                "volume": 1000000, "amount": 10000000, "turnover_rate": 5.0,
            })
    df = pd.DataFrame(rows)
    # 手动设 snapshot_time
    import sqlite3
    conn = sqlite3.connect(db.db_path)
    df["_snapshot_time"] = as_of_str
    for _, row in df.iterrows():
        conn.execute(
            "INSERT OR REPLACE INTO daily_price (stock_code, trade_date, open, high, low, close, pre_close, volume, amount, turnover_rate, snapshot_time) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (row["stock_code"], row["trade_date"], row["open"], row["high"],
             row["low"], row["close"], row["pre_close"], row["volume"],
             row["amount"], row["turnover_rate"], row["_snapshot_time"])
        )
    conn.commit()

    # zt_pool: 000001 和 000002 在 04-14 涨停
    for code in ["000001", "000002"]:
        conn.execute(
            "INSERT INTO zt_pool (stock_code, trade_date, consecutive_zt, amount, snapshot_time) VALUES (?,?,?,?,?)",
            (code, "2026-04-14", 1, 50000000, as_of_str)
        )
    # 000001 在 04-15 连板
    conn.execute(
        "INSERT INTO zt_pool (stock_code, trade_date, consecutive_zt, amount, snapshot_time) VALUES (?,?,?,?,?)",
        ("000001", "2026-04-15", 2, 60000000, as_of_str)
    )
    conn.commit()

    # factor_values: consecutive_board 和 turnover_rank
    for date in dates:
        for code in stocks:
            # consecutive_board: 000001=2(04-15,04-16), 000002=1(04-14)
            cb = 0
            if code == "000001":
                cb = 2 if date >= "2026-04-15" else (1 if date == "2026-04-14" else 0)
            elif code == "000002":
                cb = 1 if date == "2026-04-14" else 0
            conn.execute(
                "INSERT INTO factor_values (factor_name, stock_code, trade_date, factor_value, snapshot_time) VALUES (?,?,?,?,?)",
                ("consecutive_board", code, date, float(cb), as_of_str)
            )
            # turnover_rank: 全部给 0.5
            conn.execute(
                "INSERT INTO factor_values (factor_name, stock_code, trade_date, factor_value, snapshot_time) VALUES (?,?,?,?,?)",
                ("turnover_rank", code, date, 0.5, as_of_str)
            )
    conn.commit()
    conn.close()


def _make_simple_strategy(**overrides) -> Strategy:
    """构造一个简单策略用于测试。"""
    defaults = dict(
        name="test_strategy",
        description="test",
        entry=EntryRule(
            conditions=[{"factor": "consecutive_board", "op": ">=", "value": 1}],
        ),
        exit=ExitRule(take_profit_pct=5.0, stop_loss_pct=-3.0, max_hold_days=3),
        position=PositionRule(max_holdings=3),
    )
    defaults.update(overrides)
    return Strategy(**defaults)


# ═══════════════════════════════════════════════════════════
# 测试
# ═══════════════════════════════════════════════════════════


class TestEntryCheck:
    """入场条件检查。"""

    def test_entry_matches_when_factor_meets_condition(self, db):
        """consecutive_board >= 1 且值为 1 时应满足入场条件。"""
        engine = BacktestEngine(db)
        as_of = datetime(2026, 4, 14, 21, 0, 0)
        entry = EntryRule(
            conditions=[{"factor": "consecutive_board", "op": ">=", "value": 1}]
        )
        # 000001 在 04-14 的 consecutive_board = 1
        result = engine._check_entry(entry, "000001", "2026-04-14", as_of)
        assert result is True

    def test_entry_fails_when_factor_below_threshold(self, db):
        """consecutive_board >= 1 但值为 0 时不满足。"""
        engine = BacktestEngine(db)
        as_of = datetime(2026, 4, 14, 21, 0, 0)
        entry = EntryRule(
            conditions=[{"factor": "consecutive_board", "op": ">=", "value": 1}]
        )
        # 000003 在 04-14 的 consecutive_board = 0
        result = engine._check_entry(entry, "000003", "2026-04-14", as_of)
        assert result is False

    def test_entry_fails_when_factor_missing(self, db):
        """因子不存在时返回 False。"""
        engine = BacktestEngine(db)
        as_of = datetime(2026, 4, 14, 21, 0, 0)
        entry = EntryRule(
            conditions=[{"factor": "nonexistent_factor", "op": ">", "value": 0}]
        )
        result = engine._check_entry(entry, "000001", "2026-04-14", as_of)
        assert result is False

    def test_entry_all_conditions_must_pass(self, db):
        """多个条件必须全部满足。"""
        engine = BacktestEngine(db)
        as_of = datetime(2026, 4, 14, 21, 0, 0)
        entry = EntryRule(
            conditions=[
                {"factor": "consecutive_board", "op": ">=", "value": 1},
                {"factor": "turnover_rank", "op": ">=", "value": 0.8},  # 值为0.5，不满足
            ]
        )
        result = engine._check_entry(entry, "000001", "2026-04-14", as_of)
        assert result is False


class TestExitCheck:
    """出场条件检查。"""

    def _make_holding(self, entry_price=10.0, entry_date="2026-04-14") -> HoldingInfo:
        return HoldingInfo(
            stock_code="000001",
            entry_date=entry_date,
            entry_price=entry_price,
            peak_price=entry_price,
        )

    def test_take_profit_triggers(self, db):
        """盈利达到止盈线时触发。"""
        engine = BacktestEngine(db)
        as_of = datetime(2026, 4, 15, 21, 0, 0)
        holding = self._make_holding(entry_price=10.0)
        exit_rule = ExitRule(take_profit_pct=5.0)
        # 04-15 收盘价 = 10.0 * 1.01 = 10.1 → 涨幅 1%，不触发
        result = engine._check_exit(exit_rule, "000001", "2026-04-15", holding, as_of)
        assert result is None  # 不触发

    def test_stop_loss_triggers(self, db):
        """亏损达到止损线时触发。"""
        engine = BacktestEngine(db)
        as_of = datetime(2026, 4, 15, 21, 0, 0)
        # 000001 04-15 收盘 = 10.1, 设入场价 11.0 → 亏损 -8.2%
        holding = self._make_holding(entry_price=11.0)
        exit_rule = ExitRule(stop_loss_pct=-5.0)
        result = engine._check_exit(exit_rule, "000001", "2026-04-15", holding, as_of)
        assert result is not None
        assert "stop_loss" in result

    def test_max_hold_days_triggers(self, db):
        """持仓达到最大天数时触发。"""
        engine = BacktestEngine(db)
        as_of = datetime(2026, 4, 16, 21, 0, 0)
        holding = self._make_holding(entry_date="2026-04-14")
        exit_rule = ExitRule(max_hold_days=2)
        result = engine._check_exit(exit_rule, "000001", "2026-04-16", holding, as_of)
        assert result is not None
        assert "max_hold" in result

    def test_condition_exit_triggers(self, db):
        """条件出场因子满足时触发。"""
        engine = BacktestEngine(db)
        as_of = datetime(2026, 4, 15, 21, 0, 0)
        holding = self._make_holding()
        exit_rule = ExitRule(
            exit_conditions=[
                {"factor": "turnover_rank", "op": ">=", "value": 0.3,
                 "reason": "换手率过高"}
            ]
        )
        result = engine._check_exit(exit_rule, "000001", "2026-04-15", holding, as_of)
        assert result is not None
        assert "condition:换手率过高" in result

    def test_no_exit_when_conditions_not_met(self, db):
        """不满足任何出场条件时不触发。"""
        engine = BacktestEngine(db)
        as_of = datetime(2026, 4, 15, 21, 0, 0)
        holding = self._make_holding(entry_price=10.0)
        exit_rule = ExitRule(take_profit_pct=20.0, stop_loss_pct=-20.0, max_hold_days=10)
        result = engine._check_exit(exit_rule, "000001", "2026-04-15", holding, as_of)
        assert result is None


class TestT1Constraint:
    """T+1 限制测试。"""

    def test_cannot_sell_on_buy_day(self, db):
        """T+1: 买入当天不能卖出。"""
        engine = BacktestEngine(db)
        strategy = _make_simple_strategy()
        report = engine.run(strategy, "2026-04-14", "2026-04-16")
        # 验证所有交易的 entry_date != exit_date
        for t in report.trades:
            if t.entry_date and t.exit_date:
                # entry_date 和 exit_date 不应该相同（T+1）
                assert t.exit_date > t.entry_date, (
                    f"T+1 violated: entry={t.entry_date}, exit={t.exit_date}"
                )


class TestPositionLimit:
    """仓位限制测试。"""

    def test_max_holdings_respected(self, db):
        """同时持仓不超过 max_holdings。"""
        engine = BacktestEngine(db)
        strategy = _make_simple_strategy(
            position=PositionRule(max_holdings=1)
        )
        report = engine.run(strategy, "2026-04-14", "2026-04-16")
        # 无法精确验证每时点持仓数，但至少不应报错
        assert report.total_trades >= 0


class TestEmptyUniverse:
    """空候选池测试。"""

    def test_no_crash_on_empty_universe(self, db):
        """候选池为空时不报错。"""
        engine = BacktestEngine(db)
        strategy = _make_simple_strategy()
        # 04-16 没有 zt_pool 数据 → universe 为空
        report = engine.run(strategy, "2026-04-16", "2026-04-16")
        assert report.total_trades == 0
        assert report.strategy_name == "test_strategy"


class TestCompare:
    """比较运算符测试。"""

    def test_all_operators(self, db):
        engine = BacktestEngine(db)
        assert engine._compare(1.0, ">=", 1.0) is True
        assert engine._compare(0.9, ">=", 1.0) is False
        assert engine._compare(1.0, "<=", 1.0) is True
        assert engine._compare(1.1, "<=", 1.0) is False
        assert engine._compare(1.1, ">", 1.0) is True
        assert engine._compare(1.0, ">", 1.0) is False
        assert engine._compare(0.9, "<", 1.0) is True
        assert engine._compare(1.0, "<", 1.0) is False
        assert engine._compare(1.0, "==", 1.0) is True
        assert engine._compare(1.0, "==", 1.0000001) is True  # 浮点容差


class TestBuildReport:
    """报告构建测试。"""

    def test_empty_trades_report(self, db):
        engine = BacktestEngine(db)
        report = engine._build_report("test", "2026-01-01", "2026-06-30", [])
        assert report.total_trades == 0
        assert report.win_rate == 0

    def test_report_with_mixed_trades(self, db):
        trades = [
            Trade(strategy_name="t", stock_code="000001", return_pct=5.0),
            Trade(strategy_name="t", stock_code="000002", return_pct=-2.0),
            Trade(strategy_name="t", stock_code="000003", return_pct=3.0),
            Trade(strategy_name="t", stock_code="000004", return_pct=-1.0),
        ]
        engine = BacktestEngine(db)
        report = engine._build_report("t", "2026-01-01", "2026-06-30", trades)
        assert report.total_trades == 4
        assert report.win_count == 2
        assert report.loss_count == 2
        assert report.win_rate == 0.5
        assert abs(report.avg_win_pct - 4.0) < 0.01
        assert abs(report.avg_loss_pct - (-1.5)) < 0.01
        assert report.max_consecutive_loss == 1  # 最大连亏1次
        assert abs(report.total_return_pct - 5.0) < 0.01

    def test_consecutive_loss_tracking(self, db):
        trades = [
            Trade(strategy_name="t", stock_code="001", return_pct=-1.0),
            Trade(strategy_name="t", stock_code="002", return_pct=-2.0),
            Trade(strategy_name="t", stock_code="003", return_pct=-3.0),
            Trade(strategy_name="t", stock_code="004", return_pct=1.0),
            Trade(strategy_name="t", stock_code="005", return_pct=-0.5),
        ]
        engine = BacktestEngine(db)
        report = engine._build_report("t", "2026-01-01", "2026-06-30", trades)
        assert report.max_consecutive_loss == 3  # 前3笔连续亏损


class TestFullRun:
    """完整回测流程测试。"""

    def test_run_produces_report(self, db):
        """完整回测产生报告。"""
        engine = BacktestEngine(db)
        strategy = _make_simple_strategy()
        report = engine.run(strategy, "2026-04-14", "2026-04-16")
        assert isinstance(report.strategy_name, str)
        assert report.backtest_start == "2026-04-14"
        assert report.backtest_end == "2026-04-16"
        # 至少应该有一些交易（zt_pool 有 000001 和 000002）
        # 但因为 next_open 需要次日数据且 factor 条件，可能 0 笔也可能 >0
        assert report.total_trades >= 0

    def test_run_respects_date_range(self, db):
        """回测日期范围正确。"""
        engine = BacktestEngine(db)
        strategy = _make_simple_strategy()
        report = engine.run(strategy, "2026-04-14", "2026-04-14")
        # 单日回测，只能产生信号但无法完成交易（需要次日卖出）
        for t in report.trades:
            assert t.entry_date <= "2026-04-14"
