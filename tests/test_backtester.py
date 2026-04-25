"""因子回测器测试。"""

import os
import tempfile
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pytest

from src.data.storage import Storage
from src.mining.backtester import BacktestResult, FactorBacktester


# ============================================================
# Fixtures
# ============================================================

# snapshot_time 必须：
#   1. 在 datetime.now() 之前（否则 _get_trade_dates 的 query_range 看不到数据）
#   2. 在最早 trade_date 的 15:00 之前（否则 _get_universe 的 query 过滤掉数据）
# 第一天 2026-03-30 15:00 之前 = 2026-03-29 任意时间
SNAPSHOT_TIME = datetime(2026, 3, 29, 10, 0, 0)


def _make_trade_dates(n=20):
    """生成最近 n 个工作日列表 (截止到 2026-04-24)。"""
    dates = []
    d = datetime(2026, 4, 24)  # 周五
    while len(dates) < n:
        if d.weekday() < 5:  # 跳过周末
            dates.append(d.strftime("%Y-%m-%d"))
        d -= timedelta(days=1)
    return list(reversed(dates))


@pytest.fixture
def populated_db():
    """创建并填充临时数据库 — 20 个交易日 × 30 只股票。"""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    db = Storage(db_path)
    db.init_db()

    trade_dates = _make_trade_dates(20)
    assert len(trade_dates) >= 15, f"Need >= 15 trade dates, got {len(trade_dates)}"

    stocks = [f"{i:06d}" for i in range(1, 31)]  # 000001 ~ 000030
    rows = []
    for date_str in trade_dates:
        for j, stock in enumerate(stocks):
            base_close = 10.0 + j * 1.5
            day_seed = hash(date_str) % 1000
            close = base_close + (day_seed % 10 - 5) * 0.3
            amount = 1_000_000 + j * 50_000 + day_seed
            rows.append({
                "stock_code": stock,
                "trade_date": date_str,
                "open": close - 0.1,
                "high": close + 0.2,
                "low": close - 0.2,
                "close": close,
                "volume": amount / close,
                "amount": amount,
                "turnover_rate": 1.0 + j * 0.1,
            })

    df = pd.DataFrame(rows)
    db.insert("daily_price", df, snapshot_time=SNAPSHOT_TIME)
    yield db
    os.unlink(db_path)


@pytest.fixture
def empty_db():
    """创建空的临时数据库。"""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    db = Storage(db_path)
    db.init_db()
    yield db
    os.unlink(db_path)


def simple_compute(universe, as_of, db):
    """简单因子：按 stock_code 生成随机因子值。"""
    rng = np.random.RandomState(42)
    values = {stock: rng.randn() for stock in universe}
    return pd.Series(values, name="test_factor")


# ============================================================
# Tests
# ============================================================


class TestRunWithSimpleFactor:
    def test_run_returns_valid_result(self, populated_db):
        """填充 DB 后回测应返回有效结果 (ic_series 非空)。"""
        bt = FactorBacktester(populated_db)
        result = bt.run(
            compute_fn=simple_compute,
            factor_name="test_simple",
            lookback_days=12,
            forward_days=1,
        )
        assert isinstance(result, BacktestResult)
        assert result.factor_name == "test_simple"
        assert result.error is None, f"Unexpected error: {result.error}"
        assert len(result.ic_series) > 0, "Expected non-empty ic_series"
        assert result.total_days == len(result.ic_series)
        assert result.total_days >= 1
        # ic_mean 应为有限浮点数
        assert np.isfinite(result.ic_mean)


class TestRunWithEmptyDb:
    def test_run_with_empty_db(self, empty_db):
        """空数据库应返回错误，不崩溃。"""
        bt = FactorBacktester(empty_db)
        result = bt.run(
            compute_fn=simple_compute,
            factor_name="test_empty",
        )
        assert isinstance(result, BacktestResult)
        assert result.error is not None
        assert result.ic_series == []
        assert result.total_days == 0


class TestICSeriesHasRegime:
    def test_ic_series_contains_required_fields(self, populated_db):
        """每条 IC 记录应包含 date / ic / regime / zt_count。"""
        bt = FactorBacktester(populated_db)
        result = bt.run(
            compute_fn=simple_compute,
            factor_name="test_regime",
            lookback_days=12,
            forward_days=1,
        )
        assert result.error is None, f"Unexpected error: {result.error}"
        assert len(result.ic_series) > 0

        for record in result.ic_series:
            assert "date" in record, f"Missing 'date' in record: {record}"
            assert "ic" in record, f"Missing 'ic' in record: {record}"
            assert "regime" in record, f"Missing 'regime' in record: {record}"
            assert "zt_count" in record, f"Missing 'zt_count' in record: {record}"
            # ic 应为浮点数
            assert isinstance(record["ic"], float)
            # regime 应为字符串
            assert isinstance(record["regime"], str)
            # zt_count 应为整数
            assert isinstance(record["zt_count"], int)


class TestBacktestResultToDict:
    def test_to_dict_serialization(self):
        """to_dict 应返回正确的序列化格式。"""
        ic_series = [
            {"date": "2026-04-24", "ic": 0.05, "regime": "normal", "zt_count": 3, "sample_size": 30},
        ]
        result = BacktestResult(
            factor_name="test_factor",
            ic_mean=0.05,
            icir=1.2,
            win_rate=0.6,
            pnl_ratio=1.5,
            sample_per_day=30.0,
            total_days=1,
            ic_series=ic_series,
        )
        d = result.to_dict()

        # 验证所有键都存在
        expected_keys = {
            "ic_mean", "icir", "win_rate", "pnl_ratio",
            "sample_per_day", "total_days", "ic_series", "error",
        }
        assert set(d.keys()) == expected_keys
        # 验证值正确
        assert d["ic_mean"] == 0.05
        assert d["icir"] == 1.2
        assert d["win_rate"] == 0.6
        assert d["pnl_ratio"] == 1.5
        assert d["sample_per_day"] == 30.0
        assert d["total_days"] == 1
        assert d["ic_series"] == ic_series
        assert d["error"] is None

    def test_to_dict_with_error(self):
        """带错误的 BacktestResult 序列化。"""
        result = BacktestResult(
            factor_name="bad_factor",
            error="交易日数据不足: 5 天",
        )
        d = result.to_dict()
        assert d["error"] == "交易日数据不足: 5 天"
        assert d["ic_mean"] == 0.0
        assert d["ic_series"] == []
