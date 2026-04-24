"""H2: IC 计算端到端硬断言测试。

构造已知 Spearman IC 的数据集，验证 ICTracker 计算结果精确匹配。

核心方法：构造 factor_values 和 daily_price 使得 Spearman IC 有已知的精确值。
- 完美正相关 → IC = 1.0
- 完美负相关 → IC = -1.0
- 随机无相关 → IC ≈ 0.0
"""
from datetime import datetime

import numpy as np
import pandas as pd
import pytest
from scipy import stats as scipy_stats

from src.data.storage import Storage
from src.drift.ic_tracker import ICTracker


@pytest.fixture
def db(tmp_path):
    s = Storage(str(tmp_path / "test.db"))
    s.init_db()
    return s


def _setup_perfect_positive(db, snap, trade_dates):
    """构造完美正相关的因子和价格数据。

    10只股票，因子值=1..10，收益率也单调递增 → Spearman IC=1.0
    """
    stocks = [f"00000{i}" for i in range(10)]
    factor_values_list = []
    price_rows = []

    # 因子值固定不变（不需要按天变）
    # 但 factor_values 需要按天记录
    for date in trade_dates:
        for i, code in enumerate(stocks):
            factor_values_list.append({
                "factor_name": "perfect_pos",
                "stock_code": code,
                "trade_date": date,
                "factor_value": float(i + 1),  # 1, 2, ..., 10
            })

    # 价格：每只股票每天涨一点，涨幅与因子值成正比
    base_prices = {code: 10.0 + i * 2.0 for i, code in enumerate(stocks)}
    for di, date in enumerate(trade_dates):
        for i, code in enumerate(stocks):
            price_rows.append({
                "stock_code": code,
                "trade_date": date,
                "open": base_prices[code] + di * 0.5,
                "high": base_prices[code] + di * 0.5 + 0.5,
                "low": base_prices[code] + di * 0.5 - 0.5,
                "close": base_prices[code] + di * (i + 1) * 0.1,  # 涨幅与 i 成正比
                "volume": 1000,
                "amount": 10000,
                "turnover_rate": 1.0,
            })

    db.insert("factor_values", pd.DataFrame(factor_values_list), snapshot_time=snap)
    db.insert("daily_price", pd.DataFrame(price_rows), snapshot_time=snap)


def _setup_perfect_negative(db, snap, trade_dates):
    """构造完美负相关的因子数据。

    因子值=1..10，收益率单调递减 → Spearman IC=-1.0
    """
    stocks = [f"00000{i}" for i in range(10)]
    factor_values_list = []
    price_rows = []

    for date in trade_dates:
        for i, code in enumerate(stocks):
            factor_values_list.append({
                "factor_name": "perfect_neg",
                "stock_code": code,
                "trade_date": date,
                "factor_value": float(i + 1),
            })

    base_prices = {code: 50.0 for code in stocks}
    for di, date in enumerate(trade_dates):
        for i, code in enumerate(stocks):
            # 收益率与 i 成反比（i 大的涨幅小）
            price_rows.append({
                "stock_code": code,
                "trade_date": date,
                "open": base_prices[code],
                "high": base_prices[code] + 0.5,
                "low": base_prices[code] - 0.5,
                "close": base_prices[code] + di * (9 - i) * 0.1,  # 涨幅与 (9-i) 成正比
                "volume": 1000,
                "amount": 10000,
                "turnover_rate": 1.0,
            })

    db.insert("factor_values", pd.DataFrame(factor_values_list), snapshot_time=snap)
    db.insert("daily_price", pd.DataFrame(price_rows), snapshot_time=snap)


class TestICPerfectPositive:
    """完美正相关 → IC ≈ 1.0"""

    def test_single_day_ic_is_1(self, db):
        """单日截面 Spearman IC 应为 1.0。"""
        snap = datetime(2024, 6, 1, 10, 0, 0)
        dates = ["2024-06-03", "2024-06-04", "2024-06-05"]
        _setup_perfect_positive(db, snap, dates)

        tracker = ICTracker(db)
        ic_df = tracker.compute_ic_series(
            "perfect_pos", "2024-06-03", "2024-06-04",
            forward_days=1, persist=False,
        )
        # 只测第一天（有第二天作为 forward）
        assert not ic_df.empty, "IC 结果为空"
        first_ic = ic_df.iloc[0]["ic"]
        assert not np.isnan(first_ic), f"IC is NaN"
        assert abs(first_ic - 1.0) < 0.02, f"IC={first_ic}, 期望 ≈1.0"

    def test_ic_series_all_positive(self, db):
        """多日 IC 序列全部应为正。"""
        snap = datetime(2024, 6, 1, 10, 0, 0)
        dates = [f"2024-06-{d:02d}" for d in range(3, 8)]
        _setup_perfect_positive(db, snap, dates)

        tracker = ICTracker(db)
        ic_df = tracker.compute_ic_series(
            "perfect_pos", "2024-06-03", "2024-06-06",
            forward_days=1, persist=False,
        )
        valid_ics = ic_df["ic"].dropna()
        assert len(valid_ics) >= 2, f"有效IC只有{len(valid_ics)}天"
        assert (valid_ics > 0.9).all(), f"IC不是全部接近1.0: {valid_ics.tolist()}"


class TestICPerfectNegative:
    """完美负相关 → IC ≈ -1.0"""

    def test_single_day_ic_is_minus_1(self, db):
        snap = datetime(2024, 6, 1, 10, 0, 0)
        dates = ["2024-06-03", "2024-06-04", "2024-06-05"]
        _setup_perfect_negative(db, snap, dates)

        tracker = ICTracker(db)
        ic_df = tracker.compute_ic_series(
            "perfect_neg", "2024-06-03", "2024-06-04",
            forward_days=1, persist=False,
        )
        assert not ic_df.empty
        first_ic = ic_df.iloc[0]["ic"]
        assert not np.isnan(first_ic)
        assert abs(first_ic - (-1.0)) < 0.02, f"IC={first_ic}, 期望 ≈-1.0"


class TestICManualComputation:
    """手工算 IC 与 tracker 结果对比。

    构造5只股票，因子值和收益率如下：
    - 000000: factor=1.0, ret=+5%
    - 000001: factor=3.0, ret=-2%
    - 000002: factor=2.0, ret=+3%
    - 000003: factor=5.0, ret=-8%
    - 000004: factor=4.0, ret=+1%

    Spearman rank:
    factor: [1,3,2,5,4] → rank [1,3,2,5,4]
    ret:    [5,-2,3,-8,1] → rank [4,2,3,1,5]
    Spearman = 1 - 6*sum(d^2)/(n*(n^2-1))
    d = [1-4,3-2,2-3,5-1,4-5] = [-3,1,-1,4,-1]
    d^2 = [9,1,1,16,1] = 28
    Spearman = 1 - 6*28/(5*24) = 1 - 168/120 = 1 - 1.4 = -0.4
    """

    def test_manual_ic_minus_0_9(self, db):
        """手工计算 Spearman IC 并与 tracker 结果对比。

        因子值: [1, 3, 2, 5, 4] → rank [1, 3, 2, 5, 4]
        收益率: [+5%, -2%, +3%, -8%, +1%]
        收益率排序: -0.08 < -0.02 < 0.01 < 0.03 < 0.05
        ret rank: [5, 2, 4, 1, 3]
        d = [-4, 1, -2, 4, 1], d^2 = [16, 1, 4, 16, 1] = 38
        Spearman = 1 - 6*38/(5*24) = 1 - 228/120 = -0.9
        """
        snap = datetime(2024, 6, 1, 10, 0, 0)
        stocks = ["000000", "000001", "000002", "000003", "000004"]
        factor_vals = [1.0, 3.0, 2.0, 5.0, 4.0]
        rets_pct = [5, -2, 3, -8, 1]
        day0_close = 10.0
        day1_close = [day0_close * (1 + r / 100) for r in rets_pct]

        fv_rows = [
            {"factor_name": "manual_ic", "stock_code": code,
             "trade_date": "2024-06-03", "factor_value": fv}
            for code, fv in zip(stocks, factor_vals)
        ]
        db.insert("factor_values", pd.DataFrame(fv_rows), snapshot_time=snap)

        price_rows = []
        for code in stocks:
            price_rows.append({
                "stock_code": code, "trade_date": "2024-06-03",
                "open": 10.0, "high": 10.5, "low": 9.5,
                "close": day0_close, "volume": 1000, "amount": 10000, "turnover_rate": 1.0,
            })
        for code, close in zip(stocks, day1_close):
            price_rows.append({
                "stock_code": code, "trade_date": "2024-06-04",
                "open": close - 0.1, "high": close + 0.2, "low": close - 0.2,
                "close": close, "volume": 1000, "amount": 10000, "turnover_rate": 1.0,
            })
        db.insert("daily_price", pd.DataFrame(price_rows), snapshot_time=snap)

        # scipy 验证
        corr, _ = scipy_stats.spearmanr(factor_vals, [r / 100 for r in rets_pct])

        tracker = ICTracker(db)
        ic_df = tracker.compute_ic_series(
            "manual_ic", "2024-06-03", "2024-06-03",
            forward_days=1, persist=False,
        )
        assert not ic_df.empty
        actual_ic = ic_df.iloc[0]["ic"]
        assert not np.isnan(actual_ic), "IC is NaN"
        assert abs(actual_ic - corr) < 0.01, \
            f"IC={actual_ic}, scipy={corr}"
        assert abs(actual_ic - (-0.9)) < 0.02, \
            f"IC={actual_ic}, 期望 -0.9"


class TestICPersist:
    """验证 IC 持久化到数据库。"""

    def test_persist_writes_to_db(self, db):
        snap = datetime(2024, 6, 1, 10, 0, 0)
        _setup_perfect_positive(db, snap, ["2024-06-03", "2024-06-04", "2024-06-05"])

        tracker = ICTracker(db)
        ic_df = tracker.compute_ic_series(
            "perfect_pos", "2024-06-03", "2024-06-04",
            forward_days=1, persist=True,
        )

        # 直接用 SQL 读，绕过时间隔离
        conn = db._get_conn()
        try:
            saved = pd.read_sql_query(
                "SELECT * FROM ic_series WHERE factor_name = 'perfect_pos'",
                conn,
            )
        finally:
            conn.close()

        assert not saved.empty, "ic_series 表为空"
        assert "ic_value" in saved.columns
        valid = saved["ic_value"].dropna()
        assert len(valid) > 0, "没有有效的 IC 值被持久化"
        assert (valid > 0.9).all(), f"持久化的 IC 值不正确: {valid.tolist()}"


class TestICEdgeCases:
    """IC 计算边界条件。"""

    def test_insufficient_stocks_returns_nan(self, db):
        """少于5只股票 → IC = NaN。"""
        snap = datetime(2024, 6, 1, 10, 0, 0)
        fv_rows = [
            {"factor_name": "tiny", "stock_code": "000001",
             "trade_date": "2024-06-03", "factor_value": 1.0},
            {"factor_name": "tiny", "stock_code": "000002",
             "trade_date": "2024-06-03", "factor_value": 2.0},
        ]
        price_rows = [
            {"stock_code": "000001", "trade_date": "2024-06-03",
             "open": 10, "high": 10, "low": 10, "close": 10,
             "volume": 100, "amount": 1000, "turnover_rate": 1.0},
            {"stock_code": "000002", "trade_date": "2024-06-03",
             "open": 20, "high": 20, "low": 20, "close": 20,
             "volume": 100, "amount": 2000, "turnover_rate": 1.0},
            {"stock_code": "000001", "trade_date": "2024-06-04",
             "open": 11, "high": 11, "low": 11, "close": 11,
             "volume": 100, "amount": 1100, "turnover_rate": 1.0},
            {"stock_code": "000002", "trade_date": "2024-06-04",
             "open": 19, "high": 19, "low": 19, "close": 19,
             "volume": 100, "amount": 1900, "turnover_rate": 1.0},
        ]
        db.insert("factor_values", pd.DataFrame(fv_rows), snapshot_time=snap)
        db.insert("daily_price", pd.DataFrame(price_rows), snapshot_time=snap)

        tracker = ICTracker(db)
        ic_df = tracker.compute_ic_series(
            "tiny", "2024-06-03", "2024-06-03",
            forward_days=1, persist=False,
        )
        assert not ic_df.empty
        assert np.isnan(ic_df.iloc[0]["ic"]), "少于5只应返回 NaN"

    def test_no_forward_data(self, db):
        """最后一天无 forward 数据 → IC = NaN。"""
        snap = datetime(2024, 6, 1, 10, 0, 0)
        stocks = [f"00000{i}" for i in range(10)]
        fv_rows = []
        price_rows = []
        for i, code in enumerate(stocks):
            fv_rows.append({
                "factor_name": "lastday", "stock_code": code,
                "trade_date": "2024-06-10", "factor_value": float(i + 1),
            })
            price_rows.append({
                "stock_code": code, "trade_date": "2024-06-10",
                "open": 10, "high": 10, "low": 10, "close": 10 + i,
                "volume": 100, "amount": 1000, "turnover_rate": 1.0,
            })
        db.insert("factor_values", pd.DataFrame(fv_rows), snapshot_time=snap)
        db.insert("daily_price", pd.DataFrame(price_rows), snapshot_time=snap)

        tracker = ICTracker(db)
        ic_df = tracker.compute_ic_series(
            "lastday", "2024-06-10", "2024-06-10",
            forward_days=1, persist=False,
        )
        assert not ic_df.empty
        assert np.isnan(ic_df.iloc[0]["ic"]), "无 forward 数据应返回 NaN"
