"""公式因子单元测试 — 用 mock 数据验证 compute 逻辑。"""

from datetime import datetime, timedelta

import pandas as pd
import pytest

from src.data.storage import Storage
from src.factors.formula import (
    ZtDtRatioFactor,
    ConsecutiveBoardFactor,
    MainFlowIntensityFactor,
    TurnoverRankFactor,
    LhbInstitutionFactor,
)


@pytest.fixture
def db(tmp_path):
    """创建临时数据库并插入测试数据。

    snapshot_time 设为 as_of 前一天，确保时间隔离查询可见。
    """
    storage = Storage(str(tmp_path / "test.db"))
    storage.init_db()

    as_of = datetime(2024, 6, 14, 15, 0, 0)
    snap = datetime(2024, 6, 13, 10, 0, 0)  # snapshot 在 as_of 之前
    date_str = "2024-06-14"

    # daily_price
    storage.insert("daily_price", pd.DataFrame([
        {"stock_code": "000001", "trade_date": date_str, "open": 10.0, "high": 11.0,
         "low": 9.0, "close": 10.5, "volume": 1000, "amount": 10500, "turnover_rate": 3.5},
        {"stock_code": "000002", "trade_date": date_str, "open": 20.0, "high": 22.0,
         "low": 18.0, "close": 21.0, "volume": 2000, "amount": 42000, "turnover_rate": 8.2},
        {"stock_code": "000003", "trade_date": date_str, "open": 15.0, "high": 16.5,
         "low": 13.5, "close": 16.0, "volume": 1500, "amount": 24000, "turnover_rate": 5.0},
        {"stock_code": "000004", "trade_date": date_str, "open": 8.0, "high": 8.5,
         "low": 7.0, "close": 7.2, "volume": 800, "amount": 5760, "turnover_rate": 1.2},
    ]), snapshot_time=snap)

    # zt_pool
    storage.insert("zt_pool", pd.DataFrame([
        {"stock_code": "000001", "trade_date": date_str, "consecutive_zt": 3,
         "amount": 50000, "circulation_mv": 200000, "open_count": 0, "zt_stats": "3/3"},
        {"stock_code": "000002", "trade_date": date_str, "consecutive_zt": 1,
         "amount": 80000, "circulation_mv": 500000, "open_count": 1, "zt_stats": "1/1"},
    ]), snapshot_time=snap)

    # fund_flow
    storage.insert("fund_flow", pd.DataFrame([
        {"stock_code": "000001", "trade_date": date_str, "super_large_net": 5000,
         "large_net": 3000, "medium_net": -1000, "small_net": -7000, "main_net": 8000},
        {"stock_code": "000002", "trade_date": date_str, "super_large_net": -2000,
         "large_net": -1000, "medium_net": 500, "small_net": 2500, "main_net": -3000},
    ]), snapshot_time=snap)

    # lhb_detail
    storage.insert("lhb_detail", pd.DataFrame([
        {"stock_code": "000001", "trade_date": date_str, "buy_amount": 3000,
         "sell_amount": 1000, "net_amount": 2000,
         "buy_depart": "机构专用", "sell_depart": "东方财富证券拉萨",
         "reason": "日涨幅偏离值达7%"},
        {"stock_code": "000002", "trade_date": date_str, "buy_amount": 5000,
         "sell_amount": 2000, "net_amount": 3000,
         "buy_depart": "华泰证券总部", "sell_depart": "机构专用",
         "reason": "日涨幅偏离值达7%"},
    ]), snapshot_time=snap)

    return storage


class TestZtDtRatio:
    def test_basic(self, db):
        factor = ZtDtRatioFactor()
        result = factor.compute(["000001"], datetime(2024, 6, 14, 15, 0, 0), db)
        assert len(result) == 1
        # 2涨停(000001, 000002) + 1跌停(000004: -10%) = 2/(2+1) = 0.667
        assert abs(result.iloc[0] - 0.6667) < 0.01

    def test_no_data(self, db):
        factor = ZtDtRatioFactor()
        # 未来日期不应有数据
        result = factor.compute(["000001"], datetime(2024, 6, 20, 15, 0, 0), db)
        assert result.iloc[0] == 0.5  # 无数据时默认 0.5


class TestConsecutiveBoard:
    def test_basic(self, db):
        factor = ConsecutiveBoardFactor()
        universe = ["000001", "000002", "000003"]
        result = factor.compute(universe, datetime(2024, 6, 14, 15, 0, 0), db)
        assert result["000001"] == 3.0  # 3连板
        assert result["000002"] == 1.0  # 1连板
        assert result["000003"] == 0.0  # 非涨停

    def test_empty(self, db):
        factor = ConsecutiveBoardFactor()
        result = factor.compute(["999999"], datetime(2024, 6, 14, 15, 0, 0), db)
        assert result["999999"] == 0.0


class TestMainFlowIntensity:
    def test_basic(self, db):
        factor = MainFlowIntensityFactor()
        result = factor.compute(["000001", "000002"], datetime(2024, 6, 14, 15, 0, 0), db)
        # 000001: main_net=8000, amount=10500 => 0.76
        assert result["000001"] > 0
        # 000002: main_net=-3000, amount=42000 => -0.07
        assert result["000002"] < 0


class TestTurnoverRank:
    def test_basic(self, db):
        factor = TurnoverRankFactor()
        universe = ["000001", "000002", "000003", "000004"]
        result = factor.compute(universe, datetime(2024, 6, 14, 15, 0, 0), db)
        # 000002 换手率最高(8.2)，应该排名最高
        assert result["000002"] > result["000004"]
        # 所有值应在 [0, 1]
        assert (result >= 0).all() and (result <= 1).all()


class TestLhbInstitution:
    def test_basic(self, db):
        factor = LhbInstitutionFactor()
        result = factor.compute(["000001", "000002"], datetime(2024, 6, 14, 15, 0, 0), db)
        # 000001: 机构买入3000, 非机构卖出 → net > 0
        assert result["000001"] > 0
        # 000002: 非机构买入5000, 机构卖出2000 → net < 0
        assert result["000002"] < 0
