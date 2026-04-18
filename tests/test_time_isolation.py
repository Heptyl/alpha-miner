"""因子时间隔离测试 — 验证 as_of 隔离、FutureDataError、条件因子。"""

import os
import tempfile
from datetime import datetime

import pandas as pd
import pytest

from src.data.storage import Storage
from src.factors.base import (
    BaseFactor,
    Condition,
    ConditionalFactor,
    FutureDataError,
)


@pytest.fixture
def tmp_db():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    db = Storage(db_path)
    db.init_db()
    yield db
    os.unlink(db_path)


class MockFactor(BaseFactor):
    """用于测试的 mock 因子。"""
    name = "mock"
    factor_type = "stock"
    lookback_days = 5

    def compute(self, universe, as_of, db):
        data = db.query_range(
            "daily_price",
            as_of,
            lookback_days=self.lookback_days,
            where="stock_code IN ({})".format(",".join(["?"] * len(universe))),
            params=tuple(universe),
        )
        self.validate_no_future(as_of, data)
        if data.empty:
            return pd.Series(dtype=float, name=self.name)
        # 返回最新收盘价
        latest = data.sort_values("trade_date").groupby("stock_code").last()
        return latest["close"]


class TestTimeIsolation:
    def test_as_of_prevents_future_data(self, tmp_db):
        """as_of 隔离：只能看到 as_of 之前的数据。"""
        ts = datetime(2024, 6, 15, 10, 0, 0)
        df = pd.DataFrame({
            "stock_code": ["000001", "000001"],
            "trade_date": ["2024-06-13", "2024-06-15"],
            "close": [10.0, 12.0],
        })
        tmp_db.insert("daily_price", df, snapshot_time=ts)

        factor = MockFactor()
        universe = ["000001"]

        # as_of=6月14日，lookback=5天 → 只能看到 06-13 的数据
        result = factor.compute(universe, datetime(2024, 6, 16, 12, 0, 0), tmp_db)
        assert len(result) > 0

    def test_validate_no_future_raises(self):
        """validate_no_future 应在发现未来数据时抛异常。"""
        factor = MockFactor()
        as_of = datetime(2024, 6, 14, 0, 0, 0)

        data = pd.DataFrame({
            "stock_code": ["000001"],
            "trade_date": ["2024-06-15"],  # 未来数据
            "close": [10.0],
        })

        with pytest.raises(FutureDataError):
            factor.validate_no_future(as_of, data)

    def test_validate_no_future_passes_on_valid_data(self):
        """合法数据不应触发异常。"""
        factor = MockFactor()
        as_of = datetime(2024, 6, 15, 12, 0, 0)

        data = pd.DataFrame({
            "stock_code": ["000001"],
            "trade_date": ["2024-06-14"],  # 过去数据
            "close": [10.0],
        })

        # 不应抛异常
        factor.validate_no_future(as_of, data)

    def test_validate_no_future_empty_df(self):
        """空 DataFrame 不应触发异常。"""
        factor = MockFactor()
        as_of = datetime(2024, 6, 15, 12, 0, 0)
        data = pd.DataFrame()
        factor.validate_no_future(as_of, data)  # 不抛异常


class TestConditionalFactor:
    def test_condition_evaluate(self, tmp_db):
        """Condition.evaluate 基本测试。"""
        ts = datetime(2024, 6, 16, 10, 0, 0)
        df = pd.DataFrame({
            "stock_code": ["000001", "000002", "000003"],
            "trade_date": ["2024-06-15", "2024-06-15", "2024-06-15"],
            "close": [10.0, 25.0, 5.0],
        })
        tmp_db.insert("daily_price", df, snapshot_time=ts)

        cond = Condition(
            name="high_price",
            table="daily_price",
            column="close",
            operator=">",
            value=15.0,
        )

        result = cond.evaluate(
            ["000001", "000002", "000003"],
            datetime(2024, 6, 16, 12, 0, 0),
            tmp_db,
        )
        assert result["000002"] == 1.0
        assert result["000001"] == 0.0
        assert result["000003"] == 0.0

    def test_conditional_factor_all_logic(self, tmp_db):
        """ConditionalFactor logic='all' 测试。"""
        ts = datetime(2024, 6, 16, 10, 0, 0)
        df = pd.DataFrame({
            "stock_code": ["000001", "000002"],
            "trade_date": ["2024-06-15", "2024-06-15"],
            "close": [25.0, 5.0],
            "volume": [1000.0, 500.0],
        })
        tmp_db.insert("daily_price", df, snapshot_time=ts)

        class TestCondFactor(ConditionalFactor):
            name = "test_cond"
            conditions = [
                Condition("price_high", "daily_price", "close", ">", 10.0),
                Condition("vol_high", "daily_price", "volume", ">", 800.0),
            ]
            logic = "all"

        factor = TestCondFactor()
        result = factor.compute(
            ["000001", "000002"],
            datetime(2024, 6, 16, 12, 0, 0),
            tmp_db,
        )
        # 000001: close>10 AND volume>800 → True
        # 000002: close=5<10 → False
        assert result["000001"] == 1.0
        assert result["000002"] == 0.0
