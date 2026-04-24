"""测试数据层健壮性。"""
import pytest
import pandas as pd
import numpy as np
from datetime import datetime

from src.data.storage import Storage


@pytest.fixture
def db(tmp_path):
    db = Storage(str(tmp_path / "test.db"))
    db.init_db()
    return db


class TestStorageEdgeCases:
    """Storage 必须优雅处理边界情况。"""

    def test_query_empty_table_returns_empty_df(self, db):
        """查询空表必须返回空 DataFrame，不崩溃。"""
        df = db.query("daily_price", datetime(2024, 6, 1))
        assert isinstance(df, pd.DataFrame)
        assert df.empty

    def test_query_invalid_table_raises(self, db):
        """查询不存在的表必须抛异常，不能静默返回空。"""
        with pytest.raises(Exception):
            db.query("nonexistent_table", datetime(2024, 6, 1))

    def test_query_range_empty_returns_empty_df(self, db):
        """query_range 在无数据时返回空 DataFrame。"""
        df = db.query_range("daily_price", datetime(2024, 6, 1), lookback_days=5)
        assert isinstance(df, pd.DataFrame)
        assert df.empty

    def test_insert_empty_df_is_noop(self, db):
        """插入空 DataFrame 不应崩溃。"""
        snap = datetime(2024, 6, 1, 10, 0, 0)
        db.insert("daily_price", pd.DataFrame(), snapshot_time=snap)
        # 验证表仍然为空
        df = db.query("daily_price", datetime(2024, 6, 2))
        assert df.empty

    def test_insert_nan_values_handled(self, db):
        """插入含 NaN 的数据不应崩溃。"""
        snap = datetime(2024, 6, 1, 10, 0, 0)
        df = pd.DataFrame([{
            "stock_code": "000001",
            "trade_date": "2024-06-01",
            "open": np.nan,
            "high": 11.0,
            "low": np.nan,
            "close": 10.5,
            "volume": 1000000,
            "amount": np.nan,
            "turnover_rate": 5.0,
        }])
        db.insert("daily_price", df, snapshot_time=snap)

        # 能查回数据
        result = db.query("daily_price", datetime(2024, 6, 2))
        assert len(result) == 1
        # NaN 应被存储（SQLite 会存为 NULL）
        assert pd.isna(result.iloc[0]["open"])

    def test_query_with_where_params(self, db):
        """带 WHERE 参数的查询必须正确过滤。"""
        snap = datetime(2024, 6, 1, 10, 0, 0)
        df = pd.DataFrame([
            {"stock_code": "000001", "trade_date": "2024-06-01",
             "open": 10, "high": 11, "low": 9, "close": 10,
             "volume": 1000, "amount": 10000, "turnover_rate": 5.0},
            {"stock_code": "000002", "trade_date": "2024-06-01",
             "open": 20, "high": 22, "low": 19, "close": 21,
             "volume": 2000, "amount": 42000, "turnover_rate": 3.0},
        ])
        db.insert("daily_price", df, snapshot_time=snap)

        # 查 000001
        result = db.query("daily_price", datetime(2024, 6, 2),
                          where="stock_code = ?", params=("000001",))
        assert len(result) == 1
        assert result.iloc[0]["stock_code"] == "000001"

    def test_snapshot_time_isolation(self, db):
        """query 的 as_of 必须严格隔离未来数据。"""
        snap1 = datetime(2024, 6, 1, 10, 0, 0)
        snap2 = datetime(2024, 6, 2, 10, 0, 0)

        df1 = pd.DataFrame([{
            "stock_code": "000001", "trade_date": "2024-06-01",
            "open": 10, "high": 11, "low": 9, "close": 10,
            "volume": 1000, "amount": 10000, "turnover_rate": 5.0,
        }])
        db.insert("daily_price", df1, snapshot_time=snap1)

        df2 = pd.DataFrame([{
            "stock_code": "000001", "trade_date": "2024-06-02",
            "open": 11, "high": 12, "low": 10, "close": 11,
            "volume": 1100, "amount": 12100, "turnover_rate": 5.5,
        }])
        db.insert("daily_price", df2, snapshot_time=snap2)

        # as_of = 6/1.5 应只看到 snap1 的数据
        result = db.query("daily_price", datetime(2024, 6, 1, 12, 0, 0))
        assert len(result) == 1
        assert result.iloc[0]["trade_date"] == "2024-06-01"

    def test_double_init_is_safe(self, db):
        """重复 init_db 不应崩溃（IF NOT EXISTS 保护）。"""
        db.init_db()
        db.init_db()
        # 不崩溃即可
