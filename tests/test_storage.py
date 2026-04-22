"""数据库存储层测试。"""

import os
import tempfile
from datetime import datetime

import pandas as pd
import pytest

from src.data.storage import Storage


@pytest.fixture
def tmp_db():
    """创建临时数据库。"""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    db = Storage(db_path)
    db.init_db()
    yield db
    os.unlink(db_path)


class TestInitDb:
    def test_init_db_creates_tables(self, tmp_db):
        """建表成功后，查 SQLite master 应有所有表。"""
        tables = tmp_db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        )
        table_names = [t["name"] for t in tables]
        assert "daily_price" in table_names
        assert "zt_pool" in table_names
        assert "zb_pool" in table_names
        assert "market_emotion" in table_names
        assert "factor_values" in table_names
        assert "ic_series" in table_names
        assert "mining_log" in table_names
        assert "market_scripts" in table_names


class TestSchemaMigration:
    def test_news_table_has_new_columns(self, tmp_db):
        """init_db 后 news 表应有 news_type 和 classify_confidence 列。"""
        import sqlite3
        conn = tmp_db._get_conn()
        try:
            cols = [row["name"] for row in conn.execute("PRAGMA table_info(news)")]
            assert "news_type" in cols
            assert "classify_confidence" in cols
        finally:
            conn.close()

    def test_init_db_idempotent(self, tmp_db):
        """多次调用 init_db 不报错（ALTER TABLE 幂等）。"""
        tmp_db.init_db()
        tmp_db.init_db()

    def test_market_scripts_table_exists(self, tmp_db):
        """market_scripts 表存在。"""
        tables = tmp_db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='market_scripts'"
        )
        assert len(list(tables)) == 1


class TestInsert:
    def test_insert_adds_snapshot_time(self, tmp_db):
        """插入 DataFrame 后，每行都应有 snapshot_time。"""
        ts = datetime(2024, 6, 15, 10, 0, 0)
        df = pd.DataFrame({
            "stock_code": ["000001", "000002"],
            "trade_date": ["2024-06-15", "2024-06-15"],
            "close": [10.0, 20.0],
        })
        count = tmp_db.insert("daily_price", df, snapshot_time=ts)
        assert count == 2

        # 用 ts 之后的时间查询
        result = tmp_db.query("daily_price", datetime(2024, 6, 15, 12, 0, 0))
        assert len(result) == 2
        assert "snapshot_time" in result.columns
        assert result["snapshot_time"].notna().all()
        # 验证 snapshot_time 值
        assert result.iloc[0]["snapshot_time"].startswith("2024-06-15 10:00:00")


class TestQueryTimeIsolation:
    def test_query_only_returns_data_before_as_of(self, tmp_db):
        """时间隔离：as_of 只返回之前插入的数据。"""
        t1 = datetime(2024, 6, 15, 10, 0, 0)
        t2 = datetime(2024, 6, 15, 14, 0, 0)

        # T1 时刻插入一批数据
        df1 = pd.DataFrame({
            "stock_code": ["000001"],
            "trade_date": ["2024-06-14"],
            "close": [10.0],
        })
        tmp_db.insert("daily_price", df1, snapshot_time=t1)

        # T2 时刻插入另一批
        df2 = pd.DataFrame({
            "stock_code": ["000002"],
            "trade_date": ["2024-06-15"],
            "close": [20.0],
        })
        tmp_db.insert("daily_price", df2, snapshot_time=t2)

        # as_of=T1+1min 应只能看到 000001
        as_of_t1 = datetime(2024, 6, 15, 10, 1, 0)
        result_t1 = tmp_db.query("daily_price", as_of=as_of_t1)
        assert len(result_t1) == 1
        assert result_t1.iloc[0]["stock_code"] == "000001"

        # as_of=T2+1min 应能看到两条
        as_of_t2 = datetime(2024, 6, 15, 14, 1, 0)
        result_t2 = tmp_db.query("daily_price", as_of=as_of_t2)
        assert len(result_t2) == 2

    def test_query_with_where_clause(self, tmp_db):
        """带额外 WHERE 条件的查询。"""
        ts = datetime(2024, 6, 15, 10, 0, 0)
        df = pd.DataFrame({
            "stock_code": ["000001", "000002", "000003"],
            "trade_date": ["2024-06-15", "2024-06-15", "2024-06-15"],
            "close": [10.0, 20.0, 30.0],
        })
        tmp_db.insert("daily_price", df, snapshot_time=ts)

        result = tmp_db.query(
            "daily_price",
            datetime(2024, 6, 15, 12, 0, 0),
            where="close > ?",
            params=(15.0,),
        )
        assert len(result) == 2


class TestQueryRange:
    def test_query_range_filters_by_date(self, tmp_db):
        """日期范围过滤。"""
        ts = datetime(2024, 6, 16, 10, 0, 0)
        df = pd.DataFrame({
            "stock_code": ["000001"] * 5,
            "trade_date": [
                "2024-06-10", "2024-06-11", "2024-06-12",
                "2024-06-13", "2024-06-15"
            ],
            "close": [10.0, 11.0, 12.0, 13.0, 15.0],
        })
        tmp_db.insert("daily_price", df, snapshot_time=ts)

        # 查 6月10-14日范围（lookback 4 天，从 6月14日往前）
        # as_of 必须晚于 snapshot_time
        result = tmp_db.query_range(
            "daily_price",
            datetime(2024, 6, 16, 12, 0, 0),
            lookback_days=4,
        )
        # 日期范围：end_date=2024-06-16, start_date=2024-06-12
        # 但我们想看 6月10-14... 让我重新算
        # as_of = 2024-06-16 12:00, lookback=4
        # start = 06-12, end = 06-16
        # trade_date 在 06-12~06-16 范围内的：06-12, 06-13, 06-15
        dates = set(result["trade_date"].tolist())
        assert "2024-06-15" in dates
        assert "2024-06-10" not in dates  # 超出4天窗口
        assert "2024-06-11" not in dates

    def test_query_range_with_stock_filter(self, tmp_db):
        """带股票过滤的范围查询。"""
        ts = datetime(2024, 6, 16, 10, 0, 0)
        df = pd.DataFrame({
            "stock_code": ["000001", "000002"],
            "trade_date": ["2024-06-15", "2024-06-15"],
            "close": [10.0, 20.0],
        })
        tmp_db.insert("daily_price", df, snapshot_time=ts)

        result = tmp_db.query_range(
            "daily_price",
            datetime(2024, 6, 16, 12, 0, 0),
            lookback_days=1,
            where="stock_code = ?",
            params=("000001",),
        )
        assert len(result) == 1
        assert result.iloc[0]["stock_code"] == "000001"
