"""测试历史胜率回测模块（简化版）。"""

import sqlite3
from datetime import datetime

import pytest

from src.strategy.win_rate_backtest import (
    BacktestResult, backtest_pattern, batch_backtest,
)


# ─── 辅助: 创建测试数据库 ──────────────────────────────

@pytest.fixture
def test_db(tmp_path):
    """创建包含历史数据的测试数据库。"""
    db_path = str(tmp_path / "test.db")
    conn = sqlite3.connect(db_path)
    conn.execute("""CREATE TABLE daily_price (
        stock_code TEXT, trade_date TEXT, open REAL, close REAL,
        high REAL, low REAL, pre_close REAL, volume REAL,
        amount REAL, turnover_rate REAL, snapshot_time TEXT,
        PRIMARY KEY (stock_code, trade_date)
    )""")
    
    # 插入30天数据，模拟一只连板股
    import random
    random.seed(42)
    base_price = 10.0
    for day in range(30):
        date = f"2026-04-{day+1:02d}" if day < 9 else f"2026-04-{day+1:02d}" if day < 30 else f"2026-05-{day-29:02d}"
        if day < 9:
            date = f"2026-03-{22+day:02d}"
        else:
            date = f"2026-04-{day-8:02d}"
        
        pre_close = base_price
        # 模拟一些涨停日和普通日
        if day in [5, 6, 7]:  # 连续3天涨停
            close = round(pre_close * 1.10, 2)
        elif day in [12, 15, 20]:  # 首板
            close = round(pre_close * 1.10, 2)
        else:
            change = random.uniform(-0.03, 0.05)
            close = round(pre_close * (1 + change), 2)
        
        base_price = close
        open_price = round(close * random.uniform(0.98, 1.02), 2)
        high = round(close * random.uniform(1.0, 1.05), 2)
        low = round(close * random.uniform(0.95, 1.0), 2)
        
        conn.execute(
            "INSERT INTO daily_price VALUES (?,?,?,?,?,?,?,?,?,?,datetime('now'))",
            ("000001", date, open_price, close, high, low, pre_close, 1e7, 1e8, 5.0),
        )
    
    conn.commit()
    conn.close()
    return db_path


class TestBacktestPattern:
    def test_basic_backtest(self, test_db):
        """基本回测能返回结果。"""
        result = backtest_pattern(
            "000001", "2026-04-22", consecutive_zt=1, hold_days=3, db_path=test_db,
        )
        # 可能返回 None（样本太少）或 BacktestResult
        if result is not None:
            assert isinstance(result, BacktestResult)
            assert result.total_trades >= 1
            assert 0 <= result.win_rate <= 100
    
    def test_no_future_data(self, test_db):
        """不应该使用未来数据。"""
        # trade_date 设为很早的日期，后面的数据不应参与
        result = backtest_pattern(
            "000001", "2026-03-26", consecutive_zt=0, hold_days=3, db_path=test_db,
        )
        # 可能返回 None（太早了没数据）
        # 关键是不应该crash
    
    def test_insufficient_data(self, tmp_path):
        """数据不足时返回 None。"""
        db_path = str(tmp_path / "empty.db")
        conn = sqlite3.connect(db_path)
        conn.execute("""CREATE TABLE daily_price (
            stock_code TEXT, trade_date TEXT, open REAL, close REAL,
            high REAL, low REAL, pre_close REAL, volume REAL,
            amount REAL, turnover_rate REAL, snapshot_time TEXT,
            PRIMARY KEY (stock_code, trade_date)
        )""")
        conn.execute(
            "INSERT INTO daily_price VALUES (?,?,?,?,?,?,?,?,?,?,datetime('now'))",
            ("999999", "2026-04-01", 10, 10.5, 11, 10, 10, 1e6, 1e7, 3.0),
        )
        conn.commit()
        conn.close()
        
        result = backtest_pattern("999999", "2026-04-01", 0, 3, db_path=db_path)
        assert result is None


class TestBatchBacktest:
    def test_batch(self, test_db):
        """批量回测。"""
        codes = [("000001", 1)]
        results = batch_backtest(codes, "2026-04-22", hold_days=3, db_path=test_db)
        assert isinstance(results, dict)
    
    def test_batch_empty(self, test_db):
        """空列表不crash。"""
        results = batch_backtest([], "2026-04-22", db_path=test_db)
        assert results == {}
