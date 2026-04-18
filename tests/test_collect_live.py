"""数据采集 Live 测试 — 需要 akshare 网络连接。

标记 @pytest.mark.live，默认跳过：
  pytest -m live tests/test_collect_live.py -v
"""

import pytest
from datetime import datetime, timedelta

from src.data.storage import Storage
from src.data.sources import akshare_price, akshare_zt_pool, akshare_lhb, akshare_fund_flow
from src.data.collector import collect_date


def _last_trade_date() -> str:
    """返回最近一个可能的交易日（简单向前推，不考虑节假日）。"""
    today = datetime.now()
    # 如果是周末，退到周五
    if today.weekday() == 5:  # Saturday
        today -= timedelta(days=1)
    elif today.weekday() == 6:  # Sunday
        today -= timedelta(days=2)
    return today.strftime("%Y-%m-%d")


@pytest.mark.live
class TestPriceLive:
    def test_fetch_daily_price(self):
        df = akshare_price.fetch(_last_trade_date())
        assert not df.empty, "日K线数据不应为空"
        assert "stock_code" in df.columns
        assert "close" in df.columns
        assert len(df) > 100  # A股至少有几千只

    def test_save_and_query(self, tmp_path):
        db = Storage(str(tmp_path / "test.db"))
        db.init_db()
        df = akshare_price.fetch(_last_trade_date())
        if not df.empty:
            count = akshare_price.save(df, db)
            assert count > 0
            result = db.query("daily_price", datetime(2099, 1, 1))
            assert len(result) > 0


@pytest.mark.live
class TestZtPoolLive:
    def test_fetch_zt_pool(self):
        df = akshare_zt_pool.fetch_zt_pool(_last_trade_date())
        # 涨停池可能为空（非交易日），但不应报错
        if not df.empty:
            assert "stock_code" in df.columns
            assert "consecutive_zt" in df.columns

    def test_fetch_zb_pool(self):
        df = akshare_zt_pool.fetch_zb_pool(_last_trade_date())
        if not df.empty:
            assert "stock_code" in df.columns


@pytest.mark.live
class TestLhbLive:
    def test_fetch_lhb(self):
        df = akshare_lhb.fetch(_last_trade_date())
        if not df.empty:
            assert "stock_code" in df.columns
            assert "net_amount" in df.columns


@pytest.mark.live
class TestFundFlowLive:
    def test_fetch_fund_flow(self):
        df = akshare_fund_flow.fetch(_last_trade_date())
        if not df.empty:
            assert "stock_code" in df.columns
            assert "main_net" in df.columns


@pytest.mark.live
class TestCollectDate:
    def test_collect_one_day(self):
        db = Storage("data/test_live.db")
        db.init_db()
        results = collect_date(_last_trade_date(), db)
        assert isinstance(results, dict)
        assert "daily_price" in results
        assert "zt_pool" in results
        print(f"Results: {results}")
