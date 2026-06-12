"""9维选股器测试。"""
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock

# 确保项目根目录在 path
import sys
ROOT = Path(__file__).parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
import numpy as np

from src.screener.base import ScreenerBase, ScreenResult


# ===== 基类测试 =====

class TestScreenResult:
    def test_is_tradeable_normal(self):
        r = ScreenResult(stock_code="000001")
        assert r.is_tradeable is True

    def test_is_tradeable_kcb(self):
        r = ScreenResult(stock_code="688001")
        assert r.is_tradeable is False

    def test_is_tradeable_kcb2(self):
        r = ScreenResult(stock_code="689001")
        assert r.is_tradeable is False

    def test_is_tradeable_bjs(self):
        r = ScreenResult(stock_code="830001")
        assert r.is_tradeable is False

    def test_is_tradeable_bjs2(self):
        r = ScreenResult(stock_code="900001")
        assert r.is_tradeable is False

    def test_is_tradeable_cy(self):
        """创业板可交易"""
        r = ScreenResult(stock_code="300001")
        assert r.is_tradeable is True


class TestScreenerBase:
    def test_calc_ma(self):
        s = pd.Series([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
        ma5 = ScreenerBase.calc_ma(s, 5)
        assert pd.isna(ma5.iloc[3])
        assert ma5.iloc[4] == 3.0  # (1+2+3+4+5)/5

    def test_calc_volume_ratio(self):
        vol = pd.Series([100, 100, 100, 100, 100, 200])
        ratio = ScreenerBase.calc_volume_ratio(vol, 5)
        # rolling(5) on [100,100,100,100,200] = 120, so 200/120 = 1.667
        assert abs(ratio.iloc[5] - 200 / 120) < 0.01

    def test_calc_rsi(self):
        close = pd.Series([10, 11, 12, 11, 13, 14, 15, 14, 16, 17,
                           16, 18, 19, 20, 19, 21, 22, 23, 22, 24])
        rsi = ScreenerBase.calc_rsi(close, 14)
        assert 0 <= rsi.iloc[-1] <= 100

    def test_calc_macd(self):
        close = pd.Series(range(50, 100), dtype=float)
        dif, dea, hist = ScreenerBase.calc_macd(close)
        assert len(dif) == 50
        assert len(dea) == 50

    def test_is_st_stock(self):
        assert ScreenerBase.is_st_stock("ST康美") is True
        assert ScreenerBase.is_st_stock("*ST必康") is True
        assert ScreenerBase.is_st_stock("退市海医") is True
        assert ScreenerBase.is_st_stock("贵州茅台") is False
        assert ScreenerBase.is_st_stock("") is False

    def test_make_result(self):
        base = ScreenerBase(":memory:")
        base.get_stock_name = MagicMock(return_value="测试股")
        r = base.make_result("000001", "2026-05-08", 0.75, ["理由1"])
        assert r.stock_code == "000001"
        assert r.stock_name == "测试股"
        assert r.score == 0.75
        assert r.signal_strength == "A"

    def test_make_result_score_b(self):
        base = ScreenerBase(":memory:")
        base.get_stock_name = MagicMock(return_value="测试")
        r = base.make_result("000001", "2026-05-08", 0.45, [])
        assert r.signal_strength == "B"

    def test_make_result_score_c(self):
        base = ScreenerBase(":memory:")
        base.get_stock_name = MagicMock(return_value="测试")
        r = base.make_result("000001", "2026-05-08", 0.20, [])
        assert r.signal_strength == "C"

    def test_make_result_score_clamp(self):
        """分数限制在0-1"""
        base = ScreenerBase(":memory:")
        base.get_stock_name = MagicMock(return_value="测试")
        r = base.make_result("000001", "2026-05-08", 1.5, [])
        assert r.score == 1.0
        r = base.make_result("000001", "2026-05-08", -0.5, [])
        assert r.score == 0.0


# ===== 策略单元测试 (mock数据) =====

def _make_price_df(n=60, start_price=10.0, trend="up"):
    """构造测试用K线数据。"""
    dates = pd.date_range("2026-03-01", periods=n, freq="D")
    if trend == "up":
        close = start_price * (1 + pd.Series(range(n)) * 0.01)
    elif trend == "down":
        close = start_price * (1 - pd.Series(range(n)) * 0.005)
    else:
        close = pd.Series([start_price] * n)

    high = close * 1.02
    low = close * 0.98
    volume = pd.Series([100000] * n)

    return pd.DataFrame({
        "trade_date": dates.strftime("%Y-%m-%d"),
        "open": close * 0.99,
        "high": high,
        "low": low,
        "close": close,
        "pre_close": close.shift(1).fillna(start_price),
        "volume": volume,
        "amount": volume * close,
    })


class TestTrendBreakoutScreener:
    def test_import(self):
        from src.screener.trend_breakout import TrendBreakoutScreener
        s = TrendBreakoutScreener(":memory:")
        assert s.name == "趋势突破"
        assert s.dimension == 1

    def test_analyze_uptrend(self):
        from src.screener.trend_breakout import TrendBreakoutScreener
        s = TrendBreakoutScreener(":memory:")
        df = _make_price_df(90, 10.0, "up")
        # 给最后一天放量
        df.loc[df.index[-1], "volume"] = 500000
        res = s._analyze("000001", df, "2026-05-08")
        assert res is not None
        assert res.score > 0.2

    def test_analyze_flat(self):
        """横盘不应有高得分"""
        from src.screener.trend_breakout import TrendBreakoutScreener
        s = TrendBreakoutScreener(":memory:")
        df = _make_price_df(90, 10.0, "flat")
        res = s._analyze("000001", df, "2026-05-08")
        # 横盘得分应该很低或被过滤
        if res is not None:
            assert res.score < 0.4


class TestVolumePullbackScreener:
    def test_import(self):
        from src.screener.volume_pullback import VolumePullbackScreener
        s = VolumePullbackScreener(":memory:")
        assert s.dimension == 2

    def test_analyze_pullback(self):
        """构造先涨后缩量回调的数据"""
        from src.screener.volume_pullback import VolumePullbackScreener
        s = VolumePullbackScreener(":memory:")
        n = 40
        dates = pd.date_range("2026-03-01", periods=n, freq="D")
        close = list(range(10, 30)) + list(range(30, 27, -1)) * 4  # 涨后回调
        close = pd.Series(close[:n])
        volume = [100000] * 20 + [200000] * 10 + [80000] * 10  # 先放量后缩量

        df = pd.DataFrame({
            "trade_date": dates.strftime("%Y-%m-%d"),
            "open": close * 0.99,
            "high": close * 1.02,
            "low": close * 0.98,
            "close": close,
            "pre_close": close.shift(1).fillna(10),
            "volume": pd.Series(volume),
            "amount": pd.Series(volume) * close,
        })
        res = s._analyze("000001", df, "2026-05-08")
        if res is not None:
            assert "缩量" in " ".join(res.reasons)


class TestCapitalFlowScreener:
    def test_import(self):
        from src.screener.capital_flow import CapitalFlowScreener
        s = CapitalFlowScreener(":memory:")
        assert s.dimension == 3


class TestSectorRotationScreener:
    def test_import(self):
        from src.screener.sector_rotation import SectorRotationScreener
        s = SectorRotationScreener(":memory:")
        assert s.dimension == 4


class TestVolumePriceScreener:
    def test_import(self):
        from src.screener.volume_price import VolumePriceScreener
        s = VolumePriceScreener(":memory:")
        assert s.dimension == 5

    def test_low_price_filtered(self):
        """低价股(<3元)应被过滤"""
        from src.screener.volume_price import VolumePriceScreener
        s = VolumePriceScreener(":memory:")
        df = _make_price_df(30, 2.0, "flat")
        res = s._analyze("000001", df, "2026-05-08")
        assert res is None


class TestFundamentalScreener:
    def test_import(self):
        from src.screener.fundamental import FundamentalScreener
        s = FundamentalScreener(":memory:")
        assert s.dimension == 6


class TestMainForceScreener:
    def test_import(self):
        from src.screener.main_force import MainForceScreener
        s = MainForceScreener(":memory:")
        assert s.dimension == 7


class TestIndustryScreener:
    def test_import(self):
        from src.screener.industry import IndustryScreener
        s = IndustryScreener(":memory:")
        assert s.dimension == 8


class TestRiskControlScreener:
    def test_import(self):
        from src.screener.risk_control import RiskControlScreener
        s = RiskControlScreener(":memory:")
        assert s.dimension == 9

    def test_filter_results_empty(self):
        from src.screener.risk_control import RiskControlScreener
        s = RiskControlScreener(":memory:")
        assert s.filter_results([], "2026-05-08") == []


class TestScreenerEngine:
    def test_import(self):
        from src.screener.engine import ScreenerEngine
        e = ScreenerEngine(":memory:")
        assert len(e.screeners) == 9

    def test_weights_sum_to_one(self):
        from src.screener.engine import WEIGHTS
        assert abs(sum(WEIGHTS.values()) - 1.0) < 0.01

    def test_stock_score_to_dict(self):
        from src.screener.engine import StockScore
        s = StockScore(
            stock_code="000001",
            stock_name="测试",
            total_score=0.75,
            signal_level="A",
        )
        d = s.to_dict()
        assert d["stock_code"] == "000001"
        assert d["total_score"] == 0.75
