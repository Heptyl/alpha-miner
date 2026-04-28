"""推荐引擎测试 — tests/test_recommend.py

覆盖:
1. 技术分析模块 (technical.py)
2. 推荐引擎核心逻辑 (recommend.py)
3. CLI 入口基本验证
"""

import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from src.data.storage import Storage
from src.strategy.technical import TechnicalAnalysis, compute_technical
from src.strategy.recommend import (
    DailyRecommendation,
    RecommendEngine,
    StockRecommendation,
)


# ── Fixtures ──────────────────────────────────────────────

@pytest.fixture
def tmp_db(tmp_path):
    """创建临时测试数据库。"""
    db_path = tmp_path / "test.db"
    db = Storage(str(db_path))
    db.init_db()
    return db


@pytest.fixture
def sample_price_df():
    """20天模拟K线数据。"""
    dates = pd.date_range("2026-04-01", periods=20, freq="D").strftime("%Y-%m-%d")
    np.random.seed(42)
    base_price = 10.0
    close_prices = base_price + np.cumsum(np.random.randn(20) * 0.2)
    close_prices = np.maximum(close_prices, 5.0)  # 确保价格为正

    df = pd.DataFrame({
        "stock_code": "000001",
        "trade_date": dates,
        "open": close_prices - np.abs(np.random.randn(20) * 0.1),
        "high": close_prices + np.abs(np.random.randn(20) * 0.3),
        "low": close_prices - np.abs(np.random.randn(20) * 0.3),
        "close": close_prices,
        "volume": np.random.randint(1e6, 1e7, 20).astype(float),
    })
    return df


@pytest.fixture
def populated_db(tmp_db):
    """填充测试数据的数据库。"""
    now = datetime.now()
    today = now.strftime("%Y-%m-%d")

    # 涨停池
    zt_df = pd.DataFrame({
        "stock_code": ["000001", "000002", "000003", "600001", "600002"],
        "trade_date": [today] * 5,
        "name": ["测试A", "测试B", "测试C", "测试D", "测试E"],
        "industry": ["半导体", "新能源", "AI", "半导体", "医药"],
        "consecutive_zt": [3, 2, 1, 1, 1],
        "open_count": [0, 0, 1, 0, 2],
        "amount": [5e8, 3e8, 2e8, 8e8, 1e8],
        "circulation_mv": [10e9, 5e9, 3e9, 20e9, 1e9],
    })
    tmp_db.insert("zt_pool", zt_df, snapshot_time=now)

    # 因子值
    factor_rows = []
    factors = {
        "000001": {"theme_crowding": 0.85, "leader_clarity": 0.80, "lhb_institution": 2e8, "turnover_rank": 0.4, "consecutive_board": 3},
        "000002": {"theme_crowding": 0.70, "leader_clarity": 0.60, "lhb_institution": 1e8, "turnover_rank": 0.5, "consecutive_board": 2},
        "000003": {"theme_crowding": 0.50, "leader_clarity": 0.40, "lhb_institution": 0, "turnover_rank": 0.7, "consecutive_board": 1},
        "600001": {"theme_crowding": 0.90, "leader_clarity": 0.75, "lhb_institution": 3e8, "turnover_rank": 0.3, "consecutive_board": 1},
        "600002": {"theme_crowding": 0.30, "leader_clarity": 0.20, "lhb_institution": -1e8, "turnover_rank": 0.9, "consecutive_board": 1},
    }
    fv_df_data = []
    for code, fdict in factors.items():
        for fname, fval in fdict.items():
            fv_df_data.append({
                "factor_name": fname,
                "stock_code": code,
                "trade_date": today,
                "factor_value": fval,
            })
    fv_df = pd.DataFrame(fv_df_data)
    tmp_db.insert("factor_values", fv_df, snapshot_time=now)

    # 日K线（为每只股票创建10天数据）
    np.random.seed(42)
    price_df_data = []
    for code in ["000001", "000002", "000003", "600001", "600002"]:
        for i in range(10):
            d = (now - timedelta(days=10-i)).strftime("%Y-%m-%d")
            price = 10.0 + np.random.randn() * 0.5
            price_df_data.append({
                "stock_code": code,
                "trade_date": d,
                "open": price - 0.1,
                "high": price + 0.3,
                "low": price - 0.3,
                "close": price,
                "volume": float(np.random.randint(1e6, 1e7)),
                "amount": float(np.random.randint(1e7, 1e8)),
                "turnover_rate": np.random.uniform(1, 10),
                "pre_close": price - 0.05,
            })
    price_df = pd.DataFrame(price_df_data)
    tmp_db.insert("daily_price", price_df, snapshot_time=now)

    # 概念映射
    concept_df = pd.DataFrame({
        "stock_code": ["000001", "000001", "000002", "600001", "600001"],
        "concept_name": ["芯片", "5G", "光伏", "芯片", "国产替代"],
    })
    tmp_db.insert("concept_mapping", concept_df, snapshot_time=now)

    # 资金流向（只使用 schema 中定义的列）
    fund_df = pd.DataFrame({
        "stock_code": ["000001", "000002", "600001"],
        "trade_date": [today] * 3,
        "super_large_net": [1e8, 3e7, -2e7],
        "large_net": [5e7, 2e7, -1e7],
        "medium_net": [3e7, 1e7, 0],
        "small_net": [2e7, -1e7, 0],
        "main_net": [2e8, 5e7, -3e7],
    })
    tmp_db.insert("fund_flow", fund_df, snapshot_time=now)

    return tmp_db, today, now


# ── 技术分析测试 ───────────────────────────────────────────

class TestTechnicalAnalysis:
    """测试 src/strategy/technical.py。"""

    def test_compute_basic(self, sample_price_df):
        """基本技术分析计算。"""
        ta = compute_technical(sample_price_df)
        assert ta is not None
        assert ta.current_price > 0
        assert ta.support_price > 0
        assert ta.resistance_price > 0
        assert ta.buy_zone_low > 0
        assert ta.buy_zone_high > 0
        assert ta.buy_zone_low <= ta.buy_zone_high
        assert 0 <= ta.momentum_score <= 1
        assert ta.trend in ("上涨", "震荡", "下跌")

    def test_ma_calculation(self, sample_price_df):
        """均线计算正确。"""
        ta = compute_technical(sample_price_df)
        assert ta is not None
        assert ta.ma5 is not None
        assert ta.ma10 is not None
        assert ta.ma20 is not None
        # MA5 应接近最近5天均价
        expected_ma5 = sample_price_df["close"].iloc[-5:].mean()
        assert abs(ta.ma5 - expected_ma5) < 0.01

    def test_insufficient_data(self):
        """数据不足时返回 None。"""
        short_df = pd.DataFrame({
            "trade_date": ["2026-04-01"],
            "close": [10.0],
            "high": [10.5],
            "low": [9.5],
            "volume": [1e6],
        })
        assert compute_technical(short_df) is None

    def test_volume_ratio(self, sample_price_df):
        """量比计算。"""
        ta = compute_technical(sample_price_df)
        assert ta is not None
        assert ta.volume_ratio > 0

    def test_atr_positive(self, sample_price_df):
        """ATR 为正数。"""
        ta = compute_technical(sample_price_df)
        assert ta is not None
        assert ta.atr >= 0

    def test_to_dict(self, sample_price_df):
        """to_dict 序列化。"""
        ta = compute_technical(sample_price_df)
        d = ta.to_dict()
        assert "current_price" in d
        assert "buy_zone_low" in d
        assert "buy_zone_high" in d
        assert d["trend"] in ("上涨", "震荡", "下跌")

    def test_uptrend_detection(self):
        """上涨趋势检测。"""
        dates = pd.date_range("2026-04-01", periods=20, freq="D").strftime("%Y-%m-%d")
        prices = np.linspace(10, 15, 20)  # 持续上涨
        df = pd.DataFrame({
            "stock_code": "000001",
            "trade_date": dates,
            "open": prices - 0.1,
            "high": prices + 0.3,
            "low": prices - 0.3,
            "close": prices,
            "volume": np.full(20, 1e7),
        })
        ta = compute_technical(df)
        assert ta is not None
        assert ta.trend == "上涨"
        assert ta.momentum_score > 0.5

    def test_downtrend_detection(self):
        """下跌趋势检测。"""
        dates = pd.date_range("2026-04-01", periods=20, freq="D").strftime("%Y-%m-%d")
        prices = np.linspace(15, 10, 20)  # 持续下跌
        df = pd.DataFrame({
            "stock_code": "000001",
            "trade_date": dates,
            "open": prices + 0.1,
            "high": prices + 0.3,
            "low": prices - 0.3,
            "close": prices,
            "volume": np.full(20, 1e7),
        })
        ta = compute_technical(df)
        assert ta is not None
        assert ta.trend == "下跌"


# ── 推荐引擎测试 ───────────────────────────────────────────

class TestRecommendEngine:
    """测试 src/strategy/recommend.py。"""

    def test_basic_recommend(self, populated_db):
        """基本推荐流程。"""
        db, today, now = populated_db
        as_of = now + timedelta(days=1)

        engine = RecommendEngine(db)
        report = engine.recommend(as_of, today, top_n=5)

        assert isinstance(report, DailyRecommendation)
        assert report.trade_date == today
        assert len(report.stocks) <= 5
        assert report.stocks  # 应该有推荐

    def test_top_n_constraint(self, populated_db):
        """推荐数量限制。"""
        db, today, now = populated_db
        as_of = now + timedelta(days=1)

        engine = RecommendEngine(db)
        report = engine.recommend(as_of, today, top_n=3)
        assert len(report.stocks) <= 3

    def test_sorted_by_score(self, populated_db):
        """按综合分降序排列。"""
        db, today, now = populated_db
        as_of = now + timedelta(days=1)

        engine = RecommendEngine(db)
        report = engine.recommend(as_of, today, top_n=5)

        scores = [s.composite_score for s in report.stocks]
        assert scores == sorted(scores, reverse=True)

    def test_price_levels_set(self, populated_db):
        """买入点位已设置。"""
        db, today, now = populated_db
        as_of = now + timedelta(days=1)

        engine = RecommendEngine(db)
        report = engine.recommend(as_of, today, top_n=5)

        for stock in report.stocks:
            assert stock.buy_price > 0
            assert stock.stop_loss > 0
            assert stock.target_price > 0
            assert stock.buy_zone_low > 0
            assert stock.buy_zone_high > 0
            assert stock.buy_zone_low <= stock.buy_zone_high
            assert stock.target_price > stock.buy_price

    def test_signal_levels(self, populated_db):
        """信号等级正确。"""
        db, today, now = populated_db
        as_of = now + timedelta(days=1)

        engine = RecommendEngine(db)
        report = engine.recommend(as_of, today, top_n=5)

        for stock in report.stocks:
            assert stock.signal_level in ("A", "B", "C")

    def test_reasons_and_risks(self, populated_db):
        """推荐理由和风险提示非空。"""
        db, today, now = populated_db
        as_of = now + timedelta(days=1)

        engine = RecommendEngine(db)
        report = engine.recommend(as_of, today, top_n=5)

        for stock in report.stocks:
            assert len(stock.reasons) > 0

    def test_no_data(self, tmp_db):
        """无数据时返回空报告。"""
        as_of = datetime.now() + timedelta(days=1)
        today = datetime.now().strftime("%Y-%m-%d")

        engine = RecommendEngine(tmp_db)
        report = engine.recommend(as_of, today, top_n=5)

        assert len(report.stocks) == 0
        assert report.zt_count == 0

    def test_market_regime(self, populated_db):
        """市场状态判断。"""
        db, today, now = populated_db
        as_of = now + timedelta(days=1)

        engine = RecommendEngine(db)
        report = engine.recommend(as_of, today, top_n=5)

        assert report.market_regime in ("强势市场", "弱势市场", "震荡市场", "数据不足")

    def test_hot_industries(self, populated_db):
        """热门板块统计。"""
        db, today, now = populated_db
        as_of = now + timedelta(days=1)

        engine = RecommendEngine(db)
        report = engine.recommend(as_of, today, top_n=5)

        # 有涨停数据时应该有热门板块
        assert isinstance(report.hot_industries, list)

    def test_to_dict_serialization(self, populated_db):
        """字典序列化。"""
        db, today, now = populated_db
        as_of = now + timedelta(days=1)

        engine = RecommendEngine(db)
        report = engine.recommend(as_of, today, top_n=5)

        d = report.to_dict()
        assert "trade_date" in d
        assert "stocks" in d
        assert isinstance(d["stocks"], list)

    def test_to_text_output(self, populated_db):
        """纯文本输出。"""
        db, today, now = populated_db
        as_of = now + timedelta(days=1)

        engine = RecommendEngine(db)
        report = engine.recommend(as_of, today, top_n=5)

        text = report.to_text()
        assert len(text) > 0
        assert "每日个股推荐" in text
        assert "买入区间" in text
        assert "免责声明" in text


class TestStockRecommendation:
    """测试 StockRecommendation 数据结构。"""

    def test_default_values(self):
        rec = StockRecommendation(
            stock_code="000001",
            stock_name="测试",
            industry="半导体",
            concepts=["芯片"],
        )
        assert rec.composite_score == 0.0
        assert rec.signal_level == ""
        assert rec.buy_price == 0.0
        assert rec.reasons == []

    def test_to_dict(self):
        rec = StockRecommendation(
            stock_code="000001",
            stock_name="测试",
            industry="半导体",
            concepts=["芯片", "5G"],
            composite_score=0.75,
            signal_level="A",
            buy_price=10.50,
            stop_loss=9.50,
            target_price=11.50,
            buy_zone_low=10.20,
            buy_zone_high=10.55,
            reasons=["龙头地位清晰"],
        )
        d = rec.to_dict()
        assert d["stock_code"] == "000001"
        assert d["composite_score"] == 0.75
        assert len(d["concepts"]) == 2
        assert d["signal_level"] == "A"
