"""叙事因子单元测试 — 用 mock 数据验证 compute 逻辑。"""

from datetime import datetime, timedelta

import pandas as pd
import pytest

from src.data.storage import Storage
from src.factors.narrative import (
    ThemeLifecycleFactor,
    NarrativeVelocityFactor,
    ThemeCrowdingFactor,
    LeaderClarityFactor,
)


AS_OF = datetime(2024, 6, 14, 15, 0, 0)
SNAP = datetime(2024, 6, 13, 10, 0, 0)
DATE_STR = "2024-06-14"


@pytest.fixture
def db(tmp_path):
    storage = Storage(str(tmp_path / "test.db"))
    storage.init_db()

    # 概念映射
    storage.insert("concept_mapping", pd.DataFrame([
        {"stock_code": "000001", "concept_name": "AI"},
        {"stock_code": "000002", "concept_name": "AI"},
        {"stock_code": "000003", "concept_name": "新能源"},
        {"stock_code": "000004", "concept_name": "新能源"},
    ]), snapshot_time=SNAP)

    # zt_pool
    storage.insert("zt_pool", pd.DataFrame([
        {"stock_code": "000001", "trade_date": DATE_STR, "consecutive_zt": 3,
         "amount": 50000, "circulation_mv": 200000, "open_count": 0, "zt_stats": "3/3"},
        {"stock_code": "000002", "trade_date": DATE_STR, "consecutive_zt": 1,
         "amount": 20000, "circulation_mv": 100000, "open_count": 1, "zt_stats": "1/1"},
        {"stock_code": "000003", "trade_date": DATE_STR, "consecutive_zt": 2,
         "amount": 30000, "circulation_mv": 150000, "open_count": 0, "zt_stats": "2/2"},
    ]), snapshot_time=SNAP)

    # concept_daily (预聚合)
    storage.insert("concept_daily", pd.DataFrame([
        {"concept_name": "AI", "trade_date": DATE_STR, "zt_count": 2,
         "leader_code": "000001", "leader_consecutive": 3},
        {"concept_name": "新能源", "trade_date": DATE_STR, "zt_count": 1,
         "leader_code": "000003", "leader_consecutive": 2},
    ]), snapshot_time=SNAP)

    # news (用 publish_time 代替 trade_date)
    storage.insert("news", pd.DataFrame([
        {"news_id": "a1", "stock_code": "000001", "title": "AI突破",
         "publish_time": DATE_STR, "content": "AI技术重大突破",
         "sentiment_score": 0.8},
        {"news_id": "a2", "stock_code": "000001", "title": "AI进展",
         "publish_time": DATE_STR, "content": "AI继续发展",
         "sentiment_score": 0.7},
    ]), snapshot_time=SNAP)

    # 3天前的新闻
    prev_date = "2024-06-11"
    storage.insert("news", pd.DataFrame([
        {"news_id": "b1", "stock_code": "000001", "title": "AI旧闻",
         "publish_time": prev_date, "content": "AI旧消息",
         "sentiment_score": 0.5},
    ]), snapshot_time=SNAP)

    return storage


class TestThemeLifecycle:
    def test_basic(self, db):
        factor = ThemeLifecycleFactor()
        universe = ["000001", "000002", "000003", "000004"]
        result = factor.compute(universe, AS_OF, db)
        # AI 概念有2个涨停 → 应该有正分数
        assert result["000001"] > 0
        assert result["000002"] > 0
        # 000004 属于新能源概念，有1涨停 → 0.37
        assert result["000004"] > 0


class TestNarrativeVelocity:
    def test_basic(self, db):
        factor = NarrativeVelocityFactor()
        result = factor.compute(["000001", "000002"], AS_OF, db)
        # 000001: 今天2条, 3天前1条 → (2-1)/1 = 1.0
        assert result["000001"] > 0
        # 000002: 没有新闻 → 0
        assert result["000002"] == 0.0


class TestThemeCrowding:
    def test_basic(self, db):
        factor = ThemeCrowdingFactor()
        universe = ["000001", "000002", "000003", "000004"]
        result = factor.compute(universe, AS_OF, db)
        # 所有值应在 [0, 1]
        assert (result >= 0).all() and (result <= 1).all()
        # 000004 属于新能源概念，该概念有涨停 → 反拥挤分数 < 0.5
        assert result["000004"] < 0.5


class TestLeaderClarity:
    def test_basic(self, db):
        factor = LeaderClarityFactor()
        universe = ["000001", "000002", "000003", "000004"]
        result = factor.compute(universe, AS_OF, db)
        # AI: 000001(50000) vs 000002(20000) → clarity = 2.5/3 → 0.83
        assert result["000001"] > 0.5
        # 000004 属于新能源概念，只有1只涨停(000003) → clarity = 1.0
        assert result["000004"] > 0
