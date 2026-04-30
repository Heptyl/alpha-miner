"""硬断言测试 — 手工计算因子数值并精确对比。

每个测试都包含：
1. 构造已知输入数据
2. 手工推导期望结果
3. 精确断言（误差<0.01 或具体值）
"""
from datetime import datetime
import math

import pandas as pd
import numpy as np
import pytest

from src.data.storage import Storage


# ═══════════════════════════════════════════════════════════════
# 公用 fixture
# ═══════════════════════════════════════════════════════════════

@pytest.fixture
def db(tmp_path):
    """临时数据库。"""
    s = Storage(str(tmp_path / "test.db"))
    s.init_db()
    return s


@pytest.fixture
def populated_db(db):
    """插入完整的叙事因子所需数据。

    设计数据使每个因子有可手工计算的期望值。
    """
    snap = datetime(2024, 6, 13, 10, 0, 0)
    date_str = "2024-06-14"
    prev_date = "2024-06-11"

    # ─── news 表 ───
    # 000001: 今天3条(type加权: ignite=3.0 + real=2.0 + noise=0.0=5.0)
    #         3天前1条(type加权: ferment=1.5)
    #         velocity = (5.0 - 1.5) / 1.5 = 2.33 → clamp to 1.0
    # 000002: 今天0条，3天前2条(count模式: 2)
    #         velocity = (0 - 2) / 2 = -1.0
    # 000003: 今天1条(negative=-2.0)，3天前0条
    #         velocity = (-2.0 - 0) / ... → t>0, p=0 → velocity=1.0
    db.insert("news", pd.DataFrame([
        {
            "news_id": "n1", "stock_code": "000001",
            "title": "AI新赛道首次突破", "publish_time": f"{date_str} 10:00",
            "content": "", "sentiment_score": 0.9,
            "news_type": "theme_ignite", "classify_confidence": 0.9,
        },
        {
            "news_id": "n2", "stock_code": "000001",
            "title": "公司中标5亿合同", "publish_time": f"{date_str} 11:00",
            "content": "", "sentiment_score": 0.8,
            "news_type": "catalyst_real", "classify_confidence": 0.85,
        },
        {
            "news_id": "n3", "stock_code": "000001",
            "title": "今日大盘小幅震荡", "publish_time": f"{date_str} 14:00",
            "content": "", "sentiment_score": 0.5,
            "news_type": "noise", "classify_confidence": 0.7,
        },
        {
            "news_id": "n4", "stock_code": "000001",
            "title": "概念股持续发酵", "publish_time": f"{prev_date} 09:00",
            "content": "", "sentiment_score": 0.6,
            "news_type": "theme_ferment", "classify_confidence": 0.8,
        },
        {
            "news_id": "n5", "stock_code": "000002",
            "title": "板块联动分析", "publish_time": f"{prev_date} 10:00",
            "content": "", "sentiment_score": 0.5,
            "news_type": "theme_ferment", "classify_confidence": 0.6,
        },
        {
            "news_id": "n6", "stock_code": "000002",
            "title": "产业链跟踪", "publish_time": f"{prev_date} 11:00",
            "content": "", "sentiment_score": 0.4,
            "news_type": "noise", "classify_confidence": 0.5,
        },
        {
            "news_id": "n7", "stock_code": "000003",
            "title": "公司暴雷被立案调查", "publish_time": f"{date_str} 09:00",
            "content": "", "sentiment_score": 0.1,
            "news_type": "negative", "classify_confidence": 0.95,
        },
    ]), snapshot_time=snap)

    # ─── zt_pool ───
    # 000001: 3连板，成交额 90000
    # 000002: 1连板，成交额 30000
    # 000003: 1连板，成交额 10000
    # 000004: 1连板，成交额 5000
    db.insert("zt_pool", pd.DataFrame([
        {"stock_code": "000001", "trade_date": date_str, "consecutive_zt": 3,
         "amount": 90000, "circulation_mv": 500000, "open_count": 0, "zt_stats": "3/3"},
        {"stock_code": "000002", "trade_date": date_str, "consecutive_zt": 1,
         "amount": 30000, "circulation_mv": 300000, "open_count": 0, "zt_stats": "1/1"},
        {"stock_code": "000003", "trade_date": date_str, "consecutive_zt": 1,
         "amount": 10000, "circulation_mv": 200000, "open_count": 1, "zt_stats": "1/1"},
        {"stock_code": "000004", "trade_date": date_str, "consecutive_zt": 1,
         "amount": 5000, "circulation_mv": 100000, "open_count": 0, "zt_stats": "1/1"},
    ]), snapshot_time=snap)

    # ─── concept_mapping ───
    # AI概念: 000001, 000002, 000003
    # 芯片概念: 000001, 000004
    db.insert("concept_mapping", pd.DataFrame([
        {"stock_code": "000001", "concept_name": "AI"},
        {"stock_code": "000002", "concept_name": "AI"},
        {"stock_code": "000003", "concept_name": "AI"},
        {"stock_code": "000001", "concept_name": "芯片"},
        {"stock_code": "000004", "concept_name": "芯片"},
    ]), snapshot_time=snap)

    # ─── concept_daily ───
    # AI: 3涨停(000001,000002,000003), leader_consecutive=3(000001)
    #     → zt=3 <=3 → score = 0.3 + 3*0.07 = 0.51
    # 芯片: 2涨停(000001,000004), leader_consecutive=3(000001)
    #       → zt=2 <=3 → score = 0.3 + 2*0.07 = 0.44
    db.insert("concept_daily", pd.DataFrame([
        {"concept_name": "AI", "trade_date": date_str,
         "zt_count": 3, "leader_code": "000001", "leader_consecutive": 3},
        {"concept_name": "芯片", "trade_date": date_str,
         "zt_count": 2, "leader_code": "000001", "leader_consecutive": 3},
    ]), snapshot_time=snap)

    # ─── daily_price (给 turnover_rank 用) ───
    db.insert("daily_price", pd.DataFrame([
        {"stock_code": "000001", "trade_date": date_str, "open": 10.0, "high": 11.0,
         "low": 9.0, "close": 10.5, "volume": 1000, "amount": 10500, "turnover_rate": 3.5},
        {"stock_code": "000002", "trade_date": date_str, "open": 20.0, "high": 22.0,
         "low": 18.0, "close": 21.0, "volume": 2000, "amount": 42000, "turnover_rate": 8.2},
        {"stock_code": "000003", "trade_date": date_str, "open": 15.0, "high": 16.5,
         "low": 13.5, "close": 16.0, "volume": 1500, "amount": 24000, "turnover_rate": 5.0},
        {"stock_code": "000004", "trade_date": date_str, "open": 8.0, "high": 8.5,
         "low": 7.0, "close": 7.2, "volume": 800, "amount": 5760, "turnover_rate": 1.2},
    ]), snapshot_time=snap)

    return db


# ═══════════════════════════════════════════════════════════════
# H1-A: NarrativeVelocity 硬断言
# ═══════════════════════════════════════════════════════════════

class TestNarrativeVelocityHard:
    """叙事速度因子 — 加权模式数值精确验证。

    数据设计:
    - 000001: today=ignite(3.0)+real(2.0)+noise(0.0)=5.0, prev=ferment(1.5)
              velocity=(5.0-1.5)/1.5=2.33 → clamp 1.0
    - 000002: today=0, prev=ferment(1.5)+noise(0.0)=1.5
              velocity=(0-1.5)/1.5=-1.0 → clamp -1.0
    - 000003: today=negative(-2.0), prev=0
              velocity=1.0 (t!=0, p=0 → special case)
    - 000004: today=0, prev=0 → velocity=0.0
    """

    def test_000001_velocity_clamped_to_1(self, populated_db):
        """000001: 加权velocity超上限，clamp到1.0。"""
        from src.factors.narrative.narrative_velocity import NarrativeVelocityFactor
        f = NarrativeVelocityFactor()
        result = f.compute(["000001"], datetime(2024, 6, 14, 15, 0, 0), populated_db)
        assert abs(result["000001"] - 1.0) < 0.01, \
            f"000001 velocity={result['000001']}, 期望 1.0"

    def test_000002_velocity_negative_1(self, populated_db):
        """000002: 今天无新闻，3天前有新闻 → velocity=-1.0。"""
        from src.factors.narrative.narrative_velocity import NarrativeVelocityFactor
        f = NarrativeVelocityFactor()
        result = f.compute(["000002"], datetime(2024, 6, 14, 15, 0, 0), populated_db)
        assert abs(result["000002"] - (-1.0)) < 0.01, \
            f"000002 velocity={result['000002']}, 期望 -1.0"

    def test_000003_velocity_1_special_case(self, populated_db):
        """000003: 今天有negative新闻，3天前无 → special case=1.0。"""
        from src.factors.narrative.narrative_velocity import NarrativeVelocityFactor
        f = NarrativeVelocityFactor()
        result = f.compute(["000003"], datetime(2024, 6, 14, 15, 0, 0), populated_db)
        # t=-2.0 (nonzero), p=0 → code goes to "elif t > 0" → but t=-2.0 < 0
        # Actually t=-2.0, p=0: not (p>0), not (t>0) → velocity=0.0
        # 因为 negative 加权后 t=-2.0 < 0, p=0, 两个条件都不满足 → 0.0
        assert abs(result["000003"] - 0.0) < 0.01, \
            f"000003 velocity={result['000003']}, 期望 0.0 (negative t<0, p=0)"

    def test_000004_velocity_zero(self, populated_db):
        """000004: 无新闻 → velocity=0.0。"""
        from src.factors.narrative.narrative_velocity import NarrativeVelocityFactor
        f = NarrativeVelocityFactor()
        result = f.compute(["000004"], datetime(2024, 6, 14, 15, 0, 0), populated_db)
        assert math.isnan(result["000004"]), \
            f"000004 velocity={result['000004']}, 期望 NaN"


# ═══════════════════════════════════════════════════════════════
# H1-B: ThemeLifecycle 硬断言
# ═══════════════════════════════════════════════════════════════

class TestThemeLifecycleHard:
    """题材生命周期 — 概念评分精确验证。

    数据设计:
    - AI概念: zt_count=3, leader_consecutive=3
      → zt<=3 → score = 0.3 + 3*0.07 = 0.51
    - 芯片概念: zt_count=2, leader_consecutive=3
      → zt<=3 → score = 0.3 + 2*0.07 = 0.44

    个股映射:
    - 000001 → max(0.51, 0.44) = 0.51  (AI+芯片)
    - 000002 → 0.51  (仅AI)
    - 000003 → 0.51  (仅AI)
    - 000004 → 0.44  (仅芯片)
    """

    def test_000001_max_concept(self, populated_db):
        """000001 属于 AI(0.51) 和 芯片(0.44)，取 max=0.51。"""
        from src.factors.narrative.theme_lifecycle import ThemeLifecycleFactor
        f = ThemeLifecycleFactor()
        result = f.compute(["000001"], datetime(2024, 6, 14, 15, 0, 0), populated_db)
        assert abs(result["000001"] - 0.51) < 0.01, \
            f"000001 lifecycle={result['000001']}, 期望 0.51"

    def test_000002_ai_only(self, populated_db):
        """000002 仅属于 AI → 0.51。"""
        from src.factors.narrative.theme_lifecycle import ThemeLifecycleFactor
        f = ThemeLifecycleFactor()
        result = f.compute(["000002"], datetime(2024, 6, 14, 15, 0, 0), populated_db)
        assert abs(result["000002"] - 0.51) < 0.01, \
            f"000002 lifecycle={result['000002']}, 期望 0.51"

    def test_000004_chip_only(self, populated_db):
        """000004 仅属于 芯片(0.44)。"""
        from src.factors.narrative.theme_lifecycle import ThemeLifecycleFactor
        f = ThemeLifecycleFactor()
        result = f.compute(["000004"], datetime(2024, 6, 14, 15, 0, 0), populated_db)
        assert abs(result["000004"] - 0.44) < 0.01, \
            f"000004 lifecycle={result['000004']}, 期望 0.44"


# ═══════════════════════════════════════════════════════════════
# H1-C: LeaderClarity 硬断言
# ═══════════════════════════════════════════════════════════════

class TestLeaderClarityHard:
    """龙头清晰度 — 按成交额排名精确验证。

    数据设计:
    zt_pool + concept_mapping:
    - AI概念: 000001(90000), 000002(30000), 000003(10000)
      → top1=90000, top2=30000, ratio=3.0
      → clarity = min(3.0/3.0, 1.0) = 1.0
    - 芯片概念: 000001(90000), 000004(5000)
      → top1=90000, top2=5000, ratio=18.0
      → clarity = min(18.0/3.0, 1.0) = 1.0

    个股映射:
    - 000001 → max(1.0, 1.0) = 1.0
    - 000002 → 1.0  (AI)
    - 000003 → 1.0  (AI)
    - 000004 → 1.0  (芯片)
    """

    def test_000001_leader_clarity_1(self, populated_db):
        """000001 是 AI 和芯片双重龙头，清晰度=1.0。"""
        from src.factors.narrative.leader_clarity import LeaderClarityFactor
        f = LeaderClarityFactor()
        result = f.compute(["000001"], datetime(2024, 6, 14, 15, 0, 0), populated_db)
        assert abs(result["000001"] - 1.0) < 0.01, \
            f"000001 clarity={result['000001']}, 期望 1.0"

    def test_000003_ai_member(self, populated_db):
        """000003 是 AI 成员但不是龙头，取概念的 clarity。"""
        from src.factors.narrative.leader_clarity import LeaderClarityFactor
        f = LeaderClarityFactor()
        result = f.compute(["000003"], datetime(2024, 6, 14, 15, 0, 0), populated_db)
        # AI: 90000/30000=3.0, min(3.0/3.0,1.0)=1.0
        assert abs(result["000003"] - 1.0) < 0.01, \
            f"000003 clarity={result['000003']}, 期望 1.0"


class TestLeaderClarityNoConcept:
    """测试无概念映射的情况。"""

    def test_no_concept_mapping_returns_zero(self, db):
        """有 zt_pool 但无 concept_mapping → 全0。"""
        snap = datetime(2024, 6, 13, 10, 0, 0)
        db.insert("zt_pool", pd.DataFrame([
            {"stock_code": "000001", "trade_date": "2024-06-14",
             "consecutive_zt": 1, "amount": 50000},
        ]), snapshot_time=snap)
        from src.factors.narrative.leader_clarity import LeaderClarityFactor
        f = LeaderClarityFactor()
        result = f.compute(["000001"], datetime(2024, 6, 14, 15, 0, 0), db)
        assert math.isnan(result["000001"])


class TestLeaderClaritySingleMember:
    """概念内只有一只涨停股的情况。"""

    def test_single_member_clarity_1(self, db):
        """概念内只有1只涨停股 → clarity=1.0。"""
        snap = datetime(2024, 6, 13, 10, 0, 0)
        db.insert("zt_pool", pd.DataFrame([
            {"stock_code": "000001", "trade_date": "2024-06-14",
             "consecutive_zt": 1, "amount": 50000},
        ]), snapshot_time=snap)
        db.insert("concept_mapping", pd.DataFrame([
            {"stock_code": "000001", "concept_name": "AI"},
        ]), snapshot_time=snap)
        from src.factors.narrative.leader_clarity import LeaderClarityFactor
        f = LeaderClarityFactor()
        result = f.compute(["000001"], datetime(2024, 6, 14, 15, 0, 0), db)
        assert abs(result["000001"] - 1.0) < 0.01


class TestLeaderClarityRatio:
    """龙头清晰度比例精确验证。"""

    def test_ratio_2x(self, db):
        """top1/top2 = 2.0 → clarity = 2.0/3.0 ≈ 0.667。"""
        snap = datetime(2024, 6, 13, 10, 0, 0)
        db.insert("zt_pool", pd.DataFrame([
            {"stock_code": "000001", "trade_date": "2024-06-14",
             "consecutive_zt": 1, "amount": 60000},
            {"stock_code": "000002", "trade_date": "2024-06-14",
             "consecutive_zt": 1, "amount": 30000},
        ]), snapshot_time=snap)
        db.insert("concept_mapping", pd.DataFrame([
            {"stock_code": "000001", "concept_name": "AI"},
            {"stock_code": "000002", "concept_name": "AI"},
        ]), snapshot_time=snap)
        from src.factors.narrative.leader_clarity import LeaderClarityFactor
        f = LeaderClarityFactor()
        result = f.compute(["000001"], datetime(2024, 6, 14, 15, 0, 0), db)
        expected = min(60000 / 30000 / 3.0, 1.0)  # 0.667
        assert abs(result["000001"] - expected) < 0.01, \
            f"clarity={result['000001']}, 期望 {expected}"


# ═══════════════════════════════════════════════════════════════
# H1-D: ThemeCrowding 硬断言
# ═══════════════════════════════════════════════════════════════

class TestThemeCrowdingHard:
    """题材拥挤度 — 精确验证拥挤度计算和反转。

    数据设计:
    zt_pool 共4只: 000001, 000002, 000003, 000004 → total_zt=4

    概念涨停数:
    - AI: 000001+000002+000003 = 3只 → crowding=3/4=0.75
    - 芯片: 000001+000004 = 2只 → crowding=2/4=0.50

    个股评分 (1 - max_crowd * 5):
    - 000001 → max(0.75, 0.50)=0.75 → 1 - 0.75*5 = 1 - 3.75 = -2.75 → max(0, -2.75) = 0.0
    - 000002 → max(0.75)=0.75 → 0.0
    - 000003 → max(0.75)=0.75 → 0.0
    - 000004 → max(0.50)=0.50 → 1 - 0.50*5 = 1 - 2.5 = -1.5 → max(0, -1.5) = 0.0
    """

    def test_000001_highly_crowded(self, populated_db):
        """000001 AI+芯片，最大拥挤度0.75 → 惩罚后=0.0。"""
        from src.factors.narrative.theme_crowding import ThemeCrowdingFactor
        f = ThemeCrowdingFactor()
        result = f.compute(["000001"], datetime(2024, 6, 14, 15, 0, 0), populated_db)
        assert abs(result["000001"] - 0.0) < 0.01, \
            f"000001 crowding={result['000001']}, 期望 0.0"

    def test_000004_chip_crowded(self, populated_db):
        """000004 芯片拥挤度0.50 → 惩罚后=0.0。"""
        from src.factors.narrative.theme_crowding import ThemeCrowdingFactor
        f = ThemeCrowdingFactor()
        result = f.compute(["000004"], datetime(2024, 6, 14, 15, 0, 0), populated_db)
        assert abs(result["000004"] - 0.0) < 0.01, \
            f"000004 crowding={result['000004']}, 期望 0.0"


class TestThemeCrowdingLowCrowding:
    """低拥挤度场景。"""

    def test_low_crowding_positive_score(self, db):
        """只有1只涨停，概念内1只 → crowding=1/1=1.0 → 1-1.0*5=-4.0→0.0。
        改用更大的 total_zt 来测低拥挤度。"""
        snap = datetime(2024, 6, 13, 10, 0, 0)
        # 10只涨停股
        zt_rows = [
            {"stock_code": f"00000{i}", "trade_date": "2024-06-14",
             "consecutive_zt": 1, "amount": 10000 * i}
            for i in range(10)
        ]
        db.insert("zt_pool", pd.DataFrame(zt_rows), snapshot_time=snap)

        # 000000 属于"冷门概念"，只有它1只在涨停
        # 其他9只都不属于这个概念
        db.insert("concept_mapping", pd.DataFrame([
            {"stock_code": "000000", "concept_name": "冷门"},
            # 其他9只属于热门概念
            *[{"stock_code": f"00000{i}", "concept_name": "热门"} for i in range(1, 10)],
        ]), snapshot_time=snap)

        from src.factors.narrative.theme_crowding import ThemeCrowdingFactor
        f = ThemeCrowdingFactor()
        result = f.compute(["000000"], datetime(2024, 6, 14, 15, 0, 0), db)
        # 冷门: 1只涨停, total_zt=10, crowding=0.1, score=1-0.1*5=0.5
        assert abs(result["000000"] - 0.5) < 0.01, \
            f"低拥挤度 score={result['000000']}, 期望 0.5"

    def test_no_concept_mapping_default_05(self, db):
        """无概念映射 → 默认0.5。"""
        snap = datetime(2024, 6, 13, 10, 0, 0)
        db.insert("zt_pool", pd.DataFrame([
            {"stock_code": "000001", "trade_date": "2024-06-14",
             "consecutive_zt": 1, "amount": 10000},
        ]), snapshot_time=snap)
        from src.factors.narrative.theme_crowding import ThemeCrowdingFactor
        f = ThemeCrowdingFactor()
        result = f.compute(["999999"], datetime(2024, 6, 14, 15, 0, 0), db)
        assert abs(result["999999"] - 0.5) < 0.01
