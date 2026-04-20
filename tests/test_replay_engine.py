"""复盘引擎测试。"""

import json
import os
import tempfile
from datetime import datetime

import pandas as pd
import pytest

from src.data.storage import Storage
from src.narrative.replay_engine import ReplayEngine, ReplayResult
from src.narrative.script_engine import ScriptEngine, MarketScript


@pytest.fixture
def tmp_db_with_script():
    """创建临时数据库，并插入昨日剧本和今日数据。"""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    db = Storage(db_path)
    db.init_db()

    ts = datetime(2024, 6, 16, 10, 0, 0)
    as_of = datetime(2024, 6, 20, 12, 0, 0)

    # ===== 昨日数据 (2024-06-14) =====
    # 剧本
    script = MarketScript(
        date="2024-06-14",
        script_title="题材轮动",
        script_narrative="AI退潮，机器人接力",
        theme_verdicts=[
            {"theme": "AI", "stage": "衰退", "verdict": "回避"},
            {"theme": "机器人", "stage": "爆发", "verdict": "关注"},
        ],
        tomorrow_playbook={
            "primary_strategy": "卡位低吸",
            "watch_list": ["机器人", "芯片"],
            "avoid_list": ["AI"],
        },
        risk_alerts=[],
        raw_snapshot={
            "date": "2024-06-14",
            "regime": "题材轮动",
            "emotion_level": "中性",
            "zt_count": 30,
            "dt_count": 3,
            "highest_board": 4,
            "zb_count": 5,
            "board_ladder": [],
            "hot_themes": [],
            "lhb_summary": [],
            "key_news": [],
            "fund_flow_summary": {},
        },
    )
    engine_s = ScriptEngine(db)
    engine_s.save_script(script)

    # 昨日 market_emotion
    db.insert("market_emotion", pd.DataFrame({
        "trade_date": ["2024-06-14"],
        "zt_count": [30],
        "dt_count": [3],
        "highest_board": [4],
        "sentiment_level": ["中性"],
    }), snapshot_time=ts)

    # ===== 今日数据 (2024-06-15) =====
    db.insert("market_emotion", pd.DataFrame({
        "trade_date": ["2024-06-15"],
        "zt_count": [50],
        "dt_count": [2],
        "highest_board": [5],
        "sentiment_level": ["偏强"],
    }), snapshot_time=ts)

    # 昨日 zt_pool（为 regime 判定提供数据）
    db.insert("zt_pool", pd.DataFrame({
        "stock_code": ["000001", "000002", "000003"],
        "trade_date": ["2024-06-14"] * 3,
        "consecutive_zt": [4, 2, 1],
        "amount": [10e8, 5e8, 3e8],
        "circulation_mv": [50e8, 30e8, 20e8],
    }), snapshot_time=ts)

    # 今日 zt_pool
    db.insert("zt_pool", pd.DataFrame({
        "stock_code": ["000001", "000002", "000003", "000004"],
        "trade_date": ["2024-06-15"] * 4,
        "consecutive_zt": [5, 3, 2, 1],
        "amount": [12e8, 6e8, 4e8, 2e8],
        "circulation_mv": [50e8, 30e8, 20e8, 15e8],
    }), snapshot_time=ts)

    # 今日 concept_daily（机器人确实来了）
    db.insert("concept_daily", pd.DataFrame({
        "concept_name": ["机器人", "芯片", "新能源"],
        "trade_date": ["2024-06-15"] * 3,
        "zt_count": [10, 5, 2],
        "leader_code": ["000001", "000002", "000003"],
        "leader_consecutive": [5, 3, 2],
    }), snapshot_time=ts)

    yield db, as_of
    os.unlink(db_path)


class TestReplayBasic:
    """基础复盘测试。"""

    def test_replay_generates_result(self, tmp_db_with_script):
        db, as_of = tmp_db_with_script
        engine = ReplayEngine(db)
        result = engine.replay(as_of, target_date="2024-06-15")

        assert result.date == "2024-06-15"
        assert result.actual_zt_count == 50
        assert result.actual_dt_count == 2
        assert result.actual_highest_board == 5
        assert len(result.narrative) > 0

    def test_playbook_hit_detected(self, tmp_db_with_script):
        """昨日 watch_list 中'机器人'出现在今日题材→命中。"""
        db, as_of = tmp_db_with_script
        engine = ReplayEngine(db)
        result = engine.replay(as_of, target_date="2024-06-15")

        assert "机器人" in result.playbook_hits
        assert "芯片" in result.playbook_hits

    def test_replay_without_yesterday_script(self):
        """无昨日剧本时不崩溃。"""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        db = Storage(db_path)
        db.init_db()

        ts = datetime(2024, 6, 15, 10, 0, 0)
        as_of = datetime(2024, 6, 20, 12, 0, 0)

        db.insert("market_emotion", pd.DataFrame({
            "trade_date": ["2024-06-15"],
            "zt_count": [20],
            "dt_count": [5],
            "highest_board": [3],
            "sentiment_level": ["中性"],
        }), snapshot_time=ts)

        engine = ReplayEngine(db)
        result = engine.replay(as_of, target_date="2024-06-15")

        assert result.date == "2024-06-15"
        assert result.narrative != ""
        os.unlink(db_path)


class TestReplaySurpriseDetection:
    """异常事件检测测试。"""

    def test_extreme_bull_detected(self):
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        db = Storage(db_path)
        db.init_db()

        ts = datetime(2024, 6, 15, 10, 0, 0)
        as_of = datetime(2024, 6, 20, 12, 0, 0)

        db.insert("market_emotion", pd.DataFrame({
            "trade_date": ["2024-06-15"],
            "zt_count": [150],
            "dt_count": [2],
            "highest_board": [8],
            "sentiment_level": ["强"],
        }), snapshot_time=ts)

        engine = ReplayEngine(db)
        result = engine.replay(as_of, target_date="2024-06-15")

        types = [e["type"] for e in result.surprise_events]
        assert "extreme_bull" in types
        os.unlink(db_path)

    def test_ice_age_detected(self):
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        db = Storage(db_path)
        db.init_db()

        ts = datetime(2024, 6, 15, 10, 0, 0)
        as_of = datetime(2024, 6, 20, 12, 0, 0)

        db.insert("market_emotion", pd.DataFrame({
            "trade_date": ["2024-06-15"],
            "zt_count": [5],
            "dt_count": [10],
            "highest_board": [2],
            "sentiment_level": ["弱"],
        }), snapshot_time=ts)

        engine = ReplayEngine(db)
        result = engine.replay(as_of, target_date="2024-06-15")

        types = [e["type"] for e in result.surprise_events]
        assert "ice_age" in types
        os.unlink(db_path)


class TestReplaySaveLoad:
    """复盘结果存取测试。"""

    def test_save_and_load_roundtrip(self, tmp_db_with_script):
        db, as_of = tmp_db_with_script
        engine = ReplayEngine(db)
        result = engine.replay(as_of, target_date="2024-06-15")
        engine.save_replay(result)

        loaded = engine.load_replay("2024-06-15")
        assert loaded is not None
        assert loaded.date == "2024-06-15"
        assert loaded.regime_match == result.regime_match
        assert loaded.playbook_hits == result.playbook_hits

    def test_load_nonexistent_returns_none(self, tmp_db_with_script):
        db, _ = tmp_db_with_script
        engine = ReplayEngine(db)
        assert engine.load_replay("2024-01-01") is None


class TestAccuracyStats:
    """准确率统计测试。"""

    def test_empty_stats(self, tmp_db_with_script):
        db, _ = tmp_db_with_script
        engine = ReplayEngine(db)
        stats = engine.get_accuracy_stats()
        assert stats["total"] == 0

    def test_stats_after_replays(self, tmp_db_with_script):
        db, as_of = tmp_db_with_script
        engine = ReplayEngine(db)

        result = engine.replay(as_of, target_date="2024-06-15")
        engine.save_replay(result)

        stats = engine.get_accuracy_stats()
        assert stats["total"] == 1
        assert 0.0 <= stats["regime_accuracy"] <= 1.0


class TestReplayResultSerialization:
    """ReplayResult 序列化测试。"""

    def test_to_dict(self):
        r = ReplayResult(
            date="2024-06-15",
            actual_regime="题材轮动",
            narrative="测试",
            lessons=["教训1"],
        )
        d = r.to_dict()
        assert d["date"] == "2024-06-15"
        assert d["actual_regime"] == "题材轮动"
        assert d["lessons"] == ["教训1"]
        assert d["regime_match"] is False
