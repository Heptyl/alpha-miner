"""市场剧本引擎测试。"""

import json
import os
import tempfile
from datetime import datetime, timedelta

import pandas as pd
import pytest

from src.data.storage import Storage
from src.narrative.script_engine import ScriptEngine, MarketScript, MarketSnapshot


@pytest.fixture
def tmp_db():
    """创建带数据的临时数据库。"""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    db = Storage(db_path)
    db.init_db()

    ts = datetime(2024, 6, 16, 10, 0, 0)
    as_of_future = datetime(2024, 6, 20, 12, 0, 0)

    # 插入 zt_pool
    zt_df = pd.DataFrame({
        "stock_code": ["000001", "000002", "000003", "000004"],
        "trade_date": ["2024-06-15"] * 4,
        "consecutive_zt": [5, 3, 2, 1],
        "amount": [10e8, 5e8, 3e8, 2e8],
        "circulation_mv": [50e8, 30e8, 20e8, 15e8],
    })
    db.insert("zt_pool", zt_df, snapshot_time=ts)

    # 插入 market_emotion
    emotion_df = pd.DataFrame({
        "trade_date": ["2024-06-15"],
        "zt_count": [50],
        "dt_count": [5],
        "highest_board": [5],
        "sentiment_level": ["偏强"],
    })
    db.insert("market_emotion", emotion_df, snapshot_time=ts)

    # 插入 zb_pool
    zb_df = pd.DataFrame({
        "stock_code": ["000010", "000011"],
        "trade_date": ["2024-06-15"] * 2,
        "amount": [1e8, 0.5e8],
    })
    db.insert("zb_pool", zb_df, snapshot_time=ts)

    # 插入 lhb_detail
    lhb_df = pd.DataFrame({
        "stock_code": ["000001", "000001", "000002"],
        "trade_date": ["2024-06-15"] * 3,
        "buy_amount": [8000e4, 5000e4, 3000e4],
        "sell_amount": [3000e4, 6000e4, 1000e4],
        "net_amount": [5000e4, -1000e4, 2000e4],
        "buy_depart": ["东方财富拉萨", "中信上海", "国泰君安深圳"],
        "sell_depart": ["华泰北京", "招商上海", "海通广州"],
        "reason": ["涨幅偏离", "涨幅偏离", "涨幅偏离"],
    })
    db.insert("lhb_detail", lhb_df, snapshot_time=ts)

    # 插入 concept_daily
    concept_df = pd.DataFrame({
        "concept_name": ["AI", "机器人", "芯片"],
        "trade_date": ["2024-06-15"] * 3,
        "zt_count": [8, 5, 3],
        "leader_code": ["000001", "000002", "000003"],
        "leader_consecutive": [5, 3, 2],
    })
    db.insert("concept_daily", concept_df, snapshot_time=ts)

    # 插入 fund_flow
    fund_df = pd.DataFrame({
        "stock_code": ["000001", "000002"],
        "trade_date": ["2024-06-15"] * 2,
        "super_large_net": [5e8, -2e8],
        "large_net": [3e8, -1e8],
        "medium_net": [1e8, 0.5e8],
        "small_net": [-0.5e8, 0.3e8],
        "main_net": [8.5e8, -2.5e8],
    })
    db.insert("fund_flow", fund_df, snapshot_time=ts)

    yield db, as_of_future
    os.unlink(db_path)


class TestBuildSnapshot:
    """_build_snapshot 测试。"""

    def test_snapshot_has_all_fields(self, tmp_db):
        db, as_of = tmp_db
        engine = ScriptEngine(db)
        snapshot = engine._build_snapshot(as_of, report_date="2024-06-15")

        assert snapshot.date == "2024-06-15"
        assert snapshot.zt_count == 50
        assert snapshot.dt_count == 5
        assert snapshot.highest_board == 5
        assert snapshot.zb_count == 2
        assert len(snapshot.board_ladder) > 0
        assert len(snapshot.hot_themes) > 0
        assert len(snapshot.lhb_summary) > 0

    def test_board_ladder_sorted_by_height(self, tmp_db):
        db, as_of = tmp_db
        engine = ScriptEngine(db)
        snapshot = engine._build_snapshot(as_of, report_date="2024-06-15")

        heights = [r["height"] for r in snapshot.board_ladder]
        assert heights == sorted(heights, reverse=True)
        assert heights[0] == 5  # 最高 5 连板

    def test_hot_themes_sorted_by_zt_count(self, tmp_db):
        db, as_of = tmp_db
        engine = ScriptEngine(db)
        snapshot = engine._build_snapshot(as_of, report_date="2024-06-15")

        counts = [t["zt_count"] for t in snapshot.hot_themes]
        assert counts == sorted(counts, reverse=True)
        assert counts[0] == 8  # AI 涨停最多

    def test_lhb_summary_aggregates(self, tmp_db):
        db, as_of = tmp_db
        engine = ScriptEngine(db)
        snapshot = engine._build_snapshot(as_of, report_date="2024-06-15")

        # 000001 的 net = (8000-3000) + (5000-6000) = 4000万
        item_001 = next(r for r in snapshot.lhb_summary if r["stock"] == "000001")
        assert item_001["total_net"] == pytest.approx(4000e4, rel=0.01)

    def test_fund_flow_summary(self, tmp_db):
        db, as_of = tmp_db
        engine = ScriptEngine(db)
        snapshot = engine._build_snapshot(as_of, report_date="2024-06-15")

        assert snapshot.fund_flow_summary["direction"] == "流入"
        assert snapshot.fund_flow_summary["super_large_net_total"] == pytest.approx(3e8, rel=0.01)

    def test_empty_db_no_crash(self):
        """空数据库不崩溃。"""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        db = Storage(db_path)
        db.init_db()
        engine = ScriptEngine(db)
        snapshot = engine._build_snapshot(datetime(2024, 6, 20), report_date="2024-06-15")
        assert snapshot.zt_count == 0
        assert snapshot.board_ladder == []
        os.unlink(db_path)


class TestFallbackScript:
    """规则回退测试。"""

    def test_fallback_generates_valid_script(self, tmp_db):
        db, as_of = tmp_db
        engine = ScriptEngine(db)  # 无 LLM
        script = engine.generate(as_of, report_date="2024-06-15")

        assert script.date == "2024-06-15"
        assert "涨停" in script.script_title
        assert len(script.script_narrative) > 0
        assert len(script.theme_verdicts) > 0
        assert "primary_strategy" in script.tomorrow_playbook

    def test_fallback_with_empty_data(self):
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        db = Storage(db_path)
        db.init_db()
        engine = ScriptEngine(db)
        script = engine.generate(datetime(2024, 6, 20), report_date="2024-06-15")

        assert script.date == "2024-06-15"
        assert len(script.script_narrative) > 0
        os.unlink(db_path)


class TestSaveLoadScript:
    """剧本存取测试。"""

    def test_save_and_load_roundtrip(self, tmp_db):
        db, as_of = tmp_db
        engine = ScriptEngine(db)

        script = MarketScript(
            date="2024-06-15",
            script_title="AI领涨",
            script_narrative="测试",
            theme_verdicts=[{"theme": "AI", "stage": "爆发"}],
            tomorrow_playbook={"primary_strategy": "追AI"},
            risk_alerts=["炸板率高"],
        )
        engine.save_script(script)

        loaded = engine.load_script("2024-06-15")
        assert loaded is not None
        assert loaded.script_title == "AI领涨"
        assert loaded.theme_verdicts[0]["theme"] == "AI"
        assert loaded.risk_alerts == ["炸板率高"]

    def test_load_nonexistent_returns_none(self, tmp_db):
        db, _ = tmp_db
        engine = ScriptEngine(db)
        assert engine.load_script("2024-01-01") is None


class TestMarketScriptToDict:
    """MarketScript 序列化测试。"""

    def test_to_dict(self):
        s = MarketScript(date="2024-06-15", script_title="test")
        d = s.to_dict()
        assert d["date"] == "2024-06-15"
        assert d["script_title"] == "test"
        assert d["theme_verdicts"] == []
        assert d["risk_alerts"] == []
