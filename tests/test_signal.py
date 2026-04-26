"""信号引擎测试."""

import sqlite3
from datetime import datetime

import pytest

from src.data.storage import Storage
from src.strategy.signal import SignalEngine, SignalCard, SignalReport


@pytest.fixture
def signal_db(tmp_path):
    """创建含测试数据的数据库."""
    db_path = str(tmp_path / "test.db")
    conn = sqlite3.connect(db_path)

    # 建表
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS daily_price (
            stock_code TEXT, trade_date TEXT, open REAL, close REAL,
            high REAL, low REAL, pre_close REAL, volume REAL,
            amount REAL, turnover_rate REAL, snapshot_time TEXT
        );
        CREATE TABLE IF NOT EXISTS zt_pool (
            stock_code TEXT, trade_date TEXT, name TEXT,
            consecutive_zt INTEGER, amount REAL, industry TEXT,
            circulation_mv REAL, open_count INTEGER, zt_stats TEXT,
            snapshot_time TEXT
        );
        CREATE TABLE IF NOT EXISTS factor_values (
            factor_name TEXT, stock_code TEXT, trade_date TEXT,
            factor_value REAL, snapshot_time TEXT
        );
        CREATE TABLE IF NOT EXISTS concept_mapping (
            stock_code TEXT, concept_name TEXT
        );
    """)

    # 插入测试数据
    date = "2026-04-24"

    # 涨停池：3只票
    conn.executemany(
        "INSERT INTO zt_pool VALUES (?,?,?,?,?,?,?,?,?,?)",
        [
            ("000001", date, "平安银行", 2, 1e9, "银行", 5e10, 0, "2/2", "2026-04-24"),
            ("600519", date, "贵州茅台", 1, 2e9, "白酒", 2e11, 3, "1/1", "2026-04-24"),
            ("300001", date, "特锐德", 3, 5e8, "充电桩", 1e10, 0, "3/3", "2026-04-24"),
        ],
    )

    # 因子值
    factor_data = [
        ("theme_crowding", "000001", date, 0.85),
        ("theme_crowding", "600519", date, 0.60),
        ("theme_crowding", "300001", date, 0.92),
        ("leader_clarity", "000001", date, 0.70),
        ("leader_clarity", "600519", date, 0.30),
        ("leader_clarity", "300001", date, 1.00),
        ("lhb_institution", "000001", date, 5e8),
        ("lhb_institution", "600519", date, -1e8),
        ("lhb_institution", "300001", date, 2e8),
        ("turnover_rank", "000001", date, 0.50),
        ("turnover_rank", "600519", date, 0.95),
        ("turnover_rank", "300001", date, 0.30),
    ]
    conn.executemany(
        "INSERT INTO factor_values VALUES (?,?,?,?,?)",
        [(f, c, d, v, "2026-04-24") for f, c, d, v in factor_data],
    )

    # 概念
    conn.executemany(
        "INSERT INTO concept_mapping VALUES (?,?)",
        [
            ("000001", "金融科技"),
            ("000001", "银行"),
            ("300001", "新能源车"),
            ("300001", "充电桩"),
        ],
    )

    # daily_price (用于跌停计算)
    conn.executemany(
        "INSERT INTO daily_price VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        [
            ("000001", date, 10.0, 11.0, 11.0, 10.0, 10.0, 1e6, 1e7, 0, "2026-04-24"),
            ("600519", date, 1800, 1750, 1800, 1740, 1800, 5e5, 1e9, 0, "2026-04-24"),
            ("300001", date, 20.0, 22.0, 22.0, 20.0, 20.0, 2e6, 4e7, 0, "2026-04-24"),
        ],
    )

    conn.commit()
    conn.close()

    db = Storage(db_path)
    return db


class TestSignalCard:
    """SignalCard 单元测试."""

    def test_to_dict(self):
        card = SignalCard(
            stock_code="000001",
            stock_name="测试",
            industry="银行",
            concepts=["金融"],
            composite_score=0.85,
            signal_level="A",
            theme_crowding=0.9,
        )
        d = card.to_dict()
        assert d["stock_code"] == "000001"
        assert d["signal_level"] == "A"
        assert d["theme_crowding"] == 0.9

    def test_to_dict_rounding(self):
        card = SignalCard(
            stock_code="000001",
            stock_name="",
            industry="",
            concepts=[],
            theme_crowding=0.123456,
        )
        d = card.to_dict()
        assert d["theme_crowding"] == 0.123  # 3位小数


class TestSignalReport:
    """SignalReport 单元测试."""

    def test_to_text_no_cards(self):
        report = SignalReport(
            trade_date="2026-04-24",
            cards=[],
            zt_count=0,
            dt_count=0,
        )
        text = report.to_text()
        assert "无符合条件" in text
        assert "2026-04-24" in text

    def test_to_text_with_cards(self):
        card = SignalCard(
            stock_code="000001",
            stock_name="平安银行",
            industry="银行",
            concepts=["金融"],
            composite_score=0.85,
            signal_level="A",
            reasons=["板块拥挤度高"],
            risks=["炸板3次"],
        )
        report = SignalReport(
            trade_date="2026-04-24",
            cards=[card],
            zt_count=5,
            dt_count=1,
        )
        text = report.to_text()
        assert "000001" in text
        assert "平安银行" in text
        assert "板块拥挤度高" in text
        assert "炸板3次" in text


class TestSignalEngine:
    """SignalEngine 集成测试."""

    def test_generate_basic(self, signal_db):
        engine = SignalEngine(signal_db)
        as_of = datetime(2026, 4, 25)
        report = engine.generate(as_of, "2026-04-24", top_n=10)

        assert report.trade_date == "2026-04-24"
        assert report.zt_count == 3
        assert len(report.cards) == 3

        # 最高分应该是 300001（3连板 + 高因子得分）
        assert report.cards[0].stock_code == "300001"
        assert report.cards[0].signal_level == "A"

    def test_generate_top_n(self, signal_db):
        engine = SignalEngine(signal_db)
        report = engine.generate(datetime(2026, 4, 25), "2026-04-24", top_n=2)
        assert len(report.cards) == 2

    def test_market_regime(self, signal_db):
        engine = SignalEngine(signal_db)
        report = engine.generate(datetime(2026, 4, 25), "2026-04-24")
        # 3涨停 0跌停 → 强势
        assert "强势" in report.market_regime or "震荡" in report.market_regime

    def test_hot_industries(self, signal_db):
        engine = SignalEngine(signal_db)
        report = engine.generate(datetime(2026, 4, 25), "2026-04-24")
        assert len(report.hot_industries) == 3
        # 每个板块只有1只，排序不重要

    def test_risk_detection(self, signal_db):
        """600519 炸板3次，应有风险提示."""
        engine = SignalEngine(signal_db)
        report = engine.generate(datetime(2026, 4, 25), "2026-04-24")

        # 找到 600519
        moutai = next(c for c in report.cards if c.stock_code == "600519")
        assert any("炸板" in r for r in moutai.risks)

    def test_reason_generation(self, signal_db):
        """300001 三连板，应有连板理由."""
        engine = SignalEngine(signal_db)
        report = engine.generate(datetime(2026, 4, 25), "2026-04-24")

        terce = next(c for c in report.cards if c.stock_code == "300001")
        assert any("3连板" in r or "连板" in r for r in terce.reasons)

    def test_concept_loaded(self, signal_db):
        """概念应被正确加载."""
        engine = SignalEngine(signal_db)
        report = engine.generate(datetime(2026, 4, 25), "2026-04-24")

        terce = next(c for c in report.cards if c.stock_code == "300001")
        assert "新能源车" in terce.concepts

    def test_empty_zt_pool(self, tmp_path):
        """无涨停池数据时应返回空报告."""
        db_path = str(tmp_path / "empty.db")
        conn = sqlite3.connect(db_path)
        conn.executescript("""
            CREATE TABLE daily_price (stock_code TEXT, trade_date TEXT, open REAL,
                close REAL, high REAL, low REAL, pre_close REAL, volume REAL,
                amount REAL, turnover_rate REAL, snapshot_time TEXT);
            CREATE TABLE zt_pool (stock_code TEXT, trade_date TEXT, name TEXT,
                consecutive_zt INTEGER, amount REAL, industry TEXT,
                circulation_mv REAL, open_count INTEGER, zt_stats TEXT,
                snapshot_time TEXT);
            CREATE TABLE factor_values (factor_name TEXT, stock_code TEXT,
                trade_date TEXT, factor_value REAL, snapshot_time TEXT);
            CREATE TABLE concept_mapping (stock_code TEXT, concept_name TEXT);
        """)
        conn.close()

        db = Storage(db_path)
        engine = SignalEngine(db)
        report = engine.generate(datetime(2026, 4, 25), "2026-04-24")

        assert report.zt_count == 0
        assert len(report.cards) == 0

    def test_signal_level_distribution(self, signal_db):
        """验证信号等级分布合理."""
        engine = SignalEngine(signal_db)
        report = engine.generate(datetime(2026, 4, 25), "2026-04-24")

        levels = {c.signal_level for c in report.cards}
        # 至少有 A 或 B
        assert levels & {"A", "B"}

    def test_composite_score_range(self, signal_db):
        """综合分应在 [0, 1] 范围内."""
        engine = SignalEngine(signal_db)
        report = engine.generate(datetime(2026, 4, 25), "2026-04-24")

        for card in report.cards:
            assert 0 <= card.composite_score <= 1.0

    def test_cards_sorted_by_score(self, signal_db):
        """信号卡应按综合分降序排列."""
        engine = SignalEngine(signal_db)
        report = engine.generate(datetime(2026, 4, 25), "2026-04-24")

        scores = [c.composite_score for c in report.cards]
        assert scores == sorted(scores, reverse=True)

    def test_to_json_roundtrip(self, signal_db):
        """to_dict 应可 JSON 序列化."""
        import json
        engine = SignalEngine(signal_db)
        report = engine.generate(datetime(2026, 4, 25), "2026-04-24")

        d = report.to_dict()
        text = json.dumps(d, ensure_ascii=False)
        assert "300001" in text
