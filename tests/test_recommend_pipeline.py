"""测试推荐相关新模块：trading_calendar, push, recommend 集成。"""

import json
import sqlite3
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

import pytest

# ─── trading_calendar 测试 ───────────────────────────


class TestTradingCalendar:
    """交易日历工具测试。"""

    @pytest.fixture
    def db_with_data(self, tmp_path):
        """创建有 daily_price 数据的测试数据库。"""
        db_path = tmp_path / "test.db"
        conn = sqlite3.connect(str(db_path))
        conn.execute("""
            CREATE TABLE daily_price (
                trade_date TEXT NOT NULL,
                stock_code TEXT NOT NULL,
                open REAL,
                close REAL
            )
        """)
        # 插入几个交易日的数据
        for date in ["2026-04-20", "2026-04-21", "2026-04-22", "2026-04-23", "2026-04-24", "2026-04-25"]:
            for code in ["000001", "000002"]:
                conn.execute(
                    "INSERT INTO daily_price VALUES (?, ?, 10.0, 10.5)",
                    (date, code),
                )
        conn.commit()
        conn.close()
        return str(db_path)

    def test_get_latest_trade_date(self, db_with_data):
        from src.data.trading_calendar import get_latest_trade_date

        result = get_latest_trade_date(db_with_data)
        assert result == "2026-04-25"

    def test_get_latest_trade_date_empty_db(self, tmp_path):
        from src.data.trading_calendar import get_latest_trade_date

        db_path = str(tmp_path / "empty.db")
        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE daily_price (trade_date TEXT, stock_code TEXT)")
        conn.commit()
        conn.close()

        result = get_latest_trade_date(db_path)
        assert result is None

    def test_get_trade_dates(self, db_with_data):
        from src.data.trading_calendar import get_trade_dates

        dates = get_trade_dates(db_with_data)
        assert len(dates) == 6
        assert dates[0] == "2026-04-25"  # 降序
        assert dates[-1] == "2026-04-20"

    def test_is_weekend(self):
        from src.data.trading_calendar import is_weekend

        # 2026-04-25 是周六, 2026-04-26 是周日
        assert is_weekend("2026-04-25") is True   # 周六
        assert is_weekend("2026-04-26") is True   # 周日
        assert is_weekend("2026-04-24") is False  # 周五

    def test_get_previous_trade_date(self, db_with_data):
        from src.data.trading_calendar import get_previous_trade_date

        result = get_previous_trade_date("2026-04-24", db_path=db_with_data)
        assert result == "2026-04-23"

    def test_get_previous_trade_date_at_boundary(self, db_with_data):
        from src.data.trading_calendar import get_previous_trade_date

        # 最早日期之前应该返回 None
        result = get_previous_trade_date("2026-04-20", db_path=db_with_data)
        assert result is None

    def test_ensure_trade_date_with_valid_date(self, db_with_data):
        from src.data.trading_calendar import ensure_trade_date

        result = ensure_trade_date("2026-04-24", db_with_data)
        assert result == "2026-04-24"

    def test_ensure_trade_date_with_invalid_date(self, db_with_data):
        from src.data.trading_calendar import ensure_trade_date

        # 不存在的日期 → 返回最新交易日
        result = ensure_trade_date("2026-01-01", db_with_data)
        assert result == "2026-04-25"

    def test_ensure_trade_date_fallback(self, tmp_path):
        from src.data.trading_calendar import ensure_trade_date

        db_path = str(tmp_path / "empty.db")
        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE daily_price (trade_date TEXT, stock_code TEXT)")
        conn.commit()
        conn.close()

        # 无数据时返回今天
        result = ensure_trade_date(db_path=db_path)
        assert result == datetime.now().strftime("%Y-%m-%d")


# ─── push 模块测试 ──────────────────────────────────


class TestPush:
    """推送模块测试。"""

    @pytest.fixture
    def sample_report(self):
        from src.strategy.recommend import DailyRecommendation, StockRecommendation

        stocks = [
            StockRecommendation(
                stock_code="000001",
                stock_name="平安银行",
                industry="银行",
                concepts=["金融"],
                composite_score=0.85,
                consecutive_zt=1,
                open_count=0,
                buy_price=15.50,
                buy_zone_low=15.30,
                buy_zone_high=15.70,
                stop_loss=14.80,
                target_price=17.00,
                signal_level="A",
                amount=5e8,
                circulation_mv=100e8,
                fund_net_amount=2e8,
                reasons=["涨停突破", "主力资金流入"],
                risks=["银行板块整体偏弱"],
                factor_scores={"theme_crowding": 0.12, "leader_clarity": 0.08},
            ),
            StockRecommendation(
                stock_code="300750",
                stock_name="宁德时代",
                industry="电气设备",
                concepts=["新能源", "锂电池"],
                composite_score=0.72,
                consecutive_zt=2,
                open_count=1,
                buy_price=220.00,
                buy_zone_low=218.00,
                buy_zone_high=223.00,
                stop_loss=210.00,
                target_price=245.00,
                signal_level="B",
                amount=20e8,
                circulation_mv=500e8,
                fund_net_amount=-1e8,
                reasons=["连板龙头", "板块共振"],
                risks=["高换手率"],
                factor_scores={"theme_crowding": 0.15},
            ),
        ]
        return DailyRecommendation(
            trade_date="2026-04-24",
            stocks=stocks,
            zt_count=45,
            dt_count=3,
            market_regime="偏强震荡",
            hot_industries=[
                {"industry": "电气设备", "zt_count": 8},
                {"industry": "医药", "zt_count": 5},
            ],
            hot_concepts=["新能源", "锂电池", "储能"],
        )

    def test_format_wechat_message(self, sample_report):
        from src.strategy.push import _format_wechat_message

        msg = _format_wechat_message(sample_report)
        assert "Alpha Miner 明日操作建议" in msg
        assert "2026-04-24" in msg
        assert "涨停45只" in msg
        assert "000001" in msg
        assert "平安银行" in msg
        assert "15.50" in msg
        assert "17.00" in msg
        assert "300750" in msg
        assert "宁德时代" in msg
        assert "不构成投资建议" in msg

    def test_format_reconfirm_message(self, sample_report):
        from src.strategy.push import _format_reconfirm_message

        changes = ["300750 宁德时代: 高换手率; 主力净流出1.0亿"]
        msg = _format_reconfirm_message(sample_report, changes, "美股收涨")
        assert "早间复盘" in msg
        assert "调整说明" in msg
        assert "300750" in msg
        assert "确认推荐" in msg

    def test_push_saves_files(self, sample_report, tmp_path):
        from src.strategy.push import push_recommendation

        results = push_recommendation(
            sample_report,
            target="",  # 不推送微信
            save_dir=str(tmp_path / "rec"),
            save_json=True,
            print_terminal=False,
        )

        assert results["file"]
        assert results["json"]
        assert Path(results["file"]).exists()
        assert Path(results["json"]).exists()

        # 检查 JSON 可解析
        data = json.loads(Path(results["json"]).read_text())
        assert data["trade_date"] == "2026-04-24"
        assert len(data["stocks"]) == 2

    def test_push_empty_report(self, tmp_path):
        from src.strategy.push import push_recommendation
        from src.strategy.recommend import DailyRecommendation

        report = DailyRecommendation(
            trade_date="2026-04-24",
            stocks=[],
            zt_count=0,
            dt_count=0,
            market_regime="弱势市场",
        )

        results = push_recommendation(
            report,
            target="",
            save_dir=str(tmp_path / "rec"),
        )

        assert results["file"]
        txt = Path(results["file"]).read_text()
        assert "无符合条件" in txt

    def test_report_to_text(self, sample_report):
        text = sample_report.to_text()
        assert "Alpha Miner 每日个股推荐" in text
        assert "买入区间" in text
        assert "目标价位" in text
        assert "止损价位" in text

    def test_report_to_dict(self, sample_report):
        data = sample_report.to_dict()
        assert data["trade_date"] == "2026-04-24"
        assert len(data["stocks"]) == 2
        assert data["zt_count"] == 45
        s0 = data["stocks"][0]
        assert s0["stock_code"] == "000001"
        assert s0["signal_level"] == "A"
        assert s0["buy_price"] == 15.5
