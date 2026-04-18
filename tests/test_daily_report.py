"""日报生成器测试。"""

import json
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from src.data.storage import Storage
from src.drift.daily_report import DailyReport


@pytest.fixture
def db(tmp_path):
    """创建测试数据库 + 基础数据。"""
    db_path = str(tmp_path / "test.db")
    db = Storage(db_path)
    db.init_db()

    date_str = "2026-04-17"
    snap = datetime(2026, 4, 17, 9, 0, 0)  # snapshot 早于 as_of
    as_of_report = datetime(2026, 4, 17, 16, 0, 0)  # 报告生成时间 > snapshot

    # daily_price
    db.insert("daily_price", pd.DataFrame([
        {"stock_code": "000001", "trade_date": date_str, "open": 10.0, "close": 10.5, "high": 11.0, "low": 9.5, "volume": 1e6, "amount": 1e7, "turnover_rate": 2.0},
        {"stock_code": "000002", "trade_date": date_str, "open": 20.0, "close": 21.0, "high": 22.0, "low": 19.0, "volume": 2e6, "amount": 4e7, "turnover_rate": 3.0},
        {"stock_code": "600000", "trade_date": date_str, "open": 5.0, "close": 5.1, "high": 5.5, "low": 4.8, "volume": 5e5, "amount": 2.5e6, "turnover_rate": 1.0},
    ]), snapshot_time=snap)

    # zt_pool
    db.insert("zt_pool", pd.DataFrame([
        {"stock_code": "000001", "trade_date": date_str, "consecutive_zt": 1, "amount": 1e7, "circulation_mv": 5e9},
    ]), snapshot_time=snap)

    # market_emotion
    db.insert("market_emotion", pd.DataFrame([{
        "trade_date": date_str, "zt_count": 30, "dt_count": 5, "highest_board": 3,
    }]), snapshot_time=snap)

    # factor_values
    db.insert("factor_values", pd.DataFrame([
        {"factor_name": "cascade_momentum", "stock_code": "000001", "trade_date": date_str, "factor_value": 0.8},
        {"factor_name": "cascade_momentum", "stock_code": "000002", "trade_date": date_str, "factor_value": 0.5},
        {"factor_name": "cascade_momentum", "stock_code": "600000", "trade_date": date_str, "factor_value": 0.3},
        {"factor_name": "turnover_rank", "stock_code": "000001", "trade_date": date_str, "factor_value": 0.6},
        {"factor_name": "turnover_rank", "stock_code": "000002", "trade_date": date_str, "factor_value": 0.4},
        {"factor_name": "turnover_rank", "stock_code": "600000", "trade_date": date_str, "factor_value": 0.2},
    ]), snapshot_time=snap)

    return db, as_of_report


@pytest.fixture
def mining_log(tmp_path):
    """创建测试挖掘日志。"""
    log_path = tmp_path / "mining_log.jsonl"
    records = [
        {
            "name": "test_factor_1",
            "source": "knowledge",
            "timestamp": "2026-04-17T10:00:00",
            "accepted": True,
            "evaluation": {"ic_mean": 0.05},
        },
        {
            "name": "test_factor_2",
            "source": "mutation",
            "timestamp": "2026-04-17T11:00:00",
            "accepted": False,
            "evaluation": {"ic_mean": 0.01},
        },
    ]
    log_path.write_text("\n".join(json.dumps(r) for r in records))
    return str(log_path)


class TestDailyReport:
    def test_generate_full(self, db, mining_log):
        """生成完整日报。"""
        db_obj, as_of = db
        report = DailyReport(db_obj, mining_log_path=mining_log)
        text = report.generate(as_of)

        # 检查各板块存在
        assert "市场概况" in text
        assert "有效因子排名" in text
        assert "漂移预警" in text
        assert "今日挖掘结果" in text
        assert "明日候选标的" in text
        assert "系统状态" in text

        # 检查具体内容
        assert "涨停: 30" in text
        assert "跌停: 5" in text
        assert "验收: 1" in text
        assert "test_factor_1" in text

    def test_generate_no_mining_log(self, db, tmp_path):
        """无挖掘日志时不崩溃。"""
        db_obj, as_of = db
        report = DailyReport(db_obj, mining_log_path=str(tmp_path / "nonexistent.jsonl"))
        text = report.generate(as_of)
        assert "无挖掘记录" in text

    def test_generate_no_data(self, tmp_path):
        """空数据库。"""
        db_path = str(tmp_path / "empty.db")
        db = Storage(db_path)
        db.init_db()

        report = DailyReport(db, mining_log_path=str(tmp_path / "nonexistent.jsonl"))
        text = report.generate(datetime(2026, 4, 17))
        assert "Alpha Miner 日报" in text
        # 不应崩溃

    def test_mining_section_filters_by_date(self, db, tmp_path):
        """挖掘结果只显示当日。"""
        db_obj, as_of = db
        log_path = tmp_path / "mining_log.jsonl"
        records = [
            {"name": "old_factor", "source": "knowledge", "timestamp": "2026-04-16T10:00:00", "accepted": True, "evaluation": {"ic_mean": 0.05}},
            {"name": "today_factor", "source": "knowledge", "timestamp": "2026-04-17T10:00:00", "accepted": True, "evaluation": {"ic_mean": 0.06}},
        ]
        log_path.write_text("\n".join(json.dumps(r) for r in records))

        report = DailyReport(db_obj, mining_log_path=str(log_path))
        text = report.generate(as_of)

        assert "today_factor" in text
        assert "old_factor" not in text
