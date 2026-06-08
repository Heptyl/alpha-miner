"""PricingRegimeDetector 定价权 regime 单元测试（决策B）。"""

from datetime import datetime

import pandas as pd
import pytest

from src.data.storage import Storage
from src.drift.regime import PricingRegimeDetector

DAYS = ["2026-01-02", "2026-01-05", "2026-01-06", "2026-01-07", "2026-01-08"]


@pytest.fixture
def db(tmp_path):
    s = Storage(str(tmp_path / "t.db"))
    s.init_db()
    return s


def _seed(db, heights, clarity):
    days = DAYS[:len(heights)]
    for d, h in zip(days, heights):
        db.insert("zt_pool", pd.DataFrame([
            dict(stock_code="A", trade_date=d, consecutive_zt=h, amount=0),
            dict(stock_code="B", trade_date=d, consecutive_zt=1, amount=0),
        ]))
        db.insert("factor_values", pd.DataFrame([
            dict(factor_name="leader_clarity", stock_code=c, trade_date=d, factor_value=clarity)
            for c in ("A", "B", "C")
        ]))
    return days


def _detect(db, day):
    return PricingRegimeDetector(db).detect(datetime.strptime(day, "%Y-%m-%d").replace(hour=15))


def test_hot_money_led(db):
    # 龙头清晰(clarity高) + 高度不衰减 → 游资主导
    days = _seed(db, [5, 5, 6, 6, 7], clarity=0.9)
    assert _detect(db, days[-1]).regime == "hot_money_led"


def test_quant_led(db):
    # 龙头不清晰(clarity低) + 高度持续走低 → 量化主导
    days = _seed(db, [7, 6, 5, 4, 3], clarity=0.2)
    assert _detect(db, days[-1]).regime == "quant_led"


def test_mixed_when_no_clarity(db):
    # 无 leader_clarity 数据 → mixed
    db.insert("zt_pool", pd.DataFrame([
        dict(stock_code="A", trade_date="2026-01-02", consecutive_zt=3, amount=0)]))
    assert _detect(db, "2026-01-02").regime == "mixed"
