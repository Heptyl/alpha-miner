"""EventStudy 事件研究回测单元测试。

纯 DB 驱动，不读写文本文件（规避 Windows GBK locale 噪声）。
每个交易日只构造一次，避免多 snapshot 行歧义。
"""

import pandas as pd
import pytest

from src.data.storage import Storage
from src.strategy.event_study import EventStudy, entry_from_factor, _agg


@pytest.fixture
def db(tmp_path):
    s = Storage(str(tmp_path / "t.db"))
    s.init_db()
    return s


def _add_day(s: Storage, date: str, prices: dict, factor: dict | None = None,
             universe: list | None = None):
    """prices: {code: (open, high, low, close, pre_close)}。同日先删后插，保证单快照。"""
    for t in ("daily_price", "zt_pool", "factor_values"):
        s.execute_write(f"DELETE FROM {t} WHERE trade_date = ?", (date,))

    rows = [dict(stock_code=code, trade_date=date, open=o, high=h, low=lo,
                 close=c, pre_close=pc, volume=0, amount=0, turnover_rate=0)
            for code, (o, h, lo, c, pc) in prices.items()]
    s.insert("daily_price", pd.DataFrame(rows))

    uni = universe if universe is not None else list(prices.keys())
    s.insert("zt_pool", pd.DataFrame(
        [dict(stock_code=code, trade_date=date, consecutive_zt=1, amount=0) for code in uni]))

    if factor:
        s.insert("factor_values", pd.DataFrame(
            [dict(factor_name="f", stock_code=code, trade_date=date, factor_value=v)
             for code, v in factor.items()]))


DAYS = ["2026-01-02", "2026-01-05", "2026-01-06", "2026-01-07", "2026-01-08", "2026-01-09"]
FLAT = (10, 10, 10, 10, 10)


def test_agg_basic():
    st = _agg([0.10, 0.20, -0.05, -0.05], forward_days=1)
    assert st.n == 4
    assert st.win_rate == 0.5
    assert round(st.avg_ret, 4) == 0.05
    assert round(st.pnl_ratio, 2) == 3.0   # avg_win 0.15 / avg_loss 0.05


def test_complete_dates_skips_thin_day(db):
    _add_day(db, "2026-01-02", {"A": FLAT, "B": FLAT})
    _add_day(db, "2026-01-03", {"A": FLAT})            # 残日，仅 1 股
    _add_day(db, "2026-01-06", {"A": FLAT, "B": FLAT})
    es = EventStudy(db, min_stocks=2)
    assert es._complete_dates() == ["2026-01-02", "2026-01-06"]


def test_basic_event_study(db):
    # A 仅在 d0 触发 (f=0.9)，买 d1 开盘=10，T+1=d2收盘=11(+10%)，T+3=d4收盘=12(+20%)
    _add_day(db, DAYS[0], {"A": FLAT, "B": FLAT, "C": FLAT}, factor={"A": 0.9, "B": 0.1, "C": 0.1})
    _add_day(db, DAYS[1], {"A": FLAT, "B": FLAT, "C": FLAT}, factor={"A": 0.1, "B": 0.1, "C": 0.1})
    _add_day(db, DAYS[2], {"A": (10, 11, 10, 11, 10), "B": FLAT, "C": FLAT}, factor={"A": 0.1})
    _add_day(db, DAYS[3], {"A": FLAT, "B": FLAT, "C": FLAT}, factor={"A": 0.1})
    _add_day(db, DAYS[4], {"A": (10, 12, 10, 12, 10), "B": FLAT, "C": FLAT}, factor={"A": 0.1})
    _add_day(db, DAYS[5], {"A": FLAT, "B": FLAT, "C": FLAT}, factor={"A": 0.1})

    es = EventStudy(db, min_stocks=2)
    r = es.run(entry_from_factor("f", ">=", 0.5), DAYS[0], DAYS[-1], forward_windows=(1, 3))
    assert r.error is None
    assert r.n_signals == 1
    assert round(r.windows[1].avg_ret, 4) == 0.10
    assert round(r.windows[3].avg_ret, 4) == 0.20
    assert r.windows[1].win_rate == 1.0


def test_limit_up_buy_excluded(db):
    # A 仅 d0 触发；买入日 d1 开盘=11，较 d0 收盘 10 高开 +10% -> 实盘买不进，剔除
    _add_day(db, DAYS[0], {"A": FLAT, "B": FLAT}, factor={"A": 0.9, "B": 0.1})
    _add_day(db, DAYS[1], {"A": (11, 11, 11, 11, 10), "B": FLAT}, factor={"A": 0.1, "B": 0.1})
    for d in DAYS[2:]:
        _add_day(db, d, {"A": FLAT, "B": FLAT}, factor={"A": 0.1, "B": 0.1})

    es = EventStudy(db, min_stocks=2, limit_up_gap=0.095)
    r = es.run(entry_from_factor("f", ">=", 0.5), DAYS[0], DAYS[-1], forward_windows=(1,))
    assert r.n_signals == 0


def _fill_uniform(db, close):
    """每个交易日 A(触发)/B(不触发) 同价，open=10、close=给定值。"""
    for d in DAYS:
        _add_day(db, d, {"A": (10, max(10, close), min(10, close), close, 10),
                         "B": (10, max(10, close), min(10, close), close, 10)},
                 factor={"A": 0.9, "B": 0.1})


def test_gate_pass(db):
    _fill_uniform(db, close=11)   # 每个样本 T+1 = +10% 全胜
    es = EventStudy(db, min_stocks=2)
    g = es.two_stage_gate(entry_from_factor("f", ">=", 0.5), DAYS[-1],
                          win_threshold=0.55, long_days=10, short_days=3, min_samples=1)
    assert g.passed is True
    assert g.wr_long == 1.0 and g.wr_short == 1.0


def test_gate_fail(db):
    _fill_uniform(db, close=9)    # 每个样本 T+1 = -10% 全负
    es = EventStudy(db, min_stocks=2)
    g = es.two_stage_gate(entry_from_factor("f", ">=", 0.5), DAYS[-1],
                          win_threshold=0.55, long_days=10, short_days=3, min_samples=1)
    assert g.passed is False
    assert g.wr_long == 0.0
