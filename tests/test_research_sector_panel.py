"""
板块日度面板测试 v2 — 覆盖 Codex 审计的 8 项修复

重点测试:
- 复合收益计算 (禁止 close/close 直接除)
- 除权跳变在目标日/窗口中间/窗口边界
- 复权价格序列 (MA20/20日新高)
- amount 单位异常日标记
- fund_flow 去重/覆盖率上限
- 时间隔离 (修改未来数据面板不变)
- ST 按日期过滤 (st_asof_risk)
- 映射一致性 (一股多板块)
- 不完整日期门禁
"""

import sqlite3
from collections import defaultdict
from pathlib import Path

import numpy as np
import pytest

import sys
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
from research_sector_daily_panel import (
    EX_RIGHT_THRESHOLD,
    MIN_SECTOR_SIZE,
    NEW_STOCK_WARMUP,
    INVALID_MARKET_STOCK_THRESHOLD,
    _compound_return,
    _adjusted_close_series,
    compute_sector_row,
    build_stock_filters,
)


@pytest.fixture
def tmp_db(tmp_path):
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("""CREATE TABLE daily_price (
        stock_code TEXT, trade_date TEXT, open REAL, high REAL, low REAL,
        close REAL, pre_close REAL, volume REAL, amount REAL,
        turnover_rate REAL, snapshot_time TEXT)""")
    conn.execute("CREATE TABLE concept_mapping (stock_code TEXT, concept_name TEXT, snapshot_time TEXT)")
    conn.execute("CREATE TABLE zt_pool (stock_code TEXT, trade_date TEXT, name TEXT, consecutive_zt INTEGER, amount REAL)")
    conn.execute("CREATE TABLE fund_flow (stock_code TEXT, trade_date TEXT, main_net REAL)")
    conn.execute("CREATE TABLE stock_blacklist (stock_code TEXT)")
    conn.commit()
    yield conn, db_path
    conn.close()


def _ins(conn, code, date, close, pre_close, volume=1e6, amount=1e8):
    conn.execute("INSERT INTO daily_price VALUES (?,?,?,?,?,?,?,?,?,NULL,NULL)",
                 (code, date, close, close, close, close, pre_close, volume, amount))


def _ins_map(conn, code, sector, snap="2026-06-08"):
    conn.execute("INSERT INTO concept_mapping VALUES (?,?,?)", (code, sector, snap))


def _ins_zt(conn, code, date, name="测试"):
    conn.execute("INSERT INTO zt_pool VALUES (?,?,?,1,1e8)", (code, date, name))


def _ins_ff(conn, code, date, main_net):
    conn.execute("INSERT INTO fund_flow VALUES (?,?,?)", (code, date, main_net))


def _make_filters(conn, early_date="2020-01-01"):
    """构建 filters 并确保股票有足够历史"""
    return build_stock_filters(conn)


def _make_base_data(conn, codes, sectors_by_code, dates, price_func=None):
    """辅助: 插入基础测试数据"""
    for code in codes:
        for d in dates:
            pre = 10.0
            close = price_func(code, d) if price_func else 10.0
            _ins(conn, code, d, close, pre)
        for sector in sectors_by_code.get(code, ["测试板块"]):
            _ins_map(conn, code, sector)


# ===== Issue 1: Compound returns =====

class TestCompoundReturn:
    """复合收益: close/pre_close 逐日连乘"""

    def test_basic_5d_compound(self):
        """5日复合收益 = (c1/pc1)*(c2/pc2)*...*(c5/pc5) - 1"""
        dates = [f"2026-01-{d:02d}" for d in range(1, 11)]
        # 每天涨1%: close/pre_close = 1.01, 5天 = 1.01^5 - 1 ≈ 5.1%
        stock_data = defaultdict(dict)
        for d in dates:
            stock_data["000001"][d] = {"close": 10.1, "pre_close": 10.0, "volume": 1e6, "amount": 1e8}
        ret = _compound_return(stock_data, "000001", dates, 5, 5, set())
        expected = (1.01 ** 5) - 1
        assert ret is not None
        assert abs(ret - expected) < 0.0001

    def test_mixed_daily_returns(self):
        """混合涨跌"""
        dates = [f"2026-01-{d:02d}" for d in range(1, 11)]
        stock_data = defaultdict(dict)
        # day1: +10%, day2: -5%, day3: +2%, day4: -1%, day5: +3%
        daily = [(11.0, 10.0), (10.45, 11.0), (10.659, 10.45),
                 (10.5524, 10.659), (10.8690, 10.5524)]
        for i, d in enumerate(dates[:5]):
            stock_data["000001"][d] = {"close": daily[i][0], "pre_close": daily[i][1],
                                       "volume": 1e6, "amount": 1e8}
        ret = _compound_return(stock_data, "000001", dates, 4, 5, set())
        # 手工算: 11/10 * 10.45/11 * 10.659/10.45 * 10.5524/10.659 * 10.869/10.5524 - 1
        expected = 11.0/10.0 * 10.45/11.0 * 10.659/10.45 * 10.5524/10.659 * 10.869/10.5524 - 1
        assert ret is not None
        assert abs(ret - expected) < 0.0001

    def test_gap_in_window_returns_none(self):
        """窗口内有缺失数据返回 None"""
        dates = [f"2026-01-{d:02d}" for d in range(1, 11)]
        stock_data = defaultdict(dict)
        for d in dates:
            stock_data["000001"][d] = {"close": 10.1, "pre_close": 10.0, "volume": 1e6, "amount": 1e8}
        # 缺 day3
        stock_data["000001"]["2026-01-03"] = {"close": 0, "pre_close": 0, "volume": 0, "amount": 0}
        ret = _compound_return(stock_data, "000001", dates, 5, 5, set())
        assert ret is None

    def test_ex_right_at_target_returns_none(self):
        """目标日有除权跳变返回 None"""
        dates = [f"2026-01-{d:02d}" for d in range(1, 11)]
        stock_data = defaultdict(dict)
        for d in dates:
            stock_data["000001"][d] = {"close": 10.1, "pre_close": 10.0, "volume": 1e6, "amount": 1e8}
        # 目标日(最后一天)有跳变: close=15, pre_close=10 → +50%
        stock_data["000001"]["2026-01-10"] = {"close": 15.0, "pre_close": 10.0, "volume": 1e6, "amount": 1e8}
        ex_right = {("000001", "2026-01-10"): 0.5}
        ret = _compound_return(stock_data, "000001", dates, 9, 5, ex_right)
        assert ret is None

    def test_ex_right_in_middle_returns_none(self):
        """窗口中间有除权跳变返回 None"""
        dates = [f"2026-01-{d:02d}" for d in range(1, 11)]
        stock_data = defaultdict(dict)
        for d in dates:
            stock_data["000001"][d] = {"close": 10.1, "pre_close": 10.0, "volume": 1e6, "amount": 1e8}
        # 中间某天有跳变
        ex_right = {("000001", "2026-01-08"): 0.5}
        ret = _compound_return(stock_data, "000001", dates, 9, 5, ex_right)
        assert ret is None

    def test_ex_right_at_window_boundary_returns_none(self):
        """窗口边界(起始日)有除权跳变返回 None"""
        dates = [f"2026-01-{d:02d}" for d in range(1, 11)]
        stock_data = defaultdict(dict)
        for d in dates:
            stock_data["000001"][d] = {"close": 10.1, "pre_close": 10.0, "volume": 1e6, "amount": 1e8}
        # 窗口起始日(tidx-4)有跳变
        ex_right = {("000001", "2026-01-06"): 0.5}
        ret = _compound_return(stock_data, "000001", dates, 9, 5, ex_right)
        assert ret is None


# ===== Issue 2: Adjusted prices =====

class TestAdjustedPrices:
    """复权价格序列"""

    def test_ma20_uses_adjusted_prices(self):
        """MA20 使用复权序列, 不直接用原始 close"""
        dates = [f"2026-01-{d:02d}" for d in range(1, 31)]
        stock_data = defaultdict(dict)
        # 前19天价格10, 最后一天涨到12 (20%跳变)
        for d in dates[:19]:
            stock_data["000001"][d] = {"close": 10.0, "pre_close": 10.0, "volume": 1e6, "amount": 1e8}
        stock_data["000001"][dates[19]] = {"close": 12.0, "pre_close": 10.0, "volume": 1e6, "amount": 1e8}
        # 有除权跳变, 整个窗口应返回 None
        ex_right = {("000001", dates[19]): 0.2}
        adj = _adjusted_close_series(stock_data, "000001", dates, 19, 20, ex_right)
        assert adj is None

    def test_clean_window_gives_series(self):
        """无跳变的窗口返回复权序列"""
        dates = [f"2026-01-{d:02d}" for d in range(1, 31)]
        stock_data = defaultdict(dict)
        for d in dates[:20]:
            stock_data["000001"][d] = {"close": 10.5, "pre_close": 10.0, "volume": 1e6, "amount": 1e8}
        adj = _adjusted_close_series(stock_data, "000001", dates, 19, 20, set())
        assert adj is not None
        assert len(adj) == 20
        # 所有因子 = 1.05, 远端累积更多: adj[0] > adj[-1]
        assert adj[0] > adj[-1]
        assert abs(adj[-1] - 1.05) < 0.001
        assert abs(adj[0] - 1.05**20) < 0.01

    def test_missing_data_in_window(self):
        """窗口内数据不足90%返回None"""
        dates = [f"2026-01-{d:02d}" for d in range(1, 31)]
        stock_data = defaultdict(dict)
        # 只有5天有数据, 不够18/20
        for d in dates[:5]:
            stock_data["000001"][d] = {"close": 10.0, "pre_close": 10.0, "volume": 1e6, "amount": 1e8}
        adj = _adjusted_close_series(stock_data, "000001", dates, 19, 20, set())
        assert adj is None


# ===== Issue 3: Amount quality =====

class TestAmountQuality:
    """amount 单位审计和日期质量"""

    def test_invalid_amount_date_marks_panel(self, tmp_db):
        conn, _ = tmp_db
        # 插入足够历史 + 当日数据但 amount 极小
        early = [f"2025-12-{d:02d}" for d in range(1, 22)]
        codes = ["000001", "000002", "000003", "000004", "000005"]
        for code in codes:
            for d in early:
                _ins(conn, code, d, 10.0, 10.0)
            _ins(conn, code, "2026-01-20", 10.5, 10.0, 1e6, 500)  # amount=500元 异常
            _ins_map(conn, code, "测试板块")

        filters = _make_filters(conn)
        date_quality = {"market_date_valid": True, "market_stock_count": 5,
                                        "amount_quality_valid": False}  # 标记为无效

        all_dates = early + ["2026-01-20"]
        date_idx = {d: i for i, d in enumerate(all_dates)}
        stock_data = defaultdict(dict)
        for code in codes:
            for d in early:
                stock_data[code][d] = {"close": 10.0, "pre_close": 10.0, "volume": 1e6, "amount": 1e8}
            stock_data[code]["2026-01-20"] = {"close": 10.5, "pre_close": 10.0, "volume": 1e6, "amount": 500}

        row = compute_sector_row(
            sector="测试板块", member_codes=codes, target_date="2026-01-20",
            date_idx=date_idx, all_dates=all_dates, stock_data=stock_data,
            filters=filters, date_quality=date_quality, zt_count=0, ff_info=None,
        )
        assert row is not None
        assert row["amount_quality_valid"] == 0
        # amount 无效时 total_amount 应为 None
        assert row["total_amount"] is None


# ===== Issue 4: Fund flow =====

class TestFundFlow:
    """fund_flow 去重/单位/覆盖率"""

    def test_coverage_bounded_by_1(self, tmp_db):
        """ff_coverage 必须 <= 1.0"""
        conn, _ = tmp_db
        early = [f"2025-12-{d:02d}" for d in range(1, 22)]
        codes = ["000001", "000002", "000003"]
        for code in codes:
            for d in early:
                _ins(conn, code, d, 10.0, 10.0)
            _ins(conn, code, "2026-01-20", 10.5, 10.0)
            _ins_map(conn, code, "测试板块")

        filters = _make_filters(conn)
        date_quality = {"market_date_valid": True, "market_stock_count": 3,
                                        "amount_quality_valid": True}
        all_dates = early + ["2026-01-20"]
        date_idx = {d: i for i, d in enumerate(all_dates)}
        stock_data = defaultdict(dict)
        for code in codes:
            for d in early:
                stock_data[code][d] = {"close": 10.0, "pre_close": 10.0, "volume": 1e6, "amount": 1e8}
            stock_data[code]["2026-01-20"] = {"close": 10.5, "pre_close": 10.0, "volume": 1e6, "amount": 1e8}

        # ff_info: 5只股票的 fund_flow 但只有3只有效 → count=5, coverage会被clamp到1.0
        ff_info = {"sum": 50000.0, "count": 5}  # 5万万元 = 5亿, 但只有3只有效票

        row = compute_sector_row(
            sector="测试板块", member_codes=codes, target_date="2026-01-20",
            date_idx=date_idx, all_dates=all_dates, stock_data=stock_data,
            filters=filters, date_quality=date_quality, zt_count=0, ff_info=ff_info,
        )
        assert row is not None
        assert row["ff_coverage"] <= 1.0

    def test_main_net_converted_to_yi(self, tmp_db):
        """main_net 从万元转为亿元"""
        conn, _ = tmp_db
        early = [f"2025-12-{d:02d}" for d in range(1, 22)]
        codes = ["000001", "000002", "000003"]
        for code in codes:
            for d in early:
                _ins(conn, code, d, 10.0, 10.0)
            _ins(conn, code, "2026-01-20", 10.5, 10.0)
            _ins_map(conn, code, "测试板块")

        filters = _make_filters(conn)
        date_quality = {"market_date_valid": True, "market_stock_count": 3,
                                        "amount_quality_valid": True}
        all_dates = early + ["2026-01-20"]
        date_idx = {d: i for i, d in enumerate(all_dates)}
        stock_data = defaultdict(dict)
        for code in codes:
            for d in early:
                stock_data[code][d] = {"close": 10.0, "pre_close": 10.0, "volume": 1e6, "amount": 1e8}
            stock_data[code]["2026-01-20"] = {"close": 10.5, "pre_close": 10.0, "volume": 1e6, "amount": 1e8}

        # 30000 万元 = 3 亿元
        ff_info = {"sum": 30000.0, "count": 3}

        row = compute_sector_row(
            sector="测试板块", member_codes=codes, target_date="2026-01-20",
            date_idx=date_idx, all_dates=all_dates, stock_data=stock_data,
            filters=filters, date_quality=date_quality, zt_count=0, ff_info=ff_info,
        )
        assert row is not None
        assert abs(row["ff_main_net_yi"] - 3.0) < 0.01  # 3亿元


# ===== Issue 5: Time isolation =====

class TestTimeIsolation:
    """时间隔离"""

    def test_future_data_does_not_change_panel(self, tmp_db):
        """修改目标日之后的数据, 面板结果不变"""
        conn, _ = tmp_db
        early = [f"2025-12-{d:02d}" for d in range(1, 22)]
        codes = ["000001", "000002", "000003", "000004", "000005"]
        for code in codes:
            for d in early:
                _ins(conn, code, d, 10.0, 10.0)
            _ins(conn, code, "2026-01-20", 10.5, 10.0)
            _ins_map(conn, code, "测试板块")

        filters = _make_filters(conn)
        date_quality = {"market_date_valid": True, "market_stock_count": 5,
                                        "amount_quality_valid": True}
        all_dates = early + ["2026-01-20", "2026-01-21", "2026-01-22"]
        date_idx = {d: i for i, d in enumerate(all_dates)}
        stock_data = defaultdict(dict)
        for code in codes:
            for d in early:
                stock_data[code][d] = {"close": 10.0, "pre_close": 10.0, "volume": 1e6, "amount": 1e8}
            stock_data[code]["2026-01-20"] = {"close": 10.5, "pre_close": 10.0, "volume": 1e6, "amount": 1e8}

        row1 = compute_sector_row(
            sector="测试板块", member_codes=codes, target_date="2026-01-20",
            date_idx=date_idx, all_dates=all_dates, stock_data=stock_data,
            filters=filters, date_quality=date_quality, zt_count=0, ff_info=None,
        )

        # 添加未来数据
        for code in codes:
            stock_data[code]["2026-01-21"] = {"close": 999.0, "pre_close": 10.5, "volume": 1e6, "amount": 1e8}
            stock_data[code]["2026-01-22"] = {"close": 888.0, "pre_close": 999.0, "volume": 1e6, "amount": 1e8}

        row2 = compute_sector_row(
            sector="测试板块", member_codes=codes, target_date="2026-01-20",
            date_idx=date_idx, all_dates=all_dates, stock_data=stock_data,
            filters=filters, date_quality=date_quality, zt_count=0, ff_info=None,
        )

        assert row1 is not None
        assert row2 is not None
        assert row1["ret_1d_mean"] == row2["ret_1d_mean"]
        assert row1["ret_1d_median"] == row2["ret_1d_median"]
        assert row1["valid_count"] == row2["valid_count"]

    def test_st_asof_risk_always_set(self, tmp_db):
        """st_asof_risk 始终为1"""
        conn, _ = tmp_db
        early = [f"2025-12-{d:02d}" for d in range(1, 22)]
        codes = ["000001", "000002", "000003", "000004", "000005"]
        for code in codes:
            for d in early:
                _ins(conn, code, d, 10.0, 10.0)
            _ins(conn, code, "2026-01-20", 10.5, 10.0)
            _ins_map(conn, code, "测试板块")

        filters = _make_filters(conn)
        date_quality = {"market_date_valid": True, "market_stock_count": 5,
                                        "amount_quality_valid": True}
        all_dates = early + ["2026-01-20"]
        date_idx = {d: i for i, d in enumerate(all_dates)}
        stock_data = defaultdict(dict)
        for code in codes:
            for d in early:
                stock_data[code][d] = {"close": 10.0, "pre_close": 10.0, "volume": 1e6, "amount": 1e8}
            stock_data[code]["2026-01-20"] = {"close": 10.5, "pre_close": 10.0, "volume": 1e6, "amount": 1e8}

        row = compute_sector_row(
            sector="测试板块", member_codes=codes, target_date="2026-01-20",
            date_idx=date_idx, all_dates=all_dates, stock_data=stock_data,
            filters=filters, date_quality=date_quality, zt_count=0, ff_info=None,
        )
        assert row is not None
        assert row["st_asof_risk"] == 1
        assert row["mapping_asof_risk"] == 1

    def test_st_only_filtered_after_observation(self, tmp_db):
        """ST 只在观察日期之后过滤, 不用未来 ST 状态过滤过去"""
        conn, _ = tmp_db
        early = [f"2025-12-{d:02d}" for d in range(1, 22)]
        codes = ["000001", "000002", "000003", "000004", "000005"]
        for code in codes:
            for d in early:
                _ins(conn, code, d, 10.0, 10.0)
            _ins(conn, code, "2026-01-20", 10.5, 10.0)
            _ins_map(conn, code, "测试板块")

        # 000001 在 2026-06-01 被标记 ST (目标日是 2026-01-20, 在 ST 之前)
        _ins_zt(conn, "000001", "2026-06-01", "*ST测试")

        filters = _make_filters(conn)
        date_quality = {"market_date_valid": True, "market_stock_count": 5,
                                        "amount_quality_valid": True}
        all_dates = early + ["2026-01-20"]
        date_idx = {d: i for i, d in enumerate(all_dates)}
        stock_data = defaultdict(dict)
        for code in codes:
            for d in early:
                stock_data[code][d] = {"close": 10.0, "pre_close": 10.0, "volume": 1e6, "amount": 1e8}
            stock_data[code]["2026-01-20"] = {"close": 10.5, "pre_close": 10.0, "volume": 1e6, "amount": 1e8}

        row = compute_sector_row(
            sector="测试板块", member_codes=codes, target_date="2026-01-20",
            date_idx=date_idx, all_dates=all_dates, stock_data=stock_data,
            filters=filters, date_quality=date_quality, zt_count=0, ff_info=None,
        )
        # 000001 应该仍在 (ST 在目标日之后才出现)
        assert row is not None
        assert row["valid_count"] == 5  # 全部5只都有效


# ===== Issue 6: Mapping consistency =====

class TestMappingConsistency:
    """映射一致性: 一股多板块"""

    def test_stock_in_two_sectors_counted_in_both(self, tmp_db):
        """一只股票属于两个板块, 两个板块都应计入"""
        conn, _ = tmp_db
        early = [f"2025-12-{d:02d}" for d in range(1, 22)]
        # 000001 属于板块A和板块B
        for d in early:
            _ins(conn, "000001", d, 10.0, 10.0)
        _ins(conn, "000001", "2026-01-20", 10.5, 10.0)
        _ins_map(conn, "000001", "板块A")
        _ins_map(conn, "000001", "板块B")

        # 000002 只属于板块A
        for d in early:
            _ins(conn, "000002", d, 10.0, 10.0)
        _ins(conn, "000002", "2026-01-20", 11.0, 10.0)
        _ins_map(conn, "000002", "板块A")

        filters = _make_filters(conn)
        date_quality = {"market_date_valid": True, "market_stock_count": 2,
                                        "amount_quality_valid": True}
        all_dates = early + ["2026-01-20"]
        date_idx = {d: i for i, d in enumerate(all_dates)}
        stock_data = defaultdict(dict)
        for code in ["000001", "000002"]:
            for d in early:
                stock_data[code][d] = {"close": 10.0, "pre_close": 10.0, "volume": 1e6, "amount": 1e8}
        stock_data["000001"]["2026-01-20"] = {"close": 10.5, "pre_close": 10.0, "volume": 1e6, "amount": 1e8}
        stock_data["000002"]["2026-01-20"] = {"close": 11.0, "pre_close": 10.0, "volume": 1e6, "amount": 1e8}

        row_a = compute_sector_row(
            sector="板块A", member_codes=["000001", "000002"], target_date="2026-01-20",
            date_idx=date_idx, all_dates=all_dates, stock_data=stock_data,
            filters=filters, date_quality=date_quality, zt_count=0, ff_info=None,
        )
        row_b = compute_sector_row(
            sector="板块B", member_codes=["000001"], target_date="2026-01-20",
            date_idx=date_idx, all_dates=all_dates, stock_data=stock_data,
            filters=filters, date_quality=date_quality, zt_count=0, ff_info=None,
        )
        assert row_a is not None
        assert row_b is not None
        assert row_a["valid_count"] == 2  # 000001 + 000002
        assert row_b["valid_count"] == 1  # 000001 only


# ===== Issue 7: Incomplete date marking =====

class TestInvalidDates:
    """不完整日期标记"""

    def test_market_date_valid_false(self, tmp_db):
        """横截面不足时 market_date_valid=0"""
        conn, _ = tmp_db
        early = [f"2025-12-{d:02d}" for d in range(1, 22)]
        codes = ["000001", "000002", "000003"]
        for code in codes:
            for d in early:
                _ins(conn, code, d, 10.0, 10.0)
            _ins(conn, code, "2026-01-20", 10.5, 10.0)
            _ins_map(conn, code, "测试板块")

        filters = _make_filters(conn)
        # 只有3只股票, 低于 INVALID_MARKET_STOCK_THRESHOLD
        date_quality = {"market_date_valid": False, "market_stock_count": 3,
                        "amount_quality_valid": True}
        all_dates = early + ["2026-01-20"]
        date_idx = {d: i for i, d in enumerate(all_dates)}
        stock_data = defaultdict(dict)
        for code in codes:
            for d in early:
                stock_data[code][d] = {"close": 10.0, "pre_close": 10.0, "volume": 1e6, "amount": 1e8}
            stock_data[code]["2026-01-20"] = {"close": 10.5, "pre_close": 10.0, "volume": 1e6, "amount": 1e8}

        row = compute_sector_row(
            sector="测试板块", member_codes=codes, target_date="2026-01-20",
            date_idx=date_idx, all_dates=all_dates, stock_data=stock_data,
            filters=filters, date_quality=date_quality, zt_count=0, ff_info=None,
        )
        assert row is not None
        assert row["market_date_valid"] == 0
        assert row["market_stock_count"] == 3


# ===== Regression: specific dates =====

class TestRegressionDates:
    """回归测试特定异常日期"""

    def test_new_stock_warmup_exclusion(self, tmp_db):
        """新股在观察期内被排除"""
        conn, _ = tmp_db
        # 股票在 2026-01-15 才首次出现, 面板计算 2026-01-20 (只有5天, 不足20天)
        _ins(conn, "000001", "2026-01-15", 10.0, 10.0)
        for d in range(16, 21):
            _ins(conn, "000001", f"2026-01-{d:02d}", 10.1, 10.0)
        _ins_map(conn, "000001", "新股板块")

        filters = _make_filters(conn)
        all_dates = [f"2026-01-{d:02d}" for d in range(15, 21)]
        date_idx = {d: i for i, d in enumerate(all_dates)}
        stock_data = defaultdict(dict)
        for d in all_dates:
            stock_data["000001"][d] = {"close": 10.1, "pre_close": 10.0, "volume": 1e6, "amount": 1e8}

        date_quality = {"market_date_valid": True, "market_stock_count": 1,
                                        "amount_quality_valid": True}

        row = compute_sector_row(
            sector="新股板块", member_codes=["000001"], target_date="2026-01-20",
            date_idx=date_idx, all_dates=all_dates, stock_data=stock_data,
            filters=filters, date_quality=date_quality, zt_count=0, ff_info=None,
        )
        # 只有5天历史, NEW_STOCK_WARMUP=20, 应被排除
        assert row is None
