#!/usr/bin/env python3
"""Alpha Miner 数据质量校验脚本。

检查项:
1. daily_price: 每日股票数、价格异常（零值、high<low、close越界）、未来日期
2. zt_pool: 每日涨停数量合理性
3. fund_flow: 日期连续性
4. factor_values: 因子覆盖率
5. 自动清理脏数据（未来日期）
6. 输出校验报告 → data/validation_report.txt + 终端
"""

import sqlite3
import sys
from datetime import datetime, date
from pathlib import Path
from collections import defaultdict

# ── 配置 ──────────────────────────────────────────────────────────
DB_PATH = Path(__file__).resolve().parent.parent / "data" / "alpha_miner.db"
REPORT_PATH = Path(__file__).resolve().parent.parent / "data" / "validation_report.txt"

THRESHOLD_DAILY_STOCKS_WARN = 100     # 每日股票数 < 此值 → 警告
THRESHOLD_DAILY_STOCKS_PASS = 1000    # 每日股票数 >= 此值 → 通过
THRESHOLD_ZT_MIN = 0
THRESHOLD_ZT_MAX = 200
THRESHOLD_FUND_FLOW_GAP = 3          # fund_flow 日期缺口 > 此值 → 警告
THRESHOLD_FACTOR_COVERAGE = 500      # 每日每因子至少覆盖此数 → 通过
THRESHOLD_FACTOR_COVERAGE_PCT = 0.30 # 因子每日覆盖率 < 此值 → 警告

TODAY = date.today()
TODAY_STR = TODAY.strftime("%Y-%m-%d")


class ValidationResult:
    """收集校验结果。"""
    def __init__(self):
        self.sections = []     # [(section_name, lines)]
        self.current_section = None
        self.current_lines = []
        self.has_failure = False
        self.cleanup_actions = []

    def section(self, name):
        if self.current_section is not None:
            self.sections.append((self.current_section, self.current_lines))
        self.current_section = name
        self.current_lines = []

    def line(self, text=""):
        self.current_lines.append(text)

    def fail(self, text):
        self.current_lines.append(f"  [FAIL] {text}")
        self.has_failure = True

    def warn(self, text):
        self.current_lines.append(f"  [WARN] {text}")

    def ok(self, text):
        self.current_lines.append(f"  [PASS] {text}")

    def cleanup(self, text):
        self.current_lines.append(f"  [CLEAN] {text}")
        self.cleanup_actions.append(text)

    def finish(self):
        if self.current_section is not None:
            self.sections.append((self.current_section, self.current_lines))

    def render(self) -> str:
        self.finish()
        lines = []
        lines.append("=" * 64)
        lines.append("  Alpha Miner 数据质量校验报告")
        lines.append(f"  生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append(f"  数据库: {DB_PATH}")
        lines.append(f"  今天: {TODAY_STR}")
        lines.append("=" * 64)
        lines.append("")

        overall = "FAIL" if self.has_failure else "ALL PASS"
        lines.append(f"  总体结果: {overall}")
        if self.cleanup_actions:
            lines.append(f"  清理操作: {len(self.cleanup_actions)} 项")
        lines.append("")

        for section_name, section_lines in self.sections:
            lines.append(f"── {section_name} {'─' * (54 - len(section_name))}")
            for sl in section_lines:
                lines.append(sl)
            lines.append("")

        lines.append("=" * 64)
        if self.cleanup_actions:
            lines.append("清理操作汇总:")
            for i, a in enumerate(self.cleanup_actions, 1):
                lines.append(f"  {i}. {a}")
            lines.append("=" * 64)

        return "\n".join(lines)


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def table_exists(conn, table_name: str) -> bool:
    r = conn.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,)
    ).fetchone()
    return r[0] > 0


# ── 1. daily_price 校验 ──────────────────────────────────────────
def check_daily_price(conn, vr: ValidationResult):
    vr.section("daily_price 校验")
    if not table_exists(conn, "daily_price"):
        vr.fail("daily_price 表不存在")
        return

    total = conn.execute("SELECT COUNT(*) FROM daily_price").fetchone()[0]
    distinct_dates = conn.execute("SELECT COUNT(DISTINCT trade_date) FROM daily_price").fetchone()[0]
    date_range = conn.execute("SELECT MIN(trade_date), MAX(trade_date) FROM daily_price").fetchone()
    distinct_stocks = conn.execute("SELECT COUNT(DISTINCT stock_code) FROM daily_price").fetchone()[0]
    vr.line(f"  总行数: {total}, 交易日: {distinct_dates}, 股票数: {distinct_stocks}")
    vr.line(f"  日期范围: {date_range[0]} ~ {date_range[1]}")
    vr.line("")

    # 1a. 未来日期清理
    future_rows = conn.execute(
        "SELECT COUNT(*) FROM daily_price WHERE trade_date > ?", (TODAY_STR,)
    ).fetchone()[0]
    if future_rows > 0:
        conn.execute("DELETE FROM daily_price WHERE trade_date > ?", (TODAY_STR,))
        conn.commit()
        vr.cleanup(f"daily_price: 删除 {future_rows} 条未来日期数据 (> {TODAY_STR})")
    else:
        vr.ok(f"无未来日期数据")

    # 1b. 每日股票数
    vr.line("")
    vr.line("  每日股票数:")
    daily_counts = conn.execute(
        "SELECT trade_date, COUNT(*) as cnt FROM daily_price "
        "GROUP BY trade_date ORDER BY trade_date"
    ).fetchall()

    warn_days = []
    fail_days = []
    for d, cnt in daily_counts:
        if cnt < THRESHOLD_DAILY_STOCKS_WARN:
            fail_days.append((d, cnt))
        elif cnt < THRESHOLD_DAILY_STOCKS_PASS:
            warn_days.append((d, cnt))

    if fail_days:
        vr.fail(f"有 {len(fail_days)} 个交易日股票数 < {THRESHOLD_DAILY_STOCKS_WARN}")
        for d, cnt in fail_days[:10]:
            vr.line(f"         {d}: {cnt} 只")
        if len(fail_days) > 10:
            vr.line(f"         ... 共 {len(fail_days)} 天")
    elif warn_days:
        vr.warn(f"有 {len(warn_days)} 个交易日股票数 < {THRESHOLD_DAILY_STOCKS_PASS} (但 >= {THRESHOLD_DAILY_STOCKS_WARN})")
        for d, cnt in warn_days[:10]:
            vr.line(f"         {d}: {cnt} 只")
    else:
        vr.ok(f"所有 {len(daily_counts)} 个交易日股票数 >= {THRESHOLD_DAILY_STOCKS_PASS}")

    # 1c. 价格零值
    zero_close = conn.execute("SELECT COUNT(*) FROM daily_price WHERE close = 0").fetchone()[0]
    zero_high = conn.execute("SELECT COUNT(*) FROM daily_price WHERE high = 0").fetchone()[0]
    zero_low = conn.execute("SELECT COUNT(*) FROM daily_price WHERE low = 0").fetchone()[0]

    zero_total = zero_close + zero_high + zero_low
    if zero_total > 0:
        vr.warn(f"发现价格零值: close=0: {zero_close}, high=0: {zero_high}, low=0: {zero_low}")
        # 标记异常详情
        zero_samples = conn.execute(
            "SELECT stock_code, trade_date, close, high, low FROM daily_price "
            "WHERE close = 0 OR high = 0 OR low = 0 LIMIT 20"
        ).fetchall()
        for row in zero_samples:
            vr.line(f"         {row[0]} {row[1]}: close={row[2]} high={row[3]} low={row[4]}")
    else:
        vr.ok("无价格零值")

    # 1d. high < low 异常
    hl_anomaly = conn.execute(
        "SELECT COUNT(*) FROM daily_price WHERE high < low AND high > 0 AND low > 0"
    ).fetchone()[0]
    if hl_anomaly > 0:
        vr.fail(f"发现 {hl_anomaly} 条 high < low 的异常数据")
        hl_samples = conn.execute(
            "SELECT stock_code, trade_date, high, low FROM daily_price "
            "WHERE high < low AND high > 0 AND low > 0 LIMIT 10"
        ).fetchall()
        for row in hl_samples:
            vr.line(f"         {row[0]} {row[1]}: high={row[2]} < low={row[3]}")
    else:
        vr.ok("无 high < low 异常")

    # 1e. close 越界 (close > high 或 close < low)
    out_of_range = conn.execute(
        "SELECT COUNT(*) FROM daily_price "
        "WHERE (close > high OR close < low) AND high > 0 AND low > 0"
    ).fetchone()[0]
    if out_of_range > 0:
        vr.warn(f"发现 {out_of_range} 条 close 超出 [low, high] 范围的数据")
        oor_samples = conn.execute(
            "SELECT stock_code, trade_date, close, high, low FROM daily_price "
            "WHERE (close > high OR close < low) AND high > 0 AND low > 0 LIMIT 10"
        ).fetchall()
        for row in oor_samples:
            vr.line(f"         {row[0]} {row[1]}: close={row[2]} high={row[3]} low={row[4]}")
    else:
        vr.ok("close 均在 [low, high] 范围内")


# ── 2. zt_pool 校验 ──────────────────────────────────────────────
def check_zt_pool(conn, vr: ValidationResult):
    vr.section("zt_pool (涨停池) 校验")
    if not table_exists(conn, "zt_pool"):
        vr.warn("zt_pool 表不存在，跳过")
        return

    total = conn.execute("SELECT COUNT(*) FROM zt_pool").fetchone()[0]
    distinct_dates = conn.execute("SELECT COUNT(DISTINCT trade_date) FROM zt_pool").fetchone()[0]
    date_range = conn.execute("SELECT MIN(trade_date), MAX(trade_date) FROM zt_pool").fetchone()
    vr.line(f"  总行数: {total}, 交易日: {distinct_dates}")
    vr.line(f"  日期范围: {date_range[0]} ~ {date_range[1]}")
    vr.line("")

    # 未来日期清理
    future_rows = conn.execute(
        "SELECT COUNT(*) FROM zt_pool WHERE trade_date > ?", (TODAY_STR,)
    ).fetchone()[0]
    if future_rows > 0:
        conn.execute("DELETE FROM zt_pool WHERE trade_date > ?", (TODAY_STR,))
        conn.commit()
        vr.cleanup(f"zt_pool: 删除 {future_rows} 条未来日期数据")

    # 每日涨停数量
    daily_counts = conn.execute(
        "SELECT trade_date, COUNT(*) as cnt FROM zt_pool "
        "GROUP BY trade_date ORDER BY trade_date"
    ).fetchall()

    out_of_range_days = []
    for d, cnt in daily_counts:
        if cnt < THRESHOLD_ZT_MIN or cnt > THRESHOLD_ZT_MAX:
            out_of_range_days.append((d, cnt))

    if out_of_range_days:
        vr.warn(f"有 {len(out_of_range_days)} 个交易日涨停数不在 [{THRESHOLD_ZT_MIN}, {THRESHOLD_ZT_MAX}] 范围内")
        for d, cnt in out_of_range_days:
            vr.line(f"         {d}: {cnt} 只")
    else:
        vr.ok(f"所有 {len(daily_counts)} 天涨停数均在 [{THRESHOLD_ZT_MIN}, {THRESHOLD_ZT_MAX}] 范围内")


# ── 3. fund_flow 校验 ────────────────────────────────────────────
def check_fund_flow(conn, vr: ValidationResult):
    vr.section("fund_flow (资金流向) 校验")
    if not table_exists(conn, "fund_flow"):
        vr.warn("fund_flow 表不存在，跳过")
        return

    total = conn.execute("SELECT COUNT(*) FROM fund_flow").fetchone()[0]
    distinct_dates = conn.execute("SELECT COUNT(DISTINCT trade_date) FROM fund_flow").fetchone()[0]
    date_range = conn.execute("SELECT MIN(trade_date), MAX(trade_date) FROM fund_flow").fetchone()
    vr.line(f"  总行数: {total}, 交易日: {distinct_dates}")
    vr.line(f"  日期范围: {date_range[0]} ~ {date_range[1]}")
    vr.line("")

    # 未来日期清理
    future_rows = conn.execute(
        "SELECT COUNT(*) FROM fund_flow WHERE trade_date > ?", (TODAY_STR,)
    ).fetchone()[0]
    if future_rows > 0:
        conn.execute("DELETE FROM fund_flow WHERE trade_date > ?", (TODAY_STR,))
        conn.commit()
        vr.cleanup(f"fund_flow: 删除 {future_rows} 条未来日期数据")

    # 日期连续性
    dates = conn.execute(
        "SELECT DISTINCT trade_date FROM fund_flow ORDER BY trade_date"
    ).fetchall()
    dates_list = [d[0] for d in dates]

    if len(dates_list) < 2:
        vr.warn("fund_flow 日期不足 2 天，无法检查连续性")
        return

    big_gaps = []
    for i in range(1, len(dates_list)):
        d_prev = datetime.strptime(dates_list[i - 1], "%Y-%m-%d").date()
        d_curr = datetime.strptime(dates_list[i], "%Y-%m-%d").date()
        gap = (d_curr - d_prev).days
        if gap > THRESHOLD_FUND_FLOW_GAP:
            big_gaps.append((dates_list[i - 1], dates_list[i], gap))

    if big_gaps:
        vr.warn(f"发现 {len(big_gaps)} 处日期缺口 > {THRESHOLD_FUND_FLOW_GAP} 天")
        for prev_d, curr_d, gap in big_gaps:
            vr.line(f"         {prev_d} → {curr_d}: 缺口 {gap} 天")
    else:
        vr.ok(f"日期连续性良好，最大缺口 <= {THRESHOLD_FUND_FLOW_GAP} 天")


# ── 4. factor_values 校验 ────────────────────────────────────────
def check_factor_coverage(conn, vr: ValidationResult):
    vr.section("factor_values (因子覆盖) 校验")
    if not table_exists(conn, "factor_values"):
        vr.warn("factor_values 表不存在，跳过")
        return

    total = conn.execute("SELECT COUNT(*) FROM factor_values").fetchone()[0]
    distinct_dates = conn.execute("SELECT COUNT(DISTINCT trade_date) FROM factor_values").fetchone()[0]
    date_range = conn.execute("SELECT MIN(trade_date), MAX(trade_date) FROM factor_values").fetchone()
    vr.line(f"  总行数: {total}, 交易日: {distinct_dates}")
    vr.line(f"  日期范围: {date_range[0]} ~ {date_range[1]}")
    vr.line("")

    # 未来日期清理
    future_rows = conn.execute(
        "SELECT COUNT(*) FROM factor_values WHERE trade_date > ?", (TODAY_STR,)
    ).fetchone()[0]
    if future_rows > 0:
        conn.execute("DELETE FROM factor_values WHERE trade_date > ?", (TODAY_STR,))
        conn.commit()
        vr.cleanup(f"factor_values: 删除 {future_rows} 条未来日期数据")

    # 获取 daily_price 每日股票总数用于计算覆盖率
    daily_total = {}
    dp_counts = conn.execute(
        "SELECT trade_date, COUNT(DISTINCT stock_code) as cnt FROM daily_price "
        "GROUP BY trade_date"
    ).fetchall()
    for d, cnt in dp_counts:
        daily_total[d] = cnt

    # 每个因子每天覆盖
    factors = conn.execute("SELECT DISTINCT factor_name FROM factor_values").fetchall()
    vr.line(f"  因子列表: {[f[0] for f in factors]}")
    vr.line("")

    any_warn = False
    for (factor_name,) in factors:
        daily_cov = conn.execute(
            "SELECT trade_date, COUNT(DISTINCT stock_code) as cnt "
            "FROM factor_values WHERE factor_name = ? "
            "GROUP BY trade_date ORDER BY trade_date",
            (factor_name,)
        ).fetchall()

        low_cov_days = []
        for d, cnt in daily_cov:
            total_stocks = daily_total.get(d, cnt)  # 无 daily_price 数据时默认用因子自身计数
            if total_stocks > 0 and cnt / total_stocks < THRESHOLD_FACTOR_COVERAGE_PCT:
                low_cov_days.append((d, cnt, total_stocks))
            elif cnt < THRESHOLD_FACTOR_COVERAGE:
                low_cov_days.append((d, cnt, total_stocks))

        if low_cov_days:
            any_warn = True
            vr.warn(f"因子 '{factor_name}': {len(low_cov_days)}/{len(daily_cov)} 天覆盖不足")
            for d, cnt, ts in low_cov_days[:5]:
                pct = cnt / ts * 100 if ts > 0 else 0
                vr.line(f"         {d}: {cnt}/{ts} ({pct:.1f}%)")
            if len(low_cov_days) > 5:
                vr.line(f"         ... 共 {len(low_cov_days)} 天")
        else:
            vr.ok(f"因子 '{factor_name}': 所有 {len(daily_cov)} 天覆盖率达标 (>= {THRESHOLD_FACTOR_COVERAGE} 只)")

    if not any_warn:
        vr.ok("所有因子每日覆盖率均达标")


# ── 5. 其他表概览 ────────────────────────────────────────────────
def check_other_tables(conn, vr: ValidationResult):
    vr.section("其他表概览")
    tables_to_check = [
        "strong_pool", "zb_pool", "lhb_detail", "market_emotion",
        "news", "concept_mapping", "concept_daily",
        "drift_events", "ic_series", "regime_state",
    ]
    for t in tables_to_check:
        if not table_exists(conn, t):
            vr.line(f"  {t:20s}: 表不存在")
            continue
        try:
            r = conn.execute(f"SELECT COUNT(*) FROM [{t}]").fetchone()
            cnt = r[0]
            # 尝试获取日期范围
            try:
                dr = conn.execute(
                    f"SELECT MIN(trade_date), MAX(trade_date) FROM [{t}]"
                ).fetchone()
                vr.line(f"  {t:20s}: {cnt:>6} rows, {dr[0]} ~ {dr[1]}")
                # 清理未来日期
                if dr[1] and dr[1] > TODAY_STR:
                    future = conn.execute(
                        f"SELECT COUNT(*) FROM [{t}] WHERE trade_date > ?", (TODAY_STR,)
                    ).fetchone()[0]
                    if future > 0:
                        conn.execute(f"DELETE FROM [{t}] WHERE trade_date > ?", (TODAY_STR,))
                        conn.commit()
                        vr.cleanup(f"{t}: 删除 {future} 条未来日期数据")
            except Exception:
                vr.line(f"  {t:20s}: {cnt:>6} rows (无 trade_date)")
        except Exception as e:
            vr.line(f"  {t:20s}: 查询失败 - {e}")


def main():
    if not DB_PATH.exists():
        print(f"错误: 数据库不存在 {DB_PATH}")
        sys.exit(1)

    vr = ValidationResult()
    conn = get_conn()

    try:
        check_daily_price(conn, vr)
        check_zt_pool(conn, vr)
        check_fund_flow(conn, vr)
        check_factor_coverage(conn, vr)
        check_other_tables(conn, vr)
    finally:
        conn.close()

    # 生成报告
    report = vr.render()

    # 输出到终端
    print(report)

    # 保存到文件
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text(report, encoding="utf-8")
    print(f"\n报告已保存至: {REPORT_PATH}")

    # 返回退出码
    sys.exit(1 if vr.has_failure else 0)


if __name__ == "__main__":
    main()
