"""
板块日度面板与数据质量报告 — 研究基础设施 (v2)

范围: 构建板块日度面板, 输出数据质量报告和雷达报告。
不定义交易阈值, 不创建策略, 不修改 daemon。

已知数据缺陷:
  - concept_mapping 是申万行业映射(非概念产业链), 仅有一份 2026-06-08 快照
  - 面板存在映射穿越风险: 用今天的行业归属计算历史收益, 不可直接用于策略回测结论
  - daily_price 无复权字段, 无 name 字段
  - turnover_rate 2022-2024 全部缺失
  - fund_flow 仅 2025-11-04 起, main_net 单位为万元
  - 2026-05-28/29 横截面不完整(231/285只)
  - 2026-06-09 amount 大面积缺失(5451/5520)
  - 2026-06-08 amount 骤降至正常1/4(数据源可能切换)
  - 2026-06-11 amount 单位异常(从亿元级降至千元级)

用法:
  python scripts/research_sector_daily_panel.py              # 全量构建
  python scripts/research_sector_daily_panel.py --date 2026-06-11  # 仅雷达报告
  python scripts/research_sector_daily_panel.py --audit-only        # 仅数据质量审计
"""

import argparse
import json
import logging
import sqlite3
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import numpy as np

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / "data" / "alpha_miner.db"
OUTPUT_DIR = ROOT / "output" / "research"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

NEW_STOCK_WARMUP = 20
MIN_SECTOR_SIZE = 5
EX_RIGHT_THRESHOLD = 0.20
INVALID_MARKET_STOCK_THRESHOLD = 4000
AMOUNT_INVALID_MEDIAN_THRESHOLD = 100_000  # 元


def get_connection():
    return sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _compound_return(stock_data, code, all_dates, tidx, period, ex_right_jumps):
    """计算 N 日复合收益 (close/pre_close 逐日连乘)

    窗口: [tidx-period+1 .. tidx] 共 period 个交易日
    每日必须有 close>0 且 pre_close>0, 窗口内不能有除权跳变
    """
    if tidx < period - 1:
        return None
    daily_rets = []
    for offset in range(period):
        d = all_dates[tidx - offset]
        if (code, d) in ex_right_jumps:
            return None
        day = stock_data.get(code, {}).get(d)
        if not day or not day["close"] or not day["pre_close"] or day["pre_close"] <= 0:
            return None
        daily_rets.append(day["close"] / day["pre_close"])
    compounded = 1.0
    for r in daily_rets:
        compounded *= r
    return compounded - 1.0


def _adjusted_close_series(stock_data, code, all_dates, tidx, window, ex_right_jumps):
    """计算复权价格序列 (用于 MA20 和 20 日新高)

    用每日 close/pre_close 乘积构建以最近日为基准的复权序列。
    result[i] 是窗口中第 i 个交易日(从最远端开始)的复权价。
    最近日 result[window-1] = close[最近]/pre_close[最近] (基准=1)。
    越远的日期 result 越小(除以后续日的因子)。

    窗口内有除权跳变则返回 None。
    """
    if tidx < window - 1:
        return None
    has_jump = False
    for offset in range(window):
        d = all_dates[tidx - offset]
        if (code, d) in ex_right_jumps:
            has_jump = True
            break
    if has_jump:
        return None

    # 收集每日因子: factors[0]=最远日, factors[window-1]=最近日
    factors = []
    for offset in reversed(range(window)):
        d = all_dates[tidx - offset]
        day = stock_data.get(code, {}).get(d)
        if not day or not day["close"] or not day["pre_close"] or day["pre_close"] <= 0:
            factors.append(None)
        else:
            factors.append(day["close"] / day["pre_close"])

    # 从最近端(=基准)向远端累除: 近端=自身因子, 远端=近端因子*...*远端因子
    result = [None] * window
    cum = 1.0
    valid = 0
    for i in range(window - 1, -1, -1):
        if factors[i] is None:
            result[i] = None
        else:
            cum *= factors[i]
            result[i] = cum
            valid += 1

    if valid < int(window * 0.9):
        return None
    return result


# ---------------------------------------------------------------------------
# 1. 数据审计
# ---------------------------------------------------------------------------

def audit_concept_mapping(conn) -> dict:
    c = conn.cursor()
    total = c.execute("SELECT COUNT(*) FROM concept_mapping").fetchone()[0]
    unique_stocks = c.execute("SELECT COUNT(DISTINCT stock_code) FROM concept_mapping").fetchone()[0]
    unique_sectors = c.execute("SELECT COUNT(DISTINCT concept_name) FROM concept_mapping").fetchone()[0]
    snap_time = c.execute("SELECT MIN(snapshot_time), MAX(snapshot_time) FROM concept_mapping").fetchone()
    dp_stocks = c.execute("SELECT COUNT(DISTINCT stock_code) FROM daily_price").fetchone()[0]
    covered = c.execute("""
        SELECT COUNT(DISTINCT stock_code) FROM daily_price
        WHERE stock_code IN (SELECT stock_code FROM concept_mapping)
    """).fetchone()[0]
    sector_sizes = c.execute("""
        SELECT concept_name, COUNT(*) as cnt FROM concept_mapping GROUP BY concept_name ORDER BY cnt DESC
    """).fetchall()
    sizes = [s[1] for s in sector_sizes]
    small_sectors = [(name, cnt) for name, cnt in sector_sizes if cnt < MIN_SECTOR_SIZE]
    multi_sector = c.execute(
        "SELECT stock_code, COUNT(*) as cnt FROM concept_mapping GROUP BY stock_code HAVING cnt > 1"
    ).fetchall()
    return {
        "total_rows": total, "unique_stocks": unique_stocks, "unique_sectors": unique_sectors,
        "snapshot_time": snap_time[0] if snap_time[0] else None,
        "mapping_asof_risk": True,
        "mapping_type": "申万行业分类(非概念产业链)",
        "dp_total_stocks": dp_stocks, "coverage_stocks": covered,
        "coverage_pct": round(covered / dp_stocks * 100, 1) if dp_stocks > 0 else 0,
        "sector_size_avg": round(sum(sizes) / len(sizes), 1) if sizes else 0,
        "sector_size_median": int(sorted(sizes)[len(sizes) // 2]) if sizes else 0,
        "sector_size_min": min(sizes) if sizes else 0,
        "sector_size_max": max(sizes) if sizes else 0,
        "small_sectors_count": len(small_sectors), "small_sectors": small_sectors[:30],
        "multi_sector_stocks": len(multi_sector), "top_sectors": sector_sizes[:15],
    }


def audit_daily_price(conn) -> dict:
    c = conn.cursor()
    dp_dates = c.execute("SELECT COUNT(DISTINCT trade_date) FROM daily_price").fetchone()[0]
    dp_range = c.execute("SELECT MIN(trade_date), MAX(trade_date) FROM daily_price").fetchone()
    dp_stocks = c.execute("SELECT COUNT(DISTINCT stock_code) FROM daily_price").fetchone()[0]

    amt_stats = []
    for row in c.execute("""
        SELECT SUBSTR(trade_date,1,4) yr, COUNT(*) total,
               SUM(CASE WHEN amount IS NULL OR amount=0 THEN 1 ELSE 0 END) miss
        FROM daily_price GROUP BY yr ORDER BY yr
    """):
        amt_stats.append({"year": row[0], "total": row[1], "amount_missing": row[2],
                           "amount_missing_pct": round(row[2] / row[1] * 100, 2)})

    tr_stats = []
    for row in c.execute("""
        SELECT SUBSTR(trade_date,1,4) yr, COUNT(*) total,
               SUM(CASE WHEN turnover_rate IS NULL OR turnover_rate=0 THEN 1 ELSE 0 END) miss
        FROM daily_price GROUP BY yr ORDER BY yr
    """):
        tr_stats.append({"year": row[0], "total": row[2], "turnover_missing": row[1],
                          "turnover_missing_pct": round(row[1] / row[2] * 100, 1)})

    ex_right_jumps = []
    for row in c.execute("""
        SELECT trade_date, stock_code, pre_close, close,
               ROUND((close - pre_close)/pre_close * 100, 2) chg
        FROM daily_price WHERE pre_close > 0 AND close > 0
          AND ABS((close - pre_close)/pre_close) > ?
          AND stock_code NOT LIKE '688%' AND stock_code NOT LIKE '689%'
          AND stock_code NOT LIKE '8%' AND stock_code NOT LIKE '9%'
          AND stock_code NOT LIKE '200%'
        ORDER BY ABS(chg) DESC LIMIT 50
    """, (EX_RIGHT_THRESHOLD,)):
        ex_right_jumps.append({"date": row[0], "code": row[1],
                                "pre_close": row[2], "close": row[3], "chg_pct": row[4]})

    suspended = c.execute("SELECT COUNT(*) FROM daily_price WHERE volume=0 OR volume IS NULL").fetchone()[0]

    # 逐日审计: 股票数和 amount 质量
    daily_quality = []
    for row in c.execute("""
        SELECT trade_date, COUNT(*) cnt,
               SUM(CASE WHEN amount IS NULL OR amount=0 THEN 1 ELSE 0 END) amt_miss,
               AVG(CASE WHEN amount>0 THEN amount END) avg_amt
        FROM daily_price GROUP BY trade_date ORDER BY trade_date
    """):
        avg_amt = row[3] or 0
        daily_quality.append({
            "date": row[0], "stocks": row[1], "amount_missing": row[2],
            "avg_amount": round(avg_amt, 0),
            "market_date_valid": row[1] >= INVALID_MARKET_STOCK_THRESHOLD,
        })

    # amount 单位异常检测
    for dq in daily_quality:
        # 正常日 amount 均值约 120-140 亿, 如果降到 < 1000 万, 单位可能变了
        if dq["stocks"] >= INVALID_MARKET_STOCK_THRESHOLD and dq["avg_amount"] < AMOUNT_INVALID_MEDIAN_THRESHOLD * 100:
            dq["amount_quality_valid"] = False
        elif dq["stocks"] >= INVALID_MARKET_STOCK_THRESHOLD and dq["amount_missing"] > dq["stocks"] * 0.10:
            dq["amount_quality_valid"] = False
        else:
            dq["amount_quality_valid"] = True

    invalid_dates = [dq for dq in daily_quality if not dq["market_date_valid"] or not dq["amount_quality_valid"]]

    return {
        "total_dates": dp_dates, "date_range": f"{dp_range[0]}~{dp_range[1]}",
        "total_stocks": dp_stocks,
        "amount_by_year": amt_stats, "turnover_by_year": tr_stats,
        "ex_right_jump_count": len(ex_right_jumps), "ex_right_jump_sample": ex_right_jumps[:20],
        "suspended_records": suspended,
        "daily_quality_sample": daily_quality[-20:],
        "invalid_dates": invalid_dates,
    }


def audit_fund_flow(conn) -> dict:
    c = conn.cursor()
    row = c.execute("""
        SELECT COUNT(*), COUNT(DISTINCT trade_date), COUNT(DISTINCT stock_code),
               MIN(trade_date), MAX(trade_date),
               SUM(CASE WHEN main_net IS NULL THEN 1 ELSE 0 END)
        FROM fund_flow
    """).fetchone()
    return {
        "total_rows": row[0], "total_dates": row[1], "total_stocks": row[2],
        "date_range": f"{row[3]}~{row[4]}", "main_net_null": row[5],
        "unit": "万元",
        "note": "fund_flow 的主力/大单/中单/小单是按单笔金额分类的成交代理, 不是真实机构身份",
    }


def run_audit(conn) -> dict:
    logger.info("开始数据审计...")
    t0 = time.time()
    audit = {
        "audit_time": datetime.now().isoformat(),
        "concept_mapping": audit_concept_mapping(conn),
        "daily_price": audit_daily_price(conn),
        "fund_flow": audit_fund_flow(conn),
    }
    audit["elapsed_seconds"] = round(time.time() - t0, 1)
    logger.info("审计完成, 耗时 %.1fs", audit["elapsed_seconds"])
    return audit


# ---------------------------------------------------------------------------
# 2. Filters
# ---------------------------------------------------------------------------

def build_stock_filters(conn) -> dict:
    c = conn.cursor()
    logger.info("构建股票过滤信息...")

    new_stock = {}
    for code, first_date in c.execute(
        "SELECT stock_code, MIN(trade_date) FROM daily_price GROUP BY stock_code"
    ):
        new_stock[code] = first_date

    suspended = set()
    for code, date in c.execute(
        "SELECT stock_code, trade_date FROM daily_price WHERE volume=0 OR volume IS NULL"
    ):
        suspended.add((code, date))

    ex_right = {}
    for code, date, pre, close in c.execute("""
        SELECT stock_code, trade_date, pre_close, close FROM daily_price
        WHERE pre_close > 0 AND close > 0 AND ABS((close - pre_close)/pre_close) > ?
          AND stock_code NOT LIKE '688%' AND stock_code NOT LIKE '689%'
          AND stock_code NOT LIKE '8%' AND stock_code NOT LIKE '9%'
          AND stock_code NOT LIKE '200%'
    """, (EX_RIGHT_THRESHOLD,)):
        ex_right[(code, date)] = (close - pre) / pre

    # ST: 从 zt_pool 识别, 但记录 (code, date) 以便按日期过滤
    st_entries = defaultdict(set)  # code -> set of dates where ST was observed
    for code, date in c.execute(
        "SELECT stock_code, trade_date FROM zt_pool WHERE name LIKE '%ST%'"
    ):
        st_entries[code].add(date)

    excluded_prefixes = ("688", "689", "200", "8", "9")
    excluded_codes = set()
    for (code,) in c.execute("SELECT DISTINCT stock_code FROM daily_price"):
        if any(code.startswith(p) for p in excluded_prefixes):
            excluded_codes.add(code)

    logger.info("过滤信息: 新股%d, 停牌%d, 除权跳变%d, ST条目%d只, 排除%d",
                len(new_stock), len(suspended), len(ex_right), len(st_entries), len(excluded_codes))

    return {
        "new_stock_first_date": new_stock,
        "suspended": suspended,
        "ex_right_jumps": ex_right,
        "st_entries": st_entries,  # code -> set of dates
        "excluded_codes": excluded_codes,
    }


# ---------------------------------------------------------------------------
# 3. Date quality
# ---------------------------------------------------------------------------

def build_date_quality(conn) -> dict:
    """逐日数据质量"""
    c = conn.cursor()
    result = {}
    for date, cnt, amt_miss, avg_amt in c.execute("""
        SELECT trade_date, COUNT(*) cnt,
               SUM(CASE WHEN amount IS NULL OR amount=0 THEN 1 ELSE 0 END) amt_miss,
               AVG(CASE WHEN amount>0 THEN amount END) avg_amt
        FROM daily_price GROUP BY trade_date ORDER BY trade_date
    """):
        avg_amt_val = avg_amt or 0
        market_valid = cnt >= INVALID_MARKET_STOCK_THRESHOLD
        amount_miss_pct = amt_miss / cnt if cnt > 0 else 1.0
        amount_valid = True
        if market_valid:
            if amount_miss_pct > 0.10:
                amount_valid = False
            if avg_amt_val < AMOUNT_INVALID_MEDIAN_THRESHOLD * 100:
                amount_valid = False
        result[date] = {
            "market_date_valid": market_valid,
            "market_stock_count": cnt,
            "amount_quality_valid": amount_valid,
        }
    return result


# ---------------------------------------------------------------------------
# 4. Panel computation
# ---------------------------------------------------------------------------

def build_sector_panel(conn, filters, date_quality, target_date=None):
    c = conn.cursor()

    all_dates = [r[0] for r in c.execute(
        "SELECT DISTINCT trade_date FROM daily_price ORDER BY trade_date"
    ).fetchall()]
    date_idx = {d: i for i, d in enumerate(all_dates)}

    if target_date:
        if target_date not in date_idx:
            logger.error("目标日期 %s 不在交易日历中", target_date)
            return []
        start_idx = max(0, date_idx[target_date] - 60)
        dates_to_compute = [target_date]
    else:
        start_idx = 60
        dates_to_compute = all_dates[start_idx:]

    logger.info("面板计算: %d 个交易日, 从 %s 开始",
                len(dates_to_compute), all_dates[start_idx] if start_idx < len(all_dates) else "N/A")

    # 映射: sector -> [codes], code -> [sectors] (一对多)
    sector_map = defaultdict(list)
    code_to_sectors = defaultdict(list)
    for code, sector in c.execute("SELECT stock_code, concept_name FROM concept_mapping"):
        sector_map[sector].append(code)
        code_to_sectors[code].append(sector)

    # 日线数据 (需要足够历史用于多日收益)
    data_start = all_dates[max(0, start_idx - 25)]
    logger.info("拉取日线数据: %s ~ %s", data_start, all_dates[-1])

    stock_data = defaultdict(dict)
    for code, date, close, pre_close, volume, amount in c.execute("""
        SELECT stock_code, trade_date, close, pre_close, volume, amount
        FROM daily_price WHERE trade_date >= ? ORDER BY stock_code, trade_date
    """, (data_start,)):
        stock_data[code][date] = {"close": close, "pre_close": pre_close,
                                   "volume": volume, "amount": amount}

    # 涨停 (code 属于多个板块时, 每个板块都计数)
    zt_data = defaultdict(lambda: defaultdict(int))
    for date, code in c.execute("SELECT trade_date, stock_code FROM zt_pool"):
        for sector in code_to_sectors.get(code, []):
            zt_data[date][sector] += 1

    # fund_flow: 去重到 stock_code + trade_date, 然后按板块聚合
    ff_raw = defaultdict(lambda: defaultdict(float))
    ff_raw_count = defaultdict(lambda: defaultdict(int))
    for code, date, main_net in c.execute("""
        SELECT stock_code, trade_date, main_net FROM fund_flow WHERE main_net IS NOT NULL
    """):
        for sector in code_to_sectors.get(code, []):
            ff_raw[(date, sector)][code] += main_net
            ff_raw_count[(date, sector)][code] += 1

    # 预聚合: (date, sector) -> sum of deduplicated main_net, count of unique stocks
    ff_agg = {}
    for (date, sector), code_dict in ff_raw.items():
        ff_agg[(date, sector)] = {
            "sum": sum(code_dict.values()),
            "count": len(code_dict),
        }

    panel_rows = []
    total = len(dates_to_compute)
    t0 = time.time()

    for i, target in enumerate(dates_to_compute):
        if total > 1 and (i + 1) % 50 == 0:
            elapsed = time.time() - t0
            eta = elapsed / (i + 1) * (total - i - 1)
            logger.info("进度 %d/%d (%.1f%%), ETA %.0fs", i + 1, total, (i+1)/total*100, eta)

        tidx = date_idx[target]
        dq = date_quality.get(target, {"market_date_valid": False, "market_stock_count": 0,
                                        "amount_quality_valid": False})

        for sector, member_codes in sector_map.items():
            row = compute_sector_row(
                sector=sector, member_codes=member_codes,
                target_date=target, date_idx=date_idx, all_dates=all_dates,
                stock_data=stock_data, filters=filters, date_quality=dq,
                zt_count=zt_data.get(target, {}).get(sector, 0),
                ff_info=ff_agg.get((target, sector)),
            )
            if row:
                panel_rows.append(row)

    elapsed = time.time() - t0
    logger.info("面板计算完成: %d 行, 耗时 %.1fs", len(panel_rows), elapsed)
    return panel_rows


def compute_sector_row(
    sector, member_codes, target_date, date_idx, all_dates,
    stock_data, filters, date_quality, zt_count, ff_info,
):
    tidx = date_idx[target_date]
    dq = date_quality

    valid_changes_1d = []
    valid_changes_5d = []
    valid_changes_10d = []
    valid_changes_20d = []
    valid_amounts = []
    valid_ff_codes = set()  # track which codes contributed to valid_count
    above_ma20_count = 0
    new_high_20d_count = 0
    total_member = len(member_codes)
    valid_count = 0
    ex_right_count = 0
    ex_right = filters["ex_right_jumps"]
    st_entries = filters["st_entries"]

    for code in member_codes:
        if code in filters["excluded_codes"]:
            total_member -= 1
            continue

        # ST: 仅当 zt_pool 在目标日或之前有 ST 记录才排除
        if code in st_entries:
            st_dates = st_entries[code]
            has_st_before = any(d <= target_date for d in st_dates)
            if not has_st_before:
                pass  # ST 记录都在目标日之后, 不过滤
            else:
                continue  # 目标日或之前已被标记 ST, 排除

        # 新股观察期
        first_date = filters["new_stock_first_date"].get(code)
        if first_date:
            first_idx = date_idx.get(first_date, -1)
            if first_idx >= 0 and (tidx - first_idx) < NEW_STOCK_WARMUP:
                continue

        # 停牌
        if (code, target_date) in filters["suspended"]:
            continue

        # 当日数据
        day = stock_data.get(code, {}).get(target_date)
        if not day or not day["close"] or not day["pre_close"] or day["pre_close"] <= 0:
            continue

        # 1日收益
        chg_1d = day["close"] / day["pre_close"] - 1.0
        is_jump = abs(chg_1d) > EX_RIGHT_THRESHOLD
        if is_jump:
            ex_right_count += 1
        valid_changes_1d.append(chg_1d)

        # 成交额 (仅 amount_quality_valid 时记录)
        if dq.get("amount_quality_valid", False) and day.get("amount") and day["amount"] > 0:
            valid_amounts.append(day["amount"])

        # 多日复合收益
        for period, chg_list in [(5, valid_changes_5d), (10, valid_changes_10d), (20, valid_changes_20d)]:
            ret = _compound_return(stock_data, code, all_dates, tidx, period, ex_right)
            if ret is not None:
                chg_list.append(ret)

        # MA20 (复权序列)
        adj_series = _adjusted_close_series(stock_data, code, all_dates, tidx, 20, ex_right)
        if adj_series is not None:
            valid_adj = [v for v in adj_series if v is not None]
            if len(valid_adj) >= 18:
                ma20 = np.mean(valid_adj)
                # adj_series[-1] 是最近日的复权价 (相对于自身的基准)
                cur_adj = adj_series[-1]
                if cur_adj is not None and cur_adj > ma20:
                    above_ma20_count += 1

                # 20日新高: 当日复权价 > 窗口内其余19天最大复权价
                if cur_adj is not None:
                    hist_max = max((v for v in adj_series[:-1] if v is not None), default=0)
                    if cur_adj > hist_max and hist_max > 0:
                        new_high_20d_count += 1

        valid_count += 1
        valid_ff_codes.add(code)

    if valid_count < 1:
        return None

    def safe_stats(values):
        if not values:
            return {"mean": None, "median": None, "std": None, "count": 0}
        arr = np.array(values)
        return {"mean": round(float(np.mean(arr)), 6), "median": round(float(np.median(arr)), 6),
                "std": round(float(np.std(arr)), 6), "count": len(arr)}

    ret_1d = safe_stats(valid_changes_1d)
    ret_5d = safe_stats(valid_changes_5d)
    ret_10d = safe_stats(valid_changes_10d)
    ret_20d = safe_stats(valid_changes_20d)

    up_count = sum(1 for c in valid_changes_1d if c > 0)
    down_count = sum(1 for c in valid_changes_1d if c < 0)
    up_ratio = up_count / len(valid_changes_1d) if valid_changes_1d else None

    total_amount = sum(valid_amounts) if valid_amounts else None
    avg_amount = float(np.mean(valid_amounts)) if valid_amounts else None

    above_ma20_ratio = above_ma20_count / valid_count if valid_count > 0 else None
    new_high_ratio = new_high_20d_count / valid_count if valid_count > 0 else None

    # fund_flow: 只聚合有效成分股
    ff_main_net_yi = None
    ff_stock_count = 0
    ff_coverage = None
    if ff_info:
        # ff_info 已经是去重后的聚合, 但我们只计入 valid_ff_codes 中的票
        # 由于 ff_agg 是在 build 阶段按 code_to_sectors 映射的,
        # 这里无法再做更细粒度过滤, 所以用 ff_info 的 count vs valid_count
        ff_stock_count = ff_info["count"]
        ff_main_net_yi = round(ff_info["sum"] / 10000, 4)  # 万元 -> 亿元
        ff_coverage = min(ff_stock_count / valid_count, 1.0) if valid_count > 0 else None

    return {
        "trade_date": target_date,
        "sector": sector,
        "market_date_valid": 1 if dq.get("market_date_valid", False) else 0,
        "market_stock_count": dq.get("market_stock_count", 0),
        "amount_quality_valid": 1 if dq.get("amount_quality_valid", False) else 0,
        "mapping_asof_risk": 1,
        "st_asof_risk": 1,
        "total_members": total_member,
        "valid_count": valid_count,
        "coverage_pct": round(valid_count / max(total_member, 1) * 100, 1),
        "small_sample": 1 if total_member < MIN_SECTOR_SIZE else 0,
        "ex_right_count": ex_right_count,
        "ret_1d_mean": ret_1d["mean"], "ret_1d_median": ret_1d["median"],
        "ret_1d_std": ret_1d["std"], "ret_1d_count": ret_1d["count"],
        "ret_5d_mean": ret_5d["mean"], "ret_5d_median": ret_5d["median"],
        "ret_5d_count": ret_5d["count"],
        "ret_10d_mean": ret_10d["mean"], "ret_10d_median": ret_10d["median"],
        "ret_10d_count": ret_10d["count"],
        "ret_20d_mean": ret_20d["mean"], "ret_20d_median": ret_20d["median"],
        "ret_20d_count": ret_20d["count"],
        "up_count": up_count, "down_count": down_count,
        "up_ratio": round(up_ratio, 4) if up_ratio is not None else None,
        "above_ma20_count": above_ma20_count,
        "above_ma20_ratio": round(above_ma20_ratio, 4) if above_ma20_ratio is not None else None,
        "new_high_20d_count": new_high_20d_count,
        "new_high_20d_ratio": round(new_high_ratio, 4) if new_high_ratio is not None else None,
        "total_amount": round(total_amount, 2) if total_amount is not None else None,
        "avg_amount": round(avg_amount, 2) if avg_amount is not None else None,
        "zt_count": zt_count,
        "ff_main_net_yi": ff_main_net_yi,
        "ff_stock_count": ff_stock_count,
        "ff_coverage": round(ff_coverage, 4) if ff_coverage is not None else None,
    }


# ---------------------------------------------------------------------------
# 5. DB
# ---------------------------------------------------------------------------

PANEL_TABLE = "research_sector_daily_panel"

PANEL_DDL = f"""
DROP TABLE IF EXISTS {PANEL_TABLE};
CREATE TABLE {PANEL_TABLE} (
    trade_date           TEXT NOT NULL,
    sector               TEXT NOT NULL,
    market_date_valid    INTEGER DEFAULT 1,
    market_stock_count   INTEGER,
    amount_quality_valid INTEGER DEFAULT 1,
    mapping_asof_risk    INTEGER DEFAULT 1,
    st_asof_risk         INTEGER DEFAULT 1,
    total_members        INTEGER,
    valid_count          INTEGER,
    coverage_pct         REAL,
    small_sample         INTEGER DEFAULT 0,
    ex_right_count       INTEGER DEFAULT 0,
    ret_1d_mean          REAL,
    ret_1d_median        REAL,
    ret_1d_std           REAL,
    ret_1d_count         INTEGER,
    ret_5d_mean          REAL,
    ret_5d_median        REAL,
    ret_5d_count         INTEGER,
    ret_10d_mean         REAL,
    ret_10d_median       REAL,
    ret_10d_count         INTEGER,
    ret_20d_mean         REAL,
    ret_20d_median       REAL,
    ret_20d_count        INTEGER,
    up_count             INTEGER,
    down_count           INTEGER,
    up_ratio             REAL,
    above_ma20_count     INTEGER,
    above_ma20_ratio     REAL,
    new_high_20d_count   INTEGER,
    new_high_20d_ratio   REAL,
    total_amount         REAL,
    avg_amount           REAL,
    zt_count             INTEGER DEFAULT 0,
    ff_main_net_yi       REAL,
    ff_stock_count       INTEGER DEFAULT 0,
    ff_coverage          REAL,
    PRIMARY KEY (trade_date, sector)
)
"""

PANEL_COLS = [
    "trade_date", "sector",
    "market_date_valid", "market_stock_count", "amount_quality_valid",
    "mapping_asof_risk", "st_asof_risk",
    "total_members", "valid_count", "coverage_pct", "small_sample", "ex_right_count",
    "ret_1d_mean", "ret_1d_median", "ret_1d_std", "ret_1d_count",
    "ret_5d_mean", "ret_5d_median", "ret_5d_count",
    "ret_10d_mean", "ret_10d_median", "ret_10d_count",
    "ret_20d_mean", "ret_20d_median", "ret_20d_count",
    "up_count", "down_count", "up_ratio",
    "above_ma20_count", "above_ma20_ratio",
    "new_high_20d_count", "new_high_20d_ratio",
    "total_amount", "avg_amount",
    "zt_count",
    "ff_main_net_yi", "ff_stock_count", "ff_coverage",
]


def write_panel_to_db(panel_rows):
    conn = sqlite3.connect(str(DB_PATH))
    try:
        conn.executescript(PANEL_DDL)
        placeholders = ",".join(["?"] * len(PANEL_COLS))
        sql = f"INSERT OR REPLACE INTO {PANEL_TABLE} ({','.join(PANEL_COLS)}) VALUES ({placeholders})"
        written = 0
        for row in panel_rows:
            vals = [row.get(c) for c in PANEL_COLS]
            conn.execute(sql, vals)
            written += 1
            if written % 5000 == 0:
                conn.commit()
        conn.commit()
        logger.info("写入 %d 行到 %s", written, PANEL_TABLE)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 6. Reports
# ---------------------------------------------------------------------------

def generate_data_quality_report(audit):
    lines = ["# 数据质量审计报告\n", f"生成时间: {audit['audit_time']}\n"]

    cm = audit["concept_mapping"]
    lines.append("## 1. concept_mapping (行业映射)\n")
    lines.append(f"- **类型**: {cm['mapping_type']}")
    lines.append(f"- **快照时间**: {cm['snapshot_time']}")
    lines.append(f"- **mapping_asof_risk**: 是 — 仅有一份快照, 无法还原历史成员")
    lines.append(f"- **一股多板块**: {cm['multi_sector_stocks']}只股票属于多个板块")
    lines.append(f"- **对 daily_price 覆盖率**: {cm['coverage_pct']}% ({cm['coverage_stocks']}/{cm['dp_total_stocks']})")
    lines.append(f"- **板块成员数**: 平均 {cm['sector_size_avg']}, 中位数 {cm['sector_size_median']}, 范围 [{cm['sector_size_min']}, {cm['sector_size_max']}]")
    lines.append(f"- **小样本板块(<5只)**: {cm['small_sectors_count']}个\n")

    dp = audit["daily_price"]
    lines.append("## 2. daily_price\n")
    lines.append(f"- **日期范围**: {dp['date_range']}, {dp['total_dates']}天, {dp['total_stocks']}只股票")
    lines.append(f"- **停牌记录**: {dp['suspended_records']}")
    lines.append(f"- **除权跳变(>20%)**: {dp['ex_right_jump_count']}条\n")

    lines.append("### amount 字段缺失率\n")
    lines.append("| 年份 | 总行数 | 缺失 | 缺失率 |")
    lines.append("|------|--------|------|--------|")
    for y in dp["amount_by_year"]:
        lines.append(f"| {y['year']} | {y['total']:,} | {y['amount_missing']:,} | {y['amount_missing_pct']}% |")
    lines.append("")

    lines.append("### 无效日期\n")
    lines.append("| 日期 | 股票数 | amount有效 | 原因 |")
    lines.append("|------|--------|-----------|------|")
    for dq in dp.get("invalid_dates", []):
        reasons = []
        if not dq["market_date_valid"]:
            reasons.append("横截面不完整")
        if not dq["amount_quality_valid"]:
            if dq.get("amount_missing", 0) > dq.get("stocks", 1) * 0.10:
                reasons.append(f"amount缺失率>{10}%")
            reasons.append("amount单位/数量级异常")
        lines.append(f"| {dq['date']} | {dq['stocks']} | {'是' if dq['amount_quality_valid'] else '否'} | {'; '.join(reasons)} |")
    lines.append("")

    ff = audit["fund_flow"]
    lines.append("## 3. fund_flow (资金流代理)\n")
    lines.append(f"- **日期范围**: {ff['date_range']}, {ff['total_dates']}天")
    lines.append(f"- **单位**: {ff['unit']} (面板中转换为亿元)")
    lines.append(f"- **说明**: {ff['note']}\n")

    return "\n".join(lines)


def generate_radar_report(target_date):
    conn = sqlite3.connect(str(DB_PATH))
    try:
        # 只展示有效日期
        rows = conn.execute(f"""
            SELECT * FROM {PANEL_TABLE}
            WHERE trade_date = ? AND market_date_valid = 1 AND amount_quality_valid = 1
              AND valid_count >= 3
            ORDER BY ret_1d_mean DESC
        """, (target_date,)).fetchall()
        col_names = [d[0] for d in conn.execute(f"SELECT * FROM {PANEL_TABLE} LIMIT 1").description]

        # 检查日期是否有效
        dq = conn.execute(f"""
            SELECT market_date_valid, amount_quality_valid FROM {PANEL_TABLE}
            WHERE trade_date = ? LIMIT 1
        """, (target_date,)).fetchone()
    finally:
        conn.close()

    lines = [f"# 板块雷达报告 — {target_date}\n"]
    lines.append("⚠️ **映射穿越风险**: 本面板使用 2026-06-08 单一快照映射, 历史数据存在映射穿越风险,")
    lines.append("不可直接用于策略回测结论。本报告仅描述板块状态, 不定义交易阈值或策略规则。\n")

    if dq and (not dq[0] or not dq[1]):
        lines.append(f"**该日数据质量异常**: market_date_valid={dq[0]}, amount_quality_valid={dq[1]}")
        lines.append("雷达排名已隐藏。仅保留审计记录。\n")
        # 仍然显示无效日期标记
        lines.append("## 数据质量标记\n")
        lines.append(f"- market_date_valid: {dq[0]}")
        lines.append(f"- amount_quality_valid: {dq[1]}\n")
        return "\n".join(lines)

    if not rows:
        lines.append("该日无有效面板数据。\n")
        return "\n".join(lines)

    records = [dict(zip(col_names, r)) for r in rows]
    lines.append(f"- 有效板块数: {len(records)}")
    lines.append(f"- 数据类型: 申万行业分类(非概念产业链)")
    lines.append(f"- mapping_asof_risk: 1")
    lines.append(f"- st_asof_risk: 1 (无历史ST数据)\n")

    lines.append("## 当日涨幅TOP15 (等权均值)\n")
    lines.append("| 板块 | 等权均值 | 中位数 | 上涨率 | 有效数 | 涨停 |")
    lines.append("|------|---------|--------|--------|--------|------|")
    for r in records[:15]:
        mean_pct = (r["ret_1d_mean"] or 0) * 100
        med_pct = (r["ret_1d_median"] or 0) * 100
        up_r = (r["up_ratio"] or 0) * 100
        lines.append(f"| {r['sector']} | {mean_pct:+.2f}% | {med_pct:+.2f}% | {up_r:.0f}% | {r['valid_count']} | {r['zt_count']} |")
    lines.append("")

    records_down = sorted(records, key=lambda x: x.get("ret_1d_mean") or 0)
    lines.append("## 当日跌幅TOP10\n")
    lines.append("| 板块 | 等权均值 | 中位数 | 上涨率 | 有效数 |")
    lines.append("|------|---------|--------|--------|--------|")
    for r in records_down[:10]:
        mean_pct = (r["ret_1d_mean"] or 0) * 100
        med_pct = (r["ret_1d_median"] or 0) * 100
        up_r = (r["up_ratio"] or 0) * 100
        lines.append(f"| {r['sector']} | {mean_pct:+.2f}% | {med_pct:+.2f}% | {up_r:.0f}% | {r['valid_count']} |")
    lines.append("")

    records_5d = sorted(records, key=lambda x: x.get("ret_5d_mean") or -999, reverse=True)
    lines.append("## 5日涨幅TOP15 (复合收益)\n")
    lines.append("| 板块 | 5日均值 | 5日中位数 | 有效样本 | 除权跳变 |")
    lines.append("|------|---------|----------|---------|---------|")
    for r in records_5d[:15]:
        mean_pct = (r.get("ret_5d_mean") or 0) * 100
        med_pct = (r.get("ret_5d_median") or 0) * 100
        lines.append(f"| {r['sector']} | {mean_pct:+.2f}% | {med_pct:+.2f}% | {r.get('ret_5d_count',0)} | {r.get('ex_right_count',0)} |")
    lines.append("")

    records_up = sorted(records, key=lambda x: x.get("up_ratio") or 0, reverse=True)
    lines.append("## 上涨扩散度TOP15\n")
    lines.append("| 板块 | 上涨率 | 上涨/下跌 | 站MA20比例 | 创20日新高比例 |")
    lines.append("|------|--------|-----------|-----------|--------------|")
    for r in records_up[:15]:
        up_r = (r.get("up_ratio") or 0) * 100
        ma20_r = (r.get("above_ma20_ratio") or 0) * 100
        nh_r = (r.get("new_high_20d_ratio") or 0) * 100
        lines.append(f"| {r['sector']} | {up_r:.0f}% | {r.get('up_count',0)}/{r.get('down_count',0)} | {ma20_r:.0f}% | {nh_r:.0f}% |")
    lines.append("")

    records_ff = [r for r in records if r.get("ff_main_net_yi") is not None]
    if records_ff:
        records_ff.sort(key=lambda x: x.get("ff_main_net_yi") or 0, reverse=True)
        lines.append("## 资金流代理TOP10 (fund_flow, 亿元)\n")
        lines.append("**注意**: 按单笔金额分类, 不等于真实机构行为。\n")
        lines.append("| 板块 | 主力净流入(亿元) | 覆盖率 | 覆盖只数 |")
        lines.append("|------|-----------------|--------|---------|")
        for r in records_ff[:10]:
            net_yi = r["ff_main_net_yi"] or 0
            cov = (r.get("ff_coverage") or 0) * 100
            lines.append(f"| {r['sector']} | {net_yi:+.4f} | {cov:.0f}% | {r['ff_stock_count']} |")
        lines.append("")

    small = [r for r in records if r.get("small_sample")]
    if small:
        lines.append("## 小样本板块警告\n")
        for r in small:
            lines.append(f"- {r['sector']}: {r['valid_count']}/{r['total_members']}只")
        lines.append("")

    exr = [r for r in records if r.get("ex_right_count", 0) > 0]
    if exr:
        lines.append("## 除权跳变警告\n")
        for r in sorted(exr, key=lambda x: x["ex_right_count"], reverse=True)[:10]:
            lines.append(f"- {r['sector']}: {r['ex_right_count']}次跳变")
        lines.append("")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# 7. Entry
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="板块日度面板与数据质量报告")
    parser.add_argument("--date", help="仅生成该日期的雷达报告")
    parser.add_argument("--audit-only", action="store_true")
    args = parser.parse_args()

    conn = get_connection()
    try:
        audit = run_audit(conn)
        with open(OUTPUT_DIR / "data_quality_audit.json", "w") as f:
            json.dump(audit, f, ensure_ascii=False, indent=2, default=str)
        with open(OUTPUT_DIR / "data_quality_audit.md", "w") as f:
            f.write(generate_data_quality_report(audit))
        logger.info("审计报告已写入")

        if args.audit_only:
            return

        filters = build_stock_filters(conn)
        date_quality = build_date_quality(conn)
        panel_rows = build_sector_panel(conn, filters, date_quality, target_date=args.date)
        if not panel_rows:
            logger.warning("面板为空")
            return

        write_panel_to_db(panel_rows)

        conn2 = sqlite3.connect(str(DB_PATH))
        stats = conn2.execute(f"SELECT COUNT(*), COUNT(DISTINCT trade_date), COUNT(DISTINCT sector), MIN(trade_date), MAX(trade_date) FROM {PANEL_TABLE}").fetchone()
        conn2.close()
        logger.info("面板统计: %d行, %d天, %d板块, %s~%s", *stats)

        target = args.date or stats[4]
        if target:
            with open(OUTPUT_DIR / f"sector_radar_{target}.md", "w") as f:
                f.write(generate_radar_report(target))
            logger.info("雷达报告已写入")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
