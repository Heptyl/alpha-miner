"""backtest_northbound_lockup_ic.py — 北向资金+解禁风险因子IC回测

计算截面IC:
  - northbound_5d: 最近5天main_net持续净流入(1)/净流出(-1)/其他(0)
  - lockup_risk: 未来30天有解禁且>日均成交额50%(1)/无(0)

IC = corr(factor_value, forward_return) 取截面均值
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path
from datetime import datetime, timedelta

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

DB_PATH = Path(__file__).resolve().parents[1] / "data" / "alpha_miner.db"


def compute_northbound_ic(conn) -> dict:
    """计算北向/主力资金5日持续流入因子的IC

    对每个交易日:
      1. 取有fund_flow数据的股票
      2. factor = 最近5天main_net之和的符号(1/-1/0)
      3. forward_return = 次日收益率
      4. IC = spearman_corr(factor, forward_return)
    """
    try:
        from scipy.stats import spearmanr
    except ImportError:
        print("需要scipy: uv add scipy")
        return {}

    # 取有fund_flow数据的交易日
    dates = conn.execute("""
        SELECT DISTINCT trade_date FROM fund_flow
        ORDER BY trade_date DESC LIMIT 60
    """).fetchall()
    dates = [d[0] for d in dates]

    ics = []
    for i, dt in enumerate(dates[:-1]):
        next_dt = dates[i - 1] if i > 0 else None  # dates是倒序的
        if not next_dt:
            continue

        # 取当日有fund_flow的股票的5日main_net
        rows = conn.execute("""
            SELECT ff.stock_code,
                   SUM(COALESCE(ff.super_large_net, 0) + COALESCE(ff.large_net, 0)) as total_main
            FROM fund_flow ff
            WHERE ff.stock_code IN (
                SELECT stock_code FROM fund_flow WHERE trade_date = ?
            )
            AND ff.trade_date <= ?
            AND ff.trade_date >= date(?, '-5 days')
            GROUP BY ff.stock_code
        """, (dt, dt, dt)).fetchall()

        if len(rows) < 10:
            continue

        # factor = sign of total main net
        factors = {}
        for code, total in rows:
            factors[code] = 1 if total > 0 else (-1 if total < 0 else 0)

        # forward return
        fwd = conn.execute("""
            SELECT a.stock_code,
                   (b.close - a.close) / a.close as fwd_ret
            FROM daily_price a
            JOIN daily_price b ON a.stock_code = b.stock_code AND b.trade_date = ?
            WHERE a.trade_date = ?
              AND a.stock_code IN ({})
              AND a.close > 0
        """.format(",".join("?" * len(factors))),
                            (next_dt, dt, *list(factors.keys()))).fetchall()

        if len(fwd) < 10:
            continue

        x, y = [], []
        for code, ret in fwd:
            if code in factors and ret is not None:
                x.append(factors[code])
                y.append(ret)

        if len(x) >= 10 and len(set(x)) > 1:
            ic, _ = spearmanr(x, y)
            if ic is not None:
                ics.append(ic)

    if not ics:
        return {"ic_mean": 0, "ic_std": 0, "icir": 0, "samples": 0}

    import statistics
    ic_mean = statistics.mean(ics)
    ic_std = statistics.stdev(ics) if len(ics) > 1 else 0
    return {
        "ic_mean": round(ic_mean, 4),
        "ic_std": round(ic_std, 4),
        "icir": round(ic_mean / ic_std, 2) if ic_std > 0 else 0,
        "samples": len(ics),
        "win_rate": round(sum(1 for ic in ics if ic > 0) / len(ics), 2),
    }


def compute_lockup_ic(conn) -> dict:
    """计算解禁风险因子的IC

    对每个交易日:
      1. factor = 未来30天有无大额解禁(1=有风险, 0=无)
      2. forward_return = 次日收益率
      3. IC = spearman_corr(factor, forward_return)
    """
    try:
        from scipy.stats import spearmanr
    except ImportError:
        return {}

    # 取有lockup数据的日期范围
    dates = conn.execute("""
        SELECT DISTINCT trade_date FROM daily_price
        WHERE trade_date >= (SELECT MIN(free_date) FROM lockup_calendar)
          AND trade_date <= (SELECT MAX(free_date) FROM lockup_calendar)
        ORDER BY trade_date DESC LIMIT 60
    """).fetchall()
    dates = [d[0] for d in dates]

    ics = []
    for i, dt in enumerate(dates[:-1]):
        next_dt = dates[i - 1] if i > 0 else None
        if not next_dt:
            continue

        # 解禁风险: 未来30天有大额解禁(>日均成交50%)
        lockup_codes = conn.execute("""
            SELECT DISTINCT lc.stock_code
            FROM lockup_calendar lc
            WHERE lc.free_date >= ? AND lc.free_date <= date(?, '+30 days')
              AND lc.lift_market_cap > 0
        """, (dt, dt)).fetchall()
        lockup_set = {r[0] for r in lockup_codes}

        if not lockup_set:
            continue

        # forward return
        fwd = conn.execute("""
            SELECT a.stock_code,
                   (b.close - a.close) / a.close as fwd_ret
            FROM daily_price a
            JOIN daily_price b ON a.stock_code = b.stock_code AND b.trade_date = ?
            WHERE a.trade_date = ? AND a.close > 0
        """, (next_dt, dt)).fetchall()

        if len(fwd) < 10:
            continue

        x, y = [], []
        for code, ret in fwd:
            if ret is not None:
                x.append(1 if code in lockup_set else 0)
                y.append(ret)

        if len(set(x)) < 2 or len(x) < 10:
            continue

        ic, _ = spearmanr(x, y)
        if ic is not None:
            ics.append(ic)

    if not ics:
        return {"ic_mean": 0, "ic_std": 0, "icir": 0, "samples": 0}

    import statistics
    ic_mean = statistics.mean(ics)
    ic_std = statistics.stdev(ics) if len(ics) > 1 else 0
    return {
        "ic_mean": round(ic_mean, 4),
        "ic_std": round(ic_std, 4),
        "icir": round(ic_mean / ic_std, 2) if ic_std > 0 else 0,
        "samples": len(ics),
        "win_rate": round(sum(1 for ic in ics if ic > 0) / len(ics), 2),
    }


def main():
    conn = sqlite3.connect(str(DB_PATH))

    print("=" * 50)
    print("因子IC回测: 北向资金 + 解禁风险")
    print("=" * 50)

    # 数据概览
    ff_count = conn.execute("SELECT COUNT(*) FROM fund_flow").fetchone()[0]
    lk_count = conn.execute("SELECT COUNT(*) FROM lockup_calendar").fetchone()[0]
    dp_count = conn.execute("SELECT COUNT(DISTINCT trade_date) FROM daily_price").fetchone()[0]
    print(f"\n数据概览:")
    print(f"  daily_price: {dp_count}天")
    print(f"  fund_flow: {ff_count}条")
    print(f"  lockup_calendar: {lk_count}条")

    # 北向/主力资金因子
    print(f"\n--- northbound_5d (主力5日持续流入) ---")
    nb_result = compute_northbound_ic(conn)
    print(f"  IC均值: {nb_result.get('ic_mean', 'N/A')}")
    print(f"  IC标准差: {nb_result.get('ic_std', 'N/A')}")
    print(f"  ICIR: {nb_result.get('icir', 'N/A')}")
    print(f"  IC胜率: {nb_result.get('win_rate', 'N/A')}")
    print(f"  样本数: {nb_result.get('samples', 'N/A')}")

    # 解禁风险因子
    print(f"\n--- lockup_risk (解禁风险) ---")
    lk_result = compute_lockup_ic(conn)
    print(f"  IC均值: {lk_result.get('ic_mean', 'N/A')}")
    print(f"  IC标准差: {lk_result.get('ic_std', 'N/A')}")
    print(f"  ICIR: {lk_result.get('icir', 'N/A')}")
    print(f"  IC胜率: {lk_result.get('win_rate', 'N/A')}")
    print(f"  样本数: {lk_result.get('samples', 'N/A')}")

    # 结论
    print(f"\n--- 结论 ---")
    for name, r in [("northbound_5d", nb_result), ("lockup_risk", lk_result)]:
        ic_mean = r.get("ic_mean", 0)
        icir = r.get("icir", 0)
        if ic_mean == 0 and r.get("samples", 0) == 0:
            print(f"  {name}: 数据不足, 无法计算IC")
        elif abs(ic_mean) > 0.03 and abs(icir) > 0.5:
            sig = "显著" if ic_mean > 0 else "反向显著"
            print(f"  {name}: {sig}(IC={ic_mean}, ICIR={icir})")
        else:
            print(f"  {name}: 不显著(IC={ic_mean}, ICIR={icir})")

    conn.close()


if __name__ == "__main__":
    main()
