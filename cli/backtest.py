"""回测 CLI — python -m cli.backtest

用法:
  python -m cli.backtest --compute-today
  python -m cli.backtest --factor zt_dt_ratio --start 2024-01-01 --end 2024-06-30
"""

import argparse
import sys
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from src.data.storage import Storage
from src.factors.registry import FactorRegistry
from src.drift.ic_tracker import ICTracker


def get_universe(db: Storage, as_of: datetime) -> list[str]:
    """获取 as_of 日的股票 universe。"""
    date_str = as_of.strftime("%Y-%m-%d")
    df = db.query("daily_price", as_of, where="trade_date = ?", params=(date_str,))
    if df.empty:
        return []
    return sorted(df["stock_code"].unique().tolist())


def compute_today(db_path: str = "data/alpha_miner.db"):
    """计算今日所有因子值并写入 factor_values 表。"""
    from src.data.storage import Storage

    db = Storage(db_path)
    as_of = datetime.now()
    date_str = as_of.strftime("%Y-%m-%d")
    snap_time = as_of

    universe = get_universe(db, as_of)
    if not universe:
        print(f"[WARN] {date_str} 无行情数据，尝试最近一个交易日")
        # 尝试往前找
        for i in range(1, 10):
            prev = as_of - timedelta(days=i)
            universe = get_universe(db, prev)
            if universe:
                date_str = prev.strftime("%Y-%m-%d")
                as_of = prev
                break
        if not universe:
            print("[ERROR] 找不到可用交易日数据")
            return

    print(f"[INFO] Universe: {len(universe)} 只股票, date={date_str}")

    registry = FactorRegistry()
    factor_names = registry.list_factors()

    total_rows = 0
    for name in factor_names:
        try:
            factor = registry.get_factor(name)
            values = factor.compute(universe, as_of, db)
            if values.empty:
                print(f"  {name}: 无数据")
                continue

            rows = []
            for code, val in values.items():
                if pd.notna(val):
                    rows.append({
                        "factor_name": name,
                        "stock_code": code,
                        "trade_date": date_str,
                        "factor_value": float(val),
                    })

            if rows:
                df = pd.DataFrame(rows)
                db.insert("factor_values", df, snapshot_time=snap_time)
                total_rows += len(rows)
                print(f"  {name}: {len(rows)} 条")

        except Exception as e:
            print(f"  {name}: ERROR - {e}")

    print(f"\n[DONE] 共写入 {total_rows} 条因子值")


def backtest_factor(
    factor_name: str,
    start_date: str,
    end_date: str,
    db_path: str = "data/alpha_miner.db",
    forward_days: int = 1,
    quantiles: int = 5,
):
    """单因子回测 — 分组收益 + IC 时序。"""
    db = Storage(db_path)
    registry = FactorRegistry()

    try:
        factor = registry.get_factor(factor_name)
    except KeyError as e:
        print(f"[ERROR] {e}")
        return

    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    # 获取交易日历
    price_df = db.query_range("daily_price", end, lookback_days=(end - start).days + 10)
    if price_df.empty:
        print("[ERROR] 无价格数据")
        return

    trade_dates = sorted(price_df["trade_date"].unique())
    trade_dates = [d for d in trade_dates if start_date <= d <= end_date]

    if not trade_dates:
        print("[ERROR] 日期范围内无交易日")
        return

    print(f"[INFO] 因子: {factor_name} ({factor.description})")
    print(f"[INFO] 回测区间: {trade_dates[0]} ~ {trade_dates[-1]}, 共 {len(trade_dates)} 个交易日")
    print(f"[INFO] 分组数: {quantiles}, forward: {forward_days}日")

    all_group_returns = {q: [] for q in range(1, quantiles + 1)}
    ic_list = []

    for i, date in enumerate(trade_dates):
        as_of = datetime.strptime(date, "%Y-%m-%d")
        universe = get_universe(db, as_of)
        if not universe:
            continue

        # 计算因子值
        try:
            values = factor.compute(universe, as_of, db)
        except Exception:
            continue

        if values.empty:
            continue

        # 分组
        values = values.dropna()
        if len(values) < quantiles:
            continue

        try:
            labels = pd.qcut(values.rank(method="first"), quantiles, labels=False) + 1
        except ValueError:
            continue

        # 取 forward return
        if i + forward_days >= len(trade_dates):
            continue

        future_date = trade_dates[min(i + forward_days, len(trade_dates) - 1)]
        current_prices = db.query("daily_price", as_of, where="trade_date = ?", params=(date,))
        future_prices = db.query(
            "daily_price",
            datetime.strptime(future_date, "%Y-%m-%d"),
            where="trade_date = ?",
            params=(future_date,),
        )

        if current_prices.empty or future_prices.empty:
            continue

        cur_p = current_prices.set_index("stock_code")["close"]
        fut_p = future_prices.set_index("stock_code")["close"]

        common = cur_p.index.intersection(fut_p.index).intersection(values.index)
        if len(common) < quantiles:
            continue

        fwd_ret = (fut_p.loc[common] - cur_p.loc[common]) / cur_p.loc[common]

        # 按组统计
        for q in range(1, quantiles + 1):
            group_codes = labels[labels == q].index.intersection(common)
            if len(group_codes) > 0:
                mean_ret = float(fwd_ret.loc[group_codes].mean())
                all_group_returns[q].append(mean_ret)

        # IC
        from scipy.stats import spearmanr
        common_vals = values.loc[common]
        common_rets = fwd_ret.loc[common]
        valid = common_vals.notna() & common_rets.notna()
        if valid.sum() >= 5:
            ic, _ = spearmanr(common_vals[valid], common_rets[valid])
            ic_list.append(ic)

    # 输出结果
    print(f"\n{'='*60}")
    print(f"  回测结果: {factor_name}")
    print(f"{'='*60}")
    print(f"  {'分组':<8} {'均值收益':>12} {'天数':>8}")
    print(f"  {'-'*32}")

    for q in range(1, quantiles + 1):
        rets = all_group_returns.get(q, [])
        if rets:
            mean_ret = np.mean(rets) * 100
            print(f"  Q{q:<7} {mean_ret:>11.4f}% {len(rets):>8}")

    if all_group_returns.get(quantiles) and all_group_returns.get(1):
        spread = np.mean(all_group_returns[quantiles]) - np.mean(all_group_returns[1])
        print(f"\n  多空价差 (Q{quantiles}-Q1): {spread*100:.4f}%")

    if ic_list:
        ic_arr = np.array(ic_list)
        print(f"\n  IC 均值: {np.mean(ic_arr):.4f}")
        print(f"  ICIR:    {np.mean(ic_arr)/np.std(ic_arr):.4f}" if np.std(ic_arr) > 0 else "  ICIR: N/A")
        print(f"  IC 胜率: {(ic_arr > 0).sum() / len(ic_arr):.2%}")
        print(f"  IC 样本数: {len(ic_list)}")

    print(f"{'='*60}")


def main():
    parser = argparse.ArgumentParser(description="Alpha Miner 回测")
    parser.add_argument("--compute-today", action="store_true", help="计算今日所有因子值")
    parser.add_argument("--factor", type=str, help="单因子回测的因子名")
    parser.add_argument("--start", type=str, help="回测开始日期")
    parser.add_argument("--end", type=str, help="回测结束日期")
    parser.add_argument("--db", type=str, default="data/alpha_miner.db", help="数据库路径")
    parser.add_argument("--forward", type=int, default=1, help="forward 天数")
    parser.add_argument("--quantiles", type=int, default=5, help="分组数")
    args = parser.parse_args()

    if args.compute_today:
        compute_today(args.db)
    elif args.factor:
        if not args.start or not args.end:
            print("[ERROR] 单因子回测需要 --start 和 --end")
            sys.exit(1)
        backtest_factor(args.factor, args.start, args.end, args.db, args.forward, args.quantiles)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
