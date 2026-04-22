"""批量计算所有因子并写入 factor_values 表。

用法:
  uv run python scripts/compute_factors.py --date 2026-04-18
  uv run python scripts/compute_factors.py --all   # 所有交易日
"""
import argparse
import sqlite3
from datetime import datetime

import pandas as pd
from src.data.storage import Storage
from src.factors.registry import FactorRegistry


def get_trade_dates(db_path: str) -> list[str]:
    """从 zt_pool 获取所有交易日。"""
    conn = sqlite3.connect(db_path)
    dates = conn.execute(
        "SELECT DISTINCT trade_date FROM zt_pool ORDER BY trade_date"
    ).fetchall()
    conn.close()
    return [r[0] for r in dates]


def compute_and_save(date_str: str, db_path: str = "data/alpha_miner.db"):
    """计算指定日期所有因子值并写入 factor_values 表。"""
    db = Storage(db_path)
    reg = FactorRegistry()

    # as_of 用于两个目的：
    #   1. db.query() 中过滤 snapshot_time < as_of（需要 >= 当天采集时间）
    #   2. 因子内部 as_of.strftime() 取目标 trade_date（需要等于目标日期）
    # 所以用当天 23:59:59，既覆盖当天采集数据，又让 strftime 返回正确日期。
    from datetime import timedelta
    as_of = datetime.strptime(date_str, "%Y-%m-%d")
    query_as_of = as_of.replace(hour=23, minute=59, second=59)

    # 获取 universe（当日涨停池的股票）
    zt_df = db.query("zt_pool", query_as_of, where="trade_date = ?", params=(date_str,))
    if zt_df.empty:
        print(f"  {date_str}: zt_pool 无数据，跳过")
        return 0

    # 去重
    if "snapshot_time" in zt_df.columns:
        zt_df = zt_df.sort_values("snapshot_time").groupby("stock_code").last().reset_index()
    universe = zt_df["stock_code"].unique().tolist()

    # 也加入强势股
    strong_df = db.query("strong_pool", query_as_of, where="trade_date = ?", params=(date_str,))
    if not strong_df.empty:
        if "snapshot_time" in strong_df.columns:
            strong_df = strong_df.sort_values("snapshot_time").groupby("stock_code").last().reset_index()
        universe = list(set(universe + strong_df["stock_code"].unique().tolist()))

    print(f"  {date_str}: universe={len(universe)} 只")

    total_rows = 0
    snapshot_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    conn = sqlite3.connect(db_path)
    for name in reg.list_factors():
        factor = reg.get_factor(name)
        try:
            values = factor.compute(universe, query_as_of, db)
        except Exception as e:
            print(f"    {name}: 计算失败 - {e}")
            continue

        if values.empty:
            print(f"    {name}: 无数据")
            continue

        # 写入 factor_values
        rows = []
        for code, val in values.items():
            if pd.isna(val):
                continue
            rows.append((date_str, code, name, float(val), snapshot_time))

        if rows:
            # 去重：先删除当天同一因子的旧记录
            conn.execute(
                "DELETE FROM factor_values WHERE factor_name = ? AND trade_date = ?",
                (name, date_str),
            )
            conn.executemany(
                "INSERT INTO factor_values (trade_date, stock_code, factor_name, factor_value, snapshot_time) "
                "VALUES (?, ?, ?, ?, ?)",
                rows,
            )
            total_rows += len(rows)
            print(f"    {name}: {len(rows)} 条")

    conn.commit()
    conn.close()
    return total_rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, help="指定日期 YYYY-MM-DD")
    parser.add_argument("--all", action="store_true", help="所有交易日")
    parser.add_argument("--db", type=str, default="data/alpha_miner.db")
    args = parser.parse_args()

    db_path = args.db
    db = Storage(db_path)
    db.init_db()

    if args.all:
        dates = get_trade_dates(db_path)
        print(f"共 {len(dates)} 个交易日")
        total = 0
        for d in dates:
            n = compute_and_save(d, db_path)
            total += n
        print(f"\n总计写入 {total} 条因子值")
    elif args.date:
        n = compute_and_save(args.date, db_path)
        print(f"\n写入 {n} 条因子值")
    else:
        print("请指定 --date 或 --all")


if __name__ == "__main__":
    main()
