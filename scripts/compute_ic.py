"""计算因子 IC 并写入 ic_series 表。

用法: .venv/bin/python scripts/compute_ic.py
"""

import sqlite3
import sys
from datetime import datetime

# 确保可以 import src
sys.path.insert(0, ".")

from src.data.storage import Storage
from src.drift.ic_tracker import ICTracker


def main():
    db = Storage("data/alpha_miner.db")
    db.init_db()

    # 获取 factor_values 的日期范围
    conn = db._get_conn()
    r = conn.execute(
        "SELECT MIN(trade_date), MAX(trade_date) FROM factor_values"
    ).fetchone()
    conn.close()

    start_date, end_date = r
    print(f"因子数据范围: {start_date} ~ {end_date}")

    # 获取所有因子名
    conn = db._get_conn()
    factors = [r[0] for r in conn.execute(
        "SELECT DISTINCT factor_name FROM factor_values"
    ).fetchall()]
    conn.close()
    print(f"因子列表: {factors}")

    tracker = ICTracker(db)

    for factor_name in factors:
        print(f"\n计算 {factor_name} ...")
        try:
            ic_df = tracker.compute_ic_series(
                factor_name,
                start_date,
                end_date,
                forward_days=1,
                window=5,
                persist=True,
            )
            if ic_df.empty:
                print(f"  无数据")
                continue

            # 打印结果
            for _, row in ic_df.iterrows():
                ic_val = row.get("ic", float("nan"))
                ic_ma = row.get("ic_ma", float("nan"))
                print(f"  {row['date']}: IC={ic_val:.4f}  IC_MA={ic_ma:.4f}")

            # 当前状态
            status = tracker.current_status(factor_name, window=5)
            print(f"  状态: {status}")
        except Exception as e:
            print(f"  错误: {e}")
            import traceback
            traceback.print_exc()

    # 最终统计
    conn = db._get_conn()
    r = conn.execute("SELECT COUNT(*) FROM ic_series").fetchone()
    conn.close()
    print(f"\nic_series 总计: {r[0]} 行")

    db.close()


if __name__ == "__main__":
    main()
