"""重新计算指定日期的因子值。"""
import warnings
warnings.filterwarnings("ignore")

from datetime import datetime
import numpy as np
import pandas as pd
from src.data.storage import Storage
from src.factors.registry import FactorRegistry

DB = "data/alpha_miner.db"

def recompute(date_str: str):
    db = Storage(DB)
    as_of = datetime.strptime(date_str, "%Y-%m-%d")

    # 清理旧数据
    conn = db._get_conn()
    conn.execute("DELETE FROM factor_values WHERE trade_date = ?", (date_str,))
    conn.commit()
    conn.close()

    # focused universe (bypass_snapshot: 数据可能是后来补采的)
    db.backtest_mode = True
    codes = set()
    for table in ("zt_pool", "strong_pool", "lhb_detail"):
        try:
            df = db.query(table, as_of, where="trade_date = ?", params=(date_str,))
            if not df.empty and "stock_code" in df.columns:
                codes.update(df["stock_code"].unique().tolist())
        except Exception:
            pass
    universe = sorted(codes)
    print(f"Universe: {len(universe)} 只, date={date_str}")

    registry = FactorRegistry()
    factor_names = registry.list_factors()
    print(f"因子: {factor_names}")

    total = 0
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
                df_out = pd.DataFrame(rows)
                db.insert("factor_values", df_out, dedup=True)
                total += len(rows)
                print(f"  {name}: {len(rows)} 条")
            else:
                print(f"  {name}: 全NaN")
        except Exception as e:
            print(f"  {name}: ERROR - {e}")

    print(f"\nTotal: {total} 条")
    return total


if __name__ == "__main__":
    import sys
    date = sys.argv[1] if len(sys.argv) > 1 else "2026-05-15"
    recompute(date)
