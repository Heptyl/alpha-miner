"""回填 zt_pool / zb_pool / strong_pool — 最近 N 个交易日。

用法: python scripts/backfill_pools.py [--days 60]
"""

import sqlite3
import sys
import time
from datetime import datetime, timedelta

import akshare as ak
import pandas as pd

DB_PATH = "data/alpha_miner.db"
RETRY = 3
DELAY = 1.5  # 每天间隔秒数


def get_trade_dates(n_days=60):
    """从 akshare 获取最近 N 个交易日。"""
    for attempt in range(3):
        try:
            # 用上证指数获取交易日历
            df = ak.stock_zh_index_daily(symbol="sh000001")
            dates = sorted(df["date"].astype(str).tolist(), reverse=True)
            # 取最近 n_days 个交易日
            return dates[:n_days]
        except Exception as e:
            print(f"[WARN] 获取交易日历失败({attempt+1}/3): {e}")
            time.sleep(3)
    return []


def safe_col(df, col, default=0):
    if col not in df.columns:
        return default
    val = df[col]
    if isinstance(val, pd.Series):
        return val
    return pd.Series([val] * len(df))


def safe_numeric(df, col, default=0.0):
    s = safe_col(df, col, default)
    if isinstance(s, (int, float)):
        return pd.Series([s] * len(df), dtype=float)
    return pd.to_numeric(s, errors="coerce").fillna(default)


def safe_str(df, col, default=""):
    s = safe_col(df, col, default)
    if isinstance(s, str):
        return pd.Series([s] * len(df), dtype=str)
    return s.astype(str).fillna(default)


def fetch_zt(trade_date):
    try:
        df = ak.stock_zt_pool_em(date=trade_date.replace("-", ""))
        if df is None or df.empty:
            return pd.DataFrame()
        return pd.DataFrame({
            "stock_code": df["代码"].values,
            "name": safe_str(df, "名称", "").values,
            "trade_date": trade_date,
            "consecutive_zt": safe_numeric(df, "连板数", 1).astype(int).values,
            "amount": safe_numeric(df, "成交额", 0).values,
            "industry": safe_str(df, "所属行业", "").values,
            "circulation_mv": safe_numeric(df, "流通市值", 0).values,
            "open_count": safe_numeric(df, "炸板次数", 0).astype(int).values,
            "zt_stats": safe_str(df, "涨停统计", "").values,
        })
    except Exception as e:
        print(f"  zt_pool {trade_date} 失败: {e}")
        return pd.DataFrame()


def fetch_zb(trade_date):
    try:
        df = ak.stock_zt_pool_zbgc_em(date=trade_date.replace("-", ""))
        if df is None or df.empty:
            return pd.DataFrame()
        return pd.DataFrame({
            "stock_code": df["代码"].values,
            "trade_date": trade_date,
            "amount": safe_numeric(df, "成交额", 0).values,
            "open_count": safe_numeric(df, "炸板次数", 0).astype(int).values,
        })
    except Exception as e:
        print(f"  zb_pool {trade_date} 失败: {e}")
        return pd.DataFrame()


def fetch_strong(trade_date):
    try:
        df = ak.stock_zt_pool_strong_em(date=trade_date.replace("-", ""))
        if df is None or df.empty:
            return pd.DataFrame()
        return pd.DataFrame({
            "stock_code": df["代码"].values,
            "name": safe_str(df, "名称", "").values,
            "trade_date": trade_date,
            "amount": safe_numeric(df, "成交额", 0).values,
            "reason": safe_str(df, "入选理由", "").values,
            "industry": safe_str(df, "所属行业", "").values,
        })
    except Exception as e:
        print(f"  strong_pool {trade_date} 失败: {e}")
        return pd.DataFrame()


def main():
    n_days = int(sys.argv[1]) if len(sys.argv) > 1 else 60
    print(f"回填最近 {n_days} 个交易日的 zt_pool / zb_pool / strong_pool")

    dates = get_trade_dates(n_days)
    if not dates:
        print("[ERROR] 无法获取交易日历")
        return

    print(f"共 {len(dates)} 个交易日: {dates[-1]} ~ {dates[0]}")

    # 已有哪些天的数据
    conn = sqlite3.connect(DB_PATH)
    existing = {}
    for table in ["zt_pool", "zb_pool", "strong_pool"]:
        rows = conn.execute(f"SELECT DISTINCT trade_date FROM {table}").fetchall()
        existing[table] = set(r[0] for r in rows)
        print(f"  {table}: 已有 {len(existing[table])} 天")

    stats = {"zt": {"ok": 0, "fail": 0, "rows": 0},
             "zb": {"ok": 0, "fail": 0, "rows": 0},
             "strong": {"ok": 0, "fail": 0, "rows": 0}}

    for i, date in enumerate(dates):
        # zt_pool
        if date not in existing["zt_pool"]:
            df = fetch_zt(date)
            if not df.empty:
                df.to_sql("zt_pool", conn, if_exists="append", index=False)
                stats["zt"]["ok"] += 1
                stats["zt"]["rows"] += len(df)
            else:
                stats["zt"]["fail"] += 1
            time.sleep(DELAY)

        # zb_pool
        if date not in existing["zb_pool"]:
            df = fetch_zb(date)
            if not df.empty:
                df.to_sql("zb_pool", conn, if_exists="append", index=False)
                stats["zb"]["ok"] += 1
                stats["zb"]["rows"] += len(df)
            else:
                stats["zb"]["fail"] += 1
            time.sleep(DELAY)

        # strong_pool
        if date not in existing["strong_pool"]:
            df = fetch_strong(date)
            if not df.empty:
                df.to_sql("strong_pool", conn, if_exists="append", index=False)
                stats["strong"]["ok"] += 1
                stats["strong"]["rows"] += len(df)
            else:
                stats["strong"]["fail"] += 1
            time.sleep(DELAY)

        if (i + 1) % 10 == 0:
            print(f"  [{i+1}/{len(dates)}] zt:ok={stats['zt']['ok']} fail={stats['zt']['fail']} rows={stats['zt']['rows']} | "
                  f"zb:ok={stats['zb']['ok']} fail={stats['zb']['fail']} rows={stats['zb']['rows']} | "
                  f"strong:ok={stats['strong']['ok']} fail={stats['strong']['fail']} rows={stats['strong']['rows']}")

    conn.commit()
    conn.close()

    print(f"\n完成:")
    for name in ["zt", "zb", "strong"]:
        s = stats[name]
        print(f"  {name}: ok={s['ok']}, fail={s['fail']}, rows={s['rows']}")


if __name__ == "__main__":
    main()
