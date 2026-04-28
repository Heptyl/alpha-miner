"""回填 daily_price — 从所有数据源汇总股票列表，拉取60天日K线。

策略:
1. 从 zt_pool / strong_pool / lhb_detail / fund_flow 汇总所有股票代码
2. 加上已有的 daily_price 中的股票代码
3. 逐只拉取 stock_zh_a_hist (东财源)，回退新浪
4. 只保留最近60个交易日

用法: python scripts/backfill_daily_price.py [--days 60]
"""

import sqlite3
import sys
import time

import akshare as ak
import pandas as pd

DB_PATH = "data/alpha_miner.db"


def get_all_codes():
    """从所有表汇总股票代码。"""
    conn = sqlite3.connect(DB_PATH)
    codes = set()
    for table in ["daily_price", "zt_pool", "zb_pool", "strong_pool", "lhb_detail", "fund_flow"]:
        try:
            rows = conn.execute(f"SELECT DISTINCT stock_code FROM {table}").fetchall()
            for r in rows:
                code = str(r[0]).strip()
                if code and len(code) == 6 and code.isdigit():
                    codes.add(code)
        except Exception:
            pass
    conn.close()
    return sorted(codes)


def get_existing():
    """已有的 stock_code + trade_date 组合。"""
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute("SELECT DISTINCT stock_code, trade_date FROM daily_price").fetchall()
    conn.close()
    existing = set()
    for code, date in rows:
        existing.add((code, date))
    return existing


def fetch_hist(code, n_days=60):
    """拉取单只股票最近N天日K线（东财源 stock_zh_a_hist）。"""
    # 判断市场前缀
    if code.startswith("6"):
        symbol = f"{code}"
        market = "sh"  # 沪市
    else:
        symbol = f"{code}"
        market = "sz"  # 深市/创业板

    # 东财源
    for attempt in range(2):
        try:
            df = ak.stock_zh_a_hist(
                symbol=symbol,
                period="daily",
                adjust="qfq",
            )
            if df is not None and not df.empty:
                # 只取最近 n_days 天
                df = df.tail(n_days)
                result = pd.DataFrame({
                    "stock_code": code,
                    "trade_date": pd.to_datetime(df["日期"]).dt.strftime("%Y-%m-%d").values,
                    "open": pd.to_numeric(df["开盘"], errors="coerce").values,
                    "high": pd.to_numeric(df["最高"], errors="coerce").values,
                    "low": pd.to_numeric(df["最低"], errors="coerce").values,
                    "close": pd.to_numeric(df["收盘"], errors="coerce").values,
                    "volume": pd.to_numeric(df["成交量"], errors="coerce").values,
                    "amount": pd.to_numeric(df["成交额"], errors="coerce").values,
                    "turnover": pd.to_numeric(df.get("换手率", 0), errors="coerce").fillna(0).values,
                })
                # 添加 pre_close
                if "昨收" in df.columns:
                    result["pre_close"] = pd.to_numeric(df["昨收"], errors="coerce").values
                else:
                    result["pre_close"] = result["close"].shift(1).values
                return result
        except Exception:
            if attempt == 0:
                time.sleep(1)

    # 新浪回退
    try:
        df = ak.stock_zh_a_daily(symbol=f"{market}{code}", adjust="qfq")
        if df is not None and not df.empty:
            df = df.tail(n_days)
            result = pd.DataFrame({
                "stock_code": code,
                "trade_date": pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d").values,
                "open": pd.to_numeric(df["open"], errors="coerce").values,
                "high": pd.to_numeric(df["high"], errors="coerce").values,
                "low": pd.to_numeric(df["low"], errors="coerce").values,
                "close": pd.to_numeric(df["close"], errors="coerce").values,
                "volume": pd.to_numeric(df["volume"], errors="coerce").values,
                "amount": 0.0,
                "turnover": 0.0,
                "pre_close": result["close"].shift(1).values if "close" in df.columns else 0.0,
            })
            return result
    except Exception:
        pass

    return pd.DataFrame()


def main():
    n_days = int(sys.argv[1]) if len(sys.argv) > 1 else 60
    print(f"回填 daily_price — 最近 {n_days} 个交易日")

    codes = get_all_codes()
    print(f"共 {len(codes)} 只股票需要拉取")

    existing = get_existing()
    print(f"已有 {len(existing)} 条 (code,date) 记录")

    conn = sqlite3.connect(DB_PATH)
    ok, fail, new_rows = 0, 0, 0
    snap_time = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")

    for i, code in enumerate(codes):
        df = fetch_hist(code, n_days)
        if df.empty:
            fail += 1
        else:
            # 过滤已存在的
            df["_key"] = df["stock_code"] + "_" + df["trade_date"]
            existing_keys = {f"{c}_{d}" for c, d in existing}
            new_df = df[~df["_key"].isin(existing_keys)].drop(columns=["_key"])

            if not new_df.empty:
                new_df["snapshot_time"] = snap_time
                new_df.to_sql("daily_price", conn, if_exists="append", index=False)
                new_rows += len(new_df)
                # 更新 existing
                for _, row in new_df.iterrows():
                    existing.add((row["stock_code"], row["trade_date"]))
            ok += 1

        if (i + 1) % 50 == 0:
            elapsed = time.time() - start_time
            eta = elapsed / (i + 1) * (len(codes) - i - 1)
            print(f"  [{i+1}/{len(codes)}] ok={ok} fail={fail} new_rows={new_rows} elapsed={int(elapsed)}s eta={int(eta)}s")

        time.sleep(0.3)  # 限速

    conn.commit()
    conn.close()

    elapsed = time.time() - start_time
    print(f"\n完成: ok={ok}, fail={fail}, new_rows={new_rows}, {int(elapsed)}s")

    # 验证
    conn = sqlite3.connect(DB_PATH)
    r = conn.execute("SELECT count(*), count(DISTINCT trade_date), count(DISTINCT stock_code) FROM daily_price").fetchone()
    print(f"  daily_price: {r[0]} rows, {r[1]} days, {r[2]} stocks")
    conn.close()


if __name__ == "__main__":
    start_time = time.time()
    main()
