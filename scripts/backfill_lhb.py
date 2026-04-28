"""回填龙虎榜 lhb_detail — 最近 N 个交易日。

用法: python scripts/backfill_lhb.py [--days 60]
"""

import sqlite3
import sys
import time

import akshare as ak
import pandas as pd

DB_PATH = "data/alpha_miner.db"


def get_trade_dates(n_days=60):
    for attempt in range(3):
        try:
            df = ak.stock_zh_index_daily(symbol="sh000001")
            dates = sorted(df["date"].astype(str).tolist(), reverse=True)
            return dates[:n_days]
        except Exception as e:
            print(f"[WARN] 获取交易日历失败({attempt+1}/3): {e}")
            time.sleep(3)
    return []


def fetch_lhb(trade_date):
    date_str = trade_date.replace("-", "")
    for attempt in range(3):
        try:
            df = ak.stock_lhb_detail_em(start_date=date_str, end_date=date_str)
            if df is None or df.empty:
                return pd.DataFrame()

            result = pd.DataFrame()
            result["stock_code"] = df["代码"].values if "代码" in df.columns else []

            for col_srcs, col_dst in [
                (["龙虎榜买入额", "买入额"], "buy_amount"),
                (["龙虎榜卖出额", "卖出额"], "sell_amount"),
                (["龙虎榜净买额", "净买入额"], "net_amount"),
            ]:
                val = 0.0
                for cs in col_srcs:
                    if cs in df.columns:
                        val = pd.to_numeric(df[cs], errors="coerce").fillna(0).values
                        break
                result[col_dst] = val

            result["trade_date"] = trade_date
            result["reason"] = df["上榜原因"].astype(str).fillna("").values if "上榜原因" in df.columns else ""
            result["buy_depart"] = ""
            result["sell_depart"] = ""
            result["_row_idx"] = range(len(result))
            return result
        except Exception as e:
            if "NoneType" in str(e):
                return pd.DataFrame()
            if attempt < 2:
                time.sleep(2)
            else:
                print(f"  lhb {trade_date} 失败: {e}")
                return pd.DataFrame()


def main():
    n_days = int(sys.argv[1]) if len(sys.argv) > 1 else 60
    print(f"回填最近 {n_days} 个交易日的龙虎榜")

    dates = get_trade_dates(n_days)
    if not dates:
        print("[ERROR] 无法获取交易日历")
        return

    print(f"共 {len(dates)} 个交易日: {dates[-1]} ~ {dates[0]}")

    conn = sqlite3.connect(DB_PATH)
    existing = set(r[0] for r in conn.execute("SELECT DISTINCT trade_date FROM lhb_detail").fetchall())
    print(f"已有 {len(existing)} 天")

    ok, fail, total_rows = 0, 0, 0
    for i, date in enumerate(dates):
        if date in existing:
            continue
        df = fetch_lhb(date)
        if not df.empty:
            df.to_sql("lhb_detail", conn, if_exists="append", index=False)
            ok += 1
            total_rows += len(df)
        else:
            fail += 1
        time.sleep(1.5)

        if (i + 1) % 10 == 0:
            print(f"  [{i+1}/{len(dates)}] ok={ok} fail={fail} rows={total_rows}")

    conn.commit()
    conn.close()
    print(f"\n完成: ok={ok}, fail={fail}, rows={total_rows}")


if __name__ == "__main__":
    main()
