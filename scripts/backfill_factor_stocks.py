"""针对 factor_values 中的股票，批量回填 30 天 K 线。

用腾讯历史K线接口 (web.ifzq.gtimg.cn)。

用法: PYTHONUNBUFFERED=1 .venv/bin/python scripts/backfill_factor_stocks.py
"""

import json
import sqlite3
import sys
import time
from datetime import datetime, timedelta

import pandas as pd
import requests

DB_PATH = "data/alpha_miner.db"


def code_to_tencent(code):
    if code.startswith("6"):
        return f"sh{code}"
    elif code.startswith("0") or code.startswith("3"):
        return f"sz{code}"
    else:
        return f"bj{code}"


def fetch_tencent_hist(code, start_date, end_date):
    tc_code = code_to_tencent(code)
    url = (
        f"https://web.ifzq.gtimg.cn/appstock/app/fqkline/get"
        f"?param={tc_code},day,{start_date},{end_date},500,qfq"
    )
    try:
        r = requests.get(url, timeout=10)
        data = json.loads(r.text)
        if data.get("code") != 0:
            return []
        stock_data = data.get("data", {}).get(tc_code, {})
        klines = stock_data.get("qfqday") or stock_data.get("day") or []
        results = []
        for k in klines:
            if len(k) >= 6:
                results.append({
                    "stock_code": code,
                    "trade_date": k[0],
                    "open": float(k[1]),
                    "close": float(k[2]),
                    "high": float(k[3]),
                    "low": float(k[4]),
                    "volume": float(k[5]),
                    "pre_close": 0,
                    "amount": 0,
                    "turnover_rate": 0,
                    "snapshot_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                })
        return results
    except Exception:
        return []


def main():
    conn = sqlite3.connect(DB_PATH)

    # 获取 factor_values 涉及的股票
    codes = [r[0] for r in conn.execute(
        "SELECT DISTINCT stock_code FROM factor_values"
    ).fetchall()]
    print(f"factor_values 涉及 {len(codes)} 只股票")

    # 日期范围: 30天前到 04-24
    end_date = "2026-04-24"
    start_date = "2026-03-20"

    # 已有记录
    existing = set()
    rows = conn.execute("SELECT stock_code, trade_date FROM daily_price").fetchall()
    for code, date in rows:
        existing.add((code, date))
    print(f"已有 {len(existing)} 条 daily_price")

    ok, fail, new_rows = 0, 0, 0
    t0 = time.time()

    for i, code in enumerate(codes):
        batch = fetch_tencent_hist(code, start_date, end_date)
        if batch:
            insert = []
            for row in batch:
                key = (row["stock_code"], row["trade_date"])
                if key not in existing:
                    insert.append(row)
                    existing.add(key)
            if insert:
                pd.DataFrame(insert).to_sql("daily_price", conn, if_exists="append", index=False)
                new_rows += len(insert)
            ok += 1
        else:
            fail += 1

        if (i + 1) % 50 == 0:
            elapsed = time.time() - t0
            eta = elapsed / (i + 1) * (len(codes) - i - 1)
            print(f"  [{i+1}/{len(codes)}] ok={ok} fail={fail} new={new_rows} eta={int(eta)}s")
            sys.stdout.flush()

        time.sleep(0.15)

    conn.commit()

    elapsed = time.time() - t0
    print(f"\n完成: ok={ok} fail={fail} new={new_rows} ({int(elapsed)}s)")

    # 最终统计
    r = conn.execute("SELECT COUNT(*), COUNT(DISTINCT trade_date), COUNT(DISTINCT stock_code) FROM daily_price").fetchone()
    print(f"daily_price: {r[0]} rows, {r[1]} days, {r[2]} stocks")

    # 每天数据量
    r2 = conn.execute("SELECT trade_date, COUNT(DISTINCT stock_code) FROM daily_price GROUP BY trade_date ORDER BY trade_date").fetchall()
    print("\nper day:")
    for row in r2:
        bar = "#" * min(row[1] // 10, 60)
        print(f"  {row[0]}: {row[1]:>4} {bar}")

    conn.close()


if __name__ == "__main__":
    main()
