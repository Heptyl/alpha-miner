"""纯腾讯源回填 — 绕开东财 IP 封锁。

用 web.ifzq.gtimg.cn 的历史K线接口，逐只拉取日K线。
不依赖 akshare / 东财，所以东财被封也不影响。

用法: PYTHONUNBUFFERED=1 .venv/bin/python scripts/backfill_tencent.py [--days 30]
"""

import json
import sqlite3
import sys
import time
from datetime import datetime, timedelta

import requests

DB_PATH = "data/alpha_miner.db"
BATCH_DELAY = 0.15  # 每只股票间隔


def get_conn():
    return sqlite3.connect(DB_PATH)


def get_all_codes(conn):
    """从所有表汇总股票代码。"""
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
    return sorted(codes)


def code_to_tencent(code):
    """股票代码转腾讯格式。"""
    if code.startswith("6"):
        return f"sh{code}"
    elif code.startswith("0") or code.startswith("3"):
        return f"sz{code}"
    else:
        return f"bj{code}"


def fetch_tencent_hist(code, start_date, end_date):
    """从腾讯接口拉取单只股票的历史K线。

    Returns: list of dicts or empty list
    """
    tc_code = code_to_tencent(code)
    start_str = start_date.replace("-", "")
    end_str = end_date.replace("-", "")
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
            # [date, open, close, high, low, volume]
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


def backfill_daily_price(conn, codes, start_date, end_date):
    """回填 daily_price。"""
    print(f"\n[1] daily_price: {len(codes)} 只股票, {start_date} ~ {end_date}")

    # 已有记录
    existing = set()
    rows = conn.execute(
        "SELECT stock_code, trade_date FROM daily_price"
    ).fetchall()
    for code, date in rows:
        existing.add((code, date))
    print(f"  已有 {len(existing)} 条")

    ok, fail, new_rows = 0, 0, 0
    start = time.time()

    for i, code in enumerate(codes):
        batch = fetch_tencent_hist(code, start_date, end_date)
        if batch:
            insert_batch = []
            for row in batch:
                key = (row["stock_code"], row["trade_date"])
                if key not in existing:
                    insert_batch.append(row)
                    existing.add(key)

            if insert_batch:
                import pandas as pd
                pd.DataFrame(insert_batch).to_sql(
                    "daily_price", conn, if_exists="append", index=False
                )
                new_rows += len(insert_batch)
            ok += 1
        else:
            fail += 1

        if (i + 1) % 100 == 0:
            elapsed = time.time() - start
            eta = elapsed / (i + 1) * (len(codes) - i - 1)
            print(f"  [{i+1}/{len(codes)}] ok={ok} fail={fail} new={new_rows} eta={int(eta)}s")

        time.sleep(BATCH_DELAY)

    conn.commit()
    elapsed = time.time() - start
    print(f"  完成: ok={ok} fail={fail} new={new_rows} ({int(elapsed)}s)")


def get_trade_dates_from_daily(conn):
    """从 zt_pool / strong_pool 已有日期推算交易日历。"""
    dates = set()
    for table in ["zt_pool", "zb_pool", "strong_pool", "lhb_detail"]:
        try:
            rows = conn.execute(
                f"SELECT DISTINCT trade_date FROM {table}"
            ).fetchall()
            for r in rows:
                dates.add(r[0])
        except Exception:
            pass
    return sorted(dates)


def compute_missing_dates(existing_dates, days_back=30):
    """计算缺失的交易日（简单工作日估算）。"""
    end = datetime.now()
    start = end - timedelta(days=days_back)
    all_dates = []
    current = start
    while current <= end:
        if current.weekday() < 5:
            all_dates.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)

    # 排除已知有大量数据的日期
    missing = [d for d in all_dates if d not in existing_dates]
    return missing


def main():
    days_back = int(sys.argv[1]) if len(sys.argv) > 1 else 30
    print(f"=== 腾讯源回填 === 目标: 最近 {days_back} 天")

    conn = get_conn()
    codes = get_all_codes(conn)
    print(f"股票代码: {len(codes)} 只")

    if not codes:
        print("[ERROR] 无股票代码，请先跑一次 collect --today")
        conn.close()
        return

    # 计算日期范围
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")

    print(f"日期范围: {start_date} ~ {end_date}")

    # Step 1: daily_price
    backfill_daily_price(conn, codes, start_date, end_date)

    # 汇总
    print("\n=== 最终统计 ===")
    for t in ["daily_price", "zt_pool", "zb_pool", "strong_pool", "fund_flow", "lhb_detail"]:
        try:
            r = conn.execute(
                f"SELECT count(*), count(DISTINCT trade_date), min(trade_date), max(trade_date) FROM {t}"
            ).fetchone()
            print(f"  {t:20s}: {r[0]:>6} rows, {r[1]:>3} days, {r[2]} ~ {r[3]}")
        except Exception:
            pass

    conn.close()
    print("\n完成!")


if __name__ == "__main__":
    main()
