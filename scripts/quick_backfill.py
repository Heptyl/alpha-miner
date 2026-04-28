#!/usr/bin/env python3
"""快速回填 daily_price — 用线程池+腾讯接口。"""

import json
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import akshare as ak

DB_PATH = "data/alpha_miner.db"
DATES = ["20260427", "20260428"]
BATCH_SIZE = 20
MAX_WORKERS = 4
DELAY = 0.3  # 每次请求间隔


def fetch_one(code: str) -> list[tuple]:
    """采集单只股票的数据。"""
    try:
        df = ak.stock_zh_a_hist(
            symbol=code,
            period="daily",
            start_date="20260427",
            end_date="20260428",
            adjust="qfq",
        )
        if df is None or df.empty:
            return []
        rows = []
        for _, r in df.iterrows():
            td = str(r["日期"]).replace("-", "")
            rows.append((
                td, code,
                float(r["开盘"]), float(r["收盘"]),
                float(r["最高"]), float(r["最低"]),
                float(r["成交量"]), float(r["成交额"]),
                float(r["换手率"]), float(r["涨跌幅"]),
            ))
        return rows
    except Exception:
        return []


def main():
    conn = sqlite3.connect(DB_PATH)
    
    # 检查已有数据
    for d in DATES:
        cnt = conn.execute("SELECT COUNT(*) FROM daily_price WHERE trade_date=?", (d,)).fetchone()[0]
        print(f"  {d}: 已有 {cnt} 条")

    with open("/tmp/stock_codes.json") as f:
        codes = json.load(f)
    print(f"共 {len(codes)} 只股票待处理")

    success = 0
    errors = 0
    inserted = 0
    t0 = time.time()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {}
        for code in codes:
            f = pool.submit(fetch_one, code)
            futures[f] = code

        for i, f in enumerate(as_completed(futures)):
            code = futures[f]
            try:
                rows = f.result()
                if rows:
                    for row in rows:
                        exists = conn.execute(
                            "SELECT COUNT(*) FROM daily_price WHERE trade_date=? AND stock_code=?",
                            (row[0], row[1]),
                        ).fetchone()[0]
                        if exists == 0:
                            conn.execute(
                                "INSERT INTO daily_price "
                                "(trade_date,stock_code,open,close,high,low,volume,amount,turnover,pct_change) "
                                "VALUES (?,?,?,?,?,?,?,?,?,?)",
                                row,
                            )
                            inserted += 1
                    success += 1
                else:
                    errors += 1
            except Exception:
                errors += 1

            if (i + 1) % 200 == 0:
                conn.commit()
                elapsed = time.time() - t0
                pct = (i + 1) / len(codes) * 100
                print(f"  [{pct:.0f}%] {i+1}/{len(codes)} 成功:{success} 插入:{inserted} 耗时:{elapsed:.0f}s")

    conn.commit()

    # 验证
    for d in DATES:
        cnt = conn.execute("SELECT COUNT(*) FROM daily_price WHERE trade_date=?", (d,)).fetchone()[0]
        print(f"  {d}: {cnt} 条")

    conn.close()
    elapsed = time.time() - t0
    print(f"完成! 成功:{success} 失败:{errors} 插入:{inserted} 耗时:{elapsed:.0f}s")


if __name__ == "__main__":
    main()
