#!/usr/bin/env python3
"""用新浪接口回填 4-27/4-28 daily_price（仅候选池股票）。"""

import json
import re
import sqlite3
import time

import requests

DB = "data/alpha_miner.db"


def sina_code(code: str) -> str:
    return f"sh{code}" if code.startswith("6") else f"sz{code}"


def main():
    conn = sqlite3.connect(DB)

    # 候选池 = zt + strong + lhb 4-28 的股票
    codes = set()
    for table in ["zt_pool", "strong_pool", "lhb_detail"]:
        rows = conn.execute(
            f"SELECT DISTINCT stock_code FROM {table} WHERE trade_date='2026-04-28'"
        ).fetchall()
        codes.update(r[0] for r in rows)
    
    # 也加上 4-24 全部（技术分析需要历史）
    rows = conn.execute("SELECT DISTINCT stock_code FROM daily_price WHERE trade_date='2026-04-24'").fetchall()
    codes.update(r[0] for r in rows)

    codes = sorted(codes)
    print(f"共 {len(codes)} 只股票待采集")

    url = "https://quotes.sina.cn/cn/api/jsonp_v2.php/callback/CN_MarketDataService.getKLineData"
    headers = {
        "Referer": "https://finance.sina.com.cn",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120",
    }

    success = errors = inserted = 0

    for i, code in enumerate(codes):
        try:
            params = {"symbol": sina_code(code), "scale": 240, "ma": "no", "datalen": 5}
            r = requests.get(url, params=params, headers=headers, timeout=10)
            match = re.search(r"callback\((\[.*\])\)", r.text, re.DOTALL)
            if not match:
                errors += 1
                continue

            data = json.loads(match.group(1))
            for item in data:
                day = item["day"].replace("-", "")
                if day not in ("20260427", "20260428"):
                    continue
                exists = conn.execute(
                    "SELECT COUNT(*) FROM daily_price WHERE trade_date=? AND stock_code=?",
                    (day, code),
                ).fetchone()[0]
                if exists:
                    continue
                conn.execute(
                    "INSERT INTO daily_price "
                    "(trade_date,stock_code,open,close,high,low,volume,amount,turnover,pct_change) "
                    "VALUES (?,?,?,?,?,?,?,0,0,0)",
                    (day, code, float(item["open"]), float(item["close"]),
                     float(item["high"]), float(item["low"]), float(item["volume"])),
                )
                inserted += 1
            success += 1
        except Exception:
            errors += 1

        # 每 50 只 commit + 休眠
        if (i + 1) % 50 == 0:
            conn.commit()
            print(f"  [{i+1}/{len(codes)}] 成功:{success} 失败:{errors} 插入:{inserted}")
            time.sleep(1)
        elif (i + 1) % 10 == 0:
            time.sleep(0.3)

    conn.commit()

    for d in ["20260427", "20260428"]:
        cnt = conn.execute("SELECT COUNT(*) FROM daily_price WHERE trade_date=?", (d,)).fetchone()[0]
        print(f"  {d}: {cnt} 条")

    conn.close()
    print(f"完成! 成功:{success} 失败:{errors} 插入:{inserted}")


if __name__ == "__main__":
    main()
