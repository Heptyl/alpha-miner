"""用Windows curl补齐指定日期缺失的daily_price K线。
用法: uv run python scripts/patch_missing_kline.py 2026-05-06 2026-05-07
"""
import json
import logging
import sqlite3
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger(__name__)

DB_PATH = "data/alpha_miner.db"
CURL = "/mnt/c/Windows/System32/curl.exe"

def _prefix(code: str) -> str:
    if code.startswith("6"):
        return "sh"
    if code.startswith(("0", "3")):
        return "sz"
    if code.startswith(("4", "8")):
        return "bj"
    return "sz"

def fetch_kline_curl(code: str, date: str) -> dict | None:
    tc = f"{_prefix(code)}{code}"
    url = (
        f"https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?"
        f"param={tc},day,{date},{date},1,qfq"
    )
    try:
        proc = subprocess.run(
            [CURL, "-sS", "--connect-timeout", "5", "--max-time", "10", url],
            capture_output=True, timeout=15
        )
        data = json.loads(proc.stdout)
        stock_data = data.get("data", {}).get(tc, {})
        kline = stock_data.get("qfqday") or stock_data.get("day")
        if kline and len(kline) > 0:
            row = kline[0]
            return {
                "stock_code": code,
                "trade_date": date,
                "open": float(row[1]),
                "close": float(row[2]),
                "high": float(row[3]),
                "low": float(row[4]),
                "volume": float(row[5]),
                "amount": 0.0,
                "turnover_rate": 0.0,
            }
    except Exception:
        pass
    return None

def get_missing_codes(date: str) -> list[str]:
    conn = sqlite3.connect(DB_PATH)
    best = conn.execute(
        "SELECT trade_date FROM daily_price GROUP BY trade_date "
        "ORDER BY COUNT(*) DESC LIMIT 1"
    ).fetchone()[0]
    all_codes = set(
        r[0] for r in conn.execute(
            "SELECT DISTINCT stock_code FROM daily_price WHERE trade_date = ?", (best,)
        ).fetchall()
    )
    existing = set(
        r[0] for r in conn.execute(
            "SELECT DISTINCT stock_code FROM daily_price WHERE trade_date = ?", (date,)
        ).fetchall()
    )
    conn.close()
    return sorted(all_codes - existing)

def backfill_date(date: str, max_workers: int = 8):
    missing = get_missing_codes(date)
    if not missing:
        logger.info("[%s] 已完整", date)
        return 0
    logger.info("[%s] 缺 %d 只，开始补齐 (workers=%d)", date, len(missing), max_workers)
    results = []
    failed = 0

    def _fetch(code):
        time.sleep(0.05)
        return fetch_kline_curl(code, date)

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(_fetch, c): c for c in missing}
        done = 0
        for f in as_completed(futures):
            done += 1
            row = f.result()
            if row:
                results.append(row)
            else:
                failed += 1
            if done % 200 == 0:
                logger.info("[%s] 进度 %d/%d (成功%d 失败%d)",
                            date, done, len(missing), len(results), failed)

    if results:
        conn = sqlite3.connect(DB_PATH)
        previous_close = {
            code: close
            for code, close in conn.execute(
                """SELECT stock_code, close
                   FROM daily_price
                   WHERE trade_date = (
                       SELECT MAX(trade_date) FROM daily_price WHERE trade_date < ?
                   )""",
                (date,),
            ).fetchall()
        }
        snapshot_time = time.strftime("%Y-%m-%d %H:%M:%S")
        for r in results:
            try:
                conn.execute(
                    """INSERT OR IGNORE INTO daily_price
                    (stock_code, trade_date, open, high, low, close, pre_close,
                     volume, amount, turnover_rate, snapshot_time)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (r["stock_code"], r["trade_date"], r["open"], r["high"], r["low"],
                     r["close"], previous_close.get(r["stock_code"], 0),
                     r["volume"], r["amount"], r["turnover_rate"], snapshot_time),
                )
            except Exception:
                pass
        conn.execute(
            """UPDATE daily_price
               SET pre_close = (
                   SELECT prev.close
                   FROM daily_price AS prev
                   WHERE prev.stock_code = daily_price.stock_code
                     AND prev.trade_date = (
                         SELECT MAX(p2.trade_date)
                         FROM daily_price AS p2
                         WHERE p2.stock_code = daily_price.stock_code
                           AND p2.trade_date < daily_price.trade_date
                     )
                   LIMIT 1
               )
               WHERE trade_date = ? AND COALESCE(pre_close, 0) <= 0""",
            (date,),
        )
        conn.commit()
        conn.close()

    logger.info("[%s] 完成: 补了 %d, 失败 %d", date, len(results), failed)
    return len(results)

if __name__ == "__main__":
    dates = sys.argv[1:] if len(sys.argv) > 1 else ["2026-05-06", "2026-05-07"]
    total = 0
    for d in dates:
        total += backfill_date(d, max_workers=24)
    print(f"\n总计补了 {total} 条")
