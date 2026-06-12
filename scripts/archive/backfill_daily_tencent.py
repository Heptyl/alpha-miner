"""用腾讯历史K线接口批量补齐 daily_price。

腾讯接口 web.ifzq.gtimg.cn 稳定、不限流、支持批量。
格式: qfqday 数组 [date, open, close, high, low, volume]
"""

import json
import logging
import sqlite3
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger(__name__)

DB_PATH = "data/alpha_miner.db"

# 代码 → 腾讯前缀
def _prefix(code: str) -> str:
    if code.startswith("6"):
        return "sh"
    if code.startswith(("0", "3")):
        return "sz"
    if code.startswith(("4", "8")):
        return "bj"
    return "sz"


def fetch_kline(code: str, date: str) -> dict | None:
    """拉单只股票指定日期K线。"""
    tc = f"{_prefix(code)}{code}"
    url = (
        f"https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?"
        f"param={tc},day,{date},{date},1,qfq"
    )
    try:
        r = requests.get(url, timeout=10)
        data = r.json()
        # 数据在 data.{tc}.qfqday 或 data.{tc}.day
        stock_data = data.get("data", {}).get(tc, {})
        kline = stock_data.get("qfqday") or stock_data.get("day")
        if kline and len(kline) > 0:
            row = kline[0]  # [date, open, close, high, low, volume]
            return {
                "stock_code": code,
                "trade_date": date,
                "open": float(row[1]),
                "close": float(row[2]),
                "high": float(row[3]),
                "low": float(row[4]),
                "volume": float(row[5]),
                "amount": 0.0,  # 腾讯不直接给成交额
                "turnover_rate": 0.0,
            }
    except Exception:
        pass
    return None


def get_missing_codes(date: str) -> list[str]:
    """获取指定日期缺失的股票代码。"""
    conn = sqlite3.connect(DB_PATH)
    # 用最完整的日期作为全集
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


def backfill_date(date: str, max_workers: int = 10):
    """补齐一天。"""
    missing = get_missing_codes(date)
    if not missing:
        logger.info("[%s] 已完整", date)
        return 0

    logger.info("[%s] 缺 %d 只，开始补齐 (workers=%d)", date, len(missing), max_workers)
    results = []
    failed = 0

    def _fetch(code):
        time.sleep(0.02)  # 极小延迟，腾讯不限制但礼貌起见
        return fetch_kline(code, date)

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

    # 批量写入
    if results:
        conn = sqlite3.connect(DB_PATH)
        for r in results:
            try:
                conn.execute(
                    """INSERT OR IGNORE INTO daily_price
                    (stock_code, trade_date, open, high, low, close, volume, amount, turnover_rate, snapshot_time)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))""",
                    (r["stock_code"], r["trade_date"], r["open"], r["high"], r["low"],
                     r["close"], r["volume"], r["amount"], r["turnover_rate"]),
                )
            except Exception:
                pass
        conn.commit()
        conn.close()

    logger.info("[%s] 完成: 补了 %d, 失败 %d", date, len(results), failed)
    return len(results)


if __name__ == "__main__":
    dates = sys.argv[1:] if len(sys.argv) > 1 else ["2026-04-28", "2026-04-29", "2026-04-30"]
    total = 0
    for d in dates:
        total += backfill_date(d, max_workers=15)
    print(f"\n总计补了 {total} 条")
