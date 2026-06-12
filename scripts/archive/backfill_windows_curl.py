"""用 Windows curl + 腾讯历史K线接口批量回填 daily_price。

WSL 内 requests 不能直接访问外网，必须通过 Windows curl 绕过。
腾讯接口 web.ifzq.gtimg.cn 支持一次拉取多天K线，效率高。

用法:
    uv run python scripts/backfill_windows_curl.py --days 120
    uv run python scripts/backfill_windows_curl.py --days 60 --workers 10
    uv run python scripts/backfill_windows_curl.py --codes 600589,002245 --days 180
"""

import json
import sqlite3
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "alpha_miner.db"
CURL = "/mnt/c/Windows/System32/curl.exe"


def _prefix(code: str) -> str:
    if code.startswith("6"):
        return "sh"
    if code.startswith(("0", "3")):
        return "sz"
    if code.startswith(("4", "8")):
        return "bj"
    return "sz"


def fetch_kline_range(code: str, start: str, end: str) -> list[dict]:
    """拉单只股票一段日期范围的K线，返回行列表。"""
    tc = f"{_prefix(code)}{code}"
    url = (
        f"https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?"
        f"param={tc},day,{start},{end},320,qfq"
    )
    try:
        r = subprocess.run(
            [CURL, "-s", "--connect-timeout", "10", "--max-time", "15", url],
            capture_output=True, text=True, timeout=20,
        )
        if r.returncode != 0:
            return []
        data = json.loads(r.stdout)
        stock_data = data.get("data", {}).get(tc, {})
        klines = stock_data.get("qfqday") or stock_data.get("day") or []
        results = []
        for row in klines:
            # [date, open, close, high, low, volume]
            if len(row) >= 6:
                results.append({
                    "stock_code": code,
                    "trade_date": row[0],
                    "open": float(row[1]),
                    "close": float(row[2]),
                    "high": float(row[3]),
                    "low": float(row[4]),
                    "volume": float(row[5]),
                    "amount": 0.0,
                    "turnover_rate": 0.0,
                })
        return results
    except Exception:
        return []


def get_all_codes() -> list[str]:
    """从 DB 各表合并获取所有股票代码。"""
    conn = sqlite3.connect(str(DB_PATH))
    codes = set()
    for table in ["daily_price", "zt_pool", "strong_pool", "lhb_detail"]:
        try:
            rows = conn.execute(
                f"SELECT DISTINCT stock_code FROM {table}"
            ).fetchall()
            codes.update(r[0] for r in rows)
        except Exception:
            pass
    conn.close()
    return sorted(codes)


def get_existing_dates(code: str) -> set[str]:
    """获取某只股票已有的交易日期。"""
    conn = sqlite3.connect(str(DB_PATH))
    rows = conn.execute(
        "SELECT trade_date FROM daily_price WHERE stock_code = ?",
        (code,),
    ).fetchall()
    conn.close()
    return set(r[0] for r in rows)


def save_rows(rows: list[dict]) -> int:
    """批量写入 daily_price，INSERT OR IGNORE。"""
    if not rows:
        return 0
    conn = sqlite3.connect(str(DB_PATH))
    saved = 0
    for r in rows:
        try:
            conn.execute(
                """INSERT OR IGNORE INTO daily_price
                (stock_code, trade_date, open, high, low, close, pre_close, volume, amount, turnover_rate, snapshot_time)
                VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?, ?, datetime('now'))""",
                (r["stock_code"], r["trade_date"], r["open"], r["high"],
                 r["low"], r["close"], r["volume"], r["amount"], r["turnover_rate"]),
            )
            saved += 1
        except Exception:
            pass
    conn.commit()
    conn.close()
    return saved


def get_trade_dates_from_db() -> list[str]:
    """从 DB 的 daily_price 获取所有已知的交易日。"""
    conn = sqlite3.connect(str(DB_PATH))
    rows = conn.execute(
        "SELECT DISTINCT trade_date FROM daily_price ORDER BY trade_date"
    ).fetchall()
    conn.close()
    return [r[0] for r in rows]


def backfill_all(days: int = 120, workers: int = 5, codes_filter: list[str] | None = None):
    """主回填逻辑。"""
    # 1. 获取交易日历（从DB已有数据推断）
    trade_dates = get_trade_dates_from_db()
    if not trade_dates:
        print("DB中无交易日数据，无法确定回填范围")
        return
    earliest = trade_dates[0]
    print(f"DB 最早交易日: {earliest}, 最新: {trade_dates[-1]}")

    # 计算回填起始日（earliest 之前 days 天）
    from datetime import datetime, timedelta
    earliest_dt = datetime.strptime(earliest, "%Y-%m-%d")
    backfill_start_dt = earliest_dt - timedelta(days=days)
    backfill_start = backfill_start_dt.strftime("%Y-%m-%d")
    backfill_end = earliest_dt.strftime("%Y-%m-%d")  # 到最早那天（含）
    print(f"回填范围: {backfill_start} ~ {backfill_end} (约{days}自然日)")

    # 2. 获取股票代码
    if codes_filter:
        codes = codes_filter
    else:
        codes = get_all_codes()
    print(f"股票数量: {len(codes)}")

    # 3. 批量回填
    total_saved = 0
    total_fetched = 0
    failed_codes = []

    def _fetch_one(code: str) -> tuple[str, int, int]:
        rows = fetch_kline_range(code, backfill_start, backfill_end)
        if not rows:
            return code, 0, 0
        # 过滤掉已有的日期
        existing = get_existing_dates(code)
        new_rows = [r for r in rows if r["trade_date"] not in existing]
        if not new_rows:
            return code, len(rows), 0
        saved = save_rows(new_rows)
        return code, len(rows), saved

    done = 0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_fetch_one, c): c for c in codes}
        for f in as_completed(futures):
            done += 1
            code, fetched, saved = f.result()
            total_fetched += fetched
            total_saved += saved
            if fetched == 0:
                failed_codes.append(code)
            if done % 100 == 0 or done == len(codes):
                print(f"  进度 {done}/{len(codes)} | 新增 {total_saved} 条 | 失败 {len(failed_codes)} 只")

    print(f"\n=== 回填完成 ===")
    print(f"  总股票: {len(codes)}")
    print(f"  获取K线条数: {total_fetched}")
    print(f"  新增写入: {total_saved}")
    print(f"  失败/无数据: {len(failed_codes)} 只")
    if failed_codes and len(failed_codes) <= 20:
        print(f"  失败代码: {failed_codes}")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="用 Windows curl 回填 daily_price")
    parser.add_argument("--days", type=int, default=120, help="回填天数（自然日，默认120）")
    parser.add_argument("--workers", type=int, default=5, help="并发数（默认5）")
    parser.add_argument("--codes", type=str, default=None, help="指定股票代码，逗号分隔")
    args = parser.parse_args()

    codes_filter = args.codes.split(",") if args.codes else None
    backfill_all(days=args.days, workers=args.workers, codes_filter=codes_filter)
