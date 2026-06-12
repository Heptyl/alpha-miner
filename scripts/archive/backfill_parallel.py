"""高效回补 — 多进程并行按天拉取。

每个子进程负责若干天，每天用baostock拉缺失股票。
baostock不是线程安全的，用multiprocessing隔离进程。
"""

import sqlite3
import sys
import time
import logging
import multiprocessing as mp
from functools import partial

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [Worker-%(process)d] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

DB_PATH = "data/alpha_miner.db"
START_DATE = "2025-06-20"
END_DATE = "2026-05-13"
MIN_STOCKS = 5000
BATCH_INSERT = 500


def get_incomplete_days():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.execute(
        f"""SELECT trade_date, COUNT(*) as cnt FROM daily_price
            WHERE trade_date >= ? AND trade_date <= ?
            GROUP BY trade_date HAVING cnt < ?
            ORDER BY trade_date""",
        (START_DATE, END_DATE, MIN_STOCKS),
    )
    days = [(row[0], row[1]) for row in cur.fetchall()]
    conn.close()
    return days


def backfill_one_day(trade_date: str) -> tuple[str, int, int]:
    """拉取单天缺失的股票。返回 (date, inserted, total_after)。"""
    import baostock as bs

    conn = sqlite3.connect(DB_PATH)

    # 获取已有代码
    existing = set(
        r[0] for r in conn.execute(
            "SELECT DISTINCT stock_code FROM daily_price WHERE trade_date = ?",
            (trade_date,),
        ).fetchall()
    )

    # 获取全量A股列表
    lg = bs.login()
    if lg.error_code != "0":
        conn.close()
        return (trade_date, -1, len(existing))

    try:
        rs = bs.query_all_stock(day=trade_date)
        if rs.error_code != "0":
            conn.close()
            return (trade_date, -2, len(existing))

        all_codes = []
        while rs.error_code == "0" and rs.next():
            row = rs.get_row_data()
            code = row[0]
            if (code.startswith("sh.6") or code.startswith("sz.0") or code.startswith("sz.3")) and len(code) == 9:
                stock_code = code[3:]
                if stock_code not in existing:
                    all_codes.append(code)

        if not all_codes:
            bs.logout()
            conn.close()
            return (trade_date, 0, len(existing))

        # 逐只拉取
        fields = "date,code,open,high,low,close,preclose,volume,amount,turn"
        results = []

        for bs_code in all_codes:
            try:
                rs = bs.query_history_k_data_plus(
                    code=bs_code, fields=fields,
                    start_date=trade_date, end_date=trade_date,
                    frequency="d", adjustflag="3",
                )
                if rs.error_code != "0":
                    continue
                while rs.error_code == "0" and rs.next():
                    row = rs.get_row_data()
                    try:
                        stock_code = row[1].replace("sh.", "").replace("sz.", "")
                        close_val = float(row[5]) if row[5] else 0.0
                        if close_val <= 0:
                            continue
                        results.append((
                            stock_code, trade_date,
                            float(row[2]) if row[2] else None,
                            float(row[3]) if row[3] else None,
                            float(row[4]) if row[4] else None,
                            close_val,
                            float(row[6]) if row[6] else None,
                            float(row[7]) if row[7] else None,
                            float(row[8]) if row[8] else None,
                            float(row[9]) if row[9] else None,
                        ))
                    except (ValueError, IndexError):
                        continue
            except Exception:
                continue

        # 批量插入
        if results:
            for j in range(0, len(results), BATCH_INSERT):
                batch = results[j:j + BATCH_INSERT]
                placeholders = ", ".join(["(?,?,?,?,?,?,?,?,?,?)"] * len(batch))
                values = []
                for r in batch:
                    values.extend(r)
                try:
                    conn.execute(
                        f"INSERT OR IGNORE INTO daily_price "
                        f"(stock_code, trade_date, open, high, low, close, "
                        f"pre_close, volume, amount, turnover_rate) "
                        f"VALUES {placeholders}",
                        values,
                    )
                except Exception as e:
                    pass
            conn.commit()

        final_cnt = conn.execute(
            "SELECT COUNT(*) FROM daily_price WHERE trade_date = ?",
            (trade_date,),
        ).fetchone()[0]

        bs.logout()
        conn.close()
        return (trade_date, len(results), final_cnt)

    except Exception as e:
        bs.logout()
        conn.close()
        return (trade_date, -3, 0)


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--workers", type=int, default=4, help="并行进程数")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=0, help="只处理前N天(测试用)")
    args = parser.parse_args()

    days = get_incomplete_days()
    if not days:
        logger.info("所有日期均完整!")
        return

    logger.info("发现 %d 天不完整", len(days))

    # 按已有数量排序，少的先补（更容易达标）
    days.sort(key=lambda x: x[1])

    if args.dry_run:
        for d, cnt in days[:20]:
            logger.info("  %s: %d只 (缺%d)", d, cnt, MIN_STOCKS - cnt)
        logger.info("  ... 共%d天", len(days))
        return

    if args.limit > 0:
        days = days[:args.limit]
        logger.info("限制: 只处理前 %d 天", args.limit)

    # 按worker数分组（轮流分配，均衡负载）
    date_list = [d for d, _ in days]
    logger.info("开始回补 %d 天, %d 并行进程", len(date_list), args.workers)

    t0 = time.time()
    total_inserted = 0

    with mp.Pool(processes=args.workers) as pool:
        for i, result in enumerate(pool.imap_unordered(backfill_one_day, date_list)):
            date, inserted, final_cnt = result
            total_inserted += max(0, inserted)
            status = "OK" if final_cnt >= MIN_STOCKS else f"仍缺{MIN_STOCKS - final_cnt}"
            elapsed = time.time() - t0
            rate = (i + 1) / elapsed
            eta = (len(date_list) - i - 1) / rate
            logger.info(
                "[%d/%d] %s: +%-5d → %d只 %s (%.1f天/min, ETA %.0fmin)",
                i + 1, len(date_list), date, inserted, final_cnt, status,
                rate * 60, eta / 60,
            )

    elapsed = time.time() - t0
    logger.info("=" * 60)
    logger.info("回补完成: 新增%d条, 耗时%.1fmin", total_inserted, elapsed / 60)

    # 最终验证
    still_incomplete = get_incomplete_days()
    if still_incomplete:
        logger.warning("仍有 %d 天不完整", len(still_incomplete))
        for d, c in still_incomplete[:10]:
            logger.warning("  %s: %d只", d, c)
    else:
        logger.info("所有日期均完整!")


if __name__ == "__main__":
    main()
