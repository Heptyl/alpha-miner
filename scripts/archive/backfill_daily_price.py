"""回补 daily_price 缺失数据 — 按股票拉日期范围（高效版）。

策略优化:
  原方案: 140天 × 5489只 = ~768K次查询, 约33小时
  优化后: 5489只 × 1次查询(含日期范围) = 5489次查询, 约46分钟
  
  每只股票用 baostock query_history_k_data_plus 拉取 2025-06-20 ~ 2026-05-13 全量日K线，
  INSERT OR IGNORE 跳过已有记录。

用法:
  python scripts/backfill_daily_price.py               # 全量回补
  python scripts/backfill_daily_price.py --dry-run      # 只看缺失情况
  python scripts/backfill_daily_price.py --date 2026-05-13  # 只补某一天(按天模式)
"""

import argparse
import sqlite3
import sys
import time
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

DB_PATH = "data/alpha_miner.db"
BATCH_INSERT = 500
START_DATE = "2025-06-20"
END_DATE = "2026-05-13"
MIN_STOCKS_PER_DAY = 5000


def get_incomplete_days(conn: sqlite3.Connection) -> list[tuple[str, int]]:
    """返回所有覆盖不足的日期。"""
    cur = conn.execute(
        f"""
        SELECT trade_date, COUNT(*) as cnt
        FROM daily_price
        WHERE trade_date >= ? AND trade_date <= ?
        GROUP BY trade_date
        HAVING cnt < ?
        ORDER BY trade_date
        """,
        (START_DATE, END_DATE, MIN_STOCKS_PER_DAY),
    )
    return [(row[0], row[1]) for row in cur.fetchall()]


def get_existing_pairs(conn: sqlite3.Connection) -> set[tuple[str, str]]:
    """获取已有的 (stock_code, trade_date) 对集合，用于快速跳过。"""
    # 为了节省内存，只取不完整日期的已有记录
    incomplete_dates = {d for d, _ in get_incomplete_days(conn)}
    if not incomplete_dates:
        return set()
    
    placeholders = ",".join("?" * len(incomplete_dates))
    cur = conn.execute(
        f"SELECT stock_code, trade_date FROM daily_price WHERE trade_date IN ({placeholders})",
        list(incomplete_dates),
    )
    pairs = set()
    for row in cur:
        pairs.add((row[0], row[1]))
    logger.info("已有记录: %d 条 (不完整日期)", len(pairs))
    return pairs


def backfill_stock_by_stock(dry_run: bool = False):
    """按股票回补 — 每只股票拉一次日期范围，INSERT OR IGNORE。"""
    import baostock as bs

    conn = sqlite3.connect(DB_PATH)
    
    # 1) 诊断
    incomplete = get_incomplete_days(conn)
    if not incomplete:
        logger.info("所有日期均完整 (>= %d 只/天)", MIN_STOCKS_PER_DAY)
        conn.close()
        return

    logger.info("发现 %d 天不完整:", len(incomplete))
    total_gap = 0
    for d, cnt in incomplete[:20]:
        gap = MIN_STOCKS_PER_DAY - cnt
        total_gap += gap
        logger.info("  %s: %d只", d, cnt)
    if len(incomplete) > 20:
        logger.info("  ... 还有 %d 天", len(incomplete) - 20)
    logger.info("预估缺失: ~%d 条", total_gap)

    if dry_run:
        logger.info("(dry-run 模式, 不执行)")
        conn.close()
        return

    # 2) 获取已有记录（用于跳过）
    existing_pairs = get_existing_pairs(conn)

    # 3) baostock 获取全量A股代码
    lg = bs.login()
    if lg.error_code != "0":
        logger.error("baostock login 失败: %s", lg.error_msg)
        conn.close()
        return

    try:
        rs = bs.query_all_stock(day=END_DATE)
        if rs.error_code != "0":
            logger.error("query_all_stock 失败: %s", rs.error_msg)
            conn.close()
            return

        all_codes = []
        while rs.error_code == "0" and rs.next():
            row = rs.get_row_data()
            code = row[0]
            if (
                code.startswith("sh.6")
                or code.startswith("sz.0")
                or code.startswith("sz.3")
            ) and len(code) == 9:
                all_codes.append(code)

        logger.info("全量 A 股: %d 只, 开始按股票回补 %s ~ %s", len(all_codes), START_DATE, END_DATE)

        fields = "date,code,open,high,low,close,preclose,volume,amount,turn"
        total_inserted = 0
        total_fetched = 0
        t0_all = time.time()

        for i, bs_code in enumerate(all_codes):
            try:
                rs = bs.query_history_k_data_plus(
                    code=bs_code,
                    fields=fields,
                    start_date=START_DATE,
                    end_date=END_DATE,
                    frequency="d",
                    adjustflag="3",
                )
                if rs.error_code != "0":
                    continue

                rows_to_insert = []
                while rs.error_code == "0" and rs.next():
                    row = rs.get_row_data()
                    try:
                        stock_code = row[1].replace("sh.", "").replace("sz.", "")
                        # 防御: 确保stock_code是纯数字
                        if not stock_code.isdigit():
                            continue
                        trade_date = row[0]
                        close_val = float(row[5]) if row[5] else 0.0
                        if close_val <= 0:
                            continue
                        # 跳过已有
                        if (stock_code, trade_date) in existing_pairs:
                            continue
                        rows_to_insert.append((
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

                if rows_to_insert:
                    # 批量插入
                    for j in range(0, len(rows_to_insert), BATCH_INSERT):
                        batch = rows_to_insert[j:j + BATCH_INSERT]
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
                            logger.error("插入失败: %s", e)
                    conn.commit()
                    total_inserted += len(rows_to_insert)

                total_fetched += 1

            except Exception as e:
                logger.debug("单只失败 %s: %s", bs_code, e)
                continue

            # 每200只打印进度
            if (i + 1) % 200 == 0:
                elapsed = time.time() - t0_all
                rate = (i + 1) / elapsed
                eta = (len(all_codes) - i - 1) / rate
                logger.info(
                    "进度: %d/%d (%.0f%%), 新增%d条, %.1f只/s, ETA %.0fmin",
                    i + 1, len(all_codes), (i + 1) / len(all_codes) * 100,
                    total_inserted, rate, eta / 60,
                )

        elapsed_all = time.time() - t0_all
        logger.info("=" * 60)
        logger.info(
            "回补完成: %d只股票, 新增%d条, 耗时%.1fmin",
            total_fetched, total_inserted, elapsed_all / 60,
        )

        # 4) 最终验证
        still_incomplete = get_incomplete_days(conn)
        if still_incomplete:
            logger.warning("仍有 %d 天不完整:", len(still_incomplete))
            for d, c in still_incomplete[:10]:
                logger.warning("  %s: %d只", d, c)
        else:
            logger.info("所有日期均已完整!")

    finally:
        bs.logout()
        conn.close()


def backfill_single(target_date: str, dry_run: bool = False):
    """只补某一天 — 按天模式，逐只拉取。"""
    import baostock as bs

    conn = sqlite3.connect(DB_PATH)
    cnt = conn.execute(
        "SELECT COUNT(*) FROM daily_price WHERE trade_date = ?",
        (target_date,),
    ).fetchone()[0]

    logger.info("%s: 当前 %d 只", target_date, cnt)

    if cnt >= MIN_STOCKS_PER_DAY:
        logger.info("已完整, 无需回补")
        conn.close()
        return

    if dry_run:
        logger.info("(dry-run, 缺~%d只)", MIN_STOCKS_PER_DAY - cnt)
        conn.close()
        return

    existing = set(
        r[0] for r in conn.execute(
            "SELECT DISTINCT stock_code FROM daily_price WHERE trade_date = ?",
            (target_date,),
        ).fetchall()
    )

    lg = bs.login()
    if lg.error_code != "0":
        logger.error("login 失败")
        conn.close()
        return

    try:
        rs = bs.query_all_stock(day=target_date)
        all_codes = []
        while rs.error_code == "0" and rs.next():
            row = rs.get_row_data()
            code = row[0]
            if (code.startswith("sh.6") or code.startswith("sz.0") or code.startswith("sz.3")) and len(code) == 9:
                stock_code = code[3:]
                if stock_code not in existing:
                    all_codes.append(code)

        logger.info("需补: %d 只", len(all_codes))
        fields = "date,code,open,high,low,close,preclose,volume,amount,turn"
        results = []

        for i, bs_code in enumerate(all_codes):
            try:
                rs = bs.query_history_k_data_plus(
                    code=bs_code, fields=fields,
                    start_date=target_date, end_date=target_date,
                    frequency="d", adjustflag="3",
                )
                if rs.error_code != "0":
                    continue
                while rs.error_code == "0" and rs.next():
                    row = rs.get_row_data()
                    try:
                        stock_code = row[1].replace("sh.", "").replace("sz.", "")
                        # 防御: 确保stock_code是纯数字
                        if not stock_code.isdigit():
                            continue
                        close_val = float(row[5]) if row[5] else 0.0
                        if close_val <= 0:
                            continue
                        results.append((
                            stock_code, target_date,
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

            if (i + 1) % 500 == 0:
                logger.info("  进度: %d/%d (获取 %d)", i + 1, len(all_codes), len(results))

        if results:
            for j in range(0, len(results), BATCH_INSERT):
                batch = results[j:j + BATCH_INSERT]
                placeholders = ", ".join(["(?,?,?,?,?,?,?,?,?,?)"] * len(batch))
                values = []
                for r in batch:
                    values.extend(r)
                conn.execute(
                    f"INSERT OR IGNORE INTO daily_price "
                    f"(stock_code, trade_date, open, high, low, close, "
                    f"pre_close, volume, amount, turnover_rate) "
                    f"VALUES {placeholders}",
                    values,
                )
            conn.commit()

        new_cnt = conn.execute(
            "SELECT COUNT(*) FROM daily_price WHERE trade_date = ?",
            (target_date,),
        ).fetchone()[0]
        logger.info("完成: 插入 %d, 验证 %s → %d只", len(results), target_date, new_cnt)

    finally:
        bs.logout()
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="回补 daily_price 缺失数据")
    parser.add_argument("--dry-run", action="store_true", help="只看缺失, 不执行")
    parser.add_argument("--date", type=str, help="只补指定日期(按天模式)")
    args = parser.parse_args()

    if args.date:
        backfill_single(args.date, dry_run=args.dry_run)
    else:
        backfill_stock_by_stock(dry_run=args.dry_run)
