"""解禁日历采集器 — 东财datacenter个股级别

数据源: 东财datacenter RPT_LIFT_STAGE (已验证API可用)
覆盖: 未来1个月解禁数据(每周采集即可)

用法:
  uv run python scripts/collect_lockup.py              # 未来30天
  uv run python scripts/collect_lockup.py --days 90    # 未来90天
  uv run python scripts/collect_lockup.py --backfill    # 回填过去60天
"""

import argparse
import logging
import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

DB_PATH = Path(__file__).resolve().parents[1] / "data" / "alpha_miner.db"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def _ensure_table(conn: sqlite3.Connection):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS lockup_calendar (
            stock_code       TEXT NOT NULL,
            stock_name       TEXT DEFAULT '',
            free_date        TEXT NOT NULL,
            free_shares      REAL,
            lift_market_cap  REAL,
            free_ratio       REAL,
            total_ratio      REAL,
            free_type        TEXT DEFAULT '',
            snapshot_time    TEXT DEFAULT (datetime('now')),
            PRIMARY KEY (stock_code, free_date, free_type)
        )
    """)


def collect(conn: sqlite3.Connection, start_date: str, end_date: str):
    """采集指定日期范围的解禁数据"""
    from src.data.sources.eastmoney import EastMoneyClient

    logger.info(f"采集解禁日历: {start_date} ~ {end_date}")

    client = EastMoneyClient()
    df = client.fetch_report(
        report_name="RPT_LIFT_STAGE",
        columns="SECURITY_CODE,SECURITY_NAME_ABBR,FREE_DATE,"
                "FREE_SHARES,LIFT_MARKET_CAP,FREE_RATIO,TOTAL_RATIO,"
                "FREE_SHARES_TYPE",
        filter_expr=f"(FREE_DATE>='{start_date}')(FREE_DATE<='{end_date}')",
        sort_columns="FREE_DATE",
        sort_types="1",
    )

    if df.empty:
        logger.warning("无解禁数据")
        return 0

    count = 0
    for _, r in df.iterrows():
        code = r.get("SECURITY_CODE", "")
        name = r.get("SECURITY_NAME_ABBR", "")
        free_date = str(r.get("FREE_DATE", ""))[:10]
        if not code or not free_date:
            continue

        # 过滤北交所(8/9开头)
        if code.startswith(("8", "9")):
            continue

        conn.execute("""
            INSERT OR REPLACE INTO lockup_calendar
            (stock_code, stock_name, free_date, free_shares, lift_market_cap,
             free_ratio, total_ratio, free_type, snapshot_time)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
        """, (
            code, name, free_date,
            r.get("FREE_SHARES"),
            r.get("LIFT_MARKET_CAP"),
            r.get("FREE_RATIO"),
            r.get("TOTAL_RATIO"),
            str(r.get("FREE_SHARES_TYPE", ""))[:50],
        ))
        count += 1

    conn.commit()
    client.close()
    logger.info(f"解禁日历入库: {count}条 ({start_date}~{end_date})")
    return count


def show_stats(conn: sqlite3.Connection):
    """显示入库数据统计"""
    total = conn.execute("SELECT COUNT(*) FROM lockup_calendar").fetchone()[0]
    latest = conn.execute("SELECT MAX(free_date) FROM lockup_calendar").fetchone()[0]
    earliest = conn.execute("SELECT MIN(free_date) FROM lockup_calendar").fetchone()[0]

    print(f"\n解禁日历统计:")
    print(f"  总条数: {total}")
    print(f"  时间范围: {earliest} ~ {latest}")

    if total > 0:
        # 按日汇总
        daily = conn.execute("""
            SELECT free_date, COUNT(*) as cnt,
                   ROUND(SUM(free_shares)/10000, 2) as free_wanwan,
                   ROUND(SUM(lift_market_cap)/100000000, 2) as cap_yi
            FROM lockup_calendar
            GROUP BY free_date
            ORDER BY free_date DESC LIMIT 10
        """).fetchall()
        print(f"\n  最近10个解禁日:")
        print(f"  {'日期':<12} {'只数':>4} {'解禁万股':>10} {'解禁市值亿':>10}")
        print(f"  {'-'*40}")
        for r in daily:
            print(f"  {r[0]:<12} {r[1]:>4} {r[2]:>10} {r[3]:>10}")

        # 近期大额解禁
        big = conn.execute("""
            SELECT stock_code, stock_name, free_date,
                   ROUND(free_shares/10000, 0) as free_wanwan,
                   ROUND(lift_market_cap/100000000, 2) as cap_yi
            FROM lockup_calendar
            WHERE free_date >= date('now')
            ORDER BY lift_market_cap DESC LIMIT 10
        """).fetchall()
        if big:
            print(f"\n  未来大额解禁TOP10:")
            print(f"  {'代码':<8} {'名称':<8} {'解禁日':<12} {'解禁万股':>8} {'市值亿':>8}")
            print(f"  {'-'*48}")
            for r in big:
                print(f"  {r[0]:<8} {r[1]:<8} {r[2]:<12} {r[3]:>8.0f} {r[4]:>8.2f}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="解禁日历采集器")
    parser.add_argument("--days", type=int, default=30, help="采集未来N天(默认30)")
    parser.add_argument("--backfill", action="store_true", help="回填过去60天")
    args = parser.parse_args()

    conn = sqlite3.connect(str(DB_PATH))
    _ensure_table(conn)

    today = datetime.now().strftime("%Y-%m-%d")
    future = (datetime.now() + timedelta(days=args.days)).strftime("%Y-%m-%d")

    if args.backfill:
        past = (datetime.now() - timedelta(days=60)).strftime("%Y-%m-%d")
        collect(conn, past, today)

    collect(conn, today, future)
    show_stats(conn)

    conn.close()
