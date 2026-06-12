"""collect_daily.py — 盘后统一采集入口

按顺序采集所有日频数据, 每步验证+自动重试, 完成后触发盘后复盘。

用法:
  # 采集今天(默认)
  uv run python scripts/collect_daily.py

  # 采集指定日期
  uv run python scripts/collect_daily.py --date 2026-05-15

  # 仅采集, 不触发复盘
  uv run python scripts/collect_daily.py --no-review

  # 跳过某些步骤
  uv run python scripts/collect_daily.py --skip news,sentiment

Crontab配置(每个交易日15:30自动运行):
  30 15 * * 1-5 cd /home/ccy/alpha-miner && /home/ccy/.local/bin/uv run python scripts/collect_daily.py >> output/trader/collect_daily.log 2>&1
"""

from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("collect_daily")

DB_PATH = Path(__file__).resolve().parents[1] / "data" / "alpha_miner.db"


def _is_trading_day(d: str) -> bool:
    """简单判断: 周末一定不是交易日"""
    dt = datetime.strptime(d, "%Y-%m-%d")
    return dt.weekday() < 5


def _count_rows(table: str, trade_date: str, conn: sqlite3.Connection) -> int:
    """查询某日某表数据条数"""
    try:
        r = conn.execute(
            f"SELECT COUNT(*) FROM {table} WHERE trade_date = ?", (trade_date,)
        ).fetchone()
        return r[0] if r else 0
    except Exception:
        return 0


def _run_step(name: str, fn, trade_date: str, conn: sqlite3.Connection,
              min_rows: int = 0, table: str = "", retries: int = 1) -> bool:
    """执行单步采集, 验证条数, 失败重试

    Args:
        name: 步骤名称
        fn: 采集函数 fn(trade_date, conn) -> int(写入条数)
        trade_date: 交易日
        conn: DB连接
        min_rows: 最低期望条数(0=不验证)
        table: 验证用表名
        retries: 重试次数

    Returns:
        是否成功
    """
    for attempt in range(retries + 1):
        try:
            t0 = time.time()
            cnt = fn(trade_date, conn)
            elapsed = time.time() - t0

            # 验证
            if min_rows > 0 and table:
                actual = _count_rows(table, trade_date, conn)
                if actual < min_rows:
                    logger.warning("[%-16s] 条数不足: DB=%d < 期望%d (%.1fs)",
                                   name, actual, min_rows, elapsed)
                    if attempt < retries:
                        logger.info("[%-16s] 第%d次重试...", name, attempt + 1)
                        time.sleep(3)
                        continue
                    return False

            logger.info("[%-16s] OK  写入%d条 (%.1fs)", name, cnt, elapsed)
            return True

        except Exception as e:
            logger.warning("[%-16s] 异常: %s", name, str(e)[:100])
            if attempt < retries:
                logger.info("[%-16s] 第%d次重试...", name, attempt + 1)
                time.sleep(3)
            else:
                logger.error("[%-16s] 失败, 跳过", name)
                return False

    return False


# ── 各步骤采集函数 ──

def step_daily_price(trade_date: str, conn: sqlite3.Connection) -> int:
    """日K线"""
    from src.data.sources.akshare_price import fetch, save
    from src.data.storage import Storage
    db = Storage(str(DB_PATH))
    df = fetch(trade_date)
    if df is None or df.empty:
        return 0
    return save(df, db)


def step_zt_pool(trade_date: str, conn: sqlite3.Connection) -> int:
    """涨停池+炸板池+强势股"""
    from src.data.sources.akshare_zt_pool import (
        fetch_zt_pool, save_zt_pool,
        fetch_zb_pool, save_zb_pool,
        fetch_strong_pool, save_strong_pool,
    )
    from src.data.storage import Storage
    db = Storage(str(DB_PATH))
    total = 0
    for fn_fetch, fn_save, label in [
        (fetch_zt_pool, save_zt_pool, "涨停"),
        (fetch_zb_pool, save_zb_pool, "炸板"),
        (fetch_strong_pool, save_strong_pool, "强势"),
    ]:
        try:
            df = fn_fetch(trade_date)
            if df is not None and not df.empty:
                total += fn_save(df, db)
        except Exception as e:
            logger.debug("  %s采集失败: %s", label, e)
    return total


def step_fund_flow(trade_date: str, conn: sqlite3.Connection) -> int:
    """资金流向"""
    from src.data.sources.akshare_fund_flow import fetch, save
    from src.data.storage import Storage
    db = Storage(str(DB_PATH))
    df = fetch(trade_date)
    if df is None or df.empty:
        return 0
    return save(df, db)


def step_lhb(trade_date: str, conn: sqlite3.Connection) -> int:
    """龙虎榜汇总"""
    from src.data.sources.akshare_lhb import fetch, save
    from src.data.storage import Storage
    db = Storage(str(DB_PATH))
    df = fetch(trade_date)
    if df is None or df.empty:
        return 0
    return save(df, db)


def step_lhb_seats(trade_date: str, conn: sqlite3.Connection) -> int:
    """龙虎榜席位明细"""
    from src.data.sources.lhb_seats import fetch_date_seats, save
    rows = fetch_date_seats(trade_date)
    if not rows:
        return 0
    return save(rows)


def step_northbound(trade_date: str, conn: sqlite3.Connection) -> int:
    """北向资金"""
    from scripts.collect_northbound import collect_today
    collect_today(conn)
    r = conn.execute(
        "SELECT COUNT(*) FROM northbound_flow WHERE trade_date = ?",
        (trade_date,),
    ).fetchone()
    return r[0] if r else 0


def step_lockup(trade_date: str, conn: sqlite3.Connection) -> int:
    """解禁日历(未来30天)"""
    from scripts.collect_lockup import collect
    end = (datetime.strptime(trade_date, "%Y-%m-%d") + timedelta(days=30)).strftime("%Y-%m-%d")
    return collect(conn, trade_date, end)


def step_news(trade_date: str, conn: sqlite3.Connection) -> int:
    """新闻采集"""
    from src.data.sources.akshare_news import fetch, save
    from src.data.storage import Storage
    db = Storage(str(DB_PATH))
    df = fetch(trade_date=trade_date)
    if df is None or df.empty:
        return 0
    return save(df, db)


def step_sentiment(trade_date: str, conn: sqlite3.Connection) -> int:
    """新闻情感分析(对当日新闻打分)"""
    from src.agent.sentiment_analyzer import SentimentAnalyzer
    sa = SentimentAnalyzer()
    rows = conn.execute(
        "SELECT news_id, title, content FROM news "
        "WHERE (stock_code IS NULL OR stock_code = '') "
        "AND publish_time LIKE ? AND sentiment_score IS NULL "
        "LIMIT 200",
        (f"{trade_date}%",),
    ).fetchall()
    updated = 0
    for r in rows:
        try:
            result = sa.analyze(r[1], r[2] or "")
            conn.execute(
                "UPDATE news SET sentiment_score = ? WHERE news_id = ?",
                (result.score, r[0]),
            )
            updated += 1
        except Exception:
            pass
    conn.commit()
    return updated


def step_concept(trade_date: str, conn: sqlite3.Connection) -> int:
    """概念板块(7天缓存)"""
    from src.data.sources.akshare_concept import fetch, save
    from src.data.storage import Storage
    db = Storage(str(DB_PATH))
    df = fetch(trade_date, db=db)
    if df is None or df.empty:
        return 0
    return save(df, db)


def step_review(trade_date: str, conn: sqlite3.Connection) -> int:
    """盘后复盘Agent"""
    from src.agent.review_agent import review_trades
    report = review_trades(trade_date, use_llm=True)
    return 1 if report else 0


# ── 主流程 ──

# 采集步骤定义: (名称, 函数, 最小条数, 验证表名)
STEPS = [
    ("daily_price",  step_daily_price,  100,  "daily_price"),
    ("zt_pool",      step_zt_pool,      0,    "zt_pool"),
    ("fund_flow",    step_fund_flow,    100,  "fund_flow"),
    ("lhb",          step_lhb,          0,    "lhb_detail"),
    ("lhb_seats",    step_lhb_seats,    0,    "lhb_seats"),
    ("northbound",   step_northbound,   0,    "northbound_flow"),
    ("lockup",       step_lockup,       0,    "lockup_calendar"),
    ("concept",      step_concept,      0,    "concept_mapping"),
    ("news",         step_news,         0,    "news"),
    ("sentiment",    step_sentiment,    0,    "news"),
]


def collect_daily(trade_date: str, skip_steps: set = None,
                  do_review: bool = True) -> dict:
    """执行完整盘后采集流程

    Args:
        trade_date: 交易日 YYYY-MM-DD
        skip_steps: 跳过的步骤集合
        do_review: 是否触发复盘

    Returns:
        {"total_steps": N, "success": N, "failed": N, "duration": float}
    """
    skip = skip_steps or set()
    conn = sqlite3.connect(str(DB_PATH))

    results = {"total": 0, "success": 0, "failed": 0}
    t_start = time.time()

    logger.info("=" * 50)
    logger.info("盘后采集 %s 开始", trade_date)
    logger.info("=" * 50)

    for name, fn, min_rows, table in STEPS:
        if name in skip:
            logger.info("[%-16s] SKIP", name)
            continue
        results["total"] += 1
        ok = _run_step(name, fn, trade_date, conn, min_rows, table, retries=1)
        if ok:
            results["success"] += 1
        else:
            results["failed"] += 1

    # 盘后复盘
    if do_review and "review" not in skip:
        logger.info("-" * 50)
        logger.info("[%-16s] 盘后复盘Agent开始...", "review")
        try:
            t0 = time.time()
            step_review(trade_date, conn)
            logger.info("[%-16s] OK  (%.1fs)", "review", time.time() - t0)
        except Exception as e:
            logger.warning("[%-16s] 复盘失败: %s", "review", str(e)[:100])

    # 刷盘LLM统计
    try:
        from src.agent.llm_client import get_client
        get_client().flush_stats()
    except Exception:
        pass

    conn.close()
    elapsed = time.time() - t_start
    results["duration"] = elapsed

    logger.info("=" * 50)
    logger.info("盘后采集完成: 成功%d/总%d 失败%d (%.1fs)",
                results["success"], results["total"],
                results["failed"], elapsed)
    logger.info("=" * 50)

    # 写入采集日志
    _save_collect_log(trade_date, results)

    return results


def _save_collect_log(trade_date: str, results: dict):
    """记录采集结果到DB"""
    try:
        conn = sqlite3.connect(str(DB_PATH))
        conn.execute("""
            CREATE TABLE IF NOT EXISTS collect_log (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                trade_date  TEXT NOT NULL,
                total       INTEGER DEFAULT 0,
                success     INTEGER DEFAULT 0,
                failed      INTEGER DEFAULT 0,
                duration_s  REAL DEFAULT 0,
                created_at  TEXT DEFAULT (datetime('now')),
                UNIQUE(trade_date)
            )
        """)
        conn.execute(
            "INSERT OR REPLACE INTO collect_log (trade_date, total, success, failed, duration_s) "
            "VALUES (?, ?, ?, ?, ?)",
            (trade_date, results["total"], results["success"],
             results["failed"], results.get("duration", 0)),
        )
        conn.commit()
        conn.close()
    except Exception:
        pass


def main():
    parser = argparse.ArgumentParser(description="盘后统一采集")
    parser.add_argument("--date", type=str, help="指定日期 YYYY-MM-DD (默认今天)")
    parser.add_argument("--no-review", action="store_true", help="跳过盘后复盘")
    parser.add_argument("--skip", type=str, default="", help="跳过的步骤(逗号分隔)")
    parser.add_argument("--dry-run", action="store_true", help="仅显示步骤不执行")
    args = parser.parse_args()

    trade_date = args.date or datetime.now().strftime("%Y-%m-%d")
    skip_steps = set(s.strip() for s in args.skip.split(",") if s.strip())

    if args.dry_run:
        print(f"采集日期: {trade_date}")
        print(f"跳过步骤: {skip_steps or '无'}")
        print(f"触发复盘: {'否' if args.no_review else '是'}")
        print("\n采集步骤:")
        for name, fn, min_rows, table in STEPS:
            status = "SKIP" if name in skip_steps else "OK"
            print(f"  [{status}] {name} (表={table}, 最低{min_rows}条)")
        if not args.no_review:
            print(f"  [OK] review (盘后复盘Agent)")
        return

    # 周末检查
    if not _is_trading_day(trade_date):
        logger.warning("%s 是周末, 可能非交易日(继续执行)", trade_date)

    collect_daily(trade_date, skip_steps, do_review=not args.no_review)


if __name__ == "__main__":
    main()
