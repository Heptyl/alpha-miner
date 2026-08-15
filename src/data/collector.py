"""数据采集调度器 — 统一调用各数据源，单源失败不影响整体。

采集完成后自动聚合：
- market_emotion：涨停数、跌停数、最高板、情绪级别（乐股源直取）
- concept_daily：每个概念当日涨停数、龙头等
"""

import concurrent.futures
import logging
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Callable, Optional

import akshare as ak
import pandas as pd

from src.data.sources import (
    akshare_concept,
    akshare_fund_flow,
    akshare_lhb,
    akshare_news,
    akshare_price,
    akshare_zt_pool,
)
from src.data.storage import Storage

logger = logging.getLogger(__name__)

_RESULT_KEYS = (
    "zt_pool", "zb_pool", "strong_pool", "lhb_detail", "daily_price",
    "fund_flow", "news", "concept_mapping", "market_emotion", "concept_daily",
)


@dataclass
class _FetchOutcome:
    data: pd.DataFrame
    elapsed: float
    error: Exception | None = None


def _fetch_many(
    tasks: dict[str, Callable[[], pd.DataFrame]],
    max_workers: int,
) -> dict[str, _FetchOutcome]:
    """并行执行互不依赖的网络读取；写库仍由主线程串行完成。"""
    outcomes: dict[str, _FetchOutcome] = {}

    def _timed_fetch(fetcher: Callable[[], pd.DataFrame]) -> _FetchOutcome:
        started = time.perf_counter()
        try:
            data = fetcher()
            return _FetchOutcome(
                data=data if data is not None else pd.DataFrame(),
                elapsed=time.perf_counter() - started,
            )
        except Exception as exc:
            return _FetchOutcome(
                data=pd.DataFrame(),
                elapsed=time.perf_counter() - started,
                error=exc,
            )

    if not tasks:
        return outcomes

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=max(1, min(max_workers, len(tasks))),
    ) as executor:
        futures = {executor.submit(_timed_fetch, fn): name for name, fn in tasks.items()}
        for future in concurrent.futures.as_completed(futures):
            name = futures[future]
            outcome = future.result()
            outcomes[name] = outcome
            if outcome.error:
                logger.warning(
                    "%s: fetch failed after %.2fs: %s",
                    name, outcome.elapsed, outcome.error,
                )
            else:
                logger.info(
                    "%s: fetched %d rows in %.2fs",
                    name, len(outcome.data), outcome.elapsed,
                )

    return outcomes


def _fetch_news_parallel(
    stock_codes: list[str],
    trade_date: str,
    max_workers: int = 4,
) -> pd.DataFrame:
    """并发拉取重点股新闻，并保持单股失败隔离。"""
    codes = list(dict.fromkeys(stock_codes))[:120]
    tasks = {
        code: (lambda stock_code=code: akshare_news.fetch(
            stock_code=stock_code,
            trade_date=trade_date,
        ))
        for code in codes
    }
    outcomes = _fetch_many(tasks, max_workers=max_workers)
    frames = [outcome.data for outcome in outcomes.values() if not outcome.data.empty]
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True).drop_duplicates(subset=["news_id"], keep="first")


def collect_date(trade_date: str, db: Optional[Storage] = None, mode: str = "today") -> dict[str, int]:
    """采集指定日期的全市场数据。

    逐个调用数据源，单源失败不影响其他源。
    采集完成后自动聚合 market_emotion 和 concept_daily。

    Args:
        trade_date: 交易日期 YYYY-MM-DD
        db: 数据库实例，None 时使用默认路径
        mode: "today" 用实时行情, "backfill" 用历史日K线

    Returns:
        dict: {source_name: row_count}
    """
    if mode not in {"today", "backfill"}:
        raise ValueError(f"unsupported collection mode: {mode}")

    results = dict.fromkeys(_RESULT_KEYS, 0)
    requested_date = datetime.strptime(trade_date, "%Y-%m-%d")
    if requested_date.weekday() >= 5:
        logger.warning("collection skipped: %s is a weekend", trade_date)
        return results

    if db is None:
        db = Storage()
        db.init_db()

    is_live_date = (
        mode == "today"
        and trade_date == datetime.now().strftime("%Y-%m-%d")
    )
    started = time.perf_counter()

    # 第一阶段：轻量、互不依赖的数据源并发读取，随后主线程串行写库。
    pool_outcomes = _fetch_many({
        "zt_pool": lambda: akshare_zt_pool.fetch_zt_pool(trade_date),
        "zb_pool": lambda: akshare_zt_pool.fetch_zb_pool(trade_date),
        "strong_pool": lambda: akshare_zt_pool.fetch_strong_pool(trade_date),
        "lhb_detail": lambda: akshare_lhb.fetch(trade_date),
    }, max_workers=4)
    pool_savers = {
        "zt_pool": akshare_zt_pool.save_zt_pool,
        "zb_pool": akshare_zt_pool.save_zb_pool,
        "strong_pool": akshare_zt_pool.save_strong_pool,
        "lhb_detail": akshare_lhb.save,
    }
    for name, saver in pool_savers.items():
        outcome = pool_outcomes[name]
        if outcome.error or outcome.data.empty:
            continue
        try:
            results[name] = saver(outcome.data, db)
        except Exception as exc:
            logger.warning("%s: save failed: %s", name, exc)

    # 第二阶段：全量行情与概念映射并行取数。
    if is_live_date:
        def price_fetcher() -> pd.DataFrame:
            return akshare_price.fetch_today(trade_date, db=db)
    else:
        def price_fetcher() -> pd.DataFrame:
            return akshare_price.fetch_history(trade_date, db=db)

    market_tasks: dict[str, Callable[[], pd.DataFrame]] = {
        "daily_price": price_fetcher,
        "concept_mapping": lambda: akshare_concept.fetch(trade_date, db=db),
    }
    if not is_live_date:
        logger.info("fund_flow: %s 非实时日期，跳过实时排行以避免历史污染", trade_date)

    market_outcomes = _fetch_many(market_tasks, max_workers=3)
    price_outcome = market_outcomes["daily_price"]
    if not price_outcome.error and not price_outcome.data.empty:
        try:
            results["daily_price"] = akshare_price.save(price_outcome.data, db, dedup=True)
        except Exception as exc:
            logger.warning("daily_price: save failed: %s", exc)

    # py_mini_racer/V8 与 AkShare 的概念接口并发初始化会导致进程级崩溃，
    # 因此等概念任务结束后在主线程拉资金流；该源自身已做安全的分页并发。
    fund_outcome = None
    if is_live_date:
        fund_started = time.perf_counter()
        try:
            fund_data = akshare_fund_flow.fetch(trade_date, db=db)
            fund_outcome = _FetchOutcome(
                data=fund_data,
                elapsed=time.perf_counter() - fund_started,
            )
            logger.info("fund_flow: fetched %d rows in %.2fs", len(fund_data), fund_outcome.elapsed)
        except Exception as exc:
            fund_outcome = _FetchOutcome(
                data=pd.DataFrame(),
                elapsed=time.perf_counter() - fund_started,
                error=exc,
            )
            logger.warning("fund_flow: fetch failed after %.2fs: %s", fund_outcome.elapsed, exc)

    if fund_outcome and not fund_outcome.error and not fund_outcome.data.empty:
        try:
            results["fund_flow"] = akshare_fund_flow.save(fund_outcome.data, db, dedup=True)
        except Exception as exc:
            logger.warning("fund_flow: save failed: %s", exc)

    concept_outcome = market_outcomes["concept_mapping"]
    if not concept_outcome.error and not concept_outcome.data.empty:
        try:
            results["concept_mapping"] = akshare_concept.save(concept_outcome.data, db)
        except Exception as exc:
            logger.warning("concept_mapping: save failed: %s", exc)

    # 新闻日常采集并发拉取；批量回填跳过，避免股票数 × 日期数的请求爆炸。
    if mode != "backfill":
        try:
            news_codes = _get_news_codes(trade_date, db)
            combined = _fetch_news_parallel(news_codes, trade_date) if news_codes else pd.DataFrame()
            if not combined.empty:
                results["news"] = akshare_news.save(combined, db)
            logger.info("news: %d rows (%d stocks)", results["news"], len(news_codes))
        except Exception as exc:
            logger.warning("news: %s", exc)

    # ── 聚合：market_emotion ──
    try:
        _aggregate_market_emotion(trade_date, db, use_live=is_live_date)
        results["market_emotion"] = 1
        logger.info("market_emotion: aggregated")
    except Exception as e:
        results["market_emotion"] = 0
        logger.warning("market_emotion: %s", e)

    # ── 聚合：concept_daily ──
    try:
        results["concept_daily"] = _aggregate_concept_daily(trade_date, db)
        logger.info("concept_daily: %d rows", results["concept_daily"])
    except Exception as e:
        results["concept_daily"] = 0
        logger.warning("concept_daily: %s", e)

    total = sum(results.values())
    logger.info(
        "Total: %d rows from %d sources in %.2fs",
        total, len(results), time.perf_counter() - started,
    )
    return results


def _aggregate_market_emotion(trade_date: str, db: Storage, use_live: bool = True) -> None:
    """聚合市场情绪 — 优先 stock_market_activity_legu 直取，回退 DB 聚合。

    stock_market_activity_legu (乐股源) 提供：真实涨停/跌停数、活跃度，
    比从 daily_price 计算更准确，且不依赖 spot_em 全量数据。
    """
    zt_count, dt_count, activity, up_count, down_count = 0, 0, "0%", 0, 0

    # 乐股只提供当前市场状态，历史回填必须完全依赖目标日 DB 数据。
    if use_live:
        try:
            ma_df = ak.stock_market_activity_legu()
            if ma_df is not None and not ma_df.empty:
                data = dict(zip(ma_df["item"], ma_df["value"]))
                zt_count = int(data.get("真实涨停", 0) or 0)
                dt_count = int(data.get("真实跌停", 0) or 0)
                activity = str(data.get("活跃度", "0%"))
                up_count = int(data.get("上涨", 0) or 0)
                down_count = int(data.get("下跌", 0) or 0)
                logger.info("market_emotion: 乐股源 zt=%d dt=%d activity=%s", zt_count, dt_count, activity)
        except Exception as e:
            logger.warning("stock_market_activity_legu 失败，回退 DB 聚合: %s", e)

    # 回退：从 DB zt_pool 聚合
    if zt_count == 0 and dt_count == 0:
        try:
            zt_df = db.query("zt_pool", datetime(2099, 1, 1), where="trade_date = ?", params=(trade_date,))
            zb_df = db.query("zb_pool", datetime(2099, 1, 1), where="trade_date = ?", params=(trade_date,))
            zt_count = len(zt_df) if not zt_df.empty else 0
            zb_count = len(zb_df) if not zb_df.empty else 0
            logger.info("market_emotion: DB 回退 zt=%d zb=%d", zt_count, zb_count)
        except Exception as e:
            logger.warning("market_emotion DB 回退也失败: %s", e)

    # 最高连板：从 zt_pool 获取
    highest_board = 0
    try:
        zt_df = db.query("zt_pool", datetime(2099, 1, 1), where="trade_date = ?", params=(trade_date,))
        if not zt_df.empty and "consecutive_zt" in zt_df.columns:
            highest_board = int(zt_df["consecutive_zt"].max())
    except Exception:
        pass

    sentiment_level = _classify_sentiment(zt_count, dt_count, highest_board)

    emotion_df = pd.DataFrame([{
        "trade_date": trade_date,
        "zt_count": zt_count,
        "dt_count": dt_count,
        "up_count": up_count,
        "down_count": down_count,
        "highest_board": highest_board,
        "activity": activity,
        "sentiment_level": sentiment_level,
    }])
    db.insert("market_emotion", emotion_df, dedup=True)


def _get_news_codes(trade_date: str, db: Storage) -> list[str]:
    """获取当日需要拉新闻的股票代码（涨停+龙虎榜）。

    不含 strong_pool（300+只太多，新闻接口限流）。
    """
    try:
        conn = db._get_conn()
        codes = []
        for table in ["zt_pool", "lhb_detail"]:
            try:
                rows = conn.execute(
                    f"SELECT DISTINCT stock_code FROM {table} WHERE trade_date = ?",
                    (trade_date,),
                ).fetchall()
                codes.extend([r[0] for r in rows])
            except Exception:
                pass
        conn.close()
        return list(dict.fromkeys(codes))  # 去重保序
    except Exception:
        return []


def _classify_sentiment(zt_count: int, dt_count: int, highest_board: int) -> str:
    """根据涨停数和最高板数分类市场情绪。"""
    if zt_count > 100 or highest_board >= 8:
        return "extreme_greed"
    elif zt_count > 60 or highest_board >= 5:
        return "greed"
    elif zt_count > 30:
        return "neutral"
    elif zt_count > 10:
        return "fear"
    else:
        return "extreme_fear"


def _aggregate_concept_daily(trade_date: str, db: Storage) -> int:
    """从 zt_pool + concept_mapping 聚合每个概念当日的涨停情况。"""
    zt_df = db.query(
        "zt_pool",
        datetime(2099, 1, 1),
        where="trade_date = ?",
        params=(trade_date,),
    )
    concept_df = db.query("concept_mapping", datetime(2099, 1, 1))

    if zt_df.empty or concept_df.empty:
        return 0

    # 合并涨停池和概念映射
    merged = zt_df.merge(concept_df, on="stock_code", how="inner")
    if merged.empty:
        return 0

    # 按概念聚合
    concept_stats = merged.groupby("concept_name").agg(
        zt_count=("stock_code", "count"),
        leader_code=("stock_code", "first"),
    ).reset_index()

    # 找每个概念中连板最高的作为龙头
    if "consecutive_zt" in merged.columns:
        leaders = merged.loc[
            merged.groupby("concept_name")["consecutive_zt"].idxmax()
        ][["concept_name", "stock_code", "consecutive_zt"]]
        leaders.columns = ["concept_name", "leader_code", "leader_consecutive"]
        concept_stats = concept_stats.drop(columns=["leader_code"], errors="ignore")
        concept_stats = concept_stats.merge(leaders, on="concept_name", how="left")

    concept_stats["trade_date"] = trade_date
    # 确保列存在
    for col in ["zt_count", "leader_consecutive"]:
        if col not in concept_stats.columns:
            concept_stats[col] = 0

    result = concept_stats[[
        "concept_name", "trade_date", "zt_count",
        "leader_code", "leader_consecutive",
    ]]
    return db.insert("concept_daily", result, dedup=True)
