"""数据采集调度器 — 统一调用各数据源，单源失败不影响整体。

采集完成后自动聚合：
- market_emotion：涨停数、跌停数、最高板、情绪级别（乐股源直取）
- concept_daily：每个概念当日涨停数、龙头等
"""

import logging
import time
from datetime import datetime
from typing import Optional

import akshare as ak
import pandas as pd

from src.data.storage import Storage
from src.data.sources import (
    akshare_price,
    akshare_zt_pool,
    akshare_lhb,
    akshare_fund_flow,
    akshare_concept,
    akshare_news,
)

logger = logging.getLogger(__name__)


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
    if db is None:
        db = Storage()
        db.init_db()

    results = {}

    # 0. 涨停池、炸板池、强势股、龙虎榜（轻量接口，先采集）
    #    daily_price 需要从这些表获取重点股票代码
    try:
        df = akshare_zt_pool.fetch_zt_pool(trade_date)
        count = akshare_zt_pool.save_zt_pool(df, db)
        results["zt_pool"] = count
        logger.info("zt_pool: %d rows", count)
    except Exception as e:
        results["zt_pool"] = 0
        logger.warning("zt_pool: %s", e)

    try:
        df = akshare_zt_pool.fetch_zb_pool(trade_date)
        count = akshare_zt_pool.save_zb_pool(df, db)
        results["zb_pool"] = count
        logger.info("zb_pool: %d rows", count)
    except Exception as e:
        results["zb_pool"] = 0
        logger.warning("zb_pool: %s", e)

    try:
        df = akshare_zt_pool.fetch_strong_pool(trade_date)
        count = akshare_zt_pool.save_strong_pool(df, db)
        results["strong_pool"] = count
        logger.info("strong_pool: %d rows", count)
    except Exception as e:
        results["strong_pool"] = 0
        logger.warning("strong_pool: %s", e)

    try:
        df = akshare_lhb.fetch(trade_date)
        count = akshare_lhb.save(df, db)
        results["lhb_detail"] = count
        logger.info("lhb_detail: %d rows", count)
    except Exception as e:
        results["lhb_detail"] = 0
        logger.warning("lhb_detail: %s", e)

    # 1. 日K线 — today 模式只拉重点股票(涨停+强势+龙虎榜), backfill 模式全量
    try:
        if mode == "backfill":
            df = akshare_price.fetch_history(trade_date)
        else:
            df = akshare_price.fetch_today(trade_date)
        count = akshare_price.save(df, db, dedup=True)
        results["daily_price"] = count
        logger.info("daily_price: %d rows", count)
    except Exception as e:
        results["daily_price"] = 0
        logger.warning("daily_price: %s", e)

    # 6. 资金流向
    try:
        df = akshare_fund_flow.fetch(trade_date)
        count = akshare_fund_flow.save(df, db, dedup=True)
        results["fund_flow"] = count
        logger.info("fund_flow: %d rows", count)
    except Exception as e:
        results["fund_flow"] = 0
        logger.warning("fund_flow: %s", e)

    # 6b. 个股新闻 — 拉涨停+强势股的新闻（限流 0.5s/只）
    try:
        news_codes = _get_news_codes(trade_date, db)
        if news_codes:
            all_news = []
            for code in news_codes:
                try:
                    df = akshare_news.fetch(stock_code=code, trade_date=trade_date)
                    if not df.empty:
                        all_news.append(df)
                except Exception:
                    pass
                time.sleep(0.5)
            if all_news:
                combined = pd.concat(all_news, ignore_index=True)
                # 去重：同 title+publish_time 只保留一条
                combined = combined.drop_duplicates(subset=["news_id"], keep="first")
                count = akshare_news.save(combined, db)
                results["news"] = count
                logger.info("news: %d rows (%d stocks)", count, len(news_codes))
            else:
                results["news"] = 0
                logger.info("news: 无当日新闻")
        else:
            results["news"] = 0
            logger.info("news: 无重点股票代码")
    except Exception as e:
        results["news"] = 0
        logger.warning("news: %s", e)

    # 7. 概念映射（不稳定，频率低，可以不是每天都更新）
    try:
        df = akshare_concept.fetch(trade_date, db=db)
        if not df.empty:
            count = akshare_concept.save(df, db)
            results["concept_mapping"] = count
            logger.info("concept_mapping: %d rows", count)
        else:
            results["concept_mapping"] = 0
            logger.info("concept_mapping: empty")
    except Exception as e:
        results["concept_mapping"] = 0
        logger.warning("concept_mapping: %s", e)

    # ── 聚合：market_emotion ──
    try:
        _aggregate_market_emotion(trade_date, db)
        results["market_emotion"] = 1
        logger.info("market_emotion: aggregated")
    except Exception as e:
        results["market_emotion"] = 0
        logger.warning("market_emotion: %s", e)

    # ── 聚合：concept_daily ──
    try:
        _aggregate_concept_daily(trade_date, db)
        results["concept_daily"] = 1
        logger.info("concept_daily: aggregated")
    except Exception as e:
        results["concept_daily"] = 0
        logger.warning("concept_daily: %s", e)

    total = sum(results.values())
    logger.info("Total: %d from %d sources", total, len(results))
    return results


def _aggregate_market_emotion(trade_date: str, db: Storage) -> None:
    """聚合市场情绪 — 优先 stock_market_activity_legu 直取，回退 DB 聚合。

    stock_market_activity_legu (乐股源) 提供：真实涨停/跌停数、活跃度，
    比从 daily_price 计算更准确，且不依赖 spot_em 全量数据。
    """
    zt_count, dt_count, activity, up_count, down_count = 0, 0, "0%", 0, 0

    # 主源：乐股直取
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


def _aggregate_concept_daily(trade_date: str, db: Storage) -> None:
    """从 zt_pool + concept_mapping 聚合每个概念当日的涨停情况。"""
    zt_df = db.query(
        "zt_pool",
        datetime(2099, 1, 1),
        where="trade_date = ?",
        params=(trade_date,),
    )
    concept_df = db.query("concept_mapping", datetime(2099, 1, 1))

    if zt_df.empty or concept_df.empty:
        return

    # 合并涨停池和概念映射
    merged = zt_df.merge(concept_df, on="stock_code", how="inner")
    if merged.empty:
        return

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
    db.insert("concept_daily", result, dedup=True)
