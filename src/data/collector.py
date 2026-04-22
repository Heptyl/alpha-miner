"""数据采集调度器 — 统一调用各数据源，单源失败不影响整体。

采集完成后自动聚合：
- market_emotion：涨停数、跌停数、最高板、情绪级别（乐股源直取）
- concept_daily：每个概念当日涨停数、龙头等
"""

import logging
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
        print(f"  [OK] zt_pool: {count} rows")
    except Exception as e:
        results["zt_pool"] = 0
        print(f"  [FAIL] zt_pool: {e}")

    try:
        df = akshare_zt_pool.fetch_zb_pool(trade_date)
        count = akshare_zt_pool.save_zb_pool(df, db)
        results["zb_pool"] = count
        print(f"  [OK] zb_pool: {count} rows")
    except Exception as e:
        results["zb_pool"] = 0
        print(f"  [FAIL] zb_pool: {e}")

    try:
        df = akshare_zt_pool.fetch_strong_pool(trade_date)
        count = akshare_zt_pool.save_strong_pool(df, db)
        results["strong_pool"] = count
        print(f"  [OK] strong_pool: {count} rows")
    except Exception as e:
        results["strong_pool"] = 0
        print(f"  [FAIL] strong_pool: {e}")

    try:
        df = akshare_lhb.fetch(trade_date)
        count = akshare_lhb.save(df, db)
        results["lhb_detail"] = count
        print(f"  [OK] lhb_detail: {count} rows")
    except Exception as e:
        results["lhb_detail"] = 0
        print(f"  [FAIL] lhb_detail: {e}")

    # 1. 日K线 — today 模式只拉重点股票(涨停+强势+龙虎榜), backfill 模式全量
    try:
        if mode == "backfill":
            df = akshare_price.fetch_history(trade_date)
        else:
            df = akshare_price.fetch_today(trade_date)
        count = akshare_price.save(df, db)
        results["daily_price"] = count
        print(f"  [OK] daily_price: {count} rows")
    except Exception as e:
        results["daily_price"] = 0
        print(f"  [FAIL] daily_price: {e}")

    # 6. 资金流向
    try:
        df = akshare_fund_flow.fetch(trade_date)
        count = akshare_fund_flow.save(df, db)
        results["fund_flow"] = count
        print(f"  [OK] fund_flow: {count} rows")
    except Exception as e:
        results["fund_flow"] = 0
        print(f"  [FAIL] fund_flow: {e}")

    # 7. 概念映射（不稳定，频率低，可以不是每天都更新）
    try:
        df = akshare_concept.fetch(trade_date, db=db)
        if not df.empty:
            count = akshare_concept.save(df, db)
            results["concept_mapping"] = count
            print(f"  [OK] concept_mapping: {count} rows")
        else:
            results["concept_mapping"] = 0
            print(f"  [SKIP] concept_mapping: empty")
    except Exception as e:
        results["concept_mapping"] = 0
        print(f"  [FAIL] concept_mapping: {e}")

    # ── 聚合：market_emotion ──
    try:
        _aggregate_market_emotion(trade_date, db)
        results["market_emotion"] = 1
        print(f"  [OK] market_emotion: aggregated")
    except Exception as e:
        results["market_emotion"] = 0
        print(f"  [FAIL] market_emotion: {e}")

    # ── 聚合：concept_daily ──
    try:
        _aggregate_concept_daily(trade_date, db)
        results["concept_daily"] = 1
        print(f"  [OK] concept_daily: aggregated")
    except Exception as e:
        results["concept_daily"] = 0
        print(f"  [FAIL] concept_daily: {e}")

    total = sum(results.values())
    print(f"  Total: {total} from {len(results)} sources")
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
    db.insert("market_emotion", emotion_df)


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
    db.insert("concept_daily", result)
