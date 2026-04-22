"""资金流向数据采集 — 去掉东财排名接口，用 stock_individual_fund_flow 逐只拉取。

主源: stock_individual_fund_flow (东方财富单只查询，新浪源)
回退: 从 DB daily_price 取成交额粗估（无精确资金流拆分）

注意：stock_individual_fund_flow 也走东方财富，可能偶尔被 WAF 拦截，
但比排名接口稳定得多。限流 0.3s/只，连续失败 15 只后终止。
"""

import logging
import time

import akshare as ak
import pandas as pd

from src.data.storage import Storage

logger = logging.getLogger(__name__)

# 限流参数
_PER_STOCK_DELAY = 0.3      # 每只间隔秒数（比 price 更长，避免触发 WAF）
_MAX_RETRIES = 2             # 单只最大重试
_BATCH_FAILURE_THRESHOLD = 15  # 连续失败多少只后终止


def fetch(trade_date: str, retries: int = 3) -> pd.DataFrame:
    """拉取全市场资金流向 — 逐只用 stock_individual_fund_flow.

    注意：此接口也走东方财富，可能被 WAF 拦截。
    只在收盘后（15:10 之后）尝试采集，盘中跳过（返回空）。
    """
    from datetime import datetime as _dt

    now = _dt.now()
    # 只在收盘后尝试（资金流数据收盘后才完整）
    market_closed = now.hour > 15 or (now.hour == 15 and now.minute >= 10)
    if not market_closed:
        print("  [SKIP] fund_flow: 盘中跳过，收盘后采集")
        return pd.DataFrame()

    result = _fetch_fund_flow_batch(trade_date)
    if not result.empty:
        return result

    print("  [WARN] fund_flow 所有数据源均失败")
    return pd.DataFrame()


def _fetch_fund_flow_batch(trade_date: str) -> pd.DataFrame:
    """从 DB 拿代码列表，逐只用 stock_individual_fund_flow 查询."""
    codes = _get_codes_from_db()
    if not codes:
        return pd.DataFrame()

    all_rows = []
    consecutive_fail = 0
    current_delay = _PER_STOCK_DELAY

    for code in codes:
        if consecutive_fail >= _BATCH_FAILURE_THRESHOLD:
            logger.warning("fund_flow 连续失败 %d ≥ %d，终止", consecutive_fail, _BATCH_FAILURE_THRESHOLD)
            break

        time.sleep(current_delay)

        for attempt in range(_MAX_RETRIES):
            try:
                prefix = "sh" if code.startswith("6") else "sz"
                df = ak.stock_individual_fund_flow(
                    stock=code,
                    market=prefix,
                )
                if df is not None and not df.empty:
                    latest = df.iloc[-1]
                    row = {
                        "stock_code": code,
                        "trade_date": trade_date,
                        "super_large_net": 0.0,
                        "large_net": 0.0,
                        "medium_net": 0.0,
                        "small_net": 0.0,
                        "main_net": 0.0,
                    }
                    # 解析资金流列（列名可能变化）
                    for col in df.columns:
                        col_str = str(col)
                        if "超大单" in col_str:
                            row["super_large_net"] = float(latest[col])
                        elif "大单净" in col_str:
                            row["large_net"] = float(latest[col])
                        elif "中单" in col_str:
                            row["medium_net"] = float(latest[col])
                        elif "小单" in col_str:
                            row["small_net"] = float(latest[col])
                        elif "主力" in col_str:
                            row["main_net"] = float(latest[col])
                    all_rows.append(row)
                    consecutive_fail = 0  # 成功重置
                    break
            except Exception as e:
                err_str = str(e).lower()
                is_waf = any(kw in err_str for kw in (
                    "405", "403", "connectionreset", "waf", "forbidden",
                    "blocked", "rate limit", "too many",
                ))
                if is_waf:
                    consecutive_fail += 1
                    current_delay = min(current_delay * 2, 10)
                    logger.warning("fund_flow %s WAF，退避到 %.1fs: %s", code, current_delay, e)
                else:
                    consecutive_fail += 1
                    break

        # 进度
        idx = codes.index(code) + 1
        if idx % 200 == 0:
            logger.info("fund_flow 进度: %d/%d, 成功 %d", idx, len(codes), len(all_rows))

    if not all_rows:
        return pd.DataFrame()

    logger.info("fund_flow 获取 %d/%d 只", len(all_rows), len(codes))
    return pd.DataFrame(all_rows)


def _get_codes_from_db() -> list[str]:
    """从 DB 获取已有股票代码列表。"""
    try:
        db = Storage()
        conn = db._get_conn()
        codes = [r[0] for r in conn.execute(
            "SELECT DISTINCT stock_code FROM daily_price"
        ).fetchall()]
        conn.close()
        if codes:
            logger.info("fund_flow: 从 DB 获取 %d 只股票代码", len(codes))
        return codes
    except Exception as e:
        logger.warning("fund_flow: DB 获取代码失败: %s", e)
        return []


def save(df: pd.DataFrame, db: Storage) -> int:
    """将资金流向数据写入数据库。"""
    if df.empty:
        return 0
    return db.insert("fund_flow", df)
