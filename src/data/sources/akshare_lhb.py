"""龙虎榜数据采集 — akshare stock_lhb_detail_em。"""

import logging
import time
from datetime import datetime

import akshare as ak
import pandas as pd

from src.data.storage import Storage

logger = logging.getLogger(__name__)


def fetch(trade_date: str, retries: int = 3) -> pd.DataFrame:
    """拉取龙虎榜明细。

    Args:
        trade_date: 交易日期 YYYY-MM-DD

    Returns:
        DataFrame: stock_code, trade_date, buy_amount, sell_amount, net_amount,
                   buy_depart, sell_depart, reason
    """
    date_str = trade_date.replace("-", "")

    for attempt in range(retries):
        try:
            df = ak.stock_lhb_detail_em(
                start_date=date_str,
                end_date=date_str,
            )
            if df is None or df.empty:
                return pd.DataFrame()

            # stock_lhb_detail_em 返回汇总数据（非明细），列名：
            # 代码, 名称, 上榜日, 解读, 收盘价, 涨跌幅,
            # 龙虎榜净买额, 龙虎榜买入额, 龙虎榜卖出额, 龙虎榜成交额, ...
            # 上榜原因, 上榜后N日...
            # 注意：没有买入/卖出营业部明细
            result = pd.DataFrame()
            result["stock_code"] = df["代码"].values if "代码" in df.columns else []
            result["trade_date"] = trade_date

            # 金额字段 — 优先用龙虎榜列名，回退通用列名
            for col_srcs, col_dst in [
                (["龙虎榜买入额", "买入额"], "buy_amount"),
                (["龙虎榜卖出额", "卖出额"], "sell_amount"),
                (["龙虎榜净买额", "净买入额"], "net_amount"),
            ]:
                val = 0.0
                for cs in col_srcs:
                    if cs in df.columns:
                        val = pd.to_numeric(df[cs], errors="coerce").fillna(0).values
                        break
                result[col_dst] = val

            # 上榜原因
            result["reason"] = df["上榜原因"].astype(str).fillna("").values if "上榜原因" in df.columns else ""
            # 汇总接口无营业部明细，留空
            result["buy_depart"] = ""
            result["sell_depart"] = ""

            # 按 (stock_code, reason) 去重 — 同一股票可能因不同原因上榜，保留每条原因
            result = result.drop_duplicates(subset=["stock_code", "reason"], keep="first").reset_index(drop=True)

            return result

        except Exception as e:
            err_str = str(e)
            # akshare 内部 bug: data_json["result"] 为 None（东财返回空）
            if "NoneType" in err_str:
                logger.info("[lhb] 东财返回空数据（可能无龙虎榜或非交易日）")
                return pd.DataFrame()
            if attempt < retries - 1:
                time.sleep(2)
            else:
                logger.warning("[lhb] 拉取失败: %s", e)
                return pd.DataFrame()


def save(df: pd.DataFrame, db: Storage) -> int:
    """将龙虎榜数据写入数据库。"""
    if df.empty:
        return 0
    # 写入前先删当天旧数据，避免重复累积
    if "trade_date" in df.columns:
        dates = df["trade_date"].unique()
        for d in dates:
            db.execute_write("DELETE FROM lhb_detail WHERE trade_date = ?", (d,))
    return db.insert("lhb_detail", df)
