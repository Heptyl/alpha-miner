"""资金流向数据采集 — akshare stock_individual_fund_flow_rank。"""

import time
from datetime import datetime

import akshare as ak
import pandas as pd

from src.data.storage import Storage


def fetch(trade_date: str, retries: int = 3) -> pd.DataFrame:
    """拉取全市场资金流向排名。

    Args:
        trade_date: 交易日期 YYYY-MM-DD

    Returns:
        DataFrame: stock_code, trade_date, super_large_net, large_net,
                   medium_net, small_net, main_net
    """
    for attempt in range(retries):
        try:
            df = ak.stock_individual_fund_flow_rank(indicator="今日")
            if df is None or df.empty:
                return pd.DataFrame()

            result = pd.DataFrame()
            result["stock_code"] = df["代码"].values if "代码" in df.columns else []
            result["trade_date"] = trade_date

            col_mapping = {
                "超大单净流入": "super_large_net",
                "大单净流入": "large_net",
                "中单净流入": "medium_net",
                "小单净流入": "small_net",
                "主力净流入": "main_net",
            }

            for col_src, col_dst in col_mapping.items():
                if col_src in df.columns:
                    result[col_dst] = pd.to_numeric(df[col_src], errors="coerce").fillna(0).values
                else:
                    result[col_dst] = 0.0

            return result

        except Exception as e:
            if attempt < retries - 1:
                print(f"[fund_flow] 尝试 {attempt + 1}/{retries} 失败: {e}")
                time.sleep(3)
            else:
                print(f"[fund_flow] 拉取失败: {e}")
                return pd.DataFrame()

    return pd.DataFrame()


def save(df: pd.DataFrame, db: Storage) -> int:
    """将资金流向数据写入数据库。"""
    if df.empty:
        return 0
    return db.insert("fund_flow", df)
