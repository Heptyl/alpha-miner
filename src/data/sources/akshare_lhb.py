"""龙虎榜数据采集 — akshare stock_lhb_detail_em。"""

import time
from datetime import datetime

import akshare as ak
import pandas as pd

from src.data.storage import Storage


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

            # 安全地构造 DataFrame，处理各种 akshare 返回格式
            result = pd.DataFrame()
            result["stock_code"] = df["代码"].values if "代码" in df.columns else []
            result["trade_date"] = trade_date

            for col_src, col_dst in [
                ("买入额", "buy_amount"),
                ("卖出额", "sell_amount"),
                ("净买入额", "net_amount"),
            ]:
                if col_src in df.columns:
                    result[col_dst] = pd.to_numeric(df[col_src], errors="coerce").fillna(0).values
                else:
                    result[col_dst] = 0.0

            for col_src, col_dst in [
                ("买入营业部", "buy_depart"),
                ("卖出营业部", "sell_depart"),
                ("上榜原因", "reason"),
            ]:
                if col_src in df.columns:
                    result[col_dst] = df[col_src].astype(str).fillna("").values
                else:
                    result[col_dst] = ""

            return result

        except Exception as e:
            err_str = str(e)
            # akshare 内部 bug: data_json["result"] 为 None（东财返回空）
            if "NoneType" in err_str:
                print(f"[lhb] 东财返回空数据（可能无龙虎榜或非交易日）")
                return pd.DataFrame()
            if attempt < retries - 1:
                time.sleep(2)
            else:
                print(f"[lhb] 拉取失败: {e}")
                return pd.DataFrame()


def save(df: pd.DataFrame, db: Storage) -> int:
    """将龙虎榜数据写入数据库。"""
    if df.empty:
        return 0
    return db.insert("lhb_detail", df)
