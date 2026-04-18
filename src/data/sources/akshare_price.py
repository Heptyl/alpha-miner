"""日K线数据采集 — akshare stock_zh_a_spot_em。"""

import time
from datetime import datetime

import akshare as ak
import pandas as pd

from src.data.storage import Storage


def fetch(trade_date: str, retries: int = 3) -> pd.DataFrame:
    """拉取全市场日K线。

    通过 stock_zh_a_spot_em 获取实时行情作为当日数据。

    Args:
        trade_date: 交易日期 YYYY-MM-DD
        retries: 重试次数

    Returns:
        DataFrame with columns: stock_code, trade_date, open, high, low, close, volume, amount, turnover_rate
        非交易日或网络故障返回空 DataFrame。
    """
    for attempt in range(retries):
        try:
            df = ak.stock_zh_a_spot_em()
            if df is None or df.empty:
                if attempt < retries - 1:
                    time.sleep(2)
                    continue
                return pd.DataFrame()

            result = pd.DataFrame({
                "stock_code": df["代码"].values,
                "trade_date": trade_date,
                "open": pd.to_numeric(df["今开"], errors="coerce").values,
                "high": pd.to_numeric(df["最高"], errors="coerce").values,
                "low": pd.to_numeric(df["最低"], errors="coerce").values,
                "close": pd.to_numeric(df["最新价"], errors="coerce").values,
                "volume": pd.to_numeric(df["成交量"], errors="coerce").values,
                "amount": pd.to_numeric(df["成交额"], errors="coerce").values,
                "turnover_rate": pd.to_numeric(df["换手率"], errors="coerce").values,
            })

            # 过滤无效数据
            result = result.dropna(subset=["close"])
            result = result[result["close"] > 0]
            return result

        except Exception as e:
            if attempt < retries - 1:
                print(f"[akshare_price] 尝试 {attempt + 1}/{retries} 失败: {e}")
                time.sleep(3)
            else:
                print(f"[akshare_price] 拉取失败: {e}")
                return pd.DataFrame()

    return pd.DataFrame()


def save(df: pd.DataFrame, db: Storage) -> int:
    """将日K线数据写入数据库。"""
    if df.empty:
        return 0
    return db.insert("daily_price", df)
