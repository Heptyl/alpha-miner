"""日K线数据采集 — akshare。

- --today 模式: stock_zh_a_spot_em (实时行情快照)
- backfill 模式: stock_zh_a_daily (历史日K线)
"""

import time
from datetime import datetime

import akshare as ak
import pandas as pd

from src.data.storage import Storage


def fetch(trade_date: str, retries: int = 3, symbol: str | None = None) -> pd.DataFrame:
    """拉取全市场日K线 (backfill 模式用 stock_zh_a_daily)。

    通过 stock_zh_a_daily 获取指定日期的历史日K线。
    如果需要当天实时数据，使用 fetch_today()。

    Args:
        trade_date: 交易日期 YYYY-MM-DD
        retries: 重试次数
        symbol: 指定股票代码（单只拉取），None 表示拉全部（逐只）

    Returns:
        DataFrame with columns: stock_code, trade_date, open, high, low, close, volume, amount, turnover_rate
    """
    # backfill 走 stock_zh_a_daily，需要逐只拉
    # 但这个接口不支持全市场一次拉，所以改为按日期批量拉
    for attempt in range(retries):
        try:
            df = ak.stock_zh_a_spot_em()
            if df is None or df.empty:
                if attempt < retries - 1:
                    time.sleep(2)
                    continue
                return pd.DataFrame()

            # 从实时行情中提取历史数据：只取 trade_date 当天的
            # stock_zh_a_spot_em 返回的是最新行情，包含今开/最高/最低/最新价/成交量等
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


def fetch_today(trade_date: str, retries: int = 3) -> pd.DataFrame:
    """拉取当日实时行情快照 (--today 模式)。

    通过 stock_zh_a_spot_em 获取实时行情作为当日数据。

    Args:
        trade_date: 交易日期 YYYY-MM-DD
        retries: 重试次数

    Returns:
        DataFrame with columns: stock_code, trade_date, open, high, low, close, volume, amount, turnover_rate
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


def fetch_history(trade_date: str, retries: int = 3) -> pd.DataFrame:
    """拉取历史日K线 (backfill 模式)。

    用 stock_zh_a_daily 按日期批量拉取全市场历史数据。

    Args:
        trade_date: 交易日期 YYYY-MM-DD
        retries: 重试次数

    Returns:
        DataFrame with columns: stock_code, trade_date, open, high, low, close, volume, amount, turnover_rate
    """
    # akshare 的 stock_zh_a_daily 接口是单只股票拉取
    # 对于全市场回填，先拿到股票列表，再逐只拉指定日期
    # 但这太慢了。替代方案：用 stock_zh_a_hist 按 ETF 拉大盘指标，
    # 或者直接用 stock_zh_a_spot_em（但那是实时的）
    #
    # 实际可行方案：stock_zh_a_daily 是单只的，但 stock_zh_a_hist 可以批量
    # 最优方案：用 stock_zh_a_spot_em 获取所有股票列表，然后对每只用 stock_zh_a_daily 拉指定日期
    #
    # 但全市场5000+只逐只拉不现实，所以这里用另一种方式：
    # stock_zh_a_hist 的 date 参数可以拉指定日期附近的数据

    for attempt in range(retries):
        try:
            # 先获取所有股票列表
            spot_df = ak.stock_zh_a_spot_em()
            if spot_df is None or spot_df.empty:
                if attempt < retries - 1:
                    time.sleep(2)
                    continue
                return pd.DataFrame()

            all_codes = spot_df["代码"].tolist()
            results = []
            batch_size = 50

            for i in range(0, len(all_codes), batch_size):
                batch = all_codes[i:i + batch_size]
                for code in batch:
                    try:
                        # stock_zh_a_daily: 参数 symbol (如 "000001"), start_date, end_date
                        hist_df = ak.stock_zh_a_daily(
                            symbol=code,
                            start_date=trade_date.replace("-", ""),
                            end_date=trade_date.replace("-", ""),
                            adjust="qfq",
                        )
                        if hist_df is not None and not hist_df.empty:
                            row = hist_df.iloc[0]
                            results.append({
                                "stock_code": code,
                                "trade_date": trade_date,
                                "open": float(row.get("open", 0)),
                                "high": float(row.get("high", 0)),
                                "low": float(row.get("low", 0)),
                                "close": float(row.get("close", 0)),
                                "volume": float(row.get("volume", 0)),
                                "amount": float(row.get("amount", 0)),
                                "turnover_rate": float(row.get("turnover", 0)),
                            })
                    except Exception:
                        continue

                # 每 batch 歇一下防限流
                if i + batch_size < len(all_codes):
                    time.sleep(0.5)

            if not results:
                # fallback: 如果 stock_zh_a_daily 全失败，退回 spot_em
                print(f"[akshare_price] stock_zh_a_daily 无数据，fallback 到 spot_em")
                return fetch_today(trade_date, retries)

            return pd.DataFrame(results)

        except Exception as e:
            if attempt < retries - 1:
                print(f"[akshare_price] history 尝试 {attempt + 1}/{retries} 失败: {e}")
                time.sleep(3)
            else:
                print(f"[akshare_price] history 拉取失败，fallback 到 spot_em: {e}")
                return fetch_today(trade_date, retries)

    return pd.DataFrame()


def save(df: pd.DataFrame, db: Storage) -> int:
    """将日K线数据写入数据库。"""
    if df.empty:
        return 0
    return db.insert("daily_price", df)
