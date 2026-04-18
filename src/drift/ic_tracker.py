"""IC 追踪器 — 因子有效性追踪。

每日截面 Spearman(因子值, 未来收益) → 滚动 IC/ICIR/胜率/盈亏比。
"""

from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from scipy import stats as scipy_stats

from src.data.storage import Storage


class ICTracker:
    """追踪因子的 IC (Information Coefficient) 时序。"""

    def __init__(self, db: Storage):
        self.db = db

    def _get_factor_values(self, factor_name: str, date: str) -> pd.Series:
        """获取某日某因子的全截面值。"""
        df = self.db.query(
            "factor_values",
            datetime.strptime(date, "%Y-%m-%d"),
            where="factor_name = ? AND trade_date = ?",
            params=(factor_name, date),
        )
        if df.empty:
            return pd.Series(dtype=float)
        return df.set_index("stock_code")["factor_value"]

    def _get_forward_returns(self, date: str, forward_days: int) -> pd.Series:
        """获取某日起未来 N 天收益率。"""
        current = self.db.query(
            "daily_price",
            datetime.strptime(date, "%Y-%m-%d"),
            where="trade_date = ?",
            params=(date,),
        )
        if current.empty:
            return pd.Series(dtype=float)

        future_date = (datetime.strptime(date, "%Y-%m-%d") + timedelta(days=forward_days * 2)).strftime("%Y-%m-%d")
        future = self.db.query(
            "daily_price",
            datetime.strptime(future_date, "%Y-%m-%d"),
            where="trade_date > ?",
            params=(date,),
        )
        if future.empty:
            return pd.Series(dtype=float)

        # 取每个股票在 date 之后的第 forward_days 个交易日
        future_sorted = future[future["trade_date"] > date].sort_values(["stock_code", "trade_date"])

        # 计算 forward return
        current_prices = current.set_index("stock_code")["close"]
        returns = {}
        for code in current_prices.index:
            stock_future = future_sorted[future_sorted["stock_code"] == code]
            if len(stock_future) >= forward_days:
                future_close = float(stock_future.iloc[forward_days - 1]["close"])
                current_close = float(current_prices[code])
                if current_close > 0:
                    returns[code] = (future_close - current_close) / current_close

        return pd.Series(returns)

    def _compute_spearman_ic(self, factor_values: pd.Series, forward_returns: pd.Series) -> float:
        """计算截面 Spearman IC。"""
        common = factor_values.index.intersection(forward_returns.index)
        if len(common) < 5:
            return np.nan
        fv = factor_values.loc[common].astype(float)
        fr = forward_returns.loc[common].astype(float)
        # 去掉 NaN
        mask = fv.notna() & fr.notna()
        fv = fv[mask]
        fr = fr[mask]
        if len(fv) < 5:
            return np.nan
        corr, _ = scipy_stats.spearmanr(fv, fr)
        return float(corr) if not np.isnan(corr) else np.nan

    def compute_ic_series(
        self,
        factor_name: str,
        start_date: str,
        end_date: str,
        forward_days: int = 1,
        window: int = 20,
    ) -> pd.DataFrame:
        """计算因子 IC 时序。

        Returns:
            DataFrame with columns: [date, ic, ic_ma, icir, win_rate, pnl_ratio]
        """
        # 获取日期范围
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")

        # 从 daily_price 获取交易日历
        price_df = self.db.query_range("daily_price", end, lookback_days=(end - start).days + 10)
        if price_df.empty:
            return pd.DataFrame()

        trade_dates = sorted(price_df["trade_date"].unique())
        trade_dates = [d for d in trade_dates if start_date <= d <= end_date]

        ic_list = []
        for date in trade_dates:
            fv = self._get_factor_values(factor_name, date)
            fr = self._get_forward_returns(date, forward_days)
            ic = self._compute_spearman_ic(fv, fr)
            ic_list.append({"date": date, "ic": ic})

        ic_df = pd.DataFrame(ic_list)
        if ic_df.empty:
            return ic_df

        # 滚动统计
        ic_series = ic_df["ic"]
        ic_df["ic_ma"] = ic_series.rolling(window, min_periods=1).mean()
        ic_df["icir"] = ic_series.rolling(window, min_periods=1).mean() / ic_series.rolling(window, min_periods=1).std().replace(0, np.nan)
        ic_df["win_rate"] = ic_series.rolling(window, min_periods=1).apply(lambda x: (x > 0).sum() / len(x), raw=True)
        # 盈亏比：IC>0均值 / |IC<0均值|
        ic_df["pnl_ratio"] = ic_series.rolling(window, min_periods=1).apply(
            lambda x: x[x > 0].mean() / abs(x[x < 0].mean()) if len(x[x < 0]) > 0 and x[x < 0].mean() != 0 else np.nan,
            raw=True,
        )

        return ic_df

    def current_status(self, factor_name: str, window: int = 20) -> dict:
        """返回因子当前 IC 状态摘要。"""
        # 取最近 window*2 天的数据
        end = datetime.now()
        start = end - timedelta(days=window * 3)
        ic_df = self.compute_ic_series(
            factor_name,
            start.strftime("%Y-%m-%d"),
            end.strftime("%Y-%m-%d"),
            forward_days=1,
            window=window,
        )
        if ic_df.empty:
            return {
                "factor_name": factor_name,
                "latest_ic": np.nan,
                "ic_avg": np.nan,
                "icir": np.nan,
                "win_rate": np.nan,
                "trend": "unknown",
                "status": "no_data",
            }

        latest = ic_df.iloc[-1]
        latest_ic = float(latest["ic"]) if not pd.isna(latest["ic"]) else np.nan
        ic_avg = float(latest["ic_ma"]) if not pd.isna(latest["ic_ma"]) else np.nan
        icir = float(latest["icir"]) if not pd.isna(latest["icir"]) else np.nan
        win_rate = float(latest["win_rate"]) if not pd.isna(latest["win_rate"]) else np.nan

        # 趋势判断：最近5个IC的斜率
        recent_ics = ic_df["ic"].dropna().tail(5).values
        if len(recent_ics) >= 3:
            slope = np.polyfit(range(len(recent_ics)), recent_ics, 1)[0]
            if slope > 0.01:
                trend = "improving"
            elif slope < -0.01:
                trend = "declining"
            else:
                trend = "stable"
        else:
            trend = "unknown"

        # 状态判断
        if pd.isna(ic_avg):
            status = "no_data"
        elif abs(ic_avg) < 0.02:
            status = "dead"
        elif ic_avg > 0.03 and win_rate > 0.5:
            status = "healthy"
        elif ic_avg > 0.02:
            status = "weak"
        else:
            status = "negative"

        return {
            "factor_name": factor_name,
            "latest_ic": latest_ic,
            "ic_avg": ic_avg,
            "icir": icir,
            "win_rate": win_rate,
            "trend": trend,
            "status": status,
        }
