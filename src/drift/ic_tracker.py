"""IC 追踪器 — 因子有效性追踪 + 持久化。

每日截面 Spearman(因子值, 未来收益) → 滚动 IC/ICIR/胜率/盈亏比。
计算完成后自动写入 ic_series 表。
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
        conn = self.db._get_conn()
        try:
            sql = """
                SELECT stock_code, factor_value
                FROM factor_values
                WHERE factor_name = ? AND trade_date = ?
                ORDER BY snapshot_time DESC
            """
            df = pd.read_sql_query(sql, conn, params=(factor_name, date))
        finally:
            conn.close()

        if df.empty:
            return pd.Series(dtype=float)
        # 去重：同一股票同一日期只取最新 snapshot
        df = df.drop_duplicates(subset=["stock_code"], keep="first")
        return df.set_index("stock_code")["factor_value"]

    def _get_forward_returns(self, date: str, forward_days: int) -> pd.Series:
        """获取某日起未来 N 天收益率（向量化实现）。"""
        conn = self.db._get_conn()
        try:
            # 获取当日收盘价
            current = pd.read_sql_query(
                "SELECT stock_code, close FROM daily_price WHERE trade_date = ?",
                conn, params=(date,),
            )
            if current.empty:
                return pd.Series(dtype=float)

            # 获取未来交易日列表
            future_dates = [r[0] for r in conn.execute(
                "SELECT DISTINCT trade_date FROM daily_price "
                "WHERE trade_date > ? ORDER BY trade_date LIMIT ?",
                (date, forward_days),
            ).fetchall()]

            if len(future_dates) < forward_days:
                return pd.Series(dtype=float)

            target_date = future_dates[forward_days - 1]

            future = pd.read_sql_query(
                "SELECT stock_code, close FROM daily_price WHERE trade_date = ?",
                conn, params=(target_date,),
            )
        finally:
            conn.close()

        if future.empty:
            return pd.Series(dtype=float)

        current_prices = current.set_index("stock_code")["close"]
        future_prices = future.set_index("stock_code")["close"]

        common = current_prices.index.intersection(future_prices.index)
        valid = current_prices[common] > 0
        common = common[valid]

        returns = (future_prices[common] - current_prices[common]) / current_prices[common]
        return returns

    def _compute_spearman_ic(self, factor_values: pd.Series, forward_returns: pd.Series) -> float:
        """计算截面 Spearman IC。"""
        common = factor_values.index.intersection(forward_returns.index)
        if len(common) < 5:
            return np.nan
        fv = factor_values.loc[common].astype(float)
        fr = forward_returns.loc[common].astype(float)
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
        persist: bool = True,
    ) -> pd.DataFrame:
        """计算因子 IC 时序，可选持久化到 ic_series 表。

        Returns:
            DataFrame with columns: [date, ic, ic_ma, icir, win_rate, pnl_ratio]
        """
        conn = self.db._get_conn()
        try:
            trade_dates = [r[0] for r in conn.execute(
                "SELECT DISTINCT trade_date FROM factor_values "
                "WHERE factor_name = ? AND trade_date >= ? AND trade_date <= ? "
                "ORDER BY trade_date",
                (factor_name, start_date, end_date),
            ).fetchall()]
        finally:
            conn.close()

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
        ic_df["pnl_ratio"] = ic_series.rolling(window, min_periods=1).apply(
            lambda x: x[x > 0].mean() / abs(x[x < 0].mean()) if len(x[x < 0]) > 0 and x[x < 0].mean() != 0 else np.nan,
            raw=True,
        )

        # 持久化到 ic_series 表
        if persist and not ic_df.empty:
            self._persist_ic(factor_name, ic_df, forward_days)

        return ic_df

    def _persist_ic(self, factor_name: str, ic_df: pd.DataFrame, forward_days: int) -> int:
        """将 IC 计算结果写入 ic_series 表。"""
        rows = []
        for _, row in ic_df.iterrows():
            if pd.isna(row["ic"]):
                continue
            rows.append({
                "factor_name": factor_name,
                "trade_date": row["date"],
                "ic_value": float(row["ic"]),
                "forward_days": forward_days,
            })

        if not rows:
            return 0

        df = pd.DataFrame(rows)
        return self.db.insert("ic_series", df, dedup=True)

    def current_status(self, factor_name: str, window: int = 20) -> dict:
        """返回因子当前 IC 状态摘要。"""
        end = datetime.now()
        start = end - timedelta(days=window * 3)
        ic_df = self.compute_ic_series(
            factor_name,
            start.strftime("%Y-%m-%d"),
            end.strftime("%Y-%m-%d"),
            forward_days=1,
            window=window,
            persist=False,
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

        # 趋势判断
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
