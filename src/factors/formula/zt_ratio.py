"""涨停/跌停比率 — 市场级因子。

涨停数 / (涨停数 + 跌停数)，衡量市场整体多空力量。
"""

from datetime import datetime

import pandas as pd

from src.data.storage import Storage
from src.factors.base import BaseFactor


class ZtDtRatioFactor(BaseFactor):
    name = "zt_dt_ratio"
    factor_type = "market"
    description = "涨停/(涨停+跌停) — 市场多空力量比"
    lookback_days = 1

    def compute(self, universe: list[str], as_of: datetime, db: Storage) -> pd.Series:
        zt_df = db.query(
            "zt_pool",
            as_of,
            where="trade_date <= ?",
            params=(as_of.strftime("%Y-%m-%d"),),
        )
        # 跌停数从 daily_price 推算（跌幅 > 9.5%）
        daily_df = db.query_range(
            "daily_price",
            as_of,
            lookback_days=1,
        )
        self.validate_no_future(as_of, zt_df)
        self.validate_no_future(as_of, daily_df, date_col="trade_date")

        zt_count = len(zt_df[zt_df["trade_date"] == as_of.strftime("%Y-%m-%d")]) if not zt_df.empty else 0
        dt_count = 0
        if not daily_df.empty:
            today_str = as_of.strftime("%Y-%m-%d")
            today_data = daily_df[daily_df["trade_date"] == today_str]
            if not today_data.empty and "open" in today_data.columns and "close" in today_data.columns:
                # 跌停判断：跌幅 < -9.5%（排除 ST 后改为 -4.5%）
                pct_change = (today_data["close"] - today_data["open"]) / today_data["open"] * 100
                dt_count = int((pct_change < -9.5).sum())

        denom = zt_count + dt_count
        ratio = zt_count / denom if denom > 0 else 0.5

        return pd.Series([ratio], index=["market"], name=self.name)
