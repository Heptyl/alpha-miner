"""叙事速度 — 股票级叙事因子。

新闻数量3日变化率，衡量叙事传播加速度。
"""

from datetime import datetime, timedelta

import pandas as pd

from src.data.storage import Storage
from src.factors.base import BaseFactor, dedup_latest


class NarrativeVelocityFactor(BaseFactor):
    name = "narrative_velocity"
    factor_type = "stock"
    description = "新闻数量3日变化率 — 叙事传播加速度"
    lookback_days = 5

    def compute(self, universe: list[str], as_of: datetime, db: Storage) -> pd.Series:
        date_str = as_of.strftime("%Y-%m-%d")
        prev_date = (as_of - timedelta(days=3)).strftime("%Y-%m-%d")

        # 当日新闻（news 表用 publish_time，不是 trade_date）
        news_today = db.query(
            "news",
            as_of,
            where="publish_time LIKE ?",
            params=(f"{date_str}%",),
        )

        # 3天前新闻
        news_prev = db.query(
            "news",
            as_of,
            where="publish_time LIKE ?",
            params=(f"{prev_date}%",),
        )

        news_today = dedup_latest(news_today, key_cols=("stock_code", "title"), time_col="publish_time")
        news_prev = dedup_latest(news_prev, key_cols=("stock_code", "title"), time_col="publish_time")
        self.validate_no_future(as_of, news_today, date_col="publish_time")
        self.validate_no_future(as_of, news_prev, date_col="publish_time")

        # 统计每个股票的新闻数
        today_counts = {}
        if not news_today.empty and "stock_code" in news_today.columns:
            today_counts = news_today.groupby("stock_code").size().to_dict()

        prev_counts = {}
        if not news_prev.empty and "stock_code" in news_prev.columns:
            prev_counts = news_prev.groupby("stock_code").size().to_dict()

        result = {}
        for code in universe:
            t = today_counts.get(code, 0)
            p = prev_counts.get(code, 0)
            if p > 0:
                velocity = (t - p) / p  # 变化率
            elif t > 0:
                velocity = 1.0  # 从0到有 → 最大加速度
            else:
                velocity = 0.0
            # 归一化到 [-1, 1]
            result[code] = max(min(velocity, 1.0), -1.0)

        return pd.Series(result, index=universe, name=self.name)
