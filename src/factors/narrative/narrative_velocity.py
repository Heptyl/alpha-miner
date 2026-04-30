"""叙事速度 — 股票级叙事因子。

新闻数量3日变化率，衡量叙事传播加速度。
V2: 当 news_type 可用时，使用加权模式（不同类型新闻权重不同）。
"""

from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from src.data.storage import Storage
from src.factors.base import BaseFactor, dedup_latest

# 新闻类型权重（与 NewsClassifier 共享）
_WEIGHTS = {
    "theme_ignite": 3.0,
    "theme_ferment": 1.5,
    "catalyst_real": 2.0,
    "catalyst_expect": 1.0,
    "good_realize": -0.5,
    "negative": -2.0,
    "noise": 0.0,
}


class NarrativeVelocityFactor(BaseFactor):
    name = "narrative_velocity"
    factor_type = "stock"
    description = "新闻加权3日变化率 — 叙事传播加速度（V2: 类型加权）"
    lookback_days = 5

    def compute(self, universe: list[str], as_of: datetime, db: Storage) -> pd.Series:
        date_str = as_of.strftime("%Y-%m-%d")
        prev_date = (as_of - timedelta(days=3)).strftime("%Y-%m-%d")

        # 当日新闻
        news_today = db.query(
            "news", as_of,
            where="publish_time LIKE ?", params=(f"{date_str}%",),
        )
        # 3天前新闻
        news_prev = db.query(
            "news", as_of,
            where="publish_time LIKE ?", params=(f"{prev_date}%",),
        )

        news_today = dedup_latest(news_today, key_cols=("stock_code", "title"), time_col="publish_time")
        news_prev = dedup_latest(news_prev, key_cols=("stock_code", "title"), time_col="publish_time")
        self.validate_no_future(as_of, news_today, date_col="publish_time")
        self.validate_no_future(as_of, news_prev, date_col="publish_time")

        # 检测 news_type 列是否存在且含有非 noise 值（加权模式）
        has_type = False
        if not news_today.empty and "news_type" in news_today.columns:
            non_noise = news_today["news_type"].ne("noise").any()
            has_type = bool(non_noise)

        if has_type:
            today_scores = self._weighted_scores(news_today)
            prev_scores = self._weighted_scores(news_prev)
        else:
            today_scores = self._count_scores(news_today)
            prev_scores = self._count_scores(news_prev)

        result = {}
        for code in universe:
            t = today_scores.get(code, 0.0)
            p = prev_scores.get(code, 0.0)
            if code not in today_scores and code not in prev_scores:
                result[code] = np.nan
            elif p > 0:
                velocity = (t - p) / p
                result[code] = max(min(velocity, 1.0), -1.0)
            elif t > 0:
                result[code] = 1.0
            else:
                result[code] = 0.0

        return pd.Series(result, index=universe, name=self.name)

    @staticmethod
    def _weighted_scores(df: pd.DataFrame) -> dict[str, float]:
        """按 news_type 加权求和。"""
        if df.empty or "stock_code" not in df.columns:
            return {}
        scores: dict[str, float] = {}
        for _, row in df.iterrows():
            code = row.get("stock_code", "")
            if not code:
                continue
            ntype = str(row.get("news_type", "noise"))
            weight = _WEIGHTS.get(ntype, 0.0)
            scores[code] = scores.get(code, 0.0) + weight
        return scores

    @staticmethod
    def _count_scores(df: pd.DataFrame) -> dict[str, float]:
        """简单计数模式（向后兼容）。"""
        if df.empty or "stock_code" not in df.columns:
            return {}
        return df.groupby("stock_code").size().to_dict()
