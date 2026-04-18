"""题材生命周期 — 股票级叙事因子。

基于概念板块的涨停天数，判断题材处于萌芽/爆发/衰退阶段。
"""

from datetime import datetime

import pandas as pd

from src.data.storage import Storage
from src.factors.base import BaseFactor, dedup_latest


class ThemeLifecycleFactor(BaseFactor):
    name = "theme_lifecycle"
    factor_type = "stock"
    description = "题材生命周期阶段分数 — 萌芽→爆发→衰退"
    lookback_days = 10

    def compute(self, universe: list[str], as_of: datetime, db: Storage) -> pd.Series:
        date_str = as_of.strftime("%Y-%m-%d")

        # 从 concept_daily 拿到每个概念当日的涨停数
        concept_daily_df = db.query(
            "concept_daily",
            as_of,
            where="trade_date = ?",
            params=(date_str,),
        )
        concept_daily_df = dedup_latest(concept_daily_df, key_cols=("concept_name", "trade_date"))
        self.validate_no_future(as_of, concept_daily_df)

        # 概念映射
        concept_map_df = db.query("concept_mapping", as_of)
        concept_map_df = dedup_latest(concept_map_df, key_cols=("stock_code", "concept_name"))

        if concept_daily_df.empty or concept_map_df.empty:
            return pd.Series(0.0, index=universe, name=self.name)

        # 计算每个概念的生命周期分数
        concept_scores = {}
        if "zt_count" in concept_daily_df.columns:
            for _, row in concept_daily_df.iterrows():
                name = row["concept_name"]
                zt = int(row.get("zt_count", 0))
                leader_board = int(row.get("leader_consecutive", 0))

                # 生命周期评分：
                # 萌芽(1-3涨停): 0.3-0.5
                # 爆发(4-8涨停或龙头>=3): 0.7-1.0
                # 衰退(>8涨停但龙头<=2): 0.1-0.3
                if zt <= 3:
                    score = 0.3 + zt * 0.07
                elif zt <= 8 or leader_board >= 3:
                    score = min(0.7 + zt * 0.03, 1.0)
                else:
                    score = 0.1 + min(zt * 0.01, 0.2)
                concept_scores[name] = score

        # 映射到个股
        stock_concept = concept_map_df.groupby("stock_code")["concept_name"].apply(list)
        result = {}
        for code in universe:
            concepts = stock_concept.get(code, [])
            if concepts:
                scores = [concept_scores.get(c, 0.0) for c in concepts]
                result[code] = max(scores)  # 取最强的概念分数
            else:
                result[code] = 0.0

        return pd.Series(result, index=universe, name=self.name)
