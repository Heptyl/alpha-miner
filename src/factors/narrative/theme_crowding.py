"""题材拥挤度 — 股票级叙事因子。

题材涨停占全市场涨停的比例。拥挤度越高分数越低（反拥挤）。
"""

from datetime import datetime

import pandas as pd

from src.data.storage import Storage
from src.factors.base import BaseFactor


class ThemeCrowdingFactor(BaseFactor):
    name = "theme_crowding"
    factor_type = "stock"
    description = "题材拥挤度 — 涨停占比越高分数越低（反拥挤）"
    lookback_days = 5

    def compute(self, universe: list[str], as_of: datetime, db: Storage) -> pd.Series:
        date_str = as_of.strftime("%Y-%m-%d")

        zt_df = db.query(
            "zt_pool",
            as_of,
            where="trade_date = ?",
            params=(date_str,),
        )
        concept_map_df = db.query("concept_mapping", as_of)

        self.validate_no_future(as_of, zt_df)

        if zt_df.empty or concept_map_df.empty:
            return pd.Series(0.5, index=universe, name=self.name)

        total_zt = len(zt_df)
        if total_zt == 0:
            return pd.Series(0.5, index=universe, name=self.name)

        # 每个概念的涨停数
        zt_codes = set(zt_df["stock_code"].tolist())
        concept_counts = {}
        for _, row in concept_map_df.iterrows():
            code = row["stock_code"]
            concept = row["concept_name"]
            if code in zt_codes:
                concept_counts[concept] = concept_counts.get(concept, 0) + 1

        # 每个概念的拥挤度 = 概念涨停数/全市场涨停数
        concept_crowding = {}
        for concept, count in concept_counts.items():
            concept_crowding[concept] = count / total_zt

        # 映射到个股：取该股所有概念中的最大拥挤度，反转（越拥挤分越低）
        stock_concepts = concept_map_df.groupby("stock_code")["concept_name"].apply(list)
        result = {}
        for code in universe:
            concepts = stock_concepts.get(code, [])
            if concepts:
                max_crowd = max(concept_crowding.get(c, 0.0) for c in concepts)
                # 反转：1 - 拥挤度 → 拥挤度高的分数低
                result[code] = max(0.0, 1.0 - max_crowd * 5)  # 放大惩罚
            else:
                result[code] = 0.5

        return pd.Series(result, index=universe, name=self.name)
