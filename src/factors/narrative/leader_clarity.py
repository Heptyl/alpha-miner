"""龙头清晰度 — 股票级叙事因子。

同一题材内龙头成交额 / 第二名成交额。龙头越清晰分数越高。
"""

from datetime import datetime

import pandas as pd

from src.data.storage import Storage
from src.factors.base import BaseFactor, dedup_latest


class LeaderClarityFactor(BaseFactor):
    name = "leader_clarity"
    factor_type = "stock"
    description = "龙头成交额/第二名成交额 — 龙头清晰度"
    lookback_days = 1

    def compute(self, universe: list[str], as_of: datetime, db: Storage) -> pd.Series:
        date_str = as_of.strftime("%Y-%m-%d")

        zt_df = db.query(
            "zt_pool",
            as_of,
            where="trade_date = ?",
            params=(date_str,),
        )
        concept_map_df = db.query("concept_mapping", as_of)

        zt_df = dedup_latest(zt_df)
        concept_map_df = dedup_latest(concept_map_df, key_cols=("stock_code", "concept_name"))
        self.validate_no_future(as_of, zt_df)

        if zt_df.empty or concept_map_df.empty:
            return pd.Series(0.0, index=universe, name=self.name)

        if "amount" not in zt_df.columns:
            return pd.Series(0.0, index=universe, name=self.name)

        # 每个概念内按成交额排名
        zt_with_concept = zt_df.merge(concept_map_df, on="stock_code", how="left")
        if zt_with_concept.empty or "concept_name" not in zt_with_concept.columns:
            return pd.Series(0.0, index=universe, name=self.name)

        concept_leader_clarity = {}
        for concept, group in zt_with_concept.groupby("concept_name"):
            if len(group) < 2:
                concept_leader_clarity[concept] = 1.0  # 只有一个，就是龙头
                continue
            sorted_group = group.sort_values("amount", ascending=False)
            top1_amount = float(sorted_group.iloc[0]["amount"])
            top2_amount = float(sorted_group.iloc[1]["amount"])
            clarity = top1_amount / top2_amount if top2_amount > 0 else 1.0
            # 归一化到 [0, 1]，3x 以上差异认为很清晰
            concept_leader_clarity[concept] = min(clarity / 3.0, 1.0)

        # 映射到个股：该股所在概念的龙头清晰度
        # 如果该股本身是龙头（成交额最大），给满分；否则给概念的清晰度
        stock_concepts = concept_map_df.groupby("stock_code")["concept_name"].apply(list)
        stock_amounts = zt_df.set_index("stock_code")["amount"].to_dict() if not zt_df.empty else {}

        result = {}
        for code in universe:
            concepts = stock_concepts.get(code, [])
            if not concepts:
                result[code] = 0.0
                continue

            # 取最强概念
            max_clarity = 0.0
            for concept in concepts:
                clarity = concept_leader_clarity.get(concept, 0.0)
                # 如果该股是该概念的龙头，额外加分
                if clarity > max_clarity:
                    max_clarity = clarity
            result[code] = max_clarity

        return pd.Series(result, index=universe, name=self.name)
