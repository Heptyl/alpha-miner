"""策略8: 行业景气选股器。

核心逻辑(基于现有数据):
1. 所属行业在涨停池中占比高 (行业热度)
2. 板块内有多只涨停股 (资金共识)
3. 行业龙头涨幅领先

数据源: zt_pool, concept_daily, concept_mapping
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from collections import Counter

from src.screener.base import ScreenerBase, ScreenResult


class IndustryScreener(ScreenerBase):
    """行业景气选股器 — 维度8。"""

    name = "行业景气"
    dimension = 8

    def screen(self, report_date: str) -> list[ScreenResult]:
        conn = self._get_conn()
        try:
            # 当日涨停池
            zt = pd.read_sql_query(
                "SELECT stock_code, name, consecutive_zt, amount, industry "
                "FROM zt_pool WHERE trade_date = ?",
                conn, params=(report_date,),
            )
            # 近3天涨停池 (看板块持续性)
            zt_3d = pd.read_sql_query(
                "SELECT stock_code, industry FROM zt_pool "
                "WHERE trade_date <= ? ORDER BY trade_date DESC",
                conn, params=(report_date,),
            )
            # 概念映射
            concepts = pd.read_sql_query(
                "SELECT stock_code, concept_name FROM concept_mapping",
                conn,
            )
        finally:
            conn.close()

        if zt.empty:
            return []

        # --- 按行业统计涨停数量 ---
        industry_counts = Counter()
        for _, row in zt.iterrows():
            ind = row.get("industry", "") or ""
            if ind:
                industry_counts[ind] += 1

        # 热门行业: 涨停>=2
        hot_industries = {k: v for k, v in industry_counts.items() if v >= 2}

        results = []
        for _, row in zt.iterrows():
            code = row["stock_code"]
            if code.startswith("688") or code.startswith("689"):
                continue
            if len(code) == 6 and code[0] in ("8", "9"):
                continue

            ind = row.get("industry", "") or ""
            ind_count = industry_counts.get(ind, 0)

            score = 0.0
            reasons = []
            risks = []

            # === 行业涨停数 (权重40%) ===
            if ind_count >= 5:
                score += 0.40
                reasons.append(f"行业[{ind}]今日{ind_count}只涨停(极热)")
            elif ind_count >= 3:
                score += 0.25
                reasons.append(f"行业[{ind}]今日{ind_count}只涨停(强势)")
            elif ind_count >= 2:
                score += 0.15
                reasons.append(f"行业[{ind}]今日{ind_count}只涨停")
            else:
                continue  # 单独1只不构成行业景气

            # === 个股在行业中的地位 (权重30%) ===
            consec = int(row.get("consecutive_zt", 1) or 1)
            if consec >= 3:
                score += 0.30
                reasons.append(f"连板{consec}天(行业龙头)")
            elif consec >= 2:
                score += 0.15
                reasons.append(f"连板{consec}天(板块先锋)")

            # === 概念加成 (权重30%) ===
            stock_concepts = concepts[concepts["stock_code"] == code]
            if not stock_concepts.empty:
                n_concepts = len(stock_concepts)
                if n_concepts >= 3:
                    score += 0.15
                    concept_names = stock_concepts["concept_name"].head(3).tolist()
                    reasons.append(f"覆盖{n_concepts}个热门概念")
                if n_concepts >= 5:
                    score += 0.15
                    reasons.append("多概念叠加(资金关注度分散)")

            if score < 0.20:
                continue

            results.append(self.make_result(
                stock_code=code,
                report_date=report_date,
                score=min(score, 1.0),
                reasons=reasons,
                risks=["行业轮动快，注意追高风险"] if ind_count < 3 else [],
                extra={
                    "industry": ind,
                    "industry_zt_count": ind_count,
                    "consecutive_zt": consec,
                    "dimension": self.dimension,
                },
            ))

        results.sort(key=lambda r: r.score, reverse=True)
        return results
