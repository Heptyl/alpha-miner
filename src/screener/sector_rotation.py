"""策略4: 板块轮动选股器。

核心逻辑:
1. 概念板块涨停数量骤增 (板块启动信号)
2. 板块龙头股连续涨停
3. 同板块内个股跟涨
4. 资金集中流入该板块

数据源: zt_pool, concept_mapping, concept_daily
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from src.screener.base import ScreenerBase, ScreenResult


class SectorRotationScreener(ScreenerBase):
    """板块轮动选股器 — 维度4。"""

    name = "板块轮动"
    dimension = 4

    def screen(self, report_date: str) -> list[ScreenResult]:
        conn = self._get_conn()
        try:
            # 获取当日涨停池
            zt = pd.read_sql_query(
                "SELECT stock_code, name, consecutive_zt, amount, industry "
                "FROM zt_pool WHERE trade_date = ?",
                conn, params=(report_date,),
            )
            # 获取概念映射
            concepts = pd.read_sql_query(
                "SELECT stock_code, concept_name FROM concept_mapping",
                conn,
            )
            # 获取概念板块每日统计
            concept_daily = pd.read_sql_query(
                "SELECT concept_name, trade_date, zt_count, "
                "leader_code, leader_consecutive "
                "FROM concept_daily WHERE trade_date <= ? "
                "ORDER BY trade_date",
                conn, params=(report_date,),
            )
        finally:
            conn.close()

        if zt.empty:
            return []

        # --- 找热门板块 ---
        # 统计每个概念今日涨停数量
        zt_with_concept = zt.merge(concepts, on="stock_code", how="left")
        if zt_with_concept.empty or "concept_name" not in zt_with_concept.columns:
            return []

        concept_zt_count = (
            zt_with_concept.groupby("concept_name")
            .size()
            .reset_index(name="today_zt_count")
        )

        # 热门板块: 今日涨停>=2只
        hot_concepts = concept_zt_count[concept_zt_count["today_zt_count"] >= 2]
        if hot_concepts.empty:
            # 降级: 只要1只涨停也算
            hot_concepts = concept_zt_count[concept_zt_count["today_zt_count"] >= 1]

        results = []

        for _, concept_row in hot_concepts.iterrows():
            concept_name = concept_row["concept_name"]
            today_zt = concept_row["today_zt_count"]

            # 获取该板块的所有股票
            concept_stocks = concepts[concepts["concept_name"] == concept_name]
            stock_codes = concept_stocks["stock_code"].tolist()

            # 从涨停池中找该板块的涨停股
            concept_zt_stocks = zt_with_concept[
                zt_with_concept["concept_name"] == concept_name
            ]

            # 板块热度评分
            heat_score = min(today_zt / 5.0, 1.0) * 0.5  # 涨停数越多越热

            # 看历史: 该板块前几天是否也在升温
            if not concept_daily.empty:
                cd = concept_daily[concept_daily["concept_name"] == concept_name]
                if not cd.empty:
                    cd = cd.sort_values("trade_date")
                    # 3天内涨停数趋势
                    recent_zt = cd.tail(3)["zt_count"].tolist()
                    if len(recent_zt) >= 2 and recent_zt[-1] > recent_zt[0]:
                        heat_score += 0.2  # 板块升温
                    # 龙头连板
                    leader_consec = cd.tail(1)["leader_consecutive"].values
                    if len(leader_consec) > 0 and pd.notna(leader_consec[0]):
                        if leader_consec[0] >= 3:
                            heat_score += 0.3
                        elif leader_consec[0] >= 2:
                            heat_score += 0.15

            heat_score = min(heat_score, 1.0)

            # 生成结果: 该板块内的涨停股
            for _, stock_row in concept_zt_stocks.iterrows():
                code = stock_row["stock_code"]
                # 排除科创板和北交所
                if code.startswith("688") or code.startswith("689"):
                    continue
                if len(code) == 6 and code[0] in ("8", "9"):
                    continue

                consec = int(stock_row.get("consecutive_zt", 1) or 1)
                stock_score = heat_score * (0.5 + 0.5 * min(consec / 3, 1.0))

                reasons = [
                    f"板块[{concept_name}]今日{today_zt}只涨停",
                ]
                if consec >= 2:
                    reasons.append(f"连板{consec}天(板块龙头)")
                if heat_score >= 0.6:
                    reasons.append("板块持续升温")

                results.append(self.make_result(
                    stock_code=code,
                    report_date=report_date,
                    score=stock_score,
                    reasons=reasons,
                    risks=["板块轮动快，追涨风险高"] if heat_score < 0.5 else [],
                    extra={
                        "concept": concept_name,
                        "concept_zt_count": int(today_zt),
                        "consecutive_zt": consec,
                        "dimension": self.dimension,
                    },
                ))

        results.sort(key=lambda r: r.score, reverse=True)
        return results
