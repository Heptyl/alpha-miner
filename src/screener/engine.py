"""9维选股引擎 — 多策略融合评分。

架构:
1. 9个独立选股器并行运行
2. 每只股票汇总各维度得分
3. 五维加权综合评分 (借鉴 KHunter):
   - 技术面 (维度1+2+5) 权重35%
   - 资金面 (维度3+7) 权重35%
   - 基本面 (维度6) 权重10%
   - 板块面 (维度4+8) 权重10%
   - 风控面 (维度9) 权重10%
4. 风控后置过滤
5. 输出 TOP N 推荐
"""
from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

from src.screener.base import ScreenResult
from src.screener.trend_breakout import TrendBreakoutScreener
from src.screener.volume_pullback import VolumePullbackScreener
from src.screener.capital_flow import CapitalFlowScreener
from src.screener.sector_rotation import SectorRotationScreener
from src.screener.volume_price import VolumePriceScreener
from src.screener.fundamental import FundamentalScreener
from src.screener.main_force import MainForceScreener
from src.screener.industry import IndustryScreener
from src.screener.risk_control import RiskControlScreener


# 五维权重配置 (借鉴 KHunter 体系)
WEIGHTS = {
    "technical": 0.35,   # 技术面: 维度1+2+5
    "capital": 0.35,     # 资金面: 维度3+7
    "fundamental": 0.10, # 基本面: 维度6
    "sector": 0.10,      # 板块面: 维度4+8
    "risk": 0.10,        # 风控: 维度9
}

# 维度 -> 五维类别映射
DIM_CATEGORY = {
    1: "technical",  # 趋势突破
    2: "technical",  # 缩量回调
    5: "technical",  # 量价筛选
    3: "capital",    # 资金连续流入
    7: "capital",    # 主力资金
    6: "fundamental",# 基本面排雷
    4: "sector",     # 板块轮动
    8: "sector",     # 行业景气
    9: "risk",       # 风控
}


@dataclass
class StockScore:
    """一只股票的综合评分。"""
    stock_code: str
    stock_name: str = ""
    total_score: float = 0.0
    category_scores: dict = field(default_factory=dict)  # {category: score}
    dimension_scores: dict = field(default_factory=dict)  # {dimension: score}
    dimension_details: dict = field(default_factory=dict)  # {dimension: [reasons]}
    risks: list[str] = field(default_factory=list)
    signal_level: str = "C"

    def to_dict(self) -> dict:
        return {
            "stock_code": self.stock_code,
            "stock_name": self.stock_name,
            "total_score": round(self.total_score, 3),
            "signal_level": self.signal_level,
            "category_scores": {k: round(v, 3) for k, v in self.category_scores.items()},
            "dimension_scores": {k: round(v, 3) for k, v in self.dimension_scores.items()},
            "dimension_details": self.dimension_details,
            "risks": self.risks,
        }


class ScreenerEngine:
    """9维选股引擎。"""

    def __init__(self, db_path: str = "data/alpha_miner.db"):
        self.db_path = db_path
        self.screeners = [
            TrendBreakoutScreener(db_path),
            VolumePullbackScreener(db_path),
            CapitalFlowScreener(db_path),
            SectorRotationScreener(db_path),
            VolumePriceScreener(db_path),
            FundamentalScreener(db_path),
            MainForceScreener(db_path),
            IndustryScreener(db_path),
            RiskControlScreener(db_path),
        ]
        self.risk_screener = RiskControlScreener(db_path)

    def run(self, report_date: str, top_n: int = 20,
            min_score: float = 0.30) -> list[StockScore]:
        """执行9维选股并返回综合排名。

        Args:
            report_date: 选股日期 (YYYY-MM-DD)
            top_n: 返回前N只
            min_score: 最低综合分
        """
        # === Step 1: 各策略独立运行 ===
        all_results: dict[int, list[ScreenResult]] = {}

        for screener in self.screeners:
            dim = screener.dimension
            try:
                results = screener.screen(report_date)
                all_results[dim] = results
            except Exception as e:
                all_results[dim] = []

        # === Step 2: 按股票汇总各维度得分 ===
        stock_data: dict[str, dict] = defaultdict(lambda: {
            "name": "",
            "dim_scores": {},      # {dimension: score}
            "dim_reasons": {},     # {dimension: [reasons]}
            "dim_risks": [],       # 所有维度汇总的risks
        })

        for dim, results in all_results.items():
            for r in results:
                code = r.stock_code
                if not r.is_tradeable:
                    continue
                stock_data[code]["name"] = r.stock_name or stock_data[code]["name"]
                stock_data[code]["dim_scores"][dim] = max(
                    stock_data[code]["dim_scores"].get(dim, 0),
                    r.score,
                )
                stock_data[code]["dim_reasons"][dim] = r.reasons
                stock_data[code]["dim_risks"].extend(r.risks)

        if not stock_data:
            return []

        # === Step 2.5: 批量补全名称 ===
        codes_without_name = [c for c, d in stock_data.items()
                              if not d["name"] or d["name"] == c]
        if codes_without_name:
            from src.screener.base import ScreenerBase
            base = ScreenerBase(self.db_path)
            batch_names = base.get_stock_names_batch(codes_without_name, report_date)
            for c in codes_without_name:
                if batch_names.get(c, c) != c:
                    stock_data[c]["name"] = batch_names[c]

        # === Step 3: 计算五维综合分 ===
        scores: list[StockScore] = []

        for code, data in stock_data.items():
            dim_scores = data["dim_scores"]

            # 计算各五维类别得分 (取该类别下所有维度的最高分)
            category_scores = defaultdict(float)
            category_dim_count = defaultdict(int)

            for dim, score in dim_scores.items():
                cat = DIM_CATEGORY.get(dim, "technical")
                category_scores[cat] = max(category_scores[cat], score)
                category_dim_count[cat] += 1

            # 加权综合分
            total = 0.0
            for cat, weight in WEIGHTS.items():
                cat_score = category_scores.get(cat, 0.0)
                total += cat_score * weight

            # 维度命中加成: 命中越多维度，额外加分
            n_dims = len(dim_scores)
            if n_dims >= 5:
                total = min(total * 1.3, 1.0)  # 5维以上加成30%
            elif n_dims >= 3:
                total = min(total * 1.15, 1.0)  # 3维以上加成15%

            sig = "A" if total >= 0.65 else ("B" if total >= 0.40 else "C")

            scores.append(StockScore(
                stock_code=code,
                stock_name=data["name"],
                total_score=total,
                category_scores=dict(category_scores),
                dimension_scores=dim_scores,
                dimension_details=data["dim_reasons"],
                risks=list(set(data["dim_risks"])),
                signal_level=sig,
            ))

        # === Step 4: 风控后置过滤 ===
        filtered = self.risk_screener.filter_results(
            [self._score_to_result(s) for s in scores],
            report_date,
        )
        filtered_codes = {r.stock_code for r in filtered}
        scores = [s for s in scores if s.stock_code in filtered_codes]

        # === Step 5: ST/退市过滤 ===
        from src.screener.base import ScreenerBase
        scores = [s for s in scores if not ScreenerBase.is_st_stock(s.stock_name)]

        # === Step 6: 排序输出 ===
        scores.sort(key=lambda s: s.total_score, reverse=True)
        scores = [s for s in scores if s.total_score >= min_score]

        return scores[:top_n]

    @staticmethod
    def _score_to_result(score: StockScore) -> ScreenResult:
        """临时转换，用于风控过滤接口。"""
        return ScreenResult(
            stock_code=score.stock_code,
            stock_name=score.stock_name,
            strategy_name="composite",
            score=score.total_score,
            risks=score.risks,
        )

    def run_and_save(self, report_date: str, top_n: int = 20,
                     output_dir: str = "output/recommendations") -> str:
        """运行选股并保存JSON结果。"""
        scores = self.run(report_date, top_n)

        output = {
            "date": report_date,
            "generated_at": datetime.now().isoformat(),
            "total_candidates": len(scores),
            "signal_levels": {
                "A": sum(1 for s in scores if s.signal_level == "A"),
                "B": sum(1 for s in scores if s.signal_level == "B"),
                "C": sum(1 for s in scores if s.signal_level == "C"),
            },
            "stocks": [s.to_dict() for s in scores],
            "dimension_stats": self._calc_dimension_stats(scores),
        }

        Path(output_dir).mkdir(parents=True, exist_ok=True)
        out_path = Path(output_dir) / f"screen_{report_date}.json"
        out_path.write_text(json.dumps(output, ensure_ascii=False, indent=2),
                           encoding="utf-8")
        return str(out_path)

    @staticmethod
    def _calc_dimension_stats(scores: list[StockScore]) -> dict:
        """统计各维度的命中情况。"""
        stats = {}
        dim_names = {
            1: "趋势突破", 2: "缩量回调", 3: "资金连续流入",
            4: "板块轮动", 5: "量价筛选", 6: "基本面排雷",
            7: "主力资金", 8: "行业景气", 9: "风控筛选",
        }
        for dim in range(1, 10):
            hit = sum(1 for s in scores if dim in s.dimension_scores)
            avg_score = 0.0
            if hit > 0:
                avg_score = sum(
                    s.dimension_scores.get(dim, 0) for s in scores if dim in s.dimension_scores
                ) / hit
            stats[dim_names.get(dim, f"dim{dim}")] = {
                "hits": hit,
                "avg_score": round(avg_score, 3),
            }
        return stats
