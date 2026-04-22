"""策略进化器 — 通过网格搜索调优策略参数。

核心逻辑:
1. 选择一个基础策略
2. 对参数(止盈/止损/持仓天数/仓位)进行网格搜索
3. 对每个变体运行回测
4. 按目标函数排序选出最优
5. 新策略继承 parent 字段

设计原则: 纯计算、无外部API调用、可离线运行。
"""

from __future__ import annotations

import copy
import itertools
from dataclasses import dataclass, field
from typing import Optional

from src.strategy.schema import (
    EntryRule, ExitRule, PositionRule, Strategy, StrategyReport,
)
from src.strategy.backtest_engine import BacktestEngine
from src.data.storage import Storage


# ── 搜索空间定义 ───────────────────────────────────────

PARAM_GRID = {
    "take_profit_pct": [3.0, 5.0, 7.0, 10.0],
    "stop_loss_pct": [-2.0, -3.0, -5.0, -7.0],
    "max_hold_days": [1, 2, 3, 5],
    "single_position_pct": [10.0, 15.0, 20.0, 25.0],
}


@dataclass
class EvolveResult:
    """进化结果。"""
    best_strategy: Strategy
    best_report: StrategyReport
    all_variants: int = 0
    improvements: list[dict] = field(default_factory=list)
    """improvements: 相比原策略的提升指标"""


class StrategyEvolver:
    """策略进化器。"""

    def __init__(self, db: Storage):
        self.db = db
        self.engine = BacktestEngine(db)

    def evolve(
        self,
        base: Strategy,
        start_date: str,
        end_date: str,
        param_grid: Optional[dict] = None,
        max_variants: int = 200,
        objective: str = "sharpe",
        top_k: int = 3,
    ) -> EvolveResult:
        """对 base 策略进行参数调优。

        Args:
            base: 基础策略
            start_date/end_date: 回测区间
            param_grid: 参数搜索空间，None 使用默认
            max_variants: 最大变体数(限制组合爆炸)
            objective: 优化目标 "sharpe" / "win_rate" / "profit_loss_ratio"
            top_k: 返回前 K 个改进

        Returns:
            EvolveResult 包含最优策略及其报告
        """
        grid = param_grid or PARAM_GRID

        # 先跑基础策略作为基准
        base_report = self.engine.run(base, start_date, end_date)
        base_score = self._score(base_report, objective)

        # 生成所有参数组合
        combos = self._generate_combos(grid)
        if len(combos) > max_variants:
            # 随机采样
            import random
            random.seed(42)
            combos = random.sample(combos, max_variants)

        # 评估每个变体
        results: list[tuple[Strategy, StrategyReport, float]] = []
        for combo in combos:
            variant = self._apply_params(base, combo)
            report = self.engine.run(variant, start_date, end_date)
            score = self._score(report, objective)
            results.append((variant, report, score))

        # 按目标函数排序
        results.sort(key=lambda x: x[2], reverse=True)

        if not results:
            return EvolveResult(
                best_strategy=base,
                best_report=base_report,
                all_variants=0,
            )

        # 选最优
        best_variant, best_report, best_score = results[0]

        # 如果最优不如基准，保留基准
        if best_score <= base_score:
            best_variant = base
            best_report = base_report
            best_score = base_score

        # 收集改进
        improvements = []
        for i, (variant, report, score) in enumerate(results[:top_k]):
            delta = score - base_score
            improvements.append({
                "rank": i + 1,
                "params": self._diff_params(base, variant),
                "score": round(score, 4),
                "delta": round(delta, 4),
                "win_rate": report.win_rate,
                "total_return_pct": report.total_return_pct,
                "sharpe_ratio": report.sharpe_ratio,
            })

        return EvolveResult(
            best_strategy=best_variant,
            best_report=best_report,
            all_variants=len(combos),
            improvements=improvements,
        )

    def _score(self, report: StrategyReport, objective: str) -> float:
        """计算策略得分。"""
        if report.total_trades == 0:
            return -999.0

        if objective == "sharpe":
            return report.sharpe_ratio
        elif objective == "win_rate":
            return report.win_rate
        elif objective == "profit_loss_ratio":
            return report.profit_loss_ratio
        elif objective == "total_return":
            return report.total_return_pct
        else:
            # 综合评分: 夏普 * 0.4 + 胜率 * 0.3 + 盈亏比 * 0.3
            sharpe = min(max(report.sharpe_ratio, -5), 5) / 5  # 归一化到 [-1, 1]
            wr = report.win_rate
            plr = min(report.profit_loss_ratio, 5) / 5
            return sharpe * 0.4 + wr * 0.3 + plr * 0.3

    def _generate_combos(self, grid: dict) -> list[dict]:
        """生成参数组合。"""
        keys = list(grid.keys())
        values = [grid[k] if isinstance(grid[k], list) else [grid[k]] for k in keys]
        return [dict(zip(keys, combo)) for combo in itertools.product(*values)]

    def _apply_params(self, base: Strategy, params: dict) -> Strategy:
        """将参数应用到策略，返回新变体。"""
        variant = copy.deepcopy(base)

        if "take_profit_pct" in params:
            variant.exit.take_profit_pct = params["take_profit_pct"]
        if "stop_loss_pct" in params:
            variant.exit.stop_loss_pct = params["stop_loss_pct"]
        if "max_hold_days" in params:
            variant.exit.max_hold_days = params["max_hold_days"]
        if "trailing_stop_pct" in params:
            variant.exit.trailing_stop_pct = params["trailing_stop_pct"]
        if "single_position_pct" in params:
            variant.position.single_position_pct = params["single_position_pct"]
        if "max_holdings" in params:
            variant.position.max_holdings = params["max_holdings"]

        # 标记来源和版本
        variant.parent = base.name
        variant.version = base.version + 1
        variant.source = "evolver"
        variant.name = f"{base.name}_v{variant.version}"

        return variant

    def _diff_params(self, base: Strategy, variant: Strategy) -> dict:
        """比较两个策略的参数差异。"""
        diff = {}
        if base.exit.take_profit_pct != variant.exit.take_profit_pct:
            diff["take_profit_pct"] = f"{base.exit.take_profit_pct}→{variant.exit.take_profit_pct}"
        if base.exit.stop_loss_pct != variant.exit.stop_loss_pct:
            diff["stop_loss_pct"] = f"{base.exit.stop_loss_pct}→{variant.exit.stop_loss_pct}"
        if base.exit.max_hold_days != variant.exit.max_hold_days:
            diff["max_hold_days"] = f"{base.exit.max_hold_days}→{variant.exit.max_hold_days}"
        if base.position.single_position_pct != variant.position.single_position_pct:
            diff["single_position_pct"] = f"{base.position.single_position_pct}→{variant.position.single_position_pct}"
        return diff
