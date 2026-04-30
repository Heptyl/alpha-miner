"""推荐复盘反馈环模块 — 将每日复盘结果反馈到推荐引擎，动态调整策略参数。

核心流程：
1. 读取累计复盘统计 (recommendations/review_stats.json)
2. 读取历史推荐与复盘明细，分析失败模式
   - 哪类因子得分高但实际不涨（因子失效）
   - 哪个信号等级(A/B/C)命中率高/低
   - 止损触发率过高的股票特征
3. 动态调整 recommend.yaml 的参数
   - 因子权重调整（IC下降的因子降权）
   - 信号等级阈值调整
   - min_composite_score 根据整体胜率微调
4. 生成调整报告保存到 recommendations/feedback_report.json

约束：
- 权重调整幅度每次不超过 ±20%
- 至少积累 5 天数据才启动反馈
- 如果胜率 > 60% 不做调整（避免过拟合）
- 调整后保留备份 config/recommend.yaml.bak
"""

from __future__ import annotations

import json
import logging
import shutil
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

import yaml

logger = logging.getLogger(__name__)

# ── 常量 ──────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).parent.parent.parent
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config" / "recommend.yaml"
DEFAULT_STATS_PATH = PROJECT_ROOT / "recommendations" / "review_stats.json"
DEFAULT_REPORT_PATH = PROJECT_ROOT / "recommendations" / "feedback_report.json"
DEFAULT_REC_DIR = PROJECT_ROOT / "recommendations"

MIN_DAYS_TO_FEEDBACK = 5       # 至少积累5天数据才启动
MAX_WEIGHT_ADJUST = 0.20       # 每次权重调整幅度不超过 ±20%
HIGH_WIN_RATE_CAP = 60.0       # 胜率>60%不做调整
MIN_SCORE_ADJUST_STEP = 0.01   # min_composite_score 每次最小调整步长
MAX_SCORE_ADJUST_STEP = 0.05   # min_composite_score 每次最大调整步长


@dataclass
class FactorPerformance:
    """因子近期表现分析结果。"""
    factor_name: str
    total_appearances: int = 0
    avg_score_in_wins: float = 0.0
    avg_score_in_losses: float = 0.0
    win_rate_when_high: float = 0.0   # 因子得分 > 0.5 时的胜率
    loss_rate_when_high: float = 0.0   # 因子得分 > 0.5 时的亏损率
    effectiveness: float = 0.0         # 综合有效性评分 (正=有效, 负=失效)
    suggested_weight_adjust: float = 0.0  # 建议权重调整比例 (-0.2 ~ +0.2)


@dataclass
class SignalLevelPerformance:
    """信号等级表现分析结果。"""
    level: str
    total: int = 0
    hit_buy_count: int = 0
    hit_target_count: int = 0
    hit_stop_count: int = 0
    win_rate: float = 0.0
    avg_profit: float = 0.0
    stop_loss_rate: float = 0.0


@dataclass
class StopLossPattern:
    """止损触发股票的共性问题分析。"""
    total_stop_triggered: int = 0
    avg_consecutive_zt: float = 0.0
    avg_circulation_mv: float = 0.0
    avg_amount: float = 0.0
    common_factors: dict = field(default_factory=dict)
    common_industries: dict = field(default_factory=dict)
    summary: str = ""


@dataclass
class FeedbackReport:
    """反馈调整报告。"""
    generated_at: str = ""
    data_days: int = 0
    overall_win_rate: float = 0.0
    overall_avg_profit: float = 0.0
    should_adjust: bool = False
    skip_reason: str = ""

    # 分析结果
    factor_performances: list[FactorPerformance] = field(default_factory=list)
    signal_level_performances: list[SignalLevelPerformance] = field(default_factory=list)
    stop_loss_pattern: Optional[StopLossPattern] = None

    # 参数调整记录
    weight_adjustments: dict = field(default_factory=dict)      # factor -> (old, new)
    threshold_adjustments: dict = field(default_factory=dict)    # key -> (old, new)
    score_threshold_adjustment: dict = field(default_factory=dict)  # min_composite_score (old, new)

    # 调整后的完整配置
    new_config: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "generated_at": self.generated_at,
            "data_days": self.data_days,
            "overall_win_rate": round(self.overall_win_rate, 2),
            "overall_avg_profit": round(self.overall_avg_profit, 2),
            "should_adjust": self.should_adjust,
            "skip_reason": self.skip_reason,
            "factor_performances": [
                {
                    "factor": fp.factor_name,
                    "appearances": fp.total_appearances,
                    "avg_score_in_wins": round(fp.avg_score_in_wins, 3),
                    "avg_score_in_losses": round(fp.avg_score_in_losses, 3),
                    "win_rate_when_high": round(fp.win_rate_when_high, 2),
                    "effectiveness": round(fp.effectiveness, 3),
                    "suggested_weight_adjust": round(fp.suggested_weight_adjust, 4),
                }
                for fp in self.factor_performances
            ],
            "signal_level_performances": [
                {
                    "level": slp.level,
                    "total": slp.total,
                    "hit_buy_count": slp.hit_buy_count,
                    "hit_target_count": slp.hit_target_count,
                    "hit_stop_count": slp.hit_stop_count,
                    "win_rate": round(slp.win_rate, 2),
                    "avg_profit": round(slp.avg_profit, 2),
                    "stop_loss_rate": round(slp.stop_loss_rate, 2),
                }
                for slp in self.signal_level_performances
            ],
            "stop_loss_pattern": (
                {
                    "total_stop_triggered": self.stop_loss_pattern.total_stop_triggered,
                    "avg_consecutive_zt": round(self.stop_loss_pattern.avg_consecutive_zt, 2),
                    "avg_circulation_mv": round(self.stop_loss_pattern.avg_circulation_mv, 0),
                    "common_industries": self.stop_loss_pattern.common_industries,
                    "summary": self.stop_loss_pattern.summary,
                }
                if self.stop_loss_pattern
                else None
            ),
            "weight_adjustments": {
                k: {"old": round(v[0], 4), "new": round(v[1], 4)}
                for k, v in self.weight_adjustments.items()
            },
            "threshold_adjustments": {
                k: {"old": round(v[0], 4), "new": round(v[1], 4)}
                for k, v in self.threshold_adjustments.items()
            },
            "score_threshold_adjustment": (
                {
                    "old": round(self.score_threshold_adjustment["old"], 4),
                    "new": round(self.score_threshold_adjustment["new"], 4),
                }
                if self.score_threshold_adjustment
                else {}
            ),
        }

    def to_text(self) -> str:
        """终端纯文本输出。"""
        lines = []
        lines.append("=" * 60)
        lines.append(f"  Alpha Miner 反馈环报告 — {self.generated_at}")
        lines.append("=" * 60)

        lines.append(f"\n  数据天数: {self.data_days} 天")
        lines.append(f"  整体胜率: {self.overall_win_rate:.1f}%")
        lines.append(f"  平均盈亏: {self.overall_avg_profit:+.2f}%")

        if not self.should_adjust:
            lines.append(f"\n  ⏸ 不调整: {self.skip_reason}")
        else:
            lines.append(f"\n  ✅ 执行参数调整")

        # 因子分析
        if self.factor_performances:
            lines.append(f"\n  {'─' * 56}")
            lines.append(f"  因子有效性分析:")
            for fp in self.factor_performances:
                mark = "✓" if fp.effectiveness >= 0 else "✗"
                adj = ""
                if fp.suggested_weight_adjust != 0:
                    adj = f" → {'升权' if fp.suggested_weight_adjust > 0 else '降权'} {abs(fp.suggested_weight_adjust)*100:.1f}%"
                lines.append(
                    f"    {mark} {fp.factor_name:20s}  "
                    f"有效性={fp.effectiveness:+.3f}  "
                    f"高得分胜率={fp.win_rate_when_high:.0f}%{adj}"
                )

        # 信号等级
        if self.signal_level_performances:
            lines.append(f"\n  {'─' * 56}")
            lines.append(f"  信号等级表现:")
            for slp in self.signal_level_performances:
                lines.append(
                    f"    [{slp.level}] 总{slp.total}只  "
                    f"买点命中{slp.win_rate:.0f}%  "
                    f"止损率{slp.stop_loss_rate:.0f}%  "
                    f"均盈亏{slp.avg_profit:+.2f}%"
                )

        # 止损模式
        if self.stop_loss_pattern and self.stop_loss_pattern.total_stop_triggered > 0:
            lines.append(f"\n  {'─' * 56}")
            lines.append(f"  止损触发模式:")
            lines.append(f"    总触发: {self.stop_loss_pattern.total_stop_triggered} 次")
            lines.append(f"    {self.stop_loss_pattern.summary}")

        # 调整记录
        if self.weight_adjustments:
            lines.append(f"\n  {'─' * 56}")
            lines.append(f"  权重调整:")
            for factor, (old, new) in self.weight_adjustments.items():
                arrow = "↑" if new > old else "↓"
                lines.append(f"    {factor:20s}  {old:.3f} → {new:.3f} {arrow}")

        if self.threshold_adjustments:
            lines.append(f"\n  信号阈值调整:")
            for key, (old, new) in self.threshold_adjustments.items():
                lines.append(f"    {key:20s}  {old:.3f} → {new:.3f}")

        if self.score_threshold_adjustment:
            old = self.score_threshold_adjustment.get("old", 0)
            new = self.score_threshold_adjustment.get("new", 0)
            lines.append(f"\n  最低综合得分: {old:.3f} → {new:.3f}")

        lines.append(f"\n{'=' * 60}")
        return "\n".join(lines)


# ── 核心反馈引擎 ──────────────────────────────────────────────


class FeedbackEngine:
    """复盘反馈引擎：分析历史表现，动态调整推荐参数。"""

    def __init__(
        self,
        config_path: str | Path | None = None,
        stats_path: str | Path | None = None,
        rec_dir: str | Path | None = None,
        report_path: str | Path | None = None,
    ):
        self.config_path = Path(config_path) if config_path else DEFAULT_CONFIG_PATH
        self.stats_path = Path(stats_path) if stats_path else DEFAULT_STATS_PATH
        self.rec_dir = Path(rec_dir) if rec_dir else DEFAULT_REC_DIR
        self.report_path = Path(report_path) if report_path else DEFAULT_REPORT_PATH

    # ── 主入口 ────────────────────────────────────────────────

    def run(self) -> FeedbackReport:
        """执行反馈环主流程。

        Returns:
            FeedbackReport 调整报告
        """
        report = FeedbackReport(generated_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

        # 1. 加载累计统计
        stats = self._load_stats()
        if stats is None:
            report.should_adjust = False
            report.skip_reason = f"无统计文件 {self.stats_path}"
            self._save_report(report)
            return report

        daily_log = stats.get("daily_log", [])
        report.data_days = len(daily_log)

        # 2. 检查最小天数
        if report.data_days < MIN_DAYS_TO_FEEDBACK:
            report.should_adjust = False
            report.skip_reason = f"数据不足 ({report.data_days}/{MIN_DAYS_TO_FEEDBACK} 天)"
            self._save_report(report)
            return report

        # 3. 计算整体胜率
        profits = stats.get("all_profits", [])
        if profits:
            wins = sum(1 for p in profits if p > 0)
            report.overall_win_rate = wins / len(profits) * 100
            report.overall_avg_profit = sum(profits) / len(profits)

        # 4. 胜率过高 → 不调整
        if report.overall_win_rate > HIGH_WIN_RATE_CAP:
            report.should_adjust = False
            report.skip_reason = (
                f"胜率 {report.overall_win_rate:.1f}% > {HIGH_WIN_RATE_CAP}%，"
                f"避免过拟合不做调整"
            )
            self._save_report(report)
            return report

        # 5. 分析失败模式
        rec_histories = self._load_recommendation_histories()
        review_histories = self._load_review_histories()

        factor_perfs = self._analyze_factor_performance(rec_histories, review_histories)
        signal_perfs = self._analyze_signal_levels(rec_histories, review_histories)
        stop_pattern = self._analyze_stop_loss_patterns(rec_histories, review_histories)

        report.factor_performances = factor_perfs
        report.signal_level_performances = signal_perfs
        report.stop_loss_pattern = stop_pattern

        # 6. 加载当前配置
        config = self._load_config()
        if not config:
            report.should_adjust = False
            report.skip_reason = "无法加载 recommend.yaml"
            self._save_report(report)
            return report

        # 7. 执行调整
        new_config, adjustments = self._compute_adjustments(
            config, factor_perfs, signal_perfs, report.overall_win_rate,
        )

        report.weight_adjustments = adjustments.get("weights", {})
        report.threshold_adjustments = adjustments.get("thresholds", {})
        report.score_threshold_adjustment = adjustments.get("score_threshold", {})
        report.should_adjust = True
        report.new_config = new_config

        # 8. 备份 & 写入
        self._backup_config()
        self._save_config(new_config)

        # 9. 保存报告
        self._save_report(report)
        return report

    # ── 数据加载 ──────────────────────────────────────────────

    def _load_stats(self) -> dict | None:
        """加载累计复盘统计。"""
        if not self.stats_path.exists():
            logger.warning(f"统计文件不存在: {self.stats_path}")
            return None
        try:
            with open(self.stats_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"加载统计文件失败: {e}")
            return None

    def _load_config(self) -> dict:
        """加载 recommend.yaml 配置。"""
        if not self.config_path.exists():
            logger.warning(f"配置文件不存在: {self.config_path}")
            return {}
        try:
            with open(self.config_path, "r", encoding="utf-8") as f:
                return yaml.safe_load(f) or {}
        except Exception as e:
            logger.error(f"加载配置文件失败: {e}")
            return {}

    def _load_recommendation_histories(self) -> list[dict]:
        """加载所有历史推荐文件。"""
        histories = []
        if not self.rec_dir.exists():
            return histories
        for f in sorted(self.rec_dir.glob("*_recommend.json")):
            try:
                with open(f, "r", encoding="utf-8") as fp:
                    data = json.load(fp)
                    data["_source_file"] = f.name
                    histories.append(data)
            except Exception:
                continue
        return histories

    def _load_review_histories(self) -> list[dict]:
        """加载所有历史复盘文件。"""
        histories = []
        if not self.rec_dir.exists():
            return histories
        for f in sorted(self.rec_dir.glob("*_review.json")):
            try:
                with open(f, "r", encoding="utf-8") as fp:
                    data = json.load(fp)
                    data["_source_file"] = f.name
                    histories.append(data)
            except Exception:
                continue
        return histories

    # ── 分析失败模式 ──────────────────────────────────────────

    def _analyze_factor_performance(
        self,
        rec_histories: list[dict],
        review_histories: list[dict],
    ) -> list[FactorPerformance]:
        """分析因子有效性：哪些因子得分高但实际不涨。

        策略：
        - 遍历每只推荐的因子得分
        - 根据对应的复盘结果区分盈/亏
        - 计算每个因子的「有效性」= win_rate_when_high - loss_rate_when_high
        - 有效性 < 0 的因子建议降权，> 0 的建议升权
        """
        # 构建回顾索引: rec_date -> { stock_code -> review_result }
        review_idx: dict[str, dict[str, dict]] = {}
        for rv in review_histories:
            rec_date = rv.get("rec_date", "")
            if not rec_date:
                continue
            stock_map = {}
            for s in rv.get("stocks", []):
                stock_map[s.get("stock_code", "")] = s
            review_idx[rec_date] = stock_map

        # 收集因子数据
        factor_data: dict[str, dict] = defaultdict(
            lambda: {
                "win_scores": [],
                "loss_scores": [],
                "high_total": 0,
                "high_wins": 0,
                "high_losses": 0,
                "appearances": 0,
            }
        )

        for rec in rec_histories:
            trade_date = rec.get("trade_date", "")
            # 查找对应复盘
            reviews_for_date = review_idx.get(trade_date, {})

            for stock in rec.get("stocks", []):
                code = stock.get("stock_code", "")
                factor_scores = stock.get("factor_scores", {})
                review_stock = reviews_for_date.get(code)

                if review_stock is None:
                    continue

                is_profit = review_stock.get("profit_pct", 0) > 0
                is_stop = review_stock.get("hit_stop_loss", False)

                for factor_name, score in factor_scores.items():
                    fd = factor_data[factor_name]
                    fd["appearances"] += 1

                    if is_profit:
                        fd["win_scores"].append(score)
                    else:
                        fd["loss_scores"].append(score)

                    # 因子得分 > 0.5 视为"高得分"
                    if score > 0.5:
                        fd["high_total"] += 1
                        if is_profit:
                            fd["high_wins"] += 1
                        else:
                            fd["high_losses"] += 1

        # 计算每个因子的有效性
        results = []
        for factor_name, fd in factor_data.items():
            avg_win = (
                sum(fd["win_scores"]) / len(fd["win_scores"])
                if fd["win_scores"]
                else 0.0
            )
            avg_loss = (
                sum(fd["loss_scores"]) / len(fd["loss_scores"])
                if fd["loss_scores"]
                else 0.0
            )
            wr_high = (
                fd["high_wins"] / fd["high_total"] * 100
                if fd["high_total"] > 0
                else 50.0
            )
            lr_high = (
                fd["high_losses"] / fd["high_total"] * 100
                if fd["high_total"] > 0
                else 50.0
            )

            # 有效性 = 高得分胜率 - 高得分亏损率（带偏移）
            effectiveness = (wr_high - lr_high) / 100.0

            # 建议调整：正的有效性升权，负的降权，但限幅
            raw_adjust = effectiveness * 0.3  # 缩放因子
            suggested = max(-MAX_WEIGHT_ADJUST, min(MAX_WEIGHT_ADJUST, raw_adjust))

            results.append(
                FactorPerformance(
                    factor_name=factor_name,
                    total_appearances=fd["appearances"],
                    avg_score_in_wins=avg_win,
                    avg_score_in_losses=avg_loss,
                    win_rate_when_high=wr_high,
                    loss_rate_when_high=lr_high,
                    effectiveness=effectiveness,
                    suggested_weight_adjust=suggested,
                )
            )

        # 按有效性排序（最差的在前）
        results.sort(key=lambda x: x.effectiveness)
        return results

    def _analyze_signal_levels(
        self,
        rec_histories: list[dict],
        review_histories: list[dict],
    ) -> list[SignalLevelPerformance]:
        """分析不同信号等级(A/B/C)的实际表现。"""
        review_idx: dict[str, dict[str, dict]] = {}
        for rv in review_histories:
            rec_date = rv.get("rec_date", "")
            if not rec_date:
                continue
            stock_map = {}
            for s in rv.get("stocks", []):
                stock_map[s.get("stock_code", "")] = s
            review_idx[rec_date] = stock_map

        level_data: dict[str, dict] = defaultdict(
            lambda: {
                "total": 0,
                "hit_buy": 0,
                "hit_target": 0,
                "hit_stop": 0,
                "profits": [],
            }
        )

        for rec in rec_histories:
            trade_date = rec.get("trade_date", "")
            reviews_for_date = review_idx.get(trade_date, {})

            for stock in rec.get("stocks", []):
                code = stock.get("stock_code", "")
                level = stock.get("signal_level", "C")
                review_stock = reviews_for_date.get(code)

                if review_stock is None:
                    continue

                ld = level_data[level]
                ld["total"] += 1
                if review_stock.get("hit_buy_zone", False):
                    ld["hit_buy"] += 1
                if review_stock.get("hit_target", False):
                    ld["hit_target"] += 1
                if review_stock.get("hit_stop_loss", False):
                    ld["hit_stop"] += 1
                if review_stock.get("hit_buy_zone", False):
                    ld["profits"].append(review_stock.get("profit_pct", 0))

        results = []
        for level in ["A", "B", "C"]:
            ld = level_data.get(level, level_data.default_factory())
            profits = ld["profits"]
            wins = sum(1 for p in profits if p > 0)
            wr = wins / len(profits) * 100 if profits else 0.0
            avg_p = sum(profits) / len(profits) if profits else 0.0
            stop_rate = ld["hit_stop"] / ld["total"] * 100 if ld["total"] > 0 else 0.0

            results.append(
                SignalLevelPerformance(
                    level=level,
                    total=ld["total"],
                    hit_buy_count=ld["hit_buy"],
                    hit_target_count=ld["hit_target"],
                    hit_stop_count=ld["hit_stop"],
                    win_rate=wr,
                    avg_profit=avg_p,
                    stop_loss_rate=stop_rate,
                )
            )
        return results

    def _analyze_stop_loss_patterns(
        self,
        rec_histories: list[dict],
        review_histories: list[dict],
    ) -> StopLossPattern:
        """分析止损触发股票的共性特征。"""
        review_idx: dict[str, dict[str, dict]] = {}
        for rv in review_histories:
            rec_date = rv.get("rec_date", "")
            if not rec_date:
                continue
            stock_map = {}
            for s in rv.get("stocks", []):
                stock_map[s.get("stock_code", "")] = s
            review_idx[rec_date] = stock_map

        stop_stocks = []
        for rec in rec_histories:
            trade_date = rec.get("trade_date", "")
            reviews_for_date = review_idx.get(trade_date, {})

            for stock in rec.get("stocks", []):
                code = stock.get("stock_code", "")
                review_stock = reviews_for_date.get(code)

                if review_stock and review_stock.get("hit_stop_loss", False):
                    stop_stocks.append({
                        "code": code,
                        "industry": stock.get("industry", ""),
                        "consecutive_zt": stock.get("consecutive_zt", 0),
                        "circulation_mv": stock.get("circulation_mv", 0),
                        "amount": stock.get("amount", 0),
                        "factor_scores": stock.get("factor_scores", {}),
                        "signal_level": stock.get("signal_level", ""),
                    })

        total_stop = len(stop_stocks)
        if total_stop == 0:
            return StopLossPattern(summary="无止损触发记录")

        avg_zt = sum(s["consecutive_zt"] for s in stop_stocks) / total_stop
        avg_mv = sum(s["circulation_mv"] for s in stop_stocks) / total_stop
        avg_amt = sum(s["amount"] for s in stop_stocks) / total_stop

        # 高频行业
        industry_counts: dict[str, int] = defaultdict(int)
        for s in stop_stocks:
            if s["industry"]:
                industry_counts[s["industry"]] += 1

        # 高频因子得分特征
        factor_avg: dict[str, list[float]] = defaultdict(list)
        for s in stop_stocks:
            for fn, fv in s["factor_scores"].items():
                factor_avg[fn].append(fv)
        common_factors = {
            k: round(sum(v) / len(v), 3) for k, v in factor_avg.items()
        }

        # 生成摘要
        top_industries = sorted(industry_counts.items(), key=lambda x: -x[1])[:5]
        industry_str = ", ".join(f"{k}({v})" for k, v in top_industries)
        summary = (
            f"止损{total_stop}次，平均连板{avg_zt:.1f}，"
            f"平均市值{avg_mv/1e8:.1f}亿。"
            f"集中行业: {industry_str}"
        )

        return StopLossPattern(
            total_stop_triggered=total_stop,
            avg_consecutive_zt=avg_zt,
            avg_circulation_mv=avg_mv,
            avg_amount=avg_amt,
            common_factors=common_factors,
            common_industries=dict(top_industries),
            summary=summary,
        )

    # ── 参数调整计算 ──────────────────────────────────────────

    def _compute_adjustments(
        self,
        config: dict,
        factor_perfs: list[FactorPerformance],
        signal_perfs: list[SignalLevelPerformance],
        overall_win_rate: float,
    ) -> tuple[dict, dict]:
        """根据分析结果计算参数调整。

        Returns:
            (new_config, adjustments_dict)
        """
        import copy

        new_config = copy.deepcopy(config)
        adjustments: dict = {"weights": {}, "thresholds": {}, "score_threshold": {}}

        # ── 1. 因子权重调整 ──
        old_weights = dict(config.get("factor_weights", {}))
        new_weights = dict(old_weights)

        # 构建因子名 -> 建议调整 map
        factor_adjust_map = {fp.factor_name: fp for fp in factor_perfs}

        for factor_name, old_w in old_weights.items():
            fp = factor_adjust_map.get(factor_name)
            if fp is None:
                continue
            if fp.total_appearances < 3:
                # 样本太少，不调整
                continue

            # 按建议比例调整
            adjust_ratio = fp.suggested_weight_adjust
            new_w = old_w * (1 + adjust_ratio)
            # 保证权重非负且合理
            new_w = max(0.01, min(0.50, new_w))
            new_weights[factor_name] = round(new_w, 4)

            if abs(new_w - old_w) > 1e-6:
                adjustments["weights"][factor_name] = (old_w, new_w)

        # 归一化权重（保证总和 = 1.0）
        total_w = sum(new_weights.values())
        if total_w > 0:
            new_weights = {k: round(v / total_w, 4) for k, v in new_weights.items()}
            # 更新 adjustments 中的 new 值
            for k in adjustments["weights"]:
                old_val, _ = adjustments["weights"][k]
                adjustments["weights"][k] = (old_val, new_weights.get(k, old_val))

        new_config["factor_weights"] = new_weights

        # ── 2. 信号等级阈值调整 ──
        # 根据实际胜率调整阈值
        # 如果某等级胜率很低 → 提高阈值（更严格）
        # 如果某等级胜率很高 → 可适当降低阈值
        level_thresholds = {"A": 0.65, "B": 0.45, "C": 0.0}
        # 从 RecommendEngine 的默认值中读取（如果 config 有覆盖）
        # 目前 config 中没有显式配置信号阈值，用默认值

        for slp in signal_perfs:
            if slp.total < 3:
                continue
            if slp.level not in level_thresholds:
                continue

            old_threshold = level_thresholds[slp.level]
            if old_threshold <= 0:
                continue  # C 级无阈值

            # 胜率低于40% → 提高5%，高于55% → 降低3%
            if slp.win_rate < 40:
                adjustment = 0.05
            elif slp.win_rate > 55:
                adjustment = -0.03
            else:
                continue

            new_threshold = old_threshold + adjustment
            # 限幅
            new_threshold = max(0.20, min(0.80, new_threshold))
            level_thresholds[slp.level] = new_threshold

            if abs(new_threshold - old_threshold) > 1e-6:
                key = f"level_{slp.level}_threshold"
                adjustments["thresholds"][key] = (old_threshold, new_threshold)

        # 将阈值写入 config（新增字段，供 RecommendEngine 读取）
        if "signal_thresholds" not in new_config:
            new_config["signal_thresholds"] = {}
        new_config["signal_thresholds"].update(level_thresholds)

        # ── 3. min_composite_score 微调 ──
        old_min_score = config.get("filters", {}).get("min_composite_score", 0.35)

        # 胜率越低 → 提高门槛（过滤低质量推荐）
        # 胜率越高 → 可以稍微降低门槛（已有HIGH_WIN_RATE_CAP兜底，这里不会超过60%）
        if overall_win_rate < 30:
            step = MAX_SCORE_ADJUST_STEP
        elif overall_win_rate < 40:
            step = MIN_SCORE_ADJUST_STEP + 0.02
        elif overall_win_rate > 55:
            step = -MIN_SCORE_ADJUST_STEP
        else:
            step = -MIN_SCORE_ADJUST_STEP * 0.5

        new_min_score = old_min_score + step
        new_min_score = max(0.20, min(0.60, new_min_score))
        new_min_score = round(new_min_score, 2)

        if abs(new_min_score - old_min_score) > 1e-4:
            adjustments["score_threshold"] = {"old": old_min_score, "new": new_min_score}
            if "filters" not in new_config:
                new_config["filters"] = {}
            new_config["filters"]["min_composite_score"] = new_min_score

        return new_config, adjustments

    # ── 配置备份与保存 ────────────────────────────────────────

    def _backup_config(self) -> None:
        """备份当前配置文件。"""
        backup_path = self.config_path.with_suffix(".yaml.bak")
        if self.config_path.exists():
            shutil.copy2(self.config_path, backup_path)
            logger.info(f"配置已备份到 {backup_path}")

    def _save_config(self, config: dict) -> None:
        """保存配置到 recommend.yaml。"""
        self.config_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.config_path, "w", encoding="utf-8") as f:
            yaml.dump(
                config,
                f,
                default_flow_style=False,
                allow_unicode=True,
                sort_keys=False,
            )
        logger.info(f"配置已更新: {self.config_path}")

    def _save_report(self, report: FeedbackReport) -> None:
        """保存调整报告。"""
        self.report_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.report_path, "w", encoding="utf-8") as f:
            json.dump(report.to_dict(), f, ensure_ascii=False, indent=2)
        logger.info(f"反馈报告已保存: {self.report_path}")


# ── 便捷函数 ──────────────────────────────────────────────────


def run_feedback(
    config_path: str | None = None,
    stats_path: str | None = None,
    rec_dir: str | None = None,
    report_path: str | None = None,
) -> FeedbackReport:
    """执行反馈环（便捷入口）。

    Args:
        config_path: recommend.yaml 路径（默认 config/recommend.yaml）
        stats_path: review_stats.json 路径（默认 recommendations/review_stats.json）
        rec_dir: 推荐文件目录（默认 recommendations/）
        report_path: 反馈报告输出路径（默认 recommendations/feedback_report.json）

    Returns:
        FeedbackReport
    """
    engine = FeedbackEngine(
        config_path=config_path,
        stats_path=stats_path,
        rec_dir=rec_dir,
        report_path=report_path,
    )
    return engine.run()


# ── CLI 入口 ──────────────────────────────────────────────────


def main():
    """命令行入口：运行反馈环。"""
    import argparse

    parser = argparse.ArgumentParser(description="推荐复盘反馈环")
    parser.add_argument(
        "--config", default=None, help="recommend.yaml 路径"
    )
    parser.add_argument(
        "--stats", default=None, help="review_stats.json 路径"
    )
    parser.add_argument(
        "--rec-dir", default=None, help="推荐文件目录"
    )
    parser.add_argument(
        "--report", default=None, help="反馈报告输出路径"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="只生成报告，不修改配置文件",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    )

    if args.dry_run:
        # dry-run 模式：不备份、不写入配置
        engine = FeedbackEngine(
            config_path=args.config,
            stats_path=args.stats,
            rec_dir=args.rec_dir,
            report_path=args.report,
        )
        report = engine.run()
        # 回滚配置文件（因为 run() 已经写入了）
        bak_path = engine.config_path.with_suffix(".yaml.bak")
        if bak_path.exists():
            import shutil
            shutil.copy2(bak_path, engine.config_path)
            logger.info("(dry-run) 配置已回滚")
    else:
        report = run_feedback(
            config_path=args.config,
            stats_path=args.stats,
            rec_dir=args.rec_dir,
            report_path=args.report,
        )

    logger.info(report.to_text())


if __name__ == "__main__":
    main()
