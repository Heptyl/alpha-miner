"""因子手术台 — 按市场状态/情绪/时间分段解剖IC序列，诊断因子有效性来源。

Evolution Engine v2 升级组件 (Step 2)。
"""

from __future__ import annotations

import math
from collections import defaultdict
from dataclasses import dataclass, field


# ============================================================
# 数据结构
# ============================================================

@dataclass
class RegimeIC:
    """单个 regime 的IC统计。"""

    regime: str
    ic_mean: float
    icir: float
    sample_days: int
    effective: bool


@dataclass
class EmotionIC:
    """情绪分桶IC统计。"""

    bucket: str       # strong / normal / weak
    ic_mean: float
    icir: float
    sample_days: int
    effective: bool


@dataclass
class TimeSegmentIC:
    """时间段IC对比。"""

    segment: str      # "first_half" / "second_half"
    ic_mean: float
    sample_days: int


@dataclass
class GoldenWindow:
    """黄金窗口 — IC持续高位的连续时段。"""

    start_date: str
    end_date: str
    regime: str
    avg_ic: float


@dataclass
class SurgeryReport:
    """因子手术台完整报告。"""

    factor_name: str
    overall_ic: float
    overall_icir: float
    regime_breakdown: list[RegimeIC] = field(default_factory=list)
    emotion_breakdown: list[EmotionIC] = field(default_factory=list)
    time_decay: list[TimeSegmentIC] = field(default_factory=list)
    golden_windows: list[GoldenWindow] = field(default_factory=list)
    diagnosis: str = "no_signal"
    best_regime: str | None = None
    best_emotion: str | None = None
    suggestion: str = ""


# ============================================================
# 手术台
# ============================================================

class FactorSurgeryTable:
    """因子IC手术台：按 regime / zt_count / 时间分段解剖IC序列。"""

    # emotion 分桶阈值
    _ZT_STRONG = 60
    _ZT_WEAK = 20

    def analyze(
        self,
        ic_series: list[dict],
        factor_name: str,
        ic_threshold: float = 0.03,
    ) -> SurgeryReport:
        """分析IC序列，输出手术台报告。

        Args:
            ic_series: 来自 BacktestResult.ic_series,
                       每项含 {date, ic, regime, zt_count, sample_size}
            factor_name: 因子名
            ic_threshold: IC有效阈值，默认 0.03

        Returns:
            SurgeryReport
        """
        if not ic_series:
            return SurgeryReport(
                factor_name=factor_name,
                overall_ic=0.0,
                overall_icir=0.0,
                diagnosis="no_signal",
                suggestion="IC序列为空，无法分析。",
            )

        # ---------- 全局统计 ----------
        overall_ic, overall_icir = self._compute_stats(ic_series)

        # ---------- 分段统计 ----------
        regime_breakdown = self._by_regime(ic_series, ic_threshold)
        emotion_breakdown = self._by_emotion(ic_series, ic_threshold)
        time_decay = self._by_time(ic_series)
        golden_windows = self._detect_golden_windows(ic_series, ic_threshold)

        # ---------- 诊断 ----------
        diagnosis, best_regime, best_emotion = self._diagnose(
            overall_ic=overall_ic,
            overall_icir=overall_icir,
            regime_breakdown=regime_breakdown,
            emotion_breakdown=emotion_breakdown,
            time_decay=time_decay,
            ic_threshold=ic_threshold,
        )

        suggestion = self._make_suggestion(diagnosis, best_regime, best_emotion)

        return SurgeryReport(
            factor_name=factor_name,
            overall_ic=overall_ic,
            overall_icir=overall_icir,
            regime_breakdown=regime_breakdown,
            emotion_breakdown=emotion_breakdown,
            time_decay=time_decay,
            golden_windows=golden_windows,
            diagnosis=diagnosis,
            best_regime=best_regime,
            best_emotion=best_emotion,
            suggestion=suggestion,
        )

    # ----------------------------------------------------------
    # 内部方法
    # ----------------------------------------------------------

    @staticmethod
    def _compute_stats(entries: list[dict]) -> tuple[float, float]:
        """计算IC均值和ICIR。

        零方差（所有IC相同）视为完美稳定，ICIR 设为 float('inf')。
        """
        ics = [e["ic"] for e in entries if isinstance(e.get("ic"), (int, float))]
        if not ics:
            return 0.0, 0.0
        n = len(ics)
        ic_mean = sum(ics) / n
        variance = sum((x - ic_mean) ** 2 for x in ics) / n if n > 1 else 0.0
        ic_std = math.sqrt(variance)
        if ic_std < 1e-12:
            # 零方差 = 完美稳定，用 inf 表示
            return ic_mean, float("inf") if ic_mean > 0 else -float("inf")
        icir = ic_mean / ic_std
        return ic_mean, icir

    def _by_regime(self, ic_series: list[dict], ic_threshold: float) -> list[RegimeIC]:
        """按 regime 分组统计。"""
        groups: dict[str, list[dict]] = defaultdict(list)
        for entry in ic_series:
            regime = entry.get("regime", "unknown")
            groups[regime].append(entry)

        results: list[RegimeIC] = []
        for regime, entries in groups.items():
            ic_mean, icir = self._compute_stats(entries)
            effective = ic_mean > ic_threshold and icir > 0.3
            results.append(RegimeIC(
                regime=regime,
                ic_mean=ic_mean,
                icir=icir,
                sample_days=len(entries),
                effective=effective,
            ))
        return results

    def _by_emotion(self, ic_series: list[dict], ic_threshold: float) -> list[EmotionIC]:
        """按 zt_count 分桶统计。"""
        groups: dict[str, list[dict]] = defaultdict(list)
        for entry in ic_series:
            zt = entry.get("zt_count", 0)
            if zt > self._ZT_STRONG:
                bucket = "strong"
            elif zt >= self._ZT_WEAK:
                bucket = "normal"
            else:
                bucket = "weak"
            groups[bucket].append(entry)

        results: list[EmotionIC] = []
        for bucket in ("strong", "normal", "weak"):
            entries = groups.get(bucket, [])
            if not entries:
                continue
            ic_mean, icir = self._compute_stats(entries)
            effective = ic_mean > ic_threshold
            results.append(EmotionIC(
                bucket=bucket,
                ic_mean=ic_mean,
                icir=icir,
                sample_days=len(entries),
                effective=effective,
            ))
        return results

    def _by_time(self, ic_series: list[dict]) -> list[TimeSegmentIC]:
        """按时序前半/后半统计。"""
        sorted_series = sorted(ic_series, key=lambda e: e.get("date", ""))
        n = len(sorted_series)
        mid = n // 2

        first = sorted_series[:mid]
        second = sorted_series[mid:]

        results: list[TimeSegmentIC] = []
        if first:
            ic_mean, _ = self._compute_stats(first)
            results.append(TimeSegmentIC(
                segment="first_half",
                ic_mean=ic_mean,
                sample_days=len(first),
            ))
        if second:
            ic_mean, _ = self._compute_stats(second)
            results.append(TimeSegmentIC(
                segment="second_half",
                ic_mean=ic_mean,
                sample_days=len(second),
            ))
        return results

    def _detect_golden_windows(
        self,
        ic_series: list[dict],
        ic_threshold: float,
    ) -> list[GoldenWindow]:
        """检测黄金窗口：IC >= threshold*1.5 持续 3+ 天。"""
        sorted_series = sorted(ic_series, key=lambda e: e.get("date", ""))
        threshold_high = ic_threshold * 1.5

        windows: list[GoldenWindow] = []
        streak: list[dict] = []

        for entry in sorted_series:
            ic = entry.get("ic", 0.0)
            if ic >= threshold_high:
                streak.append(entry)
            else:
                if len(streak) >= 3:
                    windows.append(self._make_window(streak))
                streak = []

        # 结尾检查
        if len(streak) >= 3:
            windows.append(self._make_window(streak))

        return windows

    @staticmethod
    def _make_window(streak: list[dict]) -> GoldenWindow:
        """从连续高IC记录中构造 GoldenWindow。"""
        ics = [e["ic"] for e in streak]
        avg_ic = sum(ics) / len(ics)
        # 使用窗口内最常见的 regime
        regime_counts: dict[str, int] = defaultdict(int)
        for e in streak:
            regime_counts[e.get("regime", "unknown")] += 1
        dominant_regime = max(regime_counts, key=regime_counts.get)
        return GoldenWindow(
            start_date=streak[0].get("date", ""),
            end_date=streak[-1].get("date", ""),
            regime=dominant_regime,
            avg_ic=avg_ic,
        )

    def _diagnose(
        self,
        overall_ic: float,
        overall_icir: float,
        regime_breakdown: list[RegimeIC],
        emotion_breakdown: list[EmotionIC],
        time_decay: list[TimeSegmentIC],
        ic_threshold: float,
    ) -> tuple[str, str | None, str | None]:
        """按优先级诊断。

        Returns:
            (diagnosis, best_regime, best_emotion)
        """
        # 先检测 time_decayed（衰减是致命缺陷，优先级最高）
        first_half_ic = 0.0
        second_half_ic = 0.0
        for seg in time_decay:
            if seg.segment == "first_half":
                first_half_ic = seg.ic_mean
            elif seg.segment == "second_half":
                second_half_ic = seg.ic_mean
        if first_half_ic > ic_threshold and second_half_ic < 0.015:
            return "time_decayed", None, None

        # 1. universally_effective
        if overall_ic > ic_threshold and overall_icir > 0.5:
            return "universally_effective", None, None

        # 2. regime_dependent
        effective_regimes = [r for r in regime_breakdown if r.effective]
        if effective_regimes:
            best = max(effective_regimes, key=lambda r: r.ic_mean)
            return "regime_dependent", best.regime, None

        # 3. emotion_dependent
        effective_emotions = [e for e in emotion_breakdown if e.effective]
        if effective_emotions:
            best = max(effective_emotions, key=lambda e: e.ic_mean)
            return "emotion_dependent", None, best.bucket

        # 4. no_signal
        return "no_signal", None, None

    @staticmethod
    def _make_suggestion(
        diagnosis: str,
        best_regime: str | None,
        best_emotion: str | None,
    ) -> str:
        """生成人类可读的建议。"""
        suggestions: dict[str, str] = {
            "universally_effective": (
                "因子在全市场状态下均有效，建议直接入库并进入实盘监控。"
            ),
            "regime_dependent": (
                f"因子仅在特定市场状态下有效 (best_regime={best_regime})，"
                "建议增加 regime 过滤条件后再入库。"
            ),
            "emotion_dependent": (
                f"因子仅在特定情绪环境下有效 (best_emotion={best_emotion})，"
                "建议增加 zt_count 相关的条件过滤。"
            ),
            "time_decayed": (
                "因子存在明显的时间衰减，前半段有效但后半段失效，"
                "建议缩短回测窗口或检测因子是否已过拟合。"
            ),
            "no_signal": (
                "因子未表现出有效的预测信号，建议更换假说或调整因子构造方式。"
            ),
        }
        return suggestions.get(diagnosis, "未知诊断结果。")
