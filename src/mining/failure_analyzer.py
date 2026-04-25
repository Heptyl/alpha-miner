"""因子失败原因分析器。

根据回测结果诊断失败模式，输出结构化建议。
"""

from dataclasses import dataclass

import numpy as np
import pandas as pd

from src.mining.surgery_table import SurgeryReport


@dataclass
class FailureDiagnosis:
    """失败诊断结果。"""
    diagnosis: str       # too_strict / too_loose / no_signal / noisy_but_directional / reversed / redundant / inconsistent
    suggestion: str      # 人类可读建议
    details: dict        # 补充信息


class FailureAnalyzer:
    """分析因子回测失败的具体原因，指导下一轮迭代。"""

    # 6 种失败模式
    DIAGNOSES = [
        "too_strict", "too_loose", "no_signal",
        "noisy_but_directional", "reversed", "redundant", "inconsistent",
    ]

    def analyze(
        self,
        factor_name: str,
        backtest_result: dict,
        surgery_report: SurgeryReport | None = None,
    ) -> FailureDiagnosis:
        """分析失败原因。

        Args:
            factor_name: 因子名
            backtest_result: 包含 ic_mean, icir, avg_sample_per_day, max_correlation, ic_series 等
            surgery_report: 可选的 SurgeryReport，若提供则优先使用其诊断

        Returns:
            FailureDiagnosis
        """
        # ---- 手术台诊断优先 ----
        if surgery_report is not None:
            diag = surgery_report.diagnosis
            if diag == "universally_effective":
                # 因子已通过验收，不应出现在失败路径，走原有逻辑
                pass
            elif diag == "regime_dependent":
                return FailureDiagnosis(
                    diagnosis="regime_dependent",
                    suggestion=(
                        f"因子依赖特定市场状态 (best_regime={surgery_report.best_regime})，"
                        "建议添加 regime 过滤条件，仅在该状态下启用"
                    ),
                    details={
                        "best_regime": surgery_report.best_regime,
                        "regime_breakdown": [
                            {"regime": r.regime, "ic_mean": r.ic_mean, "icir": r.icir, "effective": r.effective}
                            for r in surgery_report.regime_breakdown
                        ],
                    },
                )
            elif diag == "emotion_dependent":
                return FailureDiagnosis(
                    diagnosis="emotion_dependent",
                    suggestion=(
                        f"因子依赖特定情绪环境 (best_emotion={surgery_report.best_emotion})，"
                        "建议添加 zt_count 条件过滤，仅在匹配的情绪区间启用"
                    ),
                    details={
                        "best_emotion": surgery_report.best_emotion,
                        "emotion_breakdown": [
                            {"bucket": e.bucket, "ic_mean": e.ic_mean, "icir": e.icir, "effective": e.effective}
                            for e in surgery_report.emotion_breakdown
                        ],
                    },
                )
            elif diag == "time_decayed":
                return FailureDiagnosis(
                    diagnosis="time_decayed",
                    suggestion="因子存在明显时间衰减，建议缩短回看窗口并尝试反转方向",
                    details={
                        "time_decay": [
                            {"segment": t.segment, "ic_mean": t.ic_mean, "sample_days": t.sample_days}
                            for t in surgery_report.time_decay
                        ],
                    },
                )
            # no_signal → fall through to existing logic

        ic = backtest_result.get("ic_mean", 0.0)
        icir = backtest_result.get("icir", 0.0)
        sample = backtest_result.get("avg_sample_per_day", 0)
        correlation = backtest_result.get("max_correlation", 0.0)
        ic_series = backtest_result.get("ic_series", pd.Series(dtype=float))

        details = {
            "ic": ic,
            "icir": icir,
            "sample": sample,
            "correlation": correlation,
        }

        # 1. IC ≈ 0 → 无信号
        if abs(ic) < 0.01:
            if sample < 5:
                return FailureDiagnosis(
                    diagnosis="too_strict",
                    suggestion="条件太严格，样本不足。放宽阈值或减少条件数量",
                    details={**details, "current_sample": sample},
                )
            elif sample > 100:
                return FailureDiagnosis(
                    diagnosis="too_loose",
                    suggestion="条件太宽，几乎所有股票都满足，无区分度。收紧条件或增加过滤",
                    details={**details, "current_sample": sample},
                )
            else:
                return FailureDiagnosis(
                    diagnosis="no_signal",
                    suggestion="假说可能不成立。尝试不同的数据维度或更换理论",
                    details=details,
                )

        # 2. IC 为负 → 反向
        if ic < -0.01:
            return FailureDiagnosis(
                diagnosis="reversed",
                suggestion="因子方向反了。反转因子值（1 - factor 或 -factor）重新测试",
                details=details,
            )

        # 3. 高相关 → 冗余
        if correlation > 0.7:
            corr_with = backtest_result.get("most_correlated_factor", "unknown")
            return FailureDiagnosis(
                diagnosis="redundant",
                suggestion=f"与已有因子 {corr_with} 高度相关（r={correlation:.2f}）。加入差异化条件",
                details={**details, "correlated_with": corr_with},
            )

        # 4. IC > 0 但不稳定
        if ic > 0.01 and icir < 0.5:
            if isinstance(ic_series, pd.Series) and len(ic_series) > 0:
                ic_positive_pct = float((ic_series > 0).mean())
            else:
                ic_positive_pct = 0.5

            if ic_positive_pct > 0.6:
                return FailureDiagnosis(
                    diagnosis="noisy_but_directional",
                    suggestion="信号有方向性但噪音大。加入 regime 过滤（如只在情绪强势时启用）或增加平滑窗口",
                    details={**details, "ic_positive_pct": ic_positive_pct},
                )
            else:
                return FailureDiagnosis(
                    diagnosis="inconsistent",
                    suggestion="信号不稳定。尝试增加平滑窗口、更换时间窗口、或加入 regime 条件",
                    details={**details, "ic_positive_pct": ic_positive_pct},
                )

        # 5. 检查衰减
        if isinstance(ic_series, pd.Series) and len(ic_series) >= 10:
            mid = len(ic_series) // 2
            ic_first = float(ic_series.iloc[:mid].mean())
            ic_second = float(ic_series.iloc[mid:].mean())
            if ic_first > 0.03 and ic_second < 0.01:
                return FailureDiagnosis(
                    diagnosis="noisy_but_directional",
                    suggestion="因子存在衰减（前半段IC高后半段低）。尝试更短的回看窗口或标记为 regime-dependent",
                    details={**details, "ic_first_half": ic_first, "ic_second_half": ic_second, "decay_detected": True},
                )

        # 默认
        return FailureDiagnosis(
            diagnosis="no_signal",
            suggestion="未明确诊断。检查因子逻辑和数据质量",
            details=details,
        )
