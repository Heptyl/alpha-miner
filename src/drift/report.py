"""漂移报告 — 汇总所有因子状态 + 漂移事件。"""

from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from src.data.storage import Storage
from src.drift.ic_tracker import ICTracker
from src.drift.cusum import detect_changepoints
from src.drift.regime import RegimeDetector
from src.factors.registry import FactorRegistry


class DriftReport:
    """生成因子漂移报告。"""

    def __init__(self, db: Storage):
        self.db = db
        self.ic_tracker = ICTracker(db)
        self.regime_detector = RegimeDetector(db)
        self.registry = FactorRegistry()

    def generate(self, as_of: datetime, ic_window: int = 20) -> dict:
        """生成完整漂移报告。

        Returns:
            {
                "date": str,
                "regime": RegimeInfo,
                "factors": [{"name": ..., "status": ..., "ic": ..., ...}],
                "changepoints": [...],
                "alerts": [...],
            }
        """
        date_str = as_of.strftime("%Y-%m-%d")

        # 1. 市场状态
        regime = self.regime_detector.detect(as_of)

        # 2. 所有因子状态
        factor_names = self.registry.list_names()
        factors = []
        for name in factor_names:
            status = self.ic_tracker.current_status(name, window=ic_window)
            factors.append(status)

        # 3. 漂移事件检测
        alerts = []
        changepoints = []
        for name in factor_names:
            # 取 IC 序列检测变点
            ic_df = self.ic_tracker.compute_ic_series(
                name,
                (as_of - timedelta(days=ic_window * 3)).strftime("%Y-%m-%d"),
                date_str,
                forward_days=1,
                window=ic_window,
            )
            if not ic_df.empty and len(ic_df) > 10:
                ic_series = ic_df["ic"].dropna()
                cp_result = detect_changepoints(ic_series, threshold=1.5, min_segment=5)
                if cp_result.changepoints:
                    for cp_idx in cp_result.changepoints:
                        cp_date = ic_series.index[cp_idx] if cp_idx < len(ic_series) else "unknown"
                        alerts.append({
                            "type": "changepoint",
                            "factor": name,
                            "position": int(cp_idx),
                            "detail": f"IC 变点 @ {cp_date}",
                        })
                    changepoints.extend(cp_result.changepoints)

            # 检查因子状态告警
            status = self.ic_tracker.current_status(name, window=ic_window)
            if status["status"] == "dead":
                alerts.append({
                    "type": "dead_factor",
                    "factor": name,
                    "detail": f"IC_avg={status['ic_avg']:.4f} → 因子已失效",
                })
            elif status["status"] == "negative":
                alerts.append({
                    "type": "negative_ic",
                    "factor": name,
                    "detail": f"IC_avg={status['ic_avg']:.4f} → 因子反向",
                })
            elif status["trend"] == "declining":
                alerts.append({
                    "type": "declining",
                    "factor": name,
                    "detail": f"IC 趋势下降中",
                })

        return {
            "date": date_str,
            "regime": regime,
            "factors": factors,
            "changepoints": changepoints,
            "alerts": alerts,
        }

    def format_rich(self, report: dict) -> str:
        """格式化为 rich 文本输出。"""
        lines = []
        lines.append("=" * 60)
        lines.append(f"  Alpha Miner 漂移报告 — {report['date']}")
        lines.append("=" * 60)

        # 市场状态
        regime = report["regime"]
        lines.append(f"\n[市场状态] {regime.regime} (置信度: {regime.confidence:.2f})")
        if regime.details:
            for k, v in regime.details.items():
                lines.append(f"  {k}: {v}")

        # 因子状态表
        lines.append(f"\n[因子状态]")
        lines.append(f"  {'因子':<25} {'状态':<10} {'最新IC':>8} {'IC均值':>8} {'ICIR':>8} {'胜率':>8} {'趋势':<10}")
        lines.append("  " + "-" * 85)
        for f in report["factors"]:
            ic_avg = f"{f['ic_avg']:.4f}" if not np.isnan(f.get('ic_avg', np.nan)) else "N/A"
            latest = f"{f['latest_ic']:.4f}" if not np.isnan(f.get('latest_ic', np.nan)) else "N/A"
            icir = f"{f['icir']:.2f}" if not np.isnan(f.get('icir', np.nan)) else "N/A"
            wr = f"{f['win_rate']:.2f}" if not np.isnan(f.get('win_rate', np.nan)) else "N/A"
            lines.append(
                f"  {f['factor_name']:<25} {f['status']:<10} {latest:>8} {ic_avg:>8} {icir:>8} {wr:>8} {f['trend']:<10}"
            )

        # 告警
        if report["alerts"]:
            lines.append(f"\n[告警] ({len(report['alerts'])} 条)")
            for alert in report["alerts"]:
                lines.append(f"  [{alert['type']}] {alert['factor']}: {alert['detail']}")
        else:
            lines.append(f"\n[告警] 无")

        lines.append("\n" + "=" * 60)
        return "\n".join(lines)
