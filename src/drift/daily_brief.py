"""盘后决策简报 — 三大交付物。

交付物一：市场温度计（regime + 情绪 + 有效因子 + 建议仓位）
交付物二：候选决策卡片（综合打分 Top N，含因子贡献、反向视角、建议）
交付物三：持仓风险预警（三班组条件 + 资金流背离）
"""

import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Optional

import numpy as np
import pandas as pd

from src.data.storage import Storage
from src.drift.ic_tracker import ICTracker
from src.drift.regime import RegimeDetector
from src.factors.registry import FactorRegistry

# ── 数据结构 ──────────────────────────────────────────────

@dataclass
class FactorContribution:
    """单个因子的贡献信息。"""
    name: str
    value: float        # 因子原始值
    ic: float           # IC 均值
    weight: float       # regime 权重
    contribution: float # value * |ic| * weight
    pct: float = 0.0    # 占总分百分比


@dataclass
class CandidateCard:
    """候选决策卡片。"""
    stock_code: str
    stock_name: str = ""
    score_raw: float = 0.0      # 加权原始分
    score_normalized: float = 0.0  # 归一化到 0-10
    factor_contributions: list[FactorContribution] = field(default_factory=list)
    red_flags: list[str] = field(default_factory=list)     # 反向视角
    tags: list[str] = field(default_factory=list)          # 连板/题材等标签
    suggestion: str = "观望"  # 买入 / 观望 / 回避
    suggestion_reason: str = ""


@dataclass
class HoldingAlert:
    """持仓风险预警。"""
    stock_code: str
    stock_name: str = ""
    danger_signals: list[str] = field(default_factory=list)
    santai_risk_score: int = 0   # 0-6
    santai_triggers: list[str] = field(default_factory=list)
    advice: str = ""


@dataclass
class MarketThermometer:
    """市场温度计。"""
    date: str
    regime: str = ""
    regime_cn: str = ""
    zt_count: int = 0
    dt_count: int = 0
    highest_board: int = 0
    zb_count: int = 0
    emotion_level: str = "未知"      # 极弱 / 弱 / 中性 / 偏强 / 强
    suggested_position: float = 0.0  # 0-1 建议仓位比例
    active_factors: list[dict] = field(default_factory=list)  # {name, ic, trend, status}
    regime_note: str = ""
    data_sufficient: bool = False


# ── regime 权重表 ────────────────────────────────────────

REGIME_WEIGHTS = {
    "board_rally": {
        "consecutive_board": 2.0,
        "zt_dt_ratio": 1.5,
        "leader_clarity": 1.8,
        "turnover_rank": 1.2,
        "main_flow_intensity": 1.3,
    },
    "theme_rotation": {
        "theme_crowding": 1.8,
        "narrative_velocity": 1.5,
        "theme_lifecycle": 1.5,
        "leader_clarity": 1.2,
    },
    "low_volume": {},   # 地量不操作
    "broad_move": {
        "main_flow_intensity": 1.5,
        "turnover_rank": 1.3,
    },
    "normal": {},       # 无特殊加权
}

REGIME_CN = {
    "board_rally": "连板潮",
    "theme_rotation": "题材轮动",
    "low_volume": "地量",
    "broad_move": "普涨/普跌",
    "normal": "正常",
}

REGIME_NOTES = {
    "board_rally": "连板因子权重 70%，量价因子权重 30%。优先看龙头辨识度和封板质量。",
    "theme_rotation": "叙事因子权重 70%，量价因子权重 30%。题材轮动期优先看叙事，不追高位连板。",
    "low_volume": "地量环境，因子信号稀疏。建议空仓或极低仓位等待放量信号。",
    "broad_move": "普涨/普跌环境，系统性风险/机会主导，因子选股能力下降。",
    "normal": "各因子等权参与，综合打分。",
}

# 情绪级别 → 建议仓位映射
EMOTION_POSITION = {
    "极弱": 0.0,
    "弱": 0.2,
    "中性": 0.4,
    "偏强": 0.6,
    "强": 0.8,
}


def normalize_holdings(holdings: list[object]) -> list[str]:
    """Validate holding codes as six ASCII digits without numeric conversion."""
    normalized = []
    for raw_code in holdings:
        if not isinstance(raw_code, str):
            raise ValueError(
                f"持仓代码必须是 6 位数字字符串，收到类型 {type(raw_code).__name__}"
            )
        code = raw_code.strip()
        if re.fullmatch(r"[0-9]{6}", code) is None:
            raise ValueError(f"持仓代码必须是 6 位数字字符串，收到: {raw_code!r}")
        normalized.append(code)
    return normalized


# ── 核心类 ──────────────────────────────────────────────

class DailyBrief:
    """盘后决策简报生成器。

    输入：
    - 当日因子值（factor_values 表）
    - 因子 IC 状态（ic_tracker）
    - 市场数据（market_emotion + zt_pool + fund_flow）
    - 用户持仓（可选）

    输出：
    - 市场温度计
    - 候选决策卡片（Top N）
    - 持仓风险预警
    """

    def __init__(self, db: Storage):
        self.db = db
        self.ic_tracker = ICTracker(db)
        self.regime_detector = RegimeDetector(db)
        self.registry = FactorRegistry()

    @staticmethod
    def _no_data_factor_status(factor_name: str) -> dict:
        """Return the explicit status used when no finite IC can be calculated."""
        return {
            "factor_name": factor_name,
            "latest_ic": np.nan,
            "ic_avg": np.nan,
            "icir": np.nan,
            "win_rate": np.nan,
            "trend": "unknown",
            "status": "no_data",
        }

    def _current_factor_status(self, factor_name: str, window: int = 20) -> dict:
        """Calculate the brief's status fields without empty-slice means.

        ``ICTracker.current_status`` also calculates an unused profit/loss ratio;
        a one-sided IC window makes that metric take the mean of an empty slice.
        The brief only needs IC, ICIR, win rate, trend, and status, so calculate
        those same fields here after checking the aligned samples are finite.
        """
        end = datetime.now()
        start = end - timedelta(days=window * 3)
        conn = self.db._get_conn()
        try:
            trade_dates = [
                row[0]
                for row in conn.execute(
                    "SELECT DISTINCT trade_date FROM factor_values "
                    "WHERE factor_name = ? AND trade_date >= ? AND trade_date <= ? "
                    "ORDER BY trade_date",
                    (factor_name, start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")),
                ).fetchall()
            ]
        finally:
            conn.close()

        ic_values: list[float] = []
        for trade_date in trade_dates:
            factor_values = self.ic_tracker._get_factor_values(factor_name, trade_date)
            forward_returns = self.ic_tracker._get_forward_returns(trade_date, 1)
            common = factor_values.index.intersection(forward_returns.index)
            if len(common) < 5:
                ic_values.append(np.nan)
                continue

            aligned = pd.DataFrame({
                "factor": pd.to_numeric(factor_values.loc[common], errors="coerce"),
                "return": pd.to_numeric(forward_returns.loc[common], errors="coerce"),
            }).replace([np.inf, -np.inf], np.nan).dropna()
            if (
                len(aligned) < 5
                or aligned["factor"].nunique() < 2
                or aligned["return"].nunique() < 2
            ):
                ic_values.append(np.nan)
                continue

            ic_values.append(self.ic_tracker._compute_spearman_ic(
                aligned["factor"], aligned["return"]
            ))

        if not ic_values:
            return self._no_data_factor_status(factor_name)

        ic_series = pd.Series(ic_values, dtype=float).replace([np.inf, -np.inf], np.nan)
        window_values = ic_series.tail(window)
        if window_values.empty or not np.isfinite(window_values).all():
            return self._no_data_factor_status(factor_name)

        latest_value = ic_series.iloc[-1]
        latest_ic = float(latest_value) if np.isfinite(latest_value) else np.nan
        ic_avg = float(window_values.mean())
        ic_std = float(window_values.std())
        icir = ic_avg / ic_std if np.isfinite(ic_std) and ic_std != 0 else np.nan
        win_rate = float((window_values > 0).sum() / len(window_values))

        recent_ics = ic_series.dropna().tail(5).to_numpy()
        if len(recent_ics) >= 3:
            slope = np.polyfit(range(len(recent_ics)), recent_ics, 1)[0]
            if slope > 0.01:
                trend = "improving"
            elif slope < -0.01:
                trend = "declining"
            else:
                trend = "stable"
        else:
            trend = "unknown"

        if abs(ic_avg) < 0.02:
            status = "dead"
        elif ic_avg > 0.03 and win_rate > 0.5:
            status = "healthy"
        elif ic_avg > 0.02:
            status = "weak"
        else:
            status = "negative"

        return {
            "factor_name": factor_name,
            "latest_ic": latest_ic,
            "ic_avg": ic_avg,
            "icir": icir,
            "win_rate": win_rate,
            "trend": trend,
            "status": status,
        }

    # ================================================================
    # 交付物一：市场温度计
    # ================================================================

    def build_thermometer(self, as_of: datetime, report_date: str = "") -> MarketThermometer:
        """构建市场温度计。

        Args:
            as_of: 时间隔离点（必须大于数据 snapshot_time）
            report_date: 报告目标交易日（默认取 as_of 前一天）
        """
        if not report_date:
            # 默认取 as_of 前一天作为报告日
            from datetime import timedelta
            report_date = (as_of - timedelta(days=1)).strftime("%Y-%m-%d")
        thermo = MarketThermometer(date=report_date)

        # 1. regime
        regime_info = self.regime_detector.detect(as_of)
        thermo.regime = regime_info.regime
        thermo.regime_cn = REGIME_CN.get(regime_info.regime, regime_info.regime)
        thermo.regime_note = REGIME_NOTES.get(regime_info.regime, "")

        # 2. 市场情绪指标
        market_df = self.db.query("market_emotion", as_of,
                                  where="trade_date = ?", params=(report_date,))
        if not market_df.empty:
            row = market_df.iloc[-1]
            market_values = pd.to_numeric(
                row.reindex(["zt_count", "dt_count", "highest_board"]),
                errors="coerce",
            )
            if np.isfinite(market_values).all():
                thermo.zt_count = int(market_values["zt_count"])
                thermo.dt_count = int(market_values["dt_count"])
                thermo.highest_board = int(market_values["highest_board"])
                thermo.data_sufficient = True

        # 炸板数
        zb_df = self.db.query("zb_pool", as_of,
                              where="trade_date = ?", params=(report_date,))
        thermo.zb_count = len(zb_df) if not zb_df.empty else 0

        # 3. 情绪级别判定
        if thermo.data_sufficient:
            thermo.emotion_level = self._classify_emotion(thermo)
            thermo.suggested_position = EMOTION_POSITION.get(thermo.emotion_level, 0.3)
        else:
            thermo.regime_cn = "不可判断"
            thermo.regime_note = "市场情绪数据不足，无法判断市场状态与建议仓位。"
            thermo.emotion_level = "未知"
            thermo.suggested_position = 0.0

        # 4. 有效因子列表
        factor_names = self.registry.list_factors()
        active = []
        for name in factor_names:
            status = self._current_factor_status(name, window=20)
            if status["status"] not in ("no_data",):
                ic = status.get("ic_avg", 0)
                if np.isnan(ic):
                    ic = 0
                active.append({
                    "name": name,
                    "ic": ic,
                    "trend": status.get("trend", "unknown"),
                    "status": status["status"],
                })
        # 按 |IC| 降序
        active.sort(key=lambda x: abs(x["ic"]), reverse=True)
        thermo.active_factors = active

        return thermo

    def _classify_emotion(self, thermo: MarketThermometer) -> str:
        """基于多指标判定情绪级别。"""
        score = 0
        zt = thermo.zt_count
        dt = thermo.dt_count
        zb = thermo.zb_count
        hb = thermo.highest_board

        # 涨停数
        if zt >= 80:
            score += 3
        elif zt >= 50:
            score += 2
        elif zt >= 20:
            score += 1
        else:
            score -= 1

        # 跌停数
        if dt >= 20:
            score -= 3
        elif dt >= 10:
            score -= 2
        elif dt >= 5:
            score -= 1

        # 炸板率
        total_attempts = zt + zb
        if total_attempts > 0:
            zb_rate = zb / total_attempts
            if zb_rate > 0.4:
                score -= 2
            elif zb_rate > 0.25:
                score -= 1
            elif zb_rate < 0.15:
                score += 1

        # 最高板
        if hb >= 6:
            score += 2
        elif hb >= 4:
            score += 1

        # 映射
        if score >= 5:
            return "强"
        elif score >= 3:
            return "偏强"
        elif score >= 1:
            return "中性"
        elif score >= -1:
            return "弱"
        else:
            return "极弱"

    # ================================================================
    # 交付物二：候选决策卡片
    # ================================================================

    def _compute_dynamic_regime_weights(
        self, as_of: datetime, regime: str
    ) -> dict[str, float]:
        """Compute dynamic regime weights based on recent IC performance.

        For each registered factor, uses recent IC mean as base weight,
        disables factors with negative IC (< -0.01), adjusts by the
        hardcoded REGIME_WEIGHTS prior, and normalizes to sum to ~1.0.

        Falls back to hardcoded REGIME_WEIGHTS if insufficient data
        (< 10 days of IC history) or any error occurs.
        """
        try:
            hardcoded = REGIME_WEIGHTS.get(regime, {})

            # If regime has no hardcoded weights (e.g. "low_volume", "normal"),
            # return empty dict — same as before.
            if not hardcoded:
                return hardcoded

            factor_names = self.registry.list_factors()
            if not factor_names:
                return hardcoded

            raw_weights: dict[str, float] = {}
            sufficient_data = True
            days_threshold = 10

            for name in factor_names:
                # Only compute dynamic weights for factors present in
                # the hardcoded regime config.
                if name not in hardcoded:
                    continue

                # Retrieve IC history through time-isolated query.
                ic_df = self.db.query(
                    "ic_series", as_of,
                    where="factor_name = ?",
                    params=(name,),
                )

                if ic_df is None or ic_df.empty or len(ic_df) < days_threshold:
                    sufficient_data = False
                    break

                # Use the most recent finite IC values; sparse windows cannot
                # support a dynamic weight.
                ic_series = (
                    pd.to_numeric(ic_df["ic_value"], errors="coerce")
                    if "ic_value" in ic_df.columns
                    else pd.Series(dtype=float)
                )
                recent_ic = (
                    ic_series.tail(days_threshold)
                    .replace([np.inf, -np.inf], np.nan)
                    .dropna()
                )
                if len(recent_ic) < days_threshold:
                    sufficient_data = False
                    break

                ic_mean = float(recent_ic.mean())

                # Negative IC in current regime → disable factor.
                if ic_mean < -0.01:
                    raw_weights[name] = 0.0
                else:
                    base_weight = abs(ic_mean)
                    # Multiply by hardcoded prior for this regime.
                    prior = hardcoded.get(name, 1.0)
                    raw_weights[name] = base_weight * prior

            if not sufficient_data:
                return dict(hardcoded)

            # Normalize so weights sum to ~1.0.
            total = sum(raw_weights.values())
            if total <= 0:
                return dict(hardcoded)

            normalized = {k: v / total for k, v in raw_weights.items()}
            return normalized

        except Exception:
            # Any error → fall back to hardcoded weights.
            return dict(REGIME_WEIGHTS.get(regime, {}))

    def build_candidates(
        self,
        as_of: datetime,
        top_n: int = 10,
        report_date: str = "",
    ) -> list[CandidateCard]:
        """综合打分，返回 Top N 候选卡片。"""
        if not report_date:
            from datetime import timedelta
            report_date = (as_of - timedelta(days=1)).strftime("%Y-%m-%d")

        # 1. 获取 regime 权重（动态计算，不足时回退到硬编码）
        regime_info = self.regime_detector.detect(as_of)
        rw = self._compute_dynamic_regime_weights(as_of, regime_info.regime)

        # 2. 收集所有有效因子的截面值和 IC
        factor_names = self.registry.list_factors()
        factor_data = {}  # name -> {ic, weight, values: pd.Series}

        for name in factor_names:
            status = self._current_factor_status(name, window=20)

            # 无有限 IC 时不可评分，避免用虚构的默认值生成候选。
            if status["status"] == "no_data":
                continue
            elif status["status"] in ("dead", "negative"):
                continue
            else:
                ic = status.get("ic_avg", 0)
                if np.isnan(ic):
                    ic = 0

            fv_df = self.db.query(
                "factor_values", as_of,
                where="factor_name = ? AND trade_date = ?",
                params=(name, report_date),
            )
            if fv_df.empty:
                continue

            fv_df = fv_df.sort_values("snapshot_time").groupby("stock_code").last().reset_index()
            values = pd.to_numeric(
                fv_df.set_index("stock_code")["factor_value"], errors="coerce"
            ).replace([np.inf, -np.inf], np.nan).dropna()
            if values.empty:
                continue

            weight = abs(ic)
            if name in rw:
                weight *= rw[name]

            factor_data[name] = {"ic": ic, "weight": weight, "values": values}

        if not factor_data:
            return []

        # 3. 计算综合分
        # 综合分 = sum(factor_value_i * |ic_i| * regime_weight_i) / sum(|ic_i| * regime_weight_i) * 10
        all_codes = set()
        for fd in factor_data.values():
            all_codes.update(fd["values"].dropna().index)

        total_weight = sum(fd["weight"] for fd in factor_data.values())
        if total_weight == 0:
            return []

        cards = []
        for code in all_codes:
            raw_score = 0
            contributions = []
            red_flags = []

            for name, fd in factor_data.items():
                val = fd["values"].get(code, np.nan)
                if pd.isna(val):
                    continue

                contrib = val * fd["weight"]
                raw_score += contrib

                contributions.append(FactorContribution(
                    name=name,
                    value=round(val, 4),
                    ic=round(fd["ic"], 4),
                    weight=round(fd["weight"], 4),
                    contribution=round(contrib, 4),
                ))

                # 反向视角：因子值为负即为风险信号
                if val < -0.3:
                    red_flags.append(f"{name} = {val:.2f}（强负值）")
                elif val < 0:
                    red_flags.append(f"{name} = {val:.2f}（负值）")

            if not contributions:
                continue

            normalized = (raw_score / total_weight) * 10 if total_weight > 0 else 0

            # 排序贡献，计算百分比
            contributions.sort(key=lambda c: abs(c.contribution), reverse=True)
            total_contrib = sum(abs(c.contribution) for c in contributions)
            for c in contributions:
                c.pct = round(abs(c.contribution) / total_contrib * 100, 1) if total_contrib > 0 else 0

            # 获取标签（连板、题材等）
            tags = self._get_stock_tags(code, as_of, report_date)

            # 建议判定
            severe_flags = sum(1 for c in contributions if c.value < -0.5)
            suggestion, reason = self._make_suggestion(normalized, len(red_flags), severe_flags)

            cards.append(CandidateCard(
                stock_code=code,
                score_raw=round(raw_score, 4),
                score_normalized=round(max(0, min(10, normalized)), 1),
                factor_contributions=contributions,
                red_flags=red_flags,
                tags=tags,
                suggestion=suggestion,
                suggestion_reason=reason,
            ))

        # 按 normalized score 降序
        cards.sort(key=lambda c: c.score_normalized, reverse=True)
        return cards[:top_n]

    def _get_stock_tags(self, code: str, as_of: datetime, report_date: str = "") -> list[str]:
        """获取股票标签：连板数、概念等。"""
        tags = []
        if not report_date:
            from datetime import timedelta
            report_date = (as_of - timedelta(days=1)).strftime("%Y-%m-%d")

        # 连板
        zt_df = self.db.query("zt_pool", as_of,
                              where="stock_code = ? AND trade_date = ?",
                              params=(code, report_date))
        if not zt_df.empty and "consecutive_zt" in zt_df.columns:
            boards = int(zt_df.iloc[-1]["consecutive_zt"])
            if boards >= 2:
                tags.append(f"{boards}连板")

        # 概念
        concept_df = self.db.query("concept_mapping", as_of,
                                   where="stock_code = ?",
                                   params=(code,))
        if not concept_df.empty and "concept_name" in concept_df.columns:
            top_concepts = concept_df["concept_name"].head(2).tolist()
            tags.extend(top_concepts)

        return tags

    def _make_suggestion(
        self,
        score: float,
        red_count: int,
        severe_count: int,
    ) -> tuple[str, str]:
        """综合判定建议。"""
        if score >= 7 and severe_count == 0:
            return "买入", f"综合分 {score:.1f}，无严重反向因素"
        elif score >= 7 and severe_count <= 1:
            return "观望", f"综合分 {score:.1f}，但有 {severe_count} 个严重反向因素"
        elif score >= 5:
            return "观望", f"综合分 {score:.1f}，信号不够强"
        else:
            return "回避", f"综合分 {score:.1f}，反向因素较强"

    # ================================================================
    # 交付物三：持仓风险预警
    # ================================================================

    def build_holding_alerts(
        self,
        as_of: datetime,
        holdings: list[str],
        report_date: str = "",
    ) -> list[HoldingAlert]:
        """对持仓股跑风险检测。"""
        if not report_date:
            from datetime import timedelta
            report_date = (as_of - timedelta(days=1)).strftime("%Y-%m-%d")
        alerts = []
        for code in normalize_holdings(list(holdings)):
            alert = self._check_holding(code, as_of, report_date)
            if alert:
                alerts.append(alert)
        return alerts

    def _check_holding(self, code: str, as_of: datetime, report_date: str = "") -> Optional[HoldingAlert]:
        """检查单只持仓股风险。"""
        if not report_date:
            from datetime import timedelta
            report_date = (as_of - timedelta(days=1)).strftime("%Y-%m-%d")
        alert = HoldingAlert(stock_code=code)

        # 1. 三班组风险检测（小市值 + 低换手 + 无题材）
        santai_score = 0
        santai_triggers = []

        price_df = self.db.query("daily_price", as_of,
                                 where="stock_code = ? AND trade_date = ?",
                                 params=(code, report_date))
        if not price_df.empty:
            row = price_df.iloc[-1]
            turnover = float(row.get("turnover_rate", 0))

            # 小市值判断（需要 amount 和 volume 推断）
            # 简化：换手率 < 10% 视为低换手
            if turnover > 0 and turnover < 10:
                santai_score += 1
                santai_triggers.append(f"低换手 {turnover:.1f}%")

            # 无题材判断
            concept_df = self.db.query("concept_mapping", as_of,
                                       where="stock_code = ?",
                                       params=(code,))
            if concept_df.empty:
                santai_score += 1
                santai_triggers.append("无题材标签")

        # 三班组需要至少 2/3 触发
        alert.santai_risk_score = santai_score
        alert.santai_triggers = santai_triggers

        # 2. 资金流背离检测
        fund_df = self.db.query("fund_flow", as_of,
                                where="stock_code = ? AND trade_date = ?",
                                params=(code, report_date))
        if not fund_df.empty:
            row = fund_df.iloc[-1]
            super_large = float(row.get("super_large_net", 0))
            large = float(row.get("large_net", 0))
            main_net = float(row.get("main_net_inflow", 0))

            # 超大单买入 + 大单卖出 = 资金流背离
            if super_large > 0 and large < 0:
                alert.danger_signals.append(
                    f"超大单买入 + 大单卖出 = 资金流背离"
                    f"（超大单 {super_large/1e4:.0f}万 / 大单 {large/1e4:.0f}万）"
                )

            # 主力净流出
            if main_net < 0:
                alert.danger_signals.append(
                    f"主力净流出 {main_net/1e4:.0f}万"
                )

        # 3. 换手率安全线
        if not price_df.empty:
            turnover = float(price_df.iloc[-1].get("turnover_rate", 0))
            if turnover > 0 and turnover < 10:
                alert.danger_signals.append(
                    f"换手率 {turnover:.1f}%，低于安全线 10%"
                )

        # 4. 题材拥挤度
        concept_df = self.db.query("concept_mapping", as_of,
                                   where="stock_code = ?",
                                   params=(code,))
        if not concept_df.empty and "concept_name" in concept_df.columns:
            for concept in concept_df["concept_name"].unique():
                # 统计同概念涨停数
                zt_concept = self.db.query("zt_pool", as_of,
                                           where="trade_date = ?",
                                           params=(report_date,))
                if not zt_concept.empty:
                    # 简化：统计涨停股数占总数比例
                    total_stocks = len(self.db.query("daily_price", as_of,
                                                     where="trade_date = ?",
                                                     params=(report_date,)))
                    zt_count = len(zt_concept)
                    if total_stocks > 0:
                        crowd_ratio = zt_count / total_stocks
                        if crowd_ratio > 0.02:  # > 2% 视为拥挤
                            alert.danger_signals.append(
                                f"涨停拥挤度 {crowd_ratio:.1%}"
                            )

        # 5. 综合建议
        has_holding_data = not price_df.empty or not fund_df.empty or not concept_df.empty
        if not has_holding_data:
            alert.advice = "数据不足，持仓风险不可判断"
        elif alert.santai_risk_score >= 2:
            alert.advice = "三班组特征明显，明日竞价观察，高开 3% 以上考虑减仓"
        elif alert.danger_signals:
            alert.advice = "存在风险信号，注意盘面变化"
        else:
            alert.advice = "无明显风险信号"

        return alert

    # ================================================================
    # 格式化输出
    # ================================================================

    def format_thermometer(self, thermo: MarketThermometer) -> str:
        """格式化市场温度计。"""
        lines = []
        w = 50

        lines.append("┌" + "─" * w + "┐")
        header = f"  {thermo.date}  市场温度：{thermo.regime_cn}"
        lines.append(f"│{header:<{w}}│")
        stats = f"  涨停 {thermo.zt_count} │ 跌停 {thermo.dt_count} │ 最高板 {thermo.highest_board} │ 炸板 {thermo.zb_count}"
        lines.append(f"│{stats:<{w}}│")
        lines.append("│" + " " * w + "│")

        # 情绪 + 仓位
        if thermo.data_sufficient:
            emotion_icon = {"极弱": "❄️", "弱": "☁️", "中性": "⛅", "偏强": "⚡", "强": "🔥"}
            icon = emotion_icon.get(thermo.emotion_level, "")
            emotion_line = f"  情绪级别：{icon} {thermo.emotion_level}（市场情绪参考）"
            pos_line = f"  市场情绪参考仓位上限：{thermo.suggested_position:.0%}（非交易建议）"
        else:
            emotion_line = "  情绪级别：数据不足/不可判断"
            pos_line = "  市场情绪参考仓位上限：数据不足/不可判断（非交易建议）"
        lines.append(f"│{emotion_line:<{w}}│")
        lines.append(f"│{pos_line:<{w}}│")
        lines.append("│" + " " * w + "│")

        # 有效因子
        lines.append(f"│  当前有效因子（按 |IC| 排序）：{' ' * (w - 22)}│")

        status_icon = {"healthy": "✅", "weak": "⚠️ ", "dead": "❌", "negative": "❌", "no_data": "⬜"}
        trend_arrow = {
            "improving": "↑",
            "declining": "↓",
            "stable": "→",
            "unknown": "未知/数据不足",
        }

        for f in thermo.active_factors[:8]:
            si = status_icon.get(f["status"], "⬜")
            ta = trend_arrow.get(f["trend"], "未知/数据不足")
            ic_str = f"{f['ic']:.2f}" if not np.isnan(f["ic"]) else "N/A"
            line = f"  {si} {f['name']:<22} IC={ic_str:>6}  趋势{ta}"
            lines.append(f"│{line:<{w}}│")

        # regime 说明
        lines.append("│" + " " * w + "│")
        note = thermo.regime_note
        # 分行显示
        while note:
            chunk = note[:w - 4]
            lines.append(f"│  {chunk:<{w-2}}│")
            note = note[w - 4:]

        lines.append("└" + "─" * w + "┘")
        return "\n".join(lines)

    def format_candidate_card(self, card: CandidateCard, max_contributions: int = 5) -> str:
        """格式化单张候选决策卡片。"""
        lines = []
        w = 50

        # 头部
        tag_str = " │ ".join(card.tags) if card.tags else ""
        header = f"  {card.stock_code} {card.stock_name}"
        if tag_str:
            header += f"  {tag_str}"
        lines.append("┌" + "─" * w + "┐")
        score_line = f"  综合分 {card.score_normalized}/10"
        lines.append(f"│{header:<{w}}│")
        lines.append(f"│{score_line:<{w}}│")
        lines.append("├" + "─" * w + "┤")

        # 因子贡献
        lines.append(f"│  为什么上榜（因子贡献）：{' ' * (w - 20)}│")
        for c in card.factor_contributions[:max_contributions]:
            bar_len = int(c.pct / 10)
            bar_full = "■" * bar_len
            bar_empty = "□" * (10 - bar_len)
            line = f"  {bar_full}{bar_empty} {c.name:<16} (贡献 {c.pct:>4.0f}%)"
            lines.append(f"│{line:<{w}}│")

        # 反向视角
        if card.red_flags:
            lines.append("├" + "─" * w + "┤")
            lines.append(f"│  反向视角（为什么不该买）：{' ' * (w - 20)}│")
            for flag in card.red_flags[:4]:
                line = f"  ⚠ {flag}"
                if len(line) > w:
                    line = line[:w - 3] + "..."
                lines.append(f"│{line:<{w}}│")

        # 建议
        lines.append("├" + "─" * w + "┤")
        sug_icon = {"买入": "🟢", "观望": "🟡", "回避": "🔴"}
        si = sug_icon.get(card.suggestion, "⚪")
        sug_line = f"  {si} 建议：{card.suggestion}（{card.suggestion_reason}）"
        lines.append(f"│{sug_line:<{w}}│")

        lines.append("└" + "─" * w + "┘")
        return "\n".join(lines)

    def format_holding_alert(self, alert: HoldingAlert) -> str:
        """格式化持仓风险预警。"""
        lines = []
        w = 50

        lines.append("┌" + "─" * w + "┐")
        header = f"  ⚠ 持仓预警：{alert.stock_code} {alert.stock_name}"
        lines.append(f"│{header:<{w}}│")
        lines.append("│" + " " * w + "│")

        # 危险信号
        if alert.danger_signals:
            lines.append(f"│  危险信号：{' ' * (w - 12)}│")
            for sig in alert.danger_signals:
                line = f"  🔴 {sig}"
                if len(line) > w:
                    line = line[:w - 3] + "..."
                lines.append(f"│{line:<{w}}│")

        # 三班组风险
        if alert.santai_risk_score > 0:
            lines.append("│" + " " * w + "│")
            score_line = f"  三班组风险评分：{alert.santai_risk_score}/6（{'高' if alert.santai_risk_score >= 3 else '中等' if alert.santai_risk_score >= 2 else '低'}）"
            lines.append(f"│{score_line:<{w}}│")
            triggers = f"  触发条件：{'、'.join(alert.santai_triggers)}"
            if len(triggers) > w:
                triggers = triggers[:w - 3] + "..."
            lines.append(f"│{triggers:<{w}}│")

        # 建议
        lines.append("│" + " " * w + "│")
        advice_line = f"  建议：{alert.advice}"
        lines.append(f"│{advice_line:<{w}}│")

        lines.append("└" + "─" * w + "┘")
        return "\n".join(lines)

    def generate_full_report(
        self,
        as_of: datetime,
        holdings: Optional[list[str]] = None,
        top_n: int = 10,
        report_date: str = "",
        enable_strategy_scan: bool = False,
    ) -> str:
        """生成完整盘后简报。"""
        parts = []

        if not report_date:
            from datetime import timedelta
            report_date = (as_of - timedelta(days=1)).strftime("%Y-%m-%d")

        # 交付物一：市场温度计
        thermo = self.build_thermometer(as_of, report_date=report_date)
        parts.append(self.format_thermometer(thermo))

        # 如果情绪极弱，跳过候选卡片
        candidates = []
        if thermo.emotion_level in ("极弱",) and thermo.suggested_position == 0:
            parts.append("\n⚠ 情绪极弱，建议休息，不生成候选卡片。")
        else:
            # 交付物二：候选决策卡片
            candidates = self.build_candidates(as_of, top_n=top_n, report_date=report_date)
            if candidates:
                parts.append(f"\n候选决策卡片（Top {len(candidates)}）：")
                for card in candidates:
                    parts.append("")
                    parts.append(self.format_candidate_card(card))
            else:
                parts.append("\n（无有效候选 — 因子数据不足）")

        if not candidates:
            parts.append("\n当前可执行新增仓位：0%（本简报无有效候选）")

        # 交付物三：持仓风险预警
        if holdings:
            alerts = self.build_holding_alerts(as_of, holdings, report_date=report_date)
            if alerts:
                parts.append("\n持仓风险预警：")
                for alert in alerts:
                    parts.append("")
                    parts.append(self.format_holding_alert(alert))
            else:
                parts.append("\n持仓无风险预警。")

        # 交付物四：策略扫描信号
        if enable_strategy_scan:
            scan_text = self._strategy_scan(as_of, report_date)
            if scan_text:
                parts.append(scan_text)

        return "\n".join(parts)

    def _strategy_scan(self, as_of: datetime, report_date: str) -> str:
        """用预置策略扫描当日信号。"""
        try:
            from src.strategy.backtest_engine import BacktestEngine
            from src.strategy.loader import load_strategies
        except ImportError:
            return ""

        strategies = load_strategies()
        if not strategies:
            return ""

        engine = BacktestEngine(self.db)
        lines = ["\n策略扫描信号："]

        any_signal = False
        for s in strategies:
            universe = engine._get_universe(report_date, "zt_pool", as_of)
            signals = []
            for code in universe:
                if engine._check_entry(s.entry, code, report_date, as_of):
                    signals.append(code)

            if signals:
                any_signal = True
                sig_str = ", ".join(signals[:5])
                if len(signals) > 5:
                    sig_str += f" ...+{len(signals)-5}"
                lines.append(f"  ▸ {s.name}: {sig_str}")

        if not any_signal:
            lines.append("  （无策略信号）")

        return "\n".join(lines)
