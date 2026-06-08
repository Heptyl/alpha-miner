"""市场状态（Regime）识别。

基于市场数据识别当前 regime：
- board_rally: 连板潮（高度>=4，涨停数>=30）
- theme_rotation: 题材轮动（涨停数>=20但连板低）
- low_volume: 地量（成交额低于60日均值50%以下）
- broad_move: 普涨/普跌（涨跌比>3:1或<1:3）
"""

from dataclasses import dataclass
from datetime import datetime

import numpy as np
import pandas as pd

from src.data.storage import Storage


@dataclass
class RegimeInfo:
    """市场状态信息。"""
    regime: str          # board_rally / theme_rotation / low_volume / broad_move / normal
    confidence: float    # 0-1 置信度
    details: dict        # 补充信息


class RegimeDetector:
    """市场状态检测器。"""

    REGIMES = ["board_rally", "theme_rotation", "low_volume", "broad_move", "normal"]

    def __init__(self, db: Storage):
        self.db = db

    def detect(self, as_of: datetime) -> RegimeInfo:
        """检测 as_of 日期的市场状态。"""
        date_str = as_of.strftime("%Y-%m-%d")

        # 收集市场数据
        zt_df = self.db.query("zt_pool", as_of, where="trade_date = ?", params=(date_str,))
        price_df = self.db.query("daily_price", as_of, where="trade_date = ?", params=(date_str,))
        market_df = self.db.query("market_emotion", as_of, where="trade_date = ?", params=(date_str,))

        zt_count = len(zt_df) if not zt_df.empty else 0
        highest_board = 0
        if not zt_df.empty and "consecutive_zt" in zt_df.columns:
            highest_board = int(zt_df["consecutive_zt"].max())

        # 检查每个 regime
        candidates = []

        # 1. 连板潮
        if highest_board >= 4 and zt_count >= 30:
            conf = min((highest_board - 3) / 5 + (zt_count - 29) / 50, 1.0)
            candidates.append(("board_rally", conf, {
                "zt_count": zt_count, "highest_board": highest_board,
            }))

        # 2. 题材轮动
        if zt_count >= 20 and highest_board <= 3:
            conf = min((zt_count - 19) / 30, 1.0)
            candidates.append(("theme_rotation", conf, {
                "zt_count": zt_count, "highest_board": highest_board,
            }))

        # 3. 地量
        if not price_df.empty and "amount" in price_df.columns:
            total_amount = float(price_df["amount"].sum())
            # 取60日均成交额
            hist_df = self.db.query_range("daily_price", as_of, lookback_days=60)
            if not hist_df.empty:
                daily_totals = hist_df.groupby("trade_date")["amount"].sum()
                avg_amount = float(daily_totals.mean())
                if avg_amount > 0 and total_amount < avg_amount * 0.5:
                    conf = min((avg_amount * 0.5 - total_amount) / (avg_amount * 0.3), 1.0)
                    candidates.append(("low_volume", conf, {
                        "total_amount": total_amount, "avg_amount": avg_amount,
                    }))

        # 4. 普涨/普跌
        if not price_df.empty and "close" in price_df.columns and "open" in price_df.columns:
            pct = (price_df["close"] - price_df["open"]) / price_df["open"]
            up = int((pct > 0).sum())
            down = int((pct < 0).sum())
            total = up + down
            if total > 0:
                ratio = up / total
                if ratio > 0.75:
                    conf = min((ratio - 0.75) / 0.2, 1.0)
                    candidates.append(("broad_move", conf, {"direction": "up", "ratio": ratio}))
                elif ratio < 0.25:
                    conf = min((0.25 - ratio) / 0.2, 1.0)
                    candidates.append(("broad_move", conf, {"direction": "down", "ratio": ratio}))

        # 选择置信度最高的 regime
        if candidates:
            best = max(candidates, key=lambda x: x[1])
            return RegimeInfo(regime=best[0], confidence=best[1], details=best[2])

        return RegimeInfo(regime="normal", confidence=1.0, details={
            "zt_count": zt_count, "highest_board": highest_board,
        })


@dataclass
class PricingRegimeInfo:
    """定价权 regime 信息（决策B：与情绪 regime 正交的独立轴）。"""
    regime: str          # hot_money_led / quant_led / mixed
    confidence: float
    details: dict


class PricingRegimeDetector:
    """定价权 regime 检测器 —— 游资主导 vs 量化主导。

    判别特征（决策B）：
    - 龙头唯一性：当日 leader_clarity 高分位高 = 游资造妖、龙头清晰。
    - 连板高度衰减斜率：近 lookback 交易日最高连板数持续走低 = 量化套利压制高度。

    规则：
    - hot_money_led: 龙头清晰 且 高度未持续衰减
    - quant_led:     龙头不清晰 且 高度持续衰减
    - mixed:         其余（含数据不足）
    阈值为经验值，是人工闸门，可按盘面校准。
    """

    REGIMES = ["hot_money_led", "quant_led", "mixed"]

    def __init__(self, db: Storage, lookback: int = 6,
                 clarity_thr: float = 0.6, slope_thr: float = -0.15):
        self.db = db
        self.lookback = lookback
        self.clarity_thr = clarity_thr
        self.slope_thr = slope_thr

    def detect(self, as_of: datetime) -> PricingRegimeInfo:
        date_str = as_of.strftime("%Y-%m-%d")

        # ── 连板高度衰减斜率：近 lookback 交易日 max(consecutive_zt) ──
        rows = self.db.execute(
            "SELECT trade_date AS d, MAX(consecutive_zt) AS m FROM zt_pool "
            "WHERE trade_date <= ? GROUP BY trade_date ORDER BY trade_date DESC LIMIT ?",
            (date_str, self.lookback),
        )
        heights = [r["m"] for r in reversed(rows)]   # 旧 -> 新
        slope = 0.0
        if len(heights) >= 3:
            slope = float(np.polyfit(range(len(heights)), heights, 1)[0])

        # ── 龙头唯一性：当日 leader_clarity 的 p75 ──
        cr = self.db.execute(
            "SELECT factor_value AS v FROM factor_values "
            "WHERE factor_name = 'leader_clarity' AND trade_date = ? "
            "AND factor_value IS NOT NULL",
            (date_str,),
        )
        clarity_vals = [r["v"] for r in cr]
        clarity = float(np.percentile(clarity_vals, 75)) if clarity_vals else float("nan")

        details = {
            "leader_clarity_p75": None if np.isnan(clarity) else round(clarity, 3),
            "height_slope": round(slope, 3),
            "heights": heights,
        }

        if np.isnan(clarity):
            return PricingRegimeInfo("mixed", 0.3, details)

        clarity_high = clarity >= self.clarity_thr
        slope_down = slope <= self.slope_thr

        if clarity_high and not slope_down:
            regime = "hot_money_led"
            conf = min(1.0, 0.5 + (clarity - self.clarity_thr))
        elif slope_down and not clarity_high:
            regime = "quant_led"
            conf = min(1.0, 0.5 + (self.slope_thr - slope) * 0.3)
        else:
            regime = "mixed"
            conf = 0.5
        return PricingRegimeInfo(regime, round(conf, 3), details)
