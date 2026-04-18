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
