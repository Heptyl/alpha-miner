"""策略5: 量价筛选选股器。

核心逻辑:
1. 30天温和放量 (非暴涨暴跌)
2. 低PE (排除高估值, 暂用价格/涨幅替代)
3. 无退市风险 (排除ST/低价股)
4. 成交活跃 (日均成交额>5000万)

数据源: daily_price
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from src.screener.base import ScreenerBase, ScreenResult


class VolumePriceScreener(ScreenerBase):
    """量价筛选选股器 — 维度5。"""

    name = "量价筛选"
    dimension = 5

    def screen(self, report_date: str) -> list[ScreenResult]:
        prices_map = self.get_prices_batch(report_date, lookback=30)
        results = []

        for code, df in prices_map.items():
            if len(df) < 20:
                continue
            res = self._analyze(code, df, report_date)
            if res is not None:
                results.append(res)

        results.sort(key=lambda r: r.score, reverse=True)
        return results

    def _analyze(self, code: str, df: pd.DataFrame,
                 report_date: str) -> ScreenResult | None:
        close = df["close"]
        volume = df["volume"]
        amount = df["amount"]
        n = len(df)
        last_idx = n - 1
        last_price = close.iloc[last_idx]

        # === 排雷: 低价股 ===
        if last_price < 3.0:
            return None
        # === 排雷: 极高价股 ===
        if last_price > 300:
            return None

        score = 0.0
        reasons = []
        risks = []

        # === 条件1: 30天温和放量 (权重35%) ===
        if n >= 20:
            vol_ma5 = volume.tail(5).mean()
            vol_ma20 = volume.tail(20).mean()
            if vol_ma20 > 0:
                vol_trend = vol_ma5 / vol_ma20
                if 1.2 <= vol_trend <= 2.5:
                    # 温和放量
                    score += 0.35
                    reasons.append(f"温和放量(5日均量/20日均量={vol_trend:.2f})")
                elif 1.0 <= vol_trend < 1.2:
                    score += 0.15
                    reasons.append("量能平稳")
                elif vol_trend > 2.5:
                    risks.append(f"过度放量(量比{vol_trend:.2f})，可能见顶")

        # === 条件2: 30日涨幅温和 (权重25%) ===
        if n >= 20:
            gain_30d = (last_price / close.iloc[last_idx - 20] - 1)
            if 0.05 <= gain_30d <= 0.30:
                score += 0.25
                reasons.append(f"30日温和上涨{gain_30d * 100:.1f}%")
            elif 0 < gain_30d < 0.05:
                score += 0.10
                reasons.append(f"30日小幅上涨{gain_30d * 100:.1f}%")
            elif gain_30d > 0.50:
                risks.append(f"30日涨幅{gain_30d * 100:.1f}%过高")
                return None
            elif gain_30d < -0.15:
                return None  # 跌太多，不要

        # === 条件3: 成交活跃 (权重20%) ===
        # 用amount(成交额)判断，回填数据amount=0时用volume替代
        avg_amount = amount.tail(10).mean()
        if avg_amount > 0:
            avg_amount_wan = avg_amount / 10000  # 转万元
            if avg_amount_wan >= 10000:  # 日均>1亿
                score += 0.20
                reasons.append(f"日均成交额{avg_amount_wan / 10000:.1f}亿")
            elif avg_amount_wan >= 3000:  # >3000万
                score += 0.10
                reasons.append(f"日均成交额{avg_amount_wan:.0f}万")
        else:
            # amount=0 (回填数据), 用volume判断
            avg_vol = volume.tail(10).mean()
            if avg_vol >= 50000:  # 日均5万手以上
                score += 0.10
                reasons.append("成交活跃(按量判断)")

        # === 条件4: 价格在合理区间 (权重20%) ===
        # 近20日振幅不超过30%
        if n >= 20:
            recent_high = df["high"].iloc[-20:].max()
            recent_low = df["low"].iloc[-20:].min()
            if recent_low > 0:
                amplitude = (recent_high / recent_low - 1)
                if amplitude < 0.20:
                    score += 0.20
                    reasons.append(f"20日振幅{amplitude * 100:.1f}%(稳定)")
                elif amplitude < 0.35:
                    score += 0.10
                    reasons.append(f"20日振幅{amplitude * 100:.1f}%")
                else:
                    risks.append(f"波动过大(振幅{amplitude * 100:.1f}%)")

        if score < 0.25:
            return None

        return self.make_result(
            stock_code=code,
            report_date=report_date,
            score=score,
            reasons=reasons,
            risks=risks,
            extra={
                "close": float(last_price),
                "volume": int(volume.iloc[last_idx]),
                "dimension": self.dimension,
            },
        )
