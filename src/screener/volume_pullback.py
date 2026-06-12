"""策略2: 缩量回调选股器。

借鉴 KHunter 的缩量回调逻辑:
1. 上涨趋势确认 (20日涨幅>10%)
2. 连续缩量回调 (成交量递减3天以上)
3. 回调幅度适中 (不破20日均线)
4. 回调尾声放量信号 (买点确认)

数据源: daily_price
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from src.screener.base import ScreenerBase, ScreenResult


class VolumePullbackScreener(ScreenerBase):
    """缩量回调选股器 — 维度2。"""

    name = "缩量回调"
    dimension = 2

    def screen(self, report_date: str) -> list[ScreenResult]:
        prices_map = self.get_prices_batch(report_date, lookback=60)
        results = []

        for code, df in prices_map.items():
            if len(df) < 30:
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
        n = len(df)
        last_idx = n - 1

        score = 0.0
        reasons = []
        risks = []

        # === 条件1: 上涨趋势 (权重20%) ===
        # 20日前价格 vs 当前价格
        if n >= 20:
            price_20d_ago = close.iloc[last_idx - 20]
            price_high = close.iloc[last_idx - 20:last_idx].max()
            last_price = close.iloc[last_idx]

            if pd.notna(price_20d_ago) and price_20d_ago > 0:
                gain_20d = (price_high / price_20d_ago - 1)
                if gain_20d >= 0.10:
                    score += 0.20
                    reasons.append(f"20日区间涨幅{gain_20d * 100:.1f}%")
                elif gain_20d >= 0.05:
                    score += 0.10
                    reasons.append(f"20日区间涨幅{gain_20d * 100:.1f}%(温和上涨)")

        # === 条件2: 近期高点后开始回调 (权重15%) ===
        # 找近10日内的最高价日
        lookback = min(10, n - 1)
        recent_highs = close.iloc[last_idx - lookback:last_idx + 1]
        peak_idx = recent_highs.idxmax()
        peak_price = close.iloc[peak_idx]
        last_price = close.iloc[last_idx]

        if peak_idx < last_idx:  # 高点不在今天=正在回调
            pullback_pct = (last_price / peak_price - 1)
            if -0.15 <= pullback_pct < 0:  # 回调0~15%
                score += 0.15
                reasons.append(f"自高点回调{pullback_pct * 100:.1f}%")
            elif pullback_pct < -0.15:
                risks.append(f"回调过深({pullback_pct * 100:.1f}%)")
                return None  # 回调太深，放弃
        else:
            return None  # 今天还在创新高，不是回调形态

        # === 条件3: 缩量回调 (权重35%) ===
        # 从高点开始成交量递减
        if peak_idx < last_idx:
            post_peak = volume.iloc[peak_idx + 1:last_idx + 1]
            if len(post_peak) >= 2:
                # 检查是否整体缩量
                peak_vol = volume.iloc[peak_idx]
                avg_post_vol = post_peak.mean()
                shrink_ratio = avg_post_vol / peak_vol if peak_vol > 0 else 1.0

                if shrink_ratio < 0.6:
                    score += 0.35
                    reasons.append(f"显著缩量回调(量缩至峰值{shrink_ratio * 100:.0f}%)")
                elif shrink_ratio < 0.8:
                    score += 0.20
                    reasons.append(f"温和缩量(量缩至峰值{shrink_ratio * 100:.0f}%)")
                else:
                    risks.append("回调未缩量，可能继续下跌")
                    score += 0.05

                # 检查是否连续缩量(天数加分)
                if len(post_peak) >= 3:
                    declining = all(
                        post_peak.iloc[i] >= post_peak.iloc[i + 1]
                        for i in range(len(post_peak) - 1)
                    )
                    if declining:
                        score += 0.05
                        reasons.append(f"连续{len(post_peak)}日缩量")

        # === 条件4: 不破20日均线 (权重15%) ===
        ma20 = self.calc_ma(close, 20)
        if pd.notna(ma20.iloc[last_idx]):
            if last_price >= ma20.iloc[last_idx]:
                score += 0.15
                reasons.append("回调未破20日均线(MA20支撑)")
            elif last_price >= ma20.iloc[last_idx] * 0.97:
                score += 0.05
                reasons.append("接近20日均线")
            else:
                risks.append("已跌破20日均线")

        # === 条件5: 回调尾声放量信号 (权重15%) ===
        # 最后一天成交量 > 前一天 = 可能反转
        if n >= 3:
            vol_today = volume.iloc[last_idx]
            vol_yesterday = volume.iloc[last_idx - 1]
            if vol_today > vol_yesterday * 1.2 and last_price > close.iloc[last_idx - 1]:
                score += 0.15
                reasons.append("尾日放量反弹(买点信号)")
            elif vol_today > vol_yesterday * 1.5:
                score += 0.05
                reasons.append("尾日放量(关注方向)")

        # 最低门槛
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
                "pullback_from_peak": float((last_price / peak_price - 1) * 100),
                "dimension": self.dimension,
            },
        )
