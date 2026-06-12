"""策略1: 趋势突破选股器。

借鉴 KHunter 阻力位突破策略 + 多金叉共振:
1. 均线多头排列 (MA5 > MA10 > MA20 > MA60)
2. 放量突破近期高点 (60日新高 + 量比>2)
3. MACD金叉确认
4. 涨停回马枪形态 (涨停后回调缩量企稳)

数据源: daily_price
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from src.screener.base import ScreenerBase, ScreenResult


class TrendBreakoutScreener(ScreenerBase):
    """趋势突破选股器 — 维度1。"""

    name = "趋势突破"
    dimension = 1

    def screen(self, report_date: str) -> list[ScreenResult]:
        """执行趋势突破选股。"""
        prices_map = self.get_prices_batch(report_date, lookback=90)
        results = []

        for code, df in prices_map.items():
            # 快速过滤: 至少60天数据
            if len(df) < 60:
                continue

            res = self._analyze(code, df, report_date)
            if res is not None:
                results.append(res)

        # 按得分排序
        results.sort(key=lambda r: r.score, reverse=True)
        return results

    def _analyze(self, code: str, df: pd.DataFrame,
                 report_date: str) -> ScreenResult | None:
        """分析单只股票的趋势突破信号。"""
        close = df["close"]
        volume = df["volume"]
        high = df["high"]

        # --- 计算指标 ---
        ma5 = self.calc_ma(close, 5)
        ma10 = self.calc_ma(close, 10)
        ma20 = self.calc_ma(close, 20)
        ma60 = self.calc_ma(close, 60)
        vol_ratio = self.calc_volume_ratio(volume, 5)
        dif, dea, hist = self.calc_macd(close)
        rsi = self.calc_rsi(close)

        last_idx = len(df) - 1
        last = df.iloc[last_idx]

        score = 0.0
        reasons = []
        risks = []

        # === 条件1: 均线多头排列 (权重25%) ===
        if (pd.notna(ma5.iloc[last_idx]) and
            pd.notna(ma60.iloc[last_idx])):
            is_bull_align = (
                ma5.iloc[last_idx] > ma10.iloc[last_idx] >
                ma20.iloc[last_idx] > ma60.iloc[last_idx]
            )
            if is_bull_align:
                score += 0.25
                reasons.append("均线多头排列(MA5>10>20>60)")

        # === 条件2: 放量突破60日新高 (权重30%) ===
        if len(df) >= 60:
            high_60 = high.iloc[-60:-1].max()  # 前59日最高
            last_high = high.iloc[last_idx]
            last_vol_ratio = vol_ratio.iloc[last_idx]

            if pd.notna(high_60) and last_high > high_60:
                # 突破60日新高
                if pd.notna(last_vol_ratio) and last_vol_ratio >= 1.5:
                    score += 0.30
                    pct_above = (last_high / high_60 - 1) * 100
                    reasons.append(
                        f"放量突破60日新高(+{pct_above:.1f}%, 量比{last_vol_ratio:.1f})"
                    )
                else:
                    score += 0.15
                    reasons.append("缩量突破60日新高(需放量确认)")
                    risks.append("突破缺少成交量配合")

        # === 条件3: MACD金叉 (权重20%) ===
        if (pd.notna(dif.iloc[last_idx]) and pd.notna(dea.iloc[last_idx]) and
            pd.notna(dif.iloc[last_idx - 1]) and pd.notna(dea.iloc[last_idx - 1])):
            # 今天金叉或最近3天金叉
            golden_cross = False
            for i in range(max(0, last_idx - 3), last_idx + 1):
                if (dif.iloc[i] > dea.iloc[i] and
                    dif.iloc[i - 1] <= dea.iloc[i - 1]):
                    golden_cross = True
                    break
            if golden_cross:
                score += 0.20
                reasons.append("MACD金叉确认")

        # === 条件4: 涨停回马枪 (权重25%) ===
        # 近6日内有涨停(涨幅>=9.5%), 之后缩量回调不破涨停收盘价95%
        if len(df) >= 10:
            pct_changes = close.pct_change()
            recent = min(6, len(df) - 1)
            zt_found = False
            for i in range(last_idx, max(last_idx - recent, 0), -1):
                if pd.notna(pct_changes.iloc[i]) and pct_changes.iloc[i] >= 0.095:
                    # 找到涨停日
                    zt_close = close.iloc[i]
                    zt_idx = i
                    # 检查涨停后到今天是否缩量+不破支撑
                    if zt_idx < last_idx:
                        post_df = df.iloc[zt_idx + 1:last_idx + 1]
                        post_vol_ratio = vol_ratio.iloc[zt_idx + 1:last_idx + 1]
                        avg_shrink = post_vol_ratio.mean() if len(post_vol_ratio) > 0 else 1.0
                        low_below_support = (post_df["low"] < zt_close * 0.95).any()

                        if not low_below_support and avg_shrink < 0.8:
                            score += 0.25
                            days_after = last_idx - zt_idx
                            reasons.append(
                                f"涨停回马枪({days_after}日回调缩量企稳)"
                            )
                            zt_found = True
                        elif not low_below_support:
                            score += 0.10
                            reasons.append("涨停后企稳但未缩量")
                    break

        # === RSI过滤: 超买区扣分 ===
        if pd.notna(rsi.iloc[last_idx]):
            if rsi.iloc[last_idx] > 80:
                score -= 0.15
                risks.append(f"RSI超买({rsi.iloc[last_idx]:.0f})")
            elif rsi.iloc[last_idx] < 30:
                score += 0.05
                reasons.append(f"RSI低位({rsi.iloc[last_idx]:.0f})")

        # 最低门槛: 至少满足2个条件
        if score < 0.20:
            return None

        return self.make_result(
            stock_code=code,
            report_date=report_date,
            score=score,
            reasons=reasons,
            risks=risks,
            extra={
                "close": float(last["close"]),
                "volume": int(last["volume"]),
                "dimension": self.dimension,
            },
        )
