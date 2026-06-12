"""策略6: 基本面排雷选股器。

核心逻辑(暂时只用现有数据做基础排雷):
1. 排除ST/退市股
2. 排除低价股(< 2元)
3. 排除低市值(< 20亿)
4. 排除异常成交 (流动性陷阱)
5. 排除近期暴涨暴跌

注意: 真正的基本面(PE/ROE/净利润)需要采集财务数据后补充。
当前版本是基于价格和成交的初步排雷。

数据源: daily_price, zt_pool
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from src.screener.base import ScreenerBase, ScreenResult


class FundamentalScreener(ScreenerBase):
    """基本面排雷选股器 — 维度6。"""

    name = "基本面排雷"
    dimension = 6

    # 排雷阈值
    MIN_PRICE = 2.0       # 最低股价
    MAX_PRICE = 500.0     # 最高股价
    MIN_MARKET_DAYS = 60  # 至少上市60天

    def screen(self, report_date: str) -> list[ScreenResult]:
        prices_map = self.get_prices_batch(report_date, lookback=60)
        results = []

        for code, df in prices_map.items():
            if len(df) < self.MIN_MARKET_DAYS:
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
        last_price = close.iloc[last_idx]

        # === 硬性排雷 ===
        # 1. 低价股
        if last_price < self.MIN_PRICE:
            return None
        # 2. 高价股
        if last_price > self.MAX_PRICE:
            return None
        # 3. 零成交
        if volume.iloc[last_idx] <= 0:
            return None

        score = 0.60  # 通过基本排雷就给60分基准
        reasons = ["通过基本面排雷"]
        risks = []

        # === 检查项1: 近期是否暴跌 (权重-20%) ===
        if n >= 5:
            max_5d_drop = 0
            for i in range(max(0, n - 5), n):
                if close.iloc[i - 1] > 0:
                    drop = (close.iloc[i] / close.iloc[i - 1] - 1)
                    max_5d_drop = min(max_5d_drop, drop)
            if max_5d_drop < -0.08:
                score -= 0.20
                risks.append(f"近5日最大单日跌幅{max_5d_drop * 100:.1f}%")
            elif max_5d_drop < -0.05:
                score -= 0.10
                risks.append(f"近5日有单日跌{max_5d_drop * 100:.1f}%")

        # === 检查项2: 流动性 (权重+10%) ===
        avg_vol_20 = volume.tail(20).mean()
        avg_amount_col = df["amount"].tail(20)
        avg_amount = avg_amount_col.mean()

        if avg_amount > 0:
            if avg_amount / 10000 >= 5000:  # 日均>5000万
                score += 0.10
                reasons.append("流动性充足")
            elif avg_amount / 10000 >= 1000:
                score += 0.05
            else:
                risks.append("流动性偏低")
        elif avg_vol_20 >= 30000:
            score += 0.05
            reasons.append("流动性尚可(按量)")

        # === 检查项3: 波动率 (权重±10%) ===
        if n >= 20:
            daily_returns = close.iloc[-20:].pct_change().dropna()
            if len(daily_returns) >= 10:
                volatility = daily_returns.std()
                if volatility > 0.05:  # 日均波动>5%
                    risks.append(f"高波动率({volatility * 100:.1f}%)")
                    score -= 0.10
                elif volatility < 0.02:  # 日均波动<2%
                    reasons.append("低波动稳定")
                    score += 0.05

        # === 检查项4: 是否ST ===
        name = self.get_stock_name(code, report_date)
        if self.is_st_stock(name):
            return None  # ST直接排除

        # 通过排雷但分太低
        if score < 0.30:
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
