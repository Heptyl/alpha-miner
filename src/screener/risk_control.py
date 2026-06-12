"""策略9: 风控筛选选股器。

核心逻辑(基于现有数据的后置风控):
1. 回撤控制: 近20日最大回撤<15%
2. 不追高: 当日涨幅不超5%(非涨停买入)
3. 估值安全: 股价在合理区间
4. 流动性: 日均成交额>2000万
5. 排除解禁/质押风险(需数据补充)

这个策略不产生独立候选，而是对其他策略的结果做风控过滤。
也支持独立运行，输出"安全标的"。

数据源: daily_price
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from src.screener.base import ScreenerBase, ScreenResult


class RiskControlScreener(ScreenerBase):
    """风控筛选选股器 — 维度9。"""

    name = "风控筛选"
    dimension = 9

    # 风控参数
    MAX_DRAWDOWN_20D = 0.15    # 20日最大回撤阈值
    MAX_DAILY_GAIN = 0.05      # 当日涨幅上限(不追涨停)
    MIN_AVG_AMOUNT = 2000      # 最低日均成交额(万)
    MIN_PRICE = 3.0
    MAX_PRICE = 200.0

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

        # === 硬性风控 ===
        if last_price < self.MIN_PRICE or last_price > self.MAX_PRICE:
            return None
        if volume.iloc[last_idx] <= 0:
            return None

        score = 0.70  # 基准分(通过风控=70分)
        reasons = ["通过风控筛选"]
        risks = []
        risk_flags = []

        # === 检查1: 当日涨幅 (权重-20%) ===
        if close.iloc[last_idx - 1] > 0:
            today_gain = (last_price / close.iloc[last_idx - 1] - 1)
            if today_gain > self.MAX_DAILY_GAIN:
                risk_flags.append(f"当日涨{today_gain * 100:.1f}%(追高)")
            elif today_gain > 0.03:
                risks.append(f"当日涨{today_gain * 100:.1f}%")
            elif today_gain < -0.05:
                risk_flags.append(f"当日跌{today_gain * 100:.1f}%")
            elif 0 < today_gain <= 0.03:
                reasons.append(f"温和上涨{today_gain * 100:.1f}%")

        # === 检查2: 20日最大回撤 (权重-20%) ===
        if n >= 20:
            recent_20 = close.iloc[-20:]
            cummax = recent_20.cummax()
            drawdown = (recent_20 / cummax - 1)
            max_dd = drawdown.min()
            if max_dd < -self.MAX_DRAWDOWN_20D:
                risk_flags.append(f"20日最大回撤{max_dd * 100:.1f}%(超标)")
            elif max_dd < -0.10:
                risks.append(f"20日最大回撤{max_dd * 100:.1f}%")
            else:
                reasons.append(f"回撤可控({max_dd * 100:.1f}%)")

        # === 检查3: 流动性 (权重-15%) ===
        avg_amount_10 = amount.tail(10).mean()
        if avg_amount_10 > 0:
            avg_wan = avg_amount_10 / 10000
            if avg_wan < self.MIN_AVG_AMOUNT:
                risk_flags.append(f"日均成交仅{avg_wan:.0f}万(流动性不足)")
            elif avg_wan >= 10000:
                reasons.append(f"流动性充裕(日均{avg_wan / 10000:.1f}亿)")
        else:
            # amount=0(回填数据), 用volume
            avg_vol = volume.tail(10).mean()
            if avg_vol < 10000:
                risk_flags.append("成交量偏低")

        # === 检查4: 波动率 (权重-10%) ===
        if n >= 10:
            returns = close.tail(10).pct_change().dropna()
            if len(returns) >= 5:
                vol = returns.std()
                if vol > 0.06:
                    risks.append(f"高波动({vol * 100:.1f}%)")
                    score -= 0.10
                elif vol < 0.02:
                    reasons.append("低波动稳定")
                    score += 0.05

        # === 处理风险标志 ===
        for flag in risk_flags:
            score -= 0.15
            risks.append(flag)

        # 有2个以上风险标志直接淘汰
        if len(risk_flags) >= 2:
            return None

        if score < 0.30:
            return None

        return self.make_result(
            stock_code=code,
            report_date=report_date,
            score=min(max(score, 0.0), 1.0),
            reasons=reasons,
            risks=risks,
            extra={
                "close": float(last_price),
                "volume": int(volume.iloc[last_idx]),
                "dimension": self.dimension,
                "risk_flags": len(risk_flags),
            },
        )

    def filter_results(self, candidates: list[ScreenResult],
                       report_date: str) -> list[ScreenResult]:
        """对其他策略的候选做风控过滤。

        只过滤，不改分数。移除风控不达标的标的。
        """
        if not candidates:
            return []

        codes = [c.stock_code for c in candidates]
        prices_map = {}
        conn = self._get_conn()
        try:
            start = (pd.Timestamp(report_date) - pd.Timedelta(days=60)).strftime("%Y-%m-%d")
            df = pd.read_sql_query(
                "SELECT stock_code, trade_date, open, high, low, close, volume, amount "
                "FROM daily_price WHERE trade_date >= ? AND trade_date <= ? "
                "ORDER BY stock_code, trade_date",
                conn, params=(start, report_date),
            )
            for code, group in df.groupby("stock_code"):
                if len(group) >= 10:
                    prices_map[code] = group
        finally:
            conn.close()

        filtered = []
        for c in candidates:
            df = prices_map.get(c.stock_code)
            if df is None or len(df) < 5:
                filtered.append(c)  # 无数据不过滤
                continue

            close = pd.to_numeric(df["close"], errors="coerce")
            last = close.iloc[-1]

            # 基本风控
            if last < self.MIN_PRICE or last > self.MAX_PRICE:
                c.risks.append("风控:价格异常")
                continue  # 淘汰

            # 回撤风控
            if len(close) >= 20:
                recent = close.tail(20)
                max_dd = (recent / recent.cummax() - 1).min()
                if max_dd < -self.MAX_DRAWDOWN_20D:
                    c.risks.append(f"风控:回撤{max_dd * 100:.1f}%")
                    continue  # 淘汰

            filtered.append(c)

        return filtered
