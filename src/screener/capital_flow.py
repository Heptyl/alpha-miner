"""策略3: 资金连续流入选股器。

核心逻辑:
1. 主力资金连续N日净流入 (权重最高)
2. 主力净流入金额递增
3. 大单占比提升
4. 叠加股价上涨 (资金推动型)

数据源: fund_flow
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from src.screener.base import ScreenerBase, ScreenResult


class CapitalFlowScreener(ScreenerBase):
    """资金连续流入选股器 — 维度3。"""

    name = "资金连续流入"
    dimension = 3

    def screen(self, report_date: str) -> list[ScreenResult]:
        conn = self._get_conn()
        try:
            # 获取近10天的资金流向数据
            df = pd.read_sql_query(
                "SELECT stock_code, trade_date, main_net, amount, "
                "pct_change, inflow, outflow, net_amount "
                "FROM fund_flow "
                "WHERE trade_date <= ? "
                "ORDER BY stock_code, trade_date",
                conn, params=(report_date,),
            )
        finally:
            conn.close()

        if df.empty:
            return []

        # 只保留最近10天
        latest_date = df["trade_date"].max()
        dates = sorted(df["trade_date"].unique())
        recent_dates = dates[-10:]
        df = df[df["trade_date"].isin(recent_dates)]

        results = []
        for code, group in df.groupby("stock_code"):
            # 排除科创板和北交所
            if code.startswith("688") or code.startswith("689"):
                continue
            if len(code) == 6 and code[0] in ("8", "9"):
                continue

            res = self._analyze(code, group, report_date)
            if res is not None:
                results.append(res)

        results.sort(key=lambda r: r.score, reverse=True)
        return results

    def _analyze(self, code: str, df: pd.DataFrame,
                 report_date: str) -> ScreenResult | None:
        df = df.sort_values("trade_date")
        n = len(df)
        if n < 3:
            return None

        main_net = pd.to_numeric(df["main_net"], errors="coerce").fillna(0)
        pct_change = pd.to_numeric(df["pct_change"], errors="coerce").fillna(0)
        amount = pd.to_numeric(df["amount"], errors="coerce").fillna(0)

        score = 0.0
        reasons = []
        risks = []

        # === 条件1: 连续净流入天数 (权重40%) ===
        # 从最近往前数连续净流入天数
        consecutive_inflow = 0
        for i in range(n - 1, -1, -1):
            if main_net.iloc[i] > 0:
                consecutive_inflow += 1
            else:
                break

        if consecutive_inflow >= 5:
            score += 0.40
            reasons.append(f"主力连续{consecutive_inflow}日净流入")
        elif consecutive_inflow >= 3:
            score += 0.25
            reasons.append(f"主力连续{consecutive_inflow}日净流入")
        elif consecutive_inflow >= 2:
            score += 0.10
            reasons.append(f"主力连续{consecutive_inflow}日净流入")

        # === 条件2: 净流入金额递增 (权重20%) ===
        if consecutive_inflow >= 3:
            inflow_series = main_net.iloc[-consecutive_inflow:]
            if all(inflow_series.iloc[i] <= inflow_series.iloc[i + 1]
                   for i in range(len(inflow_series) - 1)):
                score += 0.20
                reasons.append("净流入金额递增(加速流入)")
            else:
                score += 0.05
                reasons.append("净流入趋势确立")

        # === 条件3: 资金规模 (权重20%) ===
        total_inflow = main_net.sum()
        if total_inflow > 10000:  # 万元
            score += 0.20
            reasons.append(f"区间净流入{total_inflow / 10000:.1f}亿元")
        elif total_inflow > 3000:
            score += 0.10
            reasons.append(f"区间净流入{total_inflow:.0f}万元")

        # === 条件4: 资金推动股价上涨 (权重20%) ===
        recent_pct = pct_change.iloc[-3:].sum() if n >= 3 else pct_change.sum()
        if recent_pct > 5:
            score += 0.20
            reasons.append(f"近3日涨{recent_pct:.1f}%(资金推动)")
        elif recent_pct > 0:
            score += 0.10
            reasons.append(f"近3日涨{recent_pct:.1f}%")
        else:
            risks.append(f"资金流入但股价跌{recent_pct:.1f}%(背离)")

        # 最低门槛
        if score < 0.20:
            return None

        last_row = df.iloc[-1]
        return self.make_result(
            stock_code=code,
            report_date=report_date,
            score=score,
            reasons=reasons,
            risks=risks,
            extra={
                "main_net_total": float(total_inflow),
                "consecutive_inflow": consecutive_inflow,
                "dimension": self.dimension,
            },
        )
