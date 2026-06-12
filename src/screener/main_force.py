"""策略7: 主力资金跟踪选股器。

核心逻辑:
1. 主力大额净流入 (>1000万/日)
2. 主力连续3日以上流入
3. 散户反向流出 (主力吸筹特征)
4. 资金流入时股价上涨(同向确认)

数据源: fund_flow
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from src.screener.base import ScreenerBase, ScreenResult


class MainForceScreener(ScreenerBase):
    """主力资金跟踪选股器 — 维度7。"""

    name = "主力资金"
    dimension = 7

    def screen(self, report_date: str) -> list[ScreenResult]:
        conn = self._get_conn()
        try:
            df = pd.read_sql_query(
                "SELECT stock_code, trade_date, main_net, amount, "
                "pct_change, inflow, outflow, net_amount "
                "FROM fund_flow WHERE trade_date <= ? "
                "ORDER BY stock_code, trade_date",
                conn, params=(report_date,),
            )
        finally:
            conn.close()

        if df.empty:
            return []

        dates = sorted(df["trade_date"].unique())
        recent_dates = dates[-10:]
        df = df[df["trade_date"].isin(recent_dates)]

        results = []
        for code, group in df.groupby("stock_code"):
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
        net_amount = pd.to_numeric(df["net_amount"], errors="coerce").fillna(0)

        score = 0.0
        reasons = []
        risks = []

        # === 条件1: 主力大额净流入 (权重30%) ===
        recent_main = main_net.tail(5)
        avg_main = recent_main.mean()
        total_main = recent_main.sum()

        if avg_main > 2000:  # 日均>2000万
            score += 0.30
            reasons.append(f"主力日均净流入{avg_main:.0f}万元(大额)")
        elif avg_main > 500:
            score += 0.15
            reasons.append(f"主力日均净流入{avg_main:.0f}万元")
        elif avg_main < -2000:
            risks.append(f"主力日均净流出{abs(avg_main):.0f}万元")
            return None

        # === 条件2: 连续流入天数 (权重25%) ===
        consecutive = 0
        for i in range(n - 1, -1, -1):
            if main_net.iloc[i] > 0:
                consecutive += 1
            else:
                break

        if consecutive >= 5:
            score += 0.25
            reasons.append(f"主力连续{consecutive}日净流入")
        elif consecutive >= 3:
            score += 0.15
            reasons.append(f"主力连续{consecutive}日净流入")

        # === 条件3: 净流入金额占比 (权重20%) ===
        # 流入 vs 流出的比例
        total_inflow = pd.to_numeric(df["inflow"].tail(5), errors="coerce").fillna(0).sum()
        total_outflow = pd.to_numeric(df["outflow"].tail(5), errors="coerce").fillna(0).sum()
        if total_outflow > 0:
            flow_ratio = total_inflow / total_outflow
            if flow_ratio > 1.5:
                score += 0.20
                reasons.append(f"流入/流出比={flow_ratio:.1f}(强势)")
            elif flow_ratio > 1.1:
                score += 0.10
                reasons.append(f"流入/流出比={flow_ratio:.1f}")

        # === 条件4: 资金与股价同向 (权重25%) ===
        recent_3d_pct = pct_change.tail(3)
        recent_3d_main = main_net.tail(3)
        aligned_days = ((recent_3d_pct > 0) & (recent_3d_main > 0)).sum()
        if aligned_days >= 3:
            score += 0.25
            reasons.append("资金与股价连续3日同向上行")
        elif aligned_days >= 2:
            score += 0.10
            reasons.append("资金与股价多数同向")
        else:
            pct_sum = recent_3d_pct.sum()
            if pct_sum < -5 and recent_3d_main.sum() > 0:
                risks.append("资金流入但股价下跌(背离风险)")

        if score < 0.25:
            return None

        return self.make_result(
            stock_code=code,
            report_date=report_date,
            score=score,
            reasons=reasons,
            risks=risks,
            extra={
                "main_net_total": float(total_main),
                "consecutive_inflow": consecutive,
                "dimension": self.dimension,
            },
        )
