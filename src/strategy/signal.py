"""次日选股信号引擎。

基于三大验证因子（theme_crowding / leader_clarity / lhb_institution），
从当日涨停池中筛选次日重点关注的标的。

输出结构:
  SignalCard — 单只候选股的信号卡
  SignalReport — 当日完整信号报告
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

import numpy as np
import pandas as pd

from src.data.storage import Storage


@dataclass
class SignalCard:
    """单只候选股信号卡."""

    stock_code: str
    stock_name: str
    industry: str
    concepts: list[str]

    # 因子得分
    theme_crowding: float = 0.0
    leader_clarity: float = 0.0
    lhb_institution: float = 0.0
    turnover_rank: float = 0.0

    # 市场特征
    consecutive_zt: int = 0       # 连板数
    open_count: int = 0           # 炸板次数
    amount: float = 0.0           # 成交额
    circulation_mv: float = 0.0   # 流通市值

    # 综合评估
    composite_score: float = 0.0  # 加权综合得分
    signal_level: str = ""        # 信号等级: A/B/C
    reasons: list[str] = field(default_factory=list)   # 入选理由
    risks: list[str] = field(default_factory=list)     # 风险提示

    def to_dict(self) -> dict:
        return {
            "stock_code": self.stock_code,
            "stock_name": self.stock_name,
            "industry": self.industry,
            "concepts": self.concepts,
            "theme_crowding": round(self.theme_crowding, 3),
            "leader_clarity": round(self.leader_clarity, 3),
            "lhb_institution": round(self.lhb_institution, 3),
            "turnover_rank": round(self.turnover_rank, 3),
            "consecutive_zt": self.consecutive_zt,
            "open_count": self.open_count,
            "composite_score": round(self.composite_score, 3),
            "signal_level": self.signal_level,
            "reasons": self.reasons,
            "risks": self.risks,
        }


@dataclass
class SignalReport:
    """当日信号报告."""
    trade_date: str
    cards: list[SignalCard]

    # 大盘概况
    zt_count: int = 0          # 涨停数
    dt_count: int = 0          # 跌停数
    market_regime: str = ""    # 市场状态: 强势/震荡/弱势

    # 板块热度
    hot_industries: list[dict] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "trade_date": self.trade_date,
            "zt_count": self.zt_count,
            "dt_count": self.dt_count,
            "market_regime": self.market_regime,
            "hot_industries": self.hot_industries,
            "cards": [c.to_dict() for c in self.cards],
        }

    def to_text(self) -> str:
        """终端友好的纯文本输出."""
        lines = []
        lines.append("=" * 60)
        lines.append(f"  Alpha Miner 次日信号 — {self.trade_date}")
        lines.append("=" * 60)

        # 大盘概况
        lines.append(f"\n  涨停 {self.zt_count} 只 | 跌停 {self.dt_count} 只 | {self.market_regime}")

        # 板块热度 TOP3
        if self.hot_industries:
            lines.append(f"\n  热门板块:")
            for hi in self.hot_industries[:5]:
                lines.append(f"    {hi['industry']}: {hi['zt_count']}只涨停"
                             f" {'★' * min(hi['zt_count'], 5)}")

        # 信号卡
        if not self.cards:
            lines.append(f"\n  无符合条件的候选股")
        else:
            for i, card in enumerate(self.cards, 1):
                lines.append(f"\n  ┌─ [{card.signal_level}] {card.stock_code} {card.stock_name}"
                             f" ── 综合分 {card.composite_score:.2f}")
                lines.append(f"  │  板块: {card.industry}"
                             f"  |  连板: {card.consecutive_zt}"
                             f"  |  炸板: {card.open_count}次")
                lines.append(f"  │  拥挤度: {card.theme_crowding:.2f}"
                             f"  龙头度: {card.leader_clarity:.2f}"
                             f"  资金: {card.lhb_institution:.2f}"
                             f"  换手排名: {card.turnover_rank:.2f}")
                if card.concepts:
                    lines.append(f"  │  概念: {', '.join(card.concepts[:3])}")
                if card.reasons:
                    for r in card.reasons:
                        lines.append(f"  │  ✓ {r}")
                if card.risks:
                    for r in card.risks:
                        lines.append(f"  │  ⚠ {r}")
                lines.append(f"  └{'─' * 55}")

        lines.append(f"\n  提示: 以上信号仅供参考，不构成投资建议。")
        lines.append(f"  数据截至 {self.trade_date} 收盘，建议次日集合竞价观察后再决策。")
        lines.append("=" * 60)

        return "\n".join(lines)


class SignalEngine:
    """次日选股信号引擎."""

    # 因子权重（基于 IC 验证结果）
    WEIGHTS = {
        "theme_crowding": 0.40,
        "leader_clarity": 0.35,
        "lhb_institution": 0.15,
        "turnover_rank": 0.10,  # 反向因子，用 (1 - rank) 使其正向
    }

    # 信号等级阈值
    LEVEL_A_THRESHOLD = 0.70  # A级: 高置信度
    LEVEL_B_THRESHOLD = 0.50  # B级: 中等置信度
    # C级: 低置信度

    def __init__(self, db: Storage):
        self.db = db

    def generate(self, as_of: datetime, report_date: str,
                 top_n: int = 10) -> SignalReport:
        """生成信号报告.

        Args:
            as_of: 时间锚点（用于数据隔离）
            report_date: 报告日期 YYYY-MM-DD
            top_n: 返回 TOP N 候选股
        """
        # 1. 取涨停池
        zt_df = self._load_zt_pool(as_of, report_date)

        if zt_df.empty:
            return SignalReport(
                trade_date=report_date,
                cards=[],
                zt_count=0,
                dt_count=0,
                market_regime="无数据",
            )

        # 2. 取因子值
        factor_df = self._load_factors(report_date)

        # 3. 取概念映射
        concept_map = self._load_concepts()

        # 4. 计算涨跌停统计
        zt_count, dt_count = self._count_zt_dt(as_of, report_date)

        # 5. 板块热度
        hot_industries = self._hot_industries(zt_df)

        # 6. 判断市场状态
        regime = self._market_regime(zt_count, dt_count)

        # 7. 构建信号卡
        cards = []
        for _, row in zt_df.iterrows():
            code = row["stock_code"]
            factors = factor_df.get(code, {})

            card = SignalCard(
                stock_code=code,
                stock_name=str(row.get("name", "")),
                industry=str(row.get("industry", "")),
                concepts=concept_map.get(code, []),
                consecutive_zt=int(row.get("consecutive_zt", 0)),
                open_count=int(row.get("open_count", 0)),
                amount=float(row.get("amount", 0)),
                circulation_mv=float(row.get("circulation_mv", 0)),
                theme_crowding=float(factors.get("theme_crowding", 0)),
                leader_clarity=float(factors.get("leader_clarity", 0)),
                lhb_institution=float(factors.get("lhb_institution", 0)),
                turnover_rank=float(factors.get("turnover_rank", 0)),
            )

            # 综合打分
            card.composite_score = self._compute_score(card)
            card.signal_level = self._signal_level(card.composite_score)
            card.reasons = self._generate_reasons(card)
            card.risks = self._generate_risks(card)

            cards.append(card)

        # 按综合分排序
        cards.sort(key=lambda c: c.composite_score, reverse=True)
        cards = cards[:top_n]

        return SignalReport(
            trade_date=report_date,
            cards=cards,
            zt_count=zt_count,
            dt_count=dt_count,
            market_regime=regime,
            hot_industries=hot_industries,
        )

    def _load_zt_pool(self, as_of: datetime, report_date: str) -> pd.DataFrame:
        """加载涨停池."""
        df = self.db.query(
            "zt_pool", as_of,
            where="trade_date = ?", params=(report_date,),
        )
        if df.empty:
            return df
        # 去重
        df = df.drop_duplicates(subset=["stock_code"], keep="last")
        return df

    def _load_factors(self, report_date: str) -> dict[str, dict]:
        """加载因子值，返回 {stock_code: {factor_name: value}}."""
        import sqlite3
        conn = sqlite3.connect(self.db.db_path)
        rows = conn.execute(
            "SELECT stock_code, factor_name, factor_value "
            "FROM factor_values WHERE trade_date = ?",
            (report_date,),
        ).fetchall()
        conn.close()

        result: dict[str, dict] = {}
        for code, fname, fval in rows:
            if code not in result:
                result[code] = {}
            result[code][fname] = fval
        return result

    def _load_concepts(self) -> dict[str, list[str]]:
        """加载概念映射."""
        import sqlite3
        conn = sqlite3.connect(self.db.db_path)
        rows = conn.execute(
            "SELECT stock_code, concept_name FROM concept_mapping"
        ).fetchall()
        conn.close()

        result: dict[str, list[str]] = {}
        for code, concept in rows:
            if code not in result:
                result[code] = []
            result[code].append(concept)
        return result

    def _count_zt_dt(self, as_of: datetime, report_date: str) -> tuple[int, int]:
        """统计涨跌停数."""
        import sqlite3
        conn = sqlite3.connect(self.db.db_path)
        zt = conn.execute(
            "SELECT COUNT(DISTINCT stock_code) FROM zt_pool WHERE trade_date = ?",
            (report_date,),
        ).fetchone()[0]

        # 跌停：从 daily_price 算跌幅
        price_df = pd.read_sql_query(
            f"SELECT stock_code, open, close, pre_close FROM daily_price WHERE trade_date = '{report_date}'",
            conn,
        )
        conn.close()

        dt = 0
        if not price_df.empty and "pre_close" in price_df.columns:
            pc = price_df["pre_close"].where(price_df["pre_close"] > 0, price_df["open"])
            pct = (price_df["close"] - pc) / pc * 100
            dt = int((pct < -9.5).sum())

        return zt, dt

    def _hot_industries(self, zt_df: pd.DataFrame) -> list[dict]:
        """板块热度排名."""
        if "industry" not in zt_df.columns:
            return []
        grouped = zt_df.groupby("industry").agg(
            zt_count=("stock_code", "count"),
            max_consecutive=("consecutive_zt", "max"),
        ).reset_index()
        grouped = grouped.sort_values("zt_count", ascending=False)
        return grouped.to_dict("records")

    def _market_regime(self, zt_count: int, dt_count: int) -> str:
        """简单判断市场状态."""
        total = zt_count + dt_count
        if total == 0:
            return "数据不足"
        ratio = zt_count / total
        if zt_count >= 50 and ratio > 0.85:
            return "强势市场 🟢"
        elif dt_count >= 30 or ratio < 0.6:
            return "弱势市场 🔴"
        else:
            return "震荡市场 🟡"

    def _compute_score(self, card: SignalCard) -> float:
        """计算综合得分."""
        # lhb_institution 需要归一化（范围差异大）
        lhb_norm = min(max(card.lhb_institution / 5e8, -1.0), 1.0)  # 5亿归一化到 [-1, 1]

        # turnover_rank 反向因子：排名越低（成交量小）越好 → 用 (1 - rank)
        turnover_positive = 1.0 - card.turnover_rank

        score = (
            self.WEIGHTS["theme_crowding"] * card.theme_crowding
            + self.WEIGHTS["leader_clarity"] * card.leader_clarity
            + self.WEIGHTS["lhb_institution"] * max(lhb_norm, 0)
            + self.WEIGHTS["turnover_rank"] * turnover_positive
        )

        # 连板加分（1板+0，2板+0.05，3板+0.10）
        bonus = min((card.consecutive_zt - 1) * 0.05, 0.15)
        score += bonus

        return min(score, 1.0)

    def _signal_level(self, score: float) -> str:
        """判断信号等级."""
        if score >= self.LEVEL_A_THRESHOLD:
            return "A"
        elif score >= self.LEVEL_B_THRESHOLD:
            return "B"
        else:
            return "C"

    def _generate_reasons(self, card: SignalCard) -> list[str]:
        """生成入选理由."""
        reasons = []

        if card.consecutive_zt >= 3:
            reasons.append(f"{card.consecutive_zt}连板，市场高度关注")
        elif card.consecutive_zt == 2:
            reasons.append("2连板，接力情绪强")

        if card.theme_crowding >= 0.8:
            reasons.append(f"板块拥挤度高({card.theme_crowding:.2f})，资金扎堆")

        if card.leader_clarity >= 0.7:
            reasons.append(f"龙头地位清晰({card.leader_clarity:.2f})")

        if card.lhb_institution > 1e8:
            reasons.append(f"龙虎榜净买入{card.lhb_institution/1e8:.1f}亿，大资金认可")

        if card.open_count == 0 and card.consecutive_zt >= 2:
            reasons.append("一字板/秒板，筹码锁定强")

        if not reasons:
            reasons.append("涨停首板，板块关注度一般")

        return reasons

    def _generate_risks(self, card: SignalCard) -> list[str]:
        """生成风险提示."""
        risks = []

        if card.open_count >= 3:
            risks.append(f"炸板{card.open_count}次，分歧严重")

        if card.consecutive_zt >= 5:
            risks.append("高位连板，接力风险极大")

        if card.circulation_mv > 0 and card.circulation_mv < 3e9:
            risks.append("小盘股(流通市值<30亿)，流动性风险")

        if card.circulation_mv > 0 and card.circulation_mv > 20e9:
            risks.append("大盘股，连板持续性存疑")

        if card.turnover_rank > 0.9:
            risks.append("换手率极高，短期获利盘压力大")

        if card.lhb_institution < -1e8:
            risks.append(f"龙虎榜净卖出{abs(card.lhb_institution)/1e8:.1f}亿")

        return risks
