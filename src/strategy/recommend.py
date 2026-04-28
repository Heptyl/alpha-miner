"""每日个股推荐引擎。

综合因子得分、技术分析、市场热点，每日推荐 TOP 5 个股及买入区间。

核心流程：
1. 候选池构建（涨停池 + 强势股池 + 资金流入）
2. 多因子综合打分
3. 技术面分析（支撑/压力/买入区间）
4. 过滤 & 排序
5. 输出推荐报告
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import yaml

from src.data.storage import Storage
from src.strategy.technical import TechnicalAnalysis, compute_technical


@dataclass
class StockRecommendation:
    """单只推荐股。"""

    stock_code: str
    stock_name: str
    industry: str
    concepts: list[str]

    # 因子得分
    factor_scores: dict = field(default_factory=dict)
    composite_score: float = 0.0

    # 技术面
    technical: Optional[TechnicalAnalysis] = None

    # 市场特征
    consecutive_zt: int = 0
    open_count: int = 0
    amount: float = 0.0
    circulation_mv: float = 0.0
    fund_net_amount: float = 0.0  # 主力净流入

    # 推荐信息
    buy_zone_low: float = 0.0
    buy_zone_high: float = 0.0
    buy_price: float = 0.0          # 建议买入价
    stop_loss: float = 0.0          # 止损价
    target_price: float = 0.0       # 目标价
    reasons: list[str] = field(default_factory=list)
    risks: list[str] = field(default_factory=list)
    signal_level: str = ""          # A/B/C

    def to_dict(self) -> dict:
        return {
            "stock_code": self.stock_code,
            "stock_name": self.stock_name,
            "industry": self.industry,
            "concepts": self.concepts[:3],
            "composite_score": round(self.composite_score, 3),
            "signal_level": self.signal_level,
            "consecutive_zt": self.consecutive_zt,
            "buy_zone_low": round(self.buy_zone_low, 2),
            "buy_zone_high": round(self.buy_zone_high, 2),
            "buy_price": round(self.buy_price, 2),
            "stop_loss": round(self.stop_loss, 2),
            "target_price": round(self.target_price, 2),
            "factor_scores": {k: round(v, 3) for k, v in self.factor_scores.items()},
            "technical": self.technical.to_dict() if self.technical else None,
            "fund_net_amount": round(self.fund_net_amount, 0),
            "reasons": self.reasons,
            "risks": self.risks,
        }


@dataclass
class DailyRecommendation:
    """每日推荐报告。"""

    trade_date: str
    stocks: list[StockRecommendation]

    # 大盘概况
    zt_count: int = 0
    dt_count: int = 0
    market_regime: str = ""
    hot_industries: list[dict] = field(default_factory=list)
    hot_concepts: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "trade_date": self.trade_date,
            "zt_count": self.zt_count,
            "dt_count": self.dt_count,
            "market_regime": self.market_regime,
            "hot_industries": self.hot_industries[:5],
            "hot_concepts": self.hot_concepts[:5],
            "stocks": [s.to_dict() for s in self.stocks],
        }

    def to_text(self) -> str:
        """终端纯文本输出。"""
        lines = []
        lines.append("=" * 65)
        lines.append(f"  Alpha Miner 每日个股推荐 — {self.trade_date}")
        lines.append("=" * 65)

        # 大盘
        lines.append(f"\n  涨停 {self.zt_count} 只 | 跌停 {self.dt_count} 只 | {self.market_regime}")

        # 热门板块
        if self.hot_industries:
            lines.append("\n  热门板块:")
            for hi in self.hot_industries[:3]:
                lines.append(f"    {hi.get('industry', '?')}: {hi.get('zt_count', 0)}只涨停"
                             f" {'★' * min(hi.get('zt_count', 0), 5)}")

        # 热门概念
        if self.hot_concepts:
            lines.append(f"\n  热门概念: {', '.join(self.hot_concepts[:5])}")

        # 推荐个股
        if not self.stocks:
            lines.append("\n  今日无符合条件的推荐个股")
        else:
            lines.append(f"\n  ┌{'─' * 61}┐")
            lines.append(f"  │  今日推荐 {len(self.stocks)} 只个股"
                         f"{' ' * (48 - len(str(len(self.stocks))))}│")
            lines.append(f"  └{'─' * 61}┘")

            for i, stock in enumerate(self.stocks, 1):
                lines.append(f"\n  ┌─ [{stock.signal_level}] #{i} {stock.stock_code} {stock.stock_name}"
                             f" ── 综合分 {stock.composite_score:.2f}")

                lines.append(f"  │  板块: {stock.industry}"
                             f"  |  连板: {stock.consecutive_zt}"
                             f"  |  趋势: {stock.technical.trend if stock.technical else 'N/A'}")

                # 买入点位
                lines.append(f"  │")
                lines.append(f"  │  ★ 买入区间: {stock.buy_zone_low:.2f} ~ {stock.buy_zone_high:.2f}")
                lines.append(f"  │  ★ 建议买价: {stock.buy_price:.2f}")
                lines.append(f"  │  ★ 目标价位: {stock.target_price:.2f}"
                             f" (+{(stock.target_price/stock.buy_price-1)*100:.1f}%)" if stock.buy_price > 0 else
                             f"  │  ★ 目标价位: {stock.target_price:.2f}")
                lines.append(f"  │  ★ 止损价位: {stock.stop_loss:.2f}"
                             f" (-{(1-stock.stop_loss/stock.buy_price)*100:.1f}%)" if stock.buy_price > 0 else
                             f"  │  ★ 止损价位: {stock.stop_loss:.2f}")

                # 因子得分
                if stock.factor_scores:
                    top_factors = sorted(
                        stock.factor_scores.items(), key=lambda x: x[1], reverse=True
                    )[:3]
                    factor_str = " | ".join(f"{k}: {v:.2f}" for k, v in top_factors)
                    lines.append(f"  │  核心因子: {factor_str}")

                # 资金流向
                if abs(stock.fund_net_amount) > 1e7:
                    direction = "流入" if stock.fund_net_amount > 0 else "流出"
                    lines.append(f"  │  主力资金{direction}: {abs(stock.fund_net_amount)/1e8:.2f}亿")

                # 概念
                if stock.concepts:
                    lines.append(f"  │  概念: {', '.join(stock.concepts[:3])}")

                # 理由
                for r in stock.reasons[:3]:
                    lines.append(f"  │  ✓ {r}")

                # 风险
                for r in stock.risks[:2]:
                    lines.append(f"  │  ⚠ {r}")

                lines.append(f"  └{'─' * 61}")

        lines.append(f"\n  ⚠ 免责声明: 以上推荐基于量化模型，仅供参考，不构成投资建议。")
        lines.append(f"  数据截至 {self.trade_date} 收盘。建议次日集合竞价观察后再决策。")
        lines.append("=" * 65)

        return "\n".join(lines)


class RecommendEngine:
    """每日个股推荐引擎。"""

    # 因子权重（基于 IC 验证结果 + 技术面）
    DEFAULT_WEIGHTS = {
        "theme_crowding": 0.25,
        "leader_clarity": 0.25,
        "lhb_institution": 0.15,
        "turnover_rank": 0.10,
        "consecutive_board": 0.05,
        "momentum_score": 0.10,
        "volume_ratio": 0.10,
    }

    LEVEL_A_THRESHOLD = 0.65
    LEVEL_B_THRESHOLD = 0.45

    def __init__(self, db: Storage, config_path: str | None = None):
        self.db = db
        self.config = self._load_config(config_path)

    def _load_config(self, config_path: str | None) -> dict:
        """加载推荐配置。"""
        if config_path is None:
            config_path = Path(__file__).parent.parent.parent / "config" / "recommend.yaml"
        config_path = Path(config_path)
        if config_path.exists():
            with open(config_path, "r", encoding="utf-8") as f:
                return yaml.safe_load(f) or {}
        return {}

    def recommend(
        self,
        as_of: datetime,
        report_date: str,
        top_n: int = 5,
    ) -> DailyRecommendation:
        """生成每日推荐。

        Args:
            as_of: 时间锚点
            report_date: 报告日期 YYYY-MM-DD
            top_n: 推荐只数（默认5）
        """
        # 1. 构建候选池
        candidates = self._build_candidates(as_of, report_date)

        if not candidates:
            return DailyRecommendation(
                trade_date=report_date,
                stocks=[],
                zt_count=0,
                dt_count=0,
                market_regime="无数据",
            )

        # 2. 加载因子值
        factor_map = self._load_factors(report_date)

        # 3. 加载概念映射
        concept_map = self._load_concepts()

        # 4. 加载资金流向
        fund_flow_map = self._load_fund_flow(report_date)

        # 5. 构建推荐
        recommendations = []
        for code, info in candidates.items():
            factors = factor_map.get(code, {})
            concepts = concept_map.get(code, [])
            fund = fund_flow_map.get(code, 0.0)

            rec = StockRecommendation(
                stock_code=code,
                stock_name=info.get("name", ""),
                industry=info.get("industry", ""),
                concepts=concepts,
                factor_scores=factors,
                consecutive_zt=info.get("consecutive_zt", 0),
                open_count=info.get("open_count", 0),
                amount=info.get("amount", 0),
                circulation_mv=info.get("circulation_mv", 0),
                fund_net_amount=fund,
            )

            # 6. 技术分析
            price_df = self._load_price_history(code, as_of, 30)
            if not price_df.empty:
                ta = compute_technical(price_df)
                rec.technical = ta

                # 加入技术面因子
                if ta:
                    rec.factor_scores["momentum_score"] = ta.momentum_score
                    rec.factor_scores["volume_ratio"] = min(ta.volume_ratio / 3.0, 1.0)

            # 7. 综合打分
            rec.composite_score = self._compute_score(rec)

            # 8. 计算买入点位
            self._compute_price_levels(rec)

            # 9. 生成理由和风险
            rec.reasons = self._generate_reasons(rec)
            rec.risks = self._generate_risks(rec)
            rec.signal_level = self._signal_level(rec.composite_score)

            recommendations.append(rec)

        # 10. 基本面过滤
        codes = [r.stock_code for r in recommendations]
        passed_codes, rejected_fund = self._filter_fundamentals(codes)
        recommendations = [r for r in recommendations if r.stock_code in passed_codes]

        # 11. 追高保护 — 惩罚短期暴涨股
        from src.strategy.chase_protection import batch_chase_risk
        chase_risks = batch_chase_risk(codes, report_date)
        for rec in recommendations:
            risk = chase_risks.get(rec.stock_code)
            if risk:
                rec.composite_score = rec.composite_score * (1 - risk.score_penalty)
                if risk.risk_level == "extreme":
                    rec.risks.insert(0, f"追高极危:{risk.reasons[0]}")
                elif risk.risk_level == "high":
                    rec.risks.insert(0, f"追高警示:{risk.reasons[0]}")

        # 12. 历史胜率回测
        from src.strategy.win_rate_backtest import backtest_pattern
        for rec in recommendations:
            bt = backtest_pattern(
                rec.stock_code, report_date, rec.consecutive_zt,
                hold_days=3, db_path=self.db.db_path,
            )
            if bt:
                # 胜率低于50% → 惩罚
                if bt.win_rate < 50:
                    penalty = (50 - bt.win_rate) / 100
                    rec.composite_score = rec.composite_score * (1 - penalty)
                # 胜率 > 60% → 小幅加分
                elif bt.win_rate >= 60 and bt.confidence != "low":
                    rec.composite_score = min(rec.composite_score + 0.03, 1.0)
                rec.reasons.append(f"历史胜率{bt.win_rate:.0f}%({bt.total_trades}次)")

        # 13. 过滤
        filtered = self._apply_filters(recommendations)

        # 14. 排序 & 截取
        filtered.sort(key=lambda r: r.composite_score, reverse=True)
        filtered = filtered[:top_n]

        # 15. 市场概况
        zt_count, dt_count = self._count_zt_dt(report_date)
        regime = self._market_regime(zt_count, dt_count)
        hot_industries = self._hot_industries(report_date)
        hot_concepts = self._hot_concepts(report_date, candidates)

        return DailyRecommendation(
            trade_date=report_date,
            stocks=filtered,
            zt_count=zt_count,
            dt_count=dt_count,
            market_regime=regime,
            hot_industries=hot_industries,
            hot_concepts=hot_concepts,
        )

    def _build_candidates(
        self, as_of: datetime, report_date: str,
    ) -> dict[str, dict]:
        """构建候选池: 涨停池 + 强势股池。"""
        candidates = {}

        # 涨停池（优先级最高）
        zt_df = self.db.query(
            "zt_pool", as_of, where="trade_date = ?", params=(report_date,),
            bypass_snapshot=True,  # 推荐场景：用最新数据，不做快照时间隔离
        )
        if not zt_df.empty:
            for _, row in zt_df.iterrows():
                code = row["stock_code"]
                if code not in candidates:
                    candidates[code] = {
                        "name": str(row.get("name", "")),
                        "industry": str(row.get("industry", "")),
                        "consecutive_zt": int(row.get("consecutive_zt", 0)),
                        "open_count": int(row.get("open_count", 0)),
                        "amount": float(row.get("amount", 0)),
                        "circulation_mv": float(row.get("circulation_mv", 0)),
                        "source": "zt_pool",
                    }

        # 强势股池
        strong_df = self.db.query(
            "strong_pool", as_of, where="trade_date = ?", params=(report_date,),
            bypass_snapshot=True,
        )
        if not strong_df.empty:
            for _, row in strong_df.iterrows():
                code = row["stock_code"]
                if code not in candidates:
                    candidates[code] = {
                        "name": str(row.get("name", "")),
                        "industry": str(row.get("industry", "")),
                        "consecutive_zt": 0,
                        "open_count": 0,
                        "amount": float(row.get("amount", 0)),
                        "circulation_mv": 0,
                        "source": "strong_pool",
                    }

        return candidates

    def _load_factors(self, report_date: str) -> dict[str, dict]:
        """加载因子值。"""
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
        """加载概念映射。"""
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

    def _load_fund_flow(self, report_date: str) -> dict[str, float]:
        """加载资金流向（主力净流入）。"""
        conn = sqlite3.connect(self.db.db_path)
        # 优先使用 main_net（schema定义列），如存在 net_amount 则用 net_amount
        try:
            net_col = "net_amount"
            conn.execute(f"SELECT {net_col} FROM fund_flow LIMIT 0")
        except sqlite3.OperationalError:
            net_col = "main_net"
        rows = conn.execute(
            f"SELECT stock_code, {net_col} FROM fund_flow WHERE trade_date = ?",
            (report_date,),
        ).fetchall()
        conn.close()

        result: dict[str, float] = {}
        for code, net in rows:
            result[code] = float(net) if net else 0.0
        return result

    def _load_price_history(
        self, stock_code: str, as_of: datetime, lookback_days: int = 30,
    ) -> pd.DataFrame:
        """加载个股历史K线。"""
        return self.db.query_range(
            "daily_price", as_of, lookback_days,
            where="stock_code = ?", params=(stock_code,),
        )

    def _compute_score(self, rec: StockRecommendation) -> float:
        """计算综合得分。"""
        weights = self.config.get("factor_weights", self.DEFAULT_WEIGHTS)

        score = 0.0
        total_weight = 0.0

        for factor_name, weight in weights.items():
            value = rec.factor_scores.get(factor_name, 0.0)

            # 特殊处理
            if factor_name == "turnover_rank":
                # 反向因子：排名越低越好
                value = 1.0 - min(max(value, 0), 1)
            elif factor_name == "lhb_institution":
                # 归一化到 [-1, 1]，5亿封顶
                value = min(max(value / 5e8, -1.0), 1.0)
                value = max(value, 0)  # 只取正向
            elif factor_name == "consecutive_board":
                # 连板转为 0-1
                value = min(value / 5.0, 1.0)

            score += weight * value
            total_weight += weight

        # 连板加分
        if rec.consecutive_zt >= 2:
            bonus = min((rec.consecutive_zt - 1) * 0.05, 0.15)
            score += bonus

        # 资金流入加分
        if rec.fund_net_amount > 1e8:
            score += min(rec.fund_net_amount / 1e9 * 0.05, 0.10)

        return min(score, 1.0)

    def _compute_price_levels(self, rec: StockRecommendation) -> None:
        """计算买入点位、止损、目标价。
        
        关键：所有价格基于当日收盘价（前复权），推荐的是【次日操作建议】。
        
        逻辑：
        - 非涨停股：次日低吸，买在支撑位附近
        - 涨停股：次日可能高开，买入区间参考次日开盘预期
        - 止损 = 当日收盘 * 0.95 或支撑位下方
        - 目标 = 当日收盘 * 1.05~1.10
        """
        ta = rec.technical
        if ta is None:
            # 无技术分析时，用保守默认值
            rec.buy_price = 0.0
            rec.buy_zone_low = 0.0
            rec.buy_zone_high = 0.0
            rec.stop_loss = 0.0
            rec.target_price = 0.0
            return

        close = ta.current_price   # 当日收盘价（已涨停的话就是涨停价）
        support = ta.support_price
        resistance = ta.resistance_price
        atr = ta.atr
        is_zt = rec.consecutive_zt >= 1  # 是否涨停

        if close <= 0:
            return

        if is_zt:
            # ── 涨停股的买入点位 ──
            # 涨停次日通常高开，买入区间基于【预期次日价格】
            # 次日参考价 ≈ 收盘价（涨停价），高开2%~5%常见
            next_ref = close  # 次日开盘基准
            
            # 买入区间：高开后回落的低吸区间
            rec.buy_zone_low = round(close * 0.97, 2)   # 回落3%可低吸
            rec.buy_zone_high = round(close * 1.03, 2)   # 高开3%以内可追
            
            # 建议买价：区间中位偏下
            rec.buy_price = round(
                rec.buy_zone_low + (rec.buy_zone_high - rec.buy_zone_low) * 0.4, 2
            )
            
            # 连板越多，次日高开概率越高但风险也越大
            if rec.consecutive_zt >= 3:
                # 高位连板，保守一点
                rec.buy_zone_high = round(close * 1.01, 2)
                rec.buy_price = round(close * 0.99, 2)
        else:
            # ── 非涨停股的买入点位 ──
            # 次日低吸策略
            rec.buy_zone_low = round(max(support * 0.98, close * 0.95), 2)
            rec.buy_zone_high = round(min(close * 1.01, support * 1.03), 2)
            
            # 确保下限 <= 上限
            if rec.buy_zone_low > rec.buy_zone_high:
                rec.buy_zone_low = round(close * 0.97, 2)
                rec.buy_zone_high = round(close * 1.00, 2)
            
            # 建议买价
            rec.buy_price = round(
                rec.buy_zone_low + (rec.buy_zone_high - rec.buy_zone_low) * 0.3, 2
            )

        # 止损价：当前价 -5% 或 支撑位下方1个ATR（取较大者=更保守）
        stop_by_pct = close * 0.95
        stop_by_atr = support - atr if atr > 0 else close * 0.93
        rec.stop_loss = round(max(stop_by_pct, stop_by_atr, close * 0.90), 2)
        
        # 确保止损价 < 买入价
        if rec.stop_loss >= rec.buy_price:
            rec.stop_loss = round(rec.buy_price * 0.95, 2)

        # 目标价：压力位 或 +5%~10%
        target_by_resist = resistance if resistance > close else close * 1.08
        target_by_atr = close + atr * 1.5 if atr > 0 else close * 1.05
        rec.target_price = round(min(target_by_resist, target_by_atr, close * 1.15), 2)

        # 确保目标价 > 买入价（至少3%利润空间）
        if rec.target_price <= rec.buy_price * 1.03:
            rec.target_price = round(rec.buy_price * 1.05, 2)

    def _apply_filters(
        self, recs: list[StockRecommendation],
    ) -> list[StockRecommendation]:
        """应用过滤条件。"""
        filters = self.config.get("filters", {})
        min_amount = float(filters.get("min_amount", 5e7))
        min_score = float(filters.get("min_composite_score", 0.35))
        max_open = int(filters.get("max_open_count", 2))

        result = []
        for rec in recs:
            # 最低综合分
            if rec.composite_score < min_score:
                continue
            # 炸板次数
            if rec.open_count > max_open:
                continue
            # 成交额
            if rec.amount > 0 and rec.amount < min_amount:
                continue

            result.append(rec)
        return result

    def _filter_fundamentals(
        self, codes: list[str],
    ) -> tuple[list[str], dict[str, list[str]]]:
        """基本面过滤：剔除ST/亏损/高估值。"""
        try:
            from src.data.sources.fundamentals import filter_by_fundamentals
            return filter_by_fundamentals(
                codes, db_path=self.db.db_path,
                max_pe=200.0, min_roe=-50.0, exclude_st=True,
            )
        except Exception:
            # 基本面数据采集失败时放行（不因缺数据拒绝）
            return codes, {}

    def _signal_level(self, score: float) -> str:
        if score >= self.LEVEL_A_THRESHOLD:
            return "A"
        elif score >= self.LEVEL_B_THRESHOLD:
            return "B"
        return "C"

    def _generate_reasons(self, rec: StockRecommendation) -> list[str]:
        """生成推荐理由。"""
        reasons = []

        # 连板
        if rec.consecutive_zt >= 3:
            reasons.append(f"{rec.consecutive_zt}连板，市场辨识度高")
        elif rec.consecutive_zt == 2:
            reasons.append("2连板，接力情绪强")

        # 龙头度
        if rec.factor_scores.get("leader_clarity", 0) >= 0.6:
            reasons.append(f"龙头地位清晰({rec.factor_scores['leader_clarity']:.2f})")

        # 板块拥挤度
        if rec.factor_scores.get("theme_crowding", 0) >= 0.7:
            reasons.append(f"板块资金扎堆({rec.factor_scores['theme_crowding']:.2f})")

        # 资金流入
        if rec.fund_net_amount > 5e7:
            reasons.append(f"主力净流入{rec.fund_net_amount/1e8:.1f}亿")

        # 龙虎榜
        lhb = rec.factor_scores.get("lhb_institution", 0)
        if lhb > 1e8:
            reasons.append(f"龙虎榜机构买入{lhb/1e8:.1f}亿")

        # 技术面
        if rec.technical:
            if rec.technical.momentum_score >= 0.6:
                reasons.append(f"动量强劲({rec.technical.momentum_score:.2f})")
            if rec.technical.volume_ratio >= 2.0:
                reasons.append(f"放量突破(量比{rec.technical.volume_ratio:.1f})")
            if rec.technical.trend == "上涨":
                reasons.append("均线多头排列")

        if not reasons:
            reasons.append("因子综合得分靠前")

        return reasons

    def _generate_risks(self, rec: StockRecommendation) -> list[str]:
        """生成风险提示。"""
        risks = []

        if rec.open_count >= 2:
            risks.append(f"炸板{rec.open_count}次，分歧明显")

        if rec.consecutive_zt >= 5:
            risks.append("高位连板，接力风险极大")

        mv = rec.circulation_mv
        if mv > 0:
            if mv < 3e9:
                risks.append("小盘股(<30亿)，流动性风险")
            elif mv > 50e9:
                risks.append("大盘股，连板持续性存疑")

        if rec.factor_scores.get("turnover_rank", 0) > 0.85:
            risks.append("换手率极高，获利盘压力大")

        if rec.fund_net_amount < -5e7:
            risks.append(f"主力净流出{abs(rec.fund_net_amount)/1e8:.1f}亿")

        if rec.technical and rec.technical.trend == "下跌":
            risks.append("均线空头排列，趋势偏弱")

        return risks

    def _count_zt_dt(self, report_date: str) -> tuple[int, int]:
        """统计涨跌停数。"""
        conn = sqlite3.connect(self.db.db_path)
        zt = conn.execute(
            "SELECT COUNT(DISTINCT stock_code) FROM zt_pool WHERE trade_date = ?",
            (report_date,),
        ).fetchone()[0]

        dt = 0
        price_df = pd.read_sql_query(
            f"SELECT stock_code, open, close, pre_close FROM daily_price "
            f"WHERE trade_date = '{report_date}'",
            conn,
        )
        conn.close()

        if not price_df.empty and "pre_close" in price_df.columns:
            pc = price_df["pre_close"].where(price_df["pre_close"] > 0, price_df["open"])
            pct = (price_df["close"] - pc) / pc * 100
            dt = int((pct < -9.5).sum())

        return zt, dt

    def _market_regime(self, zt_count: int, dt_count: int) -> str:
        """市场状态。"""
        total = zt_count + dt_count
        if total == 0:
            return "数据不足"
        ratio = zt_count / total
        if zt_count >= 50 and ratio > 0.85:
            return "强势市场"
        elif dt_count >= 30 or ratio < 0.6:
            return "弱势市场"
        return "震荡市场"

    def _hot_industries(self, report_date: str) -> list[dict]:
        """热门板块。"""
        conn = self.db._get_conn()
        cursor = conn.execute(
            "SELECT industry, COUNT(*) as zt_count, MAX(consecutive_zt) as max_consecutive "
            "FROM zt_pool WHERE trade_date = ? AND industry != '' "
            "GROUP BY industry ORDER BY zt_count DESC LIMIT 5",
            (report_date,),
        )
        rows = cursor.fetchall()
        conn.close()
        return [dict(r) for r in rows] if rows else []

    def _hot_concepts(
        self, report_date: str, candidates: dict,
    ) -> list[str]:
        """热门概念。"""
        if not candidates:
            return []

        codes = list(candidates.keys())
        if not codes:
            return []

        conn = sqlite3.connect(self.db.db_path)
        placeholders = ",".join(["?"] * len(codes))
        rows = conn.execute(
            f"SELECT concept_name, COUNT(*) as cnt FROM concept_mapping "
            f"WHERE stock_code IN ({placeholders}) "
            f"GROUP BY concept_name ORDER BY cnt DESC LIMIT 5",
            codes,
        ).fetchall()
        conn.close()
        return [r[0] for r in rows] if rows else []
