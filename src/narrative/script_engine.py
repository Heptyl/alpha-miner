"""市场剧本引擎 — 从结构化数据生成自然语言策略解读。

核心差异化模块：让 LLM 用行为金融理论框架写"市场剧本"。
"""

import json
import logging
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd

from src.data.storage import Storage
from src.drift.daily_brief import DailyBrief

logger = logging.getLogger(__name__)

PROMPT_PATH = Path(__file__).parent / "prompts" / "market_script.md"


@dataclass
class MarketSnapshot:
    """喂给 LLM 的当日市场快照（结构化，非原始 DataFrame）。"""
    date: str

    # 温度计数据
    regime: str = ""
    emotion_level: str = ""
    zt_count: int = 0
    dt_count: int = 0
    highest_board: int = 0
    zb_count: int = 0

    # 涨停梯队
    board_ladder: list[dict] = field(default_factory=list)
    # 题材热度
    hot_themes: list[dict] = field(default_factory=list)
    # 龙虎榜摘要
    lhb_summary: list[dict] = field(default_factory=list)
    # 关键新闻
    key_news: list[dict] = field(default_factory=list)
    # 资金流
    fund_flow_summary: dict = field(default_factory=dict)


@dataclass
class MarketScript:
    """市场剧本分析结果。"""
    date: str
    script_title: str = ""
    script_narrative: str = ""
    theme_verdicts: list[dict] = field(default_factory=list)
    tomorrow_playbook: dict = field(default_factory=dict)
    risk_alerts: list[str] = field(default_factory=list)
    raw_snapshot: Optional[dict] = None

    def to_dict(self) -> dict:
        return asdict(self)


class ScriptEngine:
    """市场剧本引擎：从结构化数据生成自然语言策略解读。"""

    def __init__(self, db: Storage, llm_client=None):
        self.db = db
        self.llm_client = llm_client
        self.brief = DailyBrief(db)

    def generate(self, as_of: datetime, report_date: str = "") -> MarketScript:
        """生成当日市场剧本。"""
        if not report_date:
            report_date = (as_of - timedelta(days=1)).strftime("%Y-%m-%d")

        # 1. 组装快照
        snapshot = self._build_snapshot(as_of, report_date)

        # 2. 无 LLM 时返回纯结构化摘要
        if not self.llm_client:
            return self._fallback_script(report_date, snapshot)

        # 3. 有 LLM 时生成完整剧本
        return self._llm_generate(report_date, snapshot)

    def _build_snapshot(self, as_of: datetime, report_date: str = "") -> MarketSnapshot:
        """从数据库组装当日快照。"""
        if not report_date:
            report_date = (as_of - timedelta(days=1)).strftime("%Y-%m-%d")

        # 温度计
        thermo = self.brief.build_thermometer(as_of, report_date=report_date)

        # 涨停梯队
        zt_df = self.db.query("zt_pool", as_of,
                              where="trade_date = ?", params=(report_date,))
        board_ladder = self._build_board_ladder(zt_df)

        # 题材热度
        concept_df = self.db.query("concept_daily", as_of,
                                   where="trade_date = ?", params=(report_date,))
        hot_themes = self._build_hot_themes(concept_df)

        # 龙虎榜摘要
        lhb_df = self.db.query("lhb_detail", as_of,
                               where="trade_date = ?", params=(report_date,))
        lhb_summary = self._build_lhb_summary(lhb_df)

        # 关键新闻（已分类）
        news_df = self.db.query("news", as_of,
                                where="publish_time LIKE ?", params=(f"{report_date}%",))
        key_news = self._build_key_news(news_df)

        # 资金流
        fund_df = self.db.query("fund_flow", as_of,
                                where="trade_date = ?", params=(report_date,))
        fund_summary = self._build_fund_summary(fund_df)

        return MarketSnapshot(
            date=report_date,
            regime=thermo.regime,
            emotion_level=thermo.emotion_level,
            zt_count=thermo.zt_count,
            dt_count=thermo.dt_count,
            highest_board=thermo.highest_board,
            zb_count=thermo.zb_count,
            board_ladder=board_ladder,
            hot_themes=hot_themes,
            lhb_summary=lhb_summary,
            key_news=key_news,
            fund_flow_summary=fund_summary,
        )

    # ── 子方法: 组装各模块数据 ─────────────────────

    def _build_board_ladder(self, zt_df: pd.DataFrame) -> list[dict]:
        """从 zt_pool 构建涨停梯队。"""
        if zt_df.empty or "consecutive_zt" not in zt_df.columns:
            return []

        ladder = []
        for height, group in zt_df.groupby("consecutive_zt"):
            stocks = []
            for _, row in group.iterrows():
                code = row.get("stock_code", "")
                name = row.get("stock_name", code)
                stocks.append(f"{code} {name}")
            ladder.append({
                "height": int(height),
                "stocks": stocks[:5],
                "count": len(group),
            })
        ladder.sort(key=lambda x: x["height"], reverse=True)
        return ladder

    def _build_hot_themes(self, concept_df: pd.DataFrame) -> list[dict]:
        """从 concept_daily 构建题材热度。"""
        if concept_df.empty:
            return []

        themes = []
        for _, row in concept_df.iterrows():
            zt_count = int(row.get("zt_count", 0))
            if zt_count < 2:
                continue
            themes.append({
                "theme": row.get("concept_name", ""),
                "zt_count": zt_count,
                "leader": row.get("leader_code", ""),
                "leader_consecutive": int(row.get("leader_consecutive", 0)),
            })
        themes.sort(key=lambda x: x["zt_count"], reverse=True)
        return themes[:10]

    def _build_lhb_summary(self, lhb_df: pd.DataFrame) -> list[dict]:
        """从 lhb_detail 构建龙虎榜摘要。"""
        if lhb_df.empty:
            return []

        # 按股票代码聚合，计算每只股的净买入总额和上榜原因
        agg = {}
        for _, row in lhb_df.iterrows():
            code = row.get("stock_code", "")
            if not code:
                continue
            if code not in agg:
                agg[code] = {
                    "stock": code,
                    "total_net": 0.0,
                    "buy_total": 0.0,
                    "sell_total": 0.0,
                    "top_buyer": row.get("buy_depart", ""),
                    "reason": row.get("reason", ""),
                }
            buy = float(row.get("buy_amount", 0) or 0)
            sell = float(row.get("sell_amount", 0) or 0)
            agg[code]["total_net"] += buy - sell
            agg[code]["buy_total"] += buy
            agg[code]["sell_total"] += sell

        # 按净买入排序取 top 5
        results = sorted(agg.values(), key=lambda x: x["total_net"], reverse=True)
        for r in results:
            r["total_net_wan"] = round(r["total_net"] / 10000, 2)
        return results[:5]

    def _build_key_news(self, news_df: pd.DataFrame) -> list[dict]:
        """从 news 表构建关键新闻列表（已分类的）。"""
        if news_df.empty:
            return []

        key = []
        for _, row in news_df.iterrows():
            ntype = row.get("news_type", "noise")
            if ntype == "noise":
                continue
            key.append({
                "stock": row.get("stock_code", ""),
                "title": row.get("title", ""),
                "type": ntype,
                "sentiment": float(row.get("sentiment_score", 0.5)),
            })
        # 按置信度降序（如果有）
        key.sort(key=lambda x: float(
            news_df[news_df["title"] == x.get("title", "")].iloc[0].get("classify_confidence", 0)
        ) if not news_df[news_df["title"] == x.get("title", "")].empty else 0, reverse=True)
        return key[:10]

    def _build_fund_summary(self, fund_df: pd.DataFrame) -> dict:
        """资金流聚合。"""
        if fund_df.empty:
            return {"super_large_net_total": 0, "large_net_total": 0, "direction": "未知"}

        sln = fund_df["super_large_net"].sum() if "super_large_net" in fund_df.columns else 0
        ln = fund_df["large_net"].sum() if "large_net" in fund_df.columns else 0
        direction = "流入" if (sln + ln) > 0 else "流出"
        return {
            "super_large_net_total": round(float(sln), 2),
            "large_net_total": round(float(ln), 2),
            "direction": direction,
        }

    # ── LLM 生成 ──────────────────────────────────

    def _llm_generate(self, date_str: str, snapshot: MarketSnapshot) -> MarketScript:
        """用 LLM 生成市场剧本。"""
        if not PROMPT_PATH.exists():
            logger.warning("market_script.md prompt 不存在，回退规则版")
            return self._fallback_script(date_str, snapshot)

        prompt_template = PROMPT_PATH.read_text(encoding="utf-8")
        snapshot_json = json.dumps(asdict(snapshot), ensure_ascii=False, indent=2)
        prompt = prompt_template.replace("{market_snapshot_json}", snapshot_json)

        try:
            response = self.llm_client.messages.create(
                model="glm-4-plus",
                max_tokens=1500,
                temperature=0.3,
                messages=[{"role": "user", "content": prompt}],
            )
            text = response.content[0].text

            # 清理 JSON
            if "```json" in text:
                text = text.split("```json")[1].split("```")[0]
            elif "```" in text:
                text = text.split("```")[1].split("```")[0]

            data = json.loads(text.strip())
            return MarketScript(
                date=date_str,
                script_title=data.get("script_title", ""),
                script_narrative=data.get("script_narrative", ""),
                theme_verdicts=data.get("theme_verdicts", []),
                tomorrow_playbook=data.get("tomorrow_playbook", {}),
                risk_alerts=data.get("risk_alerts", []),
                raw_snapshot=asdict(snapshot),
            )
        except Exception as e:
            logger.error("剧本生成失败: %s", e)
            return self._fallback_script(date_str, snapshot)

    def _fallback_script(self, date_str: str, snapshot: MarketSnapshot) -> MarketScript:
        """无 LLM 时的纯规则回退。"""
        title = f"{snapshot.regime} | 涨停{snapshot.zt_count} 跌停{snapshot.dt_count}"
        narrative = (
            f"今日 regime={snapshot.regime}，涨停{snapshot.zt_count}家，"
            f"跌停{snapshot.dt_count}家，最高板{snapshot.highest_board}连板，"
            f"炸板{snapshot.zb_count}家。情绪级别: {snapshot.emotion_level}。"
        )

        # 简单的规则性题材判断
        theme_verdicts = []
        for t in snapshot.hot_themes[:3]:
            stage = "爆发"
            if t.get("leader_consecutive", 0) <= 2:
                stage = "萌芽"
            elif t.get("zt_count", 0) > snapshot.zt_count * 0.3:
                stage = "高潮"
            theme_verdicts.append({
                "theme": t["theme"],
                "stage": stage,
                "verdict": f"题材{'处于' + stage + '期，注意风险' if stage == '高潮' else '可关注'}",
                "reasoning": f"涨停{t['zt_count']}家，龙头{t.get('leader', '')}连板{t.get('leader_consecutive', 0)}天",
            })

        # 风险提示
        risk_alerts = []
        if snapshot.zb_count > snapshot.zt_count * 0.3:
            risk_alerts.append(f"炸板率{snapshot.zb_count}/{snapshot.zt_count}，分歧加大")
        for item in snapshot.lhb_summary:
            if item.get("total_net", 0) < -50000000:
                risk_alerts.append(f"{item['stock']} 龙虎榜净卖出{abs(item.get('total_net_wan', 0))}万")

        return MarketScript(
            date=date_str,
            script_title=title,
            script_narrative=narrative,
            theme_verdicts=theme_verdicts,
            tomorrow_playbook={
                "primary_strategy": f"regime={snapshot.regime}，"
                                    f"{'追龙头' if snapshot.regime == '连板潮' else '卡位低吸' if snapshot.regime == '题材轮动' else '休息'}",
                "watch_list": [t["theme"] for t in snapshot.hot_themes[:3]],
                "avoid_list": [],
                "position_advice": f"建议仓位{'60%' if snapshot.emotion_level in ('偏强', '强') else '30%' if snapshot.emotion_level == '中性' else '0%'}",
            },
            risk_alerts=risk_alerts,
            raw_snapshot=asdict(snapshot),
        )

    def save_script(self, script: MarketScript) -> None:
        """将剧本存入 market_scripts 表。"""
        conn = self.db._get_conn()
        try:
            conn.execute(
                """INSERT OR REPLACE INTO market_scripts
                   (trade_date, script_title, script_narrative,
                    theme_verdicts, tomorrow_playbook, risk_alerts, raw_snapshot)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (
                    script.date,
                    script.script_title,
                    script.script_narrative,
                    json.dumps(script.theme_verdicts, ensure_ascii=False),
                    json.dumps(script.tomorrow_playbook, ensure_ascii=False),
                    json.dumps(script.risk_alerts, ensure_ascii=False),
                    json.dumps(script.raw_snapshot, ensure_ascii=False),
                ),
            )
            conn.commit()
        finally:
            conn.close()

    def load_script(self, trade_date: str) -> Optional[MarketScript]:
        """从 market_scripts 表读取剧本。"""
        conn = self.db._get_conn()
        try:
            row = conn.execute(
                "SELECT * FROM market_scripts WHERE trade_date = ? "
                "ORDER BY snapshot_time DESC LIMIT 1",
                (trade_date,),
            ).fetchone()
            if not row:
                return None
            return MarketScript(
                date=row["trade_date"],
                script_title=row["script_title"] or "",
                script_narrative=row["script_narrative"] or "",
                theme_verdicts=json.loads(row["theme_verdicts"] or "[]"),
                tomorrow_playbook=json.loads(row["tomorrow_playbook"] or "{}"),
                risk_alerts=json.loads(row["risk_alerts"] or "[]"),
                raw_snapshot=json.loads(row["raw_snapshot"] or "null"),
            )
        except Exception as e:
            logger.error("加载剧本失败: %s", e)
            return None
        finally:
            conn.close()
