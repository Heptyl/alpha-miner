"""复盘引擎 — 对比昨日剧本预测与今日实际走势。

核心：交易系统自检 — 记录判断对/错，发现偏差模式。
"""

import json
import logging
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd

from src.data.storage import Storage
from src.narrative.script_engine import ScriptEngine, MarketScript

logger = logging.getLogger(__name__)

PROMPT_PATH = Path(__file__).parent / "prompts" / "replay.md"


@dataclass
class ReplayResult:
    """复盘结果。"""
    date: str                       # 被复盘的日期
    yesterday_script: Optional[dict] = None  # 昨日剧本（raw_snapshot）
    yesterday_playbook: Optional[dict] = None
    actual_regime: str = ""
    actual_zt_count: int = 0
    actual_dt_count: int = 0
    actual_highest_board: int = 0

    # 对比结论
    regime_match: bool = False
    playbook_hits: list[str] = field(default_factory=list)
    playbook_misses: list[str] = field(default_factory=list)
    surprise_events: list[dict] = field(default_factory=list)

    # LLM 生成的深度复盘（可选）
    narrative: str = ""
    lessons: list[str] = field(default_factory=list)
    adjustment_suggestions: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return asdict(self)


class ReplayEngine:
    """复盘引擎：昨日剧本 vs 今日现实。"""

    def __init__(self, db: Storage, llm_client=None):
        self.db = db
        self.llm_client = llm_client
        self.script_engine = ScriptEngine(db)

    def replay(self, as_of: datetime, target_date: str = "") -> ReplayResult:
        """执行复盘。

        Args:
            as_of: 当前时间点
            target_date: 被复盘的日期（默认昨天）
        """
        if not target_date:
            target_date = (as_of - timedelta(days=1)).strftime("%Y-%m-%d")

        yesterday = (datetime.strptime(target_date, "%Y-%m-%d")
                     - timedelta(days=1)).strftime("%Y-%m-%d")

        # 1. 加载昨日剧本
        yesterday_script = self.script_engine.load_script(yesterday)

        # 2. 取今日实际数据
        from src.drift.daily_brief import DailyBrief
        brief = DailyBrief(self.db)
        thermo = brief.build_thermometer(as_of, report_date=target_date)

        # 3. 取今日题材/新闻
        concept_df = self.db.query(
            "concept_daily", as_of,
            where="trade_date = ?", params=(target_date,))
        actual_themes = self._extract_theme_names(concept_df)

        # 4. 构建初步复盘结果
        result = ReplayResult(
            date=target_date,
            yesterday_script=yesterday_script.raw_snapshot if yesterday_script else None,
            yesterday_playbook=yesterday_script.tomorrow_playbook if yesterday_script else None,
            actual_regime=thermo.regime,
            actual_zt_count=thermo.zt_count,
            actual_dt_count=thermo.dt_count,
            actual_highest_board=thermo.highest_board,
        )

        # 5. 规则对比
        self._rule_compare(result, yesterday_script, actual_themes)

        # 6. LLM 深度复盘（可选）
        if self.llm_client and yesterday_script:
            self._llm_replay(result, yesterday_script)
        else:
            self._fallback_replay(result, yesterday_script)

        return result

    def _extract_theme_names(self, concept_df: pd.DataFrame) -> list[str]:
        """提取题材名称列表。"""
        if concept_df.empty or "concept_name" not in concept_df.columns:
            return []
        return concept_df["concept_name"].tolist()

    def _rule_compare(self, result: ReplayResult,
                      yesterday_script: Optional[MarketScript],
                      actual_themes: list[str]) -> None:
        """规则对比：昨日剧本预测 vs 今日实际。"""
        # 异常事件检测（无需昨日剧本）
        if result.actual_zt_count > 100 and result.actual_dt_count < 5:
            result.surprise_events.append({
                "type": "extreme_bull",
                "detail": f"涨停{result.actual_zt_count}家，跌停仅{result.actual_dt_count}家",
            })
        if result.actual_dt_count > 50:
            result.surprise_events.append({
                "type": "panic_sell",
                "detail": f"跌停{result.actual_dt_count}家",
            })
        if result.actual_zt_count < 10 and result.actual_highest_board <= 2:
            result.surprise_events.append({
                "type": "ice_age",
                "detail": f"涨停仅{result.actual_zt_count}家，最高板{result.actual_highest_board}连板",
            })

        if not yesterday_script:
            result.regime_match = False
            return

        # regime 一致性
        playbook = yesterday_script.tomorrow_playbook or {}
        watch_list = playbook.get("watch_list", [])
        avoid_list = playbook.get("avoid_list", [])

        # 对比昨日关注的题材是否出现在今日
        for theme in watch_list:
            if any(theme in at or at in theme for at in actual_themes):
                result.playbook_hits.append(theme)
            else:
                result.playbook_misses.append(theme)

        # regime 匹配
        raw = yesterday_script.raw_snapshot or {}
        predicted_regime = raw.get("regime", "")
        result.regime_match = (predicted_regime == result.actual_regime)

    def _fallback_replay(self, result: ReplayResult,
                         yesterday_script: Optional[MarketScript]) -> None:
        """无 LLM 时的简单复盘。"""
        parts = [f"复盘{result.date}："]

        if yesterday_script:
            parts.append(
                f"昨日剧本: {yesterday_script.script_title or '无'}"
            )
            if result.regime_match:
                parts.append(f"regime 预测正确（{result.actual_regime}）")
            else:
                parts.append(f"regime 预测偏差（预期 vs 实际: {result.actual_regime}）")

            if result.playbook_hits:
                parts.append(f"命中题材: {', '.join(result.playbook_hits)}")
            if result.playbook_misses:
                parts.append(f"错过题材: {', '.join(result.playbook_misses)}")
        else:
            parts.append("无昨日剧本数据，跳过对比")

        parts.append(
            f"实际: 涨停{result.actual_zt_count}家, "
            f"跌停{result.actual_dt_count}家, "
            f"最高{result.actual_highest_board}连板"
        )

        for ev in result.surprise_events:
            parts.append(f"异常: {ev['detail']}")

        result.narrative = "\n".join(parts)

        # 简单教训
        if result.actual_dt_count > result.actual_zt_count:
            result.lessons.append("跌停多于涨停，市场极度弱势")
        if not result.regime_match:
            result.lessons.append("regime 预判有偏差，需检查 regime 判定逻辑")
        if not result.playbook_hits and result.playbook_misses:
            result.lessons.append("关注方向全部错过，题材跟踪需调整")

    def _llm_replay(self, result: ReplayResult,
                    yesterday_script: MarketScript) -> None:
        """LLM 深度复盘。"""
        if not PROMPT_PATH.exists():
            logger.warning("replay.md prompt 不存在，回退规则版")
            self._fallback_replay(result, yesterday_script)
            return

        prompt_template = PROMPT_PATH.read_text(encoding="utf-8")
        prompt = prompt_template.replace("{yesterday_script_json}",
                                         json.dumps(yesterday_script.to_dict(), ensure_ascii=False))
        prompt = prompt.replace("{actual_data_json}",
                                json.dumps(result.to_dict(), ensure_ascii=False))

        try:
            response = self.llm_client.messages.create(
                model="glm-4-plus",
                max_tokens=1000,
                temperature=0.3,
                messages=[{"role": "user", "content": prompt}],
            )
            text = response.content[0].text

            if "```json" in text:
                text = text.split("```json")[1].split("```")[0]
            elif "```" in text:
                text = text.split("```")[1].split("```")[0]

            data = json.loads(text.strip())
            result.narrative = data.get("narrative", "")
            result.lessons = data.get("lessons", [])
            result.adjustment_suggestions = data.get("adjustment_suggestions", [])
        except Exception as e:
            logger.error("LLM 复盘失败: %s", e)
            self._fallback_replay(result, yesterday_script)

    def save_replay(self, result: ReplayResult) -> None:
        """将复盘结果存入 replay_log 表。"""
        conn = self.db._get_conn()
        try:
            conn.execute(
                """CREATE TABLE IF NOT EXISTS replay_log (
                    id               INTEGER PRIMARY KEY AUTOINCREMENT,
                    trade_date       TEXT UNIQUE NOT NULL,
                    regime_match     INTEGER DEFAULT 0,
                    playbook_hits    TEXT DEFAULT '[]',
                    playbook_misses  TEXT DEFAULT '[]',
                    surprise_events  TEXT DEFAULT '[]',
                    narrative        TEXT DEFAULT '',
                    lessons          TEXT DEFAULT '[]',
                    adjustment_suggestions TEXT DEFAULT '[]',
                    snapshot_time    TEXT DEFAULT (datetime('now'))
                )"""
            )
            conn.execute(
                """INSERT OR REPLACE INTO replay_log
                   (trade_date, regime_match, playbook_hits, playbook_misses,
                    surprise_events, narrative, lessons, adjustment_suggestions)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    result.date,
                    1 if result.regime_match else 0,
                    json.dumps(result.playbook_hits, ensure_ascii=False),
                    json.dumps(result.playbook_misses, ensure_ascii=False),
                    json.dumps(result.surprise_events, ensure_ascii=False),
                    result.narrative,
                    json.dumps(result.lessons, ensure_ascii=False),
                    json.dumps(result.adjustment_suggestions, ensure_ascii=False),
                ),
            )
            conn.commit()
        finally:
            conn.close()

    def load_replay(self, trade_date: str) -> Optional[ReplayResult]:
        """从 replay_log 读取复盘结果。"""
        conn = self.db._get_conn()
        try:
            # 确保表存在
            conn.execute(
                """CREATE TABLE IF NOT EXISTS replay_log (
                    id               INTEGER PRIMARY KEY AUTOINCREMENT,
                    trade_date       TEXT UNIQUE NOT NULL,
                    regime_match     INTEGER DEFAULT 0,
                    playbook_hits    TEXT DEFAULT '[]',
                    playbook_misses  TEXT DEFAULT '[]',
                    surprise_events  TEXT DEFAULT '[]',
                    narrative        TEXT DEFAULT '',
                    lessons          TEXT DEFAULT '[]',
                    adjustment_suggestions TEXT DEFAULT '[]',
                    snapshot_time    TEXT DEFAULT (datetime('now'))
                )"""
            )
            row = conn.execute(
                "SELECT * FROM replay_log WHERE trade_date = ? "
                "ORDER BY snapshot_time DESC LIMIT 1",
                (trade_date,),
            ).fetchone()
            if not row:
                return None
            return ReplayResult(
                date=row["trade_date"],
                regime_match=bool(row["regime_match"]),
                playbook_hits=json.loads(row["playbook_hits"] or "[]"),
                playbook_misses=json.loads(row["playbook_misses"] or "[]"),
                surprise_events=json.loads(row["surprise_events"] or "[]"),
                narrative=row["narrative"] or "",
                lessons=json.loads(row["lessons"] or "[]"),
                adjustment_suggestions=json.loads(row["adjustment_suggestions"] or "[]"),
            )
        finally:
            conn.close()

    def get_accuracy_stats(self, last_n: int = 20) -> dict:
        """统计最近 N 次复盘的准确率。"""
        conn = self.db._get_conn()
        try:
            # 确保表存在
            conn.execute(
                """CREATE TABLE IF NOT EXISTS replay_log (
                    id               INTEGER PRIMARY KEY AUTOINCREMENT,
                    trade_date       TEXT UNIQUE NOT NULL,
                    regime_match     INTEGER DEFAULT 0,
                    playbook_hits    TEXT DEFAULT '[]',
                    playbook_misses  TEXT DEFAULT '[]',
                    surprise_events  TEXT DEFAULT '[]',
                    narrative        TEXT DEFAULT '',
                    lessons          TEXT DEFAULT '[]',
                    adjustment_suggestions TEXT DEFAULT '[]',
                    snapshot_time    TEXT DEFAULT (datetime('now'))
                )"""
            )
            rows = conn.execute(
                "SELECT * FROM replay_log ORDER BY trade_date DESC LIMIT ?",
                (last_n,),
            ).fetchall()
            if not rows:
                return {"total": 0, "regime_accuracy": 0.0, "avg_hits": 0.0, "hit_rate": 0.0}

            regime_correct = sum(1 for r in rows if r["regime_match"])
            total_hits = sum(
                len(json.loads(r["playbook_hits"] or "[]")) for r in rows
            )
            total_plays = total_hits + sum(
                len(json.loads(r["playbook_misses"] or "[]")) for r in rows
            )

            return {
                "total": len(rows),
                "regime_accuracy": round(regime_correct / len(rows), 2),
                "avg_hits": round(total_hits / len(rows), 1),
                "hit_rate": round(total_hits / total_plays, 2) if total_plays > 0 else 0.0,
            }
        finally:
            conn.close()
