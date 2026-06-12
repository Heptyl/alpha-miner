"""debate_agent.py — 辩论式信号融合(Bull/Bear/Judge)

对每只候选股, LLM分三步评估:
  1. Bull(看多): 找利好理由
  2. Bear(看空): 找利空理由
  3. Judge(裁判): 综合双方观点, 给出confidence(0-100)

confidence<50的候选直接过滤。
结果写入debate_results表, 供selection_score使用。

开关: DEBATE_ENABLED in daemon_config.py (默认False)
用法:
  from src.agent.debate_agent import debate_candidate
  result = debate_candidate(candidate)
"""

from __future__ import annotations

import json
import logging
import sqlite3
from datetime import date
from pathlib import Path
from typing import Optional

logger = logging.getLogger("debate_agent")

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DB_PATH = PROJECT_ROOT / "data" / "alpha_miner.db"

# ── Prompt模板 ──

BULL_PROMPT = """你是Bull分析师(看多方), 任务是为买入{name}({code})找利好的理由。

当前数据:
  策略: {strategy}
  实时价格: {price}元
  涨跌幅: {chg_pct:+.1f}%
  {extra_data}

请从以下角度找利好(每点一句话):
  1. 基本面(业绩/估值/行业地位)
  2. 资金面(主力流入/北向/龙虎榜)
  3. 技术面(趋势/支撑/量价)
  4. 行业/题材催化

用JSON格式回复:
{{"bull_points": ["点1", "点2", "点3", "点4"], "bull_confidence": 75}}"""

BEAR_PROMPT = """你是Bear分析师(看空方), 任务是指出买入{name}({code})的风险。

Bull的观点:
{bull_points}

当前数据:
  策略: {strategy}
  实时价格: {price}元
  涨跌幅: {chg_pct:+.1f}%
  {extra_data}

请从以下角度找利空(每点一句话):
  1. 基本面风险(估值过高/业绩下滑/商誉)
  2. 资金面风险(主力流出/解禁/减持)
  3. 技术面风险(阻力位/超买/破位)
  4. 宏观/行业风险(政策/竞争/周期)

用JSON格式回复:
{{"bear_points": ["点1", "点2", "点3", "点4"], "bear_confidence": 40}}"""

JUDGE_PROMPT = """你是投资决策Judge(裁判), 综合Bull和Bear的观点给出最终判断。

候选: {name}({code}) | 策略: {strategy} | 价格: {price}元

Bull(看多)观点:
{bull_text}

Bear(看空)观点:
{bear_text}

请综合评估:
  1. 哪方观点更有说服力? 为什么?
  2. 关键风险是什么?
  3. 最终判断

用JSON格式回复:
{{
  "verdict": "bull/bear/neutral",
  "confidence": 65,
  "reasoning": "一句话综合判断",
  "key_risk": "最大风险点",
  "key_catalyst": "最大催化剂"
}}"""


def _gather_context(candidate: dict) -> str:
    """从候选数据中提取上下文信息"""
    parts = []
    if candidate.get("_tier"):
        parts.append(f"档位: {candidate['_tier']}")
    if candidate.get("_lb"):
        parts.append(f"连板数: {candidate['_lb']}")
    if candidate.get("_vol_ratio"):
        parts.append(f"量比: {candidate['_vol_ratio']:.1f}")
    if candidate.get("_rsi"):
        parts.append(f"RSI: {candidate['_rsi']:.0f}")
    if candidate.get("_score"):
        parts.append(f"精选评分: {candidate['_score']}")
    if candidate.get("_score_details"):
        details = candidate["_score_details"]
        if isinstance(details, dict):
            for k, v in details.items():
                if v and v != 0:
                    parts.append(f"{k}: {v}")
    return "\n  ".join(parts) if parts else "无额外数据"


def _parse_json(text: str) -> Optional[dict]:
    """从LLM回复中解析JSON"""
    if not text:
        return None
    # 尝试直接解析
    text = text.strip()
    if "```json" in text:
        text = text.split("```json")[1].split("```")[0].strip()
    elif "```" in text:
        text = text.split("```")[1].split("```")[0].strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        # 尝试找第一个 { 到最后一个 }
        start = text.find("{")
        end = text.rfind("}")
        if start >= 0 and end > start:
            try:
                return json.loads(text[start:end + 1])
            except json.JSONDecodeError:
                pass
    return None


def debate_candidate(candidate: dict, strategy: str = "B") -> Optional[dict]:
    """对单只候选股执行Bull/Bear/Judge辩论

    Args:
        candidate: 候选数据(需含code, name, realtime_price, realtime_chg等)
        strategy: 策略A/B/C

    Returns:
        {
            "code": str,
            "name": str,
            "strategy": str,
            "bull_points": list[str],
            "bear_points": list[str],
            "verdict": "bull"/"bear"/"neutral",
            "confidence": int (0-100),
            "reasoning": str,
            "key_risk": str,
            "key_catalyst": str,
        }
        失败返回None
    """
    from src.agent.llm_client import get_client

    client = get_client()
    if not client.has_provider:
        return None

    code = candidate.get("code", "")
    name = candidate.get("name", "")
    price = candidate.get("realtime_price", 0)
    chg_pct = candidate.get("realtime_chg", 0)
    extra = _gather_context(candidate)

    # Step 1: Bull
    bull_prompt = BULL_PROMPT.format(
        name=name, code=code, strategy=strategy,
        price=price, chg_pct=chg_pct, extra_data=extra,
    )
    bull_text = client.chat(bull_prompt, caller="debate_bull", max_tokens=500)
    bull_data = _parse_json(bull_text) or {}
    bull_points = bull_data.get("bull_points", [])
    if not bull_points:
        bull_points = ["数据不足, 无法判断"]

    # Step 2: Bear
    bear_prompt = BEAR_PROMPT.format(
        name=name, code=code, strategy=strategy,
        price=price, chg_pct=chg_pct, extra_data=extra,
        bull_points="\n".join(f"- {p}" for p in bull_points),
    )
    bear_text = client.chat(bear_prompt, caller="debate_bear", max_tokens=500)
    bear_data = _parse_json(bear_text) or {}
    bear_points = bear_data.get("bear_points", [])
    if not bear_points:
        bear_points = ["数据不足, 无法判断"]

    # Step 3: Judge
    judge_prompt = JUDGE_PROMPT.format(
        name=name, code=code, strategy=strategy, price=price,
        bull_text="\n".join(f"- {p}" for p in bull_points),
        bear_text="\n".join(f"- {p}" for p in bear_points),
    )
    judge_text = client.chat(judge_prompt, caller="debate_judge", max_tokens=400)
    judge_data = _parse_json(judge_text) or {}

    result = {
        "code": code,
        "name": name,
        "strategy": strategy,
        "bull_points": bull_points[:4],
        "bear_points": bear_points[:4],
        "verdict": judge_data.get("verdict", "neutral"),
        "confidence": int(judge_data.get("confidence", 50)),
        "reasoning": judge_data.get("reasoning", ""),
        "key_risk": judge_data.get("key_risk", ""),
        "key_catalyst": judge_data.get("key_catalyst", ""),
    }

    # 写入DB
    _save_debate(result)

    logger.info("[辩论] %s(%s) verdict=%s confidence=%d",
                name, code, result["verdict"], result["confidence"])
    return result


def debate_batch(candidates: list[dict], strategy: str = "B") -> list[dict]:
    """批量辩论, 过滤confidence<50的候选

    Returns:
        通过辩论的候选列表(附_debate字段)
    """
    passed = []
    for c in candidates:
        try:
            result = debate_candidate(c, strategy)
            if result and result["confidence"] >= 50:
                c["_debate"] = result
                passed.append(c)
            elif result:
                logger.info("[辩论] %s confidence=%d<50, 过滤",
                            c.get("name", ""), result["confidence"])
        except Exception as e:
            logger.debug("[辩论] %s失败: %s", c.get("code", ""), str(e)[:80])
            # 辩论失败不阻塞, 原样放行
            passed.append(c)
    return passed


def get_debate_result(code: str, db_path: str = None) -> Optional[dict]:
    """从DB获取最近一次辩论结果"""
    path = db_path or str(DB_PATH)
    try:
        conn = sqlite3.connect(path)
        r = conn.execute("""
            SELECT verdict, confidence, reasoning, key_risk, key_catalyst,
                   bull_points, bear_points
            FROM debate_results
            WHERE stock_code = ?
            ORDER BY created_at DESC LIMIT 1
        """, (code,)).fetchone()
        conn.close()
        if r:
            return {
                "verdict": r[0], "confidence": r[1],
                "reasoning": r[2], "key_risk": r[3], "key_catalyst": r[4],
                "bull_points": json.loads(r[5]) if r[5] else [],
                "bear_points": json.loads(r[6]) if r[6] else [],
            }
    except Exception:
        pass
    return None


def _save_debate(result: dict):
    """写入debate_results表"""
    try:
        conn = sqlite3.connect(str(DB_PATH))
        conn.execute("""
            CREATE TABLE IF NOT EXISTS debate_results (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                stock_code   TEXT NOT NULL,
                stock_name   TEXT DEFAULT '',
                strategy     TEXT DEFAULT '',
                verdict      TEXT DEFAULT '',
                confidence   INTEGER DEFAULT 50,
                reasoning    TEXT DEFAULT '',
                key_risk     TEXT DEFAULT '',
                key_catalyst TEXT DEFAULT '',
                bull_points  TEXT DEFAULT '[]',
                bear_points  TEXT DEFAULT '[]',
                created_at   TEXT DEFAULT (datetime('now'))
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_debate_code ON debate_results(stock_code)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_debate_date ON debate_results(created_at)")
        conn.execute("""
            INSERT INTO debate_results
            (stock_code, stock_name, strategy, verdict, confidence,
             reasoning, key_risk, key_catalyst, bull_points, bear_points)
            VALUES (?,?,?,?,?,?,?,?,?,?)
        """, (
            result["code"], result["name"], result["strategy"],
            result["verdict"], result["confidence"],
            result["reasoning"], result["key_risk"], result["key_catalyst"],
            json.dumps(result["bull_points"], ensure_ascii=False),
            json.dumps(result["bear_points"], ensure_ascii=False),
        ))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.debug("[辩论] 写入DB失败: %s", e)
