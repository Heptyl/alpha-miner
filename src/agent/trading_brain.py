"""trading_brain.py — 交易员大脑 (多维评分架构)

TradingBrain: 买入前多维评分 + 持仓思考 + 收盘复盘

评分维度(参考 TradingAgents ICML2025 / A_Share多维评分):
  市场环境(25分): 涨跌比 + 权重翻红 + 反转信号
  个股质量(35分): 精选评分卡 + 交易记忆 + 历史同票
  策略匹配(20分): 信号强度 + 策略历史PF
  LLM辅助(20分): 新闻情感 + LLM综合判断

决策阈值: >=65买, <50拦截, 50-65观察(需LLM辅助)
"""
from __future__ import annotations

import json
import logging
import re
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Optional

logger = logging.getLogger("trading_daemon")

DB_PATH = Path(__file__).resolve().parents[2] / "data" / "alpha_miner.db"

# 买入阈值
BUY_THRESHOLD = 65
PASS_THRESHOLD = 50


class TradingBrain:
    """交易员大脑 — 多维评分驱动"""

    def __init__(self):
        self._last_hold_check: dict[str, str] = {}
        self._buy_block_cache: dict[str, str] = {}  # code → ISO[:16]

    def think_before_buy(
        self,
        code: str,
        name: str = "",
        signal_type: str = "",
        signal_reason: str = "",
        strategy: str = "",
        market_phase: str = "正常",
        ratio_now: float = -1,
        candidate: dict | None = None,
    ) -> dict:
        """买入前多维评分

        Returns:
            {
                "decision": "buy" | "pass",
                "score": 0-100,
                "breakdown": {"市场": int, "个股": int, "策略": int, "LLM": int},
                "reason": str,
                "confidence": 0-1,
            }
        """
        breakdown = {"市场": 0, "个股": 0, "策略": 0, "LLM": 0}

        # 0. 拦截缓存: 5分钟内拦过的票直接跳过
        now_min = datetime.now().isoformat()[:16]
        if self._buy_block_cache.get(code) == now_min:
            return {
                "decision": "pass", "score": 0,
                "breakdown": breakdown, "confidence": 0.95,
                "reason": "5分钟内已被大脑拦截, 跳过",
            }

        candidate = candidate or {}

        # ── 数据收集 ──

        # 个股感知
        perception = {}
        try:
            from src.agent.market_perception import perceive_stock
            perception = perceive_stock(code)
        except Exception as e:
            logger.debug(f"[大脑] perceive_stock({code})失败: {e}")

        # 交易记忆
        memory = {}
        try:
            from src.trader.trade_memory import query_similar_trades
            industry = perception.get("sector", "") or candidate.get("industry", "")
            memory = query_similar_trades(strategy, industry=industry)
        except Exception as e:
            logger.debug(f"[大脑] query_similar_trades失败: {e}")

        mem_total = memory.get("total", 0)
        mem_wr = memory.get("win_rate", 0)

        # ── 硬规则(不评分, 直接拦截) ──

        # 记忆胜率<35%且>=5条 → 直接pass
        if mem_total >= 5 and mem_wr < 0.35:
            reason = f"记忆硬拦: 策略{strategy}胜率{mem_wr:.0%}({mem_total}笔)<35%"
            self._buy_block_cache[code] = now_min
            return {
                "decision": "pass", "score": 0,
                "breakdown": breakdown, "confidence": 0.9,
                "reason": reason,
            }

        # 冰点/退潮 + 非C策略 → 直接pass
        if market_phase in ("冰点", "退潮") and strategy != "C":
            reason = f"情绪硬拦: {market_phase}期不开策略{strategy}新仓"
            self._buy_block_cache[code] = now_min
            return {
                "decision": "pass", "score": 0,
                "breakdown": breakdown, "confidence": 0.85,
                "reason": reason,
            }

        # ── 维度1: 市场环境(0-25分) ──
        breakdown["市场"] = self._score_market(market_phase, ratio_now)

        # ── 维度2: 个股质量(0-35分) ──
        breakdown["个股"] = self._score_stock(
            code, strategy, candidate, memory, mem_total, mem_wr,
        )

        # ── 维度3: 策略匹配(0-20分) ──
        breakdown["策略"] = self._score_strategy(
            strategy, signal_type, signal_reason,
        )

        # 纯数据分(不含LLM)
        data_score = breakdown["市场"] + breakdown["个股"] + breakdown["策略"]

        # ── 决策分支: 数据分是否已足够 ──

        if data_score >= BUY_THRESHOLD:
            # 数据分已够, 不需要LLM, 直接放行
            total = data_score + 0  # LLM不加分为0
            reason = self._build_reason(breakdown, data_score, total, "数据充分放行")
            return {
                "decision": "buy", "score": total,
                "breakdown": breakdown, "confidence": min(total / 100, 0.95),
                "reason": reason,
            }

        if data_score < PASS_THRESHOLD:
            # 数据分太低, LLM也救不了(最多加20分)
            total = data_score
            reason = self._build_reason(breakdown, data_score, total, "数据不足拦截")
            self._buy_block_cache[code] = now_min
            return {
                "decision": "pass", "score": total,
                "breakdown": breakdown, "confidence": max(1 - total / 100, 0.5),
                "reason": reason,
            }

        # 50 <= data_score < 65: 观察区间, 需要LLM辅助
        llm_score = self._score_llm(
            code, name, strategy, market_phase, ratio_now,
            signal_type, signal_reason, perception, memory, candidate,
        )
        breakdown["LLM"] = llm_score
        total = data_score + llm_score

        if total >= BUY_THRESHOLD:
            decision = "buy"
            tag = "LLM辅助放行"
        else:
            decision = "pass"
            tag = "观察区间拦截"
            self._buy_block_cache[code] = now_min

        reason = self._build_reason(breakdown, data_score, total, tag)
        return {
            "decision": decision, "score": total,
            "breakdown": breakdown,
            "confidence": min(total / 100, 0.95) if decision == "buy" else max(1 - total / 100, 0.5),
            "reason": reason,
        }

    # ── 评分子函数 ──

    def _score_market(self, market_phase: str, ratio_now: float) -> int:
        """市场环境分(0-25)

        涨跌比: 正常(40-60%)=15分, 冰点(<30%)=0分
        权重翻红: >=60%=5分, <30%=0分
        反转信号: 触发=5分
        """
        score = 0

        # 涨跌比(0-15分)
        if ratio_now >= 60:
            score += 15
        elif ratio_now >= 50:
            score += 12
        elif ratio_now >= 40:
            score += 8
        elif ratio_now >= 30:
            score += 4
        # <30或-1: 0分

        # 市场情绪phase加分(0-5分)
        phase_bonus = {"正常": 5, "贪婪": 5, "退潮": 2, "冰点": 0, "谨慎": 3}
        score += phase_bonus.get(market_phase, 3)

        # 反转信号(0-5分) — 查缓存
        try:
            from src.agent.market_perception import _session_cache
            if _session_cache.get("reversal_triggered"):
                score += 5
        except Exception:
            pass

        return min(score, 25)

    def _score_stock(
        self, code: str, strategy: str, candidate: dict,
        memory: dict, mem_total: int, mem_wr: float,
    ) -> int:
        """个股质量分(0-35)

        精选评分卡: 原始分*0.35
        交易记忆: 胜率>=60%=10分, <40%=0分
        历史同票: 上次赚=5分, 亏=-5分
        """
        score = 0

        # 精选评分卡(0-20分): 候选的score字段, 通常50-100, 映射到0-20
        cand_score = candidate.get("score", 0)
        if isinstance(cand_score, (int, float)) and cand_score > 0:
            # score 80+ → 20分, 60 → 12分, 40 → 4分
            mapped = (cand_score - 30) * 0.40  # 30分以下=0分
            score += max(0, min(int(mapped), 20))

        # 交易记忆胜率(0-10分)
        if mem_total >= 5:
            if mem_wr >= 0.6:
                score += 10
            elif mem_wr >= 0.5:
                score += 7
            elif mem_wr >= 0.4:
                score += 4
            # <0.4: 0分(已有硬规则拦<35%)
        elif mem_total >= 3:
            score += 5  # 数据少但不是零, 给基础分
        else:
            score += 3  # 无记忆数据, 不惩罚(还没积累)

        # 历史同票盈亏(-5~+5分)
        try:
            conn = sqlite3.connect(str(DB_PATH), timeout=10)
            row = conn.execute(
                "SELECT pnl_pct FROM daemon_trades WHERE code=? AND status='closed' "
                "ORDER BY sell_time DESC LIMIT 1",
                (code,),
            ).fetchone()
            conn.close()
            if row:
                pnl = row[0]
                if pnl > 0:
                    score += 5
                elif pnl < 0:
                    score -= 3  # 不扣满, 给二次机会
        except Exception:
            pass

        return max(0, min(score, 35))

    def _score_strategy(
        self, strategy: str, signal_type: str, signal_reason: str,
    ) -> int:
        """策略匹配分(0-20)

        信号强度: 有明确signal_type=10分, 无=0分
        策略历史PF: PF>1.5=10分, <1.0=0分
        """
        score = 0

        # 信号强度(0-10分)
        strong_signals = ["首阴反包", "暴跌日狙击", "基本面驱动", "涨停确认", "趋势牛股"]
        if any(s in signal_type for s in strong_signals):
            score += 10
        elif signal_type:
            score += 5

        # 策略历史盈亏比(0-10分)
        try:
            conn = sqlite3.connect(str(DB_PATH), timeout=10)
            row = conn.execute(
                "SELECT AVG(CASE WHEN pnl_pct > 0 THEN pnl_pct ELSE 0 END) as avg_win, "
                "       AVG(CASE WHEN pnl_pct < 0 THEN ABS(pnl_pct) ELSE 0 END) as avg_loss, "
                "       COUNT(*) as cnt "
                "FROM daemon_trades WHERE strategy=? AND status='closed'",
                (strategy,),
            ).fetchone()
            conn.close()
            if row and row[2] >= 3:
                avg_win = row[0] or 0
                avg_loss = row[1] or 0.01
                pf = avg_win / avg_loss if avg_loss > 0 else 99
                if pf >= 1.5:
                    score += 10
                elif pf >= 1.0:
                    score += 7
                elif pf >= 0.5:
                    score += 3
        except Exception:
            pass

        return min(score, 20)

    def _score_llm(
        self, code: str, name: str, strategy: str,
        market_phase: str, ratio_now: float,
        signal_type: str, signal_reason: str,
        perception: dict, memory: dict, candidate: dict,
    ) -> int:
        """LLM辅助分(0-20分)

        新闻情感: 正面=10分, 负面=0分, 中性=5分
        LLM综合判断: 支持=10分, 反对=0分
        LLM失败不扣分, 只是不加分
        """
        score = 0

        # 新闻情感(0-10分)
        sentiment_score = self._get_sentiment(code)
        if sentiment_score >= 0.3:
            score += 10
        elif sentiment_score >= -0.1:
            score += 5
        # 负面(<-0.3): 0分

        # LLM综合判断(0-10分)
        llm_result = self._llm_judge_buy(
            code=code, name=name, signal_type=signal_type,
            signal_reason=signal_reason, strategy=strategy,
            market_phase=market_phase, ratio_now=ratio_now,
            perception=perception, memory=memory, candidate=candidate,
        )
        if llm_result and llm_result.get("decision") == "buy":
            score += 10
        # LLM说pass或失败: 不加分, 也不扣分

        return min(score, 20)

    def _get_sentiment(self, code: str) -> float:
        """从news表获取最近30天加权情感分(-1~1)"""
        try:
            conn = sqlite3.connect(str(DB_PATH), timeout=10)
            row = conn.execute(
                "SELECT AVG(sentiment) FROM news "
                "WHERE stock_code=? AND trade_date >= date('now','-30 days') "
                "AND sentiment IS NOT NULL",
                (code,),
            ).fetchone()
            conn.close()
            if row and row[0] is not None:
                return float(row[0])
        except Exception:
            pass
        return 0.0  # 无新闻=中性

    def _build_reason(self, breakdown: dict, data_score: int,
                      total: int, tag: str) -> str:
        parts = []
        b = breakdown
        if b["市场"] > 0:
            parts.append(f"市场{b['市场']}")
        if b["个股"] > 0:
            parts.append(f"个股{b['个股']}")
        if b["策略"] > 0:
            parts.append(f"策略{b['策略']}")
        if b["LLM"] > 0:
            parts.append(f"LLM{b['LLM']}")
        detail = "+".join(parts) if parts else f"数据{data_score}"
        return f"[{tag}] 总{total}分({detail})"

    # ── 持仓/收盘思考 ──

    def think_during_hold(
        self, code: str, name: str, position_data: dict, market_data: dict,
    ) -> dict:
        """持仓中思考"""
        now = datetime.now().isoformat()[:16]
        if self._last_hold_check.get(code) == now:
            return {"action": "hold", "reason": "5分钟内已检查", "urgency": "low"}
        self._last_hold_check[code] = now

        buy_price = position_data.get("buy_price", 0)
        current_price = position_data.get("current_price", 0)
        if buy_price <= 0 or current_price <= 0:
            return {"action": "hold", "reason": "", "urgency": "low"}

        pnl_pct = (current_price / buy_price - 1) * 100
        phase = market_data.get("phase", "未知")

        if pnl_pct < -8 and phase in ("冰点", "退潮"):
            return {"action": "exit", "reason": f"浮亏{pnl_pct:+.1f}%+{phase}", "urgency": "high"}

        return {"action": "hold", "reason": "", "urgency": "low"}

    def think_after_close(self, today_trades: list, market_summary: dict) -> dict:
        """收盘后思考"""
        return {"review": "收盘复盘待实现", "tomorrow_plan": "", "lessons": ""}

    # ── LLM调用(仅观察区间使用) ──

    def _llm_judge_buy(
        self, code: str, name: str, signal_type: str, signal_reason: str,
        strategy: str, market_phase: str, ratio_now: float,
        perception: dict, memory: dict, candidate: dict,
    ) -> Optional[dict]:
        """LLM买入判断 — 仅在观察区间调用"""
        try:
            from src.agent.llm_client import get_client
            client = get_client()
        except Exception:
            return None

        mem_total = memory.get("total", 0)
        mem_wr = memory.get("win_rate", 0)
        mem_avg = memory.get("avg_pnl", 0)
        mem_str = f"{mem_total}笔,胜率{mem_wr:.0%},均收{mem_avg:+.1f}%" if mem_total > 0 else "无历史记忆"

        perc_sector = perception.get("sector", "未知")
        perc_logic = perception.get("logic", "无")
        perc_conf = perception.get("logic_confidence", 0)

        prompt = (
            f"你是A股金牌短线交易员。基于以下信息判断是否应该买入 {code} {name}。\n\n"
            f"【盘面】情绪={market_phase}, 涨跌比={ratio_now}%\n"
            f"【信号】策略{strategy}, 类型={signal_type}, 原因={signal_reason}\n"
            f"【个股】行业={perc_sector}, 涨因={perc_logic}(置信{perc_conf:.1f})\n"
            f"【记忆】策略{strategy}历史: {mem_str}\n"
            f"【候选数据】{', '.join(f'{k}={v}' for k, v in candidate.items() if not k.startswith('_') and v)}\n\n"
            f"严格输出JSON(不要其他文字):\n"
            f'{{"decision": "buy"或"pass", "confidence": 0到1的数字, '
            f'"reason": "一句话理由"}}'
        )

        try:
            resp = client.chat(prompt, max_tokens=150, temperature=0.2, caller="trading_brain")
            if not resp:
                return None
            m = re.search(r'\{[^}]+\}', resp)
            if not m:
                return None
            parsed = json.loads(m.group())
            decision = parsed.get("decision", "pass")
            if decision not in ("buy", "pass"):
                decision = "pass"
            confidence = max(0, min(1, float(parsed.get("confidence", 0.5))))
            reason = str(parsed.get("reason", ""))[:80]
            return {"decision": decision, "confidence": confidence, "reason": f"LLM: {reason}"}
        except Exception as e:
            logger.debug(f"[大脑] LLM判断失败: {e}")
            return None


# 单例
_brain: TradingBrain | None = None


def get_brain() -> TradingBrain:
    global _brain
    if _brain is None:
        _brain = TradingBrain()
    return _brain
