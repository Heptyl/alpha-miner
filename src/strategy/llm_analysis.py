"""LLM深度推理模块 — 用大模型对候选股做多轮分析。

分析维度：
1. 基本面解读（财报、估值、行业地位）
2. 技术面研判（趋势、量价、支撑压力）
3. 资金面分析（主力意图、龙虎榜、资金流向）
4. 综合风险评估
5. 最终推荐/不推荐 + 理由
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

DB_PATH = "data/alpha_miner.db"


@dataclass
class LLMAnalysis:
    """LLM分析结果。"""

    stock_code: str
    stock_name: str
    recommendation: str       # "strong_buy" / "buy" / "hold" / "sell" / "avoid"
    confidence: float         # 0~1
    score_adjustment: float   # 综合分调整 (-0.3 ~ +0.1)
    bull_points: list[str]    # 看多理由
    bear_points: list[str]    # 看空理由
    key_risk: str             # 最大风险
    summary: str              # 一句话总结
    raw_response: str         # LLM原始回复

    def to_dict(self) -> dict:
        return {
            "stock_code": self.stock_code,
            "recommendation": self.recommendation,
            "confidence": round(self.confidence, 2),
            "score_adjustment": round(self.score_adjustment, 2),
            "bull_points": self.bull_points,
            "bear_points": self.bear_points,
            "key_risk": self.key_risk,
            "summary": self.summary,
        }


def _gather_context(
    stock_code: str,
    trade_date: str,
    db_path: str = DB_PATH,
) -> dict:
    """收集候选股的上下文信息。"""
    conn = sqlite3.connect(db_path)
    ctx = {"stock_code": stock_code, "trade_date": trade_date}

    # 1. 基本信息（从 zt_pool / strong_pool）
    row = conn.execute(
        "SELECT name, industry, consecutive_zt, open_count, amount FROM zt_pool "
        "WHERE stock_code=? AND trade_date=?",
        (stock_code, trade_date),
    ).fetchone()
    if row:
        ctx["name"] = row[0]
        ctx["industry"] = row[1]
        ctx["consecutive_zt"] = row[2]
        ctx["open_count"] = row[3]
        ctx["amount"] = row[4]
    else:
        row = conn.execute(
            "SELECT name, industry, amount FROM strong_pool "
            "WHERE stock_code=? AND trade_date=?",
            (stock_code, trade_date),
        ).fetchone()
        if row:
            ctx["name"] = row[0]
            ctx["industry"] = row[1]
            ctx["amount"] = row[2]
        else:
            ctx["name"] = stock_code
            ctx["industry"] = "未知"

    # 2. 最近5天K线
    rows = conn.execute(
        """SELECT trade_date, open, close, high, low, pct_change
           FROM daily_price
           WHERE stock_code=? AND trade_date<=?
           ORDER BY trade_date DESC LIMIT 5""",
        (stock_code, trade_date),
    ).fetchall()
    if rows:
        klines = []
        for r in reversed(rows):
            klines.append(f"  {r[0]}: 开{r[1]:.2f} 高{r[3]:.2f} 低{r[4]:.2f} 收{r[2]:.2f} 涨跌{r[5]:.2f}%")
        ctx["recent_klines"] = "\n".join(klines)

        # 计算短期涨幅
        first_close = rows[-1][2]
        last_close = rows[0][2]
        ctx["n_day_change"] = (last_close / first_close - 1) * 100 if first_close > 0 else 0

    # 3. 龙虎榜
    row = conn.execute(
        "SELECT buy_amount, sell_amount, net_amount, reason FROM lhb_detail "
        "WHERE stock_code=? AND trade_date=?",
        (stock_code, trade_date),
    ).fetchone()
    if row:
        ctx["lhb_buy"] = row[0]
        ctx["lhb_sell"] = row[1]
        ctx["lhb_net"] = row[2]
        ctx["lhb_reason"] = row[3]

    # 4. 基本面
    row = conn.execute(
        """SELECT pe_ttm, pb, roe, profit_yoy, is_st, total_mv
           FROM stock_fundamentals
           WHERE stock_code=?
           ORDER BY trade_date DESC LIMIT 1""",
        (stock_code,),
    ).fetchone()
    if row:
        ctx["pe"] = row[0]
        ctx["pb"] = row[1]
        ctx["roe"] = row[2]
        ctx["profit_yoy"] = row[3]
        ctx["is_st"] = row[4]
        ctx["total_mv"] = row[5]

    # 5. 市场环境
    zt_cnt = conn.execute(
        "SELECT COUNT(*) FROM zt_pool WHERE trade_date=?", (trade_date,)
    ).fetchone()[0]
    ctx["market_zt_count"] = zt_cnt

    conn.close()
    return ctx


def _build_prompt(ctx: dict) -> str:
    """构建LLM分析prompt。"""
    name = ctx.get("name", ctx["stock_code"])
    industry = ctx.get("industry", "未知")
    czt = ctx.get("consecutive_zt", 0)
    klines = ctx.get("recent_klines", "无数据")
    change = ctx.get("n_day_change", 0)

    prompt = f"""你是一个专业的A股短线交易分析师。请对以下股票进行深度分析。

## 股票信息
- 代码: {ctx['stock_code']}
- 名称: {name}
- 板块: {industry}
- 连板数: {czt}
- 近5日涨幅: {change:.1f}%
- 市场涨停数: {ctx.get('market_zt_count', '未知')}只"""

    if ctx.get("pe") is not None:
        prompt += f"\n- PE(TTM): {ctx['pe']}"
    if ctx.get("pb") is not None:
        prompt += f"\n- PB: {ctx['pb']}"
    if ctx.get("roe") is not None:
        prompt += f"\n- ROE: {ctx['roe']}%"
    if ctx.get("profit_yoy") is not None:
        prompt += f"\n- 净利润同比: {ctx['profit_yoy']}%"
    if ctx.get("total_mv") is not None:
        prompt += f"\n- 总市值: {ctx['total_mv']}亿"
    if ctx.get("is_st"):
        prompt += "\n- ⚠️ ST股"

    prompt += f"""

## 最近K线
{klines}"""

    if ctx.get("lhb_net") is not None:
        prompt += f"\n\n## 龙虎榜\n净买入: {ctx['lhb_net']/1e8:.2f}亿"

    prompt += """

## 分析要求
请从以下维度分析，并给出结论：

1. **基本面**：估值是否合理？业绩是否支撑？
2. **技术面**：趋势如何？量价配合？关键位在哪？
3. **资金面**：主力意图？散户情绪？
4. **追高风险**：短期涨幅是否过大？接盘风险？
5. **综合判断**：明天是否值得买入？

## 输出格式（严格JSON）
```json
{
  "recommendation": "strong_buy/buy/hold/sell/avoid",
  "confidence": 0.0-1.0,
  "bull_points": ["看多理由1", "看多理由2"],
  "bear_points": ["看空理由1", "看空理由2"],
  "key_risk": "最大风险一句话",
  "summary": "一句话总结"
}
```"""

    return prompt


def analyze_with_llm(
    stock_code: str,
    trade_date: str,
    db_path: str = DB_PATH,
    llm_call_fn=None,
) -> Optional[LLMAnalysis]:
    """用LLM分析候选股。

    Args:
        stock_code: 股票代码
        trade_date: 交易日
        db_path: 数据库路径
        llm_call_fn: LLM调用函数 fn(prompt) -> str
            如果不提供，使用 anthropic SDK

    Returns:
        LLMAnalysis 或 None
    """
    ctx = _gather_context(stock_code, trade_date, db_path)
    prompt = _build_prompt(ctx)

    if llm_call_fn:
        response_text = llm_call_fn(prompt)
    else:
        response_text = _default_llm_call(prompt)

    if not response_text:
        return None

    # 解析JSON
    try:
        # 提取JSON块
        import re
        match = re.search(r'\{[\s\S]*\}', response_text)
        if not match:
            return None
        data = json.loads(match.group())

        rec = data.get("recommendation", "hold")
        confidence = float(data.get("confidence", 0.5))

        # 基于推荐调整分数
        score_map = {
            "strong_buy": 0.1,
            "buy": 0.05,
            "hold": -0.05,
            "sell": -0.15,
            "avoid": -0.30,
        }
        score_adj = score_map.get(rec, 0.0)

        return LLMAnalysis(
            stock_code=stock_code,
            stock_name=ctx.get("name", stock_code),
            recommendation=rec,
            confidence=confidence,
            score_adjustment=score_adj,
            bull_points=data.get("bull_points", []),
            bear_points=data.get("bear_points", []),
            key_risk=data.get("key_risk", ""),
            summary=data.get("summary", ""),
            raw_response=response_text,
        )
    except (json.JSONDecodeError, ValueError):
        return None


def _default_llm_call(prompt: str) -> Optional[str]:
    """默认LLM调用（使用项目配置的anthropic SDK）。"""
    try:
        import anthropic
        import os

        # 项目使用 Z.AI endpoint
        client = anthropic.Anthropic(
            api_key=os.environ.get("ZAI_API_KEY", ""),
            base_url=os.environ.get("ZAI_BASE_URL", "https://open.bigmodel.cn/api/paas/v4/"),
        )
        message = client.messages.create(
            model="glm-4-plus",
            max_tokens=1000,
            messages=[{"role": "user", "content": prompt}],
        )
        return message.content[0].text
    except Exception as e:
        print(f"  LLM调用失败: {e}")
        return None


def batch_analyze(
    codes: list[str],
    trade_date: str,
    db_path: str = DB_PATH,
    llm_call_fn=None,
) -> dict[str, LLMAnalysis]:
    """批量LLM分析。"""
    results = {}
    for i, code in enumerate(codes):
        print(f"  LLM分析 [{i+1}/{len(codes)}] {code}...")
        analysis = analyze_with_llm(code, trade_date, db_path, llm_call_fn)
        if analysis:
            results[code] = analysis
    return results
