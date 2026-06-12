"""盘后复盘Agent — 每日收盘后自动复盘所有交易

功能:
  1. 读取当天trade_memory中的所有交易
  2. 配对买入/卖出, 补充完整上下文(行情/行业/信号)
  3. LLM归因分析: 为什么买、为什么卖、盈亏原因
  4. 自动生成经验教训写入trade_memory.lessons
  5. 输出Markdown复盘报告到 output/daily_review/
  6. 关键发现推送到日志(连续亏损/行业集中/策略异常)

触发方式:
  - daemon收盘退出时自动调用
  - crontab: 0 16 * * 1-5 python -m src.agent.review_agent
  - 手动: python src/agent/review_agent.py --date 2026-05-30

注意: 盘后复盘不碰任何交易逻辑, 纯分析。
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
from datetime import datetime, date
from pathlib import Path
from typing import Optional

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

DB_PATH = Path(__file__).resolve().parents[2] / "data" / "alpha_miner.db"
OUTPUT_DIR = Path(__file__).resolve().parents[2] / "output" / "daily_review"

logger = logging.getLogger("review_agent")

# ── LLM Prompt ──
REVIEW_PROMPT = """你是A股量化交易复盘专家。请对以下交易进行归因分析。

## 交易信息
- 股票: {name}({code})
- 策略: 策略{strategy}
- 行业: {industry}
- 买入信号: {entry_signal}
- 买入日期: {buy_date}, 价格: ¥{buy_price}
- 卖出日期: {sell_date}, 价格: ¥{sell_price}
- 持仓天数: {hold_days}天
- 盈亏: {pnl_pct:+.1f}%
- 卖出原因: {exit_reason}
- 历史同策略胜率: {win_rate:.0%}

## 请分析
1. 买入时机: 当时的信号是否合理? 是否追高/追涨?
2. 卖出时机: 止盈/止损是否及时? 是否该持有更久?
3. 盈亏归因: 赚了因为什么/亏了因为什么(行业/个股/时机/策略)
4. 经验教训: 一句话可执行建议

输出JSON:
{{
  "entry_analysis": "<买入时机分析, 1-2句>",
  "exit_analysis": "<卖出时机分析, 1-2句>",
  "pnl_attribution": "<盈亏归因, 1-2句>",
  "lesson": "<一句话经验教训>",
  "rating": "<good/ok/bad>"
}}"""

SUMMARY_PROMPT = """你是A股量化交易系统复盘专家。请根据今日交易汇总做整体复盘。

## 今日交易汇总
{summary_text}

## 策略统计
{strategy_stats}

## 请分析
1. 今日整体表现评价
2. 哪个策略表现最好/最差, 为什么
3. 是否有系统性问题(连续亏损/行业集中/信号失效)
4. 明日操作建议

输出Markdown格式, 不要用代码块包裹。"""


def _get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def _get_llm_client():
    """复用项目LLM配置(委托给统一LLM客户端)"""
    from src.agent.llm_client import get_client
    c = get_client()
    return c.get_anthropic_client()


def _call_llm(client, model: str, prompt: str, system: str = "") -> str:
    """调用LLM, 返回文本(委托给统一LLM客户端)"""
    from src.agent.llm_client import get_client
    c = get_client()
    return c.chat(prompt, system=system, max_tokens=800, caller="review_agent") or ""


def _parse_json_response(text: str) -> dict:
    """解析LLM返回的JSON"""
    if "```json" in text:
        text = text.split("```json")[1].split("```")[0]
    elif "```" in text:
        text = text.split("```")[1].split("```")[0]
    try:
        return json.loads(text.strip())
    except Exception:
        return {}


def _get_market_context(conn: sqlite3.Connection, trade_date: str) -> dict:
    """获取当日市场环境"""
    row = conn.execute(
        "SELECT zt_count, dt_count, sentiment_level FROM market_emotion WHERE trade_date = ?",
        (trade_date,),
    ).fetchone()

    if row:
        return {
            "zt_count": row["zt_count"] or 0,
            "dt_count": row["dt_count"] or 0,
            "sentiment": row["sentiment_level"] or "正常",
        }
    return {"zt_count": 0, "dt_count": 0, "sentiment": "未知"}


def _get_price_context(conn: sqlite3.Connection, code: str, trade_date: str) -> dict:
    """获取个股当日行情"""
    row = conn.execute(
        """SELECT open, high, low, close, volume, amount, turnover_rate
           FROM daily_price WHERE stock_code=? AND trade_date=?""",
        (code, trade_date),
    ).fetchone()

    if row:
        return {
            "open": row["open"], "high": row["high"], "low": row["low"],
            "close": row["close"], "volume": row["volume"],
            "amount": row["amount"], "turnover_rate": row["turnover_rate"],
        }
    return {}


def _detect_patterns(trades: list[dict]) -> list[str]:
    """检测交易中的异常模式"""
    alerts = []

    # 连续亏损
    consecutive_losses = 0
    for t in trades:
        if t.get("action") == "sell" and (t.get("pnl_pct") or 0) < 0:
            consecutive_losses += 1
            if consecutive_losses >= 3:
                alerts.append(f"⚠️ 连续{consecutive_losses}笔亏损, 检查策略{t.get('strategy', '')}是否需要暂停")
        else:
            consecutive_losses = 0

    # 行业集中
    industry_counts = {}
    for t in trades:
        ind = t.get("industry", "")
        if ind and ind != "未知":
            industry_counts[ind] = industry_counts.get(ind, 0) + 1
    for ind, cnt in industry_counts.items():
        if cnt >= 3:
            alerts.append(f"⚠️ {ind}行业交易{cnt}笔, 注意集中度风险")

    # 策略胜率
    from collections import defaultdict
    strategy_results = defaultdict(lambda: {"wins": 0, "losses": 0, "total_pnl": 0})
    for t in trades:
        if t.get("action") == "sell":
            s = t.get("strategy", "")
            pnl = t.get("pnl_pct", 0)
            strategy_results[s]["total_pnl"] += pnl
            if pnl > 0:
                strategy_results[s]["wins"] += 1
            else:
                strategy_results[s]["losses"] += 1

    for strat, data in strategy_results.items():
        total = data["wins"] + data["losses"]
        if total >= 3:
            wr = data["wins"] / total
            if wr < 0.3:
                alerts.append(f"⚠️ 策略{strat}今日胜率{wr:.0%}({data['wins']}胜{data['losses']}负), 低于30%阈值")
            if data["total_pnl"] < -5:
                alerts.append(f"⚠️ 策略{strat}今日累计亏损{data['total_pnl']:+.1f}%, 超过预警线")

    # 快止损(买入当天/次日即止损)
    fast_stops = [t for t in trades
                  if t.get("action") == "sell" and t.get("hold_days", 99) <= 0
                  and "止损" in (t.get("exit_reason") or "")]
    if len(fast_stops) >= 2:
        alerts.append(f"⚠️ {len(fast_stops)}笔当日止损, 买入信号质量需检查")

    return alerts


def review_trades(trade_date: str = None, use_llm: bool = True) -> str:
    """执行盘后复盘

    Args:
        trade_date: 交易日期(空=今天)
        use_llm: 是否使用LLM归因(False=纯规则)

    Returns:
        Markdown复盘报告
    """
    if not trade_date:
        trade_date = date.today().isoformat()

    conn = _get_conn()
    llm_client, llm_model = (_get_llm_client() if use_llm else (None, None))

    try:
        # 1. 读取当天交易
        trades = conn.execute("""
            SELECT tm.id, tm.trade_id, tm.code, tm.name, tm.strategy, tm.action,
                   tm.pnl_pct, tm.hold_days, tm.industry, tm.entry_signal,
                   tm.exit_reason, tm.attribution, tm.lessons, tm.similar_win_rate
            FROM trade_memory tm
            WHERE DATE(tm.created_at) = ? OR EXISTS (
                SELECT 1 FROM daemon_trades dt WHERE dt.id = tm.trade_id AND dt.trade_date = ?
            )
            ORDER BY tm.action DESC, tm.id
        """, (trade_date, trade_date)).fetchall()

        if not trades:
            logger.info(f"[复盘] {trade_date} 无交易记录")
            return f"# {trade_date} 盘后复盘\n\n无交易记录。"

        sells = [dict(t) for t in trades if t["action"] == "sell"]
        buys = [dict(t) for t in trades if t["action"] == "buy"]
        all_trades = sells + buys

        logger.info(f"[复盘] {trade_date}: {len(buys)}笔买入, {len(sells)}笔卖出")

        # 2. 市场环境
        market = _get_market_context(conn, trade_date)

        # 3. 逐笔LLM归因(仅sell交易)
        trade_reviews = []
        for sell in sells:
            review = _review_single_trade(
                conn, sell, trade_date, llm_client, llm_model
            )
            trade_reviews.append(review)

            # 更新lessons到DB
            if review.get("lesson") and not sell.get("lessons"):
                conn.execute(
                    "UPDATE trade_memory SET lessons = ? WHERE id = ?",
                    (review["lesson"], sell["id"]),
                )

        if trade_reviews:
            conn.commit()

        # 4. 检测异常模式
        alerts = _detect_patterns(all_trades)

        # 5. 生成Markdown报告
        report = _generate_report(
            trade_date, market, sells, buys, trade_reviews, alerts
        )

        # 6. 保存报告
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        report_path = OUTPUT_DIR / f"{trade_date}.md"
        report_path.write_text(report, encoding="utf-8")
        logger.info(f"[复盘] 报告已保存: {report_path}")

        # 7. 推送关键发现
        for alert in alerts:
            logger.warning(f"[复盘发现] {alert}")

        return report

    finally:
        conn.close()


def _review_single_trade(conn: sqlite3.Connection, sell: dict,
                         trade_date: str, llm_client, llm_model) -> dict:
    """复盘单笔交易"""
    code = sell["code"]
    strategy = sell.get("strategy", "未知")
    pnl_pct = sell.get("pnl_pct", 0)
    hold_days = sell.get("hold_days", 0)

    # 获取买入信息
    buy = conn.execute("""
        SELECT dt.trade_date, dt.price, dt.signal_type, dt.reason, dt.ml_score
        FROM daemon_trades dt
        WHERE dt.code = ? AND dt.action = 'buy'
          AND dt.trade_date <= ? AND dt.id < ?
        ORDER BY dt.id DESC LIMIT 1
    """, (code, trade_date, sell.get("trade_id", 999999))).fetchone()

    buy_date = buy["trade_date"] if buy else trade_date
    buy_price = buy["price"] if buy else 0
    entry_signal = sell.get("entry_signal") or (buy["signal_type"] if buy else "")
    ml_score = buy["ml_score"] if buy else 0

    sell_price = conn.execute(
        "SELECT price FROM daemon_trades WHERE id = ?",
        (sell.get("trade_id", 0),),
    ).fetchone()
    sell_price = sell_price["price"] if sell_price else 0

    win_rate = sell.get("similar_win_rate", 0)

    review = {
        "code": code,
        "name": sell.get("name", ""),
        "strategy": strategy,
        "pnl_pct": pnl_pct,
        "hold_days": hold_days,
        "entry_signal": entry_signal,
        "exit_reason": sell.get("exit_reason", ""),
        "industry": sell.get("industry", ""),
        "buy_date": buy_date,
        "sell_date": trade_date,
        "buy_price": buy_price,
        "sell_price": sell_price,
        "ml_score": ml_score,
        "win_rate": win_rate,
        "entry_analysis": "",
        "exit_analysis": "",
        "pnl_attribution": "",
        "lesson": "",
        "rating": "ok",
    }

    # LLM归因
    if llm_client and llm_model:
        prompt = REVIEW_PROMPT.format(
            name=sell.get("name", ""), code=code,
            strategy=strategy,
            industry=sell.get("industry", "未知"),
            entry_signal=entry_signal,
            buy_date=buy_date, buy_price=f"{buy_price:.2f}" if buy_price else "N/A",
            sell_date=trade_date, sell_price=f"{sell_price:.2f}" if sell_price else "N/A",
            hold_days=hold_days, pnl_pct=pnl_pct,
            exit_reason=sell.get("exit_reason", "未知"),
            win_rate=win_rate if win_rate else 0.5,
        )
        response = _call_llm(llm_client, llm_model, prompt)
        parsed = _parse_json_response(response)

        if parsed:
            review["entry_analysis"] = parsed.get("entry_analysis", "")
            review["exit_analysis"] = parsed.get("exit_analysis", "")
            review["pnl_attribution"] = parsed.get("pnl_attribution", "")
            review["lesson"] = parsed.get("lesson", "")
            review["rating"] = parsed.get("rating", "ok")
    else:
        # 纯规则归因
        review["pnl_attribution"] = sell.get("attribution", "")
        review["lesson"] = sell.get("lessons", "")
        if pnl_pct > 3:
            review["rating"] = "good"
        elif pnl_pct < -3:
            review["rating"] = "bad"
        else:
            review["rating"] = "ok"

    return review


def _generate_report(trade_date: str, market: dict,
                     sells: list[dict], buys: list[dict],
                     trade_reviews: list[dict], alerts: list[str]) -> str:
    """生成Markdown复盘报告"""
    lines = []
    lines.append(f"# {trade_date} 盘后复盘")
    lines.append("")
    lines.append(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("")

    # 市场环境
    lines.append("## 市场环境")
    lines.append(f"- 涨停: {market['zt_count']}只 | 跌停: {market['dt_count']}只")
    lines.append(f"- 情绪: {market['sentiment']}")
    lines.append("")

    # 今日概览
    total_sells = len(sells)
    wins = sum(1 for s in sells if (s.get("pnl_pct") or 0) > 0)
    total_pnl = sum(s.get("pnl_pct", 0) for s in sells)
    avg_pnl = total_pnl / total_sells if total_sells > 0 else 0

    lines.append("## 今日概览")
    lines.append(f"| 指标 | 值 |")
    lines.append(f"|------|------|")
    lines.append(f"| 买入 | {len(buys)}笔 |")
    lines.append(f"| 卖出 | {total_sells}笔 |")
    lines.append(f"| 胜率 | {wins}/{total_sells} ({wins/total_sells:.0%}) |" if total_sells > 0 else "| 胜率 | N/A |")
    lines.append(f"| 均收 | {avg_pnl:+.1f}% |")
    lines.append("")

    # 策略统计
    from collections import defaultdict
    strat_stats = defaultdict(lambda: {"wins": 0, "losses": 0, "total_pnl": 0, "count": 0})
    for s in sells:
        strat = s.get("strategy", "未知")
        pnl = s.get("pnl_pct", 0)
        strat_stats[strat]["count"] += 1
        strat_stats[strat]["total_pnl"] += pnl
        if pnl > 0:
            strat_stats[strat]["wins"] += 1
        else:
            strat_stats[strat]["losses"] += 1

    lines.append("## 策略统计")
    lines.append("| 策略 | 笔数 | 胜 | 负 | 胜率 | 均收 | 累计 |")
    lines.append("|------|------|------|------|------|------|------|")
    for strat, data in sorted(strat_stats.items()):
        cnt = data["count"]
        wr = data["wins"] / cnt if cnt > 0 else 0
        avg = data["total_pnl"] / cnt if cnt > 0 else 0
        lines.append(f"| 策略{strat} | {cnt} | {data['wins']} | {data['losses']} | "
                      f"{wr:.0%} | {avg:+.1f}% | {data['total_pnl']:+.1f}% |")
    lines.append("")

    # 逐笔归因
    if trade_reviews:
        lines.append("## 逐笔归因")
        lines.append("")
        for r in trade_reviews:
            rating_icon = {"good": "✅", "ok": "➖", "bad": "❌"}.get(r["rating"], "➖")
            lines.append(f"### {rating_icon} {r['name']}({r['code']}) 策略{r['strategy']} {r['pnl_pct']:+.1f}%")
            lines.append(f"- 买入: {r['buy_date']} ¥{r['buy_price']:.2f} ({r['entry_signal']})")
            lines.append(f"- 卖出: {r['sell_date']} ¥{r['sell_price']:.2f} ({r['exit_reason']})")
            lines.append(f"- 持仓: {r['hold_days']}天 | 行业: {r['industry']}")
            if r.get("entry_analysis"):
                lines.append(f"- **买入分析**: {r['entry_analysis']}")
            if r.get("exit_analysis"):
                lines.append(f"- **卖出分析**: {r['exit_analysis']}")
            if r.get("pnl_attribution"):
                lines.append(f"- **盈亏归因**: {r['pnl_attribution']}")
            if r.get("lesson"):
                lines.append(f"- **教训**: {r['lesson']}")
            lines.append("")

    # 今日买入
    if buys:
        lines.append("## 今日买入")
        lines.append("")
        lines.append("| 代码 | 名称 | 策略 | 信号 |")
        lines.append("|------|------|------|------|")
        for b in buys:
            lines.append(f"| {b['code']} | {b.get('name', '')} | "
                          f"策略{b.get('strategy', '')} | {b.get('entry_signal', '')} |")
        lines.append("")

    # 异常告警
    if alerts:
        lines.append("## 异常告警")
        lines.append("")
        for alert in alerts:
            lines.append(f"- {alert}")
        lines.append("")

    # 关键发现
    lines.append("## 关键发现")
    lines.append("")
    findings = []
    if total_sells > 0:
        best = max(trade_reviews, key=lambda x: x.get("pnl_pct", 0)) if trade_reviews else None
        worst = min(trade_reviews, key=lambda x: x.get("pnl_pct", 0)) if trade_reviews else None
        if best:
            findings.append(f"最佳: {best['name']} {best['pnl_pct']:+.1f}% (策略{best['strategy']})")
        if worst:
            findings.append(f"最差: {worst['name']} {worst['pnl_pct']:+.1f}% (策略{worst['strategy']})")

    for strat, data in strat_stats.items():
        cnt = data["count"]
        if cnt >= 2:
            wr = data["wins"] / cnt if cnt > 0 else 0
            findings.append(f"策略{strat}: {wr:.0%}胜率, 均收{data['total_pnl']/cnt:+.1f}%")

    for f in findings:
        lines.append(f"- {f}")

    if not findings:
        lines.append("- 无显著发现")

    lines.append("")
    lines.append("---")
    lines.append(f"*由 review_agent 自动生成*")

    return "\n".join(lines)


def run_weekly_update():
    """周末更新: 因子权重 + 复盘周报"""
    from src.trader.factor_weights import update_weights
    logger.info("[周更新] 更新因子权重...")
    update_weights()
    logger.info("[周更新] 因子权重更新完成")


if __name__ == "__main__":
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="盘后复盘Agent")
    parser.add_argument("--date", type=str, help="指定日期(YYYY-MM-DD)")
    parser.add_argument("--no-llm", action="store_true", help="不使用LLM(纯规则归因)")
    parser.add_argument("--weekly", action="store_true", help="周末更新(因子权重)")
    args = parser.parse_args()

    if args.weekly:
        run_weekly_update()
    else:
        report = review_trades(
            trade_date=args.date,
            use_llm=not args.no_llm,
        )
        print(report[:2000])
        if len(report) > 2000:
            print(f"\n... (完整报告见 output/daily_review/{args.date or date.today().isoformat()}.md)")
