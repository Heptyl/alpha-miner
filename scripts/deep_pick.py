#!/usr/bin/env python3
"""精选2只最强个股 + LLM深度推理操作建议。

用法:
  uv run python scripts/deep_pick.py --date 2026-04-29
  uv run python scripts/deep_pick.py  # 自动用最新推荐
"""

import argparse
import json
import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


def gather_stock_data(codes: list[str], trade_date: str) -> str:
    """收集候选股的全部数据，格式化为LLM可读文本。"""
    conn = sqlite3.connect("data/alpha_miner.db")
    names = {}
    sections = []

    for code in codes:
        # 名称
        row = conn.execute(
            "SELECT name FROM zt_pool WHERE stock_code=? LIMIT 1", (code,)
        ).fetchone()
        if not row:
            row = conn.execute(
                "SELECT name FROM strong_pool WHERE stock_code=? LIMIT 1", (code,)
            ).fetchone()
        name = row[0] if row else code

        lines = [f"\n=== {code} {name} ==="]

        # K线
        rows = conn.execute(
            """SELECT trade_date, open, close, high, low, volume
            FROM daily_price WHERE stock_code=? AND trade_date>=?
            ORDER BY trade_date""",
            (code, (datetime.strptime(trade_date, "%Y-%m-%d") - timedelta(days=14)).strftime("%Y-%m-%d")),
        ).fetchall()
        if rows:
            lines.append("K线:")
            for r in rows:
                chg = ((r[2] - r[1]) / r[1] * 100) if r[1] > 0 else 0
                lines.append(
                    f"  {r[0]} 开{r[1]:.2f} 收{r[2]:.2f} 高{r[3]:.2f} 低{r[4]:.2f}"
                    f" 量{r[5]:.0f} 涨跌{chg:+.1f}%"
                )

        # 涨停池
        try:
            rows = conn.execute(
                "SELECT trade_date, consecutive_zt, open_count, amount, circulation_mv "
                "FROM zt_pool WHERE stock_code=? AND trade_date>=? ORDER BY trade_date",
                (code, (datetime.strptime(trade_date, "%Y-%m-%d") - timedelta(days=5)).strftime("%Y-%m-%d")),
            ).fetchall()
            if rows:
                lines.append("涨停池:")
                for r in rows:
                    lines.append(
                        f"  {r[0]} 连板={r[1]} 炸板={r[2]} 额度={r[3]:.0f} 流通市值={r[4]:.0f}"
                    )
            else:
                lines.append("涨停池: 无")
        except Exception:
            pass

        # 强势池
        try:
            rows = conn.execute(
                "SELECT trade_date, reason, amount FROM strong_pool "
                "WHERE stock_code=? AND trade_date>=? ORDER BY trade_date",
                (code, (datetime.strptime(trade_date, "%Y-%m-%d") - timedelta(days=5)).strftime("%Y-%m-%d")),
            ).fetchall()
            if rows:
                lines.append("强势池:")
                for r in rows:
                    lines.append(f"  {r[0]} {r[1]} 额度={r[2]:.0f}")
            else:
                lines.append("强势池: 无")
        except Exception:
            pass

        # 龙虎榜
        try:
            rows = conn.execute(
                "SELECT trade_date, buy_amount, sell_amount, net_buy "
                "FROM lhb_detail WHERE stock_code=? AND trade_date>=? ORDER BY trade_date",
                (code, (datetime.strptime(trade_date, "%Y-%m-%d") - timedelta(days=5)).strftime("%Y-%m-%d")),
            ).fetchall()
            if rows:
                lines.append("龙虎榜:")
                for r in rows:
                    lines.append(f"  {r[0]} 买={r[1]:.0f} 卖={r[2]:.0f} 净={r[3]:.0f}")
            else:
                lines.append("龙虎榜: 无")
        except Exception:
            pass

        # 因子
        rows = conn.execute(
            "SELECT factor_name, factor_value FROM factor_values "
            "WHERE stock_code=? AND trade_date=? ORDER BY factor_name",
            (code, trade_date),
        ).fetchall()
        if rows:
            lines.append("因子:")
            for r in rows:
                lines.append(f"  {r[0]}={r[1]:.4f}")

        # 板块
        rows = conn.execute(
            "SELECT concept_name FROM concept_mapping WHERE stock_code=? LIMIT 8",
            (code,),
        ).fetchall()
        if rows:
            lines.append(f"概念: {[r[0] for r in rows]}")

        sections.append("\n".join(lines))

    conn.close()
    return "\n".join(sections)


def build_llm_prompt(stocks_data: str, trade_date: str, market_info: str) -> str:
    """构建LLM深度推理的完整prompt。"""
    next_date = (
        datetime.strptime(trade_date, "%Y-%m-%d") + timedelta(days=1)
    ).strftime("%Y-%m-%d")

    return f"""你是一位资深A股短线操盘手。请基于以下5只个股的数据，选出逻辑涨势最强的2只，并给出极其详细的次日操作建议。

=== 候选5只（基于{trade_date}收盘数据，适用于{next_date}操作）===
{stocks_data}

=== 市场环境 ===
{market_info}

=== 请严格按照以下格式输出 ===

【选出2只】
编号、代码、名称，以及选择理由（从趋势、量价、资金、题材角度分析）

【第1只操作建议】
代码 名称
- 集合竞价（9:15-9:25）：具体挂单价格、什么情况放弃
- 开盘操作（9:30-10:00）：低开/平开/高开分别怎么操作
- 盘中买入价位：具体到小数点后2位
- 放弃信号：什么情况坚决不买
- 止盈策略：分批止盈价位
- 止损价位和执行纪律

【第2只操作建议】
同上格式

请务必结合个股实际K线数据给出精准价位，不要泛泛而谈。"""


def format_deep_pick_message(result_text: str, trade_date: str) -> str:
    """将LLM输出格式化为微信推送消息。"""
    next_date = (
        datetime.strptime(trade_date, "%Y-%m-%d") + timedelta(days=1)
    ).strftime("%m月%d日")

    # 清理LLM输出，提取核心内容
    msg = f"""🎯 Alpha Miner 精选2只 | {next_date}操作指南
📅 基于{trade_date}收盘数据 | LLM深度推理

{'─' * 30}

{result_text}

{'─' * 30}
⚠ 以上仅供参考，不构成投资建议"""
    return msg


def main():
    parser = argparse.ArgumentParser(description="精选2只最强个股")
    parser.add_argument("--date", type=str, default=None, help="日期 YYYY-MM-DD")
    args = parser.parse_args()

    # 确定交易日
    conn = sqlite3.connect("data/alpha_miner.db")
    if args.date:
        trade_date = args.date
    else:
        row = conn.execute(
            "SELECT MAX(trade_date) FROM daily_price"
        ).fetchone()
        trade_date = row[0] if row else None

    if not trade_date:
        print("❌ 无可用数据")
        return

    print(f"交易日: {trade_date}")

    # 读取推荐结果
    json_file = Path(f"recommendations/{trade_date}_recommend.json")
    if not json_file.exists():
        print(f"❌ 未找到推荐文件: {json_file}")
        return

    report = json.loads(json_file.read_text(encoding="utf-8"))
    stocks = report.get("stocks", [])
    if not stocks:
        print("❌ 无推荐个股")
        return

    codes = [s["stock_code"] for s in stocks]
    print(f"推荐个股: {codes}")

    # 市场概况
    zt_count = report.get("zt_count", 0)
    dt_count = report.get("dt_count", 0)
    regime = report.get("market_regime", "")
    hot = report.get("hot_industries", [])
    hot_str = " | ".join(
        [f"{h.get('industry', '')}({h.get('zt_count', '')}只涨停)" for h in hot[:3]]
    )
    market_info = f"涨停{zt_count}只，跌停{dt_count}只，{regime}\n热门板块: {hot_str}"

    # 收集数据
    print("收集个股数据...")
    stocks_data = gather_stock_data(codes, trade_date)

    # 构建prompt — 分两段调用避免截断
    print("LLM深度推理中（第1段：选股+第1只操作建议）...")
    from src.strategy.llm_analysis import _default_llm_call

    next_date = (
        datetime.strptime(trade_date, "%Y-%m-%d") + timedelta(days=1)
    ).strftime("%Y-%m-%d")

    # 第1段：选股 + 第1只详细建议
    prompt1 = f"""你是一位资深A股短线操盘手。请基于以下5只个股数据，选出逻辑涨势最强的2只。

=== 候选5只（{trade_date}收盘，{next_date}操作）===
{stocks_data}

=== 市场环境 ===
{market_info}

=== 输出要求（严格控制长度，每只不超过15行）===
1. 选出的2只及简要理由（各2-3行）
2. 第1只操作建议（紧凑格式）：
   集合竞价：挂单价格、放弃条件（2行）
   开盘操作：高开/平开/低开策略（3行）
   买入价位：2个精确价格（1行）
   放弃信号（1行）
   止盈：2个目标价（1行）
   止损：1个价格（1行）

只输出第1只，第2只下一轮给。不要多余的解释。"""

    result1 = _default_llm_call(prompt1)
    if not result1:
        print("❌ LLM分析第1段失败")
        return
    print(f"✅ 第1段完成 ({len(result1)} 字)")

    prompt2 = f"""上一轮你选出了2只最强个股。现在给出第2只操作建议。

上一轮输出：
{result1}

数据回顾（{trade_date}收盘）：
{stocks_data}

=== 输出要求（严格控制长度，不超过20行）===
第2只操作建议（紧凑格式）：
   集合竞价：挂单价格、放弃条件（2行）
   开盘操作：高开/平开/低开策略（3行）
   买入价位：2个精确价格（1行）
   放弃信号（1行）
   止盈：2个目标价（1行）
   止损：1个价格（1行）

最后3条操作纪律（3行）。不要多余解释。"""

    print("LLM深度推理中（第2段：第2只操作建议）...")
    result2 = _default_llm_call(prompt2)
    if not result2:
        print("❌ LLM分析第2段失败，仅使用第1段结果")
        result = result1
    else:
        result = result1 + "\n\n" + result2

    # 格式化推送消息
    msg = format_deep_pick_message(result, trade_date)

    # 保存
    Path("recommendations").mkdir(exist_ok=True)
    pick_file = Path(f"recommendations/{trade_date}_deep_pick.txt")
    pick_file.write_text(msg, encoding="utf-8")
    print(f"✅ 精选结果已保存: {pick_file}")

    # 输出消息供cron读取
    print(f"\n{'═' * 50}")
    print(msg)
    print(f"{'═' * 50}")


if __name__ == "__main__":
    main()
