#!/usr/bin/env python3
"""全部推荐股LLM深度推理操作建议（5只全覆盖）。

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


def get_close_prices(codes: list[str], trade_date: str) -> dict[str, float]:
    """获取每只股票的当日收盘价，用于验证LLM输出。"""
    conn = sqlite3.connect("data/alpha_miner.db")
    prices = {}
    for code in codes:
        row = conn.execute(
            "SELECT close FROM daily_price WHERE stock_code=? AND trade_date=?",
            (code, trade_date),
        ).fetchone()
        if row:
            prices[code] = row[0]
    conn.close()
    return prices


PRICING_RULES = """=== 关键定价规则（必须严格遵守）===
- 所有价位必须基于今日收盘价计算，绝不可以用历史低价
- 涨停股（10%板）次日竞价范围 = 收盘价 × (0.9 ~ 1.1)
- 创业板/科创板涨停（20%板）次日竞价范围 = 收盘价 × (0.8 ~ 1.2)
- 高开 = 开盘价 > 收盘价 × 1.01，低开 = 开盘价 < 收盘价 × 0.99
- 竞价建议价必须 ≥ 收盘价 × 0.97（涨停股不能建议在前低附近买入）
- 买入价必须接近收盘价（允许±3%），不能大幅低于收盘价
- 止损价 ≤ 收盘价 × 0.97"""

OPERATION_FORMAT = """每只操作建议格式（紧凑，不超过10行）：
  代码 名称 (收盘价XX.XX)
  集合竞价：挂单价格、放弃条件（2行，基于收盘价）
  开盘操作：高开/平开/低开策略（3行）
  买入价位：2个精确价格（≥ 收盘价×0.97）
  放弃信号（1行）
  止盈：2个目标价
  止损：1个价格（≤ 收盘价×0.97）"""


def main():
    parser = argparse.ArgumentParser(description="全部推荐股LLM深度分析")
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
    conn.close()

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

    # 获取收盘价
    close_prices = get_close_prices(codes, trade_date)
    price_info = " | ".join([f"{c}={close_prices.get(c, '?') if isinstance(close_prices.get(c), str) else f'{close_prices.get(c, 0):.2f}'}" for c in codes])

    from src.strategy.llm_analysis import _default_llm_call

    next_date = (
        datetime.strptime(trade_date, "%Y-%m-%d") + timedelta(days=1)
    ).strftime("%m月%d日")

    # ── 第1段：全部5只选股逻辑 + 前3只操作建议 ──
    prompt1 = f"""你是一位资深A股短线操盘手。请为以下5只个股各给出次日操作建议。

=== 候选5只（{trade_date}收盘，{next_date}操作）===
{stocks_data}

=== 今日收盘价 ===
{price_info}

=== 市场环境 ===
{market_info}

{PRICING_RULES}

=== 输出要求 ===
1. 5只简要分析（各1-2行，注明今日收盘价，从趋势/量价/资金/题材角度）
2. 前3只(#1~#3)的详细操作建议：

{OPERATION_FORMAT}

只输出前3只操作建议，后2只下一轮给。不要多余解释。"""

    print("LLM深度推理中（第1段：5只分析 + 前3只操作建议）...")
    result1 = _default_llm_call(prompt1)
    if not result1:
        print("❌ LLM分析第1段失败")
        return
    print(f"✅ 第1段完成 ({len(result1)} 字)")

    # ── 第2段：后2只操作建议 + 操作纪律 ──
    prompt2 = f"""上一轮你分析了5只个股并给出了前3只的操作建议。现在给出后2只的操作建议。

上一轮输出（前3只）：
{result1}

数据回顾（{trade_date}收盘）：
今日收盘价: {price_info}

{stocks_data}

{PRICING_RULES}

=== 输出要求（不超过25行）===
后2只(#4~#5)的详细操作建议：

{OPERATION_FORMAT}

最后附上3条操作纪律。不要多余解释。"""

    print("LLM深度推理中（第2段：后2只操作建议）...")
    result2 = _default_llm_call(prompt2)
    if not result2:
        print("❌ LLM分析第2段失败，仅使用第1段结果")
        result = result1
    else:
        result = result1 + "\n\n" + result2

    # 格式化推送消息
    msg = f"""🎯 Alpha Miner 全部5只 | {next_date}操作指南
📅 基于{trade_date}收盘数据 | LLM深度推理

{'─' * 30}

{result}

{'─' * 30}
⚠ 以上仅供参考，不构成投资建议"""

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
