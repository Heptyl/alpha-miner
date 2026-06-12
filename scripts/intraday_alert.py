#!/usr/bin/env python3
"""盘中买点分析 — 9:25 集合竞价后推送。

基于昨日推荐的5只股，结合实时行情给出具体买点建议。
用法:
  uv run python scripts/intraday_alert.py
  uv run python scripts/intraday_alert.py --date 2026-04-30
"""

import argparse
import json
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


def _get_open_price(code: str) -> float | None:
    """尝试获取今日开盘价（集合竞价结果）。

    优先级：akshare实时行情 > 数据库。
    """
    # 尝试 akshare 获取实时行情
    try:
        import akshare as ak
        df = ak.stock_zh_a_spot_em()
        if df is not None and not df.empty:
            row = df[df["代码"] == code]
            if not row.empty:
                return float(row.iloc[0]["今开"])
    except Exception:
        pass

    return None


def _load_recommend_stocks(trade_date: str) -> list[dict]:
    """加载昨日推荐结果。"""
    json_file = Path(f"recommendations/{trade_date}_recommend.json")
    if not json_file.exists():
        return []

    report = json.loads(json_file.read_text(encoding="utf-8"))
    return report.get("stocks", [])


def _get_yesterday_close(code: str, trade_date: str, conn: sqlite3.Connection) -> float | None:
    """获取昨日收盘价。"""
    row = conn.execute(
        "SELECT close FROM daily_price WHERE stock_code=? AND trade_date=?",
        (code, trade_date),
    ).fetchone()
    return row[0] if row else None


def _get_recent_kline(code: str, trade_date: str, conn: sqlite3.Connection, days: int = 5):
    """获取最近N天K线。"""
    rows = conn.execute(
        """SELECT trade_date, open, close, high, low, volume, amount
        FROM daily_price WHERE stock_code=? AND trade_date<=?
        ORDER BY trade_date DESC LIMIT ?""",
        (code, trade_date, days),
    ).fetchall()
    return rows


def _get_zt_info(code: str, trade_date: str, conn: sqlite3.Connection):
    """获取涨停信息。"""
    row = conn.execute(
        "SELECT consecutive_zt, open_count, amount FROM zt_pool WHERE stock_code=? AND trade_date=?",
        (code, trade_date),
    ).fetchone()
    if row:
        return {"consecutive_zt": row[0], "open_count": row[1], "amount": row[2]}
    return None


def _get_concepts(code: str, conn: sqlite3.Connection) -> list[str]:
    """获取概念。"""
    rows = conn.execute(
        "SELECT concept_name FROM concept_mapping WHERE stock_code=? LIMIT 5",
        (code,),
    ).fetchall()
    return [r[0] for r in rows]


def _analyze_action(
    code: str,
    name: str,
    close: float,
    open_price: float | None,
    buy_zone_low: float,
    buy_zone_high: float,
    buy_price: float,
    target_price: float,
    stop_loss: float,
    zt_info: dict | None,
    klines: list,
    concepts: list[str],
) -> str:
    """生成单只股的买点分析。

    返回格式化的分析文本。
    """
    lines = []
    cb = zt_info["consecutive_zt"] if zt_info else 0

    if open_price and close > 0:
        gap_pct = (open_price - close) / close * 100
        if gap_pct > 2:
            gap_desc = f"高开{gap_pct:.1f}%"
        elif gap_pct > 0.5:
            gap_desc = f"小幅高开{gap_pct:.1f}%"
        elif gap_pct > -0.5:
            gap_desc = f"平开{gap_pct:+.1f}%"
        elif gap_pct > -2:
            gap_desc = f"小幅低开{gap_pct:.1f}%"
        else:
            gap_desc = f"低开{gap_pct:.1f}%"

        lines.append(f"  开盘: {open_price:.2f} ({gap_desc})")

        # 判断操作
        if open_price > buy_zone_high:
            action = "⚠️ 等待"
            reason = f"开盘{open_price:.2f}高于买区上限{buy_zone_high:.2f}，等回落再买"
            if open_price > close * 1.05:
                action = "🚫 放弃"
                reason = f"高开{gap_pct:.1f}%过多，追高风险极大"
        elif open_price < buy_zone_low:
            action = "⚡ 低吸"
            reason = f"开盘{open_price:.2f}低于买区{buy_zone_low:.2f}，低开是机会，可逢低买入"
        elif open_price <= buy_price:
            action = "✅ 买入"
            reason = f"开盘{open_price:.2f}在买区内，接近基准价{buy_price:.2f}，可直接买入"
        else:
            action = "👀 观察"
            reason = f"开盘{open_price:.2f}在买区内偏上，可等回落到{buy_price:.2f}附近再买"

        lines.append(f"  {action} | {reason}")

        # 具体挂单建议
        if "买入" in action or "低吸" in action:
            entry_price = round(min(open_price * 0.995, buy_price), 2)
            lines.append(f"  建议挂单价: {entry_price:.2f}")
        elif "观察" in action:
            lines.append(f"  回落到 {buy_price:.2f} 以下可买")
    else:
        # 无实时数据，给预判方案
        lines.append(f"  开盘: 暂无数据（未开盘或获取失败）")
        lines.append(f"  三种预案:")
        lines.append(f"    高开>2%: 等回落到{buy_price:.2f}再买，超过{buy_zone_high:.2f}放弃")
        lines.append(f"    平开±1%: {buy_price:.2f}附近直接买入")
        lines.append(f"    低开>2%: {buy_zone_low:.2f}附近低吸，好机会")

    # 风控参数
    lines.append(f"  买区: {buy_zone_low:.2f}~{buy_zone_high:.2f}")
    lines.append(f"  目标: {target_price:.2f} | 止损: {stop_loss:.2f}")

    # 连板信息
    if cb >= 2:
        lines.append(f"  ⚠ {cb}连板，波动大，仓位减半")

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="盘中买点分析")
    parser.add_argument("--date", type=str, default=None, help="推荐日期(昨日)")
    args = parser.parse_args()

    now = datetime.now()
    print(f"{'=' * 60}")
    print(f"  Alpha Miner 盘中买点分析")
    print(f"  运行时间: {now.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'=' * 60}")

    conn = sqlite3.connect("data/alpha_miner.db")

    # 确定推荐日期
    if args.date:
        trade_date = args.date
    else:
        row = conn.execute(
            "SELECT MAX(trade_date) FROM daily_price"
        ).fetchone()
        trade_date = row[0] if row else None

    if not trade_date:
        print("  ❌ 无数据")
        conn.close()
        return

    print(f"  基于推荐日期: {trade_date}")

    # 加载推荐
    stocks = _load_recommend_stocks(trade_date)
    if not stocks:
        print("  ❌ 无推荐数据")
        conn.close()
        return

    print(f"  推荐个股: {len(stocks)}只\n")

    # 逐只分析
    results = []
    for i, s in enumerate(stocks, 1):
        code = s["stock_code"]
        name = s.get("stock_name", code)
        buy_zone_low = s.get("buy_zone_low", 0)
        buy_zone_high = s.get("buy_zone_high", 0)
        buy_price = s.get("buy_price", 0)
        target_price = s.get("target_price", 0)
        stop_loss = s.get("stop_loss", 0)

        close = _get_yesterday_close(code, trade_date, conn) or 0
        open_price = _get_open_price(code)
        zt_info = _get_zt_info(code, trade_date, conn)
        klines = _get_recent_kline(code, trade_date, conn)
        concepts = _get_concepts(code, conn)

        analysis = _analyze_action(
            code, name, close, open_price,
            buy_zone_low, buy_zone_high, buy_price,
            target_price, stop_loss, zt_info, klines, concepts,
        )

        results.append({
            "code": code,
            "name": name,
            "close": close,
            "analysis": analysis,
            "buy_price": buy_price,
            "target": target_price,
            "stop_loss": stop_loss,
        })

        print(f"  ── #{i} {code} {name} ──")
        print(f"  昨收: {close:.2f}")
        print(analysis)
        print()

    conn.close()

    # 格式化推送消息
    next_date_str = (
        datetime.strptime(trade_date, "%Y-%m-%d").strftime("%m月%d日")
    )
    msg_lines = [
        f"⚡ Alpha Miner 盘中买点 | {now.strftime('%m月%d日 %H:%M')}",
        f"📋 基于{trade_date}收盘推荐",
        "",
    ]

    for i, r in enumerate(results, 1):
        msg_lines.append(f"─── #{i} {r['code']} {r['name']} ───")
        msg_lines.append(f"昨收: {r['close']:.2f}")
        # 推送版精简分析（不含实时开盘价的长版文本）
        msg_lines.append(f"买区: {r['buy_price']-abs(r['buy_price']*0.02):.2f}~{r['buy_price']+abs(r['buy_price']*0.02):.2f}")
        msg_lines.append(f"基准价: {r['buy_price']:.2f} | 目标: {r['target']:.2f}")
        msg_lines.append(f"止损: {r['stop_loss']:.2f}")
        msg_lines.append("")

    msg_lines.append("─── 操作策略 ───")
    msg_lines.append("高开>2%: 等回落到基准价再买")
    msg_lines.append("平开±1%: 基准价附近直接买")
    msg_lines.append("低开>2%: 低吸机会，果断买")
    msg_lines.append("")
    msg_lines.append("⚠ 仅供参考，不构成投资建议")

    msg = "\n".join(msg_lines)

    # 保存
    Path("recommendations").mkdir(exist_ok=True)
    alert_file = Path(f"recommendations/{trade_date}_intraday_alert.txt")
    alert_file.write_text(msg, encoding="utf-8")
    print(f"\n✅ 盘中分析已保存: {alert_file}")

    # 输出推送消息
    print(f"\n{'═' * 50}")
    print(msg)
    print(f"{'═' * 50}")


if __name__ == "__main__":
    main()
