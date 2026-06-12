#!/usr/bin/env python3
"""每日复盘报告自动生成器

从DB读取当日交易/持仓/市场数据, 生成 daily_review_YYYY-MM-DD.md
由 cron_daily.sh 在日终流水线末尾自动调用
也可手动运行: uv run python scripts/generate_daily_review.py [--date 2026-05-14]
"""
import sys
import sqlite3
import json
import argparse
from pathlib import Path
from datetime import datetime, timedelta

sys.path.insert(0, str(Path(__file__).parent.parent))

DB_PATH = Path(__file__).parent.parent / "data" / "alpha_miner.db"
OUTPUT_DIR = Path(__file__).parent.parent / "output" / "reports"

WEEKDAYS = ["周一", "周二", "周三", "周四", "周五"]


def generate(date_str: str) -> str:
    date = datetime.strptime(date_str, "%Y-%m-%d")
    weekday = WEEKDAYS[date.weekday()]
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    lines = []
    lines.append(f"# Alpha Miner 日报 — {date_str} ({weekday})")
    lines.append("")

    # ── 一、模拟盘交易 ──
    lines.append("## 一、今日模拟盘交易")
    lines.append("")
    c.execute("""
        SELECT action, code, name, price, shares, signal_type, trade_date
        FROM daemon_trades WHERE trade_date = ? AND period = (SELECT MAX(period) FROM daemon_account) ORDER BY id
    """, (date_str,))
    trades = c.fetchall()
    if trades:
        lines.append("| 时间 | 操作 | 股票 | 价格 | 策略 | 原因 |")
        lines.append("|------|------|------|------|------|------|")
        for t in trades:
            action = "买入" if t["action"] == "buy" else "卖出"
            lines.append(f"| 盘中 | {action} | {t['name']} {t['code']} | {t['price']:.3f} | {t['signal_type'] or '-'} | - |")
    else:
        lines.append("*今日无交易*")
    lines.append("")

    # ── 二、收盘持仓 ──
    lines.append("## 二、收盘持仓")
    lines.append("")
    c.execute("""
        SELECT p.code, p.name, p.buy_price, p.shares, p.buy_date, p.signal_type,
               (SELECT d.close FROM daily_price d WHERE d.stock_code = p.code AND d.trade_date = ?) as latest_close
        FROM daemon_positions p WHERE p.status = 'held' AND p.period = (SELECT MAX(period) FROM daemon_account) ORDER BY p.id
    """, (date_str,))
    positions = c.fetchall()
    total_mv = 0
    total_cost = 0
    if positions:
        lines.append("| 股票 | 买入价 | 收盘价 | 盈亏% | 策略 | 持有天数 |")
        lines.append("|------|--------|--------|-------|------|----------|")
        for p in positions:
            close = p["latest_close"] or p["buy_price"]
            cost = p["buy_price"] * p["shares"]
            mv = close * p["shares"]
            pnl_pct = (close - p["buy_price"]) / p["buy_price"] * 100
            hold = (date - datetime.strptime(p["buy_date"], "%Y-%m-%d")).days + 1
            total_mv += mv
            total_cost += cost
            lines.append(f"| {p['name']} {p['code']} | {p['buy_price']:.3f} | {close:.2f} | {pnl_pct:+.1f}% | {p['signal_type'] or '-'} | {hold}天 |")

    # 账户信息
    c.execute("SELECT * FROM daemon_account WHERE period = (SELECT MAX(period) FROM daemon_account) ORDER BY date DESC LIMIT 1")
    acc = c.fetchone()
    cash = acc["cash"] if acc else 0
    if not positions:
        total_mv = 0
        total_cost = 0

    # 获取当前period的INITIAL_CAPITAL
    c.execute("SELECT cash, total_assets FROM daemon_account WHERE period = (SELECT MAX(period) FROM daemon_account) ORDER BY date DESC LIMIT 1")
    acc_row = c.fetchone()
    if acc_row and acc_row["total_assets"] > 0:
        initial = 90000  # 与daemon INITIAL_CAPITAL保持同步
    else:
        initial = 90000
    total_assets = cash + total_mv
    total_pnl = total_assets - initial
    pnl_pct_str = f"{total_pnl / initial * 100:+.2f}%"
    lines.append("")
    lines.append(f"总资产: ¥{total_assets:,.0f}({pnl_pct_str}) | 现金: ¥{cash:,.0f} | 市值: ¥{total_mv:,.0f} | 浮盈: {total_mv - total_cost:+,.0f}")
    lines.append("")

    # ── 三、数据质量概况 ──
    lines.append("## 三、DB数据质量")
    lines.append("")
    lines.append("| 表 | 记录数 | 最新日期 | 状态 |")
    lines.append("|----|--------|----------|------|")
    for table in ["daily_price", "zt_pool", "strong_pool", "fund_flow", "lhb_detail", "news", "market_emotion"]:
        try:
            c.execute(f"SELECT COUNT(*), MAX(trade_date) FROM {table}")
            cnt, latest = c.fetchone()
            # 简单状态判断
            if latest == date_str:
                status = "OK"
            elif latest and (date - datetime.strptime(latest, "%Y-%m-%d")).days <= 1:
                status = "OK"
            else:
                status = f"滞后({latest})"
            lines.append(f"| {table} | {cnt:,} | {latest or '-'} | {status} |")
        except Exception:
            lines.append(f"| {table} | - | - | N/A |")

    # daily_price覆盖
    c.execute("""
        SELECT COUNT(DISTINCT trade_date) FROM daily_price
        WHERE trade_date >= date(?, '-6 months')
    """, (date_str,))
    dp_days = c.fetchone()[0]
    c.execute("""
        SELECT COUNT(*) FROM (
            SELECT trade_date FROM daily_price
            WHERE trade_date >= date(?, '-6 months')
            GROUP BY trade_date HAVING COUNT(*) >= 5000
        )
    """, (date_str,))
    dp_complete = c.fetchone()[0]
    lines.append(f"| daily_price近半年 | {dp_days}天 | 完整{dp_complete}天 | {'OK' if dp_complete == dp_days else f'缺{dp_days - dp_complete}天'} |")
    lines.append("")

    # ── 四、明日关注 ──
    tomorrow = date + timedelta(days=1)
    # 跳过周末
    while tomorrow.weekday() >= 5:
        tomorrow += timedelta(days=1)
    tomorrow_str = tomorrow.strftime("%Y-%m-%d")

    lines.append(f"## 四、明日关注 ({tomorrow_str})")
    lines.append("")

    lines.append("### 模拟盘")
    if positions:
        for p in positions:
            close = p["latest_close"] or p["buy_price"]
            pnl_pct = (close - p["buy_price"]) / p["buy_price"] * 100
            hold = (date - datetime.strptime(p["buy_date"], "%Y-%m-%d")).days + 1
            notes = []
            if pnl_pct > 8:
                notes.append("接近止盈")
            if pnl_pct < -6:
                notes.append("接近止损")
            if hold >= 4:
                notes.append("接近时间止损")
            note_str = ", ".join(notes) if notes else "观察"
            lines.append(f"- {p['name']}({p['code']}): {pnl_pct:+.1f}%, {note_str}")
    lines.append("")

    # 候选池
    ml_file = Path(__file__).parent.parent / "output" / "ml" / "latest_prediction.json"
    if ml_file.exists():
        try:
            with open(ml_file) as f:
                pred = json.load(f)
            top = pred.get("all_top", [])[:5]
            if top:
                lines.append("### ML策略A候选 (Top5)")
                for s in top:
                    lines.append(f"- {s.get('name', s.get('code', '?'))}({s.get('code', '?')}) 分数{s.get('score', 0):.4f}")
                lines.append("")
        except Exception:
            pass

    # ── 五、模拟盘累计统计 ──
    lines.append("## 五、模拟盘累计统计")
    c.execute("SELECT COUNT(*) FROM daemon_trades WHERE action = 'buy' AND period = (SELECT MAX(period) FROM daemon_account)")
    buy_count = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM daemon_trades WHERE action = 'sell' AND period = (SELECT MAX(period) FROM daemon_account)")
    sell_count = c.fetchone()[0]
    c.execute("SELECT MIN(trade_date) FROM daemon_trades WHERE period = (SELECT MAX(period) FROM daemon_account)")
    first_trade = c.fetchone()[0]
    running_days = (date - datetime.strptime(first_trade or date_str, "%Y-%m-%d")).days + 1 if first_trade else 0
    lines.append(f"- 初始资金: ¥{initial:,}")
    lines.append(f"- 累计盈亏: ¥{total_pnl:+,.0f} ({total_pnl/initial:+.2%})")
    lines.append(f"- 交易次数: 买入{buy_count}笔 / 卖出{sell_count}笔")
    lines.append(f"- 当前持仓: {len(positions)}只")
    lines.append(f"- 运行天数: {running_days}天")
    lines.append("")

    conn.close()
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="生成每日复盘报告")
    parser.add_argument("--date", default=None, help="日期 YYYY-MM-DD, 默认今天")
    args = parser.parse_args()

    date_str = args.date or datetime.now().strftime("%Y-%m-%d")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUT_DIR / f"daily_review_{date_str}.md"

    content = generate(date_str)
    output_path.write_text(content, encoding="utf-8")
    print(f"复盘报告已生成: {output_path}")
    print(f"内容: {len(content)}字符, {content.count(chr(10))}行")


if __name__ == "__main__":
    main()
