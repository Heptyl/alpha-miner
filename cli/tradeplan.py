"""实操交易计划生成器 — python -m cli tradeplan

基于20天1181只涨停样本回测的最优策略:
  筛选: 首板封住 + 成交额>3亿 + 开板次数<=3
  买入: 次日9:40~10:30 等盘中回踩低吸(跌幅>0.5%时买入)
  止损: 买入价 -3%
  止盈: 买入价 +5%
  仓位: 每只35%资金(Half-Kelly)，同时最多2只

回测数据:
  平均收益 +2.66% | 胜率 80% | 盈亏比 2.1
  止损3%/止盈5%: 均收益 +2.30% 胜率 79%

用法:
  python -m cli tradeplan                    # 生成次日交易计划
  python -m cli tradeplan --date 2026-05-06  # 指定日期
  python -m cli tradeplan --push             # 推送到微信
  python -m cli tradeplan --capital 100000   # 指定资金量(默认10万)
"""

import argparse
import json
import sqlite3
import urllib.request
from datetime import datetime
from pathlib import Path

from rich.console import Console
from rich.panel import Panel
from rich.table import Table

console = Console()

# ── 策略参数 (回测验证) ──
MIN_AMOUNT = 3e8          # T日成交额 >= 3亿
MAX_OPEN_COUNT = 3        # T日开板次数 <= 3
STOP_LOSS_PCT = 3.0       # 止损 3%
TAKE_PROFIT_PCT = 5.0     # 止盈 5%
POSITION_PCT = 35         # 单只仓位 35%
MAX_POSITIONS = 2          # 最多同时持仓 2 只
BUY_WINDOW = "09:40-10:30" # 买入时段
BUY_DIP_PCT = 0.5         # 盘中跌 >0.5% 时低吸


def generate_plan(report_date: str, db_path: str, capital: float) -> dict:
    """生成次日交易计划。
    
    买入价算法（基于首板次日表现统计）:
    - 首板次日约52%继续涨停(买不到)，48%打开
    - 打开的票平均低开-3.7%，盘中最低-5.5%
    - 因此买入价应基于次日预估开盘价，而非T日收盘
    - 按封板质量调整: 封死→次日可能高开→买入价适当提高
                       开板→次日大概率低开→买入价要更低
    """
    conn = sqlite3.connect(db_path)

    # 1. 从涨停池筛选候选
    candidates = conn.execute("""
        SELECT z.stock_code, z.name, z.consecutive_zt, z.open_count, 
               z.industry, z.amount, z.circulation_mv,
               dp.close, dp.open, dp.high, dp.low, dp.volume, dp.pre_close
        FROM zt_pool z
        JOIN daily_price dp ON z.stock_code = dp.stock_code AND dp.trade_date = z.trade_date
        WHERE z.trade_date = ?
        AND z.consecutive_zt = 1
        AND z.open_count <= ?
        AND (z.stock_code LIKE '000%' OR z.stock_code LIKE '001%' 
             OR z.stock_code LIKE '002%' OR z.stock_code LIKE '003%'
             OR z.stock_code LIKE '300%' OR z.stock_code LIKE '301%'
             OR z.stock_code LIKE '600%' OR z.stock_code LIKE '601%'
             OR z.stock_code LIKE '603%' OR z.stock_code LIKE '605%')
        ORDER BY z.amount DESC
    """, (report_date, MAX_OPEN_COUNT)).fetchall()

    # 2. 严格过滤 + 计算次日预估买入价
    filtered = []
    for row in candidates:
        code, name, cons_zt, open_cnt, industry, amt, cmv, close, o, h, l, vol, pre = row
        if not close or not pre or pre == 0:
            continue

        # 确认涨停封住
        t_pct = (close - pre) / pre * 100
        if t_pct < 9.5:
            continue

        # 成交额过滤
        if (amt or 0) < MIN_AMOUNT:
            continue

        # ── 核心改进: 基于封板质量预估次日开盘区间 ──
        # 统计规律:
        #   封死(open=0): 次日约50%继续涨停, 打开的平均低开-2~-3%
        #   开板(open>0): 次日约60%打开, 平均低开-4~-5%
        # 策略: 买入价 = 次日可能回调到的价格(不是T日收盘)
        
        if open_cnt == 0:
            # 封死: 次日可能高开+1~2%后回调, 买入参考 = T收*(1+0%~+1%)
            # 但封死的有一半继续涨停(买不到), 实际能买到的往往低开
            # 保守估计: 次日开盘在 T收~-2% 之间, 盘中可能到 -3%
            est_open_pct = 0.0       # 预估开盘 ≈ T收平开
            buy_dip_from_open = 1.5  # 从开盘再低吸1.5%
        elif open_cnt <= 2:
            # 开1-2次: 次日大概率低开-2~-4%
            est_open_pct = -2.0      # 预估低开-2%
            buy_dip_from_open = 1.0  # 从开盘再低吸1%
        else:
            # 开3次: 次日几乎必低开-3%以上
            est_open_pct = -3.0      # 预估低开-3%
            buy_dip_from_open = 0.5  # 从开盘再低吸0.5%
        
        # 预估次日开盘价
        est_open = round(close * (1 + est_open_pct / 100), 2)
        # 目标买入价 = 预估开盘后再低吸
        entry_target = round(est_open * (1 - buy_dip_from_open / 100), 2)
        # 最高买入价 = 不追高超预估开盘+1%
        entry_max = round(est_open * 1.01, 2)
        # 最低买入价 = 预估开盘-3% (低于此说明太弱不买)
        entry_min = round(est_open * 0.97, 2)
        
        # 止损/止盈基于买入价
        stop_price = round(entry_target * (1 - STOP_LOSS_PCT / 100), 2)
        target_price = round(entry_target * (1 + TAKE_PROFIT_PCT / 100), 2)

        # 仓位计算
        shares = int(capital * POSITION_PCT / 100 / entry_target / 100) * 100  # 整百股
        actual_cost = shares * entry_target

        filtered.append({
            'code': code, 'name': name, 'industry': industry or '',
            'close': close, 't_pct': round(t_pct, 1),
            'open_cnt': open_cnt, 'amount': amt or 0,
            'est_open': est_open,
            'entry_target': entry_target, 'entry_max': entry_max, 'entry_min': entry_min,
            'stop_price': stop_price, 'target_price': target_price,
            'shares': shares, 'cost': round(actual_cost, 0),
            'risk_amt': round(shares * (entry_target - stop_price), 0),
            'profit_amt': round(shares * (target_price - entry_target), 0),
        })

    # 3. 按综合质量排序（成交额 + 封板质量）
    filtered.sort(key=lambda x: (-x['amount'], x['open_cnt']))

    # 取前 MAX_POSITIONS * 2 只（给出备选）
    top = filtered[:MAX_POSITIONS]
    backup = filtered[MAX_POSITIONS:MAX_POSITIONS * 2]

    conn.close()

    return {
        'report_date': report_date,
        'target_date': _next_trade_day(report_date),
        'capital': capital,
        'top': top,
        'backup': backup,
        'strategy': {
            'stop_loss': STOP_LOSS_PCT,
            'take_profit': TAKE_PROFIT_PCT,
            'position_pct': POSITION_PCT,
            'max_positions': MAX_POSITIONS,
            'buy_window': BUY_WINDOW,
            'buy_dip_pct': BUY_DIP_PCT,
        },
    }


def _next_trade_day(date_str: str) -> str:
    """简单推算次日（跳周末）。"""
    from datetime import timedelta
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    for _ in range(5):
        dt += timedelta(days=1)
        if dt.weekday() < 5:
            return dt.strftime("%Y-%m-%d")
    return date_str


def _print_plan(plan: dict):
    """Rich 终端输出交易计划。"""
    target = plan['target_date']
    s = plan['strategy']

    console.print()
    console.print(Panel(
        f"[bold]交易日期: {target}[/bold]\n"
        f"资金: {plan['capital']:,.0f}元 | "
        f"单只仓位: {s['position_pct']}% | "
        f"最多持仓: {s['max_positions']}只",
        title=f"[bold cyan]次日交易计划 — 基于 {plan['report_date']} 收盘[/bold cyan]",
        border_style="cyan",
    ))

    if not plan['top']:
        console.print("\n[yellow]今日无符合条件的候选股[/yellow]")
        return

    # 主选
    table = Table(
        title=f"★ 主选标的 ({len(plan['top'])}只)",
        show_lines=True, border_style="green",
    )
    table.add_column("#", width=2)
    table.add_column("代码", style="bold", width=8)
    table.add_column("名称", style="cyan", width=10)
    table.add_column("板块", width=8)
    table.add_column("T日涨", justify="right", width=6)
    table.add_column("开板", justify="right", width=4)
    table.add_column("成交额", justify="right", width=10)
    table.add_column("预估开", style="dim", justify="right", width=8)
    table.add_column("目标买价", style="bold yellow", justify="right", width=8)
    table.add_column("最高买价", justify="right", width=8)
    table.add_column("止损", style="red", justify="right", width=8)
    table.add_column("止盈", style="green", justify="right", width=8)
    table.add_column("股数", justify="right", width=6)
    table.add_column("成本", justify="right", width=8)
    table.add_column("风险", style="red", justify="right", width=8)
    table.add_column("盈利", style="green", justify="right", width=8)

    for i, st in enumerate(plan['top'], 1):
        table.add_row(
            str(i), st['code'], st['name'], st['industry'][:6],
            f"{st['t_pct']:.1f}%", str(st['open_cnt']),
            f"{st['amount']/1e8:.1f}亿",
            f"{st.get('est_open', st['close']):.2f}",
            f"{st['entry_target']:.2f}", f"{st['entry_max']:.2f}",
            f"{st['stop_price']:.2f}", f"{st['target_price']:.2f}",
            str(st['shares']), f"{st['cost']:,.0f}",
            f"{st['risk_amt']:,.0f}", f"{st['profit_amt']:,.0f}",
        )
    console.print(table)

    # 操作指南
    console.print(Panel(
        f"[bold]操作纪律:[/bold]\n"
        f"  1. 买入时段: {s['buy_window']}，等盘中回踩 >{s['buy_dip_pct']}% 时低吸\n"
        f"  2. 不追高: 价格超过 [red]{plan['top'][0]['entry_max']:.2f}[/red] 放弃\n"
        f"  3. 买入后挂止损单: [red]{s['stop_loss']}%[/red] (约亏 {plan['top'][0]['risk_amt']:,.0f}元)\n"
        f"  4. 止盈目标: [green]{s['take_profit']}%[/green] (约赚 {plan['top'][0]['profit_amt']:,.0f}元)\n"
        f"  5. 如果 {s['buy_window']} 内没跌到目标价，[yellow]放弃不买[/yellow]\n"
        f"  6. 收盘前未达止盈，[yellow]卖出观望[/yellow]",
        title="[bold]执行规则[/bold]",
        border_style="yellow",
    ))

    # 备选
    if plan['backup']:
        table2 = Table(title=f"○ 备选标的", show_lines=False, border_style="dim")
        table2.add_column("代码", width=8)
        table2.add_column("名称", width=10)
        table2.add_column("板块", width=8)
        table2.add_column("成交额", justify="right", width=10)
        table2.add_column("目标买价", justify="right", width=8)
        table2.add_column("止损", justify="right", width=8)
        table2.add_column("止盈", justify="right", width=8)
        for st in plan['backup']:
            table2.add_row(
                st['code'], st['name'], st['industry'][:6],
                f"{st['amount']/1e8:.1f}亿",
                f"{st['entry_target']:.2f}", f"{st['stop_price']:.2f}",
                f"{st['target_price']:.2f}",
            )
        console.print(table2)

    # 回测数据
    console.print(Panel(
        "[dim]策略回测数据 (20天1181只样本):\n"
        "  平均收益 +2.66% | 胜率 80% | 盈亏比 2.1\n"
        "  止损3%/止盈5%: 均收益 +2.30% 胜率 79%\n"
        "  ⚠ 历史数据不代表未来收益[/dim]",
        border_style="dim",
    ))


def _format_push_message(plan: dict) -> str:
    """格式化微信推送消息。"""
    if not plan['top']:
        return f"【交易计划 {plan['target_date']}】\n今日无符合条件的候选"

    s = plan['strategy']
    lines = [f"【交易计划 {plan['target_date']}】"]
    lines.append(f"资金{plan['capital']/1e4:.0f}万 仓位{s['position_pct']}% 止损{s['stop_loss']}% 止盈{s['take_profit']}%")

    for i, st in enumerate(plan['top'], 1):
        lines.append(
            f"{i}. {st['code']} {st['name']} {st['industry'][:4]}\n"
            f"  买{st['entry_target']:.2f}(≤{st['entry_max']:.2f}) "
            f"止{st['stop_price']:.2f} 盈{st['target_price']:.2f}\n"
            f"  {st['shares']}股 成本{st['cost']/1e4:.1f}万"
        )

    lines.append(f"\n操作: {s['buy_window']}等回踩低吸")
    lines.append("不追高! 超过最高买价放弃")
    lines.append("⚠ 仅供参考，不构成投资建议")
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="Alpha Miner 次日交易计划")
    parser.add_argument("--date", type=str, default=None, help="推荐日期(默认今天)")
    parser.add_argument("--db", type=str, default="data/alpha_miner.db", help="数据库路径")
    parser.add_argument("--capital", type=float, default=100000, help="总资金(默认10万)")
    parser.add_argument("--push", action="store_true", help="推送到微信")
    args = parser.parse_args()

    report_date = args.date or datetime.now().strftime("%Y-%m-%d")
    plan = generate_plan(report_date, args.db, args.capital)
    _print_plan(plan)

    # 保存
    Path("recommendations").mkdir(exist_ok=True)
    save_path = f"recommendations/{plan['target_date']}_tradeplan.json"
    Path(save_path).write_text(
        json.dumps(plan, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    console.print(f"\n[dim]交易计划已保存: {save_path}[/dim]")

    # 推送
    if args.push:
        msg = _format_push_message(plan)
        try:
            from src.drift.push import push_message_sync
            result = push_message_sync(msg)
            if result.get("success"):
                console.print("[green]✓ 推送成功[/green]")
            else:
                console.print(f"[red]✗ 推送失败: {result}[/red]")
        except Exception as e:
            console.print(f"[red]✗ 推送失败: {e}[/red]")


if __name__ == "__main__":
    main()
