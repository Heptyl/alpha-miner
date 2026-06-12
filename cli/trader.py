"""交易计划CLI — 路线B半自动

用法:
    uv run python -m cli.trader plan     # 生成今日交易计划
    uv run python -m cli.trader status   # 查看持仓状态
    uv run python -m cli.trader history  # 查看计划历史
"""

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, ".")

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PLAN_PATH = PROJECT_ROOT / "output" / "trader" / "daily_plan.json"


def cmd_plan(_args):
    """生成今日交易计划"""
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel

    from src.trader.plan_generator import generate_daily_plan

    console = Console()
    console.print("[bold cyan]═══════════════════════════════════════[/]")
    console.print("[bold cyan]         每日交易计划生成器          [/]")
    console.print("[bold cyan]═══════════════════════════════════════[/]\n")

    plan = generate_daily_plan()

    # 日期和摘要
    console.print(f"[bold]日期: {plan.date}[/]")
    console.print(f"[dim]{plan.summary()}[/]\n")

    # === 卖出指令 ===
    if plan.sells:
        console.print(Panel("[bold red]卖出指令[/]", style="red"))
        table = Table(show_lines=True)
        table.add_column("!", style="bold red", width=3)
        table.add_column("代码", style="cyan", width=8)
        table.add_column("名称", style="white", width=8)
        table.add_column("数量", justify="right", width=8)
        table.add_column("参考价", justify="right", width=10)
        table.add_column("金额", justify="right", width=12)
        table.add_column("原因", style="yellow")

        for s in plan.sells:
            p = "!!" if s.priority == 1 else " !"
            table.add_row(
                p, s.code, s.name,
                f"{s.shares}", f"{s.price:.2f}",
                f"¥{s.est_amount:,.0f}", s.reason,
            )
        console.print(table)
        console.print()
    else:
        console.print("[dim]  无卖出操作[/]\n")

    # === 买入指令 ===
    if plan.buys:
        console.print(Panel("[bold green]买入指令[/]", style="green"))
        table = Table(show_lines=True)
        table.add_column("序", style="dim", width=3)
        table.add_column("代码", style="cyan", width=8)
        table.add_column("名称", style="white", width=8)
        table.add_column("数量", justify="right", width=8)
        table.add_column("参考价", justify="right", width=10)
        table.add_column("金额", justify="right", width=12)
        table.add_column("得分", justify="right", width=8)
        table.add_column("原因", style="yellow")

        for i, b in enumerate(plan.buys, 1):
            table.add_row(
                str(i), b.code, b.name,
                f"{b.shares}", f"{b.price:.2f}",
                f"¥{b.est_amount:,.0f}",
                f"{b.score:.4f}", b.reason,
            )
        console.print(table)
        console.print()
    else:
        console.print("[dim]  无买入操作[/]\n")

    # === 当前持仓 ===
    console.print(Panel("[bold]当前持仓[/]", style="blue"))
    table = Table(show_lines=True)
    table.add_column("代码", style="cyan", width=8)
    table.add_column("名称", style="white", width=8)
    table.add_column("持仓", justify="right", width=8)
    table.add_column("成本", justify="right", width=10)
    table.add_column("现价", justify="right", width=10)
    table.add_column("盈亏%", justify="right", width=10)
    table.add_column("状态", style="bold", width=6)

    for h in plan.holds:
        pnl_style = "red" if h["pnl_pct"] > 0 else "green" if h["pnl_pct"] < 0 else "white"
        status = "[red]卖出[/]" if h["sold"] else "[green]持有[/]"
        table.add_row(
            h["code"], h["name"], f"{h['shares']}",
            f"{h['cost']:.2f}", f"{h['price']:.2f}",
            f"[{pnl_style}]{h['pnl_pct']:+.1f}%[/]", status,
        )
    console.print(table)
    console.print()

    # === 资金概况 ===
    console.print(Panel(
        f"交易前现金: [yellow]¥{plan.cash_before:,.0f}[/]\n"
        f"卖出回笼:   [green]+¥{sum(s.est_amount for s in plan.sells):,.0f}[/]\n"
        f"买入花费:   [red]-¥{sum(b.est_amount for b in plan.buys):,.0f}[/]\n"
        f"交易后现金: [bold]¥{plan.cash_after:,.0f}[/]",
        title="资金变化",
    ))

    if plan.cash_after < 0:
        console.print("[bold red]⚠ 现金不足! 买入计划超出可用资金，请调整[/]")

    for note in plan.notes:
        console.print(f"[dim]  {note}[/]")

    console.print(f"\n[dim]计划已保存: {PLAN_PATH}[/]")


def cmd_status(_args):
    """查看持仓状态"""
    from rich.console import Console
    from rich.table import Table

    from src.trader.plan_generator import PORTFOLIO, get_current_prices

    console = Console()
    console.print("[bold cyan]账户状态[/]\n")

    codes = list(PORTFOLIO.keys())
    prices = get_current_prices(codes)

    table = Table(show_lines=True)
    table.add_column("代码", style="cyan")
    table.add_column("名称", style="white")
    table.add_column("持仓", justify="right")
    table.add_column("成本", justify="right")
    table.add_column("现价", justify="right")
    table.add_column("市值", justify="right")
    table.add_column("盈亏", justify="right")
    table.add_column("止损线", justify="right")

    total_mv = 0
    total_pnl = 0
    for code, info in PORTFOLIO.items():
        cur = prices.get(code, info["cost"])
        mv = cur * info["shares"]
        pnl = (cur - info["cost"]) * info["shares"]
        pnl_pct = (cur / info["cost"] - 1) * 100 if info["cost"] > 0 else 0
        total_mv += mv
        total_pnl += pnl
        style = "red" if pnl > 0 else "green" if pnl < 0 else "white"
        table.add_row(
            code, info["name"], f"{info['shares']}",
            f"{info['cost']:.3f}", f"{cur:.2f}",
            f"¥{mv:,.0f}", f"[{style}]¥{pnl:+,.0f} ({pnl_pct:+.1f}%)[/]",
            f"{info.get('stop_loss', 0):.2f}",
        )

    console.print(table)
    console.print(f"\n总市值: [bold]¥{total_mv:,.0f}[/]")
    console.print(f"总盈亏: [bold]¥{total_pnl:+,.0f}[/]")
    console.print(f"可用现金: [bold]¥10,189[/]")


def cmd_history(_args):
    """查看计划历史"""
    from rich.console import Console

    console = Console()
    if PLAN_PATH.exists():
        data = json.loads(PLAN_PATH.read_text())
        console.print(f"[bold]最近计划: {data.get('date', '?')}[/]")
        console.print(f"摘要: {data.get('summary', '?')}")
    else:
        console.print("[yellow]暂无历史计划[/]")


def cmd_simback(args):
    """运行模拟盘回放"""
    from rich.console import Console
    from src.trader.paper_trader import init_tables, reset_simulation, run_simulation_backtest_v2

    console = Console()
    days = getattr(args, "days", 30)
    console.print(f"[bold cyan]════ 模拟盘回放 ({days}天) ════[/]\\n")

    init_tables()
    reset_simulation()

    result = run_simulation_backtest_v2(days=days)
    stats = result.get("stats", {})

    console.print(f"回放天数: {stats.get('total_days', '?')}")
    console.print(f"总交易数: {stats.get('total_trades', 0)}")
    console.print(f"胜率:     [bold]{stats.get('win_rate', '?')}%[/]")
    console.print(f"总盈亏:   ¥{stats.get('total_pnl', 0):+,.0f}")
    console.print(f"收益率:   {stats.get('cumulative_return', 0):+.2f}%")
    console.print(f"Sharpe:   {stats.get('sharpe', '?')}")
    console.print(f"最大回撤: {stats.get('max_drawdown', '?')}%")
    console.print(f"最终净值: ¥{stats.get('final_total', '?'):,.2f}")

    # 最近5笔
    trades = result.get("trades", [])
    if trades:
        console.print(f"\\n[bold]最近交易 (共{len(trades)}笔):[/]")
        for t in trades[-5:]:
            pnl_pct = t.get("pnl_pct", 0) * 100
            style = "green" if pnl_pct > 0 else "red"
            console.print(
                f"  {t.get('buy_date', '?')}→{t.get('sell_date', '?')} "
                f"{t.get('code', '?')} {t.get('name', ''):6s} "
                f"[{style}]{pnl_pct:+.1f}%[/] ({t.get('reason', '')})"
            )


def cmd_simstats(_args):
    """查看模拟盘统计"""
    from rich.console import Console
    from rich.table import Table
    from src.trader.paper_trader import get_simulation_stats

    console = Console()
    stats = get_simulation_stats()

    acct = stats.get("account", {})
    console.print("[bold cyan]════ 模拟盘统计 ════[/]\\n")
    console.print(f"最新日期: {acct.get('date', '?')}")
    console.print(f"现金:     ¥{acct.get('cash', 0):,.0f}")
    console.print(f"市值:     ¥{acct.get('market_value', 0):,.0f}")
    console.print(f"总资产:   ¥{acct.get('total_assets', 0):,.0f}")
    console.print(f"累计收益: {acct.get('cumulative_return', '?')}")
    console.print(f"交易次数: {stats.get('total_trades', 0)}")
    console.print(f"胜率:     {stats.get('win_rate', '?')}")
    console.print(f"Sharpe:   {stats.get('sharpe', '?')}")
    console.print(f"最大回撤: {stats.get('max_drawdown', '?')}")

    # 最近交易
    recent = stats.get("recent_trades", [])
    if recent:
        table = Table(title="最近交易", show_lines=True)
        table.add_column("日期", width=12)
        table.add_column("操作", width=4)
        table.add_column("代码", width=8)
        table.add_column("名称", width=8)
        table.add_column("盈亏%", width=8, justify="right")
        table.add_column("原因")
        for t in recent[:10]:
            pnl = t.get("pnl_pct", 0)
            style = "green" if pnl > 0 else "red"
            table.add_row(
                t.get("date", "?"),
                t.get("action", "?"),
                t.get("code", "?"),
                t.get("name", ""),
                f"[{style}]{pnl*100:+.1f}%[/]",
                t.get("reason", ""),
            )
        console.print(table)


def main():
    parser = argparse.ArgumentParser(description="交易计划")
    sub = parser.add_subparsers(dest="command")
    sub.add_parser("plan", help="生成交易计划")
    sub.add_parser("status", help="查看持仓")
    sub.add_parser("history", help="计划历史")
    sb = sub.add_parser("simback", help="模拟盘回放")
    sb.add_argument("--days", type=int, default=30, help="回放天数")
    sub.add_parser("simstats", help="模拟盘统计")

    args = parser.parse_args()
    if args.command == "plan":
        cmd_plan(args)
    elif args.command == "status":
        cmd_status(args)
    elif args.command == "history":
        cmd_history(args)
    elif args.command == "simback":
        cmd_simback(args)
    elif args.command == "simstats":
        cmd_simstats(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
