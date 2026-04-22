"""CLI 策略命令 — backtest / evolve / scan / list。

用法:
    python -m cli.strategy list
    python -m cli.strategy backtest --name "首板打板_龙头确认" --start 2026-01-01 --end 2026-03-31
    python -m cli.strategy evolve --name "首板打板_龙头确认" --start 2026-01-01 --end 2026-03-31
    python -m cli.strategy scan --date 2026-04-14
"""

import sys
from datetime import datetime

import click
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

from src.data.storage import Storage
from src.strategy.loader import load_strategies, load_strategy_by_name, list_strategy_names
from src.strategy.backtest_engine import BacktestEngine
from src.strategy.evolver import StrategyEvolver


console = Console()


def _get_db() -> Storage:
    db = Storage("data/alpha_miner.db")
    db.init_db()
    return db


@click.group()
def main():
    """Alpha Miner 策略管理。"""
    pass


@main.command("list")
def list_cmd():
    """列出所有预置策略。"""
    strategies = load_strategies()
    if not strategies:
        console.print("[yellow]无预置策略[/yellow]")
        return

    table = Table(title="预置策略库")
    table.add_column("名称", style="cyan", max_width=25)
    table.add_column("标签", style="green")
    table.add_column("来源", style="dim")
    table.add_column("止盈%", justify="right")
    table.add_column("止损%", justify="right")
    table.add_column("最大持仓天", justify="right")
    table.add_column("仓位%", justify="right")

    for s in strategies:
        tags = ", ".join(s.tags) if s.tags else ""
        table.add_row(
            s.name,
            tags,
            s.source or "",
            f"{s.exit.take_profit_pct:.1f}",
            f"{s.exit.stop_loss_pct:.1f}",
            str(s.exit.max_hold_days),
            f"{s.position.single_position_pct:.0f}",
        )

    console.print(table)


@main.command("backtest")
@click.option("--name", required=True, help="策略名称")
@click.option("--start", "start_date", required=True, help="开始日期 YYYY-MM-DD")
@click.option("--end", "end_date", required=True, help="结束日期 YYYY-MM-DD")
@click.option("--db", "db_path", default="data/alpha_miner.db", help="数据库路径")
def backtest_cmd(name: str, start_date: str, end_date: str, db_path: str):
    """回测指定策略。"""
    strategy = load_strategy_by_name(name)
    if not strategy:
        console.print(f"[red]策略不存在: {name}[/red]")
        console.print(f"可用策略: {', '.join(list_strategy_names())}")
        sys.exit(1)

    db = Storage(db_path)
    db.init_db()
    engine = BacktestEngine(db)

    console.print(f"\n[bold cyan]回测: {name}[/bold cyan]")
    console.print(f"区间: {start_date} ~ {end_date}")

    with console.status("回测中..."):
        report = engine.run(strategy, start_date, end_date)

    _print_report(report)


@main.command("evolve")
@click.option("--name", required=True, help="基础策略名称")
@click.option("--start", "start_date", required=True, help="开始日期 YYYY-MM-DD")
@click.option("--end", "end_date", required=True, help="结束日期 YYYY-MM-DD")
@click.option("--objective", default="sharpe", help="优化目标: sharpe/win_rate/profit_loss_ratio/composite")
@click.option("--top", "top_k", default=3, type=int, help="显示前K个改进")
@click.option("--max-variants", default=200, type=int, help="最大变体数")
@click.option("--db", "db_path", default="data/alpha_miner.db", help="数据库路径")
def evolve_cmd(name: str, start_date: str, end_date: str, objective: str,
               top_k: int, max_variants: int, db_path: str):
    """进化策略参数。"""
    strategy = load_strategy_by_name(name)
    if not strategy:
        console.print(f"[red]策略不存在: {name}[/red]")
        sys.exit(1)

    db = Storage(db_path)
    db.init_db()
    evolver = StrategyEvolver(db)

    console.print(f"\n[bold cyan]进化: {name}[/bold cyan]")
    console.print(f"区间: {start_date} ~ {end_date} | 目标: {objective}")

    with console.status("网格搜索中..."):
        result = evolver.evolve(
            strategy, start_date, end_date,
            objective=objective, top_k=top_k, max_variants=max_variants,
        )

    console.print(f"\n共评估 [bold]{result.all_variants}[/bold] 个变体")
    console.print(f"最优策略: [bold green]{result.best_strategy.name}[/bold green]")

    _print_report(result.best_report)

    if result.improvements:
        table = Table(title="Top 改进")
        table.add_column("#", justify="right")
        table.add_column("参数变化", style="cyan")
        table.add_column("得分", justify="right")
        table.add_column("Δ", justify="right")
        table.add_column("胜率", justify="right")
        table.add_column("总收益%", justify="right")
        table.add_column("夏普", justify="right")

        for imp in result.improvements:
            delta_str = f"+{imp['delta']:.4f}" if imp['delta'] > 0 else f"{imp['delta']:.4f}"
            delta_style = "green" if imp['delta'] > 0 else "red"
            table.add_row(
                str(imp["rank"]),
                str(imp["params"]),
                f"{imp['score']:.4f}",
                f"[{delta_style}]{delta_str}[/{delta_style}]",
                f"{imp['win_rate']:.2%}",
                f"{imp['total_return_pct']:.2f}",
                f"{imp['sharpe_ratio']:.2f}",
            )
        console.print(table)


@main.command("scan")
@click.option("--date", "target_date", default=None, help="扫描日期 YYYY-MM-DD (默认今天)")
@click.option("--db", "db_path", default="data/alpha_miner.db", help="数据库路径")
def scan_cmd(target_date: str, db_path: str):
    """用所有预置策略扫描当日信号。"""
    if not target_date:
        target_date = datetime.now().strftime("%Y-%m-%d")

    strategies = load_strategies()
    if not strategies:
        console.print("[yellow]无预置策略[/yellow]")
        return

    db = Storage(db_path)
    db.init_db()
    engine = BacktestEngine(db)

    console.print(f"\n[bold cyan]策略扫描: {target_date}[/bold cyan]")

    any_signal = False
    for s in strategies:
        # 检查每只候选股的入场条件
        from datetime import datetime as dt
        as_of = dt.strptime(target_date, "%Y-%m-%d").replace(hour=15)
        universe = engine._get_universe(target_date, "zt_pool", as_of)

        signals = []
        for code in universe:
            if engine._check_entry(s.entry, code, target_date, as_of):
                signals.append(code)

        if signals:
            any_signal = True
            console.print(f"\n[bold green]▸ {s.name}[/bold green]")
            console.print(f"  命中: {', '.join(signals)}")

    if not any_signal:
        console.print("[yellow]无策略信号[/yellow]")


def _print_report(report):
    """输出回测报告。"""
    console.print(Panel(
        f"[bold]{report.strategy_name}[/bold]\n"
        f"区间: {report.backtest_start} ~ {report.backtest_end}\n"
        f"交易数: {report.total_trades} | 胜率: {report.win_rate:.1%}\n"
        f"均盈: {report.avg_win_pct:.2f}% | 均亏: {report.avg_loss_pct:.2f}%\n"
        f"盈亏比: {report.profit_loss_ratio:.2f}\n"
        f"最大回撤: {report.max_drawdown_pct:.2f}% | 总收益: {report.total_return_pct:.2f}%\n"
        f"夏普: {report.sharpe_ratio:.2f} | 最大连亏: {report.max_consecutive_loss}",
        title="回测报告",
    ))

    if report.regime_stats:
        table = Table(title="Regime 分组")
        table.add_column("Regime", style="cyan")
        table.add_column("交易数", justify="right")
        table.add_column("胜率", justify="right")
        table.add_column("均收益%", justify="right")
        for regime, stats in report.regime_stats.items():
            table.add_row(
                regime,
                str(stats["trades"]),
                f"{stats['win_rate']:.1%}",
                f"{stats['avg_return']:.2f}",
            )
        console.print(table)


if __name__ == "__main__":
    main()
