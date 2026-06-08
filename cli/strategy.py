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

    # 决策A：进化出的入场条件必须过事件研究两段胜率准入门（人工闸门可见）
    from src.strategy.event_study import EventStudy
    gate = EventStudy(db).two_stage_gate(result.best_strategy.entry, end_date)
    _print_gate(f"{result.best_strategy.name} 入场条件", gate)


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


@main.command("event")
@click.option("--factor", "factor", default=None, help="单因子名（与 --name 二选一）")
@click.option("--op", default=">=", help="比较运算符 >=/<=/>/</==")
@click.option("--value", default=0.0, type=float, help="因子阈值")
@click.option("--name", "strat_name", default=None, help="用预置策略的入场条件")
@click.option("--start", "start_date", required=True, help="开始日期 YYYY-MM-DD")
@click.option("--end", "end_date", required=True, help="结束日期 YYYY-MM-DD")
@click.option("--windows", default="1,3,5", help="持有窗口(完整交易日)，逗号分隔")
@click.option("--min-stocks", default=4000, type=int, help="完整交易日最少股票数(剔残采日)")
@click.option("--checkup", is_flag=True, help="对所有已注册因子做体检(用区间中位数为阈值)")
@click.option("--regime-mode", default="emotion",
              type=click.Choice(["emotion", "pricing"]),
              help="分层维度: emotion(情绪) / pricing(定价权:游资vs量化)")
@click.option("--db", "db_path", default="data/alpha_miner.db", help="数据库路径")
def event_cmd(factor, op, value, strat_name, start_date, end_date,
              windows, min_stocks, checkup, regime_mode, db_path):
    """事件研究：因子组合触发样本买入后 T+N 收益分布/胜率/盈亏比。"""
    from src.strategy.event_study import EventStudy, entry_from_factor

    db = Storage(db_path)
    db.init_db()
    win = tuple(int(x) for x in windows.split(","))
    es = EventStudy(db, min_stocks=min_stocks, regime_mode=regime_mode)

    if checkup:
        _run_checkup(es, db, start_date, end_date, win)
        return

    if strat_name:
        strategy = load_strategy_by_name(strat_name)
        if not strategy:
            console.print(f"[red]策略不存在: {strat_name}[/red]")
            sys.exit(1)
        entry, label = strategy.entry, strat_name
    elif factor:
        entry = entry_from_factor(factor, op, value)
        label = f"{factor} {op} {value}"
    else:
        console.print("[red]需指定 --factor 或 --name 或 --checkup[/red]")
        sys.exit(1)

    console.print(f"\n[bold cyan]事件研究: {label}[/bold cyan]  {start_date} ~ {end_date}")
    with console.status("回测中..."):
        report = es.run(entry, start_date, end_date, win, label)
    _print_event(report)


@main.command("gate")
@click.option("--factor", "factor", default=None, help="单因子名（与 --name 二选一）")
@click.option("--op", default=">=", help="比较运算符")
@click.option("--value", default=0.0, type=float, help="因子阈值")
@click.option("--name", "strat_name", default=None, help="用预置策略的入场条件")
@click.option("--end", "end_date", required=True, help="截止日期 YYYY-MM-DD")
@click.option("--threshold", default=0.55, type=float, help="胜率门槛(两段都需≥)")
@click.option("--forward", default=1, type=int, help="持有窗口(完整交易日)")
@click.option("--long-days", default=60, type=int, help="长段交易日数")
@click.option("--short-days", default=30, type=int, help="短段交易日数")
@click.option("--min-stocks", default=4000, type=int, help="完整交易日最少股票数")
@click.option("--db", "db_path", default="data/alpha_miner.db", help="数据库路径")
def gate_cmd(factor, op, value, strat_name, end_date, threshold, forward,
             long_days, short_days, min_stocks, db_path):
    """决策A验收门：入场条件的事件研究两段胜率准入(替代 IC/ICIR 门)。"""
    from src.strategy.event_study import EventStudy, entry_from_factor

    db = Storage(db_path)
    db.init_db()
    es = EventStudy(db, min_stocks=min_stocks)

    if strat_name:
        strategy = load_strategy_by_name(strat_name)
        if not strategy:
            console.print(f"[red]策略不存在: {strat_name}[/red]")
            sys.exit(1)
        entry, label = strategy.entry, strat_name
    elif factor:
        entry = entry_from_factor(factor, op, value)
        label = f"{factor} {op} {value}"
    else:
        console.print("[red]需指定 --factor 或 --name[/red]")
        sys.exit(1)

    g = es.two_stage_gate(entry, end_date, threshold, long_days, short_days, forward)
    _print_gate(label, g)


def _print_gate(label, g):
    """打印验收门结果。"""
    color = "green" if g.passed else "red"
    verdict = "PASS" if g.passed else "FAIL"
    console.print(Panel(
        f"[bold]{label}[/bold]\n"
        f"判定: [{color}]{verdict}[/{color}]  ({g.reason})\n"
        f"长段 T+{g.forward_days} 胜率: {g.wr_long:.1%}  (n={g.n_long}, 盈亏比 {g.pnl_long:.2f})  [{g.long_span}]\n"
        f"短段 T+{g.forward_days} 胜率: {g.wr_short:.1%}  (n={g.n_short})  [{g.short_span}]\n"
        f"门槛: 两段都需 ≥ {g.win_threshold:.0%}",
        title="决策A 事件研究验收门",
    ))


def _print_event(r):
    """打印事件研究报告。"""
    if r.error:
        console.print(f"[yellow]无结果: {r.error}[/yellow]")
        return
    console.print(Panel(f"[bold]{r.label}[/bold]\n样本数: {r.n_signals}", title="事件研究"))

    t = Table(title="持有窗口收益分布")
    for col in ("窗口", "样本", "胜率", "均值", "中位", "盈亏比", "p10", "p90"):
        t.add_column(col, justify="right")
    for w, st in sorted(r.windows.items()):
        t.add_row(f"T+{w}", str(st.n), f"{st.win_rate:.1%}",
                  f"{st.avg_ret:+.2%}", f"{st.median_ret:+.2%}",
                  f"{st.pnl_ratio:.2f}", f"{st.p10:+.2%}", f"{st.p90:+.2%}")
    console.print(t)

    if r.by_segment:
        st = Table(title="分段稳定性(信号日三等分)")
        cols = ["段", "区间", "样本"] + [f"胜率T+{w}" for w in sorted(r.windows)]
        for c in cols:
            st.add_column(c, justify="right")
        for seg in r.by_segment:
            row = [str(seg["seg"]), f"{seg['start']}~{seg['end']}", str(seg["n"])]
            row += [f"{seg.get(f'win_rate_{w}', 0):.0%}" for w in sorted(r.windows)]
            st.add_row(*row)
        console.print(st)

    if len(r.by_regime) > 1:
        rt = Table(title="Regime 分层(T+1)")
        for c in ("Regime", "样本", "胜率", "均值"):
            rt.add_column(c, justify="right")
        w0 = min(r.windows)
        for rg, d in r.by_regime.items():
            s = d[w0]
            rt.add_row(rg, str(s.n), f"{s.win_rate:.1%}", f"{s.avg_ret:+.2%}")
        console.print(rt)


def _run_checkup(es, db, start_date, end_date, win):
    """对所有已注册因子各取区间中位数为阈值，做事件研究体检。"""
    import numpy as np
    from src.factors.registry import FactorRegistry
    from src.strategy.event_study import entry_from_factor

    console.print(f"\n[bold cyan]alpha 因子体检[/bold cyan]  {start_date} ~ {end_date}")
    console.print("[dim]阈值=各因子区间中位数(>=)，含轻微 look-ahead，仅诊断哪些因子还活着。"
                  "filter 角色因子(如 theme_crowding)不在此 alpha 体检。[/dim]")

    table = Table(title="alpha 因子体检 (条件: 因子值 >= 区间中位数)")
    for c in ("因子", "阈值", "样本", "胜率T+1", "均值T+1", "盈亏比T+1", "胜率T+3", "胜率T+5"):
        table.add_column(c, justify="right")

    for name in FactorRegistry().list_factors(role="alpha"):
        rows = db.execute(
            "SELECT factor_value AS v FROM factor_values "
            "WHERE factor_name = ? AND trade_date >= ? AND trade_date <= ? "
            "AND factor_value IS NOT NULL",
            (name, start_date, end_date),
        )
        vals = [r["v"] for r in rows]
        if not vals:
            table.add_row(name, "-", "0", "-", "-", "-", "-", "-")
            continue
        thr = float(np.median(vals))
        r = es.run(entry_from_factor(name, ">=", thr), start_date, end_date, win, name)
        if r.error or not r.windows:
            table.add_row(name, f"{thr:.3f}", str(r.n_signals), "-", "-", "-", "-", "-")
            continue
        w1 = r.windows.get(min(win))
        w3 = r.windows.get(3)
        w5 = r.windows.get(5)
        table.add_row(
            name, f"{thr:.3f}", str(r.n_signals),
            f"{w1.win_rate:.1%}" if w1 else "-",
            f"{w1.avg_ret:+.2%}" if w1 else "-",
            f"{w1.pnl_ratio:.2f}" if w1 else "-",
            f"{w3.win_rate:.1%}" if w3 else "-",
            f"{w5.win_rate:.1%}" if w5 else "-",
        )
    console.print(table)


if __name__ == "__main__":
    main()
