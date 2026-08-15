"""CLI for limit-up data enrichment, structural evolution, and action cards."""

import json
import time
from datetime import datetime
from pathlib import Path

import click
from rich.console import Console
from rich.table import Table

from src.data.collector import collect_date
from src.data.sources import akshare_zt_pool
from src.data.storage import Storage
from src.mining.limit_up_evolution import (
    FEATURE_LABELS,
    LimitUpEvolutionEngine,
    describe_genome,
    describe_rule,
)

console = Console()


@click.group()
def main():
    """涨停板专用研究与操作卡。"""


@main.command("daily")
@click.option("--skip-collect", is_flag=True, help="数据已由定时任务更新时跳过采集")
@click.option("--db", "db_path", default="data/alpha_miner.db", show_default=True)
@click.option("--state", "state_path", default="data/limit_up_evolution.json", show_default=True)
def daily_cmd(skip_collect: bool, db_path: str, state_path: str):
    """用户入口：盘后更新数据、计算因子并输出次日操作卡。"""
    db = Storage(db_path)
    db.init_db()
    today = datetime.now().strftime("%Y-%m-%d")
    if not skip_collect:
        console.print(f"[bold cyan]1/3 更新 {today} 数据[/bold cyan]")
        counts = collect_date(today, db)
        console.print(f"采集完成：{sum(counts.values())} 条")

    compute_step = "1/2" if skip_collect else "2/3"
    card_step = "2/2" if skip_collect else "3/3"
    console.print(f"[bold cyan]{compute_step} 计算当前因子[/bold cyan]")
    from cli.backtest import compute_today

    compute_today(db_path)
    console.print(f"[bold cyan]{card_step} 生成涨停板次日操作卡[/bold cyan]")
    engine = LimitUpEvolutionEngine(db_path=db_path, state_path=state_path)
    try:
        cards = engine.action_cards()
    except FileNotFoundError:
        console.print(
            "[red]尚无涨停专项演化状态，请由维护流程先运行 `python -m cli zt evolve`。[/red]"
        )
        return
    latest_rows = db.execute("SELECT MAX(trade_date) AS d FROM zt_pool")
    signal_date = latest_rows[0]["d"] if latest_rows else None
    console.print(f"信号数据日：{signal_date or '缺失'}")
    if datetime.now().weekday() < 5 and signal_date != today:
        console.print("[red]警告：信号不是今天的数据，不得据此操作。[/red]")
    _print_cards(cards)


@main.command("status")
@click.option("--db", "db_path", default="data/alpha_miner.db", show_default=True)
@click.option("--state", "state_path", default="data/limit_up_evolution.json", show_default=True)
def status_cmd(db_path: str, state_path: str):
    """用一张表说明数据、验证和实盘闸门是否就绪。"""
    db = Storage(db_path)
    db.init_db()
    rows = db.execute(
        "SELECT (SELECT MAX(trade_date) FROM daily_price) AS price_date, "
        "(SELECT MAX(trade_date) FROM zt_pool) AS zt_date"
    )
    latest = rows[0] if rows else {}
    state_file = Path(state_path)
    state = json.loads(state_file.read_text(encoding="utf-8")) if state_file.exists() else {}
    summary = state.get("dataset_summary", {})
    best = state.get("best") or {}
    accepted = bool(best.get("accepted"))
    table = Table(title="涨停因子可用状态")
    table.add_column("检查项")
    table.add_column("当前值")
    table.add_row("价格数据", str(latest.get("price_date") or "缺失"))
    table.add_row("涨停池", str(latest.get("zt_date") or "缺失"))
    table.add_row(
        "有效信号日",
        f"{summary.get('signal_dates', 0)} / {summary.get('minimum_signal_dates', 40)}",
    )
    table.add_row("候选因子", best.get("genome", {}).get("name", "尚未演化"))
    table.add_row(
        "实盘闸门", "[green]已通过[/green]" if accepted else "[yellow]未通过，只观察[/yellow]"
    )
    console.print(table)
    if best.get("rejection_reasons"):
        console.print("未准入原因：" + "；".join(best["rejection_reasons"]))


@main.command("enrich")
@click.option("--start", default=None, help="起始日期 YYYY-MM-DD")
@click.option("--end", default=None, help="结束日期 YYYY-MM-DD")
@click.option("--min-market-rows", default=100, type=int, show_default=True)
@click.option("--db", "db_path", default="data/alpha_miner.db", show_default=True)
def enrich_cmd(start: str | None, end: str | None, min_market_rows: int, db_path: str):
    """按已有价格日历补齐涨停池，并保存封板资金/时间/换手等基因。"""
    db = Storage(db_path)
    db.init_db()
    rows = db.execute(
        "SELECT trade_date AS d, COUNT(DISTINCT stock_code) AS n "
        "FROM daily_price GROUP BY trade_date HAVING n >= ? ORDER BY trade_date",
        (min_market_rows,),
    )
    dates = [
        row["d"]
        for row in rows
        if (not start or row["d"] >= start) and (not end or row["d"] <= end)
    ]
    total = 0
    success = 0
    for index, date in enumerate(dates, 1):
        frame = akshare_zt_pool.fetch_zt_pool(date, retries=2)
        if frame.empty:
            console.print(f"[{index}/{len(dates)}] {date}: [yellow]empty[/yellow]")
            continue
        count = db.insert("zt_pool", frame, dedup=True)
        success += 1
        total += count
        console.print(f"[{index}/{len(dates)}] {date}: [green]{count}[/green]")
        time.sleep(0.15)
    console.print(f"完成：{success}/{len(dates)} 个交易日，{total} 条涨停事件")


@main.command("evolve")
@click.option("--generations", default=5, type=int, show_default=True)
@click.option("--population", default=24, type=int, show_default=True)
@click.option("--min-signal-dates", default=40, type=int, show_default=True)
@click.option("--min-market-rows", default=100, type=int, show_default=True)
@click.option("--db", "db_path", default="data/alpha_miner.db", show_default=True)
@click.option("--state", "state_path", default="data/limit_up_evolution.json", show_default=True)
def evolve_cmd(
    generations: int,
    population: int,
    min_signal_dates: int,
    min_market_rows: int,
    db_path: str,
    state_path: str,
):
    """以次日可成交、T+1/T+2 收益为目标进化结构性涨停因子。"""
    engine = LimitUpEvolutionEngine(
        db_path=db_path,
        state_path=state_path,
        min_market_rows=min_market_rows,
        min_signal_dates=min_signal_dates,
    )
    outcome = engine.run(generations=generations, population_size=population)
    summary = outcome.dataset_summary
    console.print(
        f"数据：{summary.get('signal_dates', 0)} 个有效信号日 / "
        f"{summary.get('events', 0)} 个涨停事件；"
        f"准入要求 {summary.get('minimum_signal_dates', min_signal_dates)} 日"
    )
    _print_feature_quality(summary)
    if not outcome.evaluations:
        console.print(f"[red]没有可评估候选：{summary.get('error', '数据不足')}[/red]")
        return

    console.rule("研发候选 Top 5（锁定测试不参与选优）")
    for rank, item in enumerate(outcome.evaluations[:5], 1):
        genome = item.genome
        conclusion = "[green]可操作[/green]" if item.accepted else "[yellow]研究中[/yellow]"
        console.print(f"[cyan]{rank}. {genome.name}[/cyan]（{genome.source}） {conclusion}")
        console.print(f"   结构：{describe_genome(genome)}")
        console.print(f"   规则：{describe_rule(genome)}")
        console.print(
            "   证据："
            f"训练 {item.train.trades}笔 {item.train.avg_return:+.2f}%/{item.train.win_rate:.0%}；"
            f"验证 {item.validation.trades}笔 {item.validation.avg_return:+.2f}%/"
            f"{item.validation.win_rate:.0%}；测试 {item.test.trades}笔 "
            f"{item.test.avg_return:+.2f}%/{item.test.win_rate:.0%}，"
            f"盈亏比 {item.test.pnl_ratio:.2f}"
        )
    benchmark = next(
        (
            item
            for item in outcome.evaluations
            if item.genome.name == "zt_first_board_controlled_reseal"
        ),
        None,
    )
    if benchmark and benchmark not in outcome.evaluations[:5]:
        console.rule("可解释研究基准")
        console.print("[cyan]首板有限分歧回封[/cyan]（由当前样本发现，等待未来数据验证）")
        console.print(f"结构：{describe_genome(benchmark.genome)}")
        console.print(f"规则：{describe_rule(benchmark.genome)}")
        console.print(
            f"证据：训练 {benchmark.train.trades}笔 {benchmark.train.avg_return:+.2f}%/"
            f"{benchmark.train.win_rate:.0%}；验证 {benchmark.validation.trades}笔 "
            f"{benchmark.validation.avg_return:+.2f}%/{benchmark.validation.win_rate:.0%}；"
            f"测试 {benchmark.test.trades}笔 {benchmark.test.avg_return:+.2f}%/"
            f"{benchmark.test.win_rate:.0%}，盈亏比 {benchmark.test.pnl_ratio:.2f}"
        )
        console.print("未准入原因：" + "；".join(benchmark.rejection_reasons))
    best = outcome.best
    if best:
        label = "已准入因子" if best.accepted else "研发候选（不可操作）"
        console.print(f"{label}：{best.genome.name} | fitness={best.fitness:.3f}")
        console.print(f"结构：{describe_genome(best.genome)}")
        console.print(f"规则：{describe_rule(best.genome)}")
        if best.rejection_reasons:
            console.print("未准入原因：" + "；".join(best.rejection_reasons))
    _print_cards(engine.action_cards())


def _print_feature_quality(summary: dict) -> None:
    excluded_dates = summary.get("excluded_non_trading_dates", [])
    if excluded_dates:
        console.print(
            "[yellow]已忽略非交易日污染：[/yellow]" + "、".join(excluded_dates)
        )
    coverage = summary.get("source_coverage", {})
    if coverage:
        console.print(
            "历史原始字段覆盖："
            + "；".join(f"{name} {value:.0%}" for name, value in coverage.items())
        )
    inactive = summary.get("inactive_features", [])
    if inactive:
        console.print(
            "[yellow]已自动禁用无效基因：[/yellow]"
            + "、".join(FEATURE_LABELS.get(name, name) for name in inactive)
        )
    quality = summary.get("feature_quality", {})
    active = summary.get("active_features", [])
    if not active:
        return
    table = Table(title="有效结构基因证据（高分组 - 低分组的 T+1 收益差）")
    table.add_column("结构")
    table.add_column("开发段", justify="right")
    table.add_column("测试段", justify="right")
    table.add_column("当前解释")
    for feature in active:
        item = quality.get(feature, {})
        development = item.get("development_spread")
        test = item.get("test_spread")
        if item.get("direction_consistent"):
            if feature == "break_risk" and development is not None and development > 0:
                interpretation = "方向反常，需重新定义"
            elif development is not None and development > 0:
                interpretation = "高值暂时占优"
            else:
                interpretation = "低值暂时占优"
        else:
            interpretation = "方向不稳定"
        table.add_row(
            FEATURE_LABELS.get(feature, feature),
            "—" if development is None else f"{development:+.2f}%",
            "—" if test is None else f"{test:+.2f}%",
            interpretation,
        )
    console.print(table)
    reseal = quality.get("reseal_quality", {})
    if reseal.get("direction_consistent") and (reseal.get("development_spread") or 0) > 0:
        console.print(
            "[bold]当前可复述假设：[/bold]有限分歧后的回封（1-3次开板）"
            "比零开板/反复炸板更值得进入次日候选；仍需新样本确认。"
        )


@main.command("scan")
@click.option("--date", default=None, help="信号日期 YYYY-MM-DD，默认最新涨停日")
@click.option("--top", "top_n", default=8, type=int, show_default=True)
@click.option("--db", "db_path", default="data/alpha_miner.db", show_default=True)
@click.option("--state", "state_path", default="data/limit_up_evolution.json", show_default=True)
def scan_cmd(date: str | None, top_n: int, db_path: str, state_path: str):
    """用锁定测试后的最佳基因生成次日条件操作卡。"""
    engine = LimitUpEvolutionEngine(db_path=db_path, state_path=state_path)
    _print_cards(engine.action_cards(date=date, top_n=top_n))


def _print_cards(cards) -> None:
    if not cards:
        console.print("[yellow]没有操作卡[/yellow]")
        return
    table = Table(title="涨停板次日操作卡")
    table.add_column("代码/名称", style="cyan")
    table.add_column("动作")
    table.add_column("结构分", justify="right")
    table.add_column("主要贡献")
    for card in cards:
        action_style = {
            "CONDITIONAL_BUY": "green",
            "WATCH_ONLY": "yellow",
            "AVOID": "red",
        }.get(card.action, "white")
        table.add_row(
            f"{card.stock_code} {card.stock_name}",
            f"[{action_style}]{card.action}[/{action_style}]",
            f"{card.score:.3f}",
            "；".join(card.reasons),
        )
    console.print(table)
    first = cards[0]
    console.print(f"入场：{first.entry_rule}")
    console.print(f"退出：{first.exit_rule}")
    console.print(f"仓位：{first.position_rule}")
    if all(card.action != "CONDITIONAL_BUY" for card in cards):
        console.print("[yellow]当前因子未通过锁定测试，系统明确给出 0 仓位，只可观察。[/yellow]")


if __name__ == "__main__":
    main()
