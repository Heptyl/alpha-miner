"""CLI 采集命令 — click 框架 + rich 美化输出。

用法:
    python -m cli.collect --date 2024-06-15
    python -m cli.collect --backfill 60
    python -m cli.collect --today
"""

import sys
from datetime import datetime, timedelta

import click
from rich.console import Console
from rich.table import Table

from src.data.storage import Storage
from src.data.collector import collect_date


console = Console()


def _is_trade_date(date_str: str) -> bool:
    """简单判断是否可能是交易日（排除周末）。"""
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    return dt.weekday() < 5


def _get_recent_trade_dates(n: int) -> list[str]:
    """生成最近 N 个可能的交易日（跳过周末，不考虑节假日）。"""
    dates = []
    current = datetime.now()
    while len(dates) < n:
        current -= timedelta(days=1)
        if current.weekday() < 5:
            dates.append(current.strftime("%Y-%m-%d"))
    return list(reversed(dates))


@click.command()
@click.option("--date", "target_date", help="采集指定日期 (YYYY-MM-DD)")
@click.option("--backfill", "backfill_days", type=int, help="回填最近 N 天")
@click.option("--today", "use_today", is_flag=True, help="采集今天")
def main(target_date: str, backfill_days: int, use_today: bool):
    """Alpha Miner 数据采集器。"""
    db = Storage("data/alpha_miner.db")
    db.init_db()

    if use_today:
        target_date = datetime.now().strftime("%Y-%m-%d")

    if target_date:
        _collect_single(target_date, db)
    elif backfill_days:
        _collect_backfill(backfill_days, db)
    else:
        console.print("[red]请指定 --date, --backfill 或 --today[/red]")
        sys.exit(1)


def _collect_single(date_str: str, db: Storage) -> None:
    """采集单日数据。"""
    if not _is_trade_date(date_str):
        console.print(f"[yellow]{date_str} 是周末，可能非交易日[/yellow]")

    console.print(f"\n[bold cyan]采集数据: {date_str}[/bold cyan]")
    results = collect_date(date_str, db)

    # 输出汇总表格
    table = Table(title=f"采集结果: {date_str}")
    table.add_column("数据源", style="cyan")
    table.add_column("行数", justify="right")
    table.add_column("状态", justify="center")

    total = 0
    for name, count in results.items():
        total += count
        status = "[green]OK[/green]" if count > 0 else "[yellow]EMPTY[/yellow]"
        table.add_row(name, str(count), status)

    table.add_row("[bold]Total[/bold]", f"[bold]{total}[/bold]", "")
    console.print(table)


def _collect_backfill(days: int, db: Storage) -> None:
    """回填多日数据。"""
    dates = _get_recent_trade_dates(days)
    console.print(f"\n[bold cyan]回填 {len(dates)} 天数据[/bold cyan]")

    success = 0
    failed = 0

    for i, date_str in enumerate(dates, 1):
        console.print(f"\n[{i}/{len(dates)}] {date_str}")
        try:
            results = collect_date(date_str, db)
            day_total = sum(results.values())
            if day_total > 0:
                success += 1
                console.print(f"  [green]OK[/green]: {day_total} rows")
            else:
                failed += 1
                console.print(f"  [yellow]EMPTY[/yellow]")
        except Exception as e:
            failed += 1
            console.print(f"  [red]FAIL[/red]: {e}")

    console.print(f"\n[bold]回填完成: {success} 成功, {failed} 空/失败[/bold]")


if __name__ == "__main__":
    main()
