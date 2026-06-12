"""9维选股 CLI。

Usage:
    python -m cli.screen --date 2026-05-08
    python -m cli.screen --date 2026-05-08 --top 30 --min-score 0.3
    python -m cli.screen --date 2026-05-08 --dimension 1  # 只跑某个维度
    python -m cli.screen --date 2026-05-08 --save output/recommendations
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import click
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

# 确保项目根目录在 path
ROOT = Path(__file__).parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.screener.engine import ScreenerEngine


console = Console()

DIM_NAMES = {
    1: "趋势突破", 2: "缩量回调", 3: "资金连续流入",
    4: "板块轮动", 5: "量价筛选", 6: "基本面排雷",
    7: "主力资金", 8: "行业景气", 9: "风控筛选",
}


@click.group()
def cli():
    """9维选股系统。"""
    pass


@cli.command()
@click.option("--date", "-d", required=True, help="选股日期 YYYY-MM-DD")
@click.option("--top", "-n", default=20, help="返回前N只")
@click.option("--min-score", type=float, default=0.30, help="最低综合分")
@click.option("--dimension", type=int, default=None, help="只跑指定维度(1-9)")
@click.option("--save", "-s", default=None, help="保存JSON到目录")
@click.option("--db", default="data/alpha_miner.db", help="数据库路径")
def run(date, top, min_score, dimension, save, db):
    """执行9维选股。"""
    console.print(f"[bold cyan]9维选股系统[/] — {date}")
    console.print()

    if dimension:
        # 只跑单个维度
        _run_single_dimension(date, dimension, db)
        return

    engine = ScreenerEngine(db)

    with console.status("[bold green]正在执行9维选股..."):
        if save:
            out_path = engine.run_and_save(date, top_n=top, output_dir=save)
            scores = engine.run(date, top_n=top, min_score=min_score)
            console.print(f"[green]结果已保存到: {out_path}[/]")
        else:
            scores = engine.run(date, top_n=top, min_score=min_score)

    if not scores:
        console.print("[yellow]未找到符合条件的股票[/]")
        return

    # 显示结果
    _print_results(scores, date)


def _run_single_dimension(date: str, dim: int, db_path: str):
    """运行单个维度。"""
    from src.screener.trend_breakout import TrendBreakoutScreener
    from src.screener.volume_pullback import VolumePullbackScreener
    from src.screener.capital_flow import CapitalFlowScreener
    from src.screener.sector_rotation import SectorRotationScreener
    from src.screener.volume_price import VolumePriceScreener
    from src.screener.fundamental import FundamentalScreener
    from src.screener.main_force import MainForceScreener
    from src.screener.industry import IndustryScreener
    from src.screener.risk_control import RiskControlScreener

    screener_map = {
        1: TrendBreakoutScreener,
        2: VolumePullbackScreener,
        3: CapitalFlowScreener,
        4: SectorRotationScreener,
        5: VolumePriceScreener,
        6: FundamentalScreener,
        7: MainForceScreener,
        8: IndustryScreener,
        9: RiskControlScreener,
    }

    if dim not in screener_map:
        console.print(f"[red]无效维度: {dim}, 有效范围1-9[/]")
        return

    name = DIM_NAMES[dim]
    console.print(f"[bold]维度{dim}: {name}[/]")
    console.print()

    screener = screener_map[dim](db_path)
    with console.status(f"[green]正在执行{name}..."):
        results = screener.screen(date)

    if not results:
        console.print("[yellow]未找到符合条件的股票[/]")
        return

    # 显示
    table = Table(title=f"维度{dim}: {name} (共{len(results)}只)")
    table.add_column("代码", style="cyan", width=8)
    table.add_column("名称", style="white", width=10)
    table.add_column("得分", style="green", width=6)
    table.add_column("信号", style="yellow", width=4)
    table.add_column("理由", style="dim", max_width=50)

    for r in results[:30]:
        reasons_text = "; ".join(r.reasons[:3])
        sig_color = {"A": "red", "B": "yellow", "C": "dim"}[r.signal_strength]
        table.add_row(
            r.stock_code,
            r.stock_name,
            f"{r.score:.2f}",
            f"[{sig_color}]{r.signal_strength}[/]",
            reasons_text,
        )

    console.print(table)


def _print_results(scores, date: str):
    """打印综合选股结果。"""
    # 汇总表
    table = Table(title=f"9维选股结果 ({date}) — 共{len(scores)}只")
    table.add_column("排名", style="dim", width=4)
    table.add_column("代码", style="cyan", width=8)
    table.add_column("名称", style="white", width=10)
    table.add_column("综合分", style="bold green", width=7)
    table.add_column("信号", style="yellow", width=4)
    table.add_column("技术", width=5)
    table.add_column("资金", width=5)
    table.add_column("基本", width=5)
    table.add_column("板块", width=5)
    table.add_column("风控", width=5)
    table.add_column("维度", width=4)
    table.add_column("核心理由", style="dim", max_width=35)

    from src.screener.engine import DIM_CATEGORY

    for i, s in enumerate(scores, 1):
        cat = s.category_scores
        tech = cat.get("technical", 0)
        capital = cat.get("capital", 0)
        fund = cat.get("fundamental", 0)
        sector = cat.get("sector", 0)
        risk = cat.get("risk", 0)
        n_dims = len(s.dimension_scores)

        # 汇总所有理由取前3条
        all_reasons = []
        for dim_reasons in s.dimension_details.values():
            all_reasons.extend(dim_reasons)
        top_reasons = "; ".join(all_reasons[:3])

        sig_color = {"A": "bold red", "B": "yellow", "C": "dim"}[s.signal_level]

        table.add_row(
            str(i),
            s.stock_code,
            s.stock_name,
            f"{s.total_score:.2f}",
            f"[{sig_color}]{s.signal_level}[/]",
            f"{tech:.2f}" if tech else "-",
            f"{capital:.2f}" if capital else "-",
            f"{fund:.2f}" if fund else "-",
            f"{sector:.2f}" if sector else "-",
            f"{risk:.2f}" if risk else "-",
            f"{n_dims}维",
            top_reasons,
        )

    console.print(table)

    # 统计
    sig_counts = {"A": 0, "B": 0, "C": 0}
    for s in scores:
        sig_counts[s.signal_level] += 1

    console.print()
    console.print(Panel(
        f"信号分布: A(强)={sig_counts['A']}  B(中)={sig_counts['B']}  C(弱)={sig_counts['C']}  "
        f"| 总计={len(scores)}",
        title="[bold]统计[/]",
        border_style="blue",
    ))


if __name__ == "__main__":
    cli()
