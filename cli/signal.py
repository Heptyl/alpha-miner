"""次日信号 CLI — python -m cli signal

用法:
  python -m cli signal --date 2026-04-24
  python -m cli signal                 # 默认今天
  python -m cli signal --top 5         # 只看 TOP 5
"""

import argparse
import sys
from datetime import datetime, timedelta
from pathlib import Path

from rich.console import Console
from rich.table import Table
from rich.panel import Panel

from src.data.storage import Storage

console = Console()


def main():
    parser = argparse.ArgumentParser(description="Alpha Miner 次日选股信号")
    parser.add_argument("--date", type=str, default=None, help="信号日期 YYYY-MM-DD，默认今天")
    parser.add_argument("--db", type=str, default="data/alpha_miner.db", help="数据库路径")
    parser.add_argument("--top", type=int, default=10, help="返回 TOP N 候选股")
    parser.add_argument("--save", type=str, default=None, help="保存路径 (默认 signals/YYYY-MM-DD.txt)")
    parser.add_argument("--json", action="store_true", help="输出 JSON 格式")
    args = parser.parse_args()

    db = Storage(args.db)

    if args.date:
        report_date = args.date
        as_of = datetime.strptime(args.date, "%Y-%m-%d").replace(hour=23, minute=59, second=59)
        as_of = as_of + timedelta(days=1)
    else:
        report_date = datetime.now().strftime("%Y-%m-%d")
        as_of = datetime.now()

    console.print(f"\n[bold cyan]生成次日信号: {report_date}[/bold cyan]")

    from src.strategy.signal import SignalEngine
    engine = SignalEngine(db)

    report = engine.generate(as_of, report_date, top_n=args.top)

    if args.json:
        import json
        print(json.dumps(report.to_dict(), ensure_ascii=False, indent=2))
    else:
        # Rich 表格输出
        _print_rich(report)

    # 保存纯文本
    save_path = args.save or f"signals/{report_date}.txt"
    Path(save_path).parent.mkdir(parents=True, exist_ok=True)
    Path(save_path).write_text(report.to_text(), encoding="utf-8")
    console.print(f"\n[dim]信号已保存: {save_path}[/dim]")


def _print_rich(report):
    """Rich 终端输出."""
    # 大盘概况
    console.print(Panel(
        f"涨停 [bold green]{report.zt_count}[/bold green] 只 | "
        f"跌停 [bold red]{report.dt_count}[/bold red] 只 | "
        f"{report.market_regime}",
        title=f"次日信号 — {report.trade_date}",
    ))

    # 板块热度
    if report.hot_industries:
        ind_table = Table(title="热门板块", show_lines=False)
        ind_table.add_column("板块", style="cyan")
        ind_table.add_column("涨停数", justify="right")
        ind_table.add_column("最高连板", justify="right")
        ind_table.add_column("热度", style="yellow")

        for hi in report.hot_industries[:5]:
            stars = "★" * min(hi["zt_count"], 5)
            ind_table.add_row(
                str(hi["industry"]),
                str(hi["zt_count"]),
                str(hi["max_consecutive"]),
                stars,
            )
        console.print(ind_table)

    # 信号卡
    if not report.cards:
        console.print("[yellow]无符合条件的候选股[/yellow]")
        return

    card_table = Table(title=f"明日关注 TOP {len(report.cards)}", show_lines=True)
    card_table.add_column("等级", justify="center", width=4)
    card_table.add_column("代码", style="bold")
    card_table.add_column("名称", style="cyan")
    card_table.add_column("板块")
    card_table.add_column("连板", justify="right")
    card_table.add_column("综合分", justify="right", style="bold green")
    card_table.add_column("拥挤度", justify="right")
    card_table.add_column("龙头度", justify="right")
    card_table.add_column("资金", justify="right")
    card_table.add_column("入选理由 / 风险", max_width=40)

    for card in report.cards:
        level_style = {"A": "bold green", "B": "yellow", "C": "dim"}.get(card.signal_level, "")
        reasons_text = "\n".join(f"✓ {r}" for r in card.reasons[:2])
        if card.risks:
            reasons_text += "\n" + "\n".join(f"⚠ {r}" for r in card.risks[:2])

        card_table.add_row(
            f"[{level_style}]{card.signal_level}[/{level_style}]",
            card.stock_code,
            card.stock_name,
            card.industry,
            str(card.consecutive_zt),
            f"{card.composite_score:.2f}",
            f"{card.theme_crowding:.2f}",
            f"{card.leader_clarity:.2f}",
            f"{card.lhb_institution:.2f}",
            reasons_text,
        )

    console.print(card_table)

    # A级单独强调
    a_cards = [c for c in report.cards if c.signal_level == "A"]
    if a_cards:
        console.print("\n[bold green]★ 重点标的:[/bold green]")
        for c in a_cards:
            console.print(
                f"  {c.stock_code} {c.stock_name} — "
                f"综合{c.composite_score:.2f} | "
                f"连板{c.consecutive_zt} | "
                f"{'; '.join(c.reasons[:2])}"
            )

    console.print("\n[dim]提示: 信号仅供参考，不构成投资建议。建议集合竞价观察后再决策。[/dim]")


if __name__ == "__main__":
    main()
