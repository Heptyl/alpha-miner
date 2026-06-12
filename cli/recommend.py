"""每日个股推荐 CLI — python -m cli recommend

用法:
  python -m cli recommend --date 2026-04-26
  python -m cli recommend                 # 默认今天
  python -m cli recommend --top 5         # 推荐5只
  python -m cli recommend --json          # JSON输出
"""

import argparse
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.text import Text

from src.data.storage import Storage

console = Console()


def _auto_review(report_date: str, db_path: str):
    """自动复盘前一次推荐的实际情况。"""
    from src.strategy.review import run_review, format_review_wechat

    review = run_review(report_date, db_path=db_path)
    if review is None:
        return  # 没有前一日推荐，跳过

    # 输出复盘结果
    console.print()
    console.print(Panel(
        f"回顾 [bold]{review.rec_date}[/bold] 的 {review.total} 只推荐",
        title=f"[bold]自动复盘 — {review.review_date}[/bold]",
        border_style="yellow",
    ))

    review_table = Table(show_lines=True, border_style="dim")
    review_table.add_column("#", width=2)
    review_table.add_column("代码", width=8)
    review_table.add_column("名称", width=8)
    review_table.add_column("等级", width=4)
    review_table.add_column("推荐买", style="yellow", width=8)
    review_table.add_column("目标", style="green", width=8)
    review_table.add_column("止损", style="red", width=8)
    review_table.add_column("实际开", width=8)
    review_table.add_column("实际收", width=8)
    review_table.add_column("涨跌", width=6)
    review_table.add_column("触及买点", width=6)
    review_table.add_column("盈亏%", width=8)

    for i, s in enumerate(review.stocks, 1):
        chg_color = "green" if s.today_change_pct > 0 else "red"
        buy_icon = "✓" if s.hit_buy_zone else "✗"
        profit_color = "green" if s.profit_pct > 0 else "red"

        review_table.add_row(
            str(i),
            s.stock_code,
            s.stock_name,
            s.signal_level,
            f"{s.rec_buy_price:.2f}",
            f"{s.rec_target:.2f}",
            f"{s.rec_stop_loss:.2f}",
            f"{s.today_open:.2f}",
            f"{s.today_close:.2f}",
            f"[{chg_color}]{s.today_change_pct:+.1f}%[/{chg_color}]",
            buy_icon,
            f"[{profit_color}]{s.profit_pct:+.1f}%[/{profit_color}]",
        )

    console.print(review_table)

    # 汇总
    console.print(
        f"  触及买点: {review.hit_buy_count}/{review.total} | "
        f"命中目标: {review.hit_target_count}/{review.total} | "
        f"触发止损: {review.hit_stop_count}/{review.total} | "
        f"平均盈亏: [bold]{review.avg_profit_pct:+.2f}%[/bold] | "
        f"胜率: [bold]{review.win_rate:.0f}%[/bold]"
    )

    # 保存复盘报告
    review_path = f"recommendations/{report_date}_review.json"
    Path(review_path).parent.mkdir(parents=True, exist_ok=True)
    Path(review_path).write_text(
        json.dumps(review.to_dict(), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    console.print(f"  [dim]复盘报告已保存: {review_path}[/dim]")
    console.print()


def main():
    parser = argparse.ArgumentParser(description="Alpha Miner 每日个股推荐")
    parser.add_argument("--date", type=str, default=None, help="推荐日期 YYYY-MM-DD，默认今天")
    parser.add_argument("--db", type=str, default="data/alpha_miner.db", help="数据库路径")
    parser.add_argument("--top", type=int, default=5, help="推荐只数 (默认5)")
    parser.add_argument("--save", type=str, default=None, help="保存路径")
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

    console.print(f"\n[bold cyan]生成每日个股推荐: {report_date}[/bold cyan]")

    # ── 自动复盘前一日推荐 ──
    _auto_review(report_date, args.db)

    from src.strategy.recommend import RecommendEngine
    engine = RecommendEngine(db)

    report = engine.recommend(as_of, report_date, top_n=args.top)

    if args.json:
        print(json.dumps(report.to_dict(), ensure_ascii=False, indent=2))
    else:
        _print_rich(report)

    # 保存纯文本
    save_path = args.save or f"recommendations/{report_date}.txt"
    Path(save_path).parent.mkdir(parents=True, exist_ok=True)
    Path(save_path).write_text(report.to_text(), encoding="utf-8")
    console.print(f"\n[dim]推荐报告已保存: {save_path}[/dim]")

    # 同时保存JSON (供Web UI读取)
    json_path = f"recommendations/{report_date}_recommend_v2.json"
    Path(json_path).parent.mkdir(parents=True, exist_ok=True)
    Path(json_path).write_text(
        json.dumps(report.to_dict(), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    console.print(f"[dim]推荐JSON已保存: {json_path}[/dim]")


def _print_rich(report):
    """Rich 终端输出。"""
    # 大盘概况
    regime_color = {
        "强势市场": "bold green",
        "弱势市场": "bold red",
        "震荡市场": "yellow",
    }.get(report.market_regime, "white")

    console.print(Panel(
        f"涨停 [bold green]{report.zt_count}[/bold green] 只 | "
        f"跌停 [bold red]{report.dt_count}[/bold red] 只 | "
        f"[{regime_color}]{report.market_regime}[/{regime_color}]",
        title=f"[bold]每日个股推荐 — {report.trade_date}[/bold]",
        border_style="cyan",
    ))

    # 热门板块
    if report.hot_industries:
        ind_table = Table(title="热门板块", show_lines=False, border_style="dim")
        ind_table.add_column("板块", style="cyan")
        ind_table.add_column("涨停数", justify="right")
        ind_table.add_column("最高连板", justify="right")
        ind_table.add_column("热度", style="yellow")

        for hi in report.hot_industries[:5]:
            stars = "★" * min(hi.get("zt_count", 0), 5)
            ind_table.add_row(
                str(hi.get("industry", "")),
                str(hi.get("zt_count", 0)),
                str(hi.get("max_consecutive", 0)),
                stars,
            )
        console.print(ind_table)

    # 热门概念
    if report.hot_concepts:
        console.print(f"\n[cyan]热门概念:[/cyan] {', '.join(report.hot_concepts[:5])}")

    # 推荐个股
    if not report.stocks:
        console.print("\n[yellow]今日无符合条件的推荐个股[/yellow]")
        return

    # 汇总表
    summary_table = Table(
        title=f"今日推荐 TOP {len(report.stocks)}",
        show_lines=True,
        border_style="cyan",
    )
    summary_table.add_column("等级", justify="center", width=4)
    summary_table.add_column("#", justify="center", width=2)
    summary_table.add_column("代码", style="bold", width=8)
    summary_table.add_column("名称", style="cyan", width=8)
    summary_table.add_column("板块", width=10)
    summary_table.add_column("连板", justify="right", width=3)
    summary_table.add_column("综合分", justify="right", style="bold green", width=5)
    summary_table.add_column("买入区间", justify="right", width=16)
    summary_table.add_column("建议买价", justify="right", style="bold yellow", width=8)
    summary_table.add_column("目标价", justify="right", style="green", width=8)
    summary_table.add_column("止损价", justify="right", style="red", width=8)

    for i, stock in enumerate(report.stocks, 1):
        level_style = {"A": "bold green", "B": "yellow", "C": "dim"}.get(stock.signal_level, "")

        # 盈亏比
        profit_pct = ((stock.target_price / stock.buy_price - 1) * 100) if stock.buy_price > 0 else 0
        loss_pct = ((1 - stock.stop_loss / stock.buy_price) * 100) if stock.buy_price > 0 else 0

        summary_table.add_row(
            f"[{level_style}]{stock.signal_level}[/{level_style}]",
            str(i),
            stock.stock_code,
            stock.stock_name,
            stock.industry[:6] if stock.industry else "",
            str(stock.consecutive_zt),
            f"{stock.composite_score:.2f}",
            f"{stock.buy_zone_low:.2f} ~ {stock.buy_zone_high:.2f}",
            f"{stock.buy_price:.2f}",
            f"{stock.target_price:.2f}(+{profit_pct:.1f}%)",
            f"{stock.stop_loss:.2f}(-{loss_pct:.1f}%)",
        )

    console.print(summary_table)

    # 详情卡
    for i, stock in enumerate(report.stocks, 1):
        _print_stock_detail(i, stock)

    # 免责声明
    console.print(
        "\n[dim yellow]⚠ 以上推荐基于量化模型，仅供参考，不构成投资建议。"
        "建议次日集合竞价观察后再决策。[/dim yellow]"
    )


def _print_stock_detail(idx: int, stock):
    """打印单只推荐股详情。"""
    level_color = {"A": "green", "B": "yellow", "C": "white"}.get(stock.signal_level, "white")

    detail_lines = []
    detail_lines.append(
        f"[bold]#{idx} {stock.stock_code} {stock.stock_name}[/bold]"
        f"  [{level_color}][{stock.signal_level}][/{level_color}]"
        f"  综合分 [bold green]{stock.composite_score:.2f}[/bold green]"
    )

    # 买入点位
    profit_pct = ((stock.target_price / stock.buy_price - 1) * 100) if stock.buy_price > 0 else 0
    loss_pct = ((1 - stock.stop_loss / stock.buy_price) * 100) if stock.buy_price > 0 else 0

    detail_lines.append(
        f"  买入区间: [bold yellow]{stock.buy_zone_low:.2f} ~ {stock.buy_zone_high:.2f}[/bold yellow]"
        f"  |  建议买价: [bold]{stock.buy_price:.2f}[/bold]"
    )
    detail_lines.append(
        f"  目标价位: [green]{stock.target_price:.2f} (+{profit_pct:.1f}%)[/green]"
        f"  |  止损: [red]{stock.stop_loss:.2f} (-{loss_pct:.1f}%)[/red]"
    )

    # 因子 & 技术
    if stock.technical:
        ta = stock.technical
        ma5_str = f"{ta.ma5:.2f}" if ta.ma5 else "N/A"
        detail_lines.append(
            f"  趋势: {ta.trend} | MA5={ma5_str}"
            f" | 量比={ta.volume_ratio:.1f}"
            f" | 动量={ta.momentum_score:.2f}"
        )

    # 概念
    if stock.concepts:
        detail_lines.append(f"  概念: {', '.join(stock.concepts[:4])}")

    # 理由
    for r in stock.reasons[:3]:
        detail_lines.append(f"  [green]✓ {r}[/green]")

    # 风险
    for r in stock.risks[:2]:
        detail_lines.append(f"  [red]⚠ {r}[/red]")

    console.print(Panel(
        "\n".join(detail_lines),
        border_style="dim",
        padding=(0, 1),
    ))


if __name__ == "__main__":
    main()
