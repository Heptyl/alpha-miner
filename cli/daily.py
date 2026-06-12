"""每日一条龙 CLI — python -m cli daily

按顺序执行:
  1. 数据采集 (zt_pool, daily_price, fund_flow, news ...)
  2. 因子计算 (全部因子)
  3. 盘后推荐 (含自动复盘前一日)
  4. 微信推送 (推荐 + 复盘摘要)

用法:
  python -m cli daily
  python -m cli daily --date 2026-05-06
  python -m cli daily --no-push         # 不推微信
  python -m cli daily --no-collect      # 跳过采集，直接用已有数据
  python -m cli daily --top 10          # 推荐10只
"""

import argparse
import json
import sqlite3
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn

console = Console()


def step(name: str, emoji: str = ""):
    """打印步骤标题。"""
    console.print(f"\n[bold cyan]{emoji} {name}[/bold cyan]")
    console.rule(style="dim")


def run_daily(args):
    """一条龙执行。"""
    start = time.time()
    report_date = args.date or datetime.now().strftime("%Y-%m-%d")
    db_path = args.db

    console.print(Panel(
        f"[bold]Alpha Miner 每日流水线[/bold]\n"
        f"日期: {report_date}  |  "
        f"推送: {'否' if args.no_push else '是'}  |  "
        f"采集: {'跳过' if args.no_collect else '执行'}  |  "
        f"Top: {args.top}",
        border_style="cyan",
    ))

    results = {}

    # ── Step 1: 数据采集 ──────────────────────────────
    if not args.no_collect:
        step("Step 1/4: 数据采集", "📡")
        try:
            from cli.collect import main as collect_main
            # 构造参数
            old_argv = sys.argv
            sys.argv = ["cli.collect", "--date", report_date]
            try:
                collect_main()
            except SystemExit:
                pass
            finally:
                sys.argv = old_argv
            results["collect"] = "OK"
        except Exception as e:
            console.print(f"  [red]采集失败: {e}[/red]")
            results["collect"] = f"FAIL: {e}"
    else:
        results["collect"] = "SKIP"

    # ── Step 2: 因子计算 ──────────────────────────────
    step("Step 2/4: 因子计算", "🧮")
    try:
        from cli.backtest import compute_today
        compute_today(db_path)
        results["factors"] = "OK"
    except Exception as e:
        console.print(f"  [yellow]因子计算异常: {e}[/yellow]")
        results["factors"] = f"WARN: {e}"

    # ── Step 3: 盘后推荐 (含自动复盘) ────────────────
    step("Step 3/4: 盘后推荐 + 复盘", "🎯")
    try:
        from cli.recommend import main as recommend_main
        old_argv = sys.argv
        sys.argv = ["cli.recommend", "--date", report_date, "--top", str(args.top)]
        try:
            recommend_main()
        except SystemExit:
            pass
        finally:
            sys.argv = old_argv
        results["recommend"] = "OK"
    except Exception as e:
        console.print(f"  [red]推荐失败: {e}[/red]")
        results["recommend"] = f"FAIL: {e}"

    # ── Step 4: 交易计划 ─────────────────────────────
    step("Step 4/5: 交易计划", "📋")
    try:
        from cli.tradeplan import generate_plan, _print_plan
        plan = generate_plan(report_date, db_path, args.capital)
        _print_plan(plan)
        results["tradeplan"] = "OK"
    except Exception as e:
        console.print(f"  [red]交易计划失败: {e}[/red]")
        results["tradeplan"] = f"FAIL: {e}"

    # ── Step 5: 微信推送 ─────────────────────────────
    if not args.no_push:
        step("Step 5/5: 微信推送", "📱")
        try:
            msg = _build_push_message(report_date, db_path)
            if msg:
                _push_to_wechat(msg)
                results["push"] = "OK"
            else:
                console.print("  [yellow]无内容可推送[/yellow]")
                results["push"] = "SKIP"
        except Exception as e:
            console.print(f"  [red]推送失败: {e}[/red]")
            results["push"] = f"FAIL: {e}"
    else:
        results["push"] = "SKIP"

    # ── 汇总 ─────────────────────────────────────────
    elapsed = time.time() - start
    console.print()
    console.print(Panel(
        "\n".join(f"  {k:<12} {v}" for k, v in results.items()) + f"\n  {'耗时':<12} {elapsed:.1f}s",
        title="[bold]流水线完成[/bold]",
        border_style="green" if all("FAIL" not in v for v in results.values()) else "yellow",
    ))


def _build_push_message(report_date: str, db_path: str) -> str:
    """构建推送消息（推荐 + 复盘摘要）。"""
    parts = []

    # 1. 复盘摘要
    review_path = Path(f"recommendations/{report_date}_review.json")
    if review_path.exists():
        try:
            review = json.loads(review_path.read_text(encoding="utf-8"))
            if review.get("stocks"):
                r = review
                parts.append(
                    f"【复盘 {r.get('rec_date', '?')}】\n"
                    f"触及买点 {r.get('hit_buy_count', '?')}/{r.get('total', '?')} "
                    f"胜率 {r.get('win_rate', 0):.0f}% "
                    f"均盈 {r.get('avg_profit_pct', 0):+.1f}%"
                )
        except Exception:
            pass

    # 2. 今日推荐
    rec_path = Path(f"recommendations/{report_date}_recommend.json")
    
    # 2.5 交易计划
    from datetime import timedelta
    dt = datetime.strptime(report_date, "%Y-%m-%d")
    for _ in range(5):
        dt += timedelta(days=1)
        if dt.weekday() < 5:
            break
    target_date = dt.strftime("%Y-%m-%d")
    
    tp_path = Path(f"recommendations/{target_date}_tradeplan.json")
    if tp_path.exists():
        try:
            tp = json.loads(tp_path.read_text(encoding="utf-8"))
            if tp.get('top'):
                s = tp['strategy']
                parts.append(
                    f"【交易计划 {target_date}】\n"
                    f"仓位{s['position_pct']}% 止损{s['stop_loss']}% 止盈{s['take_profit']}%"
                )
                for i, st in enumerate(tp['top'], 1):
                    parts.append(
                        f"{i}. {st['code']} {st['name']} "
                        f"买{st['entry_target']:.2f}(≤{st['entry_max']:.2f}) "
                        f"止{st['stop_price']:.2f} 盈{st['target_price']:.2f} "
                        f"{st['shares']}股"
                    )
        except Exception:
            pass

    if not rec_path.exists():
        # 也可能是纯文本格式
        txt_path = Path(f"recommendations/{report_date}.txt")
        if txt_path.exists():
            text = txt_path.read_text(encoding="utf-8")
            # 截取推荐部分
            lines = text.split("\n")
            rec_lines = []
            in_rec = False
            for line in lines:
                if "推荐" in line or "买入" in line:
                    in_rec = True
                if in_rec and len(rec_lines) < 20:
                    rec_lines.append(line)
            if rec_lines:
                parts.append("【今日推荐】\n" + "\n".join(rec_lines[:15]))
        return "\n\n".join(parts) if parts else ""

    try:
        rec = json.loads(rec_path.read_text(encoding="utf-8"))
    except Exception:
        return "\n\n".join(parts) if parts else ""

    # 从数据库补充市场概况
    conn = sqlite3.connect(db_path)
    zt_count = conn.execute(
        "SELECT COUNT(*) FROM zt_pool WHERE trade_date=?", (report_date,)
    ).fetchone()[0]

    stocks = rec.get("stocks", [])
    if not stocks:
        conn.close()
        parts.append(f"【推荐 {report_date}】\n涨停{zt_count}只 | 无符合条件个股")
        return "\n\n".join(parts) if parts else ""

    parts.append(f"【推荐 {report_date}】涨停{zt_count}只")

    for i, s in enumerate(stocks[:5], 1):
        level = s.get("signal_level", "?")
        code = s.get("stock_code", "?")
        name = s.get("stock_name", "?")
        buy = s.get("buy_price", 0)
        target = s.get("target_price", 0)
        stop = s.get("stop_loss", 0)
        zl = s.get("buy_zone_low", 0)
        zh = s.get("buy_zone_high", 0)
        score = s.get("composite_score", 0)
        reasons = s.get("reasons", [])

        line = (
            f"{i}. [{level}] {code} {name} ({score:.2f})\n"
            f"   买 {zl:.2f}~{zh:.2f} | 目标 {target:.2f} | 止损 {stop:.2f}"
        )
        if reasons:
            line += f"\n   {';'.join(reasons[:2])}"
        parts.append(line)

    conn.close()

    parts.append("⚠ 仅供参考，不构成投资建议")
    return "\n\n".join(parts)


def _push_to_wechat(msg: str):
    """推送消息到微信 — 写入推送文件，由 Hermes cron 负责发送。"""
    push_file = Path("recommendations/_pending_push.txt")
    push_file.write_text(msg, encoding="utf-8")
    console.print(f"  [green]✓ 推送内容已写入 {push_file}[/green]")
    console.print("  [dim]Hermes cron 将自动推送到微信[/dim]")


def main():
    parser = argparse.ArgumentParser(description="Alpha Miner 每日一条龙")
    parser.add_argument("--date", type=str, default=None, help="日期 YYYY-MM-DD，默认今天")
    parser.add_argument("--db", type=str, default="data/alpha_miner.db", help="数据库路径")
    parser.add_argument("--top", type=int, default=5, help="推荐只数 (默认5)")
    parser.add_argument("--no-push", action="store_true", help="不推送到微信")
    parser.add_argument("--no-collect", action="store_true", help="跳过数据采集")
    parser.add_argument("--capital", type=float, default=100000, help="总资金(默认10万)")
    args = parser.parse_args()
    run_daily(args)


if __name__ == "__main__":
    main()
