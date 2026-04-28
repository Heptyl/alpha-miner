#!/usr/bin/env python3
"""晚间推荐脚本 — 每日23:00运行。

完整流程：
1. 确认今天是否交易日（查数据库有无当日数据）
2. 如果不是交易日（周末/假期），跳过
3. 采集当日最新数据
4. 计算因子
5. 生成 TOP 5 推荐
6. 保存 + 推送到微信

用法:
  uv run python scripts/evening_recommend.py
  uv run python scripts/evening_recommend.py --dry-run   # 只生成不推送
"""

import argparse
import json
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

# 确保项目根目录在 path 中
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


def run_cmd(cmd: str, check: bool = True) -> tuple[int, str]:
    """运行 shell 命令。"""
    result = subprocess.run(
        cmd, shell=True, capture_output=True, text=True,
        cwd=str(project_root), timeout=600,
    )
    if check and result.returncode != 0:
        print(f"  ❌ 命令失败: {cmd}")
        print(f"     {result.stderr[:500]}")
    return result.returncode, result.stdout


def main():
    parser = argparse.ArgumentParser(description="晚间推荐 — 23:00 运行")
    parser.add_argument("--dry-run", action="store_true", help="只生成不推送")
    parser.add_argument("--date", type=str, default=None, help="指定日期 YYYY-MM-DD")
    args = parser.parse_args()

    now = datetime.now()
    print(f"{'='*60}")
    print(f"  Alpha Miner 晚间推荐")
    print(f"  运行时间: {now.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}")

    # ── Step 1: 确定交易日 ──────────────────────────────
    print("\n[1/5] 确认交易日...")

    from src.data.trading_calendar import (
        get_latest_trade_date,
        is_weekend,
    )

    today = args.date or now.strftime("%Y-%m-%d")

    # 周末直接跳过
    if is_weekend(today):
        print(f"  ⏭ 今天({today})是周末，跳过")
        return

    # ── 时间边界：只用已收盘日期的数据 ──
    # 当前时间 < 15:00 说明今天还没收盘，用昨天
    # 当前时间 >= 15:00 说明今天已收盘，可以用今天
    if args.date:
        # 手动指定日期，信任用户
        max_date = args.date
    else:
        # 自动模式：根据当前时间判断
        if now.hour < 15:
            max_date = (now - timedelta(days=1)).strftime("%Y-%m-%d")
        else:
            max_date = today
    print(f"  当前时间: {now.strftime('%H:%M')}")
    print(f"  最大可用日期（已收盘）: {max_date}")

    # 查数据库最新交易日（但不超过 max_date）
    import sqlite3 as _sq
    _conn = _sq.connect("data/alpha_miner.db")
    row = _conn.execute(
        "SELECT MAX(trade_date) FROM daily_price WHERE trade_date <= ?",
        (max_date,),
    ).fetchone()
    latest_trade = row[0] if row else None
    _conn.close()
    print(f"  数据库最新已收盘交易日: {latest_trade}")

    if latest_trade is None:
        print("  ⚠ 无已收盘数据，尝试采集...")
        code, _ = run_cmd("uv run python -m cli collect --today", check=False)
        _conn = _sq.connect("data/alpha_miner.db")
        row = _conn.execute(
            "SELECT MAX(trade_date) FROM daily_price WHERE trade_date <= ?",
            (max_date,),
        ).fetchone()
        latest_trade = row[0] if row else None
        _conn.close()

    if latest_trade is None:
        print("  ❌ 数据库无任何已收盘数据，无法推荐")
        return

    trade_date = latest_trade
    print(f"  ✅ 使用交易日: {trade_date}")

    # ── Step 2: 采集数据 ────────────────────────────────
    print(f"\n[2/5] 采集 {trade_date} 数据...")
    code, output = run_cmd(
        f"uv run python -m cli collect --today",
        check=False,
    )
    if code == 0:
        print(f"  ✅ 数据采集完成")
    else:
        print(f"  ⚠ 采集部分失败（继续用已有数据）")

    # ── Step 3: 计算因子 ────────────────────────────────
    print(f"\n[3/5] 计算因子...")
    code, output = run_cmd(
        f"uv run python -m cli backtest --compute-today",
        check=False,
    )
    if code == 0:
        print(f"  ✅ 因子计算完成")
    else:
        print(f"  ⚠ 因子计算可能不完整")

    # ── Step 4: 生成推荐 ────────────────────────────────
    print(f"\n[4/5] 生成 TOP 5 推荐...")

    from src.data.storage import Storage
    from src.strategy.recommend import RecommendEngine

    db = Storage("data/alpha_miner.db")
    as_of = datetime.strptime(trade_date, "%Y-%m-%d").replace(hour=23, minute=59, second=59)
    as_of = as_of + timedelta(days=1)

    engine = RecommendEngine(db)
    report = engine.recommend(as_of, trade_date, top_n=5)

    # ── 数据校验：二次确认每只推荐股的收盘价正确 ──
    import sqlite3 as _sq
    _conn = _sq.connect("data/alpha_miner.db")
    for s in report.stocks:
        row = _conn.execute(
            "SELECT close FROM daily_price WHERE trade_date = ? AND stock_code = ?",
            (trade_date, s.stock_code),
        ).fetchone()
        if row:
            db_close = row[0]
            actual = s.technical.current_price if s.technical else 0
            status = "✅" if abs(actual - db_close) < 0.01 else "❌ 不一致!"
            print(f"  {status} {s.stock_code} {s.stock_name}: "
                  f"收盘={db_close} 推荐={actual:.2f}")
        else:
            print(f"  ⚠ {s.stock_code} 无当日K线")
    _conn.close()

    print(f"  推荐数量: {len(report.stocks)}")
    for i, s in enumerate(report.stocks, 1):
        print(f"    #{i} [{s.signal_level}] {s.stock_code} {s.stock_name}"
              f" — 买:{s.buy_price:.2f} 目标:{s.target_price:.2f} 止损:{s.stop_loss:.2f}")

    # ── Step 5: 保存 + 推送 ─────────────────────────────
    print(f"\n[5/5] 保存 & 推送...")

    from src.strategy.push import push_recommendation, _format_wechat_message

    # 保存文件
    results = push_recommendation(
        report,
        target="",  # 先不直接推送，后面用 Hermes 发
        save_dir="recommendations",
        save_json=True,
        print_terminal=False,
    )
    print(f"  文本报告: {results['file']}")
    print(f"  JSON数据: {results['json']}")

    if not args.dry_run and report.stocks:
        # 生成微信消息
        msg = _format_wechat_message(report)

        # 保存推送消息到文件（供 Hermes cron 读取发送）
        push_file = Path("recommendations") / f"{trade_date}_push.txt"
        push_file.write_text(msg, encoding="utf-8")
        print(f"  推送消息: {push_file}")

        # 直接输出消息内容（cron 模式下会被捕获推送）
        print(f"\n{'─'*60}")
        print(msg)
        print(f"{'─'*60}")
    elif args.dry_run:
        print(f"  [DRY RUN] 跳过推送")

    print(f"\n✅ 晚间推荐完成 — {now.strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()
