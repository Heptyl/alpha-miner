#!/usr/bin/env python3
"""精简版晚间推荐 — 跳过采集步骤，直接用已有数据生成推荐。"""
import sys
import json
from datetime import datetime, timedelta
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

def main():
    now = datetime.now()
    trade_date = "2026-04-30"  # 数据库最新交易日

    print(f"Alpha Miner 晚间推荐 (精简模式)")
    print(f"运行时间: {now.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"使用交易日: {trade_date}")

    # Step 1: 计算因子
    print("\n[1/2] 计算因子...")
    import subprocess
    result = subprocess.run(
        "uv run python -m cli backtest --compute-today",
        shell=True, capture_output=True, text=True,
        cwd=str(project_root), timeout=600,
    )
    if result.returncode == 0:
        print("  ✅ 因子计算完成")
    else:
        print(f"  ⚠ 因子计算可能不完整: {result.stderr[:200]}")

    # Step 2: 生成推荐
    print("\n[2/2] 生成 TOP 5 推荐...")
    from src.data.storage import Storage
    from src.strategy.recommend import RecommendEngine

    db = Storage("data/alpha_miner.db")
    as_of = datetime.strptime(trade_date, "%Y-%m-%d").replace(hour=23, minute=59, second=59)
    as_of = as_of + timedelta(days=1)

    engine = RecommendEngine(db)
    report = engine.recommend(as_of, trade_date, top_n=5)

    print(f"  推荐数量: {len(report.stocks)}")
    for i, s in enumerate(report.stocks, 1):
        print(f"    #{i} [{s.signal_level}] {s.stock_code} {s.stock_name}"
              f" — 买:{s.buy_price:.2f} 目标:{s.target_price:.2f} 止损:{s.stop_loss:.2f}")

    # 保存
    from src.strategy.push import push_recommendation, _format_wechat_message

    results = push_recommendation(
        report,
        target="",
        save_dir="recommendations",
        save_json=True,
        print_terminal=False,
    )
    print(f"\n  文本报告: {results['file']}")
    print(f"  JSON数据: {results['json']}")

    if report.stocks:
        msg = _format_wechat_message(report)
        push_file = Path("recommendations") / f"{trade_date}_push.txt"
        push_file.write_text(msg, encoding="utf-8")
        print(f"  推送消息: {push_file}")
        print(f"\n{'─'*60}")
        print(msg)
        print(f"{'─'*60}")
    
    print(f"\n✅ 晚间推荐完成 — {now.strftime('%Y-%m-%d %H:%M:%S')}")
    return len(report.stocks)

if __name__ == "__main__":
    n = main()
    print(f"RECOMMEND_COUNT={n}")
