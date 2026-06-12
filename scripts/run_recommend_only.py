#!/usr/bin/env python3
"""仅运行推荐生成，跳过数据采集和因子计算（假设已完成）。"""
import sys
import json
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from src.data.storage import Storage
from src.strategy.recommend import RecommendEngine
from src.strategy.push import push_recommendation, _format_wechat_message

trade_date = "2026-05-06"
db = Storage("data/alpha_miner.db")
as_of = datetime.strptime(trade_date, "%Y-%m-%d").replace(hour=23, minute=59, second=59)
as_of = as_of + timedelta(days=1)

engine = RecommendEngine(db)
report = engine.recommend(as_of, trade_date, top_n=5)

# Verify prices
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
        status = "OK" if abs(actual - db_close) < 0.01 else "MISMATCH"
        print(f"  {status} {s.stock_code} {s.stock_name}: close={db_close} rec={actual:.2f}")
    else:
        print(f"  NO_DATA {s.stock_code} {s.stock_name}")
_conn.close()

print(f"\n推荐数量: {len(report.stocks)}")
for i, s in enumerate(report.stocks, 1):
    print(f"  #{i} [{s.signal_level}] {s.stock_code} {s.stock_name} — 买:{s.buy_price:.2f} 目标:{s.target_price:.2f} 止损:{s.stop_loss:.2f}")

# Save files
results = push_recommendation(
    report,
    target="",
    save_dir="recommendations",
    save_json=True,
    print_terminal=False,
)
print(f"\nText: {results['file']}")
print(f"JSON: {results['json']}")

# Save push message
if report.stocks:
    msg = _format_wechat_message(report)
    push_file = Path("recommendations") / f"{trade_date}_push.txt"
    push_file.write_text(msg, encoding="utf-8")
    print(f"Push: {push_file}")
