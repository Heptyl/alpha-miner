#!/usr/bin/env python3
"""快速生成晚间推荐 — 跳过数据采集和因子计算，直接用已有数据。"""
import sys
import json
from datetime import datetime, timedelta
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import sqlite3

conn = sqlite3.connect("data/alpha_miner.db")
row = conn.execute("SELECT MAX(trade_date) FROM daily_price").fetchone()
latest_trade = row[0] if row else None
conn.close()
print(f"数据库最新交易日: {latest_trade}")

if not latest_trade:
    print("❌ 无数据")
    sys.exit(1)

trade_date = latest_trade

from src.data.storage import Storage
from src.strategy.recommend import RecommendEngine

db = Storage("data/alpha_miner.db")
as_of = datetime.strptime(trade_date, "%Y-%m-%d").replace(hour=23, minute=59, second=59)
as_of = as_of + timedelta(days=1)

print(f"生成推荐 (as_of={as_of}, trade_date={trade_date})...")
engine = RecommendEngine(db)
report = engine.recommend(as_of, trade_date, top_n=5)

print(f"推荐数量: {len(report.stocks)}")
for i, s in enumerate(report.stocks, 1):
    print(f"  #{i} [{s.signal_level}] {s.stock_code} {s.stock_name}"
          f" — 买:{s.buy_price:.2f} 目标:{s.target_price:.2f} 止损:{s.stop_loss:.2f}")

if not report.stocks:
    print("今日无推荐")
    sys.exit(0)

# Save
from src.strategy.push import push_recommendation, _format_wechat_message

results = push_recommendation(
    report,
    target="",
    save_dir="recommendations",
    save_json=True,
    print_terminal=False,
)
print(f"文本报告: {results['file']}")
print(f"JSON数据: {results['json']}")

# Generate push message
msg = _format_wechat_message(report)
push_file = Path("recommendations") / f"{trade_date}_push.txt"
push_file.write_text(msg, encoding="utf-8")
print(f"推送消息: {push_file}")

print(f"\n{'─'*60}")
print(msg)
print(f"{'─'*60}")
