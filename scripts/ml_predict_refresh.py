#!/usr/bin/env python3
"""开盘前ML预测刷新 — 9:00 cron触发"""
import sys, json, sqlite3
from pathlib import Path
from datetime import datetime

sys.path.insert(0, "/home/ccy/alpha-miner")
os_chdir = Path("/home/ccy/alpha-miner")

import os
os.chdir(os_chdir)

now = datetime.now()
weekday = now.weekday()

print(f"{'='*45}")
print(f"  ML预测刷新 {now.strftime('%Y-%m-%d %H:%M')} (周{'一二三四五六日'[weekday]})")
print(f"{'='*45}")

if weekday >= 5:
    print("周末跳过")
    exit()

# 数据最新日期
conn = sqlite3.connect("data/alpha_miner.db")
latest = conn.execute("SELECT MAX(trade_date) FROM daily_price").fetchone()[0]
conn.close()
print(f"数据截止: {latest}")

# 生成预测
print("生成ML预测...")
from src.trader.paper_trader import ml_predict_on_date
results = ml_predict_on_date(latest)

if results:
    pred_path = Path("output/signals/ml_predictions.json")
    pred_path.parent.mkdir(parents=True, exist_ok=True)
    pred_path.write_text(json.dumps({
        "date": latest,
        "total_stocks": len(results),
        "generated_at": datetime.now().isoformat(),
        "top7": results[:7],
        "all_top": results,
    }, ensure_ascii=False, indent=2))

    print(f"完成: {len(results)}只候选")
    for r in results[:5]:
        print(f"  {r.get('code','?')} score={r.get('score',0):.4f}")
else:
    print("ML预测返回空! 需检查模型和数据")
