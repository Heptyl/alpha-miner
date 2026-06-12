#!/usr/bin/env python3
"""
模拟涨停池 v3 — 最终方案
策略: 涨停价精确匹配(精确率95.5%) + 涨幅≥9.4%兜底(捕获遗漏)
验证: 和真实涨停池25天交叉对比
"""
import sqlite3

db = sqlite3.connect('data/alpha_miner.db')

# 真实涨停池
zt_set = set((r[0], r[1]) for r in db.execute('SELECT trade_date, stock_code FROM zt_pool').fetchall())
zt_dates = set(r[0] for r in db.execute('SELECT DISTINCT trade_date FROM zt_pool').fetchall())
print(f"真实涨停池: {len(zt_set)}条, {len(zt_dates)}天")

# 获取所有主板+创业板日K线
all_stocks = db.execute("""
SELECT trade_date, stock_code, close, pre_close, amount
FROM daily_price 
WHERE amount > 1000000 AND pre_close > 0
AND stock_code NOT LIKE '688%' AND stock_code NOT LIKE '689%' 
AND stock_code NOT LIKE '8%' AND stock_code NOT LIKE '9%'
""").fetchall()

print(f"候选K线: {len(all_stocks)}条")

sim_set = set()
for date, code, close, pre_close, amount in all_stocks:
    close = float(close)
    pre_close = float(pre_close)
    
    # 创业板(300/301)涨停20%
    if code.startswith('30'):
        limit_up = round(pre_close * 1.20, 2)
        if abs(close - limit_up) <= 0.02:
            sim_set.add((date, code))
            continue
    # 主板涨停10%
    else:
        limit_up = round(pre_close * 1.10, 2)
        if abs(close - limit_up) <= 0.02:
            sim_set.add((date, code))
            continue
    
    # 兜底: 涨幅≥9.8%且收盘接近涨停价(误差<0.1元)
    chg = (close / pre_close - 1) * 100
    if code.startswith('30'):
        if chg >= 19.8 and abs(close - round(pre_close * 1.20, 2)) < 0.10:
            sim_set.add((date, code))
    else:
        if chg >= 9.8 and abs(close - round(pre_close * 1.10, 2)) < 0.10:
            sim_set.add((date, code))

# 交叉验证
sim_in_period = set((d, c) for d, c in sim_set if d in zt_dates)
overlap = zt_set & sim_in_period

recall = len(overlap) / len(zt_set) * 100
precision = len(overlap) / len(sim_in_period) * 100 if sim_in_period else 0
days = len(set(d for d, c in sim_set))

print(f"\n模拟涨停池: {len(sim_set)}条, {days}天")
print(f"\n25天交叉验证:")
print(f"  召回率: {recall:.1f}% (真实涨停有多少被捕获)")
print(f"  精确率: {precision:.1f}% (模拟的有多少是真的)")

# F1
f1 = 2 * recall * precision / (recall + precision) if (recall + precision) > 0 else 0
print(f"  F1分数: {f1:.1f}%")

# 按月统计模拟涨停数
from collections import Counter
monthly = Counter()
for d, c in sim_set:
    monthly[d[:7]] += 1

print(f"\n按月分布:")
for m in sorted(monthly.keys()):
    print(f"  {m}: {monthly[m]}条")

# 写入数据库
print("\n写入sim_zt_pool表...")
c = db.cursor()
c.execute("DROP TABLE IF EXISTS sim_zt_pool")
c.execute("""
CREATE TABLE sim_zt_pool (
    trade_date TEXT,
    stock_code TEXT,
    close_price REAL,
    pre_close REAL,
    change_pct REAL,
    amount REAL,
    is_chinext INTEGER DEFAULT 0,
    PRIMARY KEY (trade_date, stock_code)
)
""")

inserted = 0
for date, code in sim_set:
    row = db.execute("""
        SELECT close, pre_close, amount 
        FROM daily_price WHERE stock_code=? AND trade_date=?
    """, (code, date)).fetchone()
    if row:
        close, pre_close, amt = float(row[0]), float(row[1]), float(row[2])
        chg = round((close / pre_close - 1) * 100, 2)
        is_30 = 1 if code.startswith('30') else 0
        c.execute("""INSERT OR IGNORE INTO sim_zt_pool 
            (trade_date, stock_code, close_price, pre_close, change_pct, amount, is_chinext)
            VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (date, code, close, pre_close, chg, amt, is_30))
        inserted += 1

db.commit()
print(f"写入完成: {inserted}条")

db.close()
