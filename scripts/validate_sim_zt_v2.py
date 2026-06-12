#!/usr/bin/env python3
"""
模拟涨停池 v2 — 精确匹配涨停价
涨停 = 收盘价 = 昨收 × 1.1 (主板) 或 昨收 × 1.2 (创业板)
"""
import sqlite3

db = sqlite3.connect('data/alpha_miner.db')

# 真实涨停池
zt_set = set((r[0], r[1]) for r in db.execute('SELECT trade_date, stock_code FROM zt_pool').fetchall())
zt_dates = set(r[0] for r in db.execute('SELECT DISTINCT trade_date FROM zt_pool').fetchall())
print(f"真实涨停池: {len(zt_set)}条, {len(zt_dates)}天")

# 方案1: 涨幅≥9.5% (原方案)
sim1 = db.execute("""
SELECT trade_date, stock_code FROM daily_price 
WHERE amount > 1000000 AND (close/pre_close - 1) * 100 >= 9.5
AND stock_code NOT LIKE '688%' AND stock_code NOT LIKE '689%' AND stock_code NOT LIKE '8%'
""").fetchall()
sim1_set = set(sim1)

# 方案2: 涨幅≥9.4% (放宽一点覆盖浮点误差)
sim2 = db.execute("""
SELECT trade_date, stock_code FROM daily_price 
WHERE amount > 1000000 AND (close/pre_close - 1) * 100 >= 9.4
AND stock_code NOT LIKE '688%' AND stock_code NOT LIKE '689%' AND stock_code NOT LIKE '8%'
""").fetchall()
sim2_set = set(sim2)

# 方案3: 涨幅≥9.4% + 成交额>500万(排除冷门)
sim3 = db.execute("""
SELECT trade_date, stock_code FROM daily_price 
WHERE amount > 5000000 AND (close/pre_close - 1) * 100 >= 9.4
AND stock_code NOT LIKE '688%' AND stock_code NOT LIKE '689%' AND stock_code NOT LIKE '8%'
""").fetchall()
sim3_set = set(sim3)

for name, sim_set, total in [("≥9.5%", sim1_set, sim1), ("≥9.4%", sim2_set, sim2), ("≥9.4%+500万", sim3_set, sim3)]:
    sim_in = set((r[0], r[1]) for r in total if r[0] in zt_dates)
    overlap = zt_set & sim_in
    recall = len(overlap) / len(zt_set) * 100 if zt_set else 0
    precision = len(overlap) / len(sim_in) * 100 if sim_in else 0
    days = len(set(r[0] for r in total))
    print(f"\n{name}: {len(total)}条/{days}天")
    print(f"  召回: {recall:.1f}%  精确: {precision:.1f}%")

# 方案4: 用涨停价精确判断
# 收盘价四舍五入到2位 = round(昨收 * 1.1, 2) 或 round(昨收 * 1.2, 2)
all_stocks = db.execute("""
SELECT trade_date, stock_code, close, pre_close
FROM daily_price 
WHERE amount > 1000000
AND stock_code NOT LIKE '688%' AND stock_code NOT LIKE '689%' AND stock_code NOT LIKE '8%'
AND pre_close > 0
""").fetchall()

sim4_set = set()
for date, code, close, pre_close in all_stocks:
    close = float(close)
    pre_close = float(pre_close)
    limit_up_10 = round(pre_close * 1.10, 2)
    limit_up_20 = round(pre_close * 1.20, 2)
    if abs(close - limit_up_10) <= 0.02 or abs(close - limit_up_20) <= 0.02:
        sim4_set.add((date, code))

sim4_in = set((d, c) for d, c in sim4_set if d in zt_dates)
overlap4 = zt_set & sim4_in
recall4 = len(overlap4) / len(zt_set) * 100 if zt_set else 0
prec4 = len(overlap4) / len(sim4_in) * 100 if sim4_in else 0
days4 = len(set(d for d, c in sim4_set))
print(f"\n涨停价精确匹配: {len(sim4_set)}条/{days4}天")
print(f"  召回: {recall4:.1f}%  精确: {prec4:.1f}%")

db.close()
