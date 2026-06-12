#!/usr/bin/env python3
"""验证用日K线涨幅模拟涨停池的准确性"""
import sqlite3

db = sqlite3.connect('data/alpha_miner.db')

# 真实涨停池
zt_set = set((r[0], r[1]) for r in db.execute('SELECT trade_date, stock_code FROM zt_pool').fetchall())
zt_dates = set(r[0] for r in db.execute('SELECT DISTINCT trade_date FROM zt_pool').fetchall())
print(f"真实涨停池: {len(zt_set)}条, {len(zt_dates)}天")

# 模拟涨停: 主板≥9.5% 创业板≥19.5%, 排除科创板/北交所
simulated = db.execute("""
SELECT trade_date, stock_code, 
       ROUND((close/pre_close - 1) * 100, 2) as chg
FROM daily_price 
WHERE amount > 1000000
  AND (close/pre_close - 1) * 100 >= 9.5
  AND stock_code NOT LIKE '688%' 
  AND stock_code NOT LIKE '689%' 
  AND stock_code NOT LIKE '8%'
""").fetchall()

sim_set = set((r[0], r[1]) for r in simulated)
sim_in_period = set((r[0], r[1]) for r in simulated if r[0] in zt_dates)
overlap = zt_set & sim_in_period

recall = len(overlap) / len(zt_set) * 100 if zt_set else 0
precision = len(overlap) / len(sim_in_period) * 100 if sim_in_period else 0

print(f"\n模拟涨停: {len(simulated)}条, {len(set(r[0] for r in simulated))}天")
print(f"\n25天交叉验证:")
print(f"  真实: {len(zt_set)}条")
print(f"  模拟: {len(sim_in_period)}条")
print(f"  重叠: {len(overlap)}条")
print(f"  召回率(真实→模拟): {recall:.1f}%")
print(f"  精确率(模拟→真实): {precision:.1f}%")

# 漏掉的
missed = zt_set - sim_in_period
if missed:
    print(f"\n漏掉{len(missed)}条:")
    for date, code in list(missed)[:10]:
        row = db.execute("SELECT (close/pre_close-1)*100 FROM daily_price WHERE stock_code=? AND trade_date=?", (code, date)).fetchone()
        if row:
            print(f"  {date} {code} 涨幅{row[0]:.2f}%")
        else:
            print(f"  {date} {code} 无日K线")

# 多出来的(精确率低的原因)
extra = sim_in_period - zt_set
print(f"\n多出{len(extra)}条(模拟有但真实无):")
# 按涨幅分组
ranges = [(9.5, 9.9, "未封10%"), (9.9, 10.1, "封住10%"), (10.1, 15, "超10%"), (15, 19.5, "10~19.5%"), (19.5, 99, "≥19.5%")]
for lo, hi, label in ranges:
    cnt = 0
    for date, code in extra:
        row = db.execute("SELECT (close/pre_close-1)*100 FROM daily_price WHERE stock_code=? AND trade_date=?", (code, date)).fetchone()
        if row and lo <= row[0] < hi:
            cnt += 1
    print(f"  {label}: {cnt}条")

db.close()
