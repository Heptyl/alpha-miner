"""快速修复数据检查 — daily_price没有pct_chg，需确认字段"""
import sqlite3

DB = 'data/alpha_miner.db'
conn = sqlite3.connect(DB)

# 字段确认
cols = conn.execute("PRAGMA table_info(daily_price)").fetchall()
print("daily_price字段:", [c[1] for c in cols])

# 看样本
row = conn.execute("""
    SELECT stock_code, trade_date, open, high, low, close, pre_close, volume, amount, turnover_rate
    FROM daily_price WHERE stock_code='000001' ORDER BY trade_date DESC LIMIT 5
""").fetchall()
for r in row:
    calc_chg = (r[5] / r[6] - 1) * 100 if r[6] else None
    print(f"  {r[0]} {r[1]} open={r[2]} close={r[5]} pre_close={r[6]} 涨幅={calc_chg:.2f}% vol={r[7]} amt={r[8]} turnover={r[9]}")

# 涨幅统计 — 用 (close/pre_close - 1)*100 计算
print("\n涨跌统计(近10天):")
rows = conn.execute("""
    SELECT trade_date,
           SUM(CASE WHEN close > pre_close THEN 1 ELSE 0 END) as up,
           SUM(CASE WHEN close < pre_close THEN 1 ELSE 0 END) as down,
           SUM(CASE WHEN (close/pre_close - 1)*100 >= 9.5 THEN 1 ELSE 0 END) as zt_approx,
           COUNT(*) as total
    FROM daily_price
    WHERE trade_date >= '2026-05-01'
    GROUP BY trade_date
    ORDER BY trade_date DESC
    LIMIT 10
""").fetchall()
for r in rows:
    print(f"  {r[0]}: 涨{r[1]}/跌{r[2]}/涨停≈{r[3]}/总{r[4]}")

# 确认pre_close是否可靠
null_preclose = conn.execute("""
    SELECT COUNT(*) FROM daily_price WHERE pre_close IS NULL OR pre_close = 0
""").fetchone()[0]
total = conn.execute("SELECT COUNT(*) FROM daily_price").fetchone()[0]
print(f"\npre_close空/零: {null_preclose}/{total} ({null_preclose/total*100:.2f}%)")

conn.close()
