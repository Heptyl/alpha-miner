import sqlite3
conn = sqlite3.connect('data/alpha_miner.db')

# 5/8 daily_price 总量和股票数
dp = conn.execute("""
    SELECT COUNT(*), COUNT(DISTINCT stock_code) 
    FROM daily_price WHERE trade_date = '2026-05-08'
""").fetchone()
print(f"daily_price 5/8: {dp[0]}条, {dp[1]}只股票")

# 对比前几天
for d in ['2026-05-07', '2026-05-06', '2026-05-05']:
    r = conn.execute(f"SELECT COUNT(*), COUNT(DISTINCT stock_code) FROM daily_price WHERE trade_date = '{d}'").fetchone()
    print(f"  {d}: {r[0]}条, {r[1]}只")

# 全市场应该有多少只（用最多的一天对比）
max_day = conn.execute("SELECT trade_date, COUNT(DISTINCT stock_code) as cnt FROM daily_price GROUP BY trade_date ORDER BY cnt DESC LIMIT 5").fetchall()
print("\nDB中每天股票数最多的5天:")
for row in max_day:
    print(f"  {row[0]}: {row[1]}只")
print(f"\n5/8有 {dp[1]}只")

# 涨停池5/8有多少只没K线
zt_no_kline = conn.execute("""
    SELECT z.stock_code, z.name 
    FROM zt_pool z 
    LEFT JOIN daily_price d ON z.stock_code = d.stock_code AND d.trade_date = '2026-05-08'
    WHERE z.trade_date = '2026-05-08' AND d.stock_code IS NULL
""").fetchall()
print(f"\n涨停池98只中无K线: {len(zt_no_kline)}只")
for r in zt_no_kline:
    print(f"  {r[0]} {r[1]}")

# 强势股池无K线
sp_no_kline = conn.execute("""
    SELECT COUNT(DISTINCT s.stock_code)
    FROM strong_pool s 
    LEFT JOIN daily_price d ON s.stock_code = d.stock_code AND d.trade_date = '2026-05-08'
    WHERE s.trade_date = '2026-05-08' AND d.stock_code IS NULL
""").fetchone()[0]
print(f"\n强势股池无K线: {sp_no_kline}只")

# 因子值5/8有多少只
fv = conn.execute("SELECT COUNT(DISTINCT stock_code) FROM factor_values WHERE trade_date = '2026-05-08'").fetchone()[0]
print(f"因子值5/8: {fv}只")

conn.close()
