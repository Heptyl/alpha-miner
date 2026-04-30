"""检查最新数据日期和复盘脚本逻辑"""
import sqlite3
conn = sqlite3.connect('data/alpha_miner.db')
c = conn.cursor()

# Check max dates
tables_cols = [
    ('daily_price', 'trade_date'),
    ('zt_pool', 'trade_date'),
    ('strong_pool', 'trade_date'),
    ('market_emotion', 'trade_date'),
    ('market_scripts', 'trade_date'),
    ('replay_log', 'trade_date'),
    ('lhb_detail', 'trade_date'),
    ('fund_flow', 'trade_date'),
]
for t, col in tables_cols:
    c.execute(f'SELECT COUNT(*), MAX({col}) FROM {t}')
    cnt, maxd = c.fetchone()
    print(f'{t}: count={cnt}, max_date={maxd}')

# Check if there's data for 2026-04-29
print("\n--- 2026-04-29 data ---")
for t, col in tables_cols:
    c.execute(f'SELECT COUNT(*) FROM {t} WHERE {col} = ?', ('2026-04-29',))
    cnt = c.fetchone()[0]
    print(f'{t}: count={cnt}')

# Check 2026-04-28
print("\n--- 2026-04-28 data ---")
for t, col in tables_cols:
    c.execute(f'SELECT COUNT(*) FROM {t} WHERE {col} = ?', ('2026-04-28',))
    cnt = c.fetchone()[0]
    print(f'{t}: count={cnt}')

conn.close()
