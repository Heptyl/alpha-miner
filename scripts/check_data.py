#!/usr/bin/env python3
import sqlite3
conn = sqlite3.connect('data/alpha_miner.db')
row = conn.execute('SELECT MAX(trade_date) FROM daily_price').fetchone()
print(f'daily_price 最新日期: {row[0]}')
row = conn.execute('SELECT COUNT(*) FROM daily_price WHERE trade_date = "2026-04-30"').fetchone()
print(f'2026-04-30 日K线数量: {row[0]}')
row = conn.execute('SELECT COUNT(*) FROM factor_values WHERE trade_date = "2026-04-30"').fetchone()
print(f'2026-04-30 因子值数量: {row[0]}')
conn.close()
