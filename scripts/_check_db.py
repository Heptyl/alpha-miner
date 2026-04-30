#!/usr/bin/env python3
import sqlite3
from datetime import datetime
conn = sqlite3.connect('data/alpha_miner.db')
row = conn.execute('SELECT MAX(trade_date) FROM daily_price').fetchone()
print('Latest trade_date:', row[0] if row else 'None')
print('Today:', datetime.now().strftime('%Y-%m-%d'))
# Check if today is weekday
d = datetime.now()
print('Weekday:', d.weekday(), '(0=Mon, 6=Sun)')
print('Is weekend:', d.weekday() >= 5)
conn.close()
