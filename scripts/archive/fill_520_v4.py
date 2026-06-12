#!/usr/bin/env python3
"""Fill missing 5-20 daily_price — concurrent akshare stock_zh_a_daily."""
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import akshare as ak

DB = '/home/ccy/alpha-miner/data/alpha_miner.db'
DATE = '2026-05-20'
DATE_FMT = '20260520'
WORKERS = 5  # 并发数，太高会被封

conn = sqlite3.connect(DB)
existing = set(r[0] for r in conn.execute(
    "SELECT DISTINCT stock_code FROM daily_price WHERE trade_date=?", (DATE,)
).fetchall())

all_codes = sorted(set(r[0] for r in conn.execute('SELECT DISTINCT stock_code FROM daily_price').fetchall()
                 if r[0].startswith(('6','0','3')) and len(r[0])==6))
missing = [c for c in all_codes if c not in existing]
print(f'已有{len(existing)}, 缺{len(missing)}只')

def to_sina(code):
    return f'sh{code}' if code.startswith('6') else f'sz{code}'

def fetch_one(code):
    try:
        df = ak.stock_zh_a_daily(symbol=to_sina(code), start_date=DATE_FMT, end_date=DATE_FMT, adjust='qfq')
        if df is None or len(df) == 0:
            return code, None
        row = df.iloc[0]
        vol = float(row.get('volume', 0))
        if vol <= 0:
            return code, None
        return code, {
            'open': float(row['open']),
            'high': float(row['high']),
            'low': float(row['low']),
            'close': float(row['close']),
            'volume': vol,
            'amount': float(row.get('amount', 0)),
            'turnover': float(row.get('turnover', 0)),
        }
    except:
        return code, None

t0 = time.time()
inserted = 0
errors = 0
done = 0

with ThreadPoolExecutor(max_workers=WORKERS) as pool:
    futures = {pool.submit(fetch_one, code): code for code in missing}
    for fut in as_completed(futures):
        done += 1
        code, data = fut.result()
        if data:
            conn.execute(
                "INSERT OR IGNORE INTO daily_price (stock_code,trade_date,open,high,low,close,pre_close,volume,amount,turnover_rate) VALUES (?,?,?,?,?,?,?,?,?,?)",
                (code, DATE, data['open'], data['high'], data['low'],
                 data['close'], 0, data['volume'], data['amount'], data['turnover'])
            )
            inserted += 1
        else:
            errors += 1

        if done % 100 == 0:
            conn.commit()
            print(f'  {done}/{len(missing)} 插入{inserted} 错误{errors} {time.time()-t0:.0f}s')

conn.commit()
total = conn.execute("SELECT COUNT(DISTINCT stock_code) FROM daily_price WHERE trade_date=?", (DATE,)).fetchone()[0]
print(f'完成! 插入{inserted} 错误{errors} 总{total}只 耗时{time.time()-t0:.0f}s')
conn.close()
