#!/usr/bin/env python3
"""Fill missing 5-20 daily_price using akshare stock_zh_a_daily (Sina)."""
import sqlite3
import time
import akshare as ak

DB = '/home/ccy/alpha-miner/data/alpha_miner.db'
DATE = '2026-05-20'
DATE_FMT = '20260520'

conn = sqlite3.connect(DB)

existing = set(r[0] for r in conn.execute(
    "SELECT DISTINCT stock_code FROM daily_price WHERE trade_date=?", (DATE,)
).fetchall())

# 取DB所有A股code
all_codes = sorted(set(r[0] for r in conn.execute('SELECT DISTINCT stock_code FROM daily_price').fetchall()
                 if r[0].startswith(('6','0','3')) and len(r[0])==6))

missing = [c for c in all_codes if c not in existing]
print(f'已有{len(existing)}, 全量{len(all_codes)}, 缺{len(missing)}')

def to_sina(code):
    return f'sh{code}' if code.startswith('6') else f'sz{code}'

t0 = time.time()
inserted = 0
errors = 0
consec_err = 0

for i, code in enumerate(missing):
    try:
        df = ak.stock_zh_a_daily(symbol=to_sina(code), start_date=DATE_FMT, end_date=DATE_FMT, adjust='qfq')
        if df is None or len(df) == 0:
            continue
        row = df.iloc[0]
        vol = float(row.get('volume', 0))
        if vol <= 0:
            continue
        conn.execute(
            "INSERT OR IGNORE INTO daily_price (stock_code,trade_date,open,high,low,close,pre_close,volume,amount,turnover_rate) VALUES (?,?,?,?,?,?,?,?,?,?)",
            (code, DATE, float(row['open']), float(row['high']), float(row['low']),
             float(row['close']), 0, vol,
             float(row.get('amount', 0)), float(row.get('turnover', 0)))
        )
        inserted += 1
        consec_err = 0
    except Exception as e:
        errors += 1
        consec_err += 1
        if consec_err >= 30:
            print(f'连续{consec_err}次错误 终止于{i} code={code}')
            break

    if (i + 1) % 100 == 0:
        conn.commit()
        print(f'  {i+1}/{len(missing)} 插入{inserted} 错误{errors} {time.time()-t0:.0f}s')
        time.sleep(1)

conn.commit()
total = conn.execute("SELECT COUNT(*) FROM daily_price WHERE trade_date=?", (DATE,)).fetchone()[0]
print(f'完成! 插入{inserted} 错误{errors} 总计{total}条 耗时{time.time()-t0:.0f}s')
conn.close()
