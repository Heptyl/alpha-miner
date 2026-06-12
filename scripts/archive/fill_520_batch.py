#!/usr/bin/env python3
"""Fill 5-20 daily_price using akshare stock_zh_a_daily (Sina source) — batch mode."""
import sqlite3
import time
import akshare as ak

DB = '/home/ccy/alpha-miner/data/alpha_miner.db'
DATE = '2026-05-20'
DATE_FMT = '20260520'

conn = sqlite3.connect(DB)

# 已有
existing = set(r[0] for r in conn.execute(
    "SELECT DISTINCT stock_code FROM daily_price WHERE trade_date=?", (DATE,)
).fetchall())
print(f'已有: {len(existing)}只')

# 获取全市场A股代码列表
all_codes = []
df = ak.stock_zh_a_spot_em()
if df is not None and len(df) > 0:
    all_codes = df['代码'].tolist()
    print(f'spot_em获取到: {len(all_codes)}只')
else:
    # fallback: 从DB取所有出现的stock_code
    all_codes = [r[0] for r in conn.execute("SELECT DISTINCT stock_code FROM daily_price").fetchall()]
    print(f'DB获取到: {len(all_codes)}只')

missing = [c for c in all_codes if c not in existing and len(c) == 6]
print(f'需补: {len(missing)}只')

# stock_zh_a_daily需要 sz000001 或 sh600000 格式
def to_sina_code(code):
    if code.startswith('6'):
        return f'sh{code}'
    else:
        return f'sz{code}'

t0 = time.time()
inserted = 0
errors = 0
consecutive_errors = 0

for i, code in enumerate(missing):
    sina = to_sina_code(code)
    try:
        df = ak.stock_zh_a_daily(symbol=sina, start_date=DATE_FMT, end_date=DATE_FMT, adjust='qfq')
        if df is None or len(df) == 0:
            continue
        
        row = df.iloc[0]
        conn.execute(
            "INSERT OR IGNORE INTO daily_price (stock_code,trade_date,open,high,low,close,pre_close,volume,amount,turnover_rate) VALUES (?,?,?,?,?,?,?,?,?,?)",
            (code, DATE, float(row['open']), float(row['high']), float(row['low']),
             float(row['close']), 0, float(row.get('volume', 0)),
             float(row.get('amount', 0)), float(row.get('turnover', 0)) if 'turnover' in row else 0)
        )
        inserted += 1
        consecutive_errors = 0
    except Exception as e:
        errors += 1
        consecutive_errors += 1
        if consecutive_errors >= 30:
            print(f'连续{consecutive_errors}次错误，终止')
            break
    
    if (i + 1) % 200 == 0:
        conn.commit()
        elapsed = time.time() - t0
        print(f'  进度: {i+1}/{len(missing)} 插入{inserted} 错误{errors} {elapsed:.0f}s')
    
    # 控速
    if (i + 1) % 50 == 0:
        time.sleep(0.5)

conn.commit()
total = conn.execute("SELECT COUNT(*) FROM daily_price WHERE trade_date=?", (DATE,)).fetchone()[0]
print(f'完成! 插入{inserted} 错误{errors} 总计{total}条 耗时{time.time()-t0:.0f}s')
conn.close()
