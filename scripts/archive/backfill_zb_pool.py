"""回补炸板池历史 — 从涨停池日期列表补充zb_pool"""
import sys, time
sys.path.insert(0, ".")
import sqlite3
from src.data.sources.akshare_zt_pool import fetch_zb_pool, save_zb_pool
from src.data.storage import Storage

conn = sqlite3.connect("data/alpha_miner.db")
db = Storage()

# 找有zt_pool但缺zb_pool的日期
zt_dates = set(r[0] for r in conn.execute("SELECT DISTINCT trade_date FROM zt_pool ORDER BY trade_date").fetchall())
zb_dates = set(r[0] for r in conn.execute("SELECT DISTINCT trade_date FROM zb_pool").fetchall())
missing = sorted(zt_dates - zb_dates)

print(f"涨停池: {len(zt_dates)}天, 炸板池: {len(zb_dates)}天, 缺失: {len(missing)}天")

if not missing:
    print("全部完整!")
    conn.close()
    exit()

total = 0
errors = 0
for i, date in enumerate(missing):
    try:
        df = fetch_zb_pool(date)
        if df is not None and not df.empty:
            cnt = save_zb_pool(df, db)
            total += cnt
            print(f"  [{i+1}/{len(missing)}] {date}: {cnt}只")
        else:
            print(f"  [{i+1}/{len(missing)}] {date}: 0只(当天无炸板)")
        time.sleep(0.3)  # 避免频率限制
    except Exception as e:
        errors += 1
        print(f"  [{i+1}/{len(missing)}] {date}: ERROR {str(e)[:50]}")
        if errors > 10:
            print("错误太多,停止")
            break

print(f"\n完成: {total}条新增, {errors}个错误")
conn.close()
