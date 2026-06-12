import sqlite3
conn = sqlite3.connect('data/alpha_miner.db')

print("=== DB tables ===")
tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()
for t in tables:
    cnt = conn.execute(f"SELECT COUNT(*) FROM {t[0]}").fetchone()[0]
    cols = conn.execute(f"PRAGMA table_info({t[0]})").fetchall()
    col_names = [c[1] for c in cols]
    print(f"  {t[0]}: {cnt}rows | {col_names}")

print()
# 检查基本面相关
basic_check = ['fundamental', 'financial', 'earning', 'balance', 'roe', 'profit', 'pe_', 'valuation']
for kw in basic_check:
    found = [t[0] for t in tables if kw in t[0].lower()]
    if found:
        print(f"  Found {kw}: {found}")

# 检查北向/解禁
for kw in ['north', 'hsgt', 'lockup', 'unlock', 'restrict']:
    found = [t[0] for t in tables if kw in t[0].lower()]
    if found:
        print(f"  Found {kw}: {found}")

# akshare能采什么
print()
print("=== akshare可用数据源 ===")
try:
    import akshare as ak
    funcs = [f for f in dir(ak) if 'stock' in f.lower() and ('individual' in f.lower() or 'financial' in f.lower() or 'north' in f.lower() or 'hsgt' in f.lower() or 'lockup' in f.lower() or 'unlock' in f.lower() or 'pe_' in f.lower())]
    for f in sorted(funcs)[:30]:
        print(f"  ak.{f}")
except Exception as e:
    print(f"  Error: {e}")
conn.close()
