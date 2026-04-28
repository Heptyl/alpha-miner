"""检查当前数据覆盖情况。"""
import sqlite3

conn = sqlite3.connect("data/alpha_miner.db")

for t in ["daily_price", "zt_pool", "zb_pool", "strong_pool", "fund_flow", "lhb_detail", "news", "concept_mapping", "factor_values", "ic_series", "drift_events", "regime_state"]:
    try:
        r = conn.execute(f"SELECT COUNT(*), COUNT(DISTINCT trade_date), MIN(trade_date), MAX(trade_date) FROM {t}").fetchone()
        print(f"{t:20s}: {r[0]:>6} rows, {r[1]:>3} days, {r[2]} ~ {r[3]}")
    except Exception:
        r = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()
        print(f"{t:20s}: {r[0]:>6} rows")

r = conn.execute("SELECT COUNT(DISTINCT stock_code) FROM daily_price").fetchone()
print(f"\ndaily_price: {r[0]} distinct stocks")

# factor_values 详情
r = conn.execute("SELECT factor_name, COUNT(*), COUNT(DISTINCT trade_date), MIN(trade_date), MAX(trade_date) FROM factor_values GROUP BY factor_name").fetchall()
print("\nfactor_values breakdown:")
for row in r:
    print(f"  {row[0]:30s}: {row[1]:>6} rows, {row[2]:>3} days, {row[3]} ~ {row[4]}")

conn.close()
