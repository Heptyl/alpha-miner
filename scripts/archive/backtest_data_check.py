"""回测引擎数据准备 — 检查数据完整性"""
import sqlite3
import json

DB = 'data/alpha_miner.db'
conn = sqlite3.connect(DB)

print("=" * 60)
print("回测数据完整性检查")
print("=" * 60)

# 1. daily_price每日覆盖
rows = conn.execute("""
    SELECT trade_date, COUNT(*) as cnt,
           COUNT(DISTINCT stock_code) as stocks
    FROM daily_price
    GROUP BY trade_date
    ORDER BY trade_date
""").fetchall()

dates = [r[0] for r in rows]
counts = [r[2] for r in rows]
print(f"\n[1] daily_price: {len(dates)}天, {dates[0]}~{dates[-1]}")
print(f"    每日股票数: min={min(counts)}, max={max(counts)}, avg={sum(counts)/len(counts):.0f}")

# 找异常日(股票数<4000)
low_days = [(d, c) for d, _, c in rows if c < 4000]
if low_days:
    print(f"    ⚠️ 股票数<4000的天数: {len(low_days)}")
    for d, c in low_days[:10]:
        print(f"      {d}: {c}只")
else:
    print(f"    ✓ 所有日期>=4000只")

# 2. zt_pool覆盖
zt_rows = conn.execute("""
    SELECT trade_date, COUNT(*) as cnt
    FROM zt_pool
    GROUP BY trade_date
    ORDER BY trade_date
""").fetchall()
zt_dates = set(r[0] for r in zt_rows)
print(f"\n[2] zt_pool: {len(zt_dates)}天, 涨停总数{sum(r[1] for r in zt_rows)}")

# 涨停池缺失的交易日(与daily_price对比)
all_dates = set(dates)
missing_zt = all_dates - zt_dates
if missing_zt:
    print(f"    ⚠️ 涨停池缺失天数: {len(missing_zt)}")
    for d in sorted(missing_zt)[:10]:
        print(f"      {d}")
else:
    print(f"    ✓ 涨停池覆盖所有交易日")

# 3. zt_pool字段检查
zt_cols = conn.execute("PRAGMA table_info(zt_pool)").fetchall()
zt_col_names = [c[1] for c in zt_cols]
print(f"\n[3] zt_pool字段: {zt_col_names}")

# 检查consecutive_zt字段
if 'consecutive_zt' in zt_col_names:
    consec = conn.execute("""
        SELECT consecutive_zt, COUNT(*) FROM zt_pool
        WHERE consecutive_zt IS NOT NULL
        GROUP BY consecutive_zt
        ORDER BY consecutive_zt
    """).fetchall()
    print(f"    连板分布: {dict(consec)}")
else:
    print(f"    ⚠️ 无consecutive_zt字段!")

# 检查stock_code格式
sample_codes = conn.execute("SELECT DISTINCT stock_code FROM zt_pool LIMIT 20").fetchall()
print(f"    代码样例: {[c[0] for c in sample_codes[:10]]}")

# 4. daily_price字段检查
dp_cols = conn.execute("PRAGMA table_info(daily_price)").fetchall()
dp_col_names = [c[1] for c in dp_cols]
print(f"\n[4] daily_price字段: {dp_col_names}")

# 5. 检查涨幅计算是否一致
sample_dp = conn.execute("""
    SELECT stock_code, trade_date, open, close, high, low, pre_close, 
           pct_chg, turnover, volume
    FROM daily_price 
    WHERE trade_date = '2026-05-15' AND stock_code = '000001'
""").fetchone()
if sample_dp:
    print(f"\n[5] 样本数据(000001 2026-05-15):")
    for i, col in enumerate(dp_col_names):
        if i < len(sample_dp):
            print(f"    {col}: {sample_dp[i]}")
    # 验证涨幅计算
    if sample_dp[4] and sample_dp[5]:  # pre_close and close
        calc_chg = (sample_dp[5] / sample_dp[6] - 1) * 100 if sample_dp[6] else None
        db_chg = sample_dp[7]
        print(f"    涨幅验证: 计算={calc_chg:.2f}%, DB={db_chg}%, 差异={abs(calc_chg-db_chg):.4f}%" if calc_chg and db_chg else "    无法验证")

# 6. 市场情绪需要的涨跌数据
print(f"\n[6] 涨跌统计(每天):")
sample_stats = conn.execute("""
    SELECT trade_date,
           SUM(CASE WHEN pct_chg > 0 THEN 1 ELSE 0 END) as up,
           SUM(CASE WHEN pct_chg < 0 THEN 1 ELSE 0 END) as down,
           SUM(CASE WHEN pct_chg >= 9.5 THEN 1 ELSE 0 END) as zt_approx,
           COUNT(*) as total
    FROM daily_price
    WHERE trade_date >= '2026-05-01'
    GROUP BY trade_date
    ORDER BY trade_date DESC
    LIMIT 10
""").fetchall()
for r in sample_stats:
    print(f"    {r[0]}: 涨{r[1]}/跌{r[2]}/涨停≈{r[3]}/总{r[4]}")

conn.close()
print("\n" + "=" * 60)
print("数据完整性检查完成")
