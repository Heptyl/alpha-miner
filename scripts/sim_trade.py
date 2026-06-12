#!/usr/bin/env python3
"""模拟交易回测 — 用历史数据验证推荐策略"""
import sqlite3
from collections import defaultdict
import sys

conn = sqlite3.connect('data/alpha_miner.db')
c = cursor = conn.cursor()

trade_date = '2026-04-24'
next_date = '2026-04-28'

print("=" * 70)
print("  Alpha Miner 模拟交易报告")
print(f"  T日: {trade_date}(周四) 收盘后推荐  |  T+1: {next_date}(周一) 执行")
print("=" * 70)

# === 因子权重 ===
factor_weights = {
    'theme_crowding': 0.25, 'leader_clarity': 0.20, 'consecutive_board': 0.15,
    'lhb_institution': 0.15, 'turnover_rank': -0.10, 'main_flow_intensity': 0.10,
    'narrative_velocity': 0.05,
}

rows = c.execute('SELECT stock_code, factor_name, factor_value FROM factor_values WHERE trade_date=?', (trade_date,)).fetchall()
stock_factors = defaultdict(dict)
for code, fname, fval in rows:
    if fval is not None:
        stock_factors[code][fname] = fval

candidates = {}
for code, factors in stock_factors.items():
    if len(factors) >= 2:
        score = sum(w * factors.get(f, 0) for f, w in factor_weights.items() if f in factors)
        candidates[code] = score

ranked = sorted(candidates.items(), key=lambda x: x[1], reverse=True)

def get_name(code):
    for t in ['zt_pool', 'strong_pool']:
        r = c.execute(f'SELECT name FROM {t} WHERE stock_code=? LIMIT 1', (code,)).fetchone()
        if r: return r[0]
    return code

# === 策略A: 因子TOP5 日内交易 ===
print("\n[策略A] 因子综合TOP5 -> 次日竞价买入 -> 收盘卖出")
print("-" * 70)

total_a = 0; cnt_a = 0
for code, score in ranked[:5]:
    name = get_name(code)
    c24 = c.execute('SELECT close FROM daily_price WHERE stock_code=? AND trade_date=?', (code, trade_date)).fetchone()
    c28 = c.execute('SELECT open, close FROM daily_price WHERE stock_code=? AND trade_date=?', (code, next_date)).fetchone()
    if c24 and c28:
        buy = c28[0]; sell = c28[1]
        pnl = (sell - buy) / buy * 100
        total_a += pnl; cnt_a += 1
        tag = "OK" if pnl > 0 else "XX"
        print(f"  {tag} {code} {name}: 买{buy:.2f} 卖{sell:.2f} -> {pnl:+.1f}%")

if cnt_a:
    print(f"  >>> 策略A 等权均收: {total_a/cnt_a:+.1f}%")

# === 策略B: 连板≥2 买入 ===
print("\n[策略B] 连板>=2 -> 次日开盘买入 -> 收盘卖出")
print("-" * 70)

total_b = 0; cnt_b = 0
zt = c.execute('SELECT stock_code, name, consecutive_zt FROM zt_pool WHERE trade_date=? AND consecutive_zt>=2 ORDER BY consecutive_zt DESC', (trade_date,)).fetchall()
for code, name, nb in zt:
    c24 = c.execute('SELECT close FROM daily_price WHERE stock_code=? AND trade_date=?', (code, trade_date)).fetchone()
    c28 = c.execute('SELECT open, close FROM daily_price WHERE stock_code=? AND trade_date=?', (code, next_date)).fetchone()
    if c24 and c28:
        buy = c28[0]; sell = c28[1]
        pnl = (sell - buy) / buy * 100
        total_b += pnl; cnt_b += 1
        tag = "OK" if pnl > 0 else "XX"
        print(f"  {tag} {code} {name}({nb}板): {buy:.2f}->{sell:.2f} {pnl:+.1f}%")

if cnt_b:
    print(f"  >>> 策略B 等权均收: {total_b/cnt_b:+.1f}%")

# === 策略C: 因子+涨停 交集 ===
print("\n[策略C] 因子TOP10 AND 涨停池 -> 次日开盘买入")
print("-" * 70)

top_codes = set(code for code, _ in ranked[:10])
zt_codes = set(r[0] for r in c.execute('SELECT stock_code FROM zt_pool WHERE trade_date=?', (trade_date,)).fetchall())
inter = top_codes & zt_codes

total_c = 0; cnt_c = 0
for code in inter:
    name = get_name(code)
    c24 = c.execute('SELECT close FROM daily_price WHERE stock_code=? AND trade_date=?', (code, trade_date)).fetchone()
    c28 = c.execute('SELECT open, close FROM daily_price WHERE stock_code=? AND trade_date=?', (code, next_date)).fetchone()
    if c24 and c28:
        buy = c28[0]; sell = c28[1]
        pnl = (sell - buy) / buy * 100
        total_c += pnl; cnt_c += 1
        tag = "OK" if pnl > 0 else "XX"
        print(f"  {tag} {code} {name}: {buy:.2f}->{sell:.2f} {pnl:+.1f}%")

if cnt_c:
    print(f"  >>> 策略C 等权均收: {total_c/cnt_c:+.1f}%")

# === 最优策略: 因子TOP5次日全持仓 ===
print("\n[最优复盘] 因子TOP5 次日收益明细")
print("-" * 70)
print(f"  {'#':>3} {'代码':>8} {'名称':<8} {'T日收':>8} {'T+1开':>8} {'T+1高':>8} {'T+1低':>8} {'T+1收':>8} {'日收%':>7}")
for i, (code, score) in enumerate(ranked[:5], 1):
    name = get_name(code)
    c24 = c.execute('SELECT close FROM daily_price WHERE stock_code=? AND trade_date=?', (code, trade_date)).fetchone()
    c28 = c.execute('SELECT open, high, low, close FROM daily_price WHERE stock_code=? AND trade_date=?', (code, next_date)).fetchone()
    if c24 and c28:
        prev = c24[0]
        cl = (c28[3]-prev)/prev*100
        print(f"  {i:>3} {code:>8} {name:<8} {c24[0]:>8.2f} {c28[0]:>8.2f} {c28[1]:>8.2f} {c28[2]:>8.2f} {c28[3]:>8.2f} {cl:>+7.1f}%")

print("\n" + "=" * 70)
print("  总结")
print("=" * 70)
print(f"  策略A(因子TOP5日内): 均收 {total_a/cnt_a:+.1f}%" if cnt_a else "  策略A: 无数据")
print(f"  策略B(连板>=2日内):  均收 {total_b/cnt_b:+.1f}%" if cnt_b else "  策略B: 无数据")
print(f"  策略C(因子+涨停):    均收 {total_c/cnt_c:+.1f}%" if cnt_c else "  策略C: 无交集")
print()
print("  注意: 单日回测, 不具统计意义。需跑更多交易日验证。")

conn.close()
