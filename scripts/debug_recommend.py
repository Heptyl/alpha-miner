"""调试推荐引擎：逐步跟踪5/8推荐为什么为空。"""
import sqlite3
import sys
sys.path.insert(0, '/home/ccy/alpha-miner')

conn = sqlite3.connect('data/alpha_miner.db')

# === 1. 涨停池 ===
zt = conn.execute("""
    SELECT stock_code, name, industry, consecutive_zt, open_count, amount, circulation_mv
    FROM zt_pool WHERE trade_date = '2026-05-08'
    ORDER BY consecutive_zt DESC, amount DESC
""").fetchall()

print(f"[1] 涨停池总计: {len(zt)}只")

excluded_prefixes = ("688", "689", "8", "9")
excluded = []
tradeable = []
for r in zt:
    code = r[0]
    is_excluded = False
    for prefix in excluded_prefixes:
        if code.startswith(prefix):
            # "8"和"9"要特殊处理，只排除8/9开头的6位数
            if prefix in ("8", "9") and len(code) == 6:
                is_excluded = True
                break
            elif prefix in ("688", "689"):
                is_excluded = True
                break
    if is_excluded:
        excluded.append(r)
    else:
        tradeable.append(r)

print(f"    排除科创板/北交所: {len(excluded)}只")
print(f"    可交易候选: {len(tradeable)}只")
print()

# 前15只
print("    前15只可交易候选:")
for r in tradeable[:15]:
    code, name, ind, cons_zt, open_cnt, amt, cmv = r
    amt_yi = amt / 1e8 if amt else 0
    cmv_yi = cmv / 1e8 if cmv else 0
    print(f"      {code} {name:8s} {ind:8s} 连板={cons_zt} 炸板={open_cnt} 额={amt_yi:.1f}亿 市={cmv_yi:.1f}亿")

# === 2. 因子值 ===
fv_count = conn.execute("SELECT COUNT(DISTINCT stock_code) FROM factor_values WHERE trade_date = '2026-05-08'").fetchone()[0]
print(f"\n[2] 因子值: {fv_count}只有数据")

factors = conn.execute("SELECT DISTINCT factor_name FROM factor_values WHERE trade_date = '2026-05-08'").fetchall()
print(f"    因子列表: {[f[0] for f in factors]}")

# === 3. 可交易候选的因子覆盖 ===
tcodes = [r[0] for r in tradeable]
ph = ','.join(['?'] * len(tcodes))

fv_covered = conn.execute(
    f"SELECT DISTINCT stock_code FROM factor_values WHERE trade_date = '2026-05-08' AND stock_code IN ({ph})",
    tcodes
).fetchall()
fv_codes = set(r[0] for r in fv_covered)
print(f"    可交易候选有因子值: {len(fv_codes)}/{len(tcodes)}只")

# === 4. K线覆盖 ===
dp_covered = conn.execute(
    f"SELECT DISTINCT stock_code FROM daily_price WHERE trade_date = '2026-05-08' AND stock_code IN ({ph})",
    tcodes
).fetchall()
dp_codes = set(r[0] for r in dp_covered)
print(f"    可交易候选有K线: {len(dp_codes)}/{len(tcodes)}只")

# 无K线的
no_kline = [r for r in tradeable if r[0] not in dp_codes]
if no_kline:
    print(f"    无K线被排除的({len(no_kline)}只):")
    for r in no_kline[:10]:
        print(f"      {r[0]} {r[1]}")

# === 5. 模拟综合打分 ===
print(f"\n[3] 模拟综合打分 (权重: theme_crowding=0.25, leader_clarity=0.25, ...)")

# 拿所有因子值
fv_rows = conn.execute("""
    SELECT stock_code, factor_name, factor_value 
    FROM factor_values WHERE trade_date = '2026-05-08'
""").fetchall()
fv_map = {}
for code, fname, fval in fv_rows:
    if code not in fv_map:
        fv_map[code] = {}
    fv_map[code][fname] = fval

# 打分
DEFAULT_WEIGHTS = {
    "theme_crowding": 0.25,
    "leader_clarity": 0.25,
    "lhb_institution": 0.15,
    "turnover_rank": 0.10,
    "consecutive_board": 0.05,
    "momentum_score": 0.10,
    "volume_ratio": 0.10,
}

scored = []
for r in tradeable:
    code = r[0]
    factors = fv_map.get(code, {})
    
    score = 0.0
    total_weight = 0.0
    for factor_name, weight in DEFAULT_WEIGHTS.items():
        value = factors.get(factor_name, 0.0)
        if factor_name == "turnover_rank":
            value = 1.0 - min(max(value, 0), 1)
        elif factor_name == "lhb_institution":
            value = min(max(value / 5e8, -1.0), 1.0)
            value = max(value, 0)
        elif factor_name == "consecutive_board":
            value = min(value / 5.0, 1.0)
        score += weight * value
        total_weight += weight
    
    # 连板加分
    cons_zt = r[3]
    if cons_zt >= 2:
        bonus = min((cons_zt - 1) * 0.05, 0.15)
        score += bonus
    
    # 炸板扣分
    open_cnt = r[4]
    if open_cnt >= 2:
        score -= 0.10
    elif open_cnt == 1:
        score -= 0.05
    
    scored.append((code, r[1], r[2], cons_zt, open_cnt, r[5], score, factors))

scored.sort(key=lambda x: x[6], reverse=True)

print(f"    打分后(前20):")
for s in scored[:20]:
    code, name, ind, cons_zt, open_cnt, amt, score, factors = s
    amt_yi = amt / 1e8 if amt else 0
    f_str = " ".join(f"{k}={v:.2f}" for k, v in sorted(factors.items())[:4])
    print(f"      {code} {name:8s} 分={score:.3f} 连板={cons_zt} 炸板={open_cnt} 额={amt_yi:.1f}亿 | {f_str}")

# === 6. 过滤条件 ===
min_score = 0.35
max_open = 2
min_amount = 5e7

passed = []
for s in scored:
    code, name, ind, cons_zt, open_cnt, amt, score, factors = s
    if score < min_score:
        continue
    if open_cnt > max_open:
        continue
    if amt > 0 and amt < min_amount:
        continue
    # 无K线
    if code not in dp_codes:
        continue
    passed.append(s)

print(f"\n[4] 过滤后 (score>={min_score}, open<={max_open}, amount>={min_amount/1e8:.1f}亿, 有K线)")
print(f"    通过: {len(passed)}只")
for s in passed[:10]:
    code, name, ind, cons_zt, open_cnt, amt, score, factors = s
    amt_yi = amt / 1e8 if amt else 0
    print(f"      {code} {name:8s} 分={score:.3f}")

if not passed:
    # 诊断为什么全被过滤
    print(f"\n    === 诊断：为什么全部被过滤 ===")
    no_score = sum(1 for s in scored if s[6] < min_score)
    too_open = sum(1 for s in scored if s[4] > max_open)
    too_small = sum(1 for s in scored if 0 < s[5] < min_amount)
    no_kline_count = sum(1 for s in scored if s[0] not in dp_codes)
    
    print(f"    分数<{min_score}: {no_score}只")
    print(f"    炸板>{max_open}: {too_open}只")
    print(f"    成交额<{min_amount/1e8:.1f}亿: {too_small}只")
    print(f"    无K线: {no_kline_count}只")
    
    # 分数分布
    scores = [s[6] for s in scored]
    print(f"\n    分数分布: max={max(scores):.3f} min={min(scores):.3f} avg={sum(scores)/len(scores):.3f}")
    brackets = [0, 0.1, 0.2, 0.3, 0.35, 0.4, 0.5, 0.6, 1.0]
    for i in range(len(brackets)-1):
        cnt = sum(1 for s in scores if brackets[i] <= s < brackets[i+1])
        print(f"      [{brackets[i]:.2f}, {brackets[i+1]:.2f}): {cnt}只")
    
    # 看分数最高的一批
    print(f"\n    分数最高的10只详情:")
    for s in scored[:10]:
        code, name, ind, cons_zt, open_cnt, amt, score, factors = s
        amt_yi = amt / 1e8 if amt else 0
        reasons = []
        if score < min_score:
            reasons.append(f"分低({score:.3f}<{min_score})")
        if open_cnt > max_open:
            reasons.append(f"炸板多({open_cnt}>{max_open})")
        if amt > 0 and amt < min_amount:
            reasons.append(f"额小({amt_yi:.1f}亿<{min_amount/1e8:.1f}亿)")
        if code not in dp_codes:
            reasons.append("无K线")
        reason_str = " | ".join(reasons) if reasons else "PASS"
        print(f"      {code} {name:8s} 分={score:.3f} 炸={open_cnt} 额={amt_yi:.1f}亿 → {reason_str}")

conn.close()
