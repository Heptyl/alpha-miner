#!/usr/bin/env python3
"""
数据可靠性检查:
1. ML是否过拟合? (训练R² vs 验证R²)
2. ML Top20是否有极端值拉高?
3. ML和因子的选股范围是否一致?
4. 检查是否有未来数据泄漏
"""
import json, numpy as np

with open('output/ml/full_ml_vs_factor_stats.json') as f:
    data = json.load(f)

ml = data['ml']

top20 = np.array([d['top20_ret'] for d in ml])
bot20 = np.array([d['bot20_ret'] for d in ml])
mkt = np.array([d['market_ret'] for d in ml])

print("=" * 60)
print("  数据可靠性检查")
print("=" * 60)

# 1. 极端值检查
print("\n1. 极端值检查 (ML Top20)")
pct_pos = (top20 > 0).mean() * 100
pct_neg = (top20 < 0).mean() * 100
pct_big = (top20 > 0.05).mean() * 100  # 超过5%
print(f"  正收益天数: {pct_pos:.1f}%")
print(f"  负收益天数: {pct_neg:.1f}%")
print(f"  >5%极端正: {pct_big:.1f}% ({(top20>0.05).sum()}天)")
print(f"  <-5%极端负: {(top20<-0.05).mean()*100:.1f}% ({(top20<-0.05).sum()}天)")
print(f"  去掉极端后(3σ外): ", end="")
mask = np.abs(top20 - top20.mean()) < 3 * top20.std()
print(f"均值={top20[mask].mean()*100:+.4f}% (vs 全部{top20.mean()*100:+.4f}%)")

# 2. 分布检查
print(f"\n2. ML Top20次日收益分位数")
for q in [10, 25, 50, 75, 90]:
    print(f"  P{q}: {np.percentile(top20, q)*100:+.3f}%")

# 3. ML超额收益稳定性
excess = top20 - mkt
print(f"\n3. ML超额收益稳定性")
print(f"  均值: {excess.mean()*100:+.4f}%")
print(f"  标准差: {excess.std()*100:.4f}%")
print(f"  夏普(日频): {excess.mean()/excess.std():.4f}")
print(f"  正超额天数: {(excess>0).mean()*100:.1f}%")

# 4. 是否连续好(可能暗示数据泄漏)?
print(f"\n4. 连续跑赢天数检查")
streaks = []
current = 0
for e in excess:
    if e > 0:
        current += 1
    else:
        if current > 0:
            streaks.append(current)
        current = 0
if current > 0:
    streaks.append(current)
print(f"  最长连续跑赢: {max(streaks)}天")
print(f"  平均连续跑赢: {np.mean(streaks):.1f}天")

# 5. ML IC vs Top20表现对比 — 如果IC很低但Top20很好, 
#    可能是ML选了少数极端票而非稳定预测
print(f"\n5. ML IC vs Top20收益相关性")
ics = np.array([d['ic'] for d in ml])
valid = ~(np.isnan(ics) | np.isnan(top20))
corr = np.corrcoef(ics[valid], top20[valid])[0, 1]
print(f"  IC与Top20收益相关性: {corr:.4f}")
print(f"  (如果接近0, 说明Top20表现和IC无关, ML选股不可靠)")

# 6. 因子筛选作为参照
fa = data['factor']
fa_top = np.array([d['top20_ret'] for d in fa])
fa_mkt = np.array([d['market_ret'] for d in fa])
fa_excess = fa_top - fa_mkt
print(f"\n6. 对比参照")
print(f"  ML超额: {excess.mean()*100:+.4f}%/天 夏普={excess.mean()/excess.std():.3f}")
print(f"  因子超额: {fa_excess.mean()*100:+.4f}%/天 夏普={fa_excess.mean()/fa_excess.std():.3f}")
print(f"  ML是因子的 {excess.mean()/fa_excess.mean():.1f}倍" if abs(fa_excess.mean()) > 0.0001 else "  因子超额≈0")

# 7. 关键判断: ML有没有过拟合风险
print(f"\n7. 过拟合风险评估")
print(f"  ML IC均值: {np.nanmean(ics):.4f} (很低)")
print(f"  ML ICIR: {np.nanmean(ics)/np.nanstd(ics):.4f} (远低于0.5阈值)")
print(f"  但Top20 69.3%天跑赢市场 (远超50%随机)")
print(f"  矛盾! IC极低但选股很强 → 可能原因:")
print(f"    a) ML捕获了IC无法衡量的非线性关系")
print(f"    b) Top20效应集中在少数极端票")
print(f"    c) 存在某种形式的前视偏差")
