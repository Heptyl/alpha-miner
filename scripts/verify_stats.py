#!/usr/bin/env python3
"""核实ML和因子统计"""
import json, numpy as np

with open('output/ml/full_ml_vs_factor_stats.json') as f:
    data = json.load(f)

ml = data['ml']
fa = data['factor']
print(f'ML有效天数: {len(ml)}')
print(f'因子有效天数: {len(fa)}')

top20 = np.array([d['top20_ret'] for d in ml])
bot20 = np.array([d['bot20_ret'] for d in ml])
mkt = np.array([d['market_ret'] for d in ml])
rand = np.array([d['rand20_ret'] for d in ml])

print(f'\n=== ML Top20 次日收益 ===')
print(f'  均值:   {top20.mean()*100:+.4f}%')
print(f'  中位数: {np.median(top20)*100:+.4f}%')
print(f'  标准差: {top20.std()*100:.4f}%')
print(f'  正收益: {(top20>0).sum()}/{len(top20)} ({(top20>0).mean()*100:.1f}%)')

print(f'\n=== ML Bottom20 ===')
print(f'  均值: {bot20.mean()*100:+.4f}%')

print(f'\n=== 全市场 ===')
print(f'  均值: {mkt.mean()*100:+.4f}%')

print(f'\n=== 随机20只 ===')
print(f'  均值: {rand.mean()*100:+.4f}%')

beat_mkt = sum(1 for d in ml if d['top20_win'])
print(f'\nML Top20跑赢市场: {beat_mkt}/{len(ml)} ({beat_mkt/len(ml)*100:.1f}%)')

ics = [d['ic'] for d in ml]
ics = [x for x in ics if not np.isnan(x)]
print(f'\nML IC: 均值={np.mean(ics):.4f} 标准差={np.std(ics):.4f} ICIR={np.mean(ics)/np.std(ics):.4f}')
print(f'  IC>0: {sum(1 for x in ics if x>0)}/{len(ics)} ({sum(1 for x in ics if x>0)/len(ics)*100:.1f}%)')

# 关键超额收益
print(f'\n=== 核心数据 ===')
print(f'ML Top20超额(vs市场): {(top20.mean()-mkt.mean())*100:+.4f}%/天')
print(f'ML Top20超额(vs随机): {(top20.mean()-rand.mean())*100:+.4f}%/天')
print(f'ML Top-Bot差: {(top20.mean()-bot20.mean())*100:+.4f}%/天')

fa_top = np.array([d['top20_ret'] for d in fa])
fa_mkt = np.array([d['market_ret'] for d in fa])
print(f'\n因子Top20超额(vs市场): {(fa_top.mean()-fa_mkt.mean())*100:+.4f}%/天')
print(f'因子Top20均值: {fa_top.mean()*100:+.4f}%')
print(f'ML/因子 Top20均值比: {top20.mean()/fa_top.mean():.1f}倍')

# 按月ML表现
ml_by_month = {}
for d in ml:
    m = d['date'][:7]
    ml_by_month.setdefault(m, []).append(d)

print(f'\n=== ML按月 ===')
for m in sorted(ml_by_month.keys()):
    days = ml_by_month[m]
    avg = np.mean([d['top20_ret'] for d in days])*100
    mavg = np.mean([d['market_ret'] for d in days])*100
    beat = sum(1 for d in days if d['top20_win'])/len(days)*100
    print(f'  {m}: Top20={avg:+.3f}% 市场={mavg:+.3f}% 超额={avg-mavg:+.3f}% 跑赢={beat:.0f}% ({len(days)}天)')
