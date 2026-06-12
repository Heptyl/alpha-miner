"""深入分析回测结果 — 找出问题根因"""
import json
from pathlib import Path
from collections import defaultdict
import statistics

output = Path("output/backtest/backtest_results.json")
data = json.loads(output.read_text(encoding="utf-8"))
trades = data['trades']

print("=" * 70)
print("回测深度分析 — 找问题根因")
print("=" * 70)

# 1. 策略B卖出原因细分盈亏
print("\n[1] 策略B卖出原因 × 盈亏分析:")
b_trades = [t for t in trades if t['strategy'] == 'B']
by_reason = defaultdict(list)
for t in b_trades:
    reason = t['reason'].split(':')[0]
    by_reason[reason].append(t)

for reason, ts in sorted(by_reason.items(), key=lambda x: -len(x[1])):
    pnls = [t['pnl'] for t in ts]
    pcts = [t['pnl_pct'] for t in ts]
    wins = sum(1 for p in pnls if p > 0)
    print(f"  {reason}: {len(ts)}笔, 总盈亏¥{sum(pnls):+,.0f}, 笔均{statistics.mean(pcts):+.2f}%, 胜率{wins/len(pnls)*100:.1f}%")

# 2. 策略B按持仓天数分析
print("\n[2] 策略B按持仓天数分析:")
by_hold = defaultdict(list)
for t in b_trades:
    by_hold[t['hold_days']].append(t)
for d in sorted(by_hold.keys()):
    ts = by_hold[d]
    pnls = [t['pnl'] for t in ts]
    pcts = [t['pnl_pct'] for t in ts]
    wins = sum(1 for p in pnls if p > 0)
    print(f"  持{d}天: {len(ts)}笔, 笔均{statistics.mean(pcts):+.2f}%, 胜率{wins/len(pnls)*100:.1f}%, 总¥{sum(pnls):+,.0f}")

# 3. 策略B: "移动止盈"真的在止盈吗？
print("\n[3] 策略B'移动止盈'的真实盈亏:")
trailing_trades = [t for t in b_trades if '移动止盈' in t['reason']]
trailing_wins = sum(1 for t in trailing_trades if t['pnl'] > 0)
trailing_losses = [t for t in trailing_trades if t['pnl'] <= 0]
print(f"  总笔数: {len(trailing_trades)}")
print(f"  盈利笔: {trailing_wins} ({trailing_wins/len(trailing_trades)*100:.1f}%)")
print(f"  亏损笔: {len(trailing_losses)} ({len(trailing_losses)/len(trailing_trades)*100:.1f}%)")
if trailing_losses:
    print(f"  亏损笔样例(前10):")
    for t in trailing_losses[:10]:
        print(f"    {t['code']} {t['name']} {t['buy_date']}→{t['sell_date']} 持{t['hold_days']}天 ¥{t['pnl']:+,.0f} ({t['pnl_pct']:+.2f}%) {t['reason']}")

# 4. 策略B: 连板数对收益的影响
print("\n[4] 策略B按连板数分析:")
by_lianban = defaultdict(list)
for t in b_trades:
    # 从signal_type/reason中提取连板数不可靠，用持仓分析
    by_lianban[t.get('hold_days', 0)].append(t)
# 直接看是否退潮市的交易更多亏损

# 5. 策略C详细分析
print("\n[5] 策略C详细分析:")
c_trades = [t for t in trades if t['strategy'] == 'C']
c_by_reason = defaultdict(list)
for t in c_trades:
    reason = t['reason'].split(':')[0]
    c_by_reason[reason].append(t)
for reason, ts in sorted(c_by_reason.items(), key=lambda x: -len(x[1])):
    pnls = [t['pnl'] for t in ts]
    pcts = [t['pnl_pct'] for t in ts]
    wins = sum(1 for p in pnls if p > 0)
    print(f"  {reason}: {len(ts)}笔, 总¥{sum(pnls):+,.0f}, 笔均{statistics.mean(pcts):+.2f}%, 胜率{wins/len(pnls)*100:.1f}%")

# 6. 策略B: 退潮vs正常市的对比(看每天的市场情绪)
print("\n[6] 策略B按退潮/正常分天分析(近似):")
daily_vals = data.get('daily_values', [])
if daily_vals:
    ebb_days = [d for d in daily_vals if d.get('phase') == '退潮']
    normal_days = [d for d in daily_vals if d.get('phase') not in ('退潮', '冰点')]
    print(f"  退潮天数: {len(ebb_days)}/{len(daily_vals)} ({len(ebb_days)/len(daily_vals)*100:.1f}%)")
    print(f"  正常+高潮天数: {len(normal_days)}/{len(daily_vals)} ({len(normal_days)/len(daily_vals)*100:.1f}%)")

# 7. 最大单笔亏损案例分析
print("\n[7] 策略B最大亏损案例TOP10:")
b_sorted = sorted(b_trades, key=lambda t: t['pnl'])
for t in b_sorted[:10]:
    print(f"  {t['code']} {t['name']} {t['buy_date']}→{t['sell_date']} 持{t['hold_days']}天 ¥{t['pnl']:+,.0f} ({t['pnl_pct']:+.2f}%) {t['reason']}")

# 8. 最大单笔盈利案例
print("\n[8] 策略B最大盈利案例TOP10:")
b_sorted_win = sorted(b_trades, key=lambda t: -t['pnl'])
for t in b_sorted_win[:10]:
    print(f"  {t['code']} {t['name']} {t['buy_date']}→{t['sell_date']} 持{t['hold_days']}天 ¥{t['pnl']:+,.0f} ({t['pnl_pct']:+.2f}%) {t['reason']}")

print("\n" + "=" * 70)
