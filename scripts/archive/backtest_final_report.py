"""
Alpha Miner 三策略终极回测报告 v6
=================================
数据: 493.6万条去重后 / 944天 / 主板354.5万条
成本: 0.5%/笔(佣金0.05%+印花税0.05%+滑点0.4% — 保守估计)
滑点: 卖出时额外扣0.2%(小盘低开股冲击成本)

严格规则:
- 只用主板(排除300/688/8xx/9xx/200/900)
- 只用open>0的记录
- 涨停识别: close/pre_close ∈ [9.5%, 10.5%]
- 策略B: ZT>200保护(当天涨停>200只,次日不买)
- Bootstrap 1000次采样验证95%置信区间
- 按季度/年度分别统计,识别衰减趋势
"""
import sqlite3
import statistics
import random
from collections import defaultdict

DB_PATH = 'data/alpha_miner.db'
# 保守成本模型
COST_BUY = 0.002   # 买入成本0.2%(佣金+滑点)
COST_SELL = 0.003  # 卖出成本0.3%(佣金+印花税+滑点)
COST_TOTAL = COST_BUY + COST_SELL  # 0.5%/笔

def load_data():
    """加载并验证数据"""
    conn = sqlite3.connect(DB_PATH)
    
    daily = {}
    zt = defaultdict(list)  # date -> [code]
    
    for row in conn.execute('''
        SELECT stock_code, trade_date, open, close, high, low, pre_close
        FROM daily_price 
        WHERE open > 0 AND close > 0 AND pre_close > 0
          AND stock_code NOT GLOB '300*' 
          AND stock_code NOT GLOB '688*'
          AND stock_code NOT GLOB '8*'
          AND stock_code NOT GLOB '9*'
          AND stock_code NOT GLOB '200*'
          AND stock_code NOT GLOB '900*'
    '''):
        code, date, o, c, h, lo, pc = row
        daily[(code, date)] = {'open': o, 'close': c, 'high': h, 'low': lo, 'pre_close': pc}
        
        # 涨停识别
        pct = (c / pc - 1) * 100
        if 9.5 <= pct <= 10.5:
            zt[date].append(code)
    
    all_dates = sorted(set(d[1] for d in daily))
    date_idx = {d: i for i, d in enumerate(all_dates)}
    
    conn.close()
    return daily, zt, all_dates, date_idx

def calc_pnl(buy_price, sell_price):
    """计算扣成本后的盈亏率"""
    return (sell_price / buy_price - 1) - COST_TOTAL

def strategy_b(daily, zt, all_dates, date_idx, 
               low_open_min=-0.02, low_open_max=-0.08,
               stop_loss=None, zt_limit=200):
    """策略B: 涨停次日低吸"""
    trades = []
    
    for date in sorted(zt.keys()):
        di = date_idx.get(date)
        if di is None or di + 1 >= len(all_dates):
            continue
        
        # ZT保护
        if zt_limit and len(zt[date]) > zt_limit:
            continue
        
        nd = all_dates[di + 1]
        
        for code in zt[date]:
            nb = daily.get((code, nd))
            if not nb or nb['pre_close'] <= 0:
                continue
            
            # 低开判断
            oc = nb['open'] / nb['pre_close'] - 1
            if oc > low_open_min or oc < low_open_max:
                continue
            
            buy_price = nb['open']
            sell_price = nb['close']
            
            # 止损检查(日内最低价触发)
            if stop_loss:
                stop_price = buy_price * (1 + stop_loss)
                if nb['low'] <= stop_price:
                    sell_price = stop_price  # 止损卖出
            
            pnl = calc_pnl(buy_price, sell_price)
            
            trades.append({
                'zt_date': date, 'code': code,
                'buy_date': nd, 'buy_price': buy_price,
                'sell_price': sell_price, 'pnl': pnl,
                'open_chg': oc, 'year': nd[:4]
            })
    
    return trades

def strategy_c(daily, zt, all_dates, date_idx):
    """策略C: 2连板→T+1大跌→T+2低吸"""
    trades = []
    
    # 找2连板: T日和T-1日都涨停
    for date in sorted(zt.keys()):
        di = date_idx.get(date)
        if di is None or di < 1 or di + 2 >= len(all_dates):
            continue
        
        prev_date = all_dates[di - 1]
        t1_date = all_dates[di + 1]  # T+1
        t2_date = all_dates[di + 2]  # T+2
        
        for code in zt[date]:
            # T-1日也涨停=2连板
            if code not in zt.get(prev_date, []):
                continue
            
            # T+1日大跌(收盘跌>3%)
            t1_bar = daily.get((code, t1_date))
            if not t1_bar or t1_bar['pre_close'] <= 0:
                continue
            t1_chg = (t1_bar['close'] / t1_bar['pre_close'] - 1)
            if t1_chg > -0.03:  # 必须跌>3%
                continue
            
            # T+2日低开买入
            t2_bar = daily.get((code, t2_date))
            if not t2_bar or t2_bar['pre_close'] <= 0:
                continue
            t2_oc = t2_bar['open'] / t2_bar['pre_close'] - 1
            if t2_oc > -0.02:  # 必须低开>2%
                continue
            
            buy_price = t2_bar['open']
            
            # T+3日卖出(收盘价)
            if di + 3 >= len(all_dates):
                continue
            t3_date = all_dates[di + 3]
            t3_bar = daily.get((code, t3_date))
            if not t3_bar:
                continue
            
            sell_price = t3_bar['close']
            
            # 止损-6%
            stop_price = buy_price * 0.94
            if t3_bar['low'] <= stop_price:
                sell_price = stop_price
            
            pnl = calc_pnl(buy_price, sell_price)
            
            trades.append({
                'zt_date': date, 'code': code,
                'buy_date': t2_date, 'buy_price': buy_price,
                'sell_price': sell_price, 'pnl': pnl,
                'open_chg': t2_oc, 'year': t2_date[:4]
            })
    
    return trades

def strategy_a_simple(daily, zt, all_dates, date_idx):
    """策略A(简化版): 超跌反弹 — 20日跌幅>10% + RSI超卖
    注: 简化回测,不包含ML信号,仅作为参考"""
    trades = []
    
    for date in all_dates:
        di = date_idx.get(date)
        if di is None or di < 20 or di + 2 >= len(all_dates):
            continue
        
        # 前一天
        prev_date = all_dates[di - 1]
        # 后两天(买入+卖出)
        buy_date = all_dates[di]  # 当天
        sell_date = all_dates[di + 1]  # 次日
        
        # 遍历所有主板股票
        for code in set(c for (c, d) in daily.keys() if d == prev_date):
            # 20日前数据
            d20 = all_dates[di - 20]
            bar_20 = daily.get((code, d20))
            bar_prev = daily.get((code, prev_date))
            bar_buy = daily.get((code, buy_date))
            bar_sell = daily.get((code, sell_date))
            
            if not all([bar_20, bar_prev, bar_buy, bar_sell]):
                continue
            if bar_prev['pre_close'] <= 0:
                continue
            
            # 20日跌幅>10%
            drop_20 = (bar_prev['close'] / bar_20['close'] - 1)
            if drop_20 > -0.10:
                continue
            
            # 当天低开(相对昨收)
            oc = bar_buy['open'] / bar_prev['close'] - 1
            if oc > -0.02:  # 至少低开2%
                continue
            
            buy_price = bar_buy['open']
            sell_price = bar_sell['close']
            
            # 止损-5%
            stop_price = buy_price * 0.95
            if bar_sell['low'] <= stop_price:
                sell_price = stop_price
            
            pnl = calc_pnl(buy_price, sell_price)
            
            trades.append({
                'code': code, 'buy_date': buy_date,
                'buy_price': buy_price, 'sell_price': sell_price,
                'pnl': pnl, 'open_chg': oc, 'year': buy_date[:4]
            })
    
    return trades

def analyze(trades, name):
    """分析交易结果"""
    if not trades:
        print(f'\n{name}: 无交易')
        return {}
    
    pnls = [t['pnl'] for t in trades]
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p <= 0]
    
    avg = statistics.mean(pnls)
    med = statistics.median(pnls)
    wr = len(wins) / len(pnls) * 100
    pf = sum(wins) / abs(sum(losses)) if losses else 999
    total_pnl = sum(pnls)
    
    # Bootstrap 95% CI
    random.seed(42)
    boot_means = []
    for _ in range(1000):
        sample = random.choices(pnls, k=len(pnls))
        boot_means.append(statistics.mean(sample))
    ci_lo = statistics.quantiles(boot_means, n=20)[0]  # 5th percentile
    ci_hi = statistics.quantiles(boot_means, n=20)[-1]  # 95th percentile
    
    # 年度统计
    yearly = defaultdict(list)
    for t in trades:
        yearly[t['year']].append(t['pnl'])
    
    # 季度统计
    quarterly = defaultdict(list)
    for t in trades:
        q = f"{t['year']}-Q{(int(t['buy_date'][5:7])-1)//3+1}"
        quarterly[q].append(t['pnl'])
    
    print(f'\n{"="*70}')
    print(f'{name}')
    print(f'{"="*70}')
    print(f'笔数: {len(pnls):,}')
    print(f'胜率: {wr:.1f}%')
    print(f'均赚: {avg*100:+.3f}%')
    print(f'中位数: {med*100:+.3f}%')
    print(f'PF: {pf:.2f}')
    print(f'总盈亏率: {total_pnl*100:+.2f}%')
    print(f'95%CI: [{ci_lo*100:+.3f}%, {ci_hi*100:+.3f}%]')
    print(f'夏普(日): {avg / statistics.stdev(pnls) if len(pnls) > 1 else 0:.3f}')
    print(f'最大单笔亏: {min(pnls)*100:.2f}%')
    print(f'最大单笔赚: {max(pnls)*100:.2f}%')
    
    # 年度
    print(f'\n  年度:')
    years_positive = 0
    for yr in sorted(yearly.keys()):
        yp = yearly[yr]
        yavg = statistics.mean(yp) * 100
        ywr = len([p for p in yp if p > 0]) / len(yp) * 100
        tag = '✓' if yavg > 0 else '✗'
        if yavg > 0: years_positive += 1
        print(f'    {yr}: {len(yp):>5}笔 {ywr:>5.1f}% {yavg:>+7.3f}% {tag}')
    print(f'  正收益年: {years_positive}/{len(yearly)}')
    
    # 季度趋势
    qs = sorted(quarterly.keys())
    q_avgs = [statistics.mean(quarterly[q])*100 for q in qs if len(quarterly[q]) >= 10]
    if len(q_avgs) >= 6:
        early = statistics.mean(q_avgs[:len(q_avgs)//2])
        late = statistics.mean(q_avgs[len(q_avgs)//2:])
        trend = '↑' if late > early else '↓'
        print(f'  趋势: 早期{early:+.3f}% → 近期{late:+.3f}% ({trend} {late-early:+.3f}%)')
    
    return {
        'count': len(pnls), 'win_rate': wr, 'avg': avg,
        'pf': pf, 'ci_lo': ci_lo, 'ci_hi': ci_hi,
        'yearly': yearly, 'quarterly': quarterly,
        'years_positive': years_positive, 'years_total': len(yearly)
    }

# ============ 执行 ============
print('加载数据...')
daily, zt, all_dates, date_idx = load_data()
print(f'主板K线: {len(daily):,}条 / {len(all_dates)}天')
print(f'涨停天数: {len(zt)}天')

# 策略B — 多参数对比
print('\n' + '='*70)
print('策略B: 涨停次日低吸 — 参数对比')
print('='*70)

for label, kwargs in [
    ('B-S1(当前daemon): 低开-2%~-8% 止损-5% ZT<200', 
     dict(low_open_min=-0.02, low_open_max=-0.08, stop_loss=-0.05, zt_limit=200)),
    ('B-S2: 低开-2%~-8% 不止损 ZT<200', 
     dict(low_open_min=-0.02, low_open_max=-0.08, stop_loss=None, zt_limit=200)),
    ('B-S3: 低开-3%~-8% 止损-5% ZT<200', 
     dict(low_open_min=-0.03, low_open_max=-0.08, stop_loss=-0.05, zt_limit=200)),
    ('B-S4: 低开-2%~-8% 止损-7% ZT<200', 
     dict(low_open_min=-0.02, low_open_max=-0.08, stop_loss=-0.07, zt_limit=200)),
]:
    trades = strategy_b(daily, zt, all_dates, date_idx, **kwargs)
    analyze(trades, label)

# 无ZT保护(对照)
trades_no_filter = strategy_b(daily, zt, all_dates, date_idx,
    low_open_min=-0.02, low_open_max=-0.08, stop_loss=None, zt_limit=None)
analyze(trades_no_filter, 'B无ZT保护(对照): 低开-2%~-8% 不止损 无熔断')

# 策略C
trades_c = strategy_c(daily, zt, all_dates, date_idx)
analyze(trades_c, '策略C: 2连板→T+1大跌→T+2低吸→T+3卖')

# 策略A(简化)
print('\n' + '='*70)
print('策略A(简化版,非ML信号): 超跌反弹 — 仅作参考')
print('='*70)
trades_a = strategy_a_simple(daily, zt, all_dates, date_idx)
analyze(trades_a, '策略A(简化): 20日跌>10%+低开>2%+次日卖')

# ============ 成本敏感性 ============
print('\n' + '='*70)
print('成本敏感性分析(策略B-S2, 不止损)')
print('='*70)
trades_s2 = strategy_b(daily, zt, all_dates, date_idx,
    low_open_min=-0.02, low_open_max=-0.08, stop_loss=None, zt_limit=200)

for extra in [0, 0.001, 0.002, 0.003, 0.005, 0.008]:
    pnls = [(t['sell_price']/t['buy_price']-1) - COST_TOTAL - extra for t in trades_s2]
    avg = statistics.mean(pnls) * 100
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p <= 0]
    pf = sum(wins)/abs(sum(losses)) if losses else 999
    tag = '✓' if avg > 0 else '✗'
    print(f'  总成本{((COST_TOTAL+extra)*100):.1f}%: 均赚{avg:+.3f}% PF={pf:.2f} {tag}')

# ============ 实盘模拟 ============
print('\n' + '='*70)
print('实盘模拟(策略B-S2, 10万本金, 每天最多买1只, 仓位1万)')
print('='*70)
random.seed(42)
trades_s2_sorted = sorted(trades_s2, key=lambda t: t['buy_date'])
capital = 100000
pos_size = 10000
daily_pnl = 0
trades_taken = []
seen_dates = set()

for t in trades_s2_sorted:
    d = t['buy_date']
    if d in seen_dates:  # 每天最多1只
        continue
    seen_dates.add(d)
    pnl_abs = pos_size * t['pnl']
    capital += pnl_abs
    trades_taken.append(t)

print(f'交易笔数: {len(trades_taken)}')
print(f'终值: ¥{capital:,.0f}')
print(f'总收益: {(capital-100000)/100000*100:+.1f}%')
years = (len(all_dates)) / 242
print(f'年化: {((capital/100000)**(1/years)-1)*100:.1f}%')

# 最大回撤
equity = 100000
peak = equity
max_dd = 0
for t in trades_taken:
    equity += pos_size * t['pnl']
    peak = max(peak, equity)
    dd = (peak - equity) / peak
    max_dd = max(max_dd, dd)
print(f'最大回撤: {max_dd*100:.1f}%')

# 连续亏损
consec_loss = 0
max_consec = 0
for t in trades_taken:
    if t['pnl'] <= 0:
        consec_loss += 1
        max_consec = max(max_consec, consec_loss)
    else:
        consec_loss = 0
print(f'最大连亏: {max_consec}笔')

print('\n完成!')
