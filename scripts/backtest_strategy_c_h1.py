#!/usr/bin/env python3
"""
Strategy C Hypothesis 1: 逆向放量反弹 (Inverse Volume Bounce) Backtest
"""
import sqlite3
import pandas as pd
import numpy as np
from collections import defaultdict
import json
import os

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data', 'alpha_miner.db')

def get_trading_dates(conn, n=60):
    """Get last n trading dates, we need at least 20 history before signal dates."""
    c = conn.cursor()
    c.execute('''
        SELECT DISTINCT trade_date FROM daily_price 
        ORDER BY trade_date DESC LIMIT ?
    ''', (n + 20,))
    dates = [r[0] for r in c.fetchall()]
    return dates

def get_stock_names(conn):
    """Build a stock_code -> name mapping from zt_pool and other sources."""
    c = conn.cursor()
    name_map = {}
    # Get names from zt_pool across all available dates
    c.execute('SELECT DISTINCT stock_code, name FROM zt_pool WHERE name IS NOT NULL AND name != ""')
    for code, name in c.fetchall():
        name_map[code] = name
    return name_map

def identify_st_delisted(name_map, all_codes):
    """Return set of ST/delisted codes."""
    st_codes = set()
    for code in all_codes:
        name = name_map.get(code, '')
        if 'ST' in name or '退' in name:
            st_codes.add(code)
    return st_codes

def run_backtest():
    conn = sqlite3.connect(DB_PATH)
    
    print("Loading trading dates...")
    all_dates = get_trading_dates(conn, 80)
    print(f"  Total dates available: {len(all_dates)}")
    
    # We need signal dates that have at least 20 days before + 3 days after
    # dates[0] is most recent
    # For signal dates: we need index from 3 to len-21 (so we have 20 days before and 3 days after)
    if len(all_dates) < 26:
        print("ERROR: Not enough trading dates for backtest (need at least 26)")
        return
    
    signal_start_idx = 3  # need 3 days after
    signal_end_idx = len(all_dates) - 21  # need 20 days before
    signal_dates = all_dates[signal_start_idx:signal_end_idx]
    print(f"  Signal dates: {signal_dates[-1]} to {signal_dates[0]} ({len(signal_dates)} dates)")
    
    print("Loading stock names...")
    name_map = get_stock_names(conn)
    
    print("Loading all daily price data for relevant period...")
    # Load data from 20 days before earliest signal to 3 days after latest signal
    earliest = all_dates[-1]  # earliest date
    latest = all_dates[0]     # latest date
    
    query = '''
        SELECT stock_code, trade_date, open, high, low, close, pre_close, volume, amount
        FROM daily_price 
        WHERE trade_date >= ? AND trade_date <= ?
        ORDER BY stock_code, trade_date
    '''
    df = pd.read_sql_query(query, conn, params=(earliest, latest))
    print(f"  Loaded {len(df)} rows")
    
    conn.close()
    
    # Get distinct codes
    all_codes = df['stock_code'].unique()
    print(f"  Unique stocks: {len(all_codes)}")
    
    # Identify ST/delisted
    st_codes = identify_st_delisted(name_map, all_codes)
    print(f"  ST/退市 stocks: {len(st_codes)}")
    
    # Create a date index map
    date_list = sorted(df['trade_date'].unique())
    date_idx = {d: i for i, d in enumerate(date_list)}
    
    # Pivot data for easier computation
    print("Building pivot tables...")
    
    # Build per-stock time series
    signals = []
    
    # For efficiency, group by stock
    grouped = df.groupby('stock_code')
    
    total_stocks = len(grouped)
    processed = 0
    
    for code, group in grouped:
        processed += 1
        if processed % 500 == 0:
            print(f"  Processing stock {processed}/{total_stocks}...")
        
        # Skip ST/退市
        if code in st_codes:
            continue
        
        # Sort by date
        group = group.sort_values('trade_date').reset_index(drop=True)
        
        # Need at least 23 rows (20 history + signal day + 3 future)
        if len(group) < 23:
            continue
        
        # Build arrays
        dates_arr = group['trade_date'].values
        close_arr = group['close'].values
        high_arr = group['high'].values
        low_arr = group['low'].values
        volume_arr = group['volume'].values
        pre_close_arr = group['pre_close'].values
        
        # Create date to row-index mapping for this stock
        date_to_idx = {d: i for i, d in enumerate(dates_arr)}
        
        # Iterate over potential signal dates
        for sig_date in signal_dates:
            if sig_date not in date_to_idx:
                continue
            idx = date_to_idx[sig_date]
            
            # Need 20 days before and 3 days after
            if idx < 20 or idx + 3 >= len(dates_arr):
                continue
            
            # Verify dates are consecutive trading days (not calendar gaps)
            # Check that signal date's index in global date list matches
            if sig_date not in date_idx:
                continue
            global_sig_idx = date_idx[sig_date]
            
            # Check day_ret
            close_today = close_arr[idx]
            pre_close_today = pre_close_arr[idx]
            if pre_close_today <= 0:
                continue
            day_ret = (close_today - pre_close_today) / pre_close_today * 100
            if day_ret > -2.0:
                continue
            
            # Check price <= 90
            if close_today > 90 or close_today <= 0:
                continue
            
            # Check volume > 0
            vol_today = volume_arr[idx]
            if vol_today <= 0:
                continue
            
            # Compute amount_ratio20 (volume ratio vs 20-day avg)
            vol_20d = volume_arr[idx-20:idx]
            if len(vol_20d) < 20 or np.any(vol_20d <= 0):
                # Need all 20 days to have valid data
                continue
            avg_vol_20 = np.mean(vol_20d)
            if avg_vol_20 <= 0:
                continue
            amount_ratio20 = vol_today / avg_vol_20
            if amount_ratio20 < 1.5:
                continue
            
            # Compute MA20 distance
            close_20d = close_arr[idx-20:idx]
            if len(close_20d) < 20:
                continue
            ma20 = np.mean(close_20d)
            if ma20 <= 0:
                continue
            ma20_dist = (close_today - ma20) / ma20 * 100
            
            # Compute ret20
            close_20d_ago = close_arr[idx-20]
            if close_20d_ago <= 0:
                continue
            ret20 = (close_today - close_20d_ago) / close_20d_ago * 100
            
            # Check that future dates are actually T+1, T+2, T+3
            # Use global date index to verify
            sig_global = date_idx[sig_date]
            
            # Get T+1, T+2, T+3 dates from global date list
            if sig_global - 1 < 0 or sig_global - 3 < 0:
                continue
            t1_date = date_list[sig_global - 1]
            t2_date = date_list[sig_global - 2]
            t3_date = date_list[sig_global - 3]
            
            # Get future prices
            t1_idx = date_to_idx.get(t1_date)
            t2_idx = date_to_idx.get(t2_date)
            t3_idx = date_to_idx.get(t3_date)
            
            if t1_idx is None or t2_idx is None or t3_idx is None:
                continue
            
            buy_price = close_today  # buy at close on signal day
            
            # T+1 max return (intraday high vs buy price)
            t1_high = high_arr[t1_idx]
            t1_close = close_arr[t1_idx]
            t1_max_ret = (t1_high - buy_price) / buy_price * 100 if buy_price > 0 else 0
            t1_close_ret = (t1_close - buy_price) / buy_price * 100 if buy_price > 0 else 0
            
            # T+2 max return (max high from T+1 to T+2)
            t2_high = high_arr[t2_idx]
            max_high_t2 = max(t1_high, t2_high)
            t2_max_ret = (max_high_t2 - buy_price) / buy_price * 100 if buy_price > 0 else 0
            
            # T+3 max return (max high from T+1 to T+3)
            t3_high = high_arr[t3_idx]
            max_high_t3 = max(t1_high, t2_high, t3_high)
            t3_max_ret = (max_high_t3 - buy_price) / buy_price * 100 if buy_price > 0 else 0
            
            # T+3 close return
            t3_close = close_arr[t3_idx]
            t3_close_ret = (t3_close - buy_price) / buy_price * 100 if buy_price > 0 else 0
            
            # MA20 distance bucket
            if ma20_dist < -10:
                ma20_bucket = '<-10%'
            elif ma20_dist < 0:
                ma20_bucket = '-10%~0%'
            elif ma20_dist < 10:
                ma20_bucket = '0%~10%'
            else:
                ma20_bucket = '>10%'
            
            # ret20 bucket
            if ret20 < -15:
                ret20_bucket = '<-15%'
            elif ret20 < 0:
                ret20_bucket = '-15%~0%'
            elif ret20 < 20:
                ret20_bucket = '0%~20%'
            else:
                ret20_bucket = '>20%'
            
            signals.append({
                'code': code,
                'name': name_map.get(code, ''),
                'signal_date': sig_date,
                'buy_price': buy_price,
                'day_ret': day_ret,
                'amount_ratio20': amount_ratio20,
                'ma20_dist': ma20_dist,
                'ma20_bucket': ma20_bucket,
                'ret20': ret20,
                'ret20_bucket': ret20_bucket,
                't1_max_ret': t1_max_ret,
                't2_max_ret': t2_max_ret,
                't3_max_ret': t3_max_ret,
                't1_close_ret': t1_close_ret,
                't3_close_ret': t3_close_ret,
            })
    
    print(f"\nTotal signals found: {len(signals)}")
    return signals

def compute_metrics(signals):
    """Compute all required metrics."""
    if not signals:
        return None
    
    df = pd.DataFrame(signals)
    n = len(df)
    
    metrics = {}
    metrics['sample_count'] = n
    
    # Overall metrics for T+1, T+2, T+3
    for horizon, col in [('T+1 max', 't1_max_ret'), ('T+2 max', 't2_max_ret'), ('T+3 max', 't3_max_ret'), ('T+3 close', 't3_close_ret')]:
        rets = df[col]
        metrics[f'{horizon}_p_max5'] = (rets >= 5).mean() * 100
        metrics[f'{horizon}_p_max8'] = (rets >= 8).mean() * 100
        metrics[f'{horizon}_p_positive'] = (rets > 0).mean() * 100
        metrics[f'{horizon}_avg'] = rets.mean()
        metrics[f'{horizon}_median'] = rets.median()
        metrics[f'{horizon}_p5'] = np.percentile(rets, 5)
        metrics[f'{horizon}_p25'] = np.percentile(rets, 25)
        metrics[f'{horizon}_p75'] = np.percentile(rets, 75)
        metrics[f'{horizon}_p95'] = np.percentile(rets, 95)
    
    # Bucket analysis: MA20 distance
    ma20_buckets = ['<-10%', '-10%~0%', '0%~10%', '>10%']
    metrics['ma20_buckets'] = {}
    for bucket in ma20_buckets:
        subset = df[df['ma20_bucket'] == bucket]
        if len(subset) > 0:
            metrics['ma20_buckets'][bucket] = {
                'count': len(subset),
                't3_max_avg': subset['t3_max_ret'].mean(),
                't3_max_median': subset['t3_max_ret'].median(),
                't3_close_avg': subset['t3_close_ret'].mean(),
                't3_close_median': subset['t3_close_ret'].median(),
                'p_max5': (subset['t3_max_ret'] >= 5).mean() * 100,
                'p_close_positive': (subset['t3_close_ret'] > 0).mean() * 100,
            }
    
    # Bucket analysis: ret20
    ret20_buckets = ['<-15%', '-15%~0%', '0%~20%', '>20%']
    metrics['ret20_buckets'] = {}
    for bucket in ret20_buckets:
        subset = df[df['ret20_bucket'] == bucket]
        if len(subset) > 0:
            metrics['ret20_buckets'][bucket] = {
                'count': len(subset),
                't3_max_avg': subset['t3_max_ret'].mean(),
                't3_max_median': subset['t3_max_ret'].median(),
                't3_close_avg': subset['t3_close_ret'].mean(),
                't3_close_median': subset['t3_close_ret'].median(),
                'p_max5': (subset['t3_max_ret'] >= 5).mean() * 100,
                'p_close_positive': (subset['t3_close_ret'] > 0).mean() * 100,
            }
    
    # Top 5 winners and losers (by T+3 max return and T+3 close return)
    metrics['top5_t3_max'] = df.nlargest(5, 't3_max_ret')[['code', 'name', 'signal_date', 'buy_price', 'day_ret', 'amount_ratio20', 't3_max_ret', 't3_close_ret']].to_dict('records')
    metrics['bottom5_t3_close'] = df.nsmallest(5, 't3_close_ret')[['code', 'name', 'signal_date', 'buy_price', 'day_ret', 'amount_ratio20', 't3_max_ret', 't3_close_ret']].to_dict('records')
    
    # Daily distribution
    daily_counts = df.groupby('signal_date').size()
    metrics['daily_signal_stats'] = {
        'mean': daily_counts.mean(),
        'median': daily_counts.median(),
        'min': daily_counts.min(),
        'max': daily_counts.max(),
        'total_dates': len(daily_counts),
    }
    
    # amount_ratio20 distribution
    metrics['amount_ratio20_stats'] = {
        'mean': df['amount_ratio20'].mean(),
        'median': df['amount_ratio20'].median(),
        'p25': df['amount_ratio20'].quantile(0.25),
        'p75': df['amount_ratio20'].quantile(0.75),
    }
    
    return metrics

def generate_report(signals, metrics):
    """Generate markdown report in Chinese."""
    if metrics is None:
        return "无法生成报告：没有找到符合条件的信号。"
    
    df = pd.DataFrame(signals)
    
    lines = []
    lines.append("# Strategy C Hypothesis 1: 逆向放量反弹 (Inverse Volume Bounce) 回测报告")
    lines.append("")
    lines.append(f"**生成时间**: 2026-06-08")
    lines.append(f"**数据来源**: Alpha Miner daily_price 表")
    lines.append("")
    
    lines.append("## 1. 策略定义")
    lines.append("")
    lines.append("### 入场条件")
    lines.append("- 排除 ST/退市股票")
    lines.append("- 当日跌幅 (day_ret) <= -2%")
    lines.append("- 量比 (amount_ratio20) >= 1.5 (相对20日平均成交量)")
    lines.append("- 收盘价 <= 90 元")
    lines.append("")
    lines.append("### 观察指标")
    lines.append("- T+1 最大收益率 (日内最高价相对买入价)")
    lines.append("- T+2 最大收益率 (T+1至T+2日内最高价相对买入价)")
    lines.append("- T+3 最大收益率 (T+1至T+3日内最高价相对买入价)")
    lines.append("- T+3 收盘收益率 (T+3收盘价相对买入价)")
    lines.append("")
    lines.append("### 买入假设")
    lines.append("- 信号日收盘价买入")
    lines.append("")
    
    lines.append("## 2. 回测概况")
    lines.append("")
    lines.append(f"| 指标 | 值 |")
    lines.append(f"|------|------|")
    lines.append(f"| 信号总数 | {metrics['sample_count']} |")
    ds = metrics['daily_signal_stats']
    lines.append(f"| 覆盖交易日数 | {ds['total_dates']} |")
    lines.append(f"| 日均信号数 | {ds['mean']:.1f} |")
    lines.append(f"| 日信号中位数 | {ds['median']:.0f} |")
    lines.append(f"| 日信号范围 | {ds['min']} ~ {ds['max']} |")
    ar = metrics['amount_ratio20_stats']
    lines.append(f"| 量比均值 | {ar['mean']:.2f} |")
    lines.append(f"| 量比中位数 | {ar['median']:.2f} |")
    lines.append("")
    
    lines.append("## 3. 整体收益分布")
    lines.append("")
    lines.append("| 指标 | T+1 max | T+2 max | T+3 max | T+3 close |")
    lines.append("|------|---------|---------|---------|-----------|")
    
    for stat_name, fmt in [
        ('avg', '{:.2f}%'), ('median', '{:.2f}%'), ('p5', '{:.2f}%'), 
        ('p25', '{:.2f}%'), ('p75', '{:.2f}%'), ('p95', '{:.2f}%'),
        ('p_max5', '{:.1f}%'), ('p_max8', '{:.1f}%'), ('p_positive', '{:.1f}%')
    ]:
        label_map = {
            'avg': '平均收益率', 'median': '中位数收益率', 
            'p5': '5th 百分位', 'p25': '25th 百分位',
            'p75': '75th 百分位', 'p95': '95th 百分位',
            'p_max5': 'P(max>=5%)', 'p_max8': 'P(max>=8%)', 
            'p_positive': 'P(正收益)'
        }
        vals = []
        for horizon in ['T+1 max', 'T+2 max', 'T+3 max', 'T+3 close']:
            v = metrics[f'{horizon}_{stat_name}']
            vals.append(fmt.format(v))
        lines.append(f"| {label_map[stat_name]} | {' | '.join(vals)} |")
    lines.append("")
    
    lines.append("## 4. MA20 距离分桶分析")
    lines.append("")
    lines.append("MA20距离 = (收盘价 - MA20) / MA20 × 100%")
    lines.append("")
    lines.append("| 分桶 | 样本数 | T+3 max 均值 | T+3 max 中位数 | T+3 close 均值 | T+3 close 中位数 | P(max>=5%) | P(close>0) |")
    lines.append("|------|--------|-------------|---------------|---------------|-----------------|------------|------------|")
    for bucket in ['<-10%', '-10%~0%', '0%~10%', '>10%']:
        b = metrics['ma20_buckets'].get(bucket)
        if b:
            lines.append(f"| {bucket} | {b['count']} | {b['t3_max_avg']:.2f}% | {b['t3_max_median']:.2f}% | {b['t3_close_avg']:.2f}% | {b['t3_close_median']:.2f}% | {b['p_max5']:.1f}% | {b['p_close_positive']:.1f}% |")
        else:
            lines.append(f"| {bucket} | 0 | - | - | - | - | - | - |")
    lines.append("")
    
    lines.append("## 5. 20日涨幅分桶分析")
    lines.append("")
    lines.append("ret20 = (收盘价 - 20日前收盘价) / 20日前收盘价 × 100%")
    lines.append("")
    lines.append("| 分桶 | 样本数 | T+3 max 均值 | T+3 max 中位数 | T+3 close 均值 | T+3 close 中位数 | P(max>=5%) | P(close>0) |")
    lines.append("|------|--------|-------------|---------------|---------------|-----------------|------------|------------|")
    for bucket in ['<-15%', '-15%~0%', '0%~20%', '>20%']:
        b = metrics['ret20_buckets'].get(bucket)
        if b:
            lines.append(f"| {bucket} | {b['count']} | {b['t3_max_avg']:.2f}% | {b['t3_max_median']:.2f}% | {b['t3_close_avg']:.2f}% | {b['t3_close_median']:.2f}% | {b['p_max5']:.1f}% | {b['p_close_positive']:.1f}% |")
        else:
            lines.append(f"| {bucket} | 0 | - | - | - | - | - | - |")
    lines.append("")
    
    lines.append("## 6. Top 5 最大赢家 (T+3 max)")
    lines.append("")
    lines.append("| 代码 | 名称 | 信号日期 | 买入价 | 当日跌幅 | 量比 | T+3 max | T+3 close |")
    lines.append("|------|------|----------|--------|----------|------|---------|-----------|")
    for s in metrics['top5_t3_max']:
        lines.append(f"| {s['code']} | {s['name']} | {s['signal_date']} | {s['buy_price']:.2f} | {s['day_ret']:.2f}% | {s['amount_ratio20']:.2f} | {s['t3_max_ret']:.2f}% | {s['t3_close_ret']:.2f}% |")
    lines.append("")
    
    lines.append("## 7. Top 5 最大输家 (T+3 close)")
    lines.append("")
    lines.append("| 代码 | 名称 | 信号日期 | 买入价 | 当日跌幅 | 量比 | T+3 max | T+3 close |")
    lines.append("|------|------|----------|--------|----------|------|---------|-----------|")
    for s in metrics['bottom5_t3_close']:
        lines.append(f"| {s['code']} | {s['name']} | {s['signal_date']} | {s['buy_price']:.2f} | {s['day_ret']:.2f}% | {s['amount_ratio20']:.2f} | {s['t3_max_ret']:.2f}% | {s['t3_close_ret']:.2f}% |")
    lines.append("")
    
    # Judging criteria
    lines.append("## 8. 上线评估")
    lines.append("")
    
    n = metrics['sample_count']
    issues = []
    passes = []
    
    # Sample size check
    if n < 300:
        issues.append(f"- ❌ **样本量不足**: {n} < 300，不能上线")
    else:
        passes.append(f"- ✅ **样本量充足**: {n} >= 300")
    
    # T+3 close median check
    t3_close_median = metrics['T+3 close_median']
    if t3_close_median <= 0:
        issues.append(f"- ❌ **T+3 收盘中位数 <= 0**: {t3_close_median:.2f}%，不能上线")
    else:
        passes.append(f"- ✅ **T+3 收盘中位数 > 0**: {t3_close_median:.2f}%")
    
    # 5th percentile check
    t3_close_p5 = metrics['T+3 close_p5']
    if t3_close_p5 < -10:
        issues.append(f"- ⚠️ **5th 百分位过低**: {t3_close_p5:.2f}%，需要止损设计")
    else:
        passes.append(f"- ✅ **5th 百分位可控**: {t3_close_p5:.2f}%")
    
    # Intraday spike check
    t3_max_avg = metrics['T+3 max_avg']
    t3_close_avg = metrics['T+3 close_avg']
    if t3_max_avg > 0 and t3_close_avg <= 0:
        issues.append(f"- ⚠️ **仅日内脉冲**: T+3 max 均值 {t3_max_avg:.2f}% 但 close 均值 {t3_close_avg:.2f}%，仅为日内冲高，不适合 daemon 持仓")
    elif t3_max_avg > 0 and t3_close_avg > 0:
        passes.append(f"- ✅ **收盘有正期望**: T+3 close 均值 {t3_close_avg:.2f}%")
    
    for p in passes:
        lines.append(p)
    for i in issues:
        lines.append(i)
    
    lines.append("")
    
    # Overall verdict
    critical_failures = sum(1 for i in issues if '❌' in i)
    if critical_failures > 0:
        lines.append(f"**总评**: ❌ 不能上线 — 存在 {critical_failures} 个关键问题")
    else:
        warnings = sum(1 for i in issues if '⚠️' in i)
        if warnings > 0:
            lines.append(f"**总评**: ⚠️ 有条件上线 — 存在 {warnings} 个需注意的问题")
        else:
            lines.append("**总评**: ✅ 可以上线")
    
    lines.append("")
    
    # Detailed analysis
    lines.append("## 9. 详细分析")
    lines.append("")
    
    # Best bucket analysis
    best_ma20 = None
    best_ma20_close_median = -999
    for bucket, b in metrics['ma20_buckets'].items():
        if b['t3_close_median'] > best_ma20_close_median:
            best_ma20_close_median = b['t3_close_median']
            best_ma20 = bucket
    
    best_ret20 = None
    best_ret20_close_median = -999
    for bucket, b in metrics['ret20_buckets'].items():
        if b['t3_close_median'] > best_ret20_close_median:
            best_ret20_close_median = b['t3_close_median']
            best_ret20 = bucket
    
    lines.append(f"### 最优分桶")
    lines.append(f"- MA20距离最优桶: **{best_ma20}** (T+3 close 中位数: {best_ma20_close_median:.2f}%)")
    lines.append(f"- 20日涨幅最优桶: **{best_ret20}** (T+3 close 中位数: {best_ret20_close_median:.2f}%)")
    lines.append("")
    
    # Strategy edge analysis
    t1_max_p5_rate = metrics['T+1 max_p_max5']
    t3_max_p5_rate = metrics['T+3 max_p_max5']
    t3_close_positive = metrics['T+3 close_p_positive']
    
    lines.append("### 策略边际分析")
    lines.append(f"- T+1 出现5%以上冲高概率: {t1_max_p5_rate:.1f}%")
    lines.append(f"- T+3 出现5%以上冲高概率: {t3_max_p5_rate:.1f}%")
    lines.append(f"- T+3 收盘正收益概率: {t3_close_positive:.1f}%")
    lines.append(f"- 从T+1到T+3的5%冲高增量: {t3_max_p5_rate - t1_max_p5_rate:.1f}个百分点")
    lines.append("")
    
    # Risk assessment
    lines.append("### 风险评估")
    lines.append(f"- T+3 close 最差5%损失: {metrics['T+3 close_p5']:.2f}%")
    lines.append(f"- T+3 close 最差25%损失: {metrics['T+3 close_p25']:.2f}%")
    lines.append(f"- T+1 max 最差5%: {metrics['T+1 max_p5']:.2f}% (注意：这是最大收益的5th百分位，不是风险指标)")
    lines.append("")
    
    if metrics['T+3 close_p5'] < -5:
        lines.append("⚠️ 尾部风险较大，建议设计止损策略（如 T+1 收盘亏损超过3%止损）")
    elif metrics['T+3 close_p5'] < -3:
        lines.append("尾部风险中等，可考虑适度止损")
    else:
        lines.append("尾部风险可控")
    lines.append("")
    
    lines.append("## 10. 结论与建议")
    lines.append("")
    
    if critical_failures > 0:
        lines.append("本策略在当前参数下**不建议直接上线**。主要问题：")
        for i in issues:
            if '❌' in i:
                lines.append(i.replace('- ❌ ', '  - '))
        lines.append("")
        lines.append("建议优化方向：")
        lines.append("1. 增加数据量（延长回测期至半年以上）")
        lines.append("2. 调整入场条件（如提高量比阈值、结合大盘情绪过滤）")
        lines.append("3. 设计止损止盈规则")
        lines.append("4. 考虑分桶中表现较好的子策略单独测试")
    else:
        if t3_close_median > 2:
            lines.append("策略表现出较好的正期望，建议：")
            lines.append("1. 可作为 daemon 候选策略，设置合理的仓位控制")
            lines.append("2. 优先关注表现较好的分桶组合")
            lines.append("3. 设置 T+1/T+2 止盈止损规则")
        else:
            lines.append("策略边际较弱，建议：")
            lines.append("1. 进一步优化入场条件")
            lines.append("2. 结合市场情绪/板块轮动过滤")
            lines.append("3. 考虑与其他因子组合使用")
    
    lines.append("")
    lines.append("---")
    lines.append("*报告由 Alpha Miner 回测引擎自动生成*")
    
    return '\n'.join(lines)


if __name__ == '__main__':
    signals = run_backtest()
    metrics = compute_metrics(signals)
    report = generate_report(signals, metrics)
    
    output_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 
                               'reports', 'agent_reviews', '2026-06-08_round2')
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, 'strategy_c_hypothesis_backtest.md')
    
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(report)
    
    print(f"\nReport saved to: {output_path}")
    print(f"\n{'='*60}")
    print("METRICS SUMMARY:")
    if metrics:
        print(f"  Sample count: {metrics['sample_count']}")
        print(f"  T+3 close median: {metrics['T+3 close_median']:.2f}%")
        print(f"  T+3 close avg: {metrics['T+3 close_avg']:.2f}%")
        print(f"  T+3 close 5th pct: {metrics['T+3 close_p5']:.2f}%")
        print(f"  P(T+3 max>=5%): {metrics['T+3 max_p_max5']:.1f}%")
        print(f"  P(T+3 max>=8%): {metrics['T+3 max_p_max8']:.1f}%")
        print(f"  P(T+3 close>0): {metrics['T+3 close_p_positive']:.1f}%")
