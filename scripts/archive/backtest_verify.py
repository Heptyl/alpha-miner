"""验证优化后的回测效果 — 新参数 vs 旧参数"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.backtest_engine import (
    get_conn, load_daily_prices, load_zt_pool, get_trading_dates,
    get_market_phase, is_filtered, Portfolio,
    INITIAL_CAPITAL, MAX_POSITIONS, MAX_AB_POSITIONS, MAX_C_POSITIONS,
    AB_POSITION_RATIO, C_POSITION_RATIO, MIN_CASH_RATIO,
    COMMISSION_RATE, STAMP_DUTY_RATE, SLIPPAGE, MIN_COMMISSION,
)
import statistics
from collections import defaultdict

# 新参数
NEW_STOP_LOSS = -0.05
NEW_SELL_PARAMS = {
    "A": {"trailing_stop_pct": 0.03, "trailing_ebb_pct": 0.02, "trailing_frost_pct": 0.015,
           "time_stop_days": 5, "time_stop_threshold": 0.01, "max_hold_days": 7},
    "B": {"trailing_stop_pct": 0.03, "trailing_ebb_pct": 0.02, "trailing_frost_pct": 0.015,
           "time_stop_days": 3, "time_stop_threshold": 0.01, "max_hold_days": 5},
    "C": {"trailing_stop_pct": 0.05, "trailing_ebb_pct": 0.03, "trailing_frost_pct": 0.02,
           "time_stop_days": 0, "time_stop_threshold": 0.01, "max_hold_days": 2},
}
NEW_OPEN_CHG = {'退潮': 2.0, '冰点': 3.0, '正常': 2.0, '高潮': 5.0}

def check_sell(pos, day, market_phase):
    strategy = pos.strategy
    params = NEW_SELL_PARAMS[strategy]
    stop_loss = -0.06 if strategy == "C" else NEW_STOP_LOSS
    
    if day['high'] > pos.highest_price:
        pos.highest_price = day['high']
    
    if pos.hold_days < 1:
        return None
    
    if strategy == "C":
        chg = (day['low'] / pos.buy_price - 1)
        if chg <= -0.06:
            return {'reason': '策略C止损', 'sell_price': pos.buy_price * 0.94}
        if pos.hold_days >= 2:
            return {'reason': '策略C到期', 'sell_price': day['close']}
        chg_today = (day['close'] / day['pre_close'] - 1) * 100 if day['pre_close'] > 0 else 0
        if chg_today < 9.5:
            return {'reason': '策略C尾盘清仓', 'sell_price': day['close']}
        return None
    
    chg_low = (day['low'] / pos.buy_price - 1)
    if chg_low <= stop_loss:
        return {'reason': '止损', 'sell_price': pos.buy_price * (1 + stop_loss)}
    
    if pos.hold_days >= params['max_hold_days']:
        return {'reason': '最长持有', 'sell_price': day['close']}
    
    tp = params['trailing_stop_pct']
    if market_phase == '退潮':
        tp = params.get('trailing_ebb_pct', 0.02)
    elif market_phase in ('冰点', '偏冷'):
        tp = params.get('trailing_frost_pct', 0.015)
    
    if pos.highest_price > pos.buy_price:
        dd = (day['low'] / pos.highest_price - 1)
        if dd <= -tp:
            return {'reason': 'trailing', 'sell_price': pos.highest_price * (1 - tp)}
    
    td = params['time_stop_days']
    if td > 0 and pos.hold_days >= td:
        chg = (day['close'] / pos.buy_price - 1)
        if chg < params['time_stop_threshold']:
            return {'reason': '时间止损', 'sell_price': day['close']}
    
    return None


def run_optimized(daily, zt_pool, trading_dates, start=None, end=None):
    portfolio = Portfolio(INITIAL_CAPITAL)
    dates = trading_dates[:]
    if start: dates = [d for d in dates if d >= start]
    if end: dates = [d for d in dates if d <= end]
    
    for trade_date in dates:
        phase_info = get_market_phase(daily, zt_pool, trade_date)
        phase = phase_info['phase']
        
        for pos in portfolio.positions[:]:
            if pos.code not in daily or trade_date not in daily[pos.code]:
                continue
            day = daily[pos.code][trade_date]
            pos.hold_days += 1
            if day['high'] > pos.highest_price:
                pos.highest_price = day['high']
            
            sell = check_sell(pos, day, phase)
            if sell:
                portfolio.sell(pos, sell['sell_price'], trade_date, sell['reason'])
        
        # 买入
        try:
            idx = trading_dates.index(trade_date)
        except ValueError:
            continue
        if idx < 1:
            continue
        
        # B候选
        prev_date = trading_dates[idx - 1]
        zt_stocks = zt_pool.get(prev_date, [])
        
        if phase_info['can_buy']:
            for zt in zt_stocks:
                code = zt['code']
                if is_filtered(code):
                    continue
                if code in portfolio.held_codes:
                    continue
                if code not in daily or trade_date not in daily[code]:
                    continue
                day = daily[code][trade_date]
                if not day['pre_close'] or day['pre_close'] <= 0:
                    continue
                buy_price = day['open'] if day['open'] > 0 else day['close']
                if buy_price <= 0:
                    continue
                
                open_chg = (buy_price / day['pre_close'] - 1) * 100
                threshold = NEW_OPEN_CHG.get(phase, 2.0)
                if open_chg > threshold:
                    continue
                
                day_chg = (day['close'] / day['pre_close'] - 1) * 100
                if day_chg >= 9.5 or day_chg <= -9.5:
                    continue
                
                if portfolio.ab_count >= MAX_AB_POSITIONS:
                    break
                portfolio.buy(code, zt['name'], 'B', buy_price, trade_date, '涨停确认')
        
        # C候选(独立于情绪)
        if idx >= 2:
            t0 = trading_dates[idx - 2]
            t1 = trading_dates[idx - 1]
            for zt in zt_pool.get(t0, []):
                code = zt['code']
                if is_filtered(code):
                    continue
                if (zt.get('consecutive_zt', 1) or 1) < 2:
                    continue
                if code not in daily or t1 not in daily[code]:
                    continue
                t1d = daily[code][t1]
                if not t1d['pre_close'] or t1d['pre_close'] <= 0:
                    continue
                t1_chg = (t1d['close'] / t1d['pre_close'] - 1) * 100
                if t1_chg > -3.0:
                    continue
                if code not in daily or trade_date not in daily[code]:
                    continue
                t2d = daily[code][trade_date]
                if not t2d['pre_close'] or t2d['pre_close'] <= 0:
                    continue
                t2_open = (t2d['open'] / t2d['pre_close'] - 1) * 100
                if t2_open > -2.0:
                    continue
                if portfolio.c_count >= 2:
                    break
                bp = t2d['open'] if t2d['open'] > 0 else t2d['close']
                if bp > 0:
                    portfolio.buy(code, zt['name'], 'C', bp, trade_date, '反弹低吸')
    
    # 清算
    for pos in portfolio.positions[:]:
        if pos.code in daily and dates[-1] in daily[pos.code]:
            portfolio.sell(pos, daily[pos.code][dates[-1]]['close'], dates[-1], '清算')
    
    return portfolio


def metrics(portfolio):
    trades = portfolio.closed_trades
    if not trades:
        return {}
    by_s = defaultdict(list)
    for t in trades:
        by_s[t['strategy']].append(t)
    
    result = {'total_trades': len(trades),
              'total_pnl': sum(t['pnl'] for t in trades),
              'final_assets': portfolio.cash}
    
    for s, ts in by_s.items():
        pnls = [t['pnl'] for t in ts]
        pcts = [t['pnl_pct'] for t in ts]
        wins = sum(1 for p in pnls if p > 0)
        result[f'strat_{s}'] = {
            'trades': len(ts),
            'total_pnl': sum(pnls),
            'avg_pct': statistics.mean(pcts),
            'win_rate': wins/len(pnls)*100,
            'pf': abs(sum(p for p in pnls if p>0) / sum(p for p in pnls if p<=0)) if sum(p for p in pnls if p<=0) else 999,
            'sharpe': statistics.mean(pcts)/statistics.stdev(pcts)*(252**0.5) if len(pcts)>1 else 0,
        }
    return result


if __name__ == '__main__':
    print("加载历史数据...")
    conn = get_conn()
    daily = load_daily_prices(conn)
    zt_pool = load_zt_pool(conn)
    conn.close()
    trading_dates = get_trading_dates(daily, zt_pool)
    
    print(f"\n{'='*70}")
    print("  优化参数回测验证")
    print(f"{'='*70}")
    print(f"新参数: 止损-5%, trailing 3%/2%/1.5%, 追高<2%")
    print(f"旧参数: 止损-8%, trailing 5%/3%/2%, 追高<5%")
    print()
    
    # 全量
    pf = run_optimized(daily, zt_pool, trading_dates)
    m = metrics(pf)
    print(f"全量回测(218天):")
    print(f"  总交易: {m['total_trades']}笔")
    print(f"  总盈亏: ¥{m['total_pnl']:+,.0f}")
    print(f"  最终资产: ¥{m['final_assets']:,.0f} (初始¥{INITIAL_CAPITAL:,.0f})")
    
    for s in ['B', 'C']:
        key = f'strat_{s}'
        if key in m:
            sm = m[key]
            name = {'B': '涨停确认', 'C': '反弹低吸'}[s]
            print(f"\n  策略{s}({name}):")
            print(f"    {sm['trades']}笔, 笔均{sm['avg_pct']:+.2f}%, 胜率{sm['win_rate']:.1f}%")
            print(f"    PF={sm['pf']:.2f}, 夏普={sm['sharpe']:.2f}, 总¥{sm['total_pnl']:+,.0f}")
    
    # 交叉验证
    print(f"\n{'='*70}")
    print("  交叉验证(新参数)")
    print(f"{'='*70}")
    n = len(trading_dates)
    for label, start, end in [
        ("前1/3", trading_dates[0], trading_dates[n//3-1]),
        ("中1/3", trading_dates[n//3], trading_dates[n//3*2-1]),
        ("后1/3", trading_dates[n//3*2], trading_dates[-1]),
    ]:
        pf_seg = run_optimized(daily, zt_pool, trading_dates, start=start, end=end)
        m_seg = metrics(pf_seg)
        b = m_seg.get('strat_B', {})
        c = m_seg.get('strat_C', {})
        print(f"\n  {label} ({start}~{end}):")
        if b:
            print(f"    B: {b['trades']}笔, 笔均{b['avg_pct']:+.2f}%, 胜率{b['win_rate']:.1f}%, 夏普{b['sharpe']:.2f}")
        if c:
            print(f"    C: {c['trades']}笔, 笔均{c['avg_pct']:+.2f}%, 胜率{c['win_rate']:.1f}%, 夏普{c['sharpe']:.2f}")
    
    # 对比表
    print(f"\n{'='*70}")
    print("  新旧参数对比(全量)")
    print(f"{'='*70}")
    print(f"  {'指标':<20} {'旧参数':>12} {'新参数':>12} {'变化':>12}")
    print(f"  {'-'*56}")
    print(f"  {'策略B总盈亏':<20} {'¥-40,923':>12}", end="")
    bpnl = m.get('strat_B', {}).get('total_pnl', 0)
    print(f" {'¥'+f'{bpnl:+,.0f}':>12}")
    print(f"  {'策略B笔均':<20} {'-1.67%':>12}", end="")
    bpct = m.get('strat_B', {}).get('avg_pct', 0)
    print(f" {bpct:+.2f}%{'':>9}")
    print(f"  {'策略B夏普':<20} {'-4.61':>12}", end="")
    bsharpe = m.get('strat_B', {}).get('sharpe', 0)
    print(f" {bsharpe:.2f}{'':>10}")
    print(f"  {'策略C总盈亏':<20} {'¥+7,543':>12}", end="")
    cpnl = m.get('strat_C', {}).get('total_pnl', 0)
    print(f" {'¥'+f'{cpnl:+,.0f}':>12}")
