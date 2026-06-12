#!/usr/bin/env python3
"""
三策略回测验证: 策略A(龙头首阴反包), 策略B(首板回踩低吸), 策略C(缩量反包)
数据源: alpha_miner.db (daily_price + zt_pool)
交易成本: 单边0.025% (买入万2.5+卖出万2.5+印花税万5)
"""
import sqlite3
import sys
from datetime import datetime, timedelta
import statistics
import math

DB_PATH = '/home/ccy/alpha-miner/data/alpha_miner.db'
COMMISSION_RATE = 0.00025  # 单边0.025%

def get_conn():
    return sqlite3.connect(DB_PATH)

def load_daily_prices(cur):
    """加载 daily_price，按 stock_code 组织成 {code: {date: row_dict}}"""
    print("Loading daily_price...")
    cur.execute("SELECT stock_code, trade_date, open, high, low, close, pre_close, volume, amount FROM daily_price ORDER BY trade_date")
    data = {}
    for row in cur.fetchall():
        code, date, o, h, l, c, pc, vol, amt = row
        if code not in data:
            data[code] = {}
        data[code][date] = {
            'open': o, 'high': h, 'low': l, 'close': c,
            'pre_close': pc, 'volume': vol, 'amount': amt
        }
    print(f"  Loaded {len(data)} stocks")
    return data

def load_zt_pool(cur):
    """加载 zt_pool, 按 trade_date 组织成 {date: [{code, consecutive_zt, ...}]}"""
    print("Loading zt_pool...")
    cur.execute("SELECT stock_code, trade_date, consecutive_zt FROM zt_pool ORDER BY trade_date")
    data = {}
    for row in cur.fetchall():
        code, date, cons_zt = row
        if date not in data:
            data[date] = []
        data[date].append({'code': code, 'consecutive_zt': cons_zt})
    print(f"  Loaded {len(data)} trading dates")
    return data

def get_all_trade_dates(cur):
    """获取所有交易日排序列表"""
    cur.execute("SELECT DISTINCT trade_date FROM daily_price ORDER BY trade_date")
    return [r[0] for r in cur.fetchall()]

def next_trade_date(trade_dates, current_date):
    """获取下一个交易日"""
    idx = trade_dates.index(current_date) if current_date in trade_dates else -1
    if idx >= 0 and idx + 1 < len(trade_dates):
        return trade_dates[idx + 1]
    return None

def get_trade_date_index(trade_dates, date):
    try:
        return trade_dates.index(date)
    except ValueError:
        return -1

# ============================================================
# 策略A: 连板龙头 → 首阴 → 次日确认 → 买入 → trailing卖出
# ============================================================
def backtest_strategy_a(daily, zt_pool, trade_dates):
    print("\n" + "="*60)
    print("策略A: 连板龙头首阴反包")
    print("="*60)
    
    trades = []
    td_idx = {d: i for i, d in enumerate(trade_dates)}
    
    for date in sorted(zt_pool.keys()):
        if date not in td_idx:
            continue
        di = td_idx[date]
        if di + 3 >= len(trade_dates):  # need at least D+3
            continue
        
        for zt in zt_pool[date]:
            if zt['consecutive_zt'] < 2:  # 连板>=2
                continue
            code = zt['code']
            if code not in daily:
                continue
            
            # Day D: 涨停日 (already confirmed by zt_pool)
            # Day D+1: 首阴日 - close < pre_close 且 close < open
            d1 = trade_dates[di + 1]
            if d1 not in daily.get(code, {}):
                continue
            bar_d1 = daily[code][d1]
            
            # 首阴条件: 收阴 (close < open) 且 下跌 (close < pre_close)
            # pre_close可能为0, 此时用D日的close作为参考
            if bar_d1['pre_close'] is None or bar_d1['pre_close'] <= 0:
                continue  # pre_close缺失, 无法判断首阴
            if not (bar_d1['close'] < bar_d1['pre_close'] and bar_d1['close'] < bar_d1['open']):
                continue
            
            # 跌幅<=5%
            chg_d1 = (bar_d1['close'] - bar_d1['pre_close']) / bar_d1['pre_close']
            if chg_d1 < -0.05:
                continue
            
            shouyin_low = bar_d1['low']  # 首阴最低价
            shouyin_close = bar_d1['close']
            
            # Day D+2: 确认日 - open > pre_close*1.02 且 close > open
            d2 = trade_dates[di + 2]
            if d2 not in daily.get(code, {}):
                continue
            bar_d2 = daily[code][d2]
            
            # 确认条件: 高开2%以上 + 收阳
            # pre_close可能为0(数据缺失), 此时用D+1的close作为参考价
            ref_price = bar_d2['pre_close'] if bar_d2['pre_close'] and bar_d2['pre_close'] > 0 else shouyin_close
            if ref_price <= 0:
                continue
            if not (bar_d2['open'] > ref_price * 1.02 and bar_d2['close'] > bar_d2['open']):
                continue
            
            # 买入: D+2 开盘价
            buy_price = bar_d2['open']
            if buy_price <= 0:
                continue
            
            # 持仓管理: 最多持3天 (含买入日D+2)
            # trailing: 从最高回落3% → 卖出
            # 止损: 跌破首阴最低价×0.98
            stop_loss_price = shouyin_low * 0.98
            highest = buy_price
            sell_price = None
            sell_date = None
            sell_reason = ''
            
            for hold_day in range(3):  # day 0,1,2 (D+2, D+3, D+4)
                d_h = trade_dates[di + 2 + hold_day]
                if d_h not in daily.get(code, {}):
                    continue
                bar_h = daily[code][d_h]
                
                # Update highest
                if bar_h['high'] > highest:
                    highest = bar_h['high']
                
                # Check stop loss first (using low)
                if bar_h['low'] <= stop_loss_price:
                    # Assume sell at stop loss price
                    sell_price = stop_loss_price
                    sell_date = d_h
                    sell_reason = 'stop_loss'
                    break
                
                # Check trailing stop (from highest drop 3%)
                trailing_price = highest * 0.97
                if bar_h['low'] <= trailing_price and hold_day > 0:
                    # Hit trailing stop
                    sell_price = trailing_price
                    sell_date = d_h
                    sell_reason = 'trailing_stop'
                    break
                
                # If last hold day, sell at close
                if hold_day == 2:
                    sell_price = bar_h['close']
                    sell_date = d_h
                    sell_reason = 'expire_3d'
                    break
                
                # End of day check: trailing from close perspective
                # If close triggers trailing (intraday high was good but close dropped)
                if bar_h['close'] <= highest * 0.97:
                    sell_price = bar_h['close']
                    sell_date = d_h
                    sell_reason = 'trailing_eod'
                    break
            
            if sell_price is None or sell_date is None:
                continue
            
            # Calculate return
            cost_buy = buy_price * (1 + COMMISSION_RATE)
            cost_sell = sell_price * (1 - COMMISSION_RATE - 0.0005)  # 卖出佣金+印花税
            ret = (cost_sell - cost_buy) / cost_buy
            
            trades.append({
                'code': code,
                'zt_date': date,
                'buy_date': trade_dates[di + 2],
                'sell_date': sell_date,
                'buy_price': buy_price,
                'sell_price': sell_price,
                'return': ret,
                'reason': sell_reason,
                'hold_days': get_trade_date_index(trade_dates, sell_date) - (di + 2) + 1,
                'highest': highest,
                'stop_loss': stop_loss_price,
                'consecutive_zt': zt['consecutive_zt'],
            })
    
    return trades

# ============================================================
# 策略B: 首板涨停次日回踩涨停开盘价±1%买入
# ============================================================
def backtest_strategy_b(daily, zt_pool, trade_dates):
    print("\n" + "="*60)
    print("策略B: 首板回踩低吸")
    print("="*60)
    
    trades = []
    td_idx = {d: i for i, d in enumerate(trade_dates)}
    
    for date in sorted(zt_pool.keys()):
        if date not in td_idx:
            continue
        di = td_idx[date]
        if di + 6 >= len(trade_dates):  # need enough future days
            continue
        
        for zt in zt_pool[date]:
            if zt['consecutive_zt'] != 1:  # 首板
                continue
            code = zt['code']
            if code not in daily:
                continue
            
            # Day D: 涨停日，获取开盘价
            if date not in daily.get(code, {}):
                continue
            bar_d = daily[code][date]
            zt_open = bar_d['open']  # 涨停开盘价
            
            if zt_open <= 0:
                continue
            
            # Day D+1: 回踩涨停开盘价±1%
            d1 = trade_dates[di + 1]
            if d1 not in daily.get(code, {}):
                continue
            bar_d1 = daily[code][d1]
            
            # 检查是否回踩到涨停开盘价±1%区间
            target_low = zt_open * 0.99
            target_high = zt_open * 1.01
            
            # 买入条件: 盘中低点触及涨停开盘价±1%区间
            if bar_d1['low'] > target_high:
                continue  # 没有回踩到
            
            # 买入价: 使用涨停开盘价(挂限价单在zt_open)
            # 这更现实: 在zt_open附近挂单等待成交
            if bar_d1['open'] <= target_high and bar_d1['open'] >= target_low:
                # 开盘就在目标区间, 用开盘价买入
                buy_price = bar_d1['open']
            elif bar_d1['low'] <= target_high:
                # 盘中回踩到目标区间, 用zt_open买入(挂单成交)
                buy_price = zt_open
            else:
                continue
            
            if buy_price <= 0:
                continue
            
            # 持仓管理: trailing 3%止盈, -3%止损
            highest = buy_price
            sell_price = None
            sell_date = None
            sell_reason = ''
            
            for hold_day in range(5):  # 最多持5天
                d_h = trade_dates[di + 1 + hold_day]
                if d_h not in daily.get(code, {}):
                    continue
                bar_h = daily[code][d_h]
                
                if bar_h['high'] > highest:
                    highest = bar_h['high']
                
                # 止损: -3%
                stop_loss = buy_price * 0.97
                if bar_h['low'] <= stop_loss:
                    sell_price = stop_loss
                    sell_date = d_h
                    sell_reason = 'stop_loss'
                    break
                
                # trailing 3%止盈
                trailing_price = highest * 0.97
                if bar_h['low'] <= trailing_price and highest > buy_price:
                    sell_price = trailing_price
                    sell_date = d_h
                    sell_reason = 'trailing_tp'
                    break
                
                # 5天到期
                if hold_day == 4:
                    sell_price = bar_h['close']
                    sell_date = d_h
                    sell_reason = 'expire_5d'
                    break
            
            if sell_price is None or sell_date is None:
                continue
            
            cost_buy = buy_price * (1 + COMMISSION_RATE)
            cost_sell = sell_price * (1 - COMMISSION_RATE - 0.0005)
            ret = (cost_sell - cost_buy) / cost_buy
            
            trades.append({
                'code': code,
                'zt_date': date,
                'buy_date': d1,
                'sell_date': sell_date,
                'buy_price': buy_price,
                'sell_price': sell_price,
                'return': ret,
                'reason': sell_reason,
                'hold_days': get_trade_date_index(trade_dates, sell_date) - td_idx[d1] + 1,
                'zt_open': zt_open,
            })
    
    return trades

# ============================================================
# 策略C: 涨停→次日缩量<50%→突破前高买入, 持2天
# ============================================================
def backtest_strategy_c(daily, zt_pool, trade_dates):
    print("\n" + "="*60)
    print("策略C: 缩量反包")
    print("="*60)
    
    trades = []
    td_idx = {d: i for i, d in enumerate(trade_dates)}
    
    for date in sorted(zt_pool.keys()):
        if date not in td_idx:
            continue
        di = td_idx[date]
        if di + 8 >= len(trade_dates):  # need future days
            continue
        
        for zt in zt_pool[date]:
            code = zt['code']
            if code not in daily:
                continue
            
            # Day D: 涨停日
            if date not in daily.get(code, {}):
                continue
            bar_d = daily[code][date]
            zt_high = bar_d['high']
            zt_vol = bar_d['volume']
            
            if zt_vol is None or zt_high is None or zt_vol <= 0 or zt_high <= 0:
                continue
            
            # Day D+1: 缩量<50%
            d1 = trade_dates[di + 1]
            if d1 not in daily.get(code, {}):
                continue
            bar_d1 = daily[code][d1]
            
            if bar_d1['volume'] is None or zt_vol is None or bar_d1['volume'] >= zt_vol * 0.5:
                continue  # 没有缩量
            
            # 寻找突破前高(涨停日最高价)的买入点
            # 从D+2开始观察, 最多观察5天
            bought = False
            for watch_day in range(5):
                d_w = trade_dates[di + 2 + watch_day]
                if d_w not in daily.get(code, {}):
                    continue
                bar_w = daily[code][d_w]
                
                # 突破: 盘中最高价 > 涨停日最高价
                if bar_w['high'] > zt_high:
                    # 修正: 如果开盘已高于zt_high(跳空高开), 只能按open买入
                    if bar_w['open'] > zt_high:
                        buy_price = bar_w['open']
                    else:
                        buy_price = zt_high  # 在前高处买入
                    bought = True
                    
                    # 持2天
                    buy_di = di + 2 + watch_day
                    sell_price = None
                    sell_date = None
                    
                    # 持2天: 在第2天收盘卖出
                    if buy_di + 2 < len(trade_dates):
                        d_sell = trade_dates[buy_di + 2]
                        if d_sell in daily.get(code, {}):
                            sell_price = daily[code][d_sell]['close']
                            sell_date = d_sell
                    
                    if sell_price is None or sell_date is None:
                        # Try day 1
                        if buy_di + 1 < len(trade_dates):
                            d_sell = trade_dates[buy_di + 1]
                            if d_sell in daily.get(code, {}):
                                sell_price = daily[code][d_sell]['close']
                                sell_date = d_sell
                    
                    if sell_price is not None and sell_date is not None:
                        cost_buy = buy_price * (1 + COMMISSION_RATE)
                        cost_sell = sell_price * (1 - COMMISSION_RATE - 0.0005)
                        ret = (cost_sell - cost_buy) / cost_buy
                        
                        trades.append({
                            'code': code,
                            'zt_date': date,
                            'buy_date': d_w,
                            'sell_date': sell_date,
                            'buy_price': buy_price,
                            'sell_price': sell_price,
                            'return': ret,
                            'reason': 'hold_2d',
                            'hold_days': get_trade_date_index(trade_dates, sell_date) - buy_di + 1,
                            'vol_ratio': bar_d1['volume'] / zt_vol,
                        })
                    break  # Only first breakout
    
    return trades

# ============================================================
# 统计输出
# ============================================================
def print_stats(trades, strategy_name):
    print(f"\n--- {strategy_name} 回测结果 ---")
    n = len(trades)
    print(f"总笔数: {n}")
    
    if n == 0:
        print("无交易, 跳过统计")
        return
    
    returns = [t['return'] for t in trades]
    wins = [r for r in returns if r > 0]
    losses = [r for r in returns if r <= 0]
    
    total_win = sum(wins)
    total_loss = abs(sum(losses))
    pf = total_win / total_loss if total_loss > 0 else float('inf')
    
    win_rate = len(wins) / n * 100
    avg_ret = statistics.mean(returns) * 100
    
    # 95% CI for mean return
    if n > 1:
        std_ret = statistics.stdev(returns) * 100
        ci_half = 1.96 * std_ret / math.sqrt(n)
        ci_low = avg_ret - ci_half
        ci_high = avg_ret + ci_half
    else:
        ci_low = ci_high = avg_ret
    
    print(f"盈利笔数: {len(wins)}, 亏损笔数: {len(losses)}")
    print(f"PF (Profit Factor): {pf:.4f}")
    print(f"胜率: {win_rate:.1f}%")
    print(f"均赚: {avg_ret:.3f}%")
    print(f"95% CI: [{ci_low:.3f}%, {ci_high:.3f}%]")
    print(f"中位数收益: {statistics.median(returns)*100:.3f}%")
    print(f"最大单笔盈利: {max(returns)*100:.3f}%")
    print(f"最大单笔亏损: {min(returns)*100:.3f}%")
    
    # Exit reason distribution
    reasons = {}
    for t in trades:
        r = t['reason']
        reasons[r] = reasons.get(r, 0) + 1
    print(f"卖出原因分布: {reasons}")
    
    # Monthly breakdown
    monthly = {}
    for t in trades:
        m = t['buy_date'][:7]  # YYYY-MM
        if m not in monthly:
            monthly[m] = []
        monthly[m].append(t['return'])
    print(f"\n月度统计:")
    for m in sorted(monthly.keys()):
        mn = len(monthly[m])
        mr = statistics.mean(monthly[m]) * 100
        mw = sum(1 for r in monthly[m] if r > 0) / mn * 100
        print(f"  {m}: {mn}笔, 均赚{mr:.2f}%, 胜率{mw:.0f}%")
    
    # Show sample trades
    print(f"\n最近10笔交易:")
    for t in trades[-10:]:
        print(f"  {t['code']} 买:{t['buy_date']}@{t['buy_price']:.2f} 卖:{t['sell_date']}@{t['sell_price']:.2f} "
              f"收益:{t['return']*100:.2f}% 持{t.get('hold_days','?')}天 原因:{t['reason']}")
    
    return {
        'n': n, 'pf': pf, 'win_rate': win_rate, 
        'avg_ret': avg_ret, 'ci_low': ci_low, 'ci_high': ci_high
    }

def main():
    conn = get_conn()
    cur = conn.cursor()
    
    # Load data
    daily = load_daily_prices(cur)
    zt_pool_data = load_zt_pool(cur)
    trade_dates = get_all_trade_dates(cur)
    
    print(f"\n交易日总数: {len(trade_dates)}")
    print(f"zt_pool日期范围: {min(zt_pool_data.keys())} ~ {max(zt_pool_data.keys())}")
    
    # Run backtests
    trades_a = backtest_strategy_a(daily, zt_pool_data, trade_dates)
    stats_a = print_stats(trades_a, "策略A (龙头首阴反包)")
    
    trades_b = backtest_strategy_b(daily, zt_pool_data, trade_dates)
    stats_b = print_stats(trades_b, "策略B (首板回踩低吸)")
    
    trades_c = backtest_strategy_c(daily, zt_pool_data, trade_dates)
    stats_c = print_stats(trades_c, "策略C (缩量反包)")
    
    # Summary
    print("\n" + "="*60)
    print("汇总对比")
    print("="*60)
    print(f"{'策略':<20} {'笔数':>6} {'PF':>8} {'胜率':>8} {'均赚':>8} {'95%CI':>20}")
    for name, s in [("策略A(龙头首阴反包)", stats_a), 
                     ("策略B(首板回踩低吸)", stats_b),
                     ("策略C(缩量反包)", stats_c)]:
        if s:
            print(f"{name:<20} {s['n']:>6} {s['pf']:>8.4f} {s['win_rate']:>7.1f}% {s['avg_ret']:>7.3f}% [{s['ci_low']:.3f}%,{s['ci_high']:.3f}%]")
        else:
            print(f"{name:<20} {'N/A':>6}")
    
    conn.close()

if __name__ == '__main__':
    main()
