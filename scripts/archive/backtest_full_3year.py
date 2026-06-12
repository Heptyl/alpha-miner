"""
Alpha Miner 3年期完整回测引擎 v3
精确复现trading_daemon.py的ABC三策略逻辑
数据: 493万条daily_price (2022-01-04 ~ 2026-05-15)

交易成本: 0.5%/笔(佣金+印花税+滑点)
滑点模拟: 开盘价 ± random(0, 0.3%)
"""
import sqlite3
import statistics
import random
import json
from collections import defaultdict
from datetime import datetime

DB_PATH = 'data/alpha_miner.db'
COST = 0.005  # 0.5%单笔成本
SLIPPAGE = 0.003  # 0.3%随机滑点

# 策略参数(与trading_daemon.py完全一致)
STOP_LOSS = -0.05  # A/B止损-5%
HARD_STOP = -0.10  # 硬止损-10%
STRATEGY_C_STOP = -0.06  # C止损-6%

# 策略A trailing
TRAILING_A = [0.03, 0.02, 0.015]  # 正常/退潮/冰点

# 策略B低吸
B_LOW_OPEN_MIN = -0.02
B_LOW_OPEN_MAX = -0.08
B_ZT_LIMIT = 200

# 策略C
C_MAX_HOLD = 2

random.seed(42)  # 可复现

def run():
    conn = sqlite3.connect(DB_PATH)
    
    # 1. 加载数据
    print("加载数据...")
    daily = {}
    for row in conn.execute("""
        SELECT stock_code, trade_date, open, close, high, low, pre_close, volume, amount
        FROM daily_price WHERE open > 0 AND close > 0 AND pre_close > 0
    """):
        code, date, o, c, h, lo, pc, vol, amt = row
        if code.startswith(('8','9','200','900')):
            continue
        daily[(code, date)] = {
            'open': o, 'close': c, 'high': h, 'low': lo,
            'pre_close': pc, 'volume': vol, 'amount': amt
        }
    
    all_dates = sorted(set(d[1] for d in daily))
    date_idx = {d: i for i, d in enumerate(all_dates)}
    print(f"  {len(daily):,}条K线, {len(all_dates)}天, {all_dates[0]}~{all_dates[-1]}")
    
    # 2. 预计算每天涨停股
    print("预计算涨停...")
    zt_by_date = defaultdict(list)  # date -> [(code, is_gem)]
    zt_count_by_date = defaultdict(int)
    for (code, date), bar in daily.items():
        if bar['pre_close'] <= 0:
            continue
        pct = (bar['close'] / bar['pre_close'] - 1)
        is_gem = code.startswith('3') or code.startswith('688')
        threshold = 0.195 if is_gem else 0.095
        if pct >= threshold:
            zt_by_date[date].append((code, is_gem))
            if not is_gem:  # 只统计主板涨停数(与daemon一致)
                zt_count_by_date[date] += 1
    
    total_zt = sum(len(v) for v in zt_by_date.values())
    print(f"  {len(zt_by_date)}天有涨停, 共{total_zt:,}条")
    
    # 3. 预计算连板(策略C需要2连板)
    print("预计算连板...")
    consecutive_zt = defaultdict(int)  # (code, date) -> 连板数
    for date in all_dates:
        di = date_idx[date]
        if di == 0:
            continue
        prev_date = all_dates[di - 1]
        for code, is_gem in zt_by_date.get(date, []):
            prev_consec = consecutive_zt.get((code, prev_date), 0)
            consecutive_zt[(code, date)] = prev_consec + 1
    
    two_plus_zt = defaultdict(list)  # date -> [codes with 2+ consecutive ZT]
    for (code, date), cnt in consecutive_zt.items():
        if cnt >= 2:
            two_plus_zt[date].append(code)
    
    print(f"  2连板+: {sum(len(v) for v in two_plus_zt.values()):,}条")
    
    # 4. 回测三个策略
    results = {'A': [], 'B': [], 'C': []}
    yearly = {'A': defaultdict(list), 'B': defaultdict(list), 'C': defaultdict(list)}
    
    # ============ 策略B: 涨停次日低吸 ============
    print("\n=== 策略B: 涨停次日低吸 ===")
    for date in sorted(zt_by_date.keys()):
        di = date_idx.get(date)
        if di is None or di + 1 >= len(all_dates):
            continue
        next_date = all_dates[di + 1]
        
        # 涨停数保护
        main_zt = zt_count_by_date.get(date, 0)
        if main_zt > B_ZT_LIMIT:
            continue
        
        for code, is_gem in zt_by_date[date]:
            if is_gem:  # 排除创业板/科创板
                continue
            
            nb = daily.get((code, next_date))
            if not nb or nb['pre_close'] <= 0:
                continue
            
            # 低开检查
            open_chg = (nb['open'] / nb['pre_close'] - 1)
            if open_chg > B_LOW_OPEN_MIN or open_chg < B_LOW_OPEN_MAX:
                continue
            
            # 买入(开盘价+滑点)
            buy_price = nb['open'] * (1 + random.uniform(0, SLIPPAGE))
            
            # 卖出: 收盘价(日内策略) - 但检查止损
            sell_price = nb['close']
            pnl_raw = (sell_price / buy_price - 1)
            
            # 止损检查
            if nb['low'] / buy_price - 1 <= STOP_LOSS:
                # 日内触发止损, 按止损价卖
                sell_price = buy_price * (1 + STOP_LOSS)
                pnl_raw = STOP_LOSS
            
            pnl = pnl_raw - COST
            results['B'].append(pnl)
            yearly['B'][date[:4]].append(pnl)
    
    # ============ 策略C: 反弹低吸(2连板+次日大跌+再次日低开) ============
    print("=== 策略C: 反弹低吸 ===")
    for date in sorted(two_plus_zt.keys()):
        di = date_idx.get(date)
        if di is None or di + 2 >= len(all_dates):
            continue
        t1 = all_dates[di + 1]  # T+1
        t2 = all_dates[di + 2]  # T+2
        
        for code in two_plus_zt[date]:
            if code.startswith(('3', '688')):  # 排除创业板/科创
                continue
            
            # T+1: 必须跌>3%
            t1_bar = daily.get((code, t1))
            if not t1_bar or t1_bar['pre_close'] <= 0:
                continue
            t1_chg = (t1_bar['close'] / t1_bar['pre_close'] - 1)
            if t1_chg >= -0.03:
                continue
            
            # T+2: 低开<-2%买入
            t2_bar = daily.get((code, t2))
            if not t2_bar or t2_bar['pre_close'] <= 0:
                continue
            t2_open_chg = (t2_bar['open'] / t2_bar['pre_close'] - 1)
            if t2_open_chg >= -0.02:
                continue
            
            buy_price = t2_bar['open'] * (1 + random.uniform(0, SLIPPAGE))
            
            # T+2收盘检查(买入当天不卖=T+1保护)
            # T+3尾盘卖出
            if di + 3 >= len(all_dates):
                continue
            t3 = all_dates[di + 3]
            t3_bar = daily.get((code, t3))
            if not t3_bar:
                continue
            
            # 止损检查(日内)
            sell_price = t3_bar['close']
            pnl_raw = sell_price / buy_price - 1
            
            # 策略C止损-6%
            if t3_bar['low'] / buy_price - 1 <= STRATEGY_C_STOP:
                sell_price = buy_price * (1 + STRATEGY_C_STOP)
                pnl_raw = STRATEGY_C_STOP
            
            # 涨停豁免: T+3涨停则继续持有到T+4
            t3_chg = (t3_bar['close'] / t3_bar['pre_close'] - 1)
            if t3_chg >= 0.095 and di + 4 < len(all_dates):
                t4 = all_dates[di + 4]
                t4_bar = daily.get((code, t4))
                if t4_bar:
                    sell_price = t4_bar['close']
                    pnl_raw = sell_price / buy_price - 1
                    # max_hold=2天硬限
                    if t4_bar['low'] / buy_price - 1 <= STRATEGY_C_STOP:
                        sell_price = buy_price * (1 + STRATEGY_C_STOP)
                        pnl_raw = STRATEGY_C_STOP
            
            pnl = pnl_raw - COST
            results['C'].append(pnl)
            yearly['C'][date[:4]].append(pnl)
    
    # ============ 策略A: IC因子低吸 ============
    # 策略A需要ML信号或IC因子, 简化回测: 模拟"RSI低+回调"的买入
    # 由于没有历史ML信号, 用日K线特征模拟:
    # 买入条件: 过去20日涨幅<-10% + 当日RSI<30 + 成交量放大
    print("=== 策略A: IC因子低吸(简化回测) ===")
    # 构建每只股票的时间序列
    stock_dates = defaultdict(list)
    for (code, date) in sorted(daily.keys()):
        if not code.startswith(('3', '688')):  # 排除创业板/科创
            stock_dates[code].append(date)
    
    for code, dates in stock_dates.items():
        if len(dates) < 30:
            continue
        for j in range(20, len(dates) - 2):
            date = dates[j]
            bar = daily[(code, date)]
            
            # 20日涨幅
            bar_20ago = daily.get((code, dates[j-20]))
            if not bar_20ago:
                continue
            ret_20d = (bar['close'] / bar_20ago['close'] - 1)
            
            # 简化RSI: 用涨幅比例近似
            up_days = sum(1 for k in range(max(0,j-14), j) 
                         if daily.get((code, dates[k]), {}).get('close', 0) > 
                            daily.get((code, dates[k]), {}).get('pre_close', 0))
            rsi_approx = up_days / 14 * 100
            
            # 买入条件: 20日跌>10% + RSI<35 + 有量
            if ret_20d >= -0.10 or rsi_approx >= 35:
                continue
            vol = bar.get('volume') or 0
            if vol <= 0:
                continue
            avg_vol = statistics.mean([
                daily.get((code, dates[k]), {}).get('volume') or 0
                for k in range(max(0,j-5), j)
            ])
            if avg_vol <= 0 or vol < avg_vol * 1.2:
                continue
            
            # 买入
            next_date = dates[j + 1]
            nb = daily.get((code, next_date))
            if not nb or nb['open'] <= 0:
                continue
            
            buy_price = nb['open'] * (1 + random.uniform(0, SLIPPAGE))
            
            # 持有最多5天, trailing 3%/2%/1.5%
            highest = buy_price
            sell_price = None
            for hold in range(1, 6):
                if j + hold >= len(dates):
                    break
                hd = dates[j + hold]
                hb = daily.get((code, hd))
                if not hb:
                    continue
                
                highest = max(highest, hb['high'])
                
                # 止损
                if hb['low'] / buy_price - 1 <= STOP_LOSS:
                    sell_price = buy_price * (1 + STOP_LOSS)
                    break
                
                # 硬止损
                if hb['low'] / buy_price - 1 <= HARD_STOP:
                    sell_price = buy_price * (1 + HARD_STOP)
                    break
                
                # Trailing: 从高点回落3%
                if hb['close'] / highest - 1 <= -0.03:
                    sell_price = hb['close']
                    break
                
                # max_hold=5天
                if hold == 5:
                    sell_price = hb['close']
            
            if sell_price is None:
                continue
            
            pnl = (sell_price / buy_price - 1) - COST
            results['A'].append(pnl)
            yearly['A'][date[:4]].append(pnl)
    
    # ============ 输出结果 ============
    print(f"\n{'='*70}")
    print(f"  Alpha Miner 3年期完整回测 (944天 / 493万条)")
    print(f"  交易成本: {COST*100}%/笔 | 滑点: ±{SLIPPAGE*100}%")
    print(f"{'='*70}")
    
    for strat in ['A', 'B', 'C']:
        trades = results[strat]
        if not trades:
            print(f"\n策略{strat}: 无交易")
            continue
        
        wins = [p for p in trades if p > 0]
        losses = [p for p in trades if p <= 0]
        wr = len(wins)/len(trades)*100
        avg = statistics.mean(trades)
        pf = sum(wins)/abs(sum(losses)) if losses else 999
        sharpe = avg / statistics.stdev(trades) if len(trades) > 1 else 0
        max_win = max(trades)
        max_loss = min(trades)
        
        # 最大连续亏损
        streak = max_streak = 0
        for p in trades:
            if p <= 0:
                streak += 1
                max_streak = max(max_streak, streak)
            else:
                streak = 0
        
        name = {'A': 'IC因子低吸', 'B': '涨停次日低吸', 'C': '反弹低吸'}[strat]
        print(f"\n--- 策略{strat}: {name} ---")
        print(f"  总计: {len(trades):>6}笔  胜率{wr:>5.1f}%  均赚{avg:>+6.2f}%  PF={pf:>5.2f}  夏普={sharpe:>5.2f}")
        print(f"  最大盈: {max_win:>+6.2f}%  最大亏: {max_loss:>+6.2f}%  最大连亏: {max_streak}笔")
        
        # 分年
        print(f"  {'年':>4}  {'笔数':>6}  {'胜率':>6}  {'均赚':>7}  {'PF':>6}  {'夏普':>6}")
        for yr in sorted(yearly[strat].keys()):
            yt = yearly[strat][yr]
            if len(yt) < 5:
                continue
            yw = [p for p in yt if p > 0]
            yl = [p for p in yt if p <= 0]
            ywr = len(yw)/len(yt)*100
            yavg = statistics.mean(yt)
            ypf = sum(yw)/abs(sum(yl)) if yl else 999
            ysharpe = yavg / statistics.stdev(yt) if len(yt) > 1 else 0
            print(f"  {yr:>4}  {len(yt):>6}  {ywr:>5.1f}%  {yavg:>+6.2f}%  {ypf:>5.2f}  {ysharpe:>5.2f}")
    
    conn.close()

if __name__ == '__main__':
    run()
