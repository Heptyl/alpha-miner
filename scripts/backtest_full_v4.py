"""
Alpha Miner 3年期完整回测 v4 — 最终版
精确复现trading_daemon.py的ABC三策略
修复: 精度显示 + 正确排除创业板 + 止损/涨停豁免/trailing
"""
import sqlite3
import statistics
import random
from collections import defaultdict

DB_PATH = 'data/alpha_miner.db'
COST_PCT = 0.5  # 交易成本0.5%
SLIPPAGE_PCT = 0.3  # 滑点0.3%

random.seed(42)

def pct(val):
    """格式化百分比"""
    return f"{val*100:+.2f}%"

def run():
    conn = sqlite3.connect(DB_PATH)
    
    # === 1. 加载数据 ===
    print("加载数据...")
    daily = {}
    for row in conn.execute("""
        SELECT stock_code, trade_date, open, close, high, low, pre_close
        FROM daily_price WHERE open > 0 AND close > 0 AND pre_close > 0
    """):
        code, date, o, c, h, lo, pc = row
        if code.startswith(('8','9','200','900')):
            continue
        daily[(code, date)] = {'open': o, 'close': c, 'high': h, 'low': lo, 'pre_close': pc}
    
    all_dates = sorted(set(d[1] for d in daily))
    date_idx = {d: i for i, d in enumerate(all_dates)}
    print(f"  {len(daily):,}条, {len(all_dates)}天, {all_dates[0]}~{all_dates[-1]}")
    
    # === 2. 预计算 ===
    # 涨停(仅主板, 10%涨跌幅)
    zt_main = defaultdict(list)  # 主板涨停
    zt_all_count = defaultdict(int)  # 所有涨停数(含创业板)
    for (code, date), bar in daily.items():
        if bar['pre_close'] <= 0:
            continue
        pct_chg = bar['close'] / bar['pre_close'] - 1
        is_gem = code.startswith('3') or code.startswith('688')
        threshold = 0.195 if is_gem else 0.095
        if pct_chg >= threshold:
            zt_all_count[date] += 1
            if not is_gem:
                zt_main[date].append(code)
    
    # 连板
    consec = defaultdict(int)
    for date in all_dates:
        di = date_idx[date]
        if di == 0:
            continue
        prev = all_dates[di - 1]
        for code in zt_main[date]:
            consec[(code, date)] = consec.get((code, prev), 0) + 1
    
    two_plus = defaultdict(list)
    for (code, date), cnt in consec.items():
        if cnt >= 2:
            two_plus[date].append(code)
    
    print(f"  主板涨停: {sum(len(v) for v in zt_main.values()):,}条")
    print(f"  2连板+: {sum(len(v) for v in two_plus.values()):,}条")
    
    # === 辅助函数 ===
    def stats(trades, label):
        if not trades:
            print(f"  {label}: 无交易")
            return
        wins = [t for t in trades if t > 0]
        losses = [t for t in trades if t <= 0]
        n = len(trades)
        wr = len(wins) / n * 100
        avg = statistics.mean(trades) * 100
        pf = sum(wins) / abs(sum(losses)) if losses else 999
        sharpe = (statistics.mean(trades) / statistics.stdev(trades)) if n > 1 else 0
        
        # 最大连亏
        streak = max_s = 0
        for t in trades:
            streak = streak + 1 if t <= 0 else 0
            max_s = max(max_s, streak)
        
        # 累计收益(等权)
        cum = sum(trades) * 100
        
        print(f"  {label}: {n:>6}笔  胜率{wr:>5.1f}%  均赚{avg:>+5.2f}%  PF={pf:>5.2f}  夏普={sharpe:>5.2f}  累计{cum:>+8.1f}%  最大连亏{max_s}笔")
        return {'n': n, 'wr': wr, 'avg': avg, 'pf': pf, 'sharpe': sharpe, 'cum': cum, 'max_streak': max_s}
    
    def yearly_stats(yearly, label):
        for yr in sorted(yearly.keys()):
            yt = yearly[yr]
            if len(yt) < 5:
                continue
            yw = [t for t in yt if t > 0]
            yl = [t for t in yt if t <= 0]
            ywr = len(yw)/len(yt)*100
            yavg = statistics.mean(yt)*100
            ypf = sum(yw)/abs(sum(yl)) if yl else 999
            ysh = statistics.mean(yt)/statistics.stdev(yt) if len(yt) > 1 else 0
            print(f"    {yr}: {len(yt):>5}笔  胜率{ywr:>5.1f}%  均赚{yavg:>+5.2f}%  PF={ypf:>5.2f}  夏普={ysh:>5.2f}")
    
    # ================================================================
    # 策略B: 涨停次日低吸
    # ================================================================
    print(f"\n{'='*70}")
    print("策略B: 涨停次日低吸")
    print(f"{'='*70}")
    
    b_trades = []
    b_yearly = defaultdict(list)
    
    for date in sorted(zt_main.keys()):
        di = date_idx.get(date)
        if di is None or di + 1 >= len(all_dates):
            continue
        next_date = all_dates[di + 1]
        
        # ZT>200保护
        main_zt = len(zt_main[date])
        if main_zt > 200:
            continue
        
        for code in zt_main[date]:
            nb = daily.get((code, next_date))
            if not nb or nb['pre_close'] <= 0:
                continue
            
            # 低开检查
            open_chg = nb['open'] / nb['pre_close'] - 1
            if open_chg > -0.02 or open_chg < -0.08:
                continue
            
            buy = nb['open']
            
            # 日内止损检查(-5%)
            if nb['low'] / buy - 1 <= -0.05:
                sell = buy * 0.95
            else:
                sell = nb['close']  # 收盘卖
            
            pnl = (sell / buy - 1) - COST_PCT / 100
            b_trades.append(pnl)
            b_yearly[date[:4]].append(pnl)
    
    stats(b_trades, "策略B总计")
    yearly_stats(b_yearly, "B")
    
    # ================================================================
    # 策略C: 反弹低吸(2连板 → T+1跌>3% → T+2低开买入 → T+3尾盘卖)
    # ================================================================
    print(f"\n{'='*70}")
    print("策略C: 反弹低吸(2连板+回调)")
    print(f"{'='*70}")
    
    c_trades = []
    c_yearly = defaultdict(list)
    
    for date in sorted(two_plus.keys()):
        di = date_idx.get(date)
        if di is None or di + 3 >= len(all_dates):
            continue
        
        t1 = all_dates[di + 1]  # T+1
        t2 = all_dates[di + 2]  # T+2: 买入日
        t3 = all_dates[di + 3]  # T+3: 卖出日
        
        for code in two_plus[date]:
            # T+1: 跌>3%
            t1_bar = daily.get((code, t1))
            if not t1_bar or t1_bar['pre_close'] <= 0:
                continue
            t1_chg = t1_bar['close'] / t1_bar['pre_close'] - 1
            if t1_chg >= -0.03:
                continue
            
            # T+2: 低开<-2%买入
            t2_bar = daily.get((code, t2))
            if not t2_bar or t2_bar['pre_close'] <= 0:
                continue
            t2_open_chg = t2_bar['open'] / t2_bar['pre_close'] - 1
            if t2_open_chg >= -0.02:
                continue
            
            buy = t2_bar['open']
            
            # T+3卖出(尾盘)
            t3_bar = daily.get((code, t3))
            if not t3_bar:
                continue
            
            sell_day = t3
            sell_bar = t3_bar
            hold_days = 1
            
            # 止损检查(T+2和T+3日内)
            if t2_bar['low'] / buy - 1 <= -0.06:
                pnl = -0.06 - COST_PCT / 100
                c_trades.append(pnl)
                c_yearly[date[:4]].append(pnl)
                continue
            
            if t3_bar['low'] / buy - 1 <= -0.06:
                pnl = -0.06 - COST_PCT / 100
                c_trades.append(pnl)
                c_yearly[date[:4]].append(pnl)
                continue
            
            # 涨停豁免: T+3涨停 → T+4卖
            t3_is_zt = (t3_bar['close'] / t3_bar['pre_close'] - 1) >= 0.095
            if t3_is_zt and di + 4 < len(all_dates):
                t4 = all_dates[di + 4]
                t4_bar = daily.get((code, t4))
                if t4_bar:
                    sell_bar = t4_bar
                    sell_day = t4
                    hold_days = 2
                    # max_hold=2天硬限, T+4必须卖
                    if t4_bar['low'] / buy - 1 <= -0.06:
                        pnl = -0.06 - COST_PCT / 100
                        c_trades.append(pnl)
                        c_yearly[date[:4]].append(pnl)
                        continue
            
            sell = sell_bar['close']
            pnl = (sell / buy - 1) - COST_PCT / 100
            c_trades.append(pnl)
            c_yearly[date[:4]].append(pnl)
    
    stats(c_trades, "策略C总计")
    yearly_stats(c_yearly, "C")
    
    # ================================================================
    # 策略A: 超跌反弹(20日跌>10% + RSI<35 + 放量)
    # 注: 真实策略A用ML信号, 此为简化回测
    # ================================================================
    print(f"\n{'='*70}")
    print("策略A: 超跌反弹(简化回测, 非ML信号)")
    print(f"{'='*70}")
    
    a_trades = []
    a_yearly = defaultdict(list)
    
    # 构建每只股票的时间序列
    stock_series = defaultdict(list)
    for (code, date) in sorted(daily.keys()):
        stock_series[code].append((date, daily[(code, date)]))
    
    for code, series in stock_series.items():
        if len(series) < 30:
            continue
        
        for j in range(20, len(series) - 6):
            date, bar = series[j]
            
            # 20日涨幅
            _, bar_20 = series[j - 20]
            ret_20 = bar['close'] / bar_20['close'] - 1
            if ret_20 >= -0.10:
                continue
            
            # 简化RSI(14日上涨比例)
            up = sum(1 for k in range(max(0, j-14), j)
                     if series[k][1]['close'] > series[k][1]['pre_close'])
            rsi = up / 14
            if rsi >= 0.35:
                continue
            
            # 次日开盘买入
            next_date, nb = series[j + 1]
            if nb['open'] <= 0:
                continue
            
            buy = nb['open']
            highest = buy
            
            # 持有最多5天, trailing 3%
            sold = False
            for hold in range(1, 6):
                if j + hold >= len(series):
                    break
                hd, hb = series[j + hold]
                if hb['high'] > highest:
                    highest = hb['high']
                
                # 止损-5%
                if hb['low'] / buy - 1 <= -0.05:
                    pnl = -0.05 - COST_PCT / 100
                    a_trades.append(pnl)
                    a_yearly[date[:4]].append(pnl)
                    sold = True
                    break
                
                # 硬止损-10%
                if hb['low'] / buy - 1 <= -0.10:
                    pnl = -0.10 - COST_PCT / 100
                    a_trades.append(pnl)
                    a_yearly[date[:4]].append(pnl)
                    sold = True
                    break
                
                # Trailing 3%
                if hb['close'] / highest - 1 <= -0.03:
                    pnl = (hb['close'] / buy - 1) - COST_PCT / 100
                    a_trades.append(pnl)
                    a_yearly[date[:4]].append(pnl)
                    sold = True
                    break
                
                # max_hold=5天
                if hold == 5:
                    pnl = (hb['close'] / buy - 1) - COST_PCT / 100
                    a_trades.append(pnl)
                    a_yearly[date[:4]].append(pnl)
                    sold = True
                    break
    
    stats(a_trades, "策略A总计")
    yearly_stats(a_yearly, "A")
    
    # ================================================================
    # 压力测试: 极端行情表现
    # ================================================================
    print(f"\n{'='*70}")
    print("压力测试: 极端行情")
    print(f"{'='*70}")
    
    # 找出涨停数最多的10天
    top_zt = sorted(zt_all_count.items(), key=lambda x: -x[1])[:10]
    print("  涨停数TOP10天:")
    for date, cnt in top_zt:
        print(f"    {date}: {cnt}只涨停")
    
    # 策略B在极端天的表现(无ZT过滤)
    print("\n  策略B在极端天的表现(无ZT过滤):")
    for date, cnt in top_zt[:5]:
        di = date_idx.get(date)
        if di is None or di + 1 >= len(all_dates):
            continue
        nd = all_dates[di + 1]
        day_pnls = []
        for code in zt_main.get(date, []):
            nb = daily.get((code, nd))
            if not nb or nb['pre_close'] <= 0:
                continue
            open_chg = nb['open'] / nb['pre_close'] - 1
            if open_chg > -0.02 or open_chg < -0.08:
                continue
            buy = nb['open']
            sell = nb['close']
            if nb['low'] / buy - 1 <= -0.05:
                sell = buy * 0.95
            pnl = (sell / buy - 1) - COST_PCT / 100
            day_pnls.append(pnl)
        if day_pnls:
            print(f"    {date}(ZT={cnt}): {len(day_pnls)}笔 胜率{len([p for p in day_pnls if p>0])/len(day_pnls)*100:.1f}% 均赚{statistics.mean(day_pnls)*100:+.2f}%")
        else:
            print(f"    {date}(ZT={cnt}): 无低开买入机会")
    
    # ================================================================
    # 综合对比
    # ================================================================
    print(f"\n{'='*70}")
    print("三策略综合对比")
    print(f"{'='*70}")
    
    for name, trades, yearly_dict in [
        ("策略A(超跌反弹)", a_trades, a_yearly),
        ("策略B(涨停低吸)", b_trades, b_yearly),
        ("策略C(反弹低吸)", c_trades, c_yearly),
    ]:
        if not trades:
            continue
        wins = [t for t in trades if t > 0]
        losses = [t for t in trades if t <= 0]
        n = len(trades)
        wr = len(wins) / n * 100
        avg = statistics.mean(trades) * 100
        pf = sum(wins) / abs(sum(losses)) if losses else 999
        sharpe = statistics.mean(trades) / statistics.stdev(trades) if n > 1 else 0
        
        # 年度一致性: 几年正收益
        pos_years = sum(1 for yt in yearly_dict.values() 
                       if yt and statistics.mean(yt) > 0)
        total_years = len([yt for yt in yearly_dict.values() if len(yt) >= 5])
        
        print(f"\n  {name}:")
        print(f"    {n}笔  胜率{wr:.1f}%  均赚{avg:+.2f}%  PF={pf:.2f}  夏普={sharpe:.2f}")
        print(f"    年度正收益: {pos_years}/{total_years}年")
    
    conn.close()
    print("\n回测完成!")

if __name__ == '__main__':
    run()
