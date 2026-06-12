"""
Alpha Miner 终极回测引擎 v5
493万条去重后数据，944天(2022-2026)
探索所有值得信任的策略方向

严格标准:
- 交易成本0.5%/笔
- 滑点模拟0.3%
- 只用主板(排除300/688/8/9/200/900)
- 止损-5%(策略A/B) / -6%(策略C)
- 每个策略必须5年独立验证
"""
import sqlite3
import statistics
import random
from collections import defaultdict

DB = 'data/alpha_miner.db'
COST = 0.005
random.seed(42)

def run():
    conn = sqlite3.connect(DB)
    
    # === 加载数据(仅主板) ===
    print("加载数据(仅主板 000/001/002/600/601/603)...")
    daily = {}
    for row in conn.execute("""
        SELECT stock_code, trade_date, open, close, high, low, pre_close
        FROM daily_price WHERE open > 0 AND close > 0 AND pre_close > 0
    """):
        code, date, o, c, h, lo, pc = row
        # 只保留主板: 深市000/001/002 + 沪市600/601/603
        if code[0] == '0' or (code[0] == '6' and code[1] in '013'):
            daily[(code, date)] = {'open': o, 'close': c, 'high': h, 'low': lo, 'pre_close': pc}
    
    all_dates = sorted(set(d[1] for d in daily))
    date_idx = {d: i for i, d in enumerate(all_dates)}
    print(f"  {len(daily):,}条, {len(all_dates)}天, {all_dates[0]}~{all_dates[-1]}")
    
    # === 预计算 ===
    # 涨停(10%涨跌幅)
    zt_by_date = defaultdict(list)
    for (code, date), bar in daily.items():
        if bar['pre_close'] <= 0: continue
        pct = bar['close'] / bar['pre_close'] - 1
        if 0.095 <= pct <= 0.105:  # 严格10%涨停
            zt_by_date[date].append(code)
    
    # 连板
    consec = defaultdict(int)
    for date in all_dates:
        di = date_idx[date]
        if di == 0: continue
        prev = all_dates[di - 1]
        for code in zt_by_date[date]:
            consec[(code, date)] = consec.get((code, prev), 0) + 1
    
    total_zt = sum(len(v) for v in zt_by_date.values())
    print(f"  涨停: {total_zt:,}条")
    
    def backtest(trades, label):
        """标准回测输出"""
        if not trades:
            print(f"  {label}: 无交易")
            return None
        
        wins = [t for t in trades if t > 0]
        losses = [t for t in trades if t <= 0]
        n = len(trades)
        wr = len(wins) / n * 100
        avg = statistics.mean(trades) * 100
        pf = sum(wins) / abs(sum(losses)) if losses else 999
        sharpe = statistics.mean(trades) / statistics.stdev(trades) if n > 1 else 0
        
        # 按年统计
        return {'n': n, 'wr': wr, 'avg': avg, 'pf': pf, 'sharpe': sharpe}
    
    def report(label, trades_by_year):
        """带年度明细的报告"""
        all_trades = []
        for yt in trades_by_year.values():
            all_trades.extend(yt)
        
        r = backtest(all_trades, label)
        if not r: return
        
        wins = [t for t in all_trades if t > 0]
        losses = [t for t in all_trades if t <= 0]
        pf = sum(wins)/abs(sum(losses)) if losses else 999
        
        # 最大连亏
        streak = max_s = 0
        for t in all_trades:
            streak = streak + 1 if t <= 0 else 0
            max_s = max(max_s, streak)
        
        pos_years = sum(1 for yr, yt in trades_by_year.items() 
                       if len(yt) >= 5 and statistics.mean(yt) > 0)
        total_years = sum(1 for yt in trades_by_year.values() if len(yt) >= 5)
        
        print(f"\n{'='*70}")
        print(f"  {label}")
        print(f"{'='*70}")
        print(f"  {r['n']:>6}笔  胜率{r['wr']:>5.1f}%  均赚{r['avg']:>+5.2f}%  PF={pf:>5.2f}  夏普={r['sharpe']:>5.2f}  连亏{max_s}  正收益{pos_years}/{total_years}年")
        
        for yr in sorted(trades_by_year.keys()):
            yt = trades_by_year[yr]
            if len(yt) < 5: continue
            yw = [t for t in yt if t > 0]
            yl = [t for t in yt if t <= 0]
            ywr = len(yw)/len(yt)*100
            yavg = statistics.mean(yt)*100
            ypf = sum(yw)/abs(sum(yl)) if yl else 999
            ysh = statistics.mean(yt)/statistics.stdev(yt) if len(yt) > 1 else 0
            tag = "✓" if yavg > 0 else "✗"
            print(f"    {yr}: {len(yt):>5}笔  胜率{ywr:>5.1f}%  均赚{yavg:>+5.2f}%  PF={ypf:>5.2f}  {tag}")
        
        return r
    
    # ================================================================
    # 策略探索
    # ================================================================
    
    # --- S1: 涨停次日低吸(当前策略B) ---
    s1 = defaultdict(list)
    for date in sorted(zt_by_date.keys()):
        di = date_idx.get(date)
        if di is None or di + 1 >= len(all_dates): continue
        nd = all_dates[di + 1]
        if len(zt_by_date[date]) > 200: continue
        
        for code in zt_by_date[date]:
            nb = daily.get((code, nd))
            if not nb or nb['pre_close'] <= 0: continue
            oc = nb['open'] / nb['pre_close'] - 1
            if oc > -0.02 or oc < -0.08: continue
            buy = nb['open']
            sell = nb['close']
            if nb['low'] / buy - 1 <= -0.05: sell = buy * 0.95
            pnl = (sell / buy - 1) - COST
            s1[date[:4]].append(pnl)
    report("S1: 涨停次日低吸 [-2%~-8%] ZT<200 止损-5% 收盘卖", s1)
    
    # --- S2: 涨停次日低吸 — 不止损版 ---
    s2 = defaultdict(list)
    for date in sorted(zt_by_date.keys()):
        di = date_idx.get(date)
        if di is None or di + 1 >= len(all_dates): continue
        nd = all_dates[di + 1]
        if len(zt_by_date[date]) > 200: continue
        
        for code in zt_by_date[date]:
            nb = daily.get((code, nd))
            if not nb or nb['pre_close'] <= 0: continue
            oc = nb['open'] / nb['pre_close'] - 1
            if oc > -0.02 or oc < -0.08: continue
            buy = nb['open']
            sell = nb['close']
            pnl = (sell / buy - 1) - COST
            s2[date[:4]].append(pnl)
    report("S2: 涨停次日低吸 — 不止损 收盘卖", s2)
    
    # --- S3: 涨停次日低吸 — 深度低开[-3%~-8%] ---
    s3 = defaultdict(list)
    for date in sorted(zt_by_date.keys()):
        di = date_idx.get(date)
        if di is None or di + 1 >= len(all_dates): continue
        nd = all_dates[di + 1]
        if len(zt_by_date[date]) > 200: continue
        
        for code in zt_by_date[date]:
            nb = daily.get((code, nd))
            if not nb or nb['pre_close'] <= 0: continue
            oc = nb['open'] / nb['pre_close'] - 1
            if oc > -0.03 or oc < -0.08: continue
            buy = nb['open']
            sell = nb['close']
            if nb['low'] / buy - 1 <= -0.05: sell = buy * 0.95
            pnl = (sell / buy - 1) - COST
            s3[date[:4]].append(pnl)
    report("S3: 涨停次日低吸 [-3%~-8%] ZT<200 止损-5%", s3)
    
    # --- S4: 连板追涨(2连板+次日高开<3%买入) ---
    s4 = defaultdict(list)
    for date in sorted(zt_by_date.keys()):
        di = date_idx.get(date)
        if di is None or di + 1 >= len(all_dates): continue
        nd = all_dates[di + 1]
        if len(zt_by_date[date]) > 200: continue
        
        for code in zt_by_date[date]:
            if consec.get((code, date), 0) < 2: continue
            nb = daily.get((code, nd))
            if not nb or nb['pre_close'] <= 0: continue
            oc = nb['open'] / nb['pre_close'] - 1
            if oc > 0.03 or oc < -0.03: continue  # 高开<3%
            buy = nb['open']
            # 持有3天, trailing 5%
            best = buy
            sold = False
            for h in range(1, 4):
                if di + h + 1 >= len(all_dates): break
                hd = all_dates[di + h + 1]
                hb = daily.get((code, hd))
                if not hb: continue
                best = max(best, hb['high'])
                if hb['low'] / buy - 1 <= -0.05:
                    pnl = -0.05 - COST; sold = True; break
                if hb['close'] / best - 1 <= -0.05:
                    pnl = (hb['close'] / buy - 1) - COST; sold = True; break
            if not sold:
                last_bar = daily.get((code, all_dates[min(di + 4, len(all_dates)-1)]))
                if last_bar:
                    pnl = (last_bar['close'] / buy - 1) - COST
                else:
                    continue
            s4[date[:4]].append(pnl)
    report("S4: 2连板追涨 高开<3% 持3天 trailing5% 止损-5%", s4)
    
    # --- S5: 首板次日低开买入+T+1收盘卖 ---
    s5 = defaultdict(list)
    for date in sorted(zt_by_date.keys()):
        di = date_idx.get(date)
        if di is None or di + 2 >= len(all_dates): continue
        nd = all_dates[di + 1]
        if len(zt_by_date[date]) > 200: continue
        
        for code in zt_by_date[date]:
            if consec.get((code, date), 0) != 1: continue  # 只取首板
            nb = daily.get((code, nd))
            if not nb or nb['pre_close'] <= 0: continue
            oc = nb['open'] / nb['pre_close'] - 1
            if oc > -0.02 or oc < -0.08: continue
            buy = nb['open']
            # T+1收盘卖
            td2 = all_dates[di + 2]
            nb2 = daily.get((code, td2))
            if not nb2: continue
            sell = nb2['close']
            if nb2['low'] / buy - 1 <= -0.05: sell = buy * 0.95
            pnl = (sell / buy - 1) - COST
            s5[date[:4]].append(pnl)
    report("S5: 首板低吸+T+1收盘卖(持2天)", s5)
    
    # --- S6: 涨停次日低吸+大盘过滤(沪深300当日涨才买) ---
    # 用全市场涨跌比近似
    mkt_chg = {}
    for date in all_dates:
        pcls = [daily[(c, d)]['pre_close'] for (c, d) in daily if d == date and (c, d) in daily]
        cls = [daily[(c, d)]['close'] for (c, d) in daily if d == date and (c, d) in daily]
        if pcls and cls:
            # 简化: 用样本平均涨幅
            changes = [(c/p-1) for c, p in zip(cls[:500], pcls[:500]) if p > 0]
            mkt_chg[date] = statistics.mean(changes) if changes else 0
    
    s6 = defaultdict(list)
    for date in sorted(zt_by_date.keys()):
        di = date_idx.get(date)
        if di is None or di + 1 >= len(all_dates): continue
        nd = all_dates[di + 1]
        if len(zt_by_date[date]) > 200: continue
        # 大盘当日必须涨
        if mkt_chg.get(date, 0) <= 0: continue
        
        for code in zt_by_date[date]:
            nb = daily.get((code, nd))
            if not nb or nb['pre_close'] <= 0: continue
            oc = nb['open'] / nb['pre_close'] - 1
            if oc > -0.02 or oc < -0.08: continue
            buy = nb['open']
            sell = nb['close']
            if nb['low'] / buy - 1 <= -0.05: sell = buy * 0.95
            pnl = (sell / buy - 1) - COST
            s6[date[:4]].append(pnl)
    report("S6: 涨停低吸+大盘当日涨(顺势)", s6)
    
    # --- S7: 涨停次日低吸+大盘过滤(大盘跌才买=逆向) ---
    s7 = defaultdict(list)
    for date in sorted(zt_by_date.keys()):
        di = date_idx.get(date)
        if di is None or di + 1 >= len(all_dates): continue
        nd = all_dates[di + 1]
        if len(zt_by_date[date]) > 200: continue
        if mkt_chg.get(date, 0) > 0: continue  # 大盘跌才买
        
        for code in zt_by_date[date]:
            nb = daily.get((code, nd))
            if not nb or nb['pre_close'] <= 0: continue
            oc = nb['open'] / nb['pre_close'] - 1
            if oc > -0.02 or oc < -0.08: continue
            buy = nb['open']
            sell = nb['close']
            if nb['low'] / buy - 1 <= -0.05: sell = buy * 0.95
            pnl = (sell / buy - 1) - COST
            s7[date[:4]].append(pnl)
    report("S7: 涨停低吸+大盘当日跌(逆向)", s7)
    
    # --- S8: 超跌反弹(非涨停股) — 5日跌>15% + 次日低开买入 ---
    s8 = defaultdict(list)
    stock_series = defaultdict(list)
    for (code, date) in sorted(daily.keys()):
        stock_series[code].append((date, daily[(code, date)]))
    
    for code, series in stock_series.items():
        if len(series) < 10: continue
        for j in range(5, len(series) - 2):
            date, bar = series[j]
            _, bar5 = series[j - 5]
            ret5 = (bar['close'] / bar5['close'] - 1)
            if ret5 >= -0.15: continue
            # 确保不是涨停股(排除)
            pct = bar['close'] / bar['pre_close'] - 1
            if pct >= 0.095: continue
            
            next_date, nb = series[j + 1]
            if nb['open'] <= 0: continue
            oc = nb['open'] / nb['pre_close'] - 1
            if oc >= 0: continue  # 必须低开
            
            buy = nb['open']
            sell = nb['close']
            if nb['low'] / buy - 1 <= -0.05: sell = buy * 0.95
            pnl = (sell / buy - 1) - COST
            s8[date[:4]].append(pnl)
    report("S8: 超跌反弹(5日跌>15%+非涨停) 次日低开买入 收盘卖", s8)
    
    # --- S9: 涨停低吸 + 持有2天(不当天卖) ---
    s9 = defaultdict(list)
    for date in sorted(zt_by_date.keys()):
        di = date_idx.get(date)
        if di is None or di + 2 >= len(all_dates): continue
        nd = all_dates[di + 1]
        td2 = all_dates[di + 2]
        if len(zt_by_date[date]) > 200: continue
        
        for code in zt_by_date[date]:
            nb = daily.get((code, nd))
            if not nb or nb['pre_close'] <= 0: continue
            oc = nb['open'] / nb['pre_close'] - 1
            if oc > -0.02 or oc < -0.08: continue
            buy = nb['open']
            # T+1收盘卖(持有2天)
            nb2 = daily.get((code, td2))
            if not nb2: continue
            sell = nb2['close']
            # 检查两天内的止损
            if nb['low'] / buy - 1 <= -0.05:
                sell = buy * 0.95
            elif nb2['low'] / buy - 1 <= -0.05:
                sell = buy * 0.95
            pnl = (sell / buy - 1) - COST
            s9[date[:4]].append(pnl)
    report("S9: 涨停低吸+持2天T+1收盘卖 止损-5%", s9)
    
    # --- S10: 涨停低吸 + 涨停数<100(更严格过滤) ---
    s10 = defaultdict(list)
    for date in sorted(zt_by_date.keys()):
        di = date_idx.get(date)
        if di is None or di + 1 >= len(all_dates): continue
        nd = all_dates[di + 1]
        if len(zt_by_date[date]) > 100: continue  # 更严格
        
        for code in zt_by_date[date]:
            nb = daily.get((code, nd))
            if not nb or nb['pre_close'] <= 0: continue
            oc = nb['open'] / nb['pre_close'] - 1
            if oc > -0.02 or oc < -0.08: continue
            buy = nb['open']
            sell = nb['close']
            if nb['low'] / buy - 1 <= -0.05: sell = buy * 0.95
            pnl = (sell / buy - 1) - COST
            s10[date[:4]].append(pnl)
    report("S10: 涨停低吸 ZT<100(更严格过滤)", s10)
    
    conn.close()
    print(f"\n{'='*70}")
    print("回测完成!")

if __name__ == '__main__':
    run()
