"""
策略B深度诊断 — 拆解每个环节的盈亏来源
目标: 找出PF=0.72的根因，定向修复
"""
import sqlite3
import statistics
from collections import defaultdict
from datetime import datetime

DB_PATH = 'data/alpha_miner.db'
COST = 0.005

def get_all_signals():
    """采集所有缩量回踩信号，带完整元数据"""
    conn = sqlite3.connect(DB_PATH)
    all_dates = [r[0] for r in conn.execute(
        "SELECT DISTINCT trade_date FROM daily_price ORDER BY trade_date"
    ).fetchall()]
    date_idx = {d: i for i, d in enumerate(all_dates)}

    zt_by_date = defaultdict(list)
    for row in conn.execute("""
        SELECT z.stock_code, z.trade_date, d.volume, d.close, d.low, d.amount as dp_amount
        FROM zt_pool z
        JOIN daily_price d ON z.stock_code = d.stock_code AND z.trade_date = d.trade_date
        WHERE z.consecutive_zt <= 1 AND d.volume >= 500000 AND d.close > 0
        AND z.trade_date >= '2024-01-01'
    """):
        zt_by_date[row[1]].append(row)

    signals = []
    
    for zt_date, stocks in zt_by_date.items():
        zt_i = date_idx.get(zt_date)
        if zt_i is None:
            continue
        for code, td, zt_vol, zt_close, zt_low, zt_dp_amount in stocks:
            future = conn.execute("""
                SELECT trade_date, open, close, high, low, volume, pre_close, amount
                FROM daily_price WHERE stock_code=? AND trade_date > ?
                ORDER BY trade_date LIMIT 15
            """, (code, zt_date)).fetchall()
            if len(future) < 3:
                continue

            found_shrink = False
            shrink_ratio = 0
            shrink_date = None
            
            for row in future:
                f_date, f_open, f_close, f_high, f_low, f_vol, f_pc, f_amt = row
                if not all([f_vol, f_open, f_close, f_pc]):
                    continue
                if not f_amt or f_amt < 30000000:
                    continue
                if f_low < zt_low * 0.95:
                    break

                vol_ratio = f_vol / zt_vol
                if not found_shrink and 0.05 <= vol_ratio <= 0.30:
                    found_shrink = True
                    shrink_ratio = vol_ratio
                    shrink_date = f_date

                if found_shrink:
                    touch_support = abs(f_low / zt_close - 1) < 0.05
                    is_yang = f_close > f_open
                    f_i = date_idx.get(f_date)
                    if f_i is None or f_i < 4:
                        continue
                    ma5_dates = all_dates[f_i-4:f_i+1]
                    cs = conn.execute(
                        f"SELECT close FROM daily_price WHERE stock_code=? AND trade_date IN ({','.join(['?']*5)}) AND close > 0",
                        (code, *ma5_dates)
                    ).fetchall()
                    if len(cs) < 5:
                        continue
                    ma5 = sum(r[0] for r in cs) / 5
                    above_ma5 = f_close > ma5
                    
                    if touch_support and is_yang and above_ma5:
                        # 三信号共振! 收集元数据
                        sig_i = date_idx.get(f_date)
                        if sig_i is None or sig_i + 1 >= len(all_dates):
                            break
                        buy_date = all_dates[sig_i + 1]
                        buy_row = conn.execute(
                            "SELECT open FROM daily_price WHERE stock_code=? AND trade_date=?",
                            (code, buy_date)
                        ).fetchone()
                        if not buy_row or buy_row[0] <= 0:
                            break
                        buy_price = buy_row[0]
                        
                        # 信号日特征
                        yang_chg = (f_close / f_pc - 1) * 100  # 阳线涨幅
                        days_from_zt = sig_i - zt_i  # 涨停到信号的天数
                        
                        # 后续5天行情(完整持仓周期)
                        hold_data = []
                        for h in range(1, 6):
                            if sig_i + 1 + h >= len(all_dates):
                                break
                            h_date = all_dates[sig_i + 1 + h]
                            hr = conn.execute(
                                "SELECT open, close, high, low FROM daily_price WHERE stock_code=? AND trade_date=?",
                                (code, h_date)
                            ).fetchone()
                            if hr:
                                hold_data.append({
                                    'date': h_date,
                                    'open': hr[0], 'close': hr[1],
                                    'high': hr[2], 'low': hr[3]
                                })
                        
                        signals.append({
                            'code': code,
                            'zt_date': zt_date,
                            'signal_date': f_date,
                            'buy_date': buy_date,
                            'buy_price': buy_price,
                            'shrink_pct': shrink_ratio * 100,
                            'yang_chg': yang_chg,
                            'days_from_zt': days_from_zt,
                            'support_dist': (f_low / zt_close - 1) * 100,
                            'hold_data': hold_data,
                            'zt_amount': zt_dp_amount,
                        })
                        break

    conn.close()
    return signals


def calc_trade_pnl(sig, hold_days, stop_loss_pct, trailing_pct, time_stop_days=0):
    """计算单笔交易的PnL"""
    bp = sig['buy_price']
    hw = bp
    for i, h in enumerate(sig['hold_data']):
        day = i + 1
        if h['high'] > 0:
            hw = max(hw, h['high'])
        if h['close'] <= 0:
            continue
        pnl = h['close'] / bp - 1 - COST * 2
        pnl_from_high = h['close'] / hw - 1
        # 止损
        if pnl <= stop_loss_pct:
            return pnl
        # 时间到
        if day >= hold_days:
            return pnl
        # 时间止损
        if time_stop_days > 0 and day >= time_stop_days and pnl < 0:
            return pnl
        # 移动止盈
        if day >= 2 and pnl_from_high <= -trailing_pct:
            return pnl
    # 没有足够数据，返回最后一天
    if sig['hold_data']:
        last = sig['hold_data'][-1]
        if last['close'] > 0:
            return last['close'] / bp - 1 - COST * 2
    return None


def report_trades(trades, label):
    if not trades:
        print(f"\n{label}: 无交易")
        return 0, 0, 0
    wins = [t for t in trades if t > 0]
    losses = [t for t in trades if t <= 0]
    total_loss = sum(abs(l) for l in losses)
    pf = sum(wins) / total_loss if total_loss > 0 else float('inf')
    wr = len(wins) / len(trades) * 100
    avg = statistics.mean(trades) * 100
    print(f"\n  {label}: {len(trades)}笔 PF={pf:.2f} 胜率={wr:.1f}% 均收益={avg:+.2f}%")
    return pf, wr, avg


def diagnose():
    print("采集信号...")
    signals = get_all_signals()
    print(f"共采集到 {len(signals)} 个三信号共振")
    
    # ========== 诊断1: 信号原始预测力 ==========
    # 不做任何止损止盈，就是买完持N天看收益
    print("\n" + "="*60)
    print("诊断1: 信号原始预测力(买入后持有N天的平均收益)")
    print("="*60)
    for hold_n in [1, 2, 3, 4, 5]:
        pnls = []
        for sig in signals:
            if len(sig['hold_data']) >= hold_n:
                h = sig['hold_data'][hold_n - 1]
                if h['close'] > 0:
                    pnls.append(h['close'] / sig['buy_price'] - 1 - COST * 2)
        if pnls:
            avg = statistics.mean(pnls) * 100
            wr = len([p for p in pnls if p > 0]) / len(pnls) * 100
            wins = [p for p in pnls if p > 0]
            losses = [p for p in pnls if p <= 0]
            pf = sum(wins) / max(sum(abs(l) for l in losses), 0.001)
            print(f"  持{hold_n}天: {len(pnls)}笔 PF={pf:.2f} 胜率={wr:.1f}% 均收益={avg:+.2f}%")

    # ========== 诊断2: 买点问题 ==========
    print("\n" + "="*60)
    print("诊断2: 信号日阳线涨幅 vs 后续收益")
    print("="*60)
    # 按信号日涨幅分组
    bins = [(0, 2, '0-2%(小阳)'), (2, 5, '2-5%(中阳)'), (5, 10, '5-10%(大阳)'), (10, 99, '10%+(涨停)')]
    for lo, hi, label in bins:
        group = [s for s in signals if lo <= s['yang_chg'] < hi]
        if len(group) < 10:
            continue
        # 持3天收益
        pnls = []
        for sig in group:
            p = calc_trade_pnl(sig, hold_days=3, stop_loss_pct=-0.08, trailing_pct=0.03)
            if p is not None:
                pnls.append(p)
        pf, wr, avg = report_trades(pnls, f"阳线{label}")
    
    # ========== 诊断3: 缩量深度 ==========
    print("\n" + "="*60)
    print("诊断3: 缩量深度 vs 后续收益")
    print("="*60)
    shrink_bins = [(5, 10, '5-10%(极缩)'), (10, 20, '10-20%(深缩)'), (20, 25, '20-25%(中缩)'), (25, 30, '25-30%(浅缩)')]
    for lo, hi, label in shrink_bins:
        group = [s for s in signals if lo <= s['shrink_pct'] < hi]
        if len(group) < 10:
            continue
        pnls = [calc_trade_pnl(s, 4, -0.08, 0.03) for s in group]
        pnls = [p for p in pnls if p is not None]
        pf, wr, avg = report_trades(pnls, f"缩量{label}")

    # ========== 诊断4: 涨停到信号的天数 ==========
    print("\n" + "="*60)
    print("诊断4: 涨停→信号间隔天数 vs 后续收益")
    print("="*60)
    day_bins = [(2, 4, '2-3天(快)'), (4, 7, '4-6天(中)'), (7, 11, '7-10天(慢)'), (11, 99, '11天+(很慢)')]
    for lo, hi, label in day_bins:
        group = [s for s in signals if lo <= s['days_from_zt'] < hi]
        if len(group) < 10:
            continue
        pnls = [calc_trade_pnl(s, 4, -0.08, 0.03) for s in group]
        pnls = [p for p in pnls if p is not None]
        pf, wr, avg = report_trades(pnls, f"间隔{label}")

    # ========== 诊断5: 涨停日成交额 ==========
    print("\n" + "="*60)
    print("诊断5: 涨停日成交额(市值代理) vs 后续收益")
    print("="*60)
    amt_bins = [
        (0, 2e8, '<2亿(小盘)'), 
        (2e8, 5e8, '2-5亿(中小)'), 
        (5e8, 10e8, '5-10亿(中)'), 
        (10e8, 999e8, '10亿+(大盘)')
    ]
    for lo, hi, label in amt_bins:
        group = [s for s in signals if lo <= s['zt_amount'] < hi]
        if len(group) < 10:
            continue
        pnls = [calc_trade_pnl(s, 4, -0.08, 0.03) for s in group]
        pnls = [p for p in pnls if p is not None]
        pf, wr, avg = report_trades(pnls, f"{label}")

    # ========== 诊断6: 去掉阳线条件(方案D) vs 保留 ==========
    print("\n" + "="*60)
    print("诊断6: 去掉阳线条件(买入缩量回踩当天的次日)")
    print("="*60)
    # 重新跑: 缩量+回踩就出信号(不要阳线不要MA5)
    conn = sqlite3.connect(DB_PATH)
    all_dates = [r[0] for r in conn.execute("SELECT DISTINCT trade_date FROM daily_price ORDER BY trade_date").fetchall()]
    date_idx = {d: i for i, d in enumerate(all_dates)}
    
    zt_by_date = defaultdict(list)
    for row in conn.execute("""
        SELECT z.stock_code, z.trade_date, d.volume, d.close, d.low
        FROM zt_pool z
        JOIN daily_price d ON z.stock_code = d.stock_code AND z.trade_date = d.trade_date
        WHERE z.consecutive_zt <= 1 AND d.volume >= 500000 AND d.close > 0
        AND z.trade_date >= '2024-01-01'
    """):
        zt_by_date[row[1]].append(row)

    no_yang_trades = []
    for zt_date, stocks in zt_by_date.items():
        zt_i = date_idx.get(zt_date)
        if zt_i is None: continue
        for code, td, zt_vol, zt_close, zt_low in stocks:
            future = conn.execute("""
                SELECT trade_date, open, close, high, low, volume, pre_close, amount
                FROM daily_price WHERE stock_code=? AND trade_date > ?
                ORDER BY trade_date LIMIT 15
            """, (code, zt_date)).fetchall()
            if len(future) < 3: continue
            found_shrink = False
            shrink_ratio = 0
            for row in future:
                f_date, f_open, f_close, f_high, f_low, f_vol, f_pc, f_amt = row
                if not all([f_vol, f_open, f_close, f_pc]): continue
                if not f_amt or f_amt < 30000000: continue
                if f_low < zt_low * 0.95: break
                vol_ratio = f_vol / zt_vol
                if not found_shrink and 0.05 <= vol_ratio <= 0.30:
                    found_shrink = True
                    shrink_ratio = vol_ratio
                if found_shrink:
                    touch_support = abs(f_low / zt_close - 1) < 0.05
                    if touch_support:
                        sig_i = date_idx.get(f_date)
                        if sig_i is None or sig_i + 1 >= len(all_dates): break
                        buy_date = all_dates[sig_i + 1]
                        buy_row = conn.execute("SELECT open FROM daily_price WHERE stock_code=? AND trade_date=?", (code, buy_date)).fetchone()
                        if not buy_row or buy_row[0] <= 0: break
                        bp = buy_row[0]
                        hw = bp
                        result = None
                        for hold in range(1, 5):
                            if sig_i + 1 + hold >= len(all_dates): break
                            h_date = all_dates[sig_i + 1 + hold]
                            hr = conn.execute("SELECT open, close, high FROM daily_price WHERE stock_code=? AND trade_date=?", (code, h_date)).fetchone()
                            if not hr: continue
                            h_o, h_c, h_h = hr
                            hw = max(hw, h_h)
                            pnl = h_c / bp - 1 - COST * 2
                            if pnl <= -0.08: result = pnl; break
                            if hold >= 4: result = pnl; break
                            if hold >= 2 and h_c / hw - 1 <= -0.03: result = pnl; break
                        if result is not None:
                            no_yang_trades.append(result)
                        break
    conn.close()
    report_trades(no_yang_trades, "方案D(缩量+回踩, 不等阳线)")

    # ========== 诊断7: 最优止损止盈参数扫描 ==========
    print("\n" + "="*60)
    print("诊断7: 止损止盈参数扫描")
    print("="*60)
    for sl in [-0.03, -0.05, -0.08, -0.10]:
        for tp in [0.02, 0.03, 0.05, 0.08]:
            pnls = [calc_trade_pnl(s, 4, sl, tp) for s in signals]
            pnls = [p for p in pnls if p is not None]
            if len(pnls) < 50:
                continue
            wins = [t for t in pnls if t > 0]
            losses = [t for t in pnls if t <= 0]
            total_loss = sum(abs(l) for l in losses)
            pf = sum(wins) / total_loss if total_loss > 0 else 999
            if pf >= 1.0:  # 只打印正期望的
                wr = len(wins) / len(pnls) * 100
                avg = statistics.mean(pnls) * 100
                print(f"  止损{sl*100:.0f}% 止盈{tp*100:.0f}%: {len(pnls)}笔 PF={pf:.2f} 胜率={wr:.1f}% 均收益={avg:+.2f}%")

    # ========== 诊断8: 持仓天数扫描 ==========
    print("\n" + "="*60)
    print("诊断8: 最优持仓天数(止损-5%, trailing 3%)")
    print("="*60)
    for hold in [1, 2, 3, 4, 5]:
        pnls = [calc_trade_pnl(s, hold, -0.05, 0.03) for s in signals]
        pnls = [p for p in pnls if p is not None]
        if pnls:
            wins = [t for t in pnls if t > 0]
            losses = [t for t in pnls if t <= 0]
            total_loss = sum(abs(l) for l in losses)
            pf = sum(wins) / total_loss if total_loss > 0 else 999
            wr = len(wins) / len(pnls) * 100
            avg = statistics.mean(pnls) * 100
            print(f"  持{hold}天: {len(pnls)}笔 PF={pf:.2f} 胜率={wr:.1f}% 均收益={avg:+.2f}%")


if __name__ == '__main__':
    diagnose()
