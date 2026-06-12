"""
策略B(缩量回踩)回测 — 验证:
1. 当前逻辑(三信号共振→次日开盘买) + 缩量基准过滤
2. 方案D(缩量+回踩→次日开盘买, 去掉阳线)

对比两组的PF/胜率/均收益, 决定是否改买点逻辑
"""
import sqlite3
import statistics
from collections import defaultdict
from datetime import datetime

DB_PATH = 'data/alpha_miner.db'
COST = 0.005  # 双向手续费
MAX_HOLD = 4  # 最大持仓天数
STOP_LOSS = -0.08  # 止损-8%
TRAILING = 0.03  # 移动止盈3%

def run_backtest(mode='original', min_zt_vol=500000):
    """
    mode:
      'original' = 三信号共振(缩量+回踩+阳穿MA5) → 次日开盘买
      'no_yang'  = 二信号(缩量+回踩, 不要阳线) → 次日开盘买
    """
    conn = sqlite3.connect(DB_PATH)
    
    # 交易日历
    all_dates = [r[0] for r in conn.execute(
        "SELECT DISTINCT trade_date FROM daily_price ORDER BY trade_date"
    ).fetchall()]
    date_idx = {d: i for i, d in enumerate(all_dates)}
    
    # 首板涨停日
    zt_by_date = defaultdict(list)
    for row in conn.execute("""
        SELECT z.stock_code, z.trade_date, d.volume
        FROM zt_pool z
        JOIN daily_price d ON z.stock_code = d.stock_code AND z.trade_date = d.trade_date
        WHERE z.consecutive_zt <= 1
        AND d.volume > 0 AND d.close > 0
        AND z.trade_date >= '2024-01-01'
    """):
        if row[2] >= min_zt_vol:  # 缩量基准过滤
            zt_by_date[row[1]].append((row[0], row[2]))
    
    print(f"[{mode}] 缩量基准>={min_zt_vol}: {sum(len(v) for v in zt_by_date.values())}只首板")
    
    trades = []
    
    for zt_date, stocks in zt_by_date.items():
        zt_i = date_idx.get(zt_date)
        if zt_i is None:
            continue
        
        for code, zt_vol in stocks:
            zt_row = conn.execute(
                "SELECT close, low FROM daily_price WHERE stock_code=? AND trade_date=?",
                (code, zt_date)
            ).fetchone()
            if not zt_row:
                continue
            zt_close, zt_low = zt_row
            
            # 涨停后15天行情
            future = conn.execute("""
                SELECT trade_date, open, close, high, low, volume, pre_close, amount
                FROM daily_price WHERE stock_code=? AND trade_date > ?
                ORDER BY trade_date LIMIT 15
            """, (code, zt_date)).fetchall()
            
            if len(future) < 3:
                continue
            
            found_shrink = False
            shrink_ratio = 0
            
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
                
                if found_shrink:
                    touch_support = abs(f_low / zt_close - 1) < 0.05
                    
                    if mode == 'original':
                        # 三信号: 缩量 + 回踩 + 阳穿MA5
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
                        signal_ok = touch_support and is_yang and above_ma5
                    else:
                        # 方案D: 只要缩量 + 回踩
                        signal_ok = touch_support
                    
                    if signal_ok:
                        # 次日开盘买入
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
                        high_water = buy_price
                        result = None
                        
                        # 持仓MAX_HOLD天
                        for hold in range(1, MAX_HOLD + 1):
                            if sig_i + 1 + hold >= len(all_dates):
                                break
                            hold_date = all_dates[sig_i + 1 + hold]
                            hr = conn.execute(
                                "SELECT open, close, high, low FROM daily_price WHERE stock_code=? AND trade_date=?",
                                (code, hold_date)
                            ).fetchone()
                            if not hr:
                                continue
                            h_open, h_close, h_high, h_low, = hr
                            high_water = max(high_water, h_high)
                            
                            pnl = h_close / buy_price - 1 - COST * 2
                            pnl_from_high = h_close / high_water - 1
                            
                            # 止损
                            if pnl <= STOP_LOSS:
                                result = pnl
                                break
                            # 时间到
                            if hold >= MAX_HOLD:
                                result = pnl
                                break
                            # 移动止盈
                            if hold >= 2 and pnl_from_high <= -TRAILING:
                                result = pnl
                                break
                        
                        if result is not None:
                            trades.append(result)
                        break  # 只取第一个信号
    
    conn.close()
    return trades


def report(trades, label):
    if not trades:
        print(f"\n{label}: 无交易")
        return
    
    wins = [t for t in trades if t > 0]
    losses = [t for t in trades if t <= 0]
    total_pnl = sum(trades)
    win_rate = len(wins) / len(trades) * 100
    avg_win = statistics.mean(wins) if wins else 0
    avg_loss = abs(statistics.mean(losses)) if losses else 0.001
    pf = (sum(wins) / (sum(abs(l) for l in losses))) if losses else float('inf')
    avg_pnl = statistics.mean(trades) * 100
    
    print(f"\n{'='*50}")
    print(f"{label}")
    print(f"{'='*50}")
    print(f"  交易笔数: {len(trades)}")
    print(f"  胜率: {win_rate:.1f}%")
    print(f"  PF: {pf:.2f}")
    print(f"  平均收益: {avg_pnl:.2f}%")
    print(f"  总收益: {total_pnl*100:.1f}%")
    print(f"  均赢/均亏: {avg_win*100:.2f}% / {avg_loss*100:.2f}%")
    
    # 收益分布
    pcts = [t * 100 for t in trades]
    pcts.sort()
    n = len(pcts)
    print(f"  收益分布: P10={pcts[int(n*0.1)]:.1f}% P25={pcts[int(n*0.25)]:.1f}% P50={pcts[int(n*0.5)]:.1f}% P75={pcts[int(n*0.75)]:.1f}% P90={pcts[int(n*0.9)]:.1f}%")


if __name__ == '__main__':
    print("策略B回测对比")
    print("周期: 2024-01至今")
    print()
    
    # 原始三信号
    trades_orig = run_backtest(mode='original', min_zt_vol=500000)
    report(trades_orig, "当前逻辑(三信号+缩量基准过滤)")
    
    # 方案D: 去掉阳线
    trades_no_yang = run_backtest(mode='no_yang', min_zt_vol=500000)
    report(trades_no_yang, "方案D(缩量+回踩, 不等阳线)")
    
    # 对比
    if trades_orig and trades_no_yang:
        print(f"\n{'='*50}")
        print("对比:")
        pf_o = sum(t for t in trades_orig if t > 0) / max(sum(abs(t) for t in trades_orig if t <= 0), 0.001)
        pf_d = sum(t for t in trades_no_yang if t > 0) / max(sum(abs(t) for t in trades_no_yang if t <= 0), 0.001)
        wr_o = len([t for t in trades_orig if t > 0]) / len(trades_orig) * 100
        wr_d = len([t for t in trades_no_yang if t > 0]) / len(trades_no_yang) * 100
        print(f"  笔数: {len(trades_orig)} → {len(trades_no_yang)} ({len(trades_no_yang)-len(trades_orig):+d})")
        print(f"  PF:   {pf_o:.2f} → {pf_d:.2f} ({pf_d-pf_o:+.2f})")
        print(f"  胜率: {wr_o:.1f}% → {wr_d:.1f}% ({wr_d-wr_o:+.1f}%)")
