"""
策略B v2回测 — 基于诊断+调研的新方案

核心改动:
1. 买点: 缩量回踩→次日开盘买 → 改为: 缩量回踩确认后, 等放量突破当天盘中买(Buy Stop逻辑)
2. 过滤: 小盘(<2亿成交额)排除, 深缩(<20%)排除
3. 信号: 不要求阳线+MA5, 改为缩量+回踩+放量突破

方案对比:
  baseline = 原始三信号(缩量+回踩+阳穿MA5→次日开盘买)
  v2_buy_stop = 缩量+回踩确认后, 等突破缩量区间高点才买
  v2_next_day = 缩量+回踩确认后, 次日开盘买(不去阳线, 不追高)
"""
import sqlite3
import statistics
from collections import defaultdict

DB_PATH = 'data/alpha_miner.db'
COST = 0.005


def run_backtest_v2_buy_stop(min_zt_vol=500000, min_zt_amount=2e8,
                              shrink_lo=20, shrink_hi=35,
                              max_days_wait=10, 
                              breakout_min_vol_ratio=1.5,
                              hold_days=4, stop_loss=-0.05, trailing=0.03):
    """
    VCP Buy Stop逻辑:
    1. 首板涨停(量>=50万手, 额>=2亿)
    2. 涨停后缩量回踩(量比20-35%, 最低价回到涨停收盘±5%)
    3. 缩量确认后, 记录缩量期间的高点(pivot)
    4. 等后续某天放量突破pivot(量>缩量日均量*1.5) → 当天收盘价买入
    5. 持N天, 止损/trailing
    """
    conn = sqlite3.connect(DB_PATH)
    all_dates = [r[0] for r in conn.execute(
        "SELECT DISTINCT trade_date FROM daily_price ORDER BY trade_date"
    ).fetchall()]
    date_idx = {d: i for i, d in enumerate(all_dates)}

    zt_by_date = defaultdict(list)
    for row in conn.execute("""
        SELECT z.stock_code, z.trade_date, d.volume, d.close, d.low, d.amount
        FROM zt_pool z
        JOIN daily_price d ON z.stock_code = d.stock_code AND z.trade_date = d.trade_date
        WHERE z.consecutive_zt <= 1 AND d.volume >= ? AND d.close > 0
        AND d.amount >= ?
        AND z.trade_date >= '2024-01-01'
    """, (min_zt_vol, min_zt_amount)):
        zt_by_date[row[1]].append(row)

    trades = []

    for zt_date, stocks in zt_by_date.items():
        zt_i = date_idx.get(zt_date)
        if zt_i is None:
            continue

        for code, td, zt_vol, zt_close, zt_low, zt_amount in stocks:
            future = conn.execute("""
                SELECT trade_date, open, close, high, low, volume, pre_close, amount
                FROM daily_price WHERE stock_code=? AND trade_date > ?
                ORDER BY trade_date LIMIT 20
            """, (code, zt_date)).fetchall()
            if len(future) < 5:
                continue

            # Phase 1: 找缩量回踩区间
            shrink_found = False
            shrink_high = 0  # 缩量期间的最高点(pivot)
            shrink_avg_vol = 0
            shrink_days_list = []

            for row in future:
                f_date, f_open, f_close, f_high, f_low, f_vol, f_pc, f_amt = row
                if not all([f_vol, f_open, f_close, f_pc]):
                    continue
                if f_low < zt_low * 0.95:
                    break

                vol_ratio = f_vol / zt_vol * 100  # 百分比

                if not shrink_found and shrink_lo <= vol_ratio <= shrink_hi:
                    touch_support = abs(f_low / zt_close - 1) < 0.05
                    if touch_support:
                        shrink_found = True

                if shrink_found:
                    shrink_days_list.append(row)
                    shrink_high = max(shrink_high, f_high)
                    shrink_avg_vol = sum(r[5] for r in shrink_days_list) / len(shrink_days_list)

                    # 缩量区间最多看max_days_wait天
                    if len(shrink_days_list) >= max_days_wait:
                        break

            if not shrink_found or len(shrink_days_list) < 2:
                continue

            # Phase 2: 等放量突破pivot
            last_shrink_date = shrink_days_list[-1][0]
            last_shrink_i = date_idx.get(last_shrink_date)
            if last_shrink_i is None:
                continue

            # 从缩量区间结束后看后续行情
            post_shrink = conn.execute("""
                SELECT trade_date, open, close, high, low, volume, pre_close, amount
                FROM daily_price WHERE stock_code=? AND trade_date > ?
                ORDER BY trade_date LIMIT 10
            """, (code, last_shrink_date)).fetchall()

            bought = False
            for row in post_shrink:
                f_date, f_open, f_close, f_high, f_low, f_vol, f_pc, f_amt = row
                if not all([f_vol, f_open, f_close, f_pc]):
                    continue
                if not f_amt or f_amt < 30000000:
                    continue

                # 放量突破: 量>缩量日均量*1.5 且 收盘>pivot
                vol_breakout = f_vol > shrink_avg_vol * breakout_min_vol_ratio
                price_breakout = f_close > shrink_high

                if vol_breakout and price_breakout:
                    # 突破日收盘价买入(模拟Buy Stop)
                    buy_price = f_close
                    buy_i = date_idx.get(f_date)
                    if buy_i is None or buy_i + 1 >= len(all_dates):
                        break

                    # 持仓
                    hw = buy_price
                    result = None
                    for hold in range(1, hold_days + 1):
                        if buy_i + hold >= len(all_dates):
                            break
                        h_date = all_dates[buy_i + hold]
                        hr = conn.execute(
                            "SELECT close, high, low FROM daily_price WHERE stock_code=? AND trade_date=?",
                            (code, h_date)
                        ).fetchone()
                        if not hr:
                            continue
                        h_c, h_h, h_l = hr
                        if h_c <= 0:
                            continue
                        hw = max(hw, h_h)
                        pnl = h_c / buy_price - 1 - COST * 2
                        pnl_from_hw = h_c / hw - 1

                        if pnl <= stop_loss:
                            result = pnl
                            break
                        if hold >= hold_days:
                            result = pnl
                            break
                        if hold >= 2 and pnl_from_hw <= -trailing:
                            result = pnl
                            break

                    if result is not None:
                        trades.append(result)
                    bought = True
                    break

                # 如果已经远离了(跌了太多), 不等了
                if f_close < shrink_high * 0.9:
                    break

            if bought:
                continue

    conn.close()
    return trades


def run_backtest_v2_next_day(min_zt_vol=500000, min_zt_amount=2e8,
                              shrink_lo=20, shrink_hi=35,
                              hold_days=4, stop_loss=-0.05, trailing=0.03):
    """
    方案: 缩量+回踩确认后 → 次日开盘买
    不要阳线确认, 不要MA5, 只要缩量回踩就进
    加上小盘过滤+深缩过滤
    """
    conn = sqlite3.connect(DB_PATH)
    all_dates = [r[0] for r in conn.execute(
        "SELECT DISTINCT trade_date FROM daily_price ORDER BY trade_date"
    ).fetchall()]
    date_idx = {d: i for i, d in enumerate(all_dates)}

    zt_by_date = defaultdict(list)
    for row in conn.execute("""
        SELECT z.stock_code, z.trade_date, d.volume, d.close, d.low, d.amount
        FROM zt_pool z
        JOIN daily_price d ON z.stock_code = d.stock_code AND z.trade_date = d.trade_date
        WHERE z.consecutive_zt <= 1 AND d.volume >= ? AND d.close > 0
        AND d.amount >= ?
        AND z.trade_date >= '2024-01-01'
    """, (min_zt_vol, min_zt_amount)):
        zt_by_date[row[1]].append(row)

    trades = []

    for zt_date, stocks in zt_by_date.items():
        zt_i = date_idx.get(zt_date)
        if zt_i is None:
            continue
        for code, td, zt_vol, zt_close, zt_low, zt_amount in stocks:
            future = conn.execute("""
                SELECT trade_date, open, close, high, low, volume, pre_close, amount
                FROM daily_price WHERE stock_code=? AND trade_date > ?
                ORDER BY trade_date LIMIT 15
            """, (code, zt_date)).fetchall()
            if len(future) < 3:
                continue

            found_shrink = False
            for row in future:
                f_date, f_open, f_close, f_high, f_low, f_vol, f_pc, f_amt = row
                if not all([f_vol, f_open, f_close, f_pc]):
                    continue
                if not f_amt or f_amt < 30000000:
                    continue
                if f_low < zt_low * 0.95:
                    break

                vol_ratio = f_vol / zt_vol * 100
                if not found_shrink and shrink_lo <= vol_ratio <= shrink_hi:
                    touch_support = abs(f_low / zt_close - 1) < 0.05
                    if touch_support:
                        found_shrink = True
                        # 缩量回踩确认! 次日开盘买
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
                        bp = buy_row[0]
                        hw = bp
                        result = None
                        for hold in range(1, hold_days + 1):
                            if sig_i + 1 + hold >= len(all_dates):
                                break
                            h_date = all_dates[sig_i + 1 + hold]
                            hr = conn.execute(
                                "SELECT close, high FROM daily_price WHERE stock_code=? AND trade_date=?",
                                (code, h_date)
                            ).fetchone()
                            if not hr:
                                continue
                            h_c, h_h = hr
                            if h_c <= 0:
                                continue
                            hw = max(hw, h_h)
                            pnl = h_c / bp - 1 - COST * 2
                            if pnl <= stop_loss:
                                result = pnl; break
                            if hold >= hold_days:
                                result = pnl; break
                            if hold >= 2 and h_c / hw - 1 <= -trailing:
                                result = pnl; break
                        if result is not None:
                            trades.append(result)
                        break

    conn.close()
    return trades


def report(trades, label):
    if not trades:
        print(f"\n  {label}: 无交易")
        return 0
    wins = [t for t in trades if t > 0]
    losses = [t for t in trades if t <= 0]
    tl = sum(abs(l) for l in losses)
    pf = sum(wins) / tl if tl > 0 else 999
    wr = len(wins) / len(trades) * 100
    avg = statistics.mean(trades) * 100
    print(f"\n  {label}: {len(trades)}笔 PF={pf:.2f} 胜率={wr:.1f}% 均收益={avg:+.2f}%")
    return pf


if __name__ == '__main__':
    print("策略B v2 回测对比")
    print("数据: 2024-01至今\n")

    # ===== 方案1: Buy Stop (VCP逻辑) =====
    print("="*60)
    print("方案1: Buy Stop — 缩量确认后等放量突破pivot")
    print("="*60)
    
    best_pf = 0
    best_params = {}
    
    for sl in [20, 25, 30]:
        for bvol in [1.2, 1.5, 2.0]:
            for sl_pct in [-0.04, -0.05, -0.06]:
                trades = run_backtest_v2_buy_stop(
                    shrink_lo=sl, shrink_hi=sl+10,
                    breakout_min_vol_ratio=bvol,
                    stop_loss=sl_pct,
                    hold_days=4, trailing=0.03
                )
                if len(trades) >= 30:
                    wins = [t for t in trades if t > 0]
                    losses = [t for t in trades if t <= 0]
                    tl = sum(abs(l) for l in losses)
                    pf = sum(wins) / tl if tl > 0 else 999
                    if pf > best_pf:
                        best_pf = pf
                        best_params = {'shrink': f'{sl}-{sl+10}', 'bvol': bvol, 'sl': sl_pct}
                    if pf >= 1.0:
                        wr = len(wins) / len(trades) * 100
                        avg = statistics.mean(trades) * 100
                        print(f"  缩{sl}-{sl+10}% 突破量{bvol}x 止损{sl_pct*100:.0f}%: {len(trades)}笔 PF={pf:.2f} 胜率={wr:.1f}% 均收益={avg:+.2f}%")

    print(f"\n  最佳参数(未达标也列出): PF={best_pf:.2f} {best_params}")
    # 跑最佳参数
    if best_params:
        trades = run_backtest_v2_buy_stop(
            shrink_lo=int(best_params['shrink'].split('-')[0]),
            shrink_hi=int(best_params['shrink'].split('-')[1]),
            breakout_min_vol_ratio=best_params['bvol'],
            stop_loss=best_params['sl'],
            hold_days=4, trailing=0.03
        )
        report(trades, f"Buy Stop最佳")

    # ===== 方案2: 缩量回踩次日开盘买(无阳线, 加过滤) =====
    print("\n" + "="*60)
    print("方案2: 缩量+回踩 → 次日开盘买(加过滤)")
    print("="*60)
    
    for sl in [20, 25, 30]:
        for sl_pct in [-0.04, -0.05, -0.06]:
            trades = run_backtest_v2_next_day(
                shrink_lo=sl, shrink_hi=sl+10,
                stop_loss=sl_pct,
                hold_days=4, trailing=0.03
            )
            if len(trades) >= 30:
                wins = [t for t in trades if t > 0]
                losses = [t for t in trades if t <= 0]
                tl = sum(abs(l) for l in losses)
                pf = sum(wins) / tl if tl > 0 else 999
                if pf >= 0.9:
                    wr = len(wins) / len(trades) * 100
                    avg = statistics.mean(trades) * 100
                    print(f"  缩{sl}-{sl+10}% 止损{sl_pct*100:.0f}%: {len(trades)}笔 PF={pf:.2f} 胜率={wr:.1f}% 均收益={avg:+.2f}%")

    # ===== 方案3: Buy Stop + 不同持仓天数 =====
    print("\n" + "="*60)
    print("方案3: Buy Stop(缩25-35%, 突破1.5x, 止损-5%) 持仓天数扫描")
    print("="*60)
    for hold in [1, 2, 3, 4, 5]:
        trades = run_backtest_v2_buy_stop(
            shrink_lo=25, shrink_hi=35,
            breakout_min_vol_ratio=1.5,
            stop_loss=-0.05,
            hold_days=hold, trailing=0.03
        )
        if trades:
            wins = [t for t in trades if t > 0]
            losses = [t for t in trades if t <= 0]
            tl = sum(abs(l) for l in losses)
            pf = sum(wins) / tl if tl > 0 else 999
            wr = len(wins) / len(trades) * 100
            avg = statistics.mean(trades) * 100
            print(f"  持{hold}天: {len(trades)}笔 PF={pf:.2f} 胜率={wr:.1f}% 均收益={avg:+.2f}%")
