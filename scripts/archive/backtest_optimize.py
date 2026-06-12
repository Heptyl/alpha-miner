"""
参数敏感性分析 — 找B策略的优化方向
测试不同止损/trailing/追高过滤参数组合
"""
import sqlite3
import json
import statistics
from pathlib import Path
from collections import defaultdict
from datetime import datetime

# 复用回测引擎的核心组件
sys_path = str(Path(__file__).resolve().parents[1])
import sys
sys.path.insert(0, sys_path)

# 直接import关键函数
from scripts.backtest_engine import (
    get_conn, load_daily_prices, load_zt_pool, get_trading_dates,
    get_market_phase, is_filtered, Portfolio, Position,
    calc_buy_cost, calc_sell_receive,
    INITIAL_CAPITAL, COMMISSION_RATE, STAMP_DUTY_RATE, SLIPPAGE,
    MIN_COMMISSION, AB_POSITION_RATIO, C_POSITION_RATIO,
    MAX_POSITIONS, MAX_AB_POSITIONS, MAX_C_POSITIONS, MIN_CASH_RATIO,
    STOP_LOSS_PCT, SELL_PARAMS, OPEN_CHG_FILTER,
)

# ═══════════════════════════════════════════════
# 参数扫描回测
# ═══════════════════════════════════════════════
def run_b_backtest(daily, zt_pool, trading_dates,
                   stop_loss=-0.08,
                   trailing_pct=0.05,
                   trailing_ebb=0.03,
                   trailing_frost=0.02,
                   max_hold=5,
                   time_stop_days=3,
                   open_chg_normal=5.0,
                   open_chg_ebb=3.0,
                   buy_filter=None,
                   start_date=None, end_date=None):
    """参数化的策略B回测"""
    
    portfolio = Portfolio(INITIAL_CAPITAL)
    dates = trading_dates[:]
    if start_date:
        dates = [d for d in dates if d >= start_date]
    if end_date:
        dates = [d for d in dates if d <= end_date]
    
    trailing_map = {'正常': trailing_pct, '退潮': trailing_ebb, '冰点': trailing_frost, '高潮': trailing_pct}
    open_chg_map = {'正常': open_chg_normal, '退潮': open_chg_ebb, '冰点': 5.0, '高潮': 8.0}
    
    trades = []
    
    for trade_date in dates:
        phase_info = get_market_phase(daily, zt_pool, trade_date)
        phase = phase_info['phase']
        
        # 卖出检查
        for pos in portfolio.positions[:]:
            if pos.code not in daily or trade_date not in daily[pos.code]:
                continue
            day = daily[pos.code][trade_date]
            pos.hold_days += 1
            if day['high'] > pos.highest_price:
                pos.highest_price = day['high']
            
            if pos.hold_days < 1:
                continue
            
            # 止损
            chg_low = (day['low'] / pos.buy_price - 1)
            if chg_low <= stop_loss:
                sell_price = pos.buy_price * (1 + stop_loss)
                portfolio.sell(pos, sell_price, trade_date, f'止损@{stop_loss*100:.0f}%')
                trades.append(portfolio.closed_trades[-1])
                continue
            
            # 最长持有
            if pos.hold_days >= max_hold:
                portfolio.sell(pos, day['close'], trade_date, f'最长{max_hold}天')
                trades.append(portfolio.closed_trades[-1])
                continue
            
            # Trailing
            tp = trailing_map.get(phase, trailing_pct)
            if pos.highest_price > pos.buy_price:
                dd = (day['low'] / pos.highest_price - 1)
                if dd <= -tp:
                    portfolio.sell(pos, pos.highest_price * (1 - tp), trade_date, f'trailing{tp*100:.0f}%')
                    trades.append(portfolio.closed_trades[-1])
                    continue
            
            # 时间止损
            if time_stop_days > 0 and pos.hold_days >= time_stop_days:
                chg = (day['close'] / pos.buy_price - 1)
                if chg < 0.01:
                    portfolio.sell(pos, day['close'], trade_date, f'时间止损{time_stop_days}天')
                    trades.append(portfolio.closed_trades[-1])
                    continue
        
        # 买入
        if len(portfolio.positions) >= MAX_POSITIONS:
            continue
        
        # 市场情绪拦截
        if not phase_info['can_buy']:
            continue
        
        # 找昨天涨停的票
        try:
            idx = trading_dates.index(trade_date)
        except ValueError:
            continue
        if idx < 1:
            continue
        prev_date = trading_dates[idx - 1]
        
        zt_stocks = zt_pool.get(prev_date, [])
        for zt in zt_stocks:
            code = zt['code']
            if is_filtered(code):
                continue
            if code in portfolio.held_codes:
                continue
            if code not in daily or trade_date not in daily[code]:
                continue
            
            day = daily[code][trade_date]
            if day['pre_close'] is None or day['pre_close'] <= 0:
                continue
            
            buy_price = day['open'] if day['open'] > 0 else day['close']
            if buy_price <= 0:
                continue
            
            open_chg = (buy_price / day['pre_close'] - 1) * 100
            
            # 追高过滤
            threshold = open_chg_map.get(phase, 5.0)
            if open_chg > threshold:
                continue
            
            # 涨停/跌停跳过
            day_chg = (day['close'] / day['pre_close'] - 1) * 100
            if day_chg >= 9.5 or day_chg <= -9.5:
                continue
            
            # 自定义过滤
            if buy_filter and not buy_filter(zt, day, open_chg, phase_info):
                continue
            
            if portfolio.ab_count >= MAX_AB_POSITIONS:
                break
            
            pos = portfolio.buy(code, zt['name'], 'B', buy_price, trade_date, '涨停确认')
            if pos:
                trades.append({
                    'code': code, 'name': zt['name'],
                    'buy_date': trade_date, 'buy_price': pos.buy_price,
                    'strategy': 'B', 'signal_type': '涨停确认',
                    'open_chg': open_chg,
                    'consecutive_zt': zt.get('consecutive_zt', 1),
                })
    
    # 清算剩余持仓
    for pos in portfolio.positions[:]:
        last_date = dates[-1]
        if pos.code in daily and last_date in daily[pos.code]:
            portfolio.sell(pos, daily[pos.code][last_date]['close'], last_date, '回测结束清算')
    
    return trades, portfolio


def calc_metrics(trades_list):
    """从trades计算指标(用closed_trades)"""
    # trades_list是portfolio.closed_trades
    if not trades_list:
        return None
    pnls = [t['pnl'] for t in trades_list]
    pcts = [t['pnl_pct'] for t in trades_list]
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p <= 0]
    return {
        'trades': len(trades_list),
        'total_pnl': sum(pnls),
        'avg_pct': statistics.mean(pcts) if pcts else 0,
        'win_rate': len(wins)/len(pnls)*100 if pnls else 0,
        'pf': abs(sum(wins)/sum(losses)) if losses and sum(losses) != 0 else 999,
        'sharpe': statistics.mean(pcts)/statistics.stdev(pcts)*(252**0.5) if len(pcts) > 1 else 0,
    }


# ═══════════════════════════════════════════════
# 测试矩阵
# ═══════════════════════════════════════════════
if __name__ == '__main__':
    print("加载历史数据...")
    conn = get_conn()
    daily = load_daily_prices(conn)
    zt_pool = load_zt_pool(conn)
    conn.close()
    trading_dates = get_trading_dates(daily, zt_pool)
    
    print(f"数据: {len(trading_dates)}天\n")
    
    # 1. 止损参数扫描
    print("=" * 70)
    print("[1] 止损参数扫描(其他参数不变)")
    print("=" * 70)
    for sl in [-0.05, -0.06, -0.07, -0.08, -0.10]:
        trades, pf = run_b_backtest(daily, zt_pool, trading_dates, stop_loss=sl)
        m = calc_metrics(pf.closed_trades)
        if m:
            print(f"  止损{sl*100:.0f}%: {m['trades']}笔, 笔均{m['avg_pct']:+.2f}%, "
                  f"胜率{m['win_rate']:.1f}%, PF={m['pf']:.2f}, 夏普{m['sharpe']:.2f}, "
                  f"总盈亏¥{m['total_pnl']:+,.0f}")
    
    # 2. Trailing参数扫描
    print(f"\n{'=' * 70}")
    print("[2] Trailing参数扫描")
    print("=" * 70)
    for tp in [0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.10]:
        trades, pf = run_b_backtest(daily, zt_pool, trading_dates, trailing_pct=tp)
        m = calc_metrics(pf.closed_trades)
        if m:
            print(f"  trailing{tp*100:.0f}%: {m['trades']}笔, 笔均{m['avg_pct']:+.2f}%, "
                  f"胜率{m['win_rate']:.1f}%, PF={m['pf']:.2f}, 夏普{m['sharpe']:.2f}, "
                  f"总盈亏¥{m['total_pnl']:+,.0f}")
    
    # 3. 追高过滤参数扫描
    print(f"\n{'=' * 70}")
    print("[3] 追高过滤参数扫描(正常市场)")
    print("=" * 70)
    for chg in [2.0, 3.0, 4.0, 5.0, 6.0, 8.0, 99.0]:
        trades, pf = run_b_backtest(daily, zt_pool, trading_dates, open_chg_normal=chg)
        m = calc_metrics(pf.closed_trades)
        if m:
            print(f"  高开<{chg:.0f}%: {m['trades']}笔, 笔均{m['avg_pct']:+.2f}%, "
                  f"胜率{m['win_rate']:.1f}%, PF={m['pf']:.2f}, 夏普{m['sharpe']:.2f}, "
                  f"总盈亏¥{m['total_pnl']:+,.0f}")
    
    # 4. 最大持仓天数扫描
    print(f"\n{'=' * 70}")
    print("[4] 最大持仓天数扫描")
    print("=" * 70)
    for mh in [2, 3, 5, 7, 10]:
        trades, pf = run_b_backtest(daily, zt_pool, trading_dates, max_hold=mh, time_stop_days=max(1, mh-2))
        m = calc_metrics(pf.closed_trades)
        if m:
            print(f"  最长{mh}天: {m['trades']}笔, 笔均{m['avg_pct']:+.2f}%, "
                  f"胜率{m['win_rate']:.1f}%, PF={m['pf']:.2f}, 夏普{m['sharpe']:.2f}, "
                  f"总盈亏¥{m['total_pnl']:+,.0f}")
    
    # 5. 连板过滤: 只追2连板以上的票
    print(f"\n{'=' * 70}")
    print("[5] 连板数过滤")
    print("=" * 70)
    for min_lb in [1, 2, 3]:
        def lb_filter(zt, day, open_chg, phase, _min=min_lb):
            return (zt.get('consecutive_zt', 1) or 1) >= _min
        trades, pf = run_b_backtest(daily, zt_pool, trading_dates, buy_filter=lb_filter)
        m = calc_metrics(pf.closed_trades)
        if m:
            print(f"  >={min_lb}连板: {m['trades']}笔, 笔均{m['avg_pct']:+.2f}%, "
                  f"胜率{m['win_rate']:.1f}%, PF={m['pf']:.2f}, 夏普{m['sharpe']:.2f}, "
                  f"总盈亏¥{m['total_pnl']:+,.0f}")
    
    # 6. 组合优化: 止损-5% + trailing 3% + 高开<3% + 2连板
    print(f"\n{'=' * 70}")
    print("[6] 优化组合测试")
    print("=" * 70)
    combos = [
        ("激进止损+紧trailing", -0.05, 0.03, 0.02, 3.0, 3.0, 3),
        ("宽松止损+宽trailing", -0.10, 0.08, 0.05, 5.0, 3.0, 5),
        ("紧止损+宽trailing+严追高", -0.06, 0.08, 0.05, 3.0, 2.0, 5),
        ("仅2连板+止损-6%+trailing5%", -0.06, 0.05, 0.03, 5.0, 3.0, 5),
        ("极保守: 止损-5%+trailing3%+高开<2%+2连板", -0.05, 0.03, 0.02, 2.0, 2.0, 3),
    ]
    for label, sl, tp, te, chg_n, chg_e, mh in combos:
        def lb_filter2(zt, day, oc, phase, _min=2):
            return (zt.get('consecutive_zt', 1) or 1) >= _min
        trades, pf = run_b_backtest(daily, zt_pool, trading_dates,
                                    stop_loss=sl, trailing_pct=tp, trailing_ebb=te,
                                    open_chg_normal=chg_n, open_chg_ebb=chg_e,
                                    max_hold=mh, buy_filter=lb_filter2)
        m = calc_metrics(pf.closed_trades)
        if m:
            print(f"  {label}:")
            print(f"    {m['trades']}笔, 笔均{m['avg_pct']:+.2f}%, 胜率{m['win_rate']:.1f}%, "
                  f"PF={m['pf']:.2f}, 夏普{m['sharpe']:.2f}, 总盈亏¥{m['total_pnl']:+,.0f}")
