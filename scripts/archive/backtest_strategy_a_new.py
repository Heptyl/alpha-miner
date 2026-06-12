"""
Alpha Miner 策略A新方向回测 — 缩量回踩突破 + MACD金叉放量
基于backtest_full_v4.py框架，复用数据加载和统计函数
只用daily_price(944天/493万条)，数据充足

策略A1: 缩量回踩突破
  - 前期放量突破20日高点(量>5日均量1.5倍)
  - 之后2-5天缩量回踩(量<5日均量0.7倍)
  - 回踩不破突破位(最低价>突破日收盘价×0.98)
  - 买入价: 回踩确认日收盘价
  - 卖出: 止损-5% / 止盈+5% / 最长5天收盘卖

策略A2: MACD金叉放量
  - MACD在零轴附近金叉(DIF上穿DEA)
  - 金叉日成交量>5日均量1.3倍
  - 股价站上20日均线
  - 买入价: 金叉日收盘价
  - 卖出: 止损-5% / 止盈+5% / 最长7天收盘卖
"""
import sqlite3
import statistics
from collections import defaultdict

DB_PATH = 'data/alpha_miner.db'
COST_PCT = 0.5   # 交易成本0.5%
SLIPPAGE = 0.003  # 滑点0.3%


def pct(val):
    return f"{val*100:+.2f}%"


def stats(trades, label):
    if not trades:
        print(f"  {label}: 无交易")
        return
    wins = [t for t in trades if t > 0]
    losses = [t for t in trades if t <= 0]
    n = len(trades)
    avg = statistics.mean(trades) * 100
    win_rate = len(wins) / n * 100
    pf = sum(wins) / abs(sum(losses)) if losses else 999
    print(f"  {label}: {n}笔 均赚{avg:+.2f}% 胜率{win_rate:.1f}% PF={pf:.2f}")


def yearly_stats(yearly, label):
    for yr in sorted(yearly):
        yt = yearly[yr]
        if len(yt) < 5:
            continue
        wins = [t for t in yt if t > 0]
        losses = [t for t in yt if t <= 0]
        wr = len(wins)/len(yt)*100
        avg = statistics.mean(yt)*100
        pf = sum(wins)/abs(sum(losses)) if losses else 999
        print(f"    {yr}: {len(yt):>5}笔  胜率{wr:>5.1f}%  均赚{avg:>+5.2f}%  PF={pf:>5.2f}")


def compute_macd(closes, fast=12, slow=26, signal=9):
    """计算MACD指标"""
    import numpy as np
    if len(closes) < slow + signal:
        return None, None, None
    ema_fast = np.zeros(len(closes))
    ema_slow = np.zeros(len(closes))
    ema_fast[0] = closes[0]
    ema_slow[0] = closes[0]
    k_fast = 2 / (fast + 1)
    k_slow = 2 / (slow + 1)
    for i in range(1, len(closes)):
        ema_fast[i] = closes[i] * k_fast + ema_fast[i-1] * (1 - k_fast)
        ema_slow[i] = closes[i] * k_slow + ema_slow[i-1] * (1 - k_slow)
    dif = ema_fast - ema_slow
    dea = np.zeros(len(closes))
    dea[slow-1] = dif[slow-1]
    k_sig = 2 / (signal + 1)
    for i in range(slow, len(closes)):
        dea[i] = dif[i] * k_sig + dea[i-1] * (1 - k_sig)
    macd_hist = (dif - dea) * 2
    return dif, dea, macd_hist


def run():
    conn = sqlite3.connect(DB_PATH)

    # === 加载数据 ===
    print("加载数据...")
    # 按股票分组存储, 每只股票的日K按日期排序
    by_stock = defaultdict(list)
    for row in conn.execute("""
        SELECT stock_code, trade_date, open, close, high, low, pre_close, volume, amount
        FROM daily_price WHERE open > 0 AND close > 0 AND pre_close > 0 AND volume > 0
    """):
        code, date, o, c, h, lo, pc, vol, amt = row
        if code.startswith(('8','9','200','900')):
            continue
        by_stock[code].append({
            'date': date, 'open': o, 'close': c, 'high': h, 'low': lo,
            'pre_close': pc, 'volume': vol, 'amount': amt
        })

    # 排序
    for code in by_stock:
        by_stock[code].sort(key=lambda x: x['date'])

    all_dates = sorted(set(d['date'] for bars in by_stock.values() for d in bars))
    date_idx = {d: i for i, d in enumerate(all_dates)}
    print(f"  {len(by_stock)}只股票, {len(all_dates)}天, {all_dates[0]}~{all_dates[-1]}")

    # ================================================================
    # 策略A1: 缩量回踩突破
    # ================================================================
    print(f"\n{'='*70}")
    print("策略A1: 缩量回踩突破")
    print(f"{'='*70}")

    a1_trades = []
    a1_yearly = defaultdict(list)
    a1_by_stock = defaultdict(list)  # 每只股票的交易记录

    for code, bars in by_stock.items():
        if len(bars) < 30:
            continue

        closes = [b['close'] for b in bars]
        volumes = [b['volume'] for b in bars]
        highs = [b['high'] for b in bars]
        lows = [b['low'] for b in bars]
        dates = [b['date'] for b in bars]

        i = 25  # 从第25天开始(留够20日窗口)
        while i < len(bars) - 6:  # 至少留6天用于卖出
            # 1. 检查今天是否突破20日新高
            high_20 = max(highs[i-20:i])
            if highs[i] <= high_20:
                i += 1
                continue

            # 2. 突破日放量(量>5日均量1.5倍)
            vol_ma5 = sum(volumes[i-5:i]) / 5
            if vol_ma5 <= 0 or volumes[i] < vol_ma5 * 1.5:
                i += 1
                continue

            # 3. 突破日确认: 收盘价>20日最高价(真突破)
            if closes[i] <= high_20:
                i += 1
                continue

            breakout_close = closes[i]  # 突破位
            breakout_idx = i

            # 4. 寻找回踩(未来2-5天内缩量回踩不破突破位)
            buy_price = None
            buy_idx = None
            for j in range(i+1, min(i+6, len(bars)-1)):
                vol_ma5_j = sum(volumes[max(0,j-5):j]) / 5
                # 缩量: 当日量<5日均量0.7倍
                if vol_ma5_j <= 0:
                    continue
                vol_ratio = volumes[j] / vol_ma5_j
                if vol_ratio > 0.7:
                    continue
                # 回踩不破突破位(最低价>突破收盘×0.98)
                if lows[j] < breakout_close * 0.98:
                    continue
                # 回踩日收盘价在突破位附近或下方(正常回踩)
                if closes[j] > breakout_close * 1.03:
                    continue

                buy_price = closes[j] * (1 + SLIPPAGE)
                buy_idx = j
                break

            if buy_price is None:
                i = breakout_idx + 6
                continue

            # 5. 卖出: 止损-5% / 止盈+5% / 最长5天
            sell_price = None
            sell_reason = ""
            for k in range(buy_idx+1, min(buy_idx+6, len(bars))):
                pnl = (bars[k]['low'] / buy_price - 1)
                if pnl <= -0.05:
                    sell_price = buy_price * 0.95
                    sell_reason = "止损-5%"
                    break
                pnl = (bars[k]['high'] / buy_price - 1)
                if pnl >= 0.05:
                    sell_price = bars[k]['close']  # 止盈用收盘价
                    sell_reason = "止盈+5%"
                    break

            if sell_price is None:
                sell_idx = min(buy_idx+5, len(bars)-1)
                sell_price = bars[sell_idx]['close']
                sell_reason = f"到期{sell_idx-buy_idx}天"

            pnl = (sell_price / buy_price - 1) - COST_PCT / 100
            a1_trades.append(pnl)
            yr = dates[buy_idx][:4]
            a1_yearly[yr].append(pnl)
            a1_by_stock[code].append({
                'buy_date': dates[buy_idx], 'sell_date': dates[min(buy_idx+5, len(bars)-1)],
                'buy_price': buy_price, 'pnl': pnl, 'reason': sell_reason
            })

            i = buy_idx + 6  # 跳过已处理的区间

    stats(a1_trades, "缩量回踩突破(总计)")
    yearly_stats(a1_yearly, "A1")

    # ================================================================
    # 策略A2: MACD金叉放量
    # ================================================================
    print(f"\n{'='*70}")
    print("策略A2: MACD金叉放量")
    print(f"{'='*70}")

    a2_trades = []
    a2_yearly = defaultdict(list)

    import numpy as np

    for code, bars in by_stock.items():
        if len(bars) < 40:
            continue

        closes = np.array([b['close'] for b in bars], dtype=float)
        volumes = np.array([b['volume'] for b in bars], dtype=float)
        dates = [b['date'] for b in bars]

        dif, dea, macd_hist = compute_macd(closes)
        if dif is None:
            continue

        # 20日均线
        ma20 = np.convolve(closes, np.ones(20)/20, mode='valid')
        # ma20从index 19开始

        for i in range(27, len(bars) - 8):  # 27 = 26(slow周期) + 1, 留8天卖出
            # 金叉: DIF上穿DEA
            if dif[i-1] <= dea[i-1] and dif[i] > dea[i]:
                # 放量确认
                if i < 5:
                    continue
                vol_ma5 = np.mean(volumes[i-5:i])
                if vol_ma5 <= 0 or volumes[i] < vol_ma5 * 1.3:
                    continue
                # 站上20日均线
                ma20_idx = i - 19
                if ma20_idx < 0 or ma20_idx >= len(ma20):
                    continue
                if closes[i] < ma20[ma20_idx]:
                    continue

                buy_price = closes[i] * (1 + SLIPPAGE)

                # 卖出: 止损-5% / 止盈+5% / 最长7天
                sell_price = None
                for k in range(i+1, min(i+8, len(bars))):
                    pnl_low = (bars[k]['low'] / buy_price - 1)
                    if pnl_low <= -0.05:
                        sell_price = buy_price * 0.95
                        break
                    pnl_high = (bars[k]['high'] / buy_price - 1)
                    if pnl_high >= 0.05:
                        sell_price = bars[k]['close']
                        break

                if sell_price is None:
                    sell_idx = min(i+7, len(bars)-1)
                    sell_price = bars[sell_idx]['close']

                pnl = (sell_price / buy_price - 1) - COST_PCT / 100
                a2_trades.append(pnl)
                yr = dates[i][:4]
                a2_yearly[yr].append(pnl)

                # 跳过3天避免重复信号
                i += 3

    stats(a2_trades, "MACD金叉放量(总计)")
    yearly_stats(a2_yearly, "A2")

    # ================================================================
    # 合并统计
    # ================================================================
    print(f"\n{'='*70}")
    print("策略A(新) 合并: 缩量回踩 + MACD金叉")
    print(f"{'='*70}")
    all_a = a1_trades + a2_trades
    stats(all_a, "总计")
    combined_yearly = defaultdict(list)
    for yr, trades in a1_yearly.items():
        combined_yearly[yr].extend(trades)
    for yr, trades in a2_yearly.items():
        combined_yearly[yr].extend(trades)
    yearly_stats(combined_yearly, "A合并")

    # ================================================================
    # 95%置信区间
    # ================================================================
    if len(all_a) >= 30:
        se = statistics.stdev(all_a) / (len(all_a) ** 0.5)
        mean = statistics.mean(all_a)
        ci_lo = (mean - 1.96 * se) * 100
        ci_hi = (mean + 1.96 * se) * 100
        print(f"\n  95%CI = [{ci_lo:+.2f}%, {ci_hi:+.2f}%]")
        if ci_lo > 0:
            print(f"  ★ 下界>0, 策略显著盈利")
        else:
            print(f"  ✗ 下界<=0, 策略不显著")

    # ================================================================
    # 和策略B对比
    # ================================================================
    print(f"\n{'='*70}")
    print("对比: 策略A(新) vs 策略B(涨停低吸)")
    print(f"{'='*70}")
    print(f"  A1(缩量回踩): {len(a1_trades)}笔")
    print(f"  A2(MACD金叉): {len(a2_trades)}笔")
    print(f"  A合计:        {len(all_a)}笔, 日均{len(all_a)/len(all_dates):.1f}只")

    if a1_trades:
        a1_wins = [t for t in a1_trades if t > 0]
        print(f"  A1年均: {len(a1_trades)/len(a1_yearly):.0f}笔, 日均约{len(a1_trades)/len(all_dates):.1f}只")
    if a2_trades:
        print(f"  A2年均: {len(a2_trades)/len(a2_yearly):.0f}笔, 日均约{len(a2_trades)/len(all_dates):.1f}只")


if __name__ == "__main__":
    run()
