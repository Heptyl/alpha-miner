#!/usr/bin/env python3
"""
回测对比: "主动调仓" vs "被动等待" 策略
时间范围: 2026-03-01 ~ 2026-05-12
"""
import sqlite3
import math
from collections import defaultdict

DB_PATH = "/home/ccy/alpha-miner/data/alpha_miner.db"
INITIAL_CASH = 20000
MAX_POS = 5
SINGLE_LIMIT = 3000
START_DATE = "2026-03-01"
END_DATE = "2026-05-12"

# 交易成本
BUY_COMM = 0.00025    # 买入佣金万2.5
SELL_COMM = 0.00025   # 卖出佣金万2.5
STAMP_TAX = 0.001     # 印花税千1

# ---- 加载数据 ----
def load_data():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    # 日K线
    c.execute("""
        SELECT stock_code, trade_date, open, high, low, close, pre_close, volume, amount
        FROM daily_price 
        WHERE trade_date >= ? AND trade_date <= ?
        ORDER BY trade_date, stock_code
    """, (START_DATE, END_DATE))
    
    daily = {}  # {(date, code): {open, high, low, close, pre_close, volume, amount}}
    for row in c.fetchall():
        code, date, o, h, lo, cl, pre_cl, vol, amt = row
        daily[(date, code)] = {
            'open': o, 'high': h, 'low': lo, 'close': cl,
            'pre_close': pre_cl, 'volume': vol, 'amount': amt
        }
    
    # 涨停池
    c.execute("""
        SELECT stock_code, trade_date, name, consecutive_zt, amount
        FROM zt_pool
        WHERE trade_date >= ? AND trade_date <= ?
    """, (START_DATE, END_DATE))
    
    zt_pool = defaultdict(list)  # {date: [{code, name, consec, amount}]}
    for row in c.fetchall():
        code, date, name, consec, amt = row
        zt_pool[date].append({
            'code': code, 'name': name, 'consecutive_zt': consec, 'amount': amt
        })
    
    # 交易日列表
    c.execute("""
        SELECT DISTINCT trade_date FROM daily_price
        WHERE trade_date >= ? AND trade_date <= ?
        ORDER BY trade_date
    """, (START_DATE, END_DATE))
    trade_dates = [r[0] for r in c.fetchall()]
    
    conn.close()
    return daily, zt_pool, trade_dates


def get_price(daily, date, code):
    """获取某日某股票的价格数据"""
    return daily.get((date, code))


def calc_buy_amount(price_per_share, max_amount):
    """计算可买股数(100股整数倍)和实际金额"""
    shares = int(max_amount / price_per_share / 100) * 100
    if shares <= 0:
        shares = 100
    actual = shares * price_per_share
    if actual > max_amount:
        shares = 0
        actual = 0
    return shares, actual


# ---- 策略信号 ----
def get_strategy_b_candidates(daily, zt_pool, current_date, prev_date):
    """策略B: 昨日涨停股, 今天没跌超-2%可买入"""
    if prev_date is None:
        return []
    
    zt_stocks = zt_pool.get(prev_date, [])
    candidates = []
    
    for zt in zt_stocks:
        code = zt['code']
        today = get_price(daily, current_date, code)
        if today is None or today['pre_close'] is None or today['pre_close'] <= 0:
            continue
        # 当天跌幅
        pct = (today['close'] - today['pre_close']) / today['pre_close']
        # 条件: 没跌超-2%
        if pct >= -0.02:
            candidates.append({
                'code': code,
                'buy_price': today['close'],
                'pct': pct,
                'strategy': 'B',
                'score': zt.get('amount', 0),  # 用成交额做评分(越大越好)
                'consec': zt.get('consecutive_zt', 1)
            })
    
    # 按连板数降序, 再按成交额降序
    candidates.sort(key=lambda x: (-x['consec'], -x['score']))
    return candidates


def get_strategy_a_candidates(daily, current_date, all_codes):
    """策略A: 当天跌幅2%-5%的股票(ML低吸)"""
    candidates = []
    for code in all_codes:
        today = get_price(daily, current_date, code)
        if today is None or today['pre_close'] is None or today['pre_close'] <= 0:
            continue
        if today['volume'] is None or today['volume'] < 100000:
            continue  # 过滤低流动性
        pct = (today['close'] - today['pre_close']) / today['pre_close']
        if -0.05 <= pct <= -0.02:
            # 评分: 跌幅越深分越高(抄底心态), 成交量大的加分
            vol_score = min(today['volume'] / 1000000, 10)
            dip_score = abs(pct) * 100
            score = dip_score * 0.7 + vol_score * 0.3
            candidates.append({
                'code': code,
                'buy_price': today['close'],
                'pct': pct,
                'strategy': 'A',
                'score': score
            })
    
    candidates.sort(key=lambda x: -x['score'])
    return candidates[:50]  # 限制候选数量


def check_sell(pos, today_data, daily, buy_date, current_date, trade_dates):
    """
    卖出判断:
    - 策略B: 3天止损期 / 5天最长持有
    - 策略A: 5天止损期 / 7天最长持有
    - 共用: -8%止损 + 3%移动止盈
    """
    strategy = pos['strategy']
    buy_price = pos['buy_price']
    current_price = today_data['close']
    pnl = (current_price - buy_price) / buy_price
    
    # 计算持有天数(交易日)
    try:
        buy_idx = trade_dates.index(buy_date)
        curr_idx = trade_dates.index(current_date)
    except ValueError:
        return False, 0, "date_error"
    
    hold_days = curr_idx - buy_idx
    
    # 最长持有期
    max_hold = 5 if strategy == 'B' else 7
    stop_loss_days = 3 if strategy == 'B' else 5
    
    # -8%止损
    if pnl <= -0.08:
        return True, pnl, f"stop_loss_{pnl:.2%}"
    
    # 时间止损(在止损期内亏损)
    if hold_days >= stop_loss_days and pnl < 0:
        return True, pnl, f"time_stop_{pnl:.2%}"
    
    # 最长持有期卖出
    if hold_days >= max_hold:
        return True, pnl, f"max_hold_{pnl:.2%}"
    
    # 3%移动止盈(曾涨到3%以上, 然后回落)
    if pos.get('high_pnl', 0) >= 0.03:
        high_pnl = pos['high_pnl']
        # 从最高点回落超过2%就卖
        if high_pnl - pnl >= 0.02:
            return True, pnl, f"trailing_stop_{pnl:.2%}"
    
    return False, pnl, ""


# ---- 回测引擎 ----
class BacktestEngine:
    def __init__(self, daily, zt_pool, trade_dates, enable_rebalance=False):
        self.daily = daily
        self.zt_pool = zt_pool
        self.trade_dates = trade_dates
        self.enable_rebalance = enable_rebalance
        
        self.cash = INITIAL_CASH
        self.positions = []  # [{code, shares, buy_price, buy_date, strategy, high_pnl, high_price}]
        self.trades = []     # [{date, code, action, price, shares, pnl, strategy, reason}]
        self.daily_equity = []  # [(date, equity)]
        self.rebalance_count = 0
        self.rebalance_cost = 0
        self.rebalance_gain = 0
        
        # 获取所有股票代码
        self.all_codes = set()
        for (d, c) in daily.keys():
            self.all_codes.add(c)
    
    def run(self):
        for i, date in enumerate(self.trade_dates):
            prev_date = self.trade_dates[i-1] if i > 0 else None
            
            # 1. 先处理卖出
            self._process_sells(date)
            
            # 2. 更新持仓最高盈利
            self._update_high_pnl(date)
            
            # 3. 获取买入候选
            candidates_b = get_strategy_b_candidates(self.daily, self.zt_pool, date, prev_date)
            candidates_a = get_strategy_a_candidates(self.daily, date, self.all_codes)
            
            # 合并候选: B优先
            all_candidates = []
            held_codes = {p['code'] for p in self.positions}
            
            for c in candidates_b:
                if c['code'] not in held_codes:
                    all_candidates.append(c)
            for c in candidates_a:
                if c['code'] not in held_codes:
                    all_candidates.append(c)
            
            # 4. 主动调仓逻辑
            if self.enable_rebalance and len(self.positions) >= MAX_POS and all_candidates:
                self._try_rebalance(date, all_candidates)
            
            # 5. 买入(空位)
            self._process_buys(date, all_candidates)
            
            # 6. 记录日净值
            equity = self._calc_equity(date)
            self.daily_equity.append((date, equity))
    
    def _process_sells(self, date):
        to_sell = []
        for pos in self.positions:
            today = get_price(self.daily, date, pos['code'])
            if today is None:
                continue
            
            should_sell, pnl, reason = check_sell(
                pos, today, self.daily, pos['buy_date'], date, self.trade_dates
            )
            
            if should_sell:
                sell_price = today['close']
                sell_amount = pos['shares'] * sell_price
                # 扣除卖出成本
                cost = sell_amount * (SELL_COMM + STAMP_TAX)
                net_amount = sell_amount - cost
                
                self.cash += net_amount
                to_sell.append(pos)
                self.trades.append({
                    'date': date, 'code': pos['code'], 'action': 'sell',
                    'price': sell_price, 'shares': pos['shares'],
                    'pnl': pnl, 'strategy': pos['strategy'],
                    'reason': reason, 'hold_days': self._hold_days(pos, date)
                })
        
        for pos in to_sell:
            self.positions.remove(pos)
    
    def _update_high_pnl(self, date):
        for pos in self.positions:
            today = get_price(self.daily, date, pos['code'])
            if today is None:
                continue
            pnl = (today['close'] - pos['buy_price']) / pos['buy_price']
            if pnl > pos.get('high_pnl', 0):
                pos['high_pnl'] = pnl
                pos['high_price'] = today['close']
    
    def _process_buys(self, date, candidates):
        for cand in candidates:
            if len(self.positions) >= MAX_POS:
                break
            if cand['code'] in {p['code'] for p in self.positions}:
                continue
            
            buy_price = cand['buy_price']
            if buy_price <= 0:
                continue
            
            # 计算可买金额
            max_amount = min(SINGLE_LIMIT, self.cash * 0.95)  # 留点余量
            if max_amount < buy_price * 100:
                continue
            
            shares, actual = calc_buy_amount(buy_price, max_amount)
            if shares <= 0:
                continue
            
            # 加上买入佣金
            buy_cost = actual * (1 + BUY_COMM)
            if buy_cost > self.cash:
                continue
            
            self.cash -= buy_cost
            self.positions.append({
                'code': cand['code'],
                'shares': shares,
                'buy_price': buy_price,
                'buy_date': date,
                'strategy': cand['strategy'],
                'high_pnl': 0,
                'high_price': buy_price,
                'score': cand.get('score', 0)
            })
            self.trades.append({
                'date': date, 'code': cand['code'], 'action': 'buy',
                'price': buy_price, 'shares': shares,
                'pnl': 0, 'strategy': cand['strategy'],
                'reason': 'new_buy', 'hold_days': 0
            })
    
    def _try_rebalance(self, date, candidates):
        """主动调仓: 满仓时, 如果新候选评分 > 最差持仓评分*1.3, 且最差浮盈<2%"""
        if not candidates:
            return
        
        best_candidate = candidates[0]
        
        # 找持仓中最差的(浮盈最低的)
        worst_pos = None
        worst_pnl = float('inf')
        for pos in self.positions:
            today = get_price(self.daily, date, pos['code'])
            if today is None:
                continue
            pnl = (today['close'] - pos['buy_price']) / pos['buy_price']
            if pnl < worst_pnl:
                worst_pnl = pnl
                worst_pos = pos
        
        if worst_pos is None:
            return
        
        # T+1检查: 不能卖当天买的
        hold_days = self._hold_days(worst_pos, date)
        if hold_days < 1:
            return
        
        # 调仓条件: 浮盈<2% 且 新候选评分 > 最差评分*1.3
        if worst_pnl >= 0.02:
            return
        
        worst_score = worst_pos.get('score', 0)
        new_score = best_candidate.get('score', 0)
        
        if worst_score <= 0:
            if new_score <= 0:
                return
        else:
            if new_score <= worst_score * 1.3:
                return
        
        # 执行调仓
        today = get_price(self.daily, date, worst_pos['code'])
        sell_price = today['close']
        sell_amount = worst_pos['shares'] * sell_price
        sell_cost = sell_amount * (SELL_COMM + STAMP_TAX)
        
        old_pnl = worst_pnl
        old_value = worst_pos['shares'] * worst_pos['buy_price']
        rebalance_pnl_gain = sell_amount - old_value  # 这次调仓的盈亏
        
        # 买入新候选
        buy_price = best_candidate['buy_price']
        available = sell_amount - sell_cost  # 卖出后可用资金
        available += self.cash  # 加上原有现金
        max_amount = min(SINGLE_LIMIT, available * 0.95)
        
        if max_amount < buy_price * 100:
            return
        
        shares, actual = calc_buy_amount(buy_price, max_amount)
        if shares <= 0:
            return
        
        buy_cost_total = actual * (1 + BUY_COMM)
        
        # 确认资金足够
        total_cost = sell_cost + buy_cost_total
        if total_cost > self.cash + sell_amount:
            return
        
        # 执行卖出
        self.cash += sell_amount - sell_cost
        self.positions.remove(worst_pos)
        self.trades.append({
            'date': date, 'code': worst_pos['code'], 'action': 'sell',
            'price': sell_price, 'shares': worst_pos['shares'],
            'pnl': old_pnl, 'strategy': worst_pos['strategy'],
            'reason': f'rebalance_sell_{old_pnl:.2%}',
            'hold_days': hold_days
        })
        
        # 执行买入
        self.cash -= buy_cost_total
        self.positions.append({
            'code': best_candidate['code'],
            'shares': shares,
            'buy_price': buy_price,
            'buy_date': date,
            'strategy': best_candidate['strategy'],
            'high_pnl': 0,
            'high_price': buy_price,
            'score': best_candidate.get('score', 0)
        })
        self.trades.append({
            'date': date, 'code': best_candidate['code'], 'action': 'buy',
            'price': buy_price, 'shares': shares,
            'pnl': 0, 'strategy': best_candidate['strategy'],
            'reason': 'rebalance_buy', 'hold_days': 0
        })
        
        self.rebalance_count += 1
        self.rebalance_cost += sell_cost + actual * BUY_COMM
        self.rebalance_gain += rebalance_pnl_gain
    
    def _hold_days(self, pos, date):
        try:
            return self.trade_dates.index(date) - self.trade_dates.index(pos['buy_date'])
        except ValueError:
            return 999
    
    def _calc_equity(self, date):
        equity = self.cash
        for pos in self.positions:
            today = get_price(self.daily, date, pos['code'])
            if today:
                equity += pos['shares'] * today['close']
            else:
                equity += pos['shares'] * pos['buy_price']
        return equity
    
    def get_stats(self):
        sell_trades = [t for t in self.trades if t['action'] == 'sell']
        buy_trades = [t for t in self.trades if t['action'] == 'buy']
        
        if not sell_trades:
            return {
                'total_return': 0, 'win_rate': 0, 'avg_pnl': 0,
                'max_drawdown': 0, 'trade_count': 0, 'sell_count': 0,
                'final_equity': INITIAL_CASH
            }
        
        # 总收益率
        final_equity = self.daily_equity[-1][1] if self.daily_equity else INITIAL_CASH
        total_return = (final_equity - INITIAL_CASH) / INITIAL_CASH
        
        # 胜率
        wins = sum(1 for t in sell_trades if t['pnl'] > 0)
        win_rate = wins / len(sell_trades) if sell_trades else 0
        
        # 平均每笔收益
        avg_pnl = sum(t['pnl'] for t in sell_trades) / len(sell_trades)
        
        # 最大回撤
        max_dd = 0
        peak = INITIAL_CASH
        for date, eq in self.daily_equity:
            if eq > peak:
                peak = eq
            dd = (peak - eq) / peak
            if dd > max_dd:
                max_dd = dd
        
        return {
            'total_return': total_return,
            'win_rate': win_rate,
            'avg_pnl': avg_pnl,
            'max_drawdown': max_dd,
            'trade_count': len(buy_trades),
            'sell_count': len(sell_trades),
            'final_equity': final_equity,
            'rebalance_count': self.rebalance_count,
            'rebalance_cost': self.rebalance_cost,
            'rebalance_gain': self.rebalance_gain,
            'sell_trades': sell_trades,
            'daily_equity': self.daily_equity
        }


def print_results(stats_passive, stats_active):
    print("=" * 80)
    print("              主动调仓 vs 被动等待 -- 回测对比报告")
    print("=" * 80)
    print(f"  回测区间: {START_DATE} ~ {END_DATE}")
    print(f"  初始资金: {INITIAL_CASH}  |  单只上限: {SINGLE_LIMIT}  |  最多持仓: {MAX_POS}")
    print(f"  交易成本: 买入万2.5 + 卖出万2.5 + 印花税千1")
    print()
    
    print("-" * 80)
    print("  指标                  被动等待              主动调仓")
    print("-" * 80)
    print(f"  最终净值              {stats_passive['final_equity']:>10.2f}          {stats_active['final_equity']:>10.2f}")
    print(f"  总收益率              {stats_passive['total_return']:>10.2%}          {stats_active['total_return']:>10.2%}")
    print(f"  胜率                  {stats_passive['win_rate']:>10.2%}          {stats_active['win_rate']:>10.2%}")
    print(f"  平均每笔收益          {stats_passive['avg_pnl']:>10.2%}          {stats_active['avg_pnl']:>10.2%}")
    print(f"  最大回撤              {stats_passive['max_drawdown']:>10.2%}          {stats_active['max_drawdown']:>10.2%}")
    print(f"  买入次数              {stats_passive['trade_count']:>10d}          {stats_active['trade_count']:>10d}")
    print(f"  卖出次数(平仓)        {stats_passive['sell_count']:>10d}          {stats_active['sell_count']:>10d}")
    print("-" * 80)
    
    print()
    print("  调仓专项分析:")
    print(f"    调仓次数:           {stats_active['rebalance_count']}")
    print(f"    调仓产生的交易成本:  {stats_active['rebalance_cost']:.2f} 元")
    print(f"    调仓腾出资金差额:    {stats_active['rebalance_gain']:.2f} 元")
    
    extra_return = stats_active['total_return'] - stats_passive['total_return']
    extra_profit = stats_active['final_equity'] - stats_passive['final_equity']
    print(f"    调仓带来的额外收益:  {extra_return:.2%} ({extra_profit:.2f} 元)")
    
    print()
    
    # 按策略分别统计
    for label, stats in [("被动等待", stats_passive), ("主动调仓", stats_active)]:
        print(f"  {label} -- 按策略拆解:")
        sell_trades = stats.get('sell_trades', [])
        for strat in ['A', 'B']:
            strat_trades = [t for t in sell_trades if t['strategy'] == strat]
            if strat_trades:
                wins = sum(1 for t in strat_trades if t['pnl'] > 0)
                avg = sum(t['pnl'] for t in strat_trades) / len(strat_trades)
                print(f"    策略{strat}: {len(strat_trades)}笔, 胜率{wins/len(strat_trades):.0%}, "
                      f"平均收益{avg:.2%}")
            else:
                print(f"    策略{strat}: 无交易")
        print()
    
    # 卖出原因分布
    for label, stats in [("被动等待", stats_passive), ("主动调仓", stats_active)]:
        sell_trades = stats.get('sell_trades', [])
        reasons = defaultdict(int)
        for t in sell_trades:
            r = t['reason']
            if 'stop_loss' in r:
                reasons['止损(-8%)'] += 1
            elif 'time_stop' in r:
                reasons['时间止损'] += 1
            elif 'max_hold' in r:
                reasons['到期卖出'] += 1
            elif 'trailing_stop' in r:
                reasons['移动止盈'] += 1
            elif 'rebalance' in r:
                reasons['调仓卖出'] += 1
            else:
                reasons[r] += 1
        print(f"  {label} -- 卖出原因:")
        for reason, cnt in sorted(reasons.items(), key=lambda x: -x[1]):
            print(f"    {reason}: {cnt}次")
        print()
    
    # 结论
    print("=" * 80)
    print("  结论:")
    print("=" * 80)
    
    if stats_active['rebalance_count'] == 0:
        print("  在本回测期间, 满仓且满足调仓条件的场景为0次,")
        print("  主动调仓策略未实际触发。两种策略结果一致。")
        print("  建议: 继续观察更长周期,或放宽调仓条件(如评分倍数降低)。")
    elif extra_return > 0.02:
        print(f"  主动调仓相比被动等待多赚 {extra_return:.2%} ({extra_profit:.2f} 元),")
        print(f"  调仓{stats_active['rebalance_count']}次产生成本{stats_active['rebalance_cost']:.2f}元,")
        print(f"  净收益为正, 说明主动调仓在本期间是值得做的。")
    elif extra_return > 0:
        print(f"  主动调仓相比被动等待多赚 {extra_return:.2%} ({extra_profit:.2f} 元),")
        print(f"  但调仓{stats_active['rebalance_count']}次的交易成本{stats_active['rebalance_cost']:.2f}元,")
        print(f"  边际收益不大, 主动调仓的性价比存疑。")
    else:
        print(f"  主动调仓相比被动等待反而少赚 {extra_return:.2%} ({extra_profit:.2f} 元),")
        print(f"  调仓{stats_active['rebalance_count']}次反而拖累了收益。")
        print(f"  建议: 维持当前被动等待策略,不做主动调仓。")
    
    print("=" * 80)


def main():
    print("正在加载数据...")
    daily, zt_pool, trade_dates = load_data()
    print(f"数据加载完成: {len(daily)}条日K线, {sum(len(v) for v in zt_pool.values())}条涨停记录, {len(trade_dates)}个交易日")
    
    print("\n开始回测被动等待策略...")
    engine_passive = BacktestEngine(daily, zt_pool, trade_dates, enable_rebalance=False)
    engine_passive.run()
    stats_passive = engine_passive.get_stats()
    
    print("开始回测主动调仓策略...")
    engine_active = BacktestEngine(daily, zt_pool, trade_dates, enable_rebalance=True)
    engine_active.run()
    stats_active = engine_active.get_stats()
    
    print("\n")
    print_results(stats_passive, stats_active)
    
    # 打印部分调仓交易明细
    if stats_active['rebalance_count'] > 0:
        print("\n调仓交易明细:")
        rebal_trades = [t for t in engine_active.trades if 'rebalance' in t.get('reason', '')]
        for t in rebal_trades[:20]:
            print(f"  {t['date']} {t['code']} {t['action']:5s} "
                  f"@{t['price']:.2f} x{t['shares']} {t['reason']}")
    
    # 打印净值曲线
    print("\n每日净值对比(被动 vs 主动):")
    pe = {d: e for d, e in stats_passive['daily_equity']}
    ae = {d: e for d, e in stats_active['daily_equity']}
    for date in trade_dates:
        pv = pe.get(date, INITIAL_CASH)
        av = ae.get(date, INITIAL_CASH)
        print(f"  {date}  被动:{pv:>9.2f}  主动:{av:>9.2f}  差额:{av-pv:>+8.2f}")


if __name__ == "__main__":
    main()
