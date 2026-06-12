#!/usr/bin/env python3
"""
策略A vs 策略B 独立Walk-Forward对比
分别只跑A/B，看各段独立表现
"""
import sqlite3, math, json, sys
from collections import defaultdict
from datetime import datetime

sys.path.insert(0, '.')
from scripts.backtest_engine_v3 import (
    load_daily_prices, load_zt_pool, BacktestEngine, INITIAL_CASH
)


class StrategyAOnly(BacktestEngine):
    """只跑策略A"""
    def run(self, by_date, by_stock, zt_by_date, start_date, end_date):
        sorted_dates = sorted([d for d in by_date.keys() if start_date <= d <= end_date])
        self._sorted_dates = sorted_dates
        self._date_index = {d: i for i, d in enumerate(sorted_dates)}
        
        for trade_date in sorted_dates:
            day_stocks = {r['code']: r for r in by_date[trade_date]}
            
            # 卖出
            to_remove = []
            for pos in self.positions:
                if pos['code'] in day_stocks:
                    sell_reason = self.check_sell(pos, day_stocks[pos['code']], trade_date)
                    if sell_reason:
                        self.sell(pos, day_stocks[pos['code']]['close'], sell_reason, trade_date)
                        to_remove.append(pos)
            for pos in to_remove:
                self.positions.remove(pos)
            
            # 只跑策略A买入
            if len(self.positions) < 5 and self.cash >= 1000:
                held_codes = {p['code'] for p in self.positions}
                candidates = []
                for code, data in day_stocks.items():
                    if code in held_codes:
                        continue
                    signal = self.check_strategy_a(by_stock, code, data, trade_date)
                    if signal:
                        candidates.append((code, data, signal))
                candidates.sort(key=lambda x: -x[1]['amount'])
                for code, data, signal in candidates:
                    if len(self.positions) >= 5:
                        break
                    if self.cash < 1000:
                        break
                    self.buy(code, code, data['close'], signal['signal_type'],
                            signal['signal_reason'], signal['strategy'], trade_date)
            
            mv = self.calc_market_value(by_stock, trade_date)
            nav = self.cash + mv
            self.daily_nav.append({
                'date': trade_date, 'nav': round(nav, 2),
                'cash': round(self.cash, 2), 'market_value': round(mv, 2),
                'positions': len(self.positions),
            })
        return self.generate_report()


class StrategyBOnly(BacktestEngine):
    """只跑策略B"""
    def run(self, by_date, by_stock, zt_by_date, start_date, end_date):
        sorted_dates = sorted([d for d in by_date.keys() if start_date <= d <= end_date])
        self._sorted_dates = sorted_dates
        self._date_index = {d: i for i, d in enumerate(sorted_dates)}
        
        for trade_date in sorted_dates:
            day_stocks = {r['code']: r for r in by_date[trade_date]}
            
            # 卖出
            to_remove = []
            for pos in self.positions:
                if pos['code'] in day_stocks:
                    sell_reason = self.check_sell(pos, day_stocks[pos['code']], trade_date)
                    if sell_reason:
                        self.sell(pos, day_stocks[pos['code']]['close'], sell_reason, trade_date)
                        to_remove.append(pos)
            for pos in to_remove:
                self.positions.remove(pos)
            
            # 只跑策略B买入
            if len(self.positions) < 5 and self.cash >= 1000:
                held_codes = {p['code'] for p in self.positions}
                candidates = []
                for code, data in day_stocks.items():
                    if code in held_codes:
                        continue
                    signal = self.check_strategy_b(by_stock, zt_by_date, code, data, trade_date)
                    if signal:
                        candidates.append((code, data, signal))
                candidates.sort(key=lambda x: -x[1]['amount'])
                for code, data, signal in candidates:
                    if len(self.positions) >= 5:
                        break
                    if self.cash < 1000:
                        break
                    self.buy(code, code, data['close'], signal['signal_type'],
                            signal['signal_reason'], signal['strategy'], trade_date)
            
            mv = self.calc_market_value(by_stock, trade_date)
            nav = self.cash + mv
            self.daily_nav.append({
                'date': trade_date, 'nav': round(nav, 2),
                'cash': round(self.cash, 2), 'market_value': round(mv, 2),
                'positions': len(self.positions),
            })
        return self.generate_report()


def main():
    print('加载数据...')
    by_date, by_stock = load_daily_prices()
    zt_by_date = load_zt_pool(use_sim=True)
    sorted_dates = sorted(by_date.keys())
    print(f'数据: {len(sorted_dates)}天, {len(by_stock)}只, 涨停池{len(zt_by_date)}天')
    
    n_splits = 4
    total_days = len(sorted_dates)
    segment_size = total_days // n_splits
    
    print()
    print('=' * 70)
    print('  策略A vs 策略B 独立Walk-Forward对比 (4段)')
    print('=' * 70)
    
    all_a = []
    all_b = []
    
    for i in range(n_splits):
        start_idx = i * segment_size
        end_idx = (i + 1) * segment_size if i < n_splits - 1 else total_days
        seg_start = sorted_dates[start_idx]
        seg_end = sorted_dates[end_idx - 1]
        seg_days = end_idx - start_idx
        
        eng_a = StrategyAOnly(trailing_stop_pct=0.05)
        rpt_a = eng_a.run(by_date, by_stock, zt_by_date, seg_start, seg_end)
        
        eng_b = StrategyBOnly(trailing_stop_pct=0.05)
        rpt_b = eng_b.run(by_date, by_stock, zt_by_date, seg_start, seg_end)
        
        a_stats = rpt_a.get('strategy_stats', {}).get('A', {})
        b_stats = rpt_b.get('strategy_stats', {}).get('B', {})
        
        all_a.append({'segment': i+1, 'report': rpt_a, 'stats': a_stats})
        all_b.append({'segment': i+1, 'report': rpt_b, 'stats': b_stats})
        
        print()
        print(f'  第{i+1}段: {seg_start}~{seg_end} ({seg_days}天)')
        print(f'  {"":16s} {"策略A(低吸/追涨)":>18s} {"策略B(涨停确认)":>18s}')
        print(f'  {"─"*56}')
        
        ret_a = rpt_a.get('total_return_pct', 0)
        ret_b = rpt_b.get('total_return_pct', 0)
        sh_a = rpt_a.get('sharpe_ratio', 0)
        sh_b = rpt_b.get('sharpe_ratio', 0)
        dd_a = rpt_a.get('max_drawdown_pct', 0)
        dd_b = rpt_b.get('max_drawdown_pct', 0)
        tr_a = rpt_a.get('total_trades', 0)
        tr_b = rpt_b.get('total_trades', 0)
        wr_a = a_stats.get('win_rate', 0)
        wr_b = b_stats.get('win_rate', 0)
        pnl_a = a_stats.get('total_pnl', 0)
        pnl_b = b_stats.get('total_pnl', 0)
        avg_a = a_stats.get('avg_pnl', 0)
        avg_b = b_stats.get('avg_pnl', 0)
        hold_a = a_stats.get('avg_hold_days', 0)
        hold_b = b_stats.get('avg_hold_days', 0)
        
        print(f'  {"收益率":16s} {ret_a:>+17.2f}% {ret_b:>+17.2f}%')
        print(f'  {"Sharpe":16s} {sh_a:>18.3f} {sh_b:>18.3f}')
        print(f'  {"最大回撤":14s} {dd_a:>17.2f}% {dd_b:>17.2f}%')
        print(f'  {"交易笔数":14s} {tr_a:>18d} {tr_b:>18d}')
        print(f'  {"胜率":16s} {wr_a:>17.1f}% {wr_b:>17.1f}%')
        print(f'  {"总盈亏":14s} {pnl_a:>+17.2f} {pnl_b:>+17.2f}')
        print(f'  {"平均盈亏":14s} {avg_a:>+17.2f} {avg_b:>+17.2f}')
        print(f'  {"平均持有天":12s} {hold_a:>18.1f} {hold_b:>18.1f}')
        
        a_reasons = a_stats.get('sell_reasons', {})
        b_reasons = b_stats.get('sell_reasons', {})
        print(f'  卖出原因A: {dict(a_reasons)}')
        print(f'  卖出原因B: {dict(b_reasons)}')
    
    # 全区间
    print()
    print('=' * 70)
    print('  全区间对比 (208天)')
    print('=' * 70)
    
    eng_a_full = StrategyAOnly(trailing_stop_pct=0.05)
    rpt_a_full = eng_a_full.run(by_date, by_stock, zt_by_date, sorted_dates[0], sorted_dates[-1])
    eng_b_full = StrategyBOnly(trailing_stop_pct=0.05)
    rpt_b_full = eng_b_full.run(by_date, by_stock, zt_by_date, sorted_dates[0], sorted_dates[-1])
    
    a_f = rpt_a_full.get('strategy_stats', {}).get('A', {})
    b_f = rpt_b_full.get('strategy_stats', {}).get('B', {})
    
    print()
    print(f'  策略A全区间:')
    print(f'    收益={rpt_a_full.get("total_return_pct",0):+.2f}% Sharpe={rpt_a_full.get("sharpe_ratio",0):.3f} 回撤={rpt_a_full.get("max_drawdown_pct",0):.2f}%')
    print(f'    交易={rpt_a_full.get("total_trades",0)}笔 胜率={a_f.get("win_rate",0)}%')
    print(f'    总盈亏=¥{a_f.get("total_pnl",0):+.2f} 均盈亏=¥{a_f.get("avg_pnl",0):+.2f}')
    print(f'    均盈=¥{a_f.get("avg_win",0):+.2f} 均亏=¥{a_f.get("avg_loss",0):+.2f} 盈亏比={abs(a_f.get("avg_win",1)/a_f.get("avg_loss",1)) if a_f.get("avg_loss",0) != 0 else 999:.2f}')
    print(f'    卖出原因: {a_f.get("sell_reasons",{})}')
    
    print()
    print(f'  策略B全区间:')
    print(f'    收益={rpt_b_full.get("total_return_pct",0):+.2f}% Sharpe={rpt_b_full.get("sharpe_ratio",0):.3f} 回撤={rpt_b_full.get("max_drawdown_pct",0):.2f}%')
    print(f'    交易={rpt_b_full.get("total_trades",0)}笔 胜率={b_f.get("win_rate",0)}%')
    print(f'    总盈亏=¥{b_f.get("total_pnl",0):+.2f} 均盈亏=¥{b_f.get("avg_pnl",0):+.2f}')
    print(f'    均盈=¥{b_f.get("avg_win",0):+.2f} 均亏=¥{b_f.get("avg_loss",0):+.2f} 盈亏比={abs(b_f.get("avg_win",1)/b_f.get("avg_loss",1)) if b_f.get("avg_loss",0) != 0 else 999:.2f}')
    print(f'    卖出原因: {b_f.get("sell_reasons",{})}')
    
    # 结论
    print()
    print('=' * 70)
    print('  结论')
    print('=' * 70)
    
    a_positive = sum(1 for x in all_a if x['report'].get('total_return_pct', 0) > 0)
    b_positive = sum(1 for x in all_b if x['report'].get('total_return_pct', 0) > 0)
    print(f'  策略A盈利段: {a_positive}/4')
    print(f'  策略B盈利段: {b_positive}/4')
    
    if rpt_a_full.get('total_return_pct', 0) <= 0:
        print('  策略A全区间亏损 → 建议关闭或大幅改进')
    elif rpt_a_full.get('total_return_pct', 0) < rpt_b_full.get('total_return_pct', 0) * 0.5:
        print('  策略A收益远低于策略B → 考虑是否值得保留')
    else:
        print('  策略A有正贡献 → 保留但观察')


if __name__ == '__main__':
    main()
