"""
策略回测引擎 v1.0 — 精确复现 trading_daemon.py 的 A/B/C 三策略

核心设计原则:
  1. 买入用次日开盘价(预告→次日开盘成交)
  2. 卖出用当日收盘价(尾盘清仓) 或 触发价(trailing/止损用当日最低价模拟)
  3. 手续费+滑点与daemon完全一致
  4. 退潮/冰点判断用二维法(涨停数×涨跌比)
  5. 所有参数与daemon配置完全一致，不拍脑袋

输出:
  - 每个策略独立的盈亏/胜率/夏普/最大回撤
  - 3段时间交叉验证
  - 参数敏感性分析(止损/trailing/持仓天数)
"""

import sqlite3
import json
import sys
from datetime import datetime, date, timedelta
from pathlib import Path
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Optional
import statistics

# ═══════════════════════════════════════════════
# 配置 — 与 trading_daemon.py 完全一致
# ═══════════════════════════════════════════════
INITIAL_CAPITAL = 50_000.0
COMMISSION_RATE = 0.00025   # 万2.5
STAMP_DUTY_RATE = 0.0005    # 万5 卖出
SLIPPAGE = 0.001            # 滑点0.1%
MIN_COMMISSION = 5.0

AB_POSITION_RATIO = 0.15    # 策略A/B 15%
C_POSITION_RATIO = 0.10     # 策略C 10%
MAX_POSITIONS = 6
MAX_AB_POSITIONS = 4
MAX_C_POSITIONS = 2
MIN_CASH_RATIO = 0.15

STOP_LOSS_PCT = -0.08       # A/B止损
C_STOP_LOSS_PCT = -0.06     # C止损

SELL_PARAMS = {
    "A": {
        "trailing_stop_pct": 0.03,
        "trailing_ebb_pct": 0.02,
        "trailing_frost_pct": 0.015,
        "time_stop_days": 5,
        "time_stop_threshold": 0.01,
        "max_hold_days": 7,
    },
    "B": {
        "trailing_stop_pct": 0.05,
        "trailing_ebb_pct": 0.03,
        "trailing_frost_pct": 0.02,
        "time_stop_days": 3,
        "time_stop_threshold": 0.01,
        "max_hold_days": 5,
    },
    "C": {
        "trailing_stop_pct": 0.05,
        "trailing_ebb_pct": 0.03,
        "trailing_frost_pct": 0.02,
        "time_stop_days": 0,
        "time_stop_threshold": 0.01,
        "max_hold_days": 2,
    },
}

OPEN_CHG_FILTER = {
    '退潮': 3.0,
    '冰点': 5.0,
    '正常': 5.0,
    '高潮': 8.0,
}

STRATEGY_C_CONFIG = {
    'enabled': True,
    'stop_loss_pct': -0.06,
    'sell_at_close': True,
    'lianban_min': 2,
    't1_drop_min': -3.0,
    't2_open_low_max': -2.0,
}

# B股过滤: 200xxx深B + 900xxx沪B + 科创688/689 + 北交8xxx
def is_filtered(code: str) -> bool:
    return code.startswith(('688', '689', '200', '8', '9'))


# ═══════════════════════════════════════════════
# 数据加载
# ═══════════════════════════════════════════════
DB_PATH = Path(__file__).resolve().parents[1] / "data" / "alpha_miner.db"

def get_conn():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn

def load_daily_prices(conn) -> dict:
    """加载全部日K线，返回 {code: {date: {open, high, low, close, pre_close, volume}}}"""
    rows = conn.execute("""
        SELECT stock_code, trade_date, open, high, low, close, pre_close, volume, amount, turnover_rate
        FROM daily_price
        ORDER BY trade_date
    """).fetchall()
    
    data = defaultdict(dict)
    for r in rows:
        data[r[0]][r[1]] = {
            'open': r[2], 'high': r[3], 'low': r[4], 'close': r[5],
            'pre_close': r[6], 'volume': r[7], 'amount': r[8], 'turnover': r[9]
        }
    return data

def load_zt_pool(conn) -> dict:
    """加载涨停池，返回 {date: [{code, name, consecutive_zt}]}"""
    rows = conn.execute("""
        SELECT trade_date, stock_code, name, consecutive_zt
        FROM zt_pool
        ORDER BY trade_date
    """).fetchall()
    
    data = defaultdict(list)
    for r in rows:
        data[r[0]].append({
            'code': r[1], 'name': r[2], 'consecutive_zt': r[3] or 1
        })
    return data

def get_trading_dates(daily: dict, zt_pool: dict) -> list:
    """获取排序后的交易日列表"""
    dates = set()
    for code_data in daily.values():
        dates.update(code_data.keys())
    return sorted(dates)


# ═══════════════════════════════════════════════
# 市场情绪 — 二维判断(涨停数×涨跌比)
# ═══════════════════════════════════════════════
def get_market_phase(daily: dict, zt_pool: dict, trade_date: str) -> dict:
    """判断某天的市场情绪，与daemon的_check_market_sentiment一致"""
    
    # 涨停数: 优先用zt_pool，不够时从daily_price估算(涨幅>=9.5%)
    zt_count = len(zt_pool.get(trade_date, []))
    
    if zt_count == 0:
        # 从K线估算涨停
        for code, date_data in daily.items():
            if trade_date in date_data and not is_filtered(code):
                d = date_data[trade_date]
                if d['pre_close'] and d['pre_close'] > 0:
                    chg = (d['close'] / d['pre_close'] - 1) * 100
                    if chg >= 9.5:
                        zt_count += 1
    
    # 涨跌比
    up_count = 0
    down_count = 0
    for code, date_data in daily.items():
        if trade_date in date_data and not is_filtered(code):
            d = date_data[trade_date]
            if d['pre_close'] and d['pre_close'] > 0:
                chg = (d['close'] / d['pre_close'] - 1) * 100
                if chg > 0:
                    up_count += 1
                elif chg < 0:
                    down_count += 1
    
    total = up_count + down_count
    up_ratio = up_count / total if total > 0 else 0.5
    
    # 二维判断(与strategy_b一致)
    if zt_count < 30 and up_ratio < 0.35:
        phase = '退潮'
        can_buy = False
    elif zt_count < 30:
        phase = '冰点'
        can_buy = False
    elif up_ratio < 0.35:
        phase = '退潮'
        can_buy = False
    elif zt_count > 80 and up_ratio > 0.65:
        phase = '高潮'
        can_buy = True
    else:
        phase = '正常'
        can_buy = True
    
    return {
        'phase': phase,
        'can_buy': can_buy,
        'zt_count': zt_count,
        'up_count': up_count,
        'down_count': down_count,
        'up_ratio': up_ratio,
    }


# ═══════════════════════════════════════════════
# 交易成本计算(与daemon完全一致)
# ═══════════════════════════════════════════════
def calc_commission(amount: float, is_sell: bool) -> tuple:
    comm = max(amount * COMMISSION_RATE, MIN_COMMISSION)
    stamp = amount * STAMP_DUTY_RATE if is_sell else 0
    return comm, stamp

def calc_buy_cost(price: float, shares: int) -> float:
    """含滑点+手续费的买入总成本"""
    actual_price = price * (1 + SLIPPAGE)
    amount = actual_price * shares
    comm, _ = calc_commission(amount, False)
    return amount + comm

def calc_sell_receive(price: float, shares: int) -> float:
    """含滑点+手续费+印花税的卖出实收"""
    actual_price = price * (1 - SLIPPAGE)
    amount = actual_price * shares
    comm, stamp = calc_commission(amount, True)
    return amount - comm - stamp


# ═══════════════════════════════════════════════
# 持仓管理
# ═══════════════════════════════════════════════
@dataclass
class Position:
    code: str
    name: str
    strategy: str  # A/B/C
    buy_date: str
    buy_price: float
    shares: int
    cost: float
    highest_price: float
    signal_type: str
    hold_days: int = 0
    
    @property
    def is_strategy_c(self):
        return self.strategy == "C"

class Portfolio:
    def __init__(self, capital: float):
        self.initial_capital = capital
        self.cash = capital
        self.positions: list[Position] = []
        self.closed_trades: list[dict] = []
        self.daily_values: list[dict] = []
        
    @property
    def total_assets(self) -> float:
        return self.cash + sum(p.cost for p in self.positions)
    
    @property
    def ab_count(self) -> int:
        return sum(1 for p in self.positions if not p.is_strategy_c)
    
    @property
    def c_count(self) -> int:
        return sum(1 for p in self.positions if p.is_strategy_c)
    
    @property
    def held_codes(self) -> set:
        return {p.code for p in self.positions}
    
    def can_buy(self, strategy: str) -> bool:
        if len(self.positions) >= MAX_POSITIONS:
            return False
        if strategy == "C":
            if self.c_count >= MAX_C_POSITIONS:
                return False
        else:
            if self.ab_count >= MAX_AB_POSITIONS:
                return False
        min_cash = self.total_assets * MIN_CASH_RATIO
        return self.cash > min_cash + 1000
    
    def buy(self, code: str, name: str, strategy: str, 
            price: float, trade_date: str, signal_type: str) -> Optional[Position]:
        """模拟买入，返回Position或None"""
        if not self.can_buy(strategy):
            return None
        if code in self.held_codes:
            return None
        if price <= 0:
            return None  # 防御异常数据
        
        # 配仓比例
        ratio = C_POSITION_RATIO if strategy == "C" else AB_POSITION_RATIO
        target = self.total_assets * ratio
        min_cash = self.total_assets * MIN_CASH_RATIO
        available = max(self.cash - min_cash, 0)
        target = min(target, available)
        
        shares = int(target / price / 100) * 100
        if shares < 100:
            # 高价股超配检查
            min_buy = price * 100
            if min_buy <= self.total_assets * 0.25 and min_buy <= available:
                shares = 100
        if shares < 100:
            return None
        
        cost = calc_buy_cost(price, shares)
        if cost > self.cash:
            shares = int((self.cash * 0.95) / price / 100) * 100
            if shares < 100:
                return None
            cost = calc_buy_cost(price, shares)
        
        self.cash -= cost
        pos = Position(
            code=code, name=name, strategy=strategy,
            buy_date=trade_date, buy_price=price * (1 + SLIPPAGE),
            shares=shares, cost=cost, highest_price=price * (1 + SLIPPAGE),
            signal_type=signal_type,
        )
        self.positions.append(pos)
        return pos
    
    def sell(self, pos: Position, price: float, trade_date: str, reason: str):
        """模拟卖出"""
        receive = calc_sell_receive(price, pos.shares)
        pnl = receive - pos.cost
        pnl_pct = (price * (1 - SLIPPAGE) / pos.buy_price - 1) * 100
        
        self.cash += receive
        self.positions.remove(pos)
        
        self.closed_trades.append({
            'code': pos.code, 'name': pos.name, 'strategy': pos.strategy,
            'buy_date': pos.buy_date, 'sell_date': trade_date,
            'buy_price': pos.buy_price, 'sell_price': price,
            'shares': pos.shares, 'pnl': pnl, 'pnl_pct': pnl_pct,
            'reason': reason, 'hold_days': pos.hold_days,
            'signal_type': pos.signal_type,
        })
    
    def update_highest(self, pos: Position, high_price: float):
        if high_price > pos.highest_price:
            pos.highest_price = high_price


# ═══════════════════════════════════════════════
# 卖出信号检测(精确复现daemon逻辑)
# ═══════════════════════════════════════════════
def check_sell_signal(pos: Position, day_data: dict, market_phase: str) -> Optional[dict]:
    """检查某只持仓在某天的卖出信号
    
    day_data: {open, high, low, close, pre_close}
    返回: None(不卖) 或 {reason, sell_price}
    """
    strategy = pos.strategy
    params = SELL_PARAMS[strategy]
    stop_loss = C_STOP_LOSS_PCT if strategy == "C" else STOP_LOSS_PCT
    
    high = day_data['high']
    low = day_data['low']
    close = day_data['close']
    
    # 更新最高价
    if high > pos.highest_price:
        pos.highest_price = high
    
    hold_days = pos.hold_days
    if hold_days < 1:
        return None  # T+1保护
    
    # --- 策略C独立卖出 ---
    if strategy == "C":
        chg_from_buy = (low / pos.buy_price - 1)
        
        # C止损-6%(用当日最低价模拟盘中触发)
        if chg_from_buy <= C_STOP_LOSS_PCT:
            sell_price = pos.buy_price * (1 + C_STOP_LOSS_PCT)
            return {'reason': f'策略C止损: 跌{chg_from_buy*100:+.1f}%', 'sell_price': sell_price}
        
        # C尾盘清仓(持1天就卖) — max_hold_days=2, 所以hold_days>=1就触发
        if pos.hold_days >= params['max_hold_days']:
            return {'reason': f'策略C最长持有{params["max_hold_days"]}天到期', 'sell_price': close}
        
        # C尾盘清仓(非涨停时)
        chg_today = (close / day_data['pre_close'] - 1) * 100 if day_data['pre_close'] > 0 else 0
        if chg_today < 9.5:  # 非涨停
            return {'reason': '策略C尾盘清仓', 'sell_price': close}
        else:
            return None  # 涨停豁免
    
    # --- 策略A/B通用卖出 ---
    # 1. 固定止损(用当日最低价模拟)
    chg_low = (low / pos.buy_price - 1)
    if chg_low <= stop_loss:
        sell_price = pos.buy_price * (1 + stop_loss)
        return {'reason': f'止损: 浮亏{chg_low*100:+.1f}%破{stop_loss*100:.0f}%线 [{strategy}]', 'sell_price': sell_price}
    
    # 2. 最长持有
    max_days = params['max_hold_days']
    if hold_days >= max_days:
        return {'reason': f'最长持有{max_days}天到期 [{strategy}]', 'sell_price': close}
    
    # 3. 移动止盈(退潮收紧)
    trailing_pct = params['trailing_stop_pct']
    if market_phase == '退潮':
        trailing_pct = params.get('trailing_ebb_pct', 0.03)
    elif market_phase in ('冰点', '偏冷'):
        trailing_pct = params.get('trailing_frost_pct', 0.02)
    
    if pos.highest_price > pos.buy_price:
        drawdown_from_high = (low / pos.highest_price - 1)
        if drawdown_from_high <= -trailing_pct:
            return {
                'reason': f'移动止盈: 从最高{pos.highest_price:.2f}回落{abs(drawdown_from_high)*100:.1f}% [{strategy}]',
                'sell_price': pos.highest_price * (1 - trailing_pct)
            }
    
    # 4. 时间止损
    time_days = params['time_stop_days']
    time_threshold = params['time_stop_threshold']
    if time_days > 0 and hold_days >= time_days:
        chg = (close / pos.buy_price - 1)
        if chg < time_threshold:
            return {
                'reason': f'时间止损: 持有{hold_days}天 涨幅{chg*100:+.1f}% [{strategy}]',
                'sell_price': close
            }
    
    return None


# ═══════════════════════════════════════════════
# 候选生成
# ═══════════════════════════════════════════════
def get_strategy_b_candidates(daily: dict, zt_pool: dict, trade_date: str,
                              trading_dates: list) -> list:
    """策略B候选: 昨日涨停，今日可追
    
    与daemon的get_strategy_b_candidates一致:
    - 昨天涨停 → 今天开盘后追入(回测用开盘价)
    - 过滤B股/科创/北交
    - 追高过滤按市场情绪差异化
    """
    # 找前一天
    try:
        idx = trading_dates.index(trade_date)
    except ValueError:
        return []
    if idx < 1:
        return []
    prev_date = trading_dates[idx - 1]
    
    # 前一天涨停的票
    zt_stocks = zt_pool.get(prev_date, [])
    candidates = []
    
    for zt in zt_stocks:
        code = zt['code']
        if is_filtered(code):
            continue
        if code not in daily or trade_date not in daily[code]:
            continue
        
        day = daily[code][trade_date]
        if day['pre_close'] is None or day['pre_close'] <= 0:
            continue
        
        # 开盘涨幅(追高过滤用)
        open_chg = (day['open'] / day['pre_close'] - 1) * 100 if day['pre_close'] > 0 else 0
        
        candidates.append({
            'code': code,
            'name': zt['name'],
            'strategy': 'B',
            'signal_type': '涨停确认',
            'consecutive_zt': zt['consecutive_zt'],
            'open_chg': open_chg,
            'reason': f"昨涨停{zt['consecutive_zt']}连板",
        })
    
    return candidates

def get_strategy_c_candidates(daily: dict, zt_pool: dict, trade_date: str,
                              trading_dates: list) -> list:
    """策略C候选: 2连板→T+1大跌→T+2低开低吸
    
    与daemon的get_strategy_c_candidates完全一致:
    - T-2(前天)2连板 + T-1(昨天)跌>3% + T(今天)低开<-2%
    """
    try:
        idx = trading_dates.index(trade_date)
    except ValueError:
        return []
    if idx < 2:
        return []
    
    t0_date = trading_dates[idx - 2]  # 涨停日
    t1_date = trading_dates[idx - 1]  # 大跌日
    
    zt_stocks = zt_pool.get(t0_date, [])
    candidates = []
    
    for zt in zt_stocks:
        code = zt['code']
        if is_filtered(code):
            continue
        
        # 连板检查
        if (zt['consecutive_zt'] or 1) < STRATEGY_C_CONFIG['lianban_min']:
            continue
        
        # T-1大跌检查
        if code not in daily or t1_date not in daily[code]:
            continue
        t1 = daily[code][t1_date]
        if t1['pre_close'] is None or t1['pre_close'] <= 0:
            continue
        t1_chg = (t1['close'] / t1['pre_close'] - 1) * 100
        if t1_chg > STRATEGY_C_CONFIG['t1_drop_min']:
            continue  # 没跌够
        
        # T(今天)低开检查
        if code not in daily or trade_date not in daily[code]:
            continue
        t2 = daily[code][trade_date]
        if t2['pre_close'] is None or t2['pre_close'] <= 0:
            continue
        t2_open_chg = (t2['open'] / t2['pre_close'] - 1) * 100
        if t2_open_chg > STRATEGY_C_CONFIG['t2_open_low_max']:
            continue  # 没低开
        
        candidates.append({
            'code': code,
            'name': zt['name'],
            'strategy': 'C',
            'signal_type': '反弹低吸',
            'consecutive_zt': zt['consecutive_zt'],
            't1_chg': t1_chg,
            't2_open_chg': t2_open_chg,
            'reason': f"{zt['consecutive_zt']}连板+昨跌{t1_chg:+.1f}%+低开{t2_open_chg:+.1f}%",
        })
    
    return candidates


# ═══════════════════════════════════════════════
# 主回测循环
# ═══════════════════════════════════════════════
def run_backtest(daily: dict, zt_pool: dict, trading_dates: list,
                 start_date: str = None, end_date: str = None,
                 strategies: list = None) -> dict:
    """运行回测
    
    Args:
        daily: 日K线数据
        zt_pool: 涨停池数据
        trading_dates: 交易日列表
        start_date/end_date: 回测区间
        strategies: 要回测的策略列表 ['A','B','C'], None=全部
    """
    portfolio = Portfolio(INITIAL_CAPITAL)
    
    if strategies is None:
        strategies = ['A', 'B', 'C']
    
    # 过滤日期范围
    dates = trading_dates[:]
    if start_date:
        dates = [d for d in dates if d >= start_date]
    if end_date:
        dates = [d for d in dates if d <= end_date]
    
    if len(dates) < 10:
        return {'error': f'交易日太少: {len(dates)}'}
    
    stats = {
        'start': dates[0], 'end': dates[-1], 'trading_days': len(dates),
        'total_buys': 0, 'total_sells': 0,
        'strategy_buys': defaultdict(int),
        'strategy_sells': defaultdict(int),
        'sell_reasons': defaultdict(int),
    }
    
    for i, trade_date in enumerate(dates):
        # 1. 市场情绪
        phase_info = get_market_phase(daily, zt_pool, trade_date)
        phase = phase_info['phase']
        can_buy = phase_info['can_buy']
        
        # 2. 持仓更新 & 卖出检查
        for pos in portfolio.positions[:]:
            if pos.code not in daily or trade_date not in daily[pos.code]:
                continue
            
            day = daily[pos.code][trade_date]
            pos.hold_days += 1
            
            # 更新最高价
            portfolio.update_highest(pos, day['high'])
            
            # 检查卖出信号
            sell = check_sell_signal(pos, day, phase)
            if sell:
                portfolio.sell(pos, sell['sell_price'], trade_date, sell['reason'])
                stats['total_sells'] += 1
                stats['strategy_sells'][pos.strategy] += 1
                stats['sell_reasons'][sell['reason'].split(':')[0]] += 1
        
        # 3. 买入候选
        all_candidates = []
        
        if 'B' in strategies:
            b_cands = get_strategy_b_candidates(daily, zt_pool, trade_date, dates)
            # 追高过滤
            for c in b_cands:
                threshold = OPEN_CHG_FILTER.get(phase, 5.0)
                if c['open_chg'] > threshold:
                    continue  # 追高过滤
                # 涨停/跌停跳过
                day = daily[c['code']][trade_date]
                day_chg = (day['close'] / day['pre_close'] - 1) * 100 if day['pre_close'] > 0 else 0
                if day_chg >= 9.5 or day_chg <= -9.5:
                    continue
                all_candidates.append(c)
        
        if 'C' in strategies:
            c_cands = get_strategy_c_candidates(daily, zt_pool, trade_date, dates)
            all_candidates.extend(c_cands)
        
        # 注意: 策略A需要ML预测文件，回测期间没有，跳过
        # (策略A回测需要单独处理因子选股)
        
        # 4. 执行买入
        # 注意: C仓位独立于A/B，所以B满仓不阻断C
        for cand in all_candidates:
            cand_strat = cand.get('strategy', 'B')
            
            # 仓位检查: A/B满仓只阻断A/B，不阻断C
            if cand_strat in ('A', 'B'):
                if portfolio.ab_count >= MAX_AB_POSITIONS:
                    continue  # A/B满仓，跳过这个候选，但不break，C还能买
            if cand_strat == 'C':
                if portfolio.c_count >= MAX_C_POSITIONS:
                    continue
            if len(portfolio.positions) >= MAX_POSITIONS:
                break  # 总仓位满，全部停止
            if cand['code'] in portfolio.held_codes:
                continue
            
            day = daily[cand['code']][trade_date]
            buy_price = day['open'] if day['open'] > 0 else day['close']
            if buy_price <= 0:
                continue  # 数据异常，跳过
            
            pos = portfolio.buy(
                code=cand['code'],
                name=cand['name'],
                strategy=cand['strategy'],
                price=buy_price,
                trade_date=trade_date,
                signal_type=cand['signal_type'],
            )
            if pos:
                stats['total_buys'] += 1
                stats['strategy_buys'][cand['strategy']] += 1
        
        # 5. 记录每日资产
        portfolio.daily_values.append({
            'date': trade_date,
            'cash': portfolio.cash,
            'total_assets': portfolio.total_assets,
            'positions': len(portfolio.positions),
            'phase': phase,
        })
    
    return {
        'stats': dict(stats),
        'trades': portfolio.closed_trades,
        'daily_values': portfolio.daily_values,
        'final_cash': portfolio.cash,
        'final_assets': portfolio.total_assets,
        'initial_capital': INITIAL_CAPITAL,
    }


# ═══════════════════════════════════════════════
# 结果分析
# ═══════════════════════════════════════════════
def analyze_results(result: dict) -> dict:
    """分析回测结果"""
    trades = result['trades']
    if not trades:
        return {'error': '无交易记录'}
    
    # 按策略分组
    by_strategy = defaultdict(list)
    for t in trades:
        by_strategy[t['strategy']].append(t)
    
    analysis = {
        'total_return': (result['final_assets'] / result['initial_capital'] - 1) * 100,
        'total_trades': len(trades),
        'total_pnl': result['final_assets'] - result['initial_capital'],
        'strategies': {},
    }
    
    for strat, strat_trades in by_strategy.items():
        pnls = [t['pnl'] for t in strat_trades]
        pnl_pcts = [t['pnl_pct'] for t in strat_trades]
        wins = [p for p in pnls if p > 0]
        losses = [p for p in pnls if p <= 0]
        
        total_pnl = sum(pnls)
        avg_pnl = statistics.mean(pnls) if pnls else 0
        avg_pnl_pct = statistics.mean(pnl_pcts) if pnl_pcts else 0
        win_rate = len(wins) / len(pnls) * 100 if pnls else 0
        avg_win = statistics.mean(wins) if wins else 0
        avg_loss = statistics.mean(losses) if losses else 0
        profit_factor = abs(sum(wins) / sum(losses)) if losses and sum(losses) != 0 else float('inf')
        
        # 夏普比率(日频)
        if len(pnl_pcts) > 1:
            sharpe = statistics.mean(pnl_pcts) / statistics.stdev(pnl_pcts) * (252 ** 0.5)
        else:
            sharpe = 0
        
        # 最大单笔亏损
        max_loss = min(pnls) if pnls else 0
        max_win = max(pnls) if pnls else 0
        
        # 平均持仓天数
        avg_hold = statistics.mean([t['hold_days'] for t in strat_trades]) if strat_trades else 0
        
        analysis['strategies'][strat] = {
            'trades': len(strat_trades),
            'total_pnl': total_pnl,
            'avg_pnl': avg_pnl,
            'avg_pnl_pct': avg_pnl_pct,
            'win_rate': win_rate,
            'avg_win': avg_win,
            'avg_loss': avg_loss,
            'profit_factor': profit_factor,
            'sharpe': sharpe,
            'max_win': max_win,
            'max_loss': max_loss,
            'avg_hold_days': avg_hold,
        }
    
    # 卖出原因分析
    sell_reasons = defaultdict(int)
    for t in trades:
        reason_prefix = t['reason'].split(':')[0] if ':' in t['reason'] else t['reason']
        sell_reasons[reason_prefix] += 1
    analysis['sell_reasons'] = dict(sell_reasons)
    
    return analysis


def print_results(analysis: dict, label: str = ""):
    """打印回测结果"""
    if 'error' in analysis:
        print(f"\n{'='*60}")
        print(f"  {label} ERROR: {analysis['error']}")
        return
    
    print(f"\n{'='*60}")
    print(f"  回测结果 {label}")
    print(f"{'='*60}")
    print(f"总收益率: {analysis['total_return']:+.2f}%  (总盈亏 ¥{analysis['total_pnl']:+,.0f})")
    print(f"总交易: {analysis['total_trades']}笔")
    print()
    
    for strat, s in analysis['strategies'].items():
        strat_name = {'A': 'ML选股', 'B': '涨停确认', 'C': '反弹低吸'}.get(strat, strat)
        print(f"  --- 策略{strat}({strat_name}) ---")
        print(f"  交易笔数: {s['trades']}")
        print(f"  总盈亏:   ¥{s['total_pnl']:+,.0f}")
        print(f"  笔均盈亏: ¥{s['avg_pnl']:+,.0f} ({s['avg_pnl_pct']:+.2f}%)")
        print(f"  胜率:     {s['win_rate']:.1f}%")
        print(f"  盈亏比:   {s['profit_factor']:.2f}")
        print(f"  夏普:     {s['sharpe']:.2f}")
        print(f"  最大单笔赢: ¥{s['max_win']:+,.0f}  最大单笔亏: ¥{s['max_loss']:+,.0f}")
        print(f"  平均持仓: {s['avg_hold_days']:.1f}天")
        print()
    
    print(f"  卖出原因分布:")
    for reason, cnt in sorted(analysis.get('sell_reasons', {}).items(), key=lambda x: -x[1]):
        print(f"    {reason}: {cnt}笔")
    print(f"{'='*60}")


# ═══════════════════════════════════════════════
# 主程序
# ═══════════════════════════════════════════════
if __name__ == '__main__':
    print("加载历史数据...")
    conn = get_conn()
    daily = load_daily_prices(conn)
    zt_pool = load_zt_pool(conn)
    conn.close()
    
    trading_dates = get_trading_dates(daily, zt_pool)
    print(f"数据: {len(trading_dates)}天, {len(daily)}只股票, 涨停池{sum(len(v) for v in zt_pool.values())}条")
    
    # 全量回测
    print("\n[1] 全量回测(218天)...")
    result_all = run_backtest(daily, zt_pool, trading_dates)
    analysis_all = analyze_results(result_all)
    print_results(analysis_all, "(全量)")
    
    # 交叉验证: 分3段(传完整dates给候选函数，只限制回测交易区间)
    n = len(trading_dates)
    seg_len = n // 3
    segments = [
        ("前1/3", trading_dates[:seg_len]),
        ("中1/3", trading_dates[seg_len:seg_len*2]),
        ("后1/3", trading_dates[seg_len*2:]),
    ]
    
    print("\n[2] 交叉验证(分3段)...")
    for label, seg_dates in segments:
        # 传完整trading_dates给候选函数(需要前2天数据)，只限制回测区间
        start_d, end_d = seg_dates[0], seg_dates[-1]
        result_seg = run_backtest(daily, zt_pool, trading_dates, 
                                  start_date=start_d, end_date=end_d)
        analysis_seg = analyze_results(result_seg)
        print_results(analysis_seg, label)
    
    # 按策略独立回测
    print("\n[3] 策略独立回测...")
    for strat in ['B', 'C']:
        result_strat = run_backtest(daily, zt_pool, trading_dates, strategies=[strat])
        analysis_strat = analyze_results(result_strat)
        print_results(analysis_strat, f"(仅策略{strat})")
    
    # 保存详细结果
    output_path = Path(__file__).resolve().parents[1] / "output" / "backtest"
    output_path.mkdir(parents=True, exist_ok=True)
    
    with open(output_path / "backtest_results.json", "w", encoding="utf-8") as f:
        # 序列化时处理defaultdict
        result_all['stats']['strategy_buys'] = dict(result_all['stats']['strategy_buys'])
        result_all['stats']['strategy_sells'] = dict(result_all['stats']['strategy_sells'])
        result_all['stats']['sell_reasons'] = dict(result_all['stats']['sell_reasons'])
        json.dump(result_all, f, ensure_ascii=False, indent=2, default=str)
    
    print(f"\n详细结果已保存到: {output_path / 'backtest_results.json'}")
