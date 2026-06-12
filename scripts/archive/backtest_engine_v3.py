#!/usr/bin/env python3
"""
真实策略回测引擎 v3
- 复用trading_daemon.py的完整买卖逻辑
- 用历史日K线模拟(非简化版)
- 扣除真实交易成本(万2.5佣金+千1印花税+0.1%滑点)
- 支持参数敏感性测试

策略A(因子筛选):
  - 买入: 当日跌幅0.5%~5% + RSI<60 + 近期非连续大跌
  - 卖出: -8%止损 / 5天时间止损 / 7天最长 / 3%移动止盈
  
策略B(涨停确认):
  - 买入: 昨日涨停 + 今日开盘不跌(-2%以上)
  - 卖出: -8%止损 / 3天时间止损 / 5天最长 / 3%移动止盈
"""
import sqlite3
import math
import json
from datetime import datetime, timedelta
from collections import defaultdict

DB_PATH = "data/alpha_miner.db"

# ============ 交易成本 ============
COMMISSION_RATE = 0.00025   # 万2.5佣金(双边)
STAMP_TAX_RATE = 0.001     # 千1印花税(卖出)
SLIPPAGE = 0.001            # 0.1%滑点

# ============ 账户参数 ============
INITIAL_CASH = 20000
MAX_POSITIONS = 5
BUY_AMOUNT_MAX = 3000

# ============ 策略参数(可覆盖) ============
STOP_LOSS_PCT = -0.08
SELL_PARAMS_DEFAULT = {
    "A": {"time_stop_days": 5, "time_stop_threshold": 0.01, "max_hold_days": 7, "trailing_stop_pct": 0.03},
    "B": {"time_stop_days": 3, "time_stop_threshold": 0.01, "max_hold_days": 5, "trailing_stop_pct": 0.03},
}


def load_daily_prices():
    """加载全部日K线到内存(按日期索引)"""
    db = sqlite3.connect(DB_PATH)
    rows = db.execute("""
        SELECT stock_code, trade_date, open, high, low, close, pre_close, volume, amount
        FROM daily_price
        WHERE volume > 0
        ORDER BY trade_date, stock_code
    """).fetchall()
    db.close()
    
    by_date = defaultdict(list)
    by_stock = defaultdict(list)
    
    for code, date, o, h, l, c, pc, vol, amt in rows:
        if not all([o, h, l, c]):  # 跳过空数据
            continue
        row = {
            "code": code, "date": date,
            "open": float(o), "high": float(h), "low": float(l),
            "close": float(c), "pre_close": float(pc) if pc else float(c),
            "volume": float(vol), "amount": float(amt),
        }
        row["change_pct"] = (row["close"] / row["pre_close"] - 1) * 100 if row["pre_close"] > 0 else 0
        by_date[date].append(row)
        by_stock[code].append(row)
    
    return by_date, by_stock


def load_zt_pool(use_sim=True):
    """加载涨停池 — 优先用模拟涨停池(103天), 真实涨停池补25天"""
    db = sqlite3.connect(DB_PATH)
    zt_by_date = defaultdict(list)
    seen = set()
    
    if use_sim:
        # 模拟涨停池(103天, 精确率95.7%)
        rows = db.execute("""
            SELECT trade_date, stock_code, close_price, pre_close, change_pct, amount
            FROM sim_zt_pool
        """).fetchall()
        for date, code, close, pre_close, chg, amt in rows:
            zt_by_date[date].append({
                "code": code, "name": code,
                "consecutive_zt": 1,
                "amount": float(amt) if amt else 0,
                "industry": "",
            })
            seen.add((date, code))
    
    # 真实涨停池补(25天, 有连板等额外信息)
    rows = db.execute("""
        SELECT trade_date, stock_code, name, consecutive_zt, amount, industry
        FROM zt_pool
    """).fetchall()
    for date, code, name, cons_zt, amt, ind in rows:
        if (date, code) not in seen:
            zt_by_date[date].append({
                "code": code, "name": name,
                "consecutive_zt": int(cons_zt) if cons_zt else 1,
                "amount": float(amt) if amt else 0,
                "industry": ind or "",
            })
            seen.add((date, code))
        else:
            # 已在模拟池中, 更新连板信息
            for item in zt_by_date[date]:
                if item["code"] == code:
                    item["consecutive_zt"] = int(cons_zt) if cons_zt else 1
                    item["name"] = name
                    break
    
    db.close()
    return zt_by_date


def compute_rsi(closes, period=14):
    """计算RSI"""
    if len(closes) < period + 1:
        return 50
    gains = []
    losses = []
    for i in range(1, len(closes)):
        diff = closes[i] - closes[i-1]
        gains.append(max(diff, 0))
        losses.append(max(-diff, 0))
    
    if len(gains) < period:
        return 50
    
    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period
    
    if avg_loss == 0:
        return 100
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def compute_macd(closes, fast=12, slow=26, signal=9):
    """简化MACD判断"""
    if len(closes) < slow + signal:
        return "unknown"
    
    ema_fast = closes[0]
    ema_slow = closes[0]
    macd_line = []
    
    k_fast = 2 / (fast + 1)
    k_slow = 2 / (slow + 1)
    
    for c in closes:
        ema_fast = c * k_fast + ema_fast * (1 - k_fast)
        ema_slow = c * k_slow + ema_slow * (1 - k_slow)
        macd_line.append(ema_fast - ema_slow)
    
    if len(macd_line) < signal:
        return "unknown"
    
    # 当前MACD
    curr_macd = macd_line[-1]
    prev_macd = macd_line[-2] if len(macd_line) >= 2 else 0
    
    if curr_macd > 0 and prev_macd <= 0:
        return "金叉"
    elif curr_macd > 0:
        return "多头"
    elif curr_macd < 0 and prev_macd >= 0:
        return "死叉"
    else:
        return "空头"


class BacktestEngine:
    def __init__(self, sell_params=None, trailing_stop_pct=None):
        """
        sell_params: 可覆盖卖出参数
        trailing_stop_pct: 可覆盖移动止盈百分比(用于敏感性测试)
        """
        self.sell_params = sell_params or json.loads(json.dumps(SELL_PARAMS_DEFAULT))
        if trailing_stop_pct is not None:
            for strat in self.sell_params:
                self.sell_params[strat]["trailing_stop_pct"] = trailing_stop_pct
        
        self.cash = INITIAL_CASH
        self.positions = []  # [{code, name, buy_price, buy_date, shares, highest, strategy, signal_type}]
        self.trades = []     # [{code, name, buy_price, sell_price, shares, pnl, pnl_pct, hold_days, strategy, sell_reason, buy_date, sell_date}]
        self.daily_nav = []  # [{date, nav, cash, market_value}]
        self.completed_trades = []
    
    def get_stock_closes(self, by_stock, code, end_date, n=30):
        """获取某只股票到end_date为止的最近n天收盘价"""
        history = by_stock.get(code, [])
        closes = [r["close"] for r in history if r["date"] <= end_date]
        return closes[-n:] if len(closes) >= n else closes
    
    def get_stock_history(self, by_stock, code, end_date, n=120):
        """获取某只股票到end_date为止的最近n天K线"""
        history = by_stock.get(code, [])
        return [r for r in history if r["date"] <= end_date][-n:]
    
    # ===== 策略A: 因子筛选买入 =====
    def check_strategy_a(self, by_stock, code, today_data, trade_date):
        """
        模拟策略A: 当日跌幅0.5%~5% + RSI<60
        回测中无法用ML预测, 用以下规则替代:
        - 近5日跌幅>3%或当日跌幅>1%(有回调)
        - RSI<60(不超买)
        - 成交额>100万(有流动性)
        - 排除ST/科创板/北交所
        """
        code_str = str(code)
        if code_str.startswith(("688", "689", "8", "9")):
            return None
        if code_str.startswith("ST") or "ST" in code_str:
            return None
        
        chg = today_data["change_pct"]
        amt = today_data["amount"]
        
        # 成交额>100万
        if amt < 1_000_000:
            return None
        
        # 获取RSI
        closes = self.get_stock_closes(by_stock, code, trade_date, 20)
        rsi = compute_rsi(closes)
        
        # 买点1: 低吸 — 当天跌0.5%~5%, RSI<60
        if -5.0 <= chg <= -0.5 and rsi < 60:
            return {
                "signal_type": "ML低吸(策略A)",
                "signal_reason": f"回调{chg:+.1f}% RSI{rsi:.0f}",
                "strategy": "A",
            }
        
        # 买点2: 追涨 — 当天涨2%~6%, MACD多头/金叉
        macd = compute_macd(closes)
        if 2.0 <= chg <= 6.0 and macd in ("金叉", "多头"):
            return {
                "signal_type": "ML追涨(策略A)",
                "signal_reason": f"放量{chg:+.1f}% MACD{macd}",
                "strategy": "A",
            }
        
        return None
    
    # ===== 策略B: 涨停确认买入 =====
    def check_strategy_b(self, by_stock, zt_by_date, code, today_data, trade_date):
        """
        策略B: 昨日涨停 + 今日不跌
        """
        # 找昨天的涨停池
        yesterday = self.get_prev_trade_date(trade_date)
        if not yesterday:
            return None
        
        zt_list = zt_by_date.get(yesterday, [])
        zt_codes = {z["code"] for z in zt_list}
        
        if code not in zt_codes:
            return None
        
        # 今日开盘不跌(跌幅>-2%)
        chg = today_data["change_pct"]
        if chg < -2:
            return None
        
        # 成交额>100万
        if today_data["amount"] < 1_000_000:
            return None
        
        zt_info = next((z for z in zt_list if z["code"] == code), {})
        
        return {
            "signal_type": "涨停确认(策略B)",
            "signal_reason": f"昨涨停{zt_info.get('consecutive_zt',1)}连板 今日{chg:+.1f}%",
            "strategy": "B",
        }
    
    def get_prev_trade_date(self, trade_date):
        """获取前一个交易日"""
        if not hasattr(self, '_sorted_dates'):
            return None
        idx = self._date_index.get(trade_date, -1)
        if idx <= 0:
            return None
        return self._sorted_dates[idx - 1]
    
    # ===== 卖出信号(和trading_daemon完全一致) =====
    def check_sell(self, pos, today_data, trade_date):
        """和trading_daemon.py的check_sell_signals完全一致的逻辑"""
        price = today_data["close"]
        buy_price = pos["buy_price"]
        highest = pos["highest"]
        pnl_pct = (price / buy_price - 1)
        
        # 更新最高价
        if price > highest:
            pos["highest"] = price
            highest = price
        
        # T+1
        buy_date = pos["buy_date"]
        hold_days = (datetime.strptime(trade_date, "%Y-%m-%d") - 
                     datetime.strptime(buy_date, "%Y-%m-%d")).days
        if hold_days < 1:
            return None
        
        # 判断策略
        signal = pos.get("signal_type", "")
        strategy = "A" if "策略A" in signal or "ML" in signal else "B"
        params = self.sell_params[strategy]
        
        # 1. 固定止损
        if pnl_pct <= STOP_LOSS_PCT:
            return f"止损 {pnl_pct*100:+.1f}% [{strategy}]"
        
        # 2. 最长持有
        max_days = params["max_hold_days"]
        if hold_days >= max_days:
            return f"最长持有{max_days}天 {pnl_pct*100:+.1f}% [{strategy}]"
        
        # 3. 移动止盈
        trailing_pct = params["trailing_stop_pct"]
        drawdown = (price / highest - 1) if highest > 0 else 0
        if highest > buy_price and drawdown <= -trailing_pct:
            return f"移动止盈 从{highest:.2f}回落{abs(drawdown)*100:.1f}% [{strategy}]"
        
        # 4. 时间止损
        time_days = params["time_stop_days"]
        time_threshold = params["time_stop_threshold"]
        if hold_days >= time_days and pnl_pct < time_threshold:
            return f"时间止损 {hold_days}天 {pnl_pct*100:+.1f}% [{strategy}]"
        
        return None
    
    def buy(self, code, name, price, signal_type, signal_reason, strategy, trade_date):
        """执行买入"""
        # 滑点
        buy_price = price * (1 + SLIPPAGE)
        shares = int(BUY_AMOUNT_MAX / buy_price / 100) * 100  # 100股整数倍
        if shares < 100:
            shares = 100
        cost = buy_price * shares * (1 + COMMISSION_RATE)
        
        if cost > self.cash or len(self.positions) >= MAX_POSITIONS:
            return False
        
        self.cash -= cost
        self.positions.append({
            "code": code, "name": name,
            "buy_price": round(buy_price, 3),
            "buy_date": trade_date,
            "shares": shares,
            "highest": buy_price,
            "strategy": strategy,
            "signal_type": signal_type,
        })
        return True
    
    def sell(self, pos, price, reason, trade_date):
        """执行卖出"""
        sell_price = price * (1 - SLIPPAGE)
        proceeds = sell_price * pos["shares"] * (1 - COMMISSION_RATE + STAMP_TAX_RATE)
        pnl = proceeds - pos["buy_price"] * pos["shares"]
        pnl_pct = (sell_price / pos["buy_price"] - 1)
        hold_days = (datetime.strptime(trade_date, "%Y-%m-%d") - 
                     datetime.strptime(pos["buy_date"], "%Y-%m-%d")).days
        
        self.cash += proceeds
        self.completed_trades.append({
            "code": pos["code"], "name": pos["name"],
            "buy_price": pos["buy_price"], "sell_price": round(sell_price, 3),
            "shares": pos["shares"], "pnl": round(pnl, 2),
            "pnl_pct": round(pnl_pct * 100, 2),
            "hold_days": hold_days,
            "strategy": pos["strategy"],
            "signal_type": pos["signal_type"],
            "sell_reason": reason,
            "buy_date": pos["buy_date"],
            "sell_date": trade_date,
        })
        return pnl
    
    def calc_market_value(self, by_stock, trade_date):
        """计算持仓市值(用当日收盘价)"""
        mv = 0
        for pos in self.positions:
            history = by_stock.get(pos["code"], [])
            today_rows = [r for r in history if r["date"] == trade_date]
            if today_rows:
                mv += today_rows[0]["close"] * pos["shares"]
            else:
                mv += pos["buy_price"] * pos["shares"]  # 无数据用买入价
        return mv
    
    def run(self, by_date, by_stock, zt_by_date, start_date, end_date):
        """执行回测"""
        sorted_dates = sorted([d for d in by_date.keys() if start_date <= d <= end_date])
        self._sorted_dates = sorted_dates
        self._date_index = {d: i for i, d in enumerate(sorted_dates)}
        
        for trade_date in sorted_dates:
            day_stocks = {r["code"]: r for r in by_date[trade_date]}
            
            # Step 1: 检查卖出
            to_remove = []
            for pos in self.positions:
                if pos["code"] in day_stocks:
                    sell_reason = self.check_sell(pos, day_stocks[pos["code"]], trade_date)
                    if sell_reason:
                        pnl = self.sell(pos, day_stocks[pos["code"]]["close"], sell_reason, trade_date)
                        to_remove.append(pos)
            for pos in to_remove:
                self.positions.remove(pos)
            
            # Step 2: 检查买入
            if len(self.positions) < MAX_POSITIONS and self.cash >= 1000:
                held_codes = {p["code"] for p in self.positions}
                candidates = []
                
                # 策略B: 涨停确认
                for code, data in day_stocks.items():
                    if code in held_codes:
                        continue
                    signal = self.check_strategy_b(by_stock, zt_by_date, code, data, trade_date)
                    if signal:
                        candidates.append((code, data, signal))
                
                # 策略A: ML选股 (全A股LightGBM)
                for code, data in day_stocks.items():
                    if code in held_codes:
                        continue
                    if any(c[0] == code for c in candidates):  # 已在策略B候选中
                        continue
                    signal = self.check_strategy_a(by_stock, code, data, trade_date)
                    if signal:
                        candidates.append((code, data, signal))
                
                # 按策略B优先、成交额排序
                candidates.sort(key=lambda x: (0 if x[2]["strategy"] == "B" else 1, -x[1]["amount"]))
                
                for code, data, signal in candidates:
                    if len(self.positions) >= MAX_POSITIONS:
                        break
                    if self.cash < 1000:
                        break
                    name = f"{code}"
                    self.buy(code, name, data["close"], signal["signal_type"],
                            signal["signal_reason"], signal["strategy"], trade_date)
            
            # 记录每日净值
            mv = self.calc_market_value(by_stock, trade_date)
            nav = self.cash + mv
            self.daily_nav.append({
                "date": trade_date, "nav": round(nav, 2),
                "cash": round(self.cash, 2), "market_value": round(mv, 2),
                "positions": len(self.positions),
            })
        
        return self.generate_report()
    
    def generate_report(self):
        """生成回测报告"""
        if not self.completed_trades:
            return {"error": "无交易记录"}
        
        navs = [d["nav"] for d in self.daily_nav]
        total_return = (navs[-1] / INITIAL_CASH - 1) * 100
        
        # 最大回撤
        peak = navs[0]
        max_dd = 0
        for n in navs:
            if n > peak:
                peak = n
            dd = (peak - n) / peak
            if dd > max_dd:
                max_dd = dd
        
        # Sharpe Ratio (年化)
        if len(navs) > 1:
            daily_returns = [(navs[i]/navs[i-1] - 1) for i in range(1, len(navs))]
            avg_ret = sum(daily_returns) / len(daily_returns)
            std_ret = math.sqrt(sum((r - avg_ret)**2 for r in daily_returns) / len(daily_returns))
            sharpe = (avg_ret / std_ret * math.sqrt(252)) if std_ret > 0 else 0
        else:
            sharpe = 0
            daily_returns = []
        
        # 按策略统计
        by_strategy = defaultdict(list)
        for t in self.completed_trades:
            by_strategy[t["strategy"]].append(t)
        
        strategy_stats = {}
        for strat, trades in by_strategy.items():
            wins = [t for t in trades if t["pnl"] > 0]
            losses = [t for t in trades if t["pnl"] <= 0]
            total_pnl = sum(t["pnl"] for t in trades)
            avg_pnl = total_pnl / len(trades) if trades else 0
            avg_hold = sum(t["hold_days"] for t in trades) / len(trades) if trades else 0
            
            # 卖出原因分布
            sell_reasons = defaultdict(int)
            for t in trades:
                sell_reasons[t["sell_reason"].split()[0]] += 1
            
            strategy_stats[strat] = {
                "trades": len(trades),
                "wins": len(wins),
                "win_rate": round(len(wins) / len(trades) * 100, 1) if trades else 0,
                "total_pnl": round(total_pnl, 2),
                "avg_pnl": round(avg_pnl, 2),
                "avg_hold_days": round(avg_hold, 1),
                "avg_win": round(sum(t["pnl"] for t in wins) / len(wins), 2) if wins else 0,
                "avg_loss": round(sum(t["pnl"] for t in losses) / len(losses), 2) if losses else 0,
                "sell_reasons": dict(sell_reasons),
            }
        
        return {
            "total_return_pct": round(total_return, 2),
            "max_drawdown_pct": round(max_dd * 100, 2),
            "sharpe_ratio": round(sharpe, 3),
            "total_trades": len(self.completed_trades),
            "total_days": len(self.daily_nav),
            "final_nav": round(navs[-1], 2),
            "initial_cash": INITIAL_CASH,
            "strategy_stats": strategy_stats,
            "daily_returns": daily_returns,
        }


def print_report(report, label=""):
    """打印回测报告"""
    print("=" * 65)
    print(f"  回测报告 {label}")
    print("=" * 65)
    print()
    print(f"  初始资金: ¥{report['initial_cash']:,.0f}")
    print(f"  最终净值: ¥{report['final_nav']:,.2f}")
    print(f"  总收益率: {report['total_return_pct']:+.2f}%")
    print(f"  最大回撤: {report['max_drawdown_pct']:.2f}%")
    print(f"  Sharpe比: {report['sharpe_ratio']:.3f}")
    print(f"  回测天数: {report['total_days']}天")
    print(f"  总交易数: {report['total_trades']}笔")
    print()
    
    for strat, stats in sorted(report["strategy_stats"].items()):
        strat_name = "策略A(低吸/追涨)" if strat == "A" else "策略B(涨停确认)"
        print(f"  {strat_name}:")
        print(f"    交易: {stats['trades']}笔")
        print(f"    胜率: {stats['win_rate']}% ({stats['wins']}胜/{stats['trades']-stats['wins']}亏)")
        print(f"    总盈亏: ¥{stats['total_pnl']:+,.2f}")
        print(f"    平均盈亏: ¥{stats['avg_pnl']:+,.2f}")
        print(f"    平均盈利: ¥{stats['avg_win']:+,.2f}")
        print(f"    平均亏损: ¥{stats['avg_loss']:+,.2f}")
        print(f"    平均持有: {stats['avg_hold_days']}天")
        print(f"    卖出原因: {stats['sell_reasons']}")
        print()


def run_parameter_sensitivity():
    """参数敏感性测试: 2%/3%/4%/5%移动止盈对比"""
    print("加载数据...")
    by_date, by_stock = load_daily_prices()
    zt_by_date = load_zt_pool(use_sim=True)
    print(f"日K线: {len(by_date)}天, {len(by_stock)}只")
    print(f"涨停池(含模拟): {len(zt_by_date)}天")
    print()
    
    # 回测区间: 2025-07-01 ~ 2026-05-12 (最长可用区间)
    start = "2025-07-01"
    end = "2026-05-12"
    
    results = {}
    for trailing_pct in [0.02, 0.03, 0.04, 0.05]:
        label = f"移动止盈{trailing_pct*100:.0f}%"
        print(f"回测: {label}...")
        engine = BacktestEngine(trailing_stop_pct=trailing_pct)
        report = engine.run(by_date, by_stock, zt_by_date, start, end)
        results[trailing_pct] = report
        print_report(report, label)
        print("-" * 65)
        print()
    
    # 汇总对比
    print("=" * 65)
    print("  参数敏感性汇总")
    print("=" * 65)
    print(f"  {'参数':<12} {'收益率':<10} {'Sharpe':<10} {'最大回撤':<10} {'交易数':<8} {'策略A胜率':<10} {'策略B胜率'}")
    print(f"  {'-'*70}")
    for pct, r in results.items():
        a_wr = f"{r['strategy_stats']['A']['win_rate']}%" if 'A' in r['strategy_stats'] else "N/A"
        b_wr = f"{r['strategy_stats']['B']['win_rate']}%" if 'B' in r['strategy_stats'] else "N/A"
        print(f"  止盈{pct*100:.0f}%{' '*6} {r['total_return_pct']:+.2f}%{' '*4} {r['sharpe_ratio']:.3f}{' '*4} {r['max_drawdown_pct']:.2f}%{' '*5} {r['total_trades']}{' '*6} {a_wr:<10} {b_wr}")
    
    # 策略B独立统计
    print()
    print("=" * 65)
    print("  策略B(涨停确认)详细对比")
    print("=" * 65)
    for pct, r in results.items():
        if 'B' in r['strategy_stats']:
            s = r['strategy_stats']['B']
            print(f"  止盈{pct*100:.0f}%: {s['trades']}笔 胜率{s['win_rate']}% 总盈亏¥{s['total_pnl']:+,.2f} 均盈亏¥{s['avg_pnl']:+,.2f}")
    
    return results


if __name__ == "__main__":
    results = run_parameter_sensitivity()
