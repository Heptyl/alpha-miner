"""逐笔交易模拟引擎。

核心设计:
1. T+1 限制: 今天买的明天才能卖
2. 涨跌停限制: 涨停无法买入，跌停无法卖出
3. 时间隔离: 只用 as_of 之前的数据判断
4. 仓位约束: 遵守 PositionRule
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

import pandas as pd

from src.data.storage import Storage
from src.drift.regime import RegimeDetector, RegimeInfo
from src.factors.registry import FactorRegistry
from src.strategy.schema import (
    EntryRule, ExitRule, Strategy, StrategyReport, Trade,
)


@dataclass
class HoldingInfo:
    """当前持仓信息。"""
    stock_code: str
    stock_name: str = ""
    entry_date: str = ""
    entry_price: float = 0.0
    entry_reason: str = ""
    regime_at_entry: str = ""
    emotion_at_entry: str = ""
    peak_price: float = 0.0  # 持仓期间最高价(用于移动止损)


class BacktestEngine:
    """逐笔交易模拟引擎。"""

    def __init__(self, db: Storage):
        self.db = db
        self.registry = FactorRegistry()
        self.regime_detector = RegimeDetector(db)

    def run(
        self,
        strategy: Strategy,
        start_date: str,
        end_date: str,
        universe_source: str = "zt_pool",
        initial_capital: float = 1_000_000,
    ) -> StrategyReport:
        """运行策略回测。"""
        trade_dates = self._get_trade_dates(start_date, end_date)
        holdings: dict[str, HoldingInfo] = {}
        all_trades: list[Trade] = []
        capital = initial_capital
        peak_capital = initial_capital
        max_drawdown = 0.0

        for i, date in enumerate(trade_dates):
            as_of = datetime.strptime(date, "%Y-%m-%d").replace(hour=15)

            # ── Step 1: 检查出场 (只检查 T+1 之前买入的) ──
            codes_to_exit: list[tuple[str, str]] = []
            for code, holding in list(holdings.items()):
                # T+1: 买入当天不能卖
                if holding.entry_date == date:
                    continue
                exit_reason = self._check_exit(
                    strategy.exit, code, date, holding, as_of
                )
                if exit_reason:
                    codes_to_exit.append((code, exit_reason))

            for code, reason in codes_to_exit:
                holding = holdings.pop(code)
                trade = self._execute_exit(holding, date, reason, as_of)
                all_trades.append(trade)

            # ── Step 2: 检查入场 ──
            if len(holdings) < strategy.position.max_holdings:
                universe = self._get_universe(date, universe_source, as_of)
                if universe:
                    # Regime 过滤
                    regime_info = self.regime_detector.detect(as_of)
                    if strategy.entry.regime_filter:
                        if regime_info.regime not in strategy.entry.regime_filter:
                            universe = []  # regime 不匹配，跳过

                    for code in universe:
                        if code in holdings:
                            continue
                        if len(holdings) >= strategy.position.max_holdings:
                            break

                        entry_match = self._check_entry(
                            strategy.entry, code, date, as_of
                        )
                        if entry_match:
                            # 涨跌停检查: 涨停无法买入
                            if self._is_limit_up(code, date, as_of):
                                continue

                            holding = self._execute_entry(
                                code, date, strategy, regime_info, as_of
                            )
                            if holding:
                                holdings[code] = holding

            # ── 更新持仓峰值和回撤 ──
            if holdings:
                for code, holding in holdings.items():
                    price = self._get_price(code, date, as_of)
                    if price and price > holding.peak_price:
                        holding.peak_price = price

            current_value = self._calc_portfolio_value(
                capital, holdings, date, as_of
            )
            peak_capital = max(peak_capital, current_value)
            if peak_capital > 0:
                dd = (peak_capital - current_value) / peak_capital * 100
                max_drawdown = max(max_drawdown, dd)

        # 结束: 强制平仓
        last_date = trade_dates[-1] if trade_dates else end_date
        for code, holding in list(holdings.items()):
            trade = self._execute_exit(
                holding, last_date, "backtest_end",
                datetime.strptime(last_date, "%Y-%m-%d").replace(hour=15)
            )
            all_trades.append(trade)

        return self._build_report(
            strategy.name, start_date, end_date, all_trades, max_drawdown
        )

    # ── 入场检查 ─────────────────────────────────────────

    def _check_entry(self, entry: EntryRule, code: str, date: str,
                     as_of: datetime) -> bool:
        """检查一只股票是否满足所有入场条件。"""
        for cond in entry.conditions:
            factor_name = cond["factor"]
            op = cond["op"]
            threshold = cond["value"]

            factor_value = self._get_factor_value(factor_name, code, date, as_of)
            if factor_value is None:
                return False
            if not self._compare(factor_value, op, threshold):
                return False

        return True

    # ── 出场检查 ─────────────────────────────────────────

    def _check_exit(self, exit_rule: ExitRule, code: str, date: str,
                    holding: HoldingInfo, as_of: datetime) -> Optional[str]:
        """检查是否触发出场。返回出场原因或 None。"""
        current_price = self._get_price(code, date, as_of)
        if current_price is None:
            return None

        # 跌停无法卖出
        if self._is_limit_down(code, date, as_of):
            return None

        return_pct = (current_price - holding.entry_price) / holding.entry_price * 100
        hold_days = self._count_trade_days(holding.entry_date, date)

        # 止盈
        if return_pct >= exit_rule.take_profit_pct:
            return f"take_profit:{return_pct:.1f}%"

        # 止损
        if return_pct <= exit_rule.stop_loss_pct:
            return f"stop_loss:{return_pct:.1f}%"

        # 移动止损
        if exit_rule.trailing_stop_pct is not None and holding.peak_price > 0:
            draw_from_peak = (holding.peak_price - current_price) / holding.peak_price * 100
            if draw_from_peak >= exit_rule.trailing_stop_pct:
                return f"trailing_stop:-{draw_from_peak:.1f}%"

        # 时间止损
        if hold_days >= exit_rule.max_hold_days:
            return f"max_hold:{hold_days}d"

        # 条件出场
        for cond in exit_rule.exit_conditions:
            factor_value = self._get_factor_value(
                cond["factor"], code, date, as_of
            )
            if factor_value is not None:
                if self._compare(factor_value, cond["op"], cond["value"]):
                    reason = cond.get("reason", cond["factor"])
                    return f"condition:{reason}"

        return None

    # ── 执行买入/卖出 ────────────────────────────────────

    def _execute_entry(self, code: str, date: str, strategy: Strategy,
                       regime_info: RegimeInfo, as_of: datetime) -> Optional[HoldingInfo]:
        """执行买入，返回 HoldingInfo 或 None。"""
        # 次日开盘价买入
        next_date = self._get_next_trade_date(date)
        if not next_date:
            return None
        entry_price = self._get_open_price(code, next_date, as_of)
        if entry_price is None:
            # fallback: 用当天收盘价
            entry_price = self._get_price(code, date, as_of)
        if entry_price is None or entry_price <= 0:
            return None

        # timing 约束: next_open_if_gap_lt_N
        if strategy.entry.timing.startswith("next_open_if_gap_lt_"):
            import re
            m = re.search(r"(\d+)", strategy.entry.timing)
            if m:
                gap_pct = float(m.group(1))
                prev_close = self._get_price(code, date, as_of)
                if prev_close and prev_close > 0:
                    gap = (entry_price - prev_close) / prev_close * 100
                    if gap >= gap_pct:
                        return None

        return HoldingInfo(
            stock_code=code,
            entry_date=next_date,
            entry_price=entry_price,
            entry_reason=str(strategy.entry.conditions),
            regime_at_entry=regime_info.regime if regime_info else "",
            peak_price=entry_price,
        )

    def _execute_exit(self, holding: HoldingInfo, date: str,
                      reason: str, as_of: datetime) -> Trade:
        """执行卖出，返回 Trade。"""
        exit_price = self._get_price(holding.entry_price, date, as_of) if False else 0.0
        exit_price = self._get_price(holding.stock_code, date, as_of)
        if exit_price is None:
            exit_price = holding.entry_price  # 无数据时按原价退出

        return_pct = (exit_price - holding.entry_price) / holding.entry_price * 100
        hold_days = self._count_trade_days(holding.entry_date, date)

        return Trade(
            strategy_name="",  # 由 _build_report 填充
            stock_code=holding.stock_code,
            entry_date=holding.entry_date,
            entry_price=holding.entry_price,
            entry_reason=holding.entry_reason,
            exit_date=date,
            exit_price=exit_price,
            exit_reason=reason,
            return_pct=round(return_pct, 2),
            hold_days=hold_days,
            regime_at_entry=holding.regime_at_entry,
        )

    # ── 辅助方法 ─────────────────────────────────────────

    def _get_factor_value(self, factor_name: str, code: str,
                          date: str, as_of: datetime) -> Optional[float]:
        """获取某只股票在某日的因子值。"""
        df = self.db.query(
            "factor_values", as_of,
            where="factor_name = ? AND stock_code = ? AND trade_date = ?",
            params=(factor_name, code, date),
        )
        if df.empty:
            return None
        return float(df.iloc[-1]["factor_value"])

    def _get_price(self, code: str, date: str,
                   as_of: datetime) -> Optional[float]:
        """获取某日收盘价。"""
        df = self.db.query(
            "daily_price", as_of,
            where="stock_code = ? AND trade_date = ?",
            params=(code, date),
        )
        if df.empty:
            return None
        return float(df.iloc[-1]["close"])

    def _get_open_price(self, code: str, date: str,
                        as_of: datetime) -> Optional[float]:
        """获取某日开盘价。"""
        df = self.db.query(
            "daily_price", as_of,
            where="stock_code = ? AND trade_date = ?",
            params=(code, date),
        )
        if df.empty:
            return None
        return float(df.iloc[-1]["open"])

    def _is_limit_up(self, code: str, date: str, as_of: datetime) -> bool:
        """判断是否涨停(收盘价 == 最高价 且 涨幅 >= 9.8%)。"""
        df = self.db.query(
            "daily_price", as_of,
            where="stock_code = ? AND trade_date = ?",
            params=(code, date),
        )
        if df.empty:
            return False
        row = df.iloc[-1]
        close = float(row["close"])
        high = float(row["high"])
        pre_close = float(row.get("pre_close", close))
        if pre_close <= 0:
            return False
        pct = (close - pre_close) / pre_close * 100
        # 涨停: 收盘=最高 且 涨幅>=9.8% (考虑ST等)
        return close == high and pct >= 9.8

    def _is_limit_down(self, code: str, date: str, as_of: datetime) -> bool:
        """判断是否跌停(收盘价 == 最低价 且 跌幅 <= -9.8%)。"""
        df = self.db.query(
            "daily_price", as_of,
            where="stock_code = ? AND trade_date = ?",
            params=(code, date),
        )
        if df.empty:
            return False
        row = df.iloc[-1]
        close = float(row["close"])
        low = float(row["low"])
        pre_close = float(row.get("pre_close", close))
        if pre_close <= 0:
            return False
        pct = (close - pre_close) / pre_close * 100
        return close == low and pct <= -9.8

    def _get_universe(self, date: str, source: str,
                      as_of: datetime) -> list[str]:
        """获取候选股票池。"""
        df = self.db.query(
            source, as_of,
            where="trade_date = ?", params=(date,),
        )
        if df.empty:
            return []
        if "stock_code" in df.columns:
            return df["stock_code"].unique().tolist()
        return []

    def _compare(self, value: float, op: str, threshold: float) -> bool:
        """比较运算。"""
        ops = {
            ">=": lambda a, b: a >= b,
            "<=": lambda a, b: a <= b,
            ">": lambda a, b: a > b,
            "<": lambda a, b: a < b,
            "==": lambda a, b: abs(a - b) < 1e-6,
        }
        return ops.get(op, lambda a, b: False)(value, threshold)

    def _get_trade_dates(self, start: str, end: str) -> list[str]:
        """获取区间内的交易日列表。"""
        try:
            end_dt = datetime.strptime(end, "%Y-%m-%d")
            start_dt = datetime.strptime(start, "%Y-%m-%d")
            lookback = (end_dt - start_dt).days + 30
        except ValueError:
            return []

        df = self.db.query_range(
            "daily_price", end_dt, lookback_days=lookback,
        )
        if df.empty:
            return []
        dates = sorted(df["trade_date"].unique().tolist())
        return [d for d in dates if start <= d <= end]

    def _get_next_trade_date(self, date: str) -> Optional[str]:
        """获取下一个交易日。"""
        dt = datetime.strptime(date, "%Y-%m-%d")
        df = self.db.query_range("daily_price", dt, lookback_days=1)
        if df.empty:
            return None
        dates = sorted(df["trade_date"].unique().tolist())
        for d in dates:
            if d > date:
                return d
        # 尝试往后多看几天
        df2 = self.db.query_range("daily_price", dt, lookback_days=10)
        if df2.empty:
            return None
        dates2 = sorted(df2["trade_date"].unique().tolist())
        for d in dates2:
            if d > date:
                return d
        return None

    def _count_trade_days(self, d1: str, d2: str) -> int:
        """计算两个日期之间的交易日数(不含 d1)。"""
        try:
            end_dt = datetime.strptime(d2, "%Y-%m-%d")
        except ValueError:
            return 0
        df = self.db.query_range("daily_price", end_dt, lookback_days=30)
        if df.empty:
            return 0
        dates = sorted(df["trade_date"].unique().tolist())
        count = sum(1 for d in dates if d1 < d <= d2)
        return count

    def _calc_portfolio_value(self, capital: float, holdings: dict,
                              date: str, as_of: datetime) -> float:
        """计算当前组合市值。"""
        value = capital
        for code, holding in holdings.items():
            price = self._get_price(code, date, as_of)
            if price:
                value += price / holding.entry_price * holding.entry_price  # 简化
        return value

    def _build_report(self, name: str, start: str, end: str,
                      trades: list[Trade], max_drawdown: float = 0.0) -> StrategyReport:
        """从交易列表构建回测报告。"""
        report = StrategyReport(
            strategy_name=name,
            backtest_start=start,
            backtest_end=end,
            total_trades=len(trades),
            trades=trades,
        )
        if not trades:
            return report

        # 填充 strategy_name
        for t in trades:
            t.strategy_name = name

        returns = [t.return_pct for t in trades]
        wins = [r for r in returns if r > 0]
        losses = [r for r in returns if r <= 0]

        report.win_count = len(wins)
        report.loss_count = len(losses)
        report.win_rate = len(wins) / len(returns) if returns else 0
        report.avg_win_pct = sum(wins) / len(wins) if wins else 0
        report.avg_loss_pct = sum(losses) / len(losses) if losses else 0
        report.profit_loss_ratio = (
            abs(report.avg_win_pct / report.avg_loss_pct)
            if report.avg_loss_pct != 0 else float('inf')
        )
        report.max_drawdown_pct = round(max_drawdown, 2)

        # 最大连亏
        max_consec = 0
        cur_consec = 0
        for r in returns:
            if r <= 0:
                cur_consec += 1
                max_consec = max(max_consec, cur_consec)
            else:
                cur_consec = 0
        report.max_consecutive_loss = max_consec

        # 总收益(简单累加每笔收益率)
        report.total_return_pct = round(sum(returns), 2)

        # 夏普比率(简化: mean/std * sqrt(252))
        if len(returns) > 1:
            import numpy as np
            arr = np.array(returns)
            if arr.std() > 0:
                report.sharpe_ratio = round(
                    float(arr.mean() / arr.std() * math.sqrt(252)), 2
                )

        # 按 regime 分组统计
        regime_groups: dict[str, list[float]] = {}
        for t in trades:
            r = t.regime_at_entry or "unknown"
            regime_groups.setdefault(r, []).append(t.return_pct)

        for r, rets in regime_groups.items():
            w = [x for x in rets if x > 0]
            report.regime_stats[r] = {
                "trades": len(rets),
                "win_rate": round(len(w) / len(rets), 3) if rets else 0,
                "avg_return": round(sum(rets) / len(rets), 2) if rets else 0,
            }

        return report
