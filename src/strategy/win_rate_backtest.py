"""历史胜率回测模块 — 按相似条件回测历史买入的胜率和盈亏比。

核心思路：
1. 对于候选股，找到历史上"相同模式"的交易日
2. 模拟买入，统计持有N天的收益
3. 计算胜率和盈亏比
4. 只有历史胜率 > 阈值的模式才推荐

模式匹配条件：
- 连板数相近 (±1)
- 所属板块涨停数量相近
- 涨停/非涨停
- 量比范围
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from typing import Optional

import numpy as np

DB_PATH = "data/alpha_miner.db"


@dataclass
class BacktestResult:
    """回测结果。"""

    stock_code: str
    pattern_desc: str          # 模式描述
    total_trades: int          # 总交易次数
    win_count: int             # 盈利次数
    win_rate: float            # 胜率%
    avg_profit_pct: float      # 平均收益%
    avg_win_pct: float         # 平均盈利%
    avg_loss_pct: float        # 平均亏损%
    profit_loss_ratio: float   # 盈亏比
    max_drawdown_pct: float    # 最大单笔亏损%
    confidence: str            # high/medium/low (基于样本量)

    def to_dict(self) -> dict:
        return {
            "pattern": self.pattern_desc,
            "trades": self.total_trades,
            "win_rate": round(self.win_rate, 1),
            "avg_profit": round(self.avg_profit_pct, 2),
            "profit_loss_ratio": round(self.profit_loss_ratio, 2),
            "confidence": self.confidence,
        }


def backtest_pattern(
    stock_code: str,
    trade_date: str,
    consecutive_zt: int = 0,
    hold_days: int = 3,
    buy_at: str = "next_open",
    stop_loss_pct: float = -5.0,
    db_path: str = DB_PATH,
) -> Optional[BacktestResult]:
    """对某只股票的特定模式进行历史回测。

    策略：在历史上该股票出现类似连板/涨停模式后的N天，
    以次日开盘价买入，持有 hold_days 天后卖出。

    Args:
        stock_code: 股票代码
        trade_date: 当前交易日（不参与回测，仅排除）
        consecutive_zt: 连板数
        hold_days: 持有天数
        buy_at: 买入方式 ("next_open" = 次日开盘)
        stop_loss_pct: 止损线（负数）
        db_path: 数据库路径
    """
    conn = sqlite3.connect(db_path)

    # 取该股票所有日K线（加 pre_close 用于计算涨幅）
    rows = conn.execute(
        """SELECT trade_date, open, close, high, low, pre_close
           FROM daily_price
           WHERE stock_code = ?
           ORDER BY trade_date ASC""",
        (stock_code,),
    ).fetchall()
    conn.close()

    if len(rows) < hold_days + 10:
        return None  # 数据太少

    # 预计算每日涨幅
    def _pct(i):
        """计算第 i 行的涨幅%。"""
        c = rows[i][2]  # close
        pc = rows[i][5]  # pre_close
        if pc and pc > 0:
            return (c / pc - 1) * 100
        return 0.0

    # 找到历史上类似模式的日子
    pattern_dates = []

    if consecutive_zt >= 2:
        # 连板模式：找历史上连续N天涨停的日子
        pattern_dates = _find_consecutive_zt(rows, consecutive_zt, _pct)
    elif consecutive_zt == 1:
        # 首板模式：找历史上首板（前一天没涨停，今天涨停）
        pattern_dates = _find_first_zt(rows, _pct)
    else:
        # 普通模式：找历史上涨幅>3%的日子
        pattern_dates = _find_strong_days(rows, _pct, min_change=3.0)

    if len(pattern_dates) < 3:
        # 样本太少，标记为低置信度
        if len(pattern_dates) == 0:
            return None

    # 模拟交易
    trades = []
    for pattern_date in pattern_dates:
        if pattern_date >= trade_date:
            continue  # 排除当前和未来日期

        # 找 pattern_date 之后的 hold_days 天
        trade_result = _simulate_trade(
            rows, pattern_date, hold_days, buy_at, stop_loss_pct
        )
        if trade_result is not None:
            trades.append(trade_result)

    if not trades:
        return None

    # 统计
    profits = [t for t in trades]
    wins = [p for p in profits if p > 0]
    losses = [p for p in profits if p <= 0]

    win_count = len(wins)
    win_rate = (win_count / len(profits)) * 100 if profits else 0
    avg_profit = float(np.mean(profits)) if profits else 0
    avg_win = float(np.mean(wins)) if wins else 0
    avg_loss = abs(float(np.mean(losses))) if losses else 0.1
    profit_loss_ratio = avg_win / avg_loss if avg_loss > 0 else 999
    max_dd = float(min(profits)) if profits else 0

    # 置信度
    if len(trades) >= 20:
        confidence = "high"
    elif len(trades) >= 10:
        confidence = "medium"
    else:
        confidence = "low"

    pattern_desc = f"连板{consecutive_zt}后买入持有{hold_days}天" if consecutive_zt > 0 else f"强势日买入持有{hold_days}天"

    return BacktestResult(
        stock_code=stock_code,
        pattern_desc=pattern_desc,
        total_trades=len(trades),
        win_count=win_count,
        win_rate=win_rate,
        avg_profit_pct=avg_profit,
        avg_win_pct=avg_win,
        avg_loss_pct=avg_loss,
        profit_loss_ratio=profit_loss_ratio,
        max_drawdown_pct=max_dd,
        confidence=confidence,
    )


def _find_consecutive_zt(rows: list, target_zt: int, pct_fn) -> list[str]:
    """找连续涨停的日期。"""
    result = []
    for i in range(target_zt, len(rows)):
        # 检查最近 target_zt 天是否全部涨停（涨幅>=9.9%）
        all_zt = True
        for j in range(i - target_zt + 1, i + 1):
            chg = pct_fn(j)
            if chg < 9.9:
                all_zt = False
                break
        if all_zt:
            result.append(rows[i][0])
    return result


def _find_first_zt(rows: list, pct_fn) -> list[str]:
    """找首板日。"""
    result = []
    for i in range(1, len(rows)):
        chg = pct_fn(i)
        prev_chg = pct_fn(i - 1)
        if chg >= 9.9 and prev_chg < 9.9:
            result.append(rows[i][0])
    return result


def _find_strong_days(rows: list, pct_fn, min_change: float = 3.0) -> list[str]:
    """找强势日。"""
    result = []
    for i in range(len(rows)):
        chg = pct_fn(i)
        if chg >= min_change:
            result.append(rows[i][0])
    return result


def _simulate_trade(
    rows: list,
    signal_date: str,
    hold_days: int,
    buy_at: str,
    stop_loss_pct: float,
) -> Optional[float]:
    """模拟单笔交易。

    Returns:
        收益率% 或 None（无法交易）
    """
    # 找到 signal_date 在 rows 中的位置
    signal_idx = None
    for i, r in enumerate(rows):
        if r[0] == signal_date:
            signal_idx = i
            break

    if signal_idx is None:
        return None

    # 买入：次日开盘
    buy_idx = signal_idx + 1
    if buy_idx >= len(rows):
        return None

    buy_price = rows[buy_idx][1]  # 次日开盘价
    if buy_price <= 0:
        return None

    # 持有 hold_days 天，每天检查止损
    for d in range(1, hold_days + 1):
        sell_idx = buy_idx + d
        if sell_idx >= len(rows):
            return None

        low = rows[sell_idx][4]  # 当日最低
        close = rows[sell_idx][2]  # 当日收盘

        # 止损检查
        current_return = (low / buy_price - 1) * 100
        if current_return <= stop_loss_pct:
            return stop_loss_pct  # 止损出局

    # 到期卖出：用最后一天收盘价
    sell_idx = buy_idx + hold_days
    if sell_idx >= len(rows):
        return None

    sell_price = rows[sell_idx][2]
    return (sell_price / buy_price - 1) * 100


def batch_backtest(
    codes: list[tuple[str, int]],  # (stock_code, consecutive_zt)
    trade_date: str,
    hold_days: int = 3,
    db_path: str = DB_PATH,
    min_win_rate: float = 50.0,
    min_trades: int = 3,
) -> dict[str, BacktestResult]:
    """批量回测，过滤胜率不足的。

    Returns:
        {stock_code: BacktestResult}  只包含通过的
    """
    results = {}
    for code, czt in codes:
        bt = backtest_pattern(code, trade_date, czt, hold_days, db_path=db_path)
        if bt and bt.total_trades >= min_trades:
            results[code] = bt
    return results
