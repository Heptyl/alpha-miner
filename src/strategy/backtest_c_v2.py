"""
策略C v2 回测引擎

回测方法:
  1. 用 financial_snapshot_date 的财务数据做基本面评分
  2. 从 start_date 开始，每天检查技术面信号
  3. 满足条件→模拟买入（考虑交易成本0.125%）
  4. 按卖出规则持仓/平仓
  5. 统计PF/胜率/最大回撤/夏普

避免前视偏差:
  - 只用 financial_snapshot_date 之前的财务数据
  - 只用当日及之前的行情数据
"""
import logging
import sqlite3
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).parent.parent.parent / "data" / "alpha_miner.db"

# 交易成本
BUY_COST = 0.00025   # 买入万2.5
SELL_COST = 0.00075  # 卖出万2.5 + 印花税万5
TOTAL_COST = BUY_COST + SELL_COST  # 单边总成本0.1%

# 策略参数
MIN_SCORE = 60
MAX_HOLD_DAYS = 30
STOP_LOSS = -0.08
TARGET_PROFIT = 0.15
MAX_POSITIONS = 3
POSITION_SIZE = 10000  # 每只1万


@dataclass
class Position:
    stock_code: str
    buy_date: str
    buy_price: float
    shares: int
    score: int
    signals: list[str]
    strategy: str = "C"
    hold_days: int = 0


@dataclass
class Trade:
    stock_code: str
    buy_date: str
    buy_price: float
    sell_date: str
    sell_price: float
    shares: int
    pnl: float
    pnl_pct: float
    hold_days: int
    sell_reason: str
    score: int


@dataclass
class BacktestResult:
    trades: list[Trade] = field(default_factory=list)
    initial_capital: float = 100000.0
    final_capital: float = 100000.0
    max_capital: float = 100000.0
    min_capital: float = 100000.0
    max_drawdown: float = 0.0
    peak_capital: float = 100000.0

    @property
    def total_return(self) -> float:
        return (self.final_capital - self.initial_capital) / self.initial_capital

    @property
    def win_rate(self) -> float:
        if not self.trades:
            return 0.0
        wins = sum(1 for t in self.trades if t.pnl > 0)
        return wins / len(self.trades)

    @property
    def profit_factor(self) -> float:
        gross_profit = sum(t.pnl for t in self.trades if t.pnl > 0)
        gross_loss = abs(sum(t.pnl for t in self.trades if t.pnl < 0))
        return gross_profit / gross_loss if gross_loss > 0 else float('inf')

    @property
    def avg_pnl_pct(self) -> float:
        if not self.trades:
            return 0.0
        return sum(t.pnl_pct for t in self.trades) / len(self.trades)

    @property
    def sharpe_ratio(self) -> float:
        if not self.trades:
            return 0.0
        import math
        returns = [t.pnl_pct for t in self.trades]
        mean_r = sum(returns) / len(returns)
        var_r = sum((r - mean_r) ** 2 for r in returns) / len(returns)
        std_r = math.sqrt(var_r) if var_r > 0 else 0.001
        # 年化: 假设每笔持仓15天，一年约24笔
        annualized = mean_r * 24 / std_r if std_r > 0 else 0
        return annualized


def get_trading_days(start_date: str, end_date: str) -> list[str]:
    """获取交易日列表"""
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    try:
        rows = conn.execute("""
            SELECT DISTINCT trade_date FROM daily_price
            WHERE trade_date >= ? AND trade_date <= ?
            ORDER BY trade_date
        """, (start_date, end_date)).fetchall()
        return [r[0] for r in rows]
    finally:
        conn.close()


def get_daily_prices(stock_code: str, trade_date: str) -> Optional[dict]:
    """获取某只股票某天的行情"""
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    try:
        conn.row_factory = sqlite3.Row
        row = conn.execute("""
            SELECT trade_date, open, high, low, close, volume, amount
            FROM daily_price
            WHERE stock_code = ? AND trade_date = ?
        """, (stock_code, trade_date)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def get_prev_close(stock_code: str, trade_date: str) -> Optional[float]:
    """获取前一交易日收盘价"""
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    try:
        row = conn.execute("""
            SELECT close FROM daily_price
            WHERE stock_code = ? AND trade_date < ?
            ORDER BY trade_date DESC LIMIT 1
        """, (stock_code, trade_date)).fetchone()
        return row[0] if row else None
    finally:
        conn.close()


def get_n_day_prices(stock_code: str, end_date: str, n: int = 30) -> list[dict]:
    """获取end_date之前n天的日K数据"""
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    try:
        conn.row_factory = sqlite3.Row
        rows = conn.execute("""
            SELECT trade_date, open, high, low, close, volume, amount
            FROM daily_price
            WHERE stock_code = ? AND trade_date <= ?
            ORDER BY trade_date DESC LIMIT ?
        """, (stock_code, end_date, n)).fetchall()
        return [dict(r) for r in reversed(rows)]
    finally:
        conn.close()


def check_technical_backtest(stock_code: str, trade_date: str) -> dict:
    """
    回测用的技术面检查(不用实时数据，用历史日K)
    """
    rows = get_n_day_prices(stock_code, trade_date, n=30)

    if len(rows) < 20:
        return {"entry": False, "signals": [], "filters": ["数据不足"]}

    closes = [r["close"] for r in rows]
    volumes = [r["volume"] for r in rows]
    today_close = closes[-1]
    today_vol = volumes[-1]

    signals = []
    filters_passed = []

    # 过滤: 涨幅 > 7%
    if len(closes) >= 2:
        today_chg = (closes[-1] - closes[-2]) / closes[-2]
        if today_chg > 0.07:
            filters_passed.append(f"涨幅{today_chg*100:.1f}%")

    # 过滤: 连续3天大涨
    if len(closes) >= 4:
        big_up = sum(1 for i in range(1, 4)
                     if (closes[-i] - closes[-i-1]) / closes[-i-1] > 0.03)
        if big_up >= 3:
            filters_passed.append("连续大涨")

    if filters_passed:
        return {"entry": False, "signals": [], "filters": filters_passed}

    # 量比
    if len(volumes) >= 6:
        avg_vol_5 = sum(volumes[-6:-1]) / 5
        if avg_vol_5 > 0:
            vol_ratio = today_vol / avg_vol_5
            if vol_ratio >= 3:
                signals.append(f"量比{vol_ratio:.1f}")

    # 均线
    ma5 = sum(closes[-5:]) / 5 if len(closes) >= 5 else None
    ma20 = sum(closes[-20:]) / 20 if len(closes) >= 20 else None

    if ma5 and ma20:
        if closes[-2] < ma20 and today_close > ma20 and today_close > ma5:
            signals.append("突破MA20")
        if (today_vol < volumes[-2] and
                abs(today_close - ma20) / ma20 < 0.02):
            signals.append("缩量回踩MA20")

    # 趋势转多
    if ma5 and ma20 and ma5 > ma20:
        prev_ma5 = sum(closes[-6:-1]) / 5 if len(closes) >= 6 else None
        if prev_ma5 and prev_ma5 <= ma20:
            signals.append("趋势转多")

    return {"entry": len(signals) > 0, "signals": signals, "filters": []}


def run_backtest(
    start_date: str = "2025-01-02",
    end_date: str = "2025-12-31",
    financial_snapshot_date: str = "20241231",
    initial_capital: float = 100000.0,
    min_score: int = MIN_SCORE,
    max_positions: int = MAX_POSITIONS,
    max_hold_days: int = MAX_HOLD_DAYS,
    stop_loss: float = STOP_LOSS,
    target_profit: float = TARGET_PROFIT,
    position_size: float = POSITION_SIZE,
) -> BacktestResult:
    """
    运行策略C v2回测
    
    Args:
        start_date: 回测开始日期
        end_date: 回测结束日期
        financial_snapshot_date: 基本面快照(财务数据报告期)
        initial_capital: 初始资金
    """
    from src.scorer.fundamental_scorer import (
        score_profitability, score_growth, score_health, score_track,
        score_signals, get_financials, get_industry,
    )

    logger.info(f"[回测] 开始 {start_date}~{end_date}, 快照={financial_snapshot_date}")

    # 1. 获取基本面合格股票(使用快照数据)
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    try:
        codes = conn.execute("""
            SELECT DISTINCT fs.stock_code
            FROM financial_summary fs
            WHERE fs.report_date = ?
        """, (financial_snapshot_date,)).fetchall()
    finally:
        conn.close()

    qualified = []
    scoring_conn = sqlite3.connect(str(DB_PATH), timeout=30)
    scoring_conn.row_factory = sqlite3.Row
    try:
        for (code,) in codes:
            financials = get_financials(scoring_conn, code, limit=4)
            # 只用快照日期之前的数据
            financials = [f for f in financials
                          if f["report_date"] <= financial_snapshot_date]
            if not financials:
                continue

            a, _ = score_profitability(financials)
            b, _ = score_growth(financials)
            c, _ = score_health(financials)
            d, _ = score_track(scoring_conn, code, financials)
            # E信号在回测中跳过(历史数据不可靠)
            total = a + b + c + d
            if total >= min_score * 0.8:  # 放宽到80%（E信号0分时）
                qualified.append((code, total, a + b + c + d))
    finally:
        scoring_conn.close()

    qualified.sort(key=lambda x: x[1], reverse=True)
    logger.info(f"[回测] 基本面合格: {len(qualified)}只 (快照={financial_snapshot_date})")

    # 2. 逐日模拟交易
    trading_days = get_trading_days(start_date, end_date)
    if not trading_days:
        logger.error(f"[回测] 无交易日数据 {start_date}~{end_date}")
        return BacktestResult()

    logger.info(f"[回测] 交易日数: {len(trading_days)}")

    positions: list[Position] = []
    trades: list[Trade] = []
    capital = initial_capital
    cash = initial_capital
    result = BacktestResult(initial_capital=initial_capital)

    # 买过的股票冷却期（卖出后10天不再买）
    cooldown: dict[str, str] = {}

    # 评分合格的股票集合
    qualified_codes = {code for code, _, _ in qualified}

    for day_idx, today in enumerate(trading_days):
        # 更新持仓天数
        for pos in positions:
            pos.hold_days += 1

        # 检查卖出
        positions_to_close = []
        for pos in positions:
            quote = get_daily_prices(pos.stock_code, today)
            if not quote:
                continue

            current = quote["close"]
            chg = (current - pos.buy_price) / pos.buy_price

            sell_reason = None
            urgency = "medium"

            # 止损
            if chg <= stop_loss:
                sell_reason = f"止损{chg*100:+.1f}%"
                urgency = "high"

            # 目标收益减仓
            elif chg >= target_profit:
                sell_reason = f"目标{chg*100:+.1f}%"
                urgency = "low"

            # 时间止损
            elif pos.hold_days >= max_hold_days:
                sell_reason = f"持{pos.hold_days}天到期"
                urgency = "medium"

            if sell_reason:
                # 卖出
                sell_price = current * (1 - SELL_COST)
                buy_cost_total = pos.buy_price * pos.shares * (1 + BUY_COST)
                sell_proceeds = sell_price * pos.shares
                pnl = sell_proceeds - buy_cost_total
                pnl_pct = (sell_price / pos.buy_price - 1)

                trade = Trade(
                    stock_code=pos.stock_code,
                    buy_date=pos.buy_date,
                    buy_price=pos.buy_price,
                    sell_date=today,
                    sell_price=current,
                    shares=pos.shares,
                    pnl=pnl,
                    pnl_pct=pnl_pct,
                    hold_days=pos.hold_days,
                    sell_reason=sell_reason,
                    score=pos.score,
                )
                trades.append(trade)
                cash += sell_proceeds
                positions_to_close.append(pos)
                cooldown[pos.stock_code] = today

        for pos in positions_to_close:
            positions.remove(pos)

        # 检查买入（有持仓空位时）
        if len(positions) < max_positions and day_idx > 0:
            # 从合格股票中检查技术面
            for code, score, _ in qualified[:100]:  # 只看TOP100
                if code in [p.stock_code for p in positions]:
                    continue
                if code in cooldown:
                    cooldown_date = cooldown[code]
                    cooldown_expiry = (
                        datetime.strptime(cooldown_date, "%Y-%m-%d") +
                        timedelta(days=10)
                    ).strftime("%Y-%m-%d")
                    if today <= cooldown_expiry:
                        continue

                tech = check_technical_backtest(code, today)
                if tech["entry"]:
                    # T+1买入: 信号日在收盘确认, 次日开盘执行
                    # 更接近实盘(实盘也是信号确认后下一个交易周期买入)
                    if day_idx + 1 >= len(trading_days):
                        continue  # 没有下一个交易日了
                    next_day = trading_days[day_idx + 1]
                    next_quote = get_daily_prices(code, next_day)
                    if not next_quote or next_quote.get("open", 0) <= 0:
                        continue

                    buy_price = next_quote["open"] * (1 + BUY_COST)
                    shares = int(position_size / buy_price / 100) * 100  # 整手
                    if shares <= 0:
                        shares = 100

                    cost = buy_price * shares
                    if cost > cash:
                        continue

                    pos = Position(
                        stock_code=code,
                        buy_date=next_day,
                        buy_price=next_quote["open"],
                        shares=shares,
                        score=score,
                        signals=tech["signals"],
                    )
                    positions.append(pos)
                    cash -= cost

                    if len(positions) >= max_positions:
                        break

        # 更新资金曲线
        total_value = cash
        for pos in positions:
            q = get_daily_prices(pos.stock_code, today)
            if q:
                total_value += q["close"] * pos.shares

        result.peak_capital = max(result.peak_capital, total_value)
        drawdown = (result.peak_capital - total_value) / result.peak_capital
        result.max_drawdown = max(result.max_drawdown, drawdown)
        result.max_capital = max(result.max_capital, total_value)
        result.min_capital = min(result.min_capital, total_value)

    # 强制平仓剩余持仓
    last_day = trading_days[-1] if trading_days else end_date
    for pos in positions:
        q = get_daily_prices(pos.stock_code, last_day)
        if q:
            sell_price = q["close"]
            buy_cost_total = pos.buy_price * pos.shares * (1 + BUY_COST)
            sell_proceeds = sell_price * pos.shares * (1 - SELL_COST)
            pnl = sell_proceeds - buy_cost_total
            pnl_pct = (sell_price / pos.buy_price - 1)
            trades.append(Trade(
                stock_code=pos.stock_code,
                buy_date=pos.buy_date,
                buy_price=pos.buy_price,
                sell_date=last_day,
                sell_price=sell_price,
                shares=pos.shares,
                pnl=pnl,
                pnl_pct=pnl_pct,
                hold_days=pos.hold_days,
                sell_reason="回测结束平仓",
                score=pos.score,
            ))
            cash += sell_proceeds

    result.trades = trades
    result.final_capital = cash

    return result


def print_backtest_report(result: BacktestResult):
    """打印回测报告"""
    print("=" * 70)
    print("          策略C v2 回测报告")
    print("=" * 70)

    print(f"\n  初始资金: ¥{result.initial_capital:,.0f}")
    print(f"  最终资金: ¥{result.final_capital:,.0f}")
    print(f"  总收益:   {result.total_return*100:+.2f}%")
    print(f"  最大回撤: {result.max_drawdown*100:.2f}%")

    if result.trades:
        print(f"\n  --- 交易统计 ---")
        print(f"  总笔数:   {len(result.trades)}")
        print(f"  胜率:     {result.win_rate*100:.1f}%")
        print(f"  盈亏比PF: {result.profit_factor:.2f}")
        print(f"  笔均盈亏: {result.avg_pnl_pct*100:+.2f}%")
        print(f"  夏普比率: {result.sharpe_ratio:.2f}")

        # 按卖出原因分组
        reasons = defaultdict(list)
        for t in result.trades:
            reasons[t.sell_reason[:4]].append(t)

        print(f"\n  --- 卖出原因 ---")
        for reason, r_trades in sorted(reasons.items()):
            wins = sum(1 for t in r_trades if t.pnl > 0)
            avg_pnl = sum(t.pnl_pct for t in r_trades) / len(r_trades) * 100
            print(f"  {reason}: {len(r_trades)}笔, 胜率{wins/len(r_trades)*100:.0f}%, 均盈{avg_pnl:+.2f}%")

        # 按分数区间分组
        print(f"\n  --- 分数区间 ---")
        score_groups = {"60-65": [], "65-70": [], "70-75": [], "75+": []}
        for t in result.trades:
            if t.score >= 75:
                score_groups["75+"].append(t)
            elif t.score >= 70:
                score_groups["70-75"].append(t)
            elif t.score >= 65:
                score_groups["65-70"].append(t)
            else:
                score_groups["60-65"].append(t)

        for group, g_trades in score_groups.items():
            if not g_trades:
                continue
            wins = sum(1 for t in g_trades if t.pnl > 0)
            avg_pnl = sum(t.pnl_pct for t in g_trades) / len(g_trades) * 100
            print(f"  {group}分: {len(g_trades)}笔, 胜率{wins/len(g_trades)*100:.0f}%, 均盈{avg_pnl:+.2f}%")

        # TOP10最佳交易
        print(f"\n  --- TOP10最佳交易 ---")
        best = sorted(result.trades, key=lambda t: t.pnl_pct, reverse=True)[:10]
        for t in best:
            print(f"  {t.stock_code} {t.buy_date}→{t.sell_date} "
                  f"持{t.hold_days}天 {t.pnl_pct*100:+.2f}% "
                  f"分数{t.score} 原因:{t.sell_reason[:8]}")

        # TOP10最差交易
        print(f"\n  --- TOP10最差交易 ---")
        worst = sorted(result.trades, key=lambda t: t.pnl_pct)[:10]
        for t in worst:
            print(f"  {t.stock_code} {t.buy_date}→{t.sell_date} "
                  f"持{t.hold_days}天 {t.pnl_pct*100:+.2f}% "
                  f"分数{t.score} 原因:{t.sell_reason[:8]}")

    print("\n" + "=" * 70)


# ========== 运行 ==========
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    # 回测1: 2025全年，用2024年报快照
    print("\n>>> 回测1: 2025-01~2025-12, 快照=2024年报 <<<\n")
    result = run_backtest(
        start_date="2025-01-02",
        end_date="2025-12-31",
        financial_snapshot_date="20241231",
        initial_capital=100000,
    )
    print_backtest_report(result)

    # 回测2: 2025下半年
    print("\n>>> 回测2: 2025-07~2025-12, 快照=2024年报 <<<\n")
    result2 = run_backtest(
        start_date="2025-07-01",
        end_date="2025-12-31",
        financial_snapshot_date="20241231",
        initial_capital=100000,
    )
    print_backtest_report(result2)
