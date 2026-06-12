"""backtest_b_exit_params.py — 策略B退出参数回测

对历史策略B持仓, 在同一买入点模拟不同退出规则:
  - trailing 止盈: 2%, 3%, 5%, 8% (从最高close回落)
  - 固定持有: 2天, 3天

输出每条规则的: 交易数, 胜率, 平均收益, 中位数收益, 最大单笔亏损, 期望值
特别标注 中兴通讯 000063 案例。

用法:
    python scripts/backtest_b_exit_params.py
    python scripts/backtest_b_exit_params.py --trailing "2,3,5,8" --hold-days "2,3"
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path
from datetime import datetime

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_ROOT / "data" / "alpha_miner.db"


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


def get_strategy_b_positions(conn: sqlite3.Connection):
    """获取所有策略B的历史持仓(含已关闭)"""
    rows = conn.execute("""
        SELECT p.*, t.trade_date as buy_trade_date
        FROM daemon_positions p
        LEFT JOIN daemon_trades t ON t.code = p.code
            AND t.action = 'buy'
            AND t.trade_date = p.buy_date
            AND t.signal_type = p.signal_type
        WHERE (p.signal_type LIKE '%B%'
               OR p.signal_type LIKE '%回踩低吸%'
               OR p.signal_type LIKE '%低开反弹%'
               OR p.signal_type LIKE '%暴跌日狙击%'
               OR p.signal_type IN ('涨停低吸', '板块补涨', '涨停确认'))
        ORDER BY p.buy_date, p.code
    """).fetchall()
    return rows


def get_daily_prices(conn: sqlite3.Connection, code: str, buy_date: str, max_days: int = 15):
    """获取买入后的日线数据 (最多 max_days 个交易日)"""
    rows = conn.execute(
        """SELECT trade_date, open, high, low, close, volume
           FROM daily_price
           WHERE stock_code = ? AND trade_date > ?
           ORDER BY trade_date ASC
           LIMIT ?""",
        (code, buy_date, max_days),
    ).fetchall()
    return rows


def simulate_trailing(pos: dict, daily_prices: list, trailing_pct: float) -> dict | None:
    """模拟 trailing 止盈退出

    从买入后跟踪最高 close, 当 close 从最高回落 trailing_pct% 时卖出。
    最长持有10个交易日, 超过则最后一天收盘卖出。

    返回 {"sell_day": N, "sell_price": float, "return_pct": float, "highest": float}
    """
    buy_price = pos["buy_price"]
    if buy_price <= 0 or not daily_prices:
        return None

    highest_close = buy_price
    result = None

    for i, dp in enumerate(daily_prices):
        day_close = dp["close"]
        if day_close <= 0:
            continue

        if day_close > highest_close:
            highest_close = day_close

        # trailing 判断: 从最高价回落 trailing_pct%
        drawdown = (day_close - highest_close) / highest_close if highest_close > 0 else 0
        if drawdown <= -trailing_pct:
            result = {
                "sell_day": i + 1,
                "sell_price": day_close,
                "return_pct": (day_close / buy_price - 1) * 100,
                "highest": highest_close,
            }
            break

        # 最长持有 10 天
        if i >= 9:
            result = {
                "sell_day": i + 1,
                "sell_price": day_close,
                "return_pct": (day_close / buy_price - 1) * 100,
                "highest": highest_close,
            }
            break

    # 如果数据不足但有一些天
    if result is None and daily_prices:
        last = daily_prices[-1]
        result = {
            "sell_day": len(daily_prices),
            "sell_price": last["close"],
            "return_pct": (last["close"] / buy_price - 1) * 100,
            "highest": highest_close,
        }

    return result


def simulate_fixed_hold(pos: dict, daily_prices: list, hold_days: int) -> dict | None:
    """模拟固定持有天数后收盘卖出"""
    buy_price = pos["buy_price"]
    if buy_price <= 0:
        return None

    if len(daily_prices) < hold_days:
        # 数据不足, 用最后一天
        if daily_prices:
            last = daily_prices[-1]
            return {
                "sell_day": len(daily_prices),
                "sell_price": last["close"],
                "return_pct": (last["close"] / buy_price - 1) * 100,
                "highest": max(buy_price, max(dp["high"] for dp in daily_prices)),
            }
        return None

    dp = daily_prices[hold_days - 1]
    highest = buy_price
    for i in range(hold_days):
        if daily_prices[i]["high"] > highest:
            highest = daily_prices[i]["high"]

    return {
        "sell_day": hold_days,
        "sell_price": dp["close"],
        "return_pct": (dp["close"] / buy_price - 1) * 100,
        "highest": highest,
    }


def print_stats(label: str, trades: list[dict], highlight_code: str | None = None):
    """输出一组交易的统计"""
    if not trades:
        print(f"  {label}: 无有效交易")
        return

    returns = [t["return_pct"] for t in trades]
    wins = [r for r in returns if r > 0]
    losses = [r for r in returns if r <= 0]
    max_loss = min(returns) if returns else 0
    avg_ret = sum(returns) / len(returns) if returns else 0
    sorted_rets = sorted(returns)
    median_ret = sorted_rets[len(sorted_rets) // 2] if sorted_rets else 0
    win_rate = len(wins) / len(returns) * 100 if returns else 0
    ev = avg_ret  # 期望值=平均收益

    print(f"  {label}:")
    print(f"    交易数: {len(trades)}, 胜率: {win_rate:.1f}% ({len(wins)}/{len(returns)})")
    print(f"    平均收益: {avg_ret:+.2f}%, 中位数收益: {median_ret:+.2f}%")
    print(f"    最大单笔亏损: {max_loss:+.2f}%, 期望值(EV): {ev:+.2f}%")

    # 高亮中兴通讯
    if highlight_code:
        zx_trades = [t for t in trades if t.get("code") == highlight_code]
        for zt in zx_trades:
            print(f"    ★ {zt['name']}({zt['code']}): "
                  f"卖第{zt['sell_day']}天@{zt['sell_price']:.2f} "
                  f"收益{zt['return_pct']:+.2f}% 最高{zt['highest']:.2f}")


def main():
    parser = argparse.ArgumentParser(description="策略B退出参数回测")
    parser.add_argument("--trailing", type=str, default="2,3,5,8",
                        help="trailing百分比列表, 逗号分隔 (默认 '2,3,5,8')")
    parser.add_argument("--hold-days", type=str, default="2,3",
                        help="固定持有天数列表, 逗号分隔 (默认 '2,3')")
    args = parser.parse_args()

    trailing_pcts = [float(x.strip()) / 100 for x in args.trailing.split(",")]
    hold_days_list = [int(x.strip()) for x in args.hold_days.split(",")]

    conn = get_conn()
    try:
        positions = get_strategy_b_positions(conn)
        if not positions:
            print("未找到策略B持仓记录。")
            return

        print(f"\n{'='*80}")
        print(f"  策略B 退出参数回测")
        print(f"{'='*80}")
        print(f"  策略B持仓数: {len(positions)}")
        print(f"  Trailing 参数: {[f'{p*100:.0f}%' for p in trailing_pcts]}")
        print(f"  固定持有天数: {hold_days_list}")
        print(f"  高亮: 中兴通讯 000063")
        print()

        # 预加载所有需要的 daily_price 数据
        price_cache = {}
        for pos in positions:
            code = pos["code"]
            buy_date = pos["buy_date"]
            key = (code, buy_date)
            if key not in price_cache:
                price_cache[key] = get_daily_prices(conn, code, buy_date, max_days=15)

        # Trailing 止盈回测
        print("=" * 60)
        print("  Trailing 止盈规则回测")
        print("=" * 60)
        for tp in trailing_pcts:
            trades = []
            for pos in positions:
                key = (pos["code"], pos["buy_date"])
                dp = price_cache.get(key, [])
                result = simulate_trailing(pos, dp, tp)
                if result:
                    result["code"] = pos["code"]
                    result["name"] = pos["name"]
                    trades.append(result)
            print_stats(f"Trailing {tp*100:.0f}%", trades, highlight_code="000063")
            print()

        # 固定持有天数回测
        print("=" * 60)
        print("  固定持有天数回测")
        print("=" * 60)
        for hd in hold_days_list:
            trades = []
            for pos in positions:
                key = (pos["code"], pos["buy_date"])
                dp = price_cache.get(key, [])
                result = simulate_fixed_hold(pos, dp, hd)
                if result:
                    result["code"] = pos["code"]
                    result["name"] = pos["name"]
                    trades.append(result)
            print_stats(f"固定持有{hd}天", trades, highlight_code="000063")
            print()

        # 汇总对比表
        print("=" * 60)
        print("  汇总对比")
        print("=" * 60)
        print(f"  {'规则':<20} {'交易数':>6} {'胜率':>8} {'平均收益':>10} {'中位收益':>10} {'最大亏损':>10} {'EV':>8}")
        print("  " + "-" * 76)

        for tp in trailing_pcts:
            trades = []
            for pos in positions:
                key = (pos["code"], pos["buy_date"])
                dp = price_cache.get(key, [])
                result = simulate_trailing(pos, dp, tp)
                if result:
                    trades.append(result)
            if trades:
                returns = [t["return_pct"] for t in trades]
                wins = len([r for r in returns if r > 0])
                avg = sum(returns) / len(returns)
                sorted_r = sorted(returns)
                median = sorted_r[len(sorted_r) // 2]
                max_loss = min(returns)
                print(f"  Trailing {tp*100:.0f}%{'':<12} {len(trades):>6} {wins/len(returns)*100:>7.1f}% "
                      f"{avg:>+9.2f}% {median:>+9.2f}% {max_loss:>+9.2f}% {avg:>+7.2f}%")

        for hd in hold_days_list:
            trades = []
            for pos in positions:
                key = (pos["code"], pos["buy_date"])
                dp = price_cache.get(key, [])
                result = simulate_fixed_hold(pos, dp, hd)
                if result:
                    trades.append(result)
            if trades:
                returns = [t["return_pct"] for t in trades]
                wins = len([r for r in returns if r > 0])
                avg = sum(returns) / len(returns)
                sorted_r = sorted(returns)
                median = sorted_r[len(sorted_r) // 2]
                max_loss = min(returns)
                print(f"  固定{hd}天{'':<15} {len(trades):>6} {wins/len(returns)*100:>7.1f}% "
                      f"{avg:>+9.2f}% {median:>+9.2f}% {max_loss:>+9.2f}% {avg:>+7.2f}%")

        print()

    finally:
        conn.close()


if __name__ == "__main__":
    main()
