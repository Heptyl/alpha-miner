"""ATR动态止损回测验证 — 固定% vs ATR×N 对比

验证ATR动态止损替代固定百分比止损的效果:
  策略A: ATR×2.5 vs 固定-5%
  策略B: ATR×2.0 vs 固定-6%
  策略C: ATR×3.0 vs 固定-10%

用法: uv run python scripts/backtest_atr_stop.py
"""

import sqlite3
import sys
from pathlib import Path
from collections import defaultdict
from dataclasses import dataclass, field

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

DB_PATH = Path(__file__).resolve().parents[1] / "data" / "alpha_miner.db"
COST = 0.00125  # 单边交易成本


@dataclass
class Position:
    code: str
    name: str
    strategy: str
    buy_price: float
    buy_date: str
    shares: int
    cost_basis: float
    highest: float = 0.0
    hold_days: int = 0
    atr_at_buy: float = 0.0  # 买入时ATR


@dataclass
class StrategyResult:
    trades: list = field(default_factory=list)
    equity_curve: list = field(default_factory=list)


def compute_atr_series(high: list, low: list, close: list, period: int = 14) -> list:
    """计算ATR序列, 返回与输入等长的list(前period个为NaN)."""
    if len(close) < period + 1:
        return [None] * len(close)
    h = pd.Series(high, dtype=float)
    l = pd.Series(low, dtype=float)
    c = pd.Series(close, dtype=float)
    prev_c = c.shift(1)
    tr = pd.concat([h - l, (h - prev_c).abs(), (l - prev_c).abs()], axis=1).max(axis=1)
    atr = tr.rolling(period).mean()
    return atr.tolist()


def calc_atr_stop(atr: float | None, buy_price: float, multiplier: float,
                  floor: float = -0.02, cap: float = -0.15) -> float | None:
    """计算ATR动态止损百分比(负数)."""
    if atr is None or buy_price <= 0:
        return None
    atr_pct = -atr / buy_price * multiplier
    return max(cap, min(floor, atr_pct))


def run_backtest(use_atr: bool = False, atr_mult: dict = None) -> dict:
    """跑一次回测. use_atr=True时用ATR动态止损."""
    conn = sqlite3.connect(str(DB_PATH))

    start_date = "2024-06-01"
    end_date = "2026-05-29"

    dates = [r[0] for r in conn.execute("""
        SELECT DISTINCT trade_date FROM daily_price
        WHERE trade_date BETWEEN ? AND ?
        ORDER BY trade_date
    """, (start_date, end_date)).fetchall()]
    date_idx = {d: i for i, d in enumerate(dates)}

    print("加载数据...")

    # zt_pool
    zt_data = defaultdict(list)
    for r in conn.execute("""
        SELECT stock_code, trade_date, amount, consecutive_zt
        FROM zt_pool
        WHERE trade_date BETWEEN ? AND ? AND amount > 100_000_000
    """, (start_date, end_date)).fetchall():
        code, dt, amt, consec = r
        if code.startswith(('688', '689', '8', '4')):
            continue
        zt_data[dt].append({"code": code, "amount": amt, "consec": consec})

    # ROE
    roe_map = {}
    for r in conn.execute("""
        SELECT stock_code, roe FROM financial_summary
        WHERE (stock_code, report_date) IN (
            SELECT stock_code, MAX(report_date) FROM financial_summary GROUP BY stock_code
        ) AND roe IS NOT NULL
    """).fetchall():
        roe_map[r[0]] = float(r[1])

    # 日K线
    print("加载日K线...")
    price_data = defaultdict(dict)
    for r in conn.execute("""
        SELECT stock_code, trade_date, open, close, high, low, volume, amount
        FROM daily_price
        WHERE trade_date BETWEEN ? AND ?
        ORDER BY stock_code, trade_date
    """, (dates[0], end_date)).fetchall():
        code, dt, op, cl, hi, lo, vol, amt = r
        if cl and vol and vol > 0:
            price_data[code][dt] = {
                "open": float(op or cl), "close": float(cl),
                "high": float(hi or cl), "low": float(lo or cl),
                "volume": float(vol), "amount": float(amt or 0),
            }

    # 预计算ATR(每只股票每天)
    atr_cache = {}  # (code, date) -> ATR值
    if use_atr:
        print(f"预计算ATR (共{len(price_data)}只股票)...")
        atr_period = 14
        for ci, (code, days) in enumerate(price_data.items()):
            sorted_dates = sorted(days.keys())
            if len(sorted_dates) < atr_period + 1:
                continue
            highs = [days[d]["high"] for d in sorted_dates]
            lows = [days[d]["low"] for d in sorted_dates]
            closes = [days[d]["close"] for d in sorted_dates]
            atrs = compute_atr_series(highs, lows, closes, atr_period)
            for d, atr_val in zip(sorted_dates, atrs):
                if atr_val is not None and not np.isnan(atr_val):
                    atr_cache[(code, d)] = float(atr_val)
            if ci % 500 == 0:
                print(f"  ATR进度: {ci}/{len(price_data)}")

    # 每日全市场涨跌
    print("计算每日市场涨跌...")
    daily_market = {}
    for d in dates:
        ups, downs, total_chg, cnt = 0, 0, 0.0, 0
        for code, days in price_data.items():
            p = days.get(d)
            if not p:
                continue
            prev_d_idx = date_idx.get(d, 0) - 1
            if prev_d_idx < 0:
                continue
            prev_d = dates[prev_d_idx]
            pp = days.get(prev_d)
            if not pp or pp["close"] <= 0:
                continue
            chg = (p["close"] - pp["close"]) / pp["close"]
            total_chg += chg
            cnt += 1
            if chg > 0:
                ups += 1
            else:
                downs += 1
        if cnt > 500:
            daily_market[d] = {"avg_chg": total_chg / cnt, "ups": ups, "downs": downs, "cnt": cnt}

    # 策略参数
    A_cap, B_cap, C_cap = 30_000.0, 10_000.0, 50_000.0
    A_max_pos, B_max_pos, C_max_pos = 3, 1, 3

    # 固定止损(基准)
    fixed_stop = {"A": -0.05, "B": -0.06, "C": -0.10}
    fixed_trail = {"A": 0.03, "B": 0.05, "C": 0.10}
    fixed_hold = {"A": 3, "B": 7, "C": 5}

    if atr_mult is None:
        atr_mult = {"A": 2.5, "B": 2.0, "C": 3.0}
    fallback_stop = {"A": -0.05, "B": -0.06, "C": -0.10}

    results = {s: StrategyResult() for s in "ABC"}
    positions = {"A": [], "B": [], "C": []}
    cash = {"A": A_cap, "B": B_cap, "C": C_cap}
    bought_codes = {"A": set(), "B": set(), "C": set()}

    label = "ATR动态止损" if use_atr else "固定%止损"
    print(f"开始回测 {len(dates)} 天 ({label})...")

    for di, today in enumerate(dates):
        if di < 1:
            continue

        # ====== 卖出检查 ======
        for strat in "ABC":
            trail_pct = fixed_trail[strat]
            max_hold = fixed_hold[strat]

            new_positions = []
            for pos in positions[strat]:
                p = price_data.get(pos.code, {}).get(today)
                if not p:
                    new_positions.append(pos)
                    pos.hold_days += 1
                    continue

                cur_price = p["close"]
                pos.highest = max(pos.highest, cur_price)
                pos.hold_days += 1
                pnl_pct = (cur_price / pos.buy_price - 1)

                sell = False
                sell_reason = ""

                # 止损判断
                if use_atr:
                    atr_val = atr_cache.get((pos.code, today))
                    atr_stop = calc_atr_stop(atr_val, pos.buy_price, atr_mult[strat])
                    if atr_stop is not None:
                        if pnl_pct <= atr_stop:
                            sell = True
                            sell_reason = f"ATR止损{pnl_pct*100:+.1f}%(ATR={atr_val:.2f}×{atr_mult[strat]})"
                    else:
                        # ATR数据不足, fallback到固定%
                        if pnl_pct <= fallback_stop[strat]:
                            sell = True
                            sell_reason = f"止损{pnl_pct*100:+.1f}%(ATR无数据,fallback)"
                else:
                    if pnl_pct <= fixed_stop[strat]:
                        sell = True
                        sell_reason = f"止损{pnl_pct*100:+.1f}%"

                # trailing
                if not sell and pos.highest > pos.buy_price:
                    drawdown = (cur_price / pos.highest - 1)
                    if drawdown <= -trail_pct:
                        sell = True
                        sell_reason = f"trailing{drawdown*100:.1f}%"

                # 时间止损
                if not sell and pos.hold_days >= max_hold:
                    sell = True
                    sell_reason = f"到期{pos.hold_days}天"

                if sell:
                    sell_amt = cur_price * pos.shares * (1 - COST)
                    cash[strat] += sell_amt
                    ret_pct = (cur_price / pos.buy_price - 1) * 100
                    results[strat].trades.append(ret_pct)
                    bought_codes[strat].discard(pos.code)
                else:
                    new_positions.append(pos)

            positions[strat] = new_positions

        # ====== 买入 ======

        # 策略A
        if di + 1 < len(dates):
            buy_date = dates[di + 1]
            for zt in zt_data.get(today, []):
                if len(positions["A"]) >= A_max_pos:
                    break
                code = zt["code"]
                if code in bought_codes["A"]:
                    continue
                p = price_data.get(code, {}).get(buy_date)
                if not p or p["open"] <= 0:
                    continue
                buy_price = p["open"] * (1 + COST)
                pos_size = A_cap / A_max_pos
                shares = int(pos_size / buy_price / 100) * 100
                if shares <= 0:
                    continue
                cost = buy_price * shares
                if cost > cash["A"]:
                    continue
                atr_at_buy = atr_cache.get((code, today), 0.0) if use_atr else 0.0
                cash["A"] -= cost
                positions["A"].append(Position(
                    code=code, name="", strategy="A",
                    buy_price=buy_price, buy_date=buy_date,
                    shares=shares, cost_basis=cost,
                    highest=p.get("high", p["close"]),
                    atr_at_buy=atr_at_buy,
                ))
                bought_codes["A"].add(code)

        # 策略B
        mk = daily_market.get(today)
        if mk and mk["avg_chg"] < -0.02 and di + 1 < len(dates):
            buy_date = dates[di + 1]
            b_candidates = []
            for code, days in price_data.items():
                if code.startswith(('688', '689', '8', '4')):
                    continue
                p = days.get(today)
                prev_d = dates[di - 1] if di > 0 else None
                if not prev_d:
                    continue
                pp = days.get(prev_d)
                if not p or not pp or pp["close"] <= 0:
                    continue
                if p.get("amount", 0) < 50_000_000:
                    continue
                chg = (p["close"] - pp["close"]) / pp["close"]
                if chg >= -0.05:
                    continue
                roe = roe_map.get(code, 0)
                if roe < 10:
                    continue
                if p["close"] < 3 or p["close"] > 100:
                    continue
                b_candidates.append({"code": code, "amount": p.get("amount", 0)})

            if b_candidates and len(positions["B"]) < B_max_pos:
                b_candidates.sort(key=lambda x: x["amount"])
                for cand in b_candidates:
                    code = cand["code"]
                    if code in bought_codes["B"]:
                        continue
                    p = price_data.get(code, {}).get(buy_date)
                    if not p or p["open"] <= 0:
                        continue
                    buy_price = p["open"] * (1 + COST)
                    pos_size = B_cap / B_max_pos
                    shares = int(pos_size / buy_price / 100) * 100
                    if shares <= 0:
                        continue
                    cost = buy_price * shares
                    if cost > cash["B"]:
                        continue
                    atr_at_buy = atr_cache.get((code, today), 0.0) if use_atr else 0.0
                    cash["B"] -= cost
                    positions["B"].append(Position(
                        code=code, name="", strategy="B",
                        buy_price=buy_price, buy_date=buy_date,
                        shares=shares, cost_basis=cost,
                        highest=p.get("high", p["close"]),
                        atr_at_buy=atr_at_buy,
                    ))
                    bought_codes["B"].add(code)
                    break

        # 策略C (简化: 高量比趋势股)
        if di + 1 < len(dates) and len(positions["C"]) < C_max_pos:
            buy_date = dates[di + 1]
            c_candidates = []
            for code, days in price_data.items():
                if code.startswith(('688', '689', '8', '4')):
                    continue
                p = days.get(today)
                if not p or p["close"] < 3 or p["close"] > 100:
                    continue
                if p.get("amount", 0) < 200_000_000 or p.get("amount", 0) > 1_000_000_000:
                    continue
                vol_20 = []
                for back in range(1, 22):
                    bd = dates[di - back] if di - back >= 0 else None
                    if bd:
                        bp = days.get(bd)
                        if bp:
                            vol_20.append(bp["volume"])
                if len(vol_20) < 15:
                    continue
                avg_vol = np.mean(vol_20)
                if avg_vol <= 0:
                    continue
                vol_ratio = p["volume"] / avg_vol
                if vol_ratio < 5:
                    continue
                c_candidates.append({"code": code, "vol_ratio": vol_ratio})

            if c_candidates:
                c_candidates.sort(key=lambda x: -x["vol_ratio"])
                for cand in c_candidates:
                    if len(positions["C"]) >= C_max_pos:
                        break
                    code = cand["code"]
                    if code in bought_codes["C"]:
                        continue
                    p = price_data.get(code, {}).get(buy_date)
                    if not p or p["open"] <= 0:
                        continue
                    buy_price = p["open"] * (1 + COST)
                    pos_size = C_cap / C_max_pos
                    shares = int(pos_size / buy_price / 100) * 100
                    if shares <= 0:
                        continue
                    cost = buy_price * shares
                    if cost > cash["C"]:
                        continue
                    atr_at_buy = atr_cache.get((code, today), 0.0) if use_atr else 0.0
                    cash["C"] -= cost
                    positions["C"].append(Position(
                        code=code, name="", strategy="C",
                        buy_price=buy_price, buy_date=buy_date,
                        shares=shares, cost_basis=cost,
                        highest=p.get("high", p["close"]),
                        atr_at_buy=atr_at_buy,
                    ))
                    bought_codes["C"].add(code)

        # 每日权益
        for strat in "ABC":
            equity = cash[strat]
            for pos in positions[strat]:
                p = price_data.get(pos.code, {}).get(today)
                if p:
                    equity += p["close"] * pos.shares
            results[strat].equity_curve.append(equity)

        if di % 50 == 0:
            print(f"  {di}/{len(dates)} ({today})")

    # 清算剩余持仓
    last_date = dates[-1]
    for strat in "ABC":
        for pos in positions[strat]:
            p = price_data.get(pos.code, {}).get(last_date)
            sell_p = p["close"] if p else pos.buy_price
            ret_pct = (sell_p / pos.buy_price - 1) * 100
            results[strat].trades.append(ret_pct)

    conn.close()
    return results


def summarize(results: dict, label: str) -> str:
    lines = [f"\n{'='*90}", f"  {label}", f"{'='*90}"]
    lines.append(f"{'策略':<6} {'笔数':>6} {'胜率':>6} {'PF':>6} {'均收益':>8} {'总收益':>10} {'最大回撤':>8}")
    lines.append("-" * 90)

    all_trades, all_equity = [], []

    for strat in "ABC":
        r = results[strat]
        trades = r.trades
        all_trades.extend(trades)
        all_equity.extend(r.equity_curve)

        if not trades:
            lines.append(f"{strat:<6} {'无交易':>6}")
            continue

        n = len(trades)
        wins = [t for t in trades if t > 0]
        losses = [t for t in trades if t <= 0]
        wr = len(wins) / n * 100
        avg = np.mean(trades)
        gp = sum(wins) if wins else 0
        gl = abs(sum(losses)) if losses else 0.001
        pf = gp / gl
        eq = np.array(r.equity_curve)
        peak = np.maximum.accumulate(eq)
        dd = (eq - peak) / np.where(peak > 0, peak, 1) * 100
        max_dd = dd.min() if len(dd) > 0 else 0
        total_ret = (eq[-1] / eq[0] - 1) * 100 if len(eq) > 0 and eq[0] > 0 else 0

        lines.append(f"{strat:<6} {n:>6} {wr:>5.1f}% {pf:>6.2f} {avg:>+7.2f}% {total_ret:>+9.1f}% {max_dd:>7.1f}%")

    if all_trades:
        n = len(all_trades)
        wins = [t for t in all_trades if t > 0]
        losses = [t for t in all_trades if t <= 0]
        wr = len(wins) / n * 100
        avg = np.mean(all_trades)
        gp = sum(wins) if wins else 0
        gl = abs(sum(losses)) if losses else 0.001
        pf = gp / gl
        eq = np.array(all_equity)
        peak = np.maximum.accumulate(eq)
        dd = (eq - peak) / np.where(peak > 0, peak, 1) * 100
        max_dd = dd.min() if len(dd) > 0 else 0
        total_ret = (eq[-1] / eq[0] - 1) * 100 if len(eq) > 0 and eq[0] > 0 else 0
        lines.append(f"{'合计':<6} {n:>6} {wr:>5.1f}% {pf:>6.2f} {avg:>+7.2f}% {total_ret:>+9.1f}% {max_dd:>7.1f}%")

    return "\n".join(lines)


if __name__ == "__main__":
    print("ATR动态止损回测验证")
    print("回测期: 2024-06-01 ~ 2026-05-29")
    print("=" * 90)

    # 回测1: 固定%止损(基准)
    print("\n>>> 回测1: 固定%止损 (A:-5%, B:-6%, C:-10%)")
    fixed_results = run_backtest(use_atr=False)
    fixed_summary = summarize(fixed_results, "固定%止损 (基准)")

    # 回测2: ATR动态止损
    print("\n>>> 回测2: ATR动态止损 (A:ATR×2.5, B:ATR×2.0, C:ATR×3.0)")
    atr_results = run_backtest(use_atr=True, atr_mult={"A": 2.5, "B": 2.0, "C": 3.0})
    atr_summary = summarize(atr_results, "ATR动态止损")

    output = f"""# ATR动态止损回测验证

> 日期: 2026-05-31
> 回测期: 2024-06-01 ~ 2026-05-29
> 资金: A:3万/3只 + B:1万/1只 + C:5万/3只 = 9万/7只

## ATR参数

| 策略 | 固定止损 | ATR倍数 | ATR止损范围 | fallback |
|------|---------|---------|------------|---------|
| A | -5% | ×2.5 | [-2%, -15%] | -5% |
| B | -6% | ×2.0 | [-2%, -15%] | -6% |
| C | -10% | ×3.0 | [-2%, -15%] | -10% |

## 结果对比

{fixed_summary}

{atr_summary}

## 对比表

| 策略 | 版本 | 笔数 | 胜率 | PF | 总收益% | 最大回撤% |
"""

    for strat in "ABC":
        for label, res in [("固定%", fixed_results), ("ATR", atr_results)]:
            r = res[strat]
            trades = r.trades
            if not trades:
                output += f"| {strat} | {label} | 0 | - | - | - | - |\n"
                continue
            n = len(trades)
            wins = [t for t in trades if t > 0]
            losses = [t for t in trades if t <= 0]
            wr = len(wins) / n * 100
            gp = sum(wins) if wins else 0
            gl = abs(sum(losses)) if losses else 0.001
            pf = gp / gl
            eq = np.array(r.equity_curve)
            total_ret = (eq[-1] / eq[0] - 1) * 100 if len(eq) > 0 and eq[0] > 0 else 0
            peak = np.maximum.accumulate(eq)
            dd = (eq - peak) / np.where(peak > 0, peak, 1) * 100
            max_dd = dd.min() if len(dd) > 0 else 0
            output += f"| {strat} | {label} | {n} | {wr:.1f}% | {pf:.2f} | {total_ret:+.1f}% | {max_dd:.1f}% |\n"

    for label, res in [("固定%", fixed_results), ("ATR", atr_results)]:
        all_trades, all_equity = [], []
        for strat in "ABC":
            all_trades.extend(res[strat].trades)
            all_equity.extend(res[strat].equity_curve)
        if all_trades:
            n = len(all_trades)
            wins = [t for t in all_trades if t > 0]
            losses = [t for t in all_trades if t <= 0]
            wr = len(wins) / n * 100
            gp = sum(wins) if wins else 0
            gl = abs(sum(losses)) if losses else 0.001
            pf = gp / gl
            eq = np.array(all_equity)
            total_ret = (eq[-1] / eq[0] - 1) * 100 if len(eq) > 0 and eq[0] > 0 else 0
            peak = np.maximum.accumulate(eq)
            dd = (eq - peak) / np.where(peak > 0, peak, 1) * 100
            max_dd = dd.min() if len(dd) > 0 else 0
            output += f"| **合计** | **{label}** | {n} | {wr:.1f}% | {pf:.2f} | {total_ret:+.1f}% | {max_dd:.1f}% |\n"

    print(output)

    report_path = Path(__file__).resolve().parents[1] / ".claude" / "backtest-atr-stop.md"
    report_path.write_text(output, encoding="utf-8")
    print(f"\n报告已写入: {report_path}")
