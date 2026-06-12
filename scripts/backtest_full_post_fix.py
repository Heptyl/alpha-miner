"""修复后完整回测验证 — 3策略2年回测，新旧参数对比

验证Round 7的3个P0参数修复:
  1. 策略B排序: 跌幅最大→市值最小
  2. 策略A trailing: 3%→5%
  3. 策略A止损: -5%→-8%

用法: uv run python scripts/backtest_full_post_fix.py
"""

import sqlite3
import sys
from pathlib import Path
from collections import defaultdict
from dataclasses import dataclass, field

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

DB_PATH = Path(__file__).resolve().parents[1] / "data" / "alpha_miner.db"
COST = 0.00125  # 单边交易成本


@dataclass
class Position:
    code: str
    name: str
    strategy: str
    buy_price: float       # 含买入成本
    buy_date: str
    shares: int
    cost_basis: float      # buy_price * shares
    highest: float = 0.0   # 持仓期间最高价(收盘价)
    hold_days: int = 0


@dataclass
class StrategyResult:
    trades: list = field(default_factory=list)  # 每笔收益率%
    equity_curve: list = field(default_factory=list)  # 每日总权益
    candidates_per_day: list = field(default_factory=list)


def run_backtest(params: dict) -> dict:
    """跑一次完整回测，返回各策略结果"""
    conn = sqlite3.connect(str(DB_PATH))

    start_date = "2024-06-01"
    end_date = "2026-05-29"

    # 加载交易日
    dates = [r[0] for r in conn.execute("""
        SELECT DISTINCT trade_date FROM daily_price
        WHERE trade_date BETWEEN ? AND ?
        ORDER BY trade_date
    """, (start_date, end_date)).fetchall()]
    date_idx = {d: i for i, d in enumerate(dates)}

    # ---- 预加载数据 ----
    print("加载数据...")

    # 1. zt_pool (策略A候选)
    zt_data = defaultdict(list)  # date -> [{code, amount, consec}]
    for r in conn.execute("""
        SELECT stock_code, trade_date, amount, consecutive_zt
        FROM zt_pool
        WHERE trade_date BETWEEN ? AND ? AND amount > 100_000_000
    """, (start_date, end_date)).fetchall():
        code, dt, amt, consec = r
        if code.startswith(('688', '689', '8', '4')):
            continue
        zt_data[dt].append({"code": code, "amount": amt, "consec": consec})

    # 2. financial_summary (ROE for 策略B)
    roe_map = {}
    for r in conn.execute("""
        SELECT stock_code, roe FROM financial_summary
        WHERE (stock_code, report_date) IN (
            SELECT stock_code, MAX(report_date) FROM financial_summary GROUP BY stock_code
        ) AND roe IS NOT NULL
    """).fetchall():
        roe_map[r[0]] = float(r[1])

    # 3. daily_price OHLCV (全量缓存到内存)
    print("加载日K线...")
    price_data = defaultdict(dict)  # code -> {date: {open, close, high, low, volume, amount}}
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

    # 4. 预计算每日全市场涨跌(策略B暴跌日判断)
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

    # ---- 策略参数 ----
    A_cap = 30_000.0
    B_cap = 10_000.0
    C_cap = 50_000.0
    A_max_pos = 3
    B_max_pos = 1
    C_max_pos = 3

    A_stop = params["A_stop"]
    A_trail = params["A_trail"]
    B_sort = params["B_sort"]  # "drop" or "amount"

    B_stop = -0.06
    B_trail = 0.05
    C_stop = -0.06
    C_trail = 0.06

    # ---- 模拟 ----
    results = {s: StrategyResult() for s in "ABC"}

    # 每个策略的持仓和现金
    positions = {"A": [], "B": [], "C": []}
    cash = {"A": A_cap, "B": B_cap, "C": C_cap}

    # 策略C候选缓存(量比计算量大,每天只算一次)
    c_signal_cache = {}  # code -> signal_date

    # 已买入记录(防止同只重复买)
    bought_codes = {"A": set(), "B": set(), "C": set()}

    print(f"开始回测 {len(dates)} 天 (参数: A_stop={A_stop}, A_trail={A_trail}, B_sort={B_sort})...")

    for di, today in enumerate(dates):
        if di < 1:
            continue

        # ====== 1. 卖出检查 ======
        for strat in "ABC":
            stop_pct = {"A": A_stop, "B": B_stop, "C": C_stop}[strat]
            trail_pct = {"A": A_trail, "B": B_trail, "C": C_trail}[strat]
            max_hold = {"A": 3, "B": 7, "C": 20}[strat]

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

                # 浮亏率
                pnl_pct = (cur_price / pos.buy_price - 1)

                sell = False
                sell_reason = ""

                # 止损
                if pnl_pct <= stop_pct:
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

        # ====== 2. 买入 ======

        # --- 策略A: 涨停次日买入 ---
        if di + 1 < len(dates):
            buy_date = dates[di + 1]
            zt_today = zt_data.get(today, [])

            for zt in zt_today:
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

                cash["A"] -= cost
                positions["A"].append(Position(
                    code=code, name="", strategy="A",
                    buy_price=buy_price, buy_date=buy_date,
                    shares=shares, cost_basis=cost,
                    highest=p.get("high", p["close"]),
                ))
                bought_codes["A"].add(code)

        # --- 策略B: 暴跌日候选, 次日买入 ---
        mk = daily_market.get(today)
        if mk and mk["avg_chg"] < -0.02:
            if di + 1 < len(dates):
                buy_date = dates[di + 1]
                crash_drop = -0.05

                # 找暴跌日跌>5%的股票
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
                    if chg >= crash_drop:
                        continue
                    roe = roe_map.get(code, 0)
                    if roe < 10:
                        continue
                    if p["close"] < 3 or p["close"] > 100:
                        continue
                    b_candidates.append({
                        "code": code, "drop": chg, "amount": p.get("amount", 0), "roe": roe,
                    })

                if b_candidates:
                    # 排序
                    if B_sort == "drop":
                        b_candidates.sort(key=lambda x: x["drop"])  # 跌幅最大(最负)
                    else:
                        b_candidates.sort(key=lambda x: x["amount"])  # 成交额最小

                    results["B"].candidates_per_day.append(len(b_candidates))

                    if len(positions["B"]) < B_max_pos:
                        for cand in b_candidates[:B_max_pos]:
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

                            cash["B"] -= cost
                            positions["B"].append(Position(
                                code=code, name="", strategy="B",
                                buy_price=buy_price, buy_date=buy_date,
                                shares=shares, cost_basis=cost,
                                highest=p.get("high", p["close"]),
                            ))
                            bought_codes["B"].add(code)
                            break  # B最多1只/天

        # --- 策略C: 高量比趋势股, 次日买入 ---
        if di + 1 < len(dates):
            buy_date = dates[di + 1]
            if len(positions["C"]) < C_max_pos:
                c_candidates = []
                for code, days in price_data.items():
                    if code.startswith(('688', '689', '8', '4')):
                        continue

                    # 需要足够历史
                    # 简化: 只检查量比和基础条件
                    p = days.get(today)
                    if not p or p["close"] < 3 or p["close"] > 100:
                        continue
                    if p.get("amount", 0) < 200_000_000 or p.get("amount", 0) > 1_000_000_000:
                        continue

                    # 计算20日均量
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

                    # MA5>MA20>MA60
                    closes_60 = []
                    for back in range(60):
                        bd = dates[di - back] if di - back >= 0 else None
                        if bd:
                            bp = days.get(bd)
                            if bp:
                                closes_60.append(bp["close"])
                    if len(closes_60) < 60:
                        continue
                    closes_60 = list(reversed(closes_60))
                    ma5 = np.mean(closes_60[-5:])
                    ma20 = np.mean(closes_60[-20:])
                    ma60 = np.mean(closes_60)
                    if not (ma5 > ma20 > ma60):
                        continue

                    # MACD>0
                    ema12 = ema26 = closes_60[0]
                    for c in closes_60:
                        ema12 = c * 2/13 + ema12 * 11/13
                        ema26 = c * 2/27 + ema26 * 25/27
                    if ema12 <= ema26:
                        continue

                    # RSI 50-70
                    if len(closes_60) >= 15:
                        deltas = np.diff(closes_60[-15:])
                        up = np.mean(deltas[deltas > 0]) if np.any(deltas > 0) else 0
                        dn = abs(np.mean(deltas[deltas < 0])) if np.any(deltas < 0) else 0.001
                        rsi = 100 - (100 / (1 + up / dn))
                        if not (50 <= rsi <= 70):
                            continue

                    # MA60距离<10%
                    pct_above = (p["close"] - ma60) / ma60
                    if pct_above > 0.10:
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

                        cash["C"] -= cost
                        positions["C"].append(Position(
                            code=code, name="", strategy="C",
                            buy_price=buy_price, buy_date=buy_date,
                            shares=shares, cost_basis=cost,
                            highest=p.get("high", p["close"]),
                        ))
                        bought_codes["C"].add(code)

        # 记录每日权益
        for strat in "ABC":
            equity = cash[strat]
            for pos in positions[strat]:
                p = price_data.get(pos.code, {}).get(today)
                if p:
                    equity += p["close"] * pos.shares
            results[strat].equity_curve.append(equity)

        # 进度
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
    """格式化结果"""
    lines = [f"\n{'='*90}", f"  {label}", f"{'='*90}"]
    lines.append(f"{'策略':<6} {'笔数':>6} {'胜率':>6} {'PF':>6} {'均收益':>8} {'总收益':>10} {'最大回撤':>8}")
    lines.append("-" * 90)

    all_trades = []
    all_equity = []

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

        # 最大回撤
        eq = np.array(r.equity_curve)
        peak = np.maximum.accumulate(eq)
        dd = (eq - peak) / np.where(peak > 0, peak, 1) * 100
        max_dd = dd.min() if len(dd) > 0 else 0

        total_ret = (eq[-1] / eq[0] - 1) * 100 if len(eq) > 0 and eq[0] > 0 else 0

        lines.append(f"{strat:<6} {n:>6} {wr:>5.1f}% {pf:>6.2f} {avg:>+7.2f}% {total_ret:>+9.1f}% {max_dd:>7.1f}%")

    # 合计
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
    print("修复后完整回测验证")
    print("回测期: 2024-06-01 ~ 2026-05-29")
    print("=" * 90)

    # 旧参数
    old_params = {"A_stop": -0.05, "A_trail": 0.03, "B_sort": "drop"}
    # 新参数
    new_params = {"A_stop": -0.08, "A_trail": 0.05, "B_sort": "amount"}

    print("\n>>> 回测1: 旧参数 (A_stop=-5%, A_trail=3%, B排序=跌幅最大)")
    old_results = run_backtest(old_params)
    old_summary = summarize(old_results, "旧参数 (修复前)")

    print("\n>>> 回测2: 新参数 (A_stop=-8%, A_trail=5%, B排序=市值最小)")
    new_results = run_backtest(new_params)
    new_summary = summarize(new_results, "新参数 (修复后)")

    # 输出对比
    output = f"""# 修复后完整回测验证

> 日期: 2026-05-30
> 回测期: 2024-06-01 ~ 2026-05-29
> 资金: A:3万/3只 + B:1万/1只 + C:5万/3只 = 9万/7只

## 修复内容

| # | 参数 | 旧值 | 新值 | 数据支撑 |
|---|------|------|------|---------|
| 1 | 策略B排序 | 跌幅最大 | 市值最小(成交额) | PF=3.24 vs 0.81 |
| 2 | 策略A trailing | 3% | 5% | PF=1.04 vs 0.96 |
| 3 | 策略A止损 | -5% | -8% | 2σ vs 1.2σ |

## 结果对比

{old_summary}

{new_summary}

## 对比表

| 策略 | 版本 | 笔数 | 胜率 | PF | 总收益% | 最大回撤% |
"""

    for strat in "ABC":
        for label, res in [("旧", old_results), ("新", new_results)]:
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

    # 合计行
    for label, res in [("旧", old_results), ("新", new_results)]:
        all_trades = []
        all_equity = []
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

    # 写入文件
    report_path = Path(__file__).resolve().parents[1] / ".claude" / "backtest-post-fix.md"
    report_path.write_text(output, encoding="utf-8")
    print(f"\n报告已写入: {report_path}")
