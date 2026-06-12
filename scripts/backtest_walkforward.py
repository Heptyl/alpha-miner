"""Walk-Forward回测框架 — 滚动窗口验证防过拟合

核心思路:
  1. 将全量回测期间按"训练窗口+验证窗口"滚动切分
  2. 每个窗口独立跑回测, 计算PF/胜率/最大回撤
  3. 对比: 全量回测PF vs Walk-Forward各窗口PF
  4. WF的PF < 全量PF的50% → 过拟合告警

用法:
  uv run python scripts/backtest_walkforward.py
  uv run python scripts/backtest_walkforward.py --train 4 --test 1 --strategy A
"""

import sqlite3
import sys
from pathlib import Path
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

DB_PATH = Path(__file__).resolve().parents[1] / "data" / "alpha_miner.db"
COST = 0.00125


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


@dataclass
class WindowResult:
    window_id: int
    train_start: str
    train_end: str
    test_start: str
    test_end: str
    strategy: str = ""
    train_end: str
    test_start: str
    test_end: str
    train_trades: list = field(default_factory=list)
    test_trades: list = field(default_factory=list)
    train_pf: float = 0
    test_pf: float = 0
    train_wr: float = 0
    test_wr: float = 0
    train_dd: float = 0
    test_dd: float = 0
    train_avg: float = 0
    test_avg: float = 0


def _compute_metrics(trades: list, equity_curve: list = None) -> dict:
    """计算回测指标"""
    if not trades:
        return {"pf": 0, "wr": 0, "avg": 0, "dd": 0, "total_ret": 0, "count": 0}

    n = len(trades)
    wins = [t for t in trades if t > 0]
    losses = [t for t in trades if t <= 0]
    wr = len(wins) / n * 100
    avg = np.mean(trades)
    gp = sum(wins) if wins else 0
    gl = abs(sum(losses)) if losses else 0.001
    pf = gp / gl

    dd = 0
    total_ret = 0
    if equity_curve and len(equity_curve) > 1:
        eq = np.array(equity_curve)
        peak = np.maximum.accumulate(eq)
        dd_arr = (eq - peak) / np.where(peak > 0, peak, 1) * 100
        dd = dd_arr.min()
        total_ret = (eq[-1] / eq[0] - 1) * 100 if eq[0] > 0 else 0

    return {"pf": round(pf, 2), "wr": round(wr, 1), "avg": round(avg, 2),
            "dd": round(dd, 1), "total_ret": round(total_ret, 1), "count": n}


def _run_period(conn, dates: list, date_idx: dict, price_data: dict,
                zt_data: dict, daily_market: dict, roe_map: dict,
                start_idx: int, end_idx: int,
                strategy: str, params: dict) -> dict:
    """在指定日期区间跑单策略回测"""

    stop_pct = {"A": params.get("A_stop", -0.08), "B": -0.06, "C": -0.06}[strategy]
    trail_pct = {"A": params.get("A_trail", 0.05), "B": 0.05, "C": 0.06}[strategy]
    max_hold = {"A": 3, "B": 7, "C": 20}[strategy]
    max_pos = {"A": 3, "B": 1, "C": 3}[strategy]
    cap = {"A": 30_000, "B": 10_000, "C": 50_000}[strategy]

    positions = []
    cash = float(cap)
    trades = []
    equity_curve = []
    bought_codes = set()

    period_dates = dates[start_idx:end_idx + 1]

    for di_offset, today in enumerate(period_dates):
        di = date_idx.get(today, start_idx + di_offset)

        # 更新持仓
        new_positions = []
        for pos in positions:
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
            if pnl_pct <= stop_pct:
                sell = True
            elif pos.highest > pos.buy_price:
                drawdown = (cur_price / pos.highest - 1)
                if drawdown <= -trail_pct:
                    sell = True
            elif pos.hold_days >= max_hold:
                sell = True

            if sell:
                sell_amt = cur_price * pos.shares * (1 - COST)
                cash += sell_amt
                ret_pct = (cur_price / pos.buy_price - 1) * 100
                trades.append(ret_pct)
                bought_codes.discard(pos.code)
            else:
                new_positions.append(pos)

        positions = new_positions

        # 买入
        next_di = di + 1
        if next_di < len(dates) and next_di <= end_idx:
            buy_date = dates[next_di]

            if strategy == "A":
                zt_today = zt_data.get(today, [])
                for zt in zt_today:
                    if len(positions) >= max_pos:
                        break
                    code = zt["code"]
                    if code in bought_codes:
                        continue
                    p = price_data.get(code, {}).get(buy_date)
                    if not p or p["open"] <= 0:
                        continue
                    buy_price = p["open"] * (1 + COST)
                    pos_size = cap / max_pos
                    shares = int(pos_size / buy_price / 100) * 100
                    if shares <= 0:
                        continue
                    cost = buy_price * shares
                    if cost > cash:
                        continue
                    cash -= cost
                    positions.append(Position(
                        code=code, name="", strategy="A",
                        buy_price=buy_price, buy_date=buy_date,
                        shares=shares, cost_basis=cost,
                        highest=p.get("high", p["close"]),
                    ))
                    bought_codes.add(code)

            elif strategy == "B":
                mk = daily_market.get(today)
                if mk and mk["avg_chg"] < -0.02:
                    crash_drop = -0.05
                    b_candidates = []
                    prev_d = dates[di - 1] if di > 0 else None
                    if prev_d:
                        for code, days in price_data.items():
                            if code.startswith(('688', '689', '8', '4')):
                                continue
                            p = days.get(today)
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
                            b_candidates.append({"code": code, "chg": chg, "amount": p.get("amount", 0)})

                    sort_key = params.get("B_sort", "amount")
                    if sort_key == "drop":
                        b_candidates.sort(key=lambda x: x["chg"])
                    else:
                        b_candidates.sort(key=lambda x: x["amount"])

                    for bc in b_candidates[:max_pos]:
                        if len(positions) >= max_pos:
                            break
                        code = bc["code"]
                        if code in bought_codes:
                            continue
                        p = price_data.get(code, {}).get(buy_date)
                        if not p or p["open"] <= 0:
                            continue
                        buy_price = p["open"] * (1 + COST)
                        pos_size = cap / max_pos
                        shares = int(pos_size / buy_price / 100) * 100
                        if shares <= 0:
                            continue
                        cost = buy_price * shares
                        if cost > cash:
                            continue
                        cash -= cost
                        positions.append(Position(
                            code=code, name="", strategy="B",
                            buy_price=buy_price, buy_date=buy_date,
                            shares=shares, cost_basis=cost,
                            highest=p.get("high", p["close"]),
                        ))
                        bought_codes.add(code)

            elif strategy == "C":
                prev_20_di = max(0, di - 20)
                c_candidates = []
                for code, days in price_data.items():
                    if code.startswith(('688', '689', '8', '4')):
                        continue
                    p = days.get(today)
                    if not p or p["close"] <= 3 or p["close"] > 200:
                        continue
                    if p.get("amount", 0) < 50_000_000:
                        continue

                    # 量比
                    vol_today = p.get("volume", 0)
                    if vol_today <= 0:
                        continue
                    vol_sum = 0
                    vol_cnt = 0
                    for vi in range(max(0, di - 20), di):
                        vd = dates[vi]
                        vp = days.get(vd)
                        if vp and vp.get("volume", 0) > 0:
                            vol_sum += vp["volume"]
                            vol_cnt += 1
                    if vol_cnt < 5:
                        continue
                    avg_vol = vol_sum / vol_cnt
                    vol_ratio = vol_today / avg_vol if avg_vol > 0 else 0
                    if vol_ratio < 3:
                        continue

                    # MA5/MA20/MA60
                    closes = []
                    for vi in range(max(0, di - 60), di + 1):
                        vd = dates[vi]
                        vp = days.get(vd)
                        if vp:
                            closes.append(vp["close"])
                    if len(closes) < 20:
                        continue
                    ma5 = np.mean(closes[-5:])
                    ma20 = np.mean(closes[-20:])
                    ma60 = np.mean(closes[-60:]) if len(closes) >= 60 else 0

                    if ma5 <= ma20 or ma20 <= 0:
                        continue
                    if ma60 > 0 and p["close"] > ma60 * 1.15:
                        continue

                    c_candidates.append({"code": code, "vol_ratio": vol_ratio,
                                         "amount": p.get("amount", 0)})

                c_candidates.sort(key=lambda x: x["vol_ratio"], reverse=True)

                for cc in c_candidates[:max_pos * 2]:
                    if len(positions) >= max_pos:
                        break
                    code = cc["code"]
                    if code in bought_codes:
                        continue
                    p = price_data.get(code, {}).get(buy_date)
                    if not p or p["open"] <= 0:
                        continue
                    buy_price = p["open"] * (1 + COST)
                    pos_size = cap / max_pos
                    shares = int(pos_size / buy_price / 100) * 100
                    if shares <= 0:
                        continue
                    cost = buy_price * shares
                    if cost > cash:
                        continue
                    cash -= cost
                    positions.append(Position(
                        code=code, name="", strategy="C",
                        buy_price=buy_price, buy_date=buy_date,
                        shares=shares, cost_basis=cost,
                        highest=p.get("high", p["close"]),
                    ))
                    bought_codes.add(code)

        # 记录equity
        pos_value = sum(pos.highest * pos.shares for pos in positions)
        equity_curve.append(cash + pos_value)

    return {"trades": trades, "equity_curve": equity_curve}


def run_walkforward(train_months: int = 3, test_months: int = 1,
                    strategy: str = None,
                    params: dict = None) -> list[WindowResult]:
    """运行Walk-Forward回测

    Args:
        train_months: 训练窗口月数
        test_months: 验证窗口月数
        strategy: 指定策略(A/B/C), 空=全部
        params: 回测参数

    Returns:
        各窗口的回测结果
    """
    if params is None:
        params = {"A_stop": -0.08, "A_trail": 0.05, "B_sort": "amount"}
    if strategy is None:
        strategy = "ABC"

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

    # 预加载数据
    print("加载数据...")
    zt_data = defaultdict(list)
    for r in conn.execute("""
        SELECT stock_code, trade_date, amount, consecutive_zt
        FROM zt_pool WHERE trade_date BETWEEN ? AND ? AND amount > 100_000_000
    """, (start_date, end_date)).fetchall():
        code, dt, amt, consec = r
        if code.startswith(('688', '689', '8', '4')):
            continue
        zt_data[dt].append({"code": code, "amount": amt, "consec": consec})

    roe_map = {}
    for r in conn.execute("""
        SELECT stock_code, roe FROM financial_summary
        WHERE (stock_code, report_date) IN (
            SELECT stock_code, MAX(report_date) FROM financial_summary GROUP BY stock_code
        ) AND roe IS NOT NULL
    """).fetchall():
        roe_map[r[0]] = float(r[1])

    price_data = defaultdict(dict)
    for r in conn.execute("""
        SELECT stock_code, trade_date, open, close, high, low, volume, amount
        FROM daily_price WHERE trade_date BETWEEN ? AND ?
        ORDER BY stock_code, trade_date
    """, (dates[0], end_date)).fetchall():
        code, dt, op, cl, hi, lo, vol, amt = r
        if cl and vol and vol > 0:
            price_data[code][dt] = {
                "open": float(op or cl), "close": float(cl),
                "high": float(hi or cl), "low": float(lo or cl),
                "volume": float(vol), "amount": float(amt or 0),
            }

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

    # 构建滚动窗口
    train_days = train_months * 22  # ~22交易日/月
    test_days = test_months * 22
    step = test_days

    windows = []
    si = 0
    window_id = 0
    while si + train_days + test_days <= len(dates):
        train_start_idx = si
        train_end_idx = si + train_days - 1
        test_start_idx = si + train_days
        test_end_idx = min(si + train_days + test_days - 1, len(dates) - 1)

        windows.append({
            "id": window_id,
            "train_start": dates[train_start_idx],
            "train_end": dates[train_end_idx],
            "test_start": dates[test_start_idx],
            "test_end": dates[test_end_idx],
            "train_start_idx": train_start_idx,
            "train_end_idx": train_end_idx,
            "test_start_idx": test_start_idx,
            "test_end_idx": test_end_idx,
        })
        window_id += 1
        si += step

    print(f"\nWalk-Forward: {len(windows)}个窗口 (训练{train_months}月/验证{test_months}月)")
    print(f"日期范围: {dates[0]} ~ {dates[-1]}")

    # 按窗口回测
    results = []
    for strat in strategy:
        print(f"\n{'='*80}")
        print(f"  策略{strat} Walk-Forward回测")
        print(f"{'='*80}")

        # 先跑全量作为基准
        full_result = _run_period(
            conn, dates, date_idx, price_data, zt_data, daily_market, roe_map,
            0, len(dates) - 1, strat, params,
        )
        full_metrics = _compute_metrics(full_result["trades"], full_result["equity_curve"])
        print(f"  全量基准: PF={full_metrics['pf']:.2f} 胜率={full_metrics['wr']:.1f}% "
              f"均收={full_metrics['avg']:+.2f}% 回撤={full_metrics['dd']:.1f}%")

        # 逐窗口
        print(f"\n  {'窗口':>4} | {'训练期间':22s} | {'验证期间':22s} | {'训练PF':>6} | {'验证PF':>6} | {'验证胜率':>6} | {'验证回撤':>6} | {'过拟合?':>6}")
        print(f"  {'-'*4}-+-{'-'*22}-+-{'-'*22}-+-{'-'*6}-+-{'-'*6}-+-{'-'*6}-+-{'-'*6}-+-{'-'*6}")

        for w in windows:
            train_result = _run_period(
                conn, dates, date_idx, price_data, zt_data, daily_market, roe_map,
                w["train_start_idx"], w["train_end_idx"], strat, params,
            )
            test_result = _run_period(
                conn, dates, date_idx, price_data, zt_data, daily_market, roe_map,
                w["test_start_idx"], w["test_end_idx"], strat, params,
            )

            train_m = _compute_metrics(train_result["trades"], train_result["equity_curve"])
            test_m = _compute_metrics(test_result["trades"], test_result["equity_curve"])

            # 过拟合检测
            overfit = ""
            if test_m["count"] > 0 and full_metrics["pf"] > 0:
                if test_m["pf"] < full_metrics["pf"] * 0.5:
                    overfit = "⚠️ 过拟合"
                elif test_m["pf"] < full_metrics["pf"] * 0.7:
                    overfit = "⚠ 轻微"

            wr = WindowResult(
                window_id=w["id"],
                train_start=w["train_start"],
                train_end=w["train_end"],
                test_start=w["test_start"],
                test_end=w["test_end"],
                strategy=strat,
                train_trades=train_result["trades"],
                test_trades=test_result["trades"],
                train_pf=train_m["pf"],
                test_pf=test_m["pf"],
                train_wr=train_m["wr"],
                test_wr=test_m["wr"],
                train_dd=train_m["dd"],
                test_dd=test_m["dd"],
                train_avg=train_m["avg"],
                test_avg=test_m["avg"],
            )
            results.append(wr)

            print(f"  {w['id']:4} | {w['train_start']}~{w['train_end']} | "
                  f"{w['test_start']}~{w['test_end']} | "
                  f"{train_m['pf']:6.2f} | {test_m['pf']:6.2f} | "
                  f"{test_m['wr']:5.1f}% | {test_m['dd']:6.1f}% | {overfit}")

    # 汇总分析
    print(f"\n{'='*80}")
    print(f"  Walk-Forward 过拟合检测汇总")
    print(f"{'='*80}")

    # 重新跑全量基准(按策略)
    for strat in strategy:
        full_result = _run_period(
            conn, dates, date_idx, price_data, zt_data, daily_market, roe_map,
            0, len(dates) - 1, strat, params,
        )
        full_m = _compute_metrics(full_result["trades"], full_result["equity_curve"])

        strat_windows = [r for r in results if r.strategy == strat and r.test_trades]
        if not strat_windows:
            continue

        # 计算WF中位数PF(中位数比均值更抗极端值)
        wf_pfs = [r.test_pf for r in strat_windows if r.test_pf > 0]
        # 剔除极端值(PF>100通常是0亏损的偶然情况)
        wf_pfs_clean = [p for p in wf_pfs if p < 100]
        wf_median_pf = float(np.median(wf_pfs_clean)) if wf_pfs_clean else 0
        wf_avg_wr = np.mean([r.test_wr for r in strat_windows])

        # 过拟合窗口数
        overfit_count = 0
        for r in strat_windows:
            if r.test_pf < full_m["pf"] * 0.5 and r.test_pf > 0:
                overfit_count += 1
        overfit_pct = overfit_count / len(strat_windows) * 100

        print(f"\n  策略{strat}:")
        print(f"    全量PF:       {full_m['pf']:.2f}")
        print(f"    WF中位数PF:   {wf_median_pf:.2f}")
        if full_m["pf"] > 0 and wf_median_pf > 0:
            decay = (1 - wf_median_pf / full_m["pf"]) * 100
            print(f"    PF衰减:       {decay:.0f}%")
        else:
            print(f"    PF衰减:       N/A")
        print(f"    WF平均胜率:   {wf_avg_wr:.1f}%")
        print(f"    过拟合窗口:   {overfit_count}/{len(strat_windows)} ({overfit_pct:.0f}%)")

        if overfit_pct > 50:
            print(f"    🚨 严重过拟合! 超过50%窗口验证PF不足全量PF的一半")
        elif overfit_pct > 25:
            print(f"    ⚠️ 存在过拟合风险, {overfit_pct:.0f}%窗口表现显著下降")
        else:
            print(f"    ✅ 过拟合风险低, 策略在不同时期表现稳定")

    conn.close()
    return results


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Walk-Forward回测")
    parser.add_argument("--train", type=int, default=3, help="训练窗口月数(默认3)")
    parser.add_argument("--test", type=int, default=1, help="验证窗口月数(默认1)")
    parser.add_argument("--strategy", type=str, help="指定策略(A/B/C)")
    args = parser.parse_args()

    strategy = args.strategy if args.strategy else None
    run_walkforward(
        train_months=args.train,
        test_months=args.test,
        strategy=strategy,
    )
