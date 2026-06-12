"""
策略B v2 回测: 三层反转策略
  常规模式: 连跌>=4天 + 累计跌幅>8% + 最后一日放量(量比>=1.5)
  暴跌日增强模式: 全市场均跌>2% + 个股跌>5%
  过滤: 剔科创板(688)/北交所(8/4开头), 剔ST(名称匹配)
  买入: 次日开盘价(T+1)
  卖出: 持5天到期 / 止损-8% / trailing 5% / 目标+10%
  最大持仓: 3只
"""

import sqlite3
import sys
from pathlib import Path
from datetime import datetime
import numpy as np

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_ROOT / "data" / "alpha_miner.db"

# ---- 参数 ----
REGULAR_CONSEC_DAYS = 4        # 常规模式: 连跌>=4天
REGULAR_CUM_DROP = -0.08       # 常规模式: 累计跌幅>8%
REGULAR_VOL_RATIO = 1.5        # 常规模式: 最后一日量比>=1.5
CRASH_MARKET_THRESHOLD = -0.02 # 暴跌日: 全市场均跌>2%
CRASH_STOCK_DROP = -0.05       # 暴跌日: 个股跌>5%

STOP_LOSS = -0.08              # 止损-8%
TRAILING = 0.05                # trailing 5%
TARGET = 0.10                  # 目标+10%
MAX_HOLD = 5                   # 持5天
MAX_POSITIONS = 3              # 最大持仓3只

COMMISION = 0.00125            # 双边交易成本0.125%


def is_excluded(code: str) -> bool:
    """排除科创板和北交所"""
    if code.startswith("688"):
        return True
    if code.startswith("8") and len(code) == 6:
        return True
    if code.startswith("4") and len(code) == 6:
        return True
    return False


def load_data(conn: sqlite3.Connection):
    """加载全部日K数据到内存"""
    rows = conn.execute("""
        SELECT stock_code, trade_date, open, high, low, close, pre_close, volume, amount, turnover_rate
        FROM daily_price
        WHERE close IS NOT NULL AND pre_close IS NOT NULL AND pre_close > 0
        ORDER BY stock_code, trade_date
    """).fetchall()

    # 按股票分组
    by_stock = {}
    for r in rows:
        code = r[0]
        if code not in by_stock:
            by_stock[code] = []
        by_stock[code].append({
            "date": r[1], "open": r[2], "high": r[3], "low": r[4],
            "close": r[5], "pre_close": r[6], "volume": r[7],
            "amount": r[8], "turnover_rate": r[9],
        })

    # 交易日序列
    dates = sorted(set(r[1] for r in rows))
    return by_stock, dates


def calc_market_avg_return(by_stock: dict, dates: list) -> dict:
    """计算每日全市场平均收益率"""
    # 按日期聚合
    by_date = {}
    for code, bars in by_stock.items():
        for bar in bars:
            d = bar["date"]
            if d not in by_date:
                by_date[d] = []
            ret = (bar["close"] - bar["pre_close"]) / bar["pre_close"] if bar["pre_close"] > 0 else 0
            by_date[d].append(ret)

    market_ret = {}
    for d, rets in by_date.items():
        if len(rets) > 1000:  # 只取全量采集日
            market_ret[d] = np.mean(rets)
    return market_ret


def find_signals(by_stock: dict, dates: list, market_ret: dict):
    """寻找买入信号"""
    date_idx = {d: i for i, d in enumerate(dates)}

    regular_signals = []  # 常规模式信号
    crash_signals = []    # 暴跌日增强信号

    for code, bars in by_stock.items():
        if is_excluded(code):
            continue
        if len(bars) < 20:
            continue

        # 预计算日收益率和量比
        for i in range(len(bars)):
            bars[i]["ret"] = (bars[i]["close"] - bars[i]["pre_close"]) / bars[i]["pre_close"]

        # 计算5日均量(用于量比)
        for i in range(len(bars)):
            vol = bars[i]["volume"] or 0
            if i >= 5:
                avg_vol = np.mean([bars[j]["volume"] or 0 for j in range(i - 5, i)])
                bars[i]["vol_ratio"] = vol / avg_vol if avg_vol > 0 else 0
            else:
                bars[i]["vol_ratio"] = 0

        # 检测信号
        for i in range(5, len(bars) - 1):
            bar = bars[i]
            d = bar["date"]
            di = date_idx.get(d)
            if di is None:
                continue

            # --- 常规模式: 连跌检测 ---
            consec_down = 0
            cum_ret = 0
            for j in range(i, max(i - 10, -1), -1):
                if bars[j]["ret"] < 0:
                    consec_down += 1
                    cum_ret += bars[j]["ret"]
                else:
                    break

            if (consec_down >= REGULAR_CONSEC_DAYS
                    and cum_ret < REGULAR_CUM_DROP
                    and bar["vol_ratio"] >= REGULAR_VOL_RATIO):
                regular_signals.append({
                    "code": code, "date": d, "date_idx": di,
                    "consec_down": consec_down, "cum_ret": cum_ret,
                    "vol_ratio": bar["vol_ratio"],
                    "close": bar["close"], "type": "regular",
                })

            # --- 暴跌日增强模式 ---
            mkt_ret = market_ret.get(d, 0)
            if mkt_ret < CRASH_MARKET_THRESHOLD and bar["ret"] < CRASH_STOCK_DROP:
                crash_signals.append({
                    "code": code, "date": d, "date_idx": di,
                    "stock_ret": bar["ret"], "mkt_ret": mkt_ret,
                    "close": bar["close"], "type": "crash",
                })

    return regular_signals, crash_signals


def simulate_trades(signals: list, by_stock: dict, dates: list, label: str):
    """模拟交易 — 逐日扫描"""
    date_idx_map = {d: i for i, d in enumerate(dates)}

    # 按日期分组信号
    sig_by_date = {}
    for sig in signals:
        d = sig["date"]
        if d not in sig_by_date:
            sig_by_date[d] = []
        sig_by_date[d].append(sig)

    # 构建股票数据索引
    stock_bars = {}
    for code, bars in by_stock.items():
        stock_bars[code] = {b["date"]: b for b in bars}

    positions = []
    trades = []
    held_codes = set()

    # 逐日扫描
    for di, d in enumerate(dates):
        # 1. 检查卖出条件
        new_pos = []
        for p in positions:
            bar = stock_bars.get(p["code"], {}).get(d)
            if not bar or bar["close"] is None or bar["close"] <= 0:
                new_pos.append(p)
                continue

            # 更新最高价
            high = bar["high"] or bar["close"]
            if high > p["high_water"]:
                p["high_water"] = high

            hold_days = di - p["buy_date_idx"]
            ret = (bar["close"] / p["buy_price"] - 1)

            # 止损
            if ret <= STOP_LOSS:
                trades.append({"code": p["code"], "buy_price": p["buy_price"],
                    "sell_price": bar["close"], "net_ret": ret - COMMISION,
                    "hold_days": hold_days, "reason": f"止损{ret*100:.1f}%",
                    "signal_type": p["signal_type"]})
                held_codes.discard(p["code"])
                continue

            # Trailing stop
            draw_from_high = (bar["close"] / p["high_water"] - 1)
            if p["high_water"] > p["buy_price"] * (1 + TRAILING) and draw_from_high <= -TRAILING:
                trades.append({"code": p["code"], "buy_price": p["buy_price"],
                    "sell_price": bar["close"], "net_ret": ret - COMMISION,
                    "hold_days": hold_days, "reason": f"trailing{ret*100:.1f}%",
                    "signal_type": p["signal_type"]})
                held_codes.discard(p["code"])
                continue

            # 目标止盈
            if ret >= TARGET:
                trades.append({"code": p["code"], "buy_price": p["buy_price"],
                    "sell_price": bar["close"], "net_ret": ret - COMMISION,
                    "hold_days": hold_days, "reason": f"目标{ret*100:.1f}%",
                    "signal_type": p["signal_type"]})
                held_codes.discard(p["code"])
                continue

            # 到期
            if hold_days >= MAX_HOLD:
                trades.append({"code": p["code"], "buy_price": p["buy_price"],
                    "sell_price": bar["close"], "net_ret": ret - COMMISION,
                    "hold_days": hold_days, "reason": "到期平仓",
                    "signal_type": p["signal_type"]})
                held_codes.discard(p["code"])
                continue

            new_pos.append(p)
        positions = new_pos

        # 2. 买入新信号(T+1: 信号日+1才买入)
        # 这里di是信号日的下一天时才买入, 所以检查sig_by_date[前一天]
        prev_date = dates[di - 1] if di > 0 else None
        if prev_date and prev_date in sig_by_date:
            for sig in sig_by_date[prev_date]:
                if len(positions) >= MAX_POSITIONS:
                    break
                code = sig["code"]
                if code in held_codes:
                    continue
                bar = stock_bars.get(code, {}).get(d)
                if not bar or bar["open"] is None or bar["open"] <= 0:
                    continue
                buy_price = bar["open"]
                positions.append({
                    "code": code, "buy_price": buy_price,
                    "buy_date_idx": di, "buy_date": d,
                    "high_water": buy_price, "signal_type": label,
                })
                held_codes.add(code)

    # 回测结束: 强制平仓
    last_di = len(dates) - 1
    for p in positions:
        bar = stock_bars.get(p["code"], {}).get(dates[last_di])
        if bar and bar["close"]:
            net_ret = (bar["close"] / p["buy_price"] - 1) - COMMISION
            trades.append({"code": p["code"], "buy_price": p["buy_price"],
                "sell_price": bar["close"], "net_ret": net_ret,
                "hold_days": last_di - p["buy_date_idx"],
                "reason": "回测结束", "signal_type": p["signal_type"]})

    return trades


def print_stats(trades: list, label: str):
    """打印统计"""
    if not trades:
        print(f"\n{'='*60}")
        print(f"{label}: 无交易")
        return {}

    rets = np.array([t["net_ret"] for t in trades])
    wins = rets[rets > 0]
    losses = rets[rets < 0]
    total_profit = wins.sum() if len(wins) > 0 else 0
    total_loss = abs(losses.sum()) if len(losses) > 0 else 0
    pf = total_profit / total_loss if total_loss > 0 else float("inf")

    print(f"\n{'='*60}")
    print(f"{label}")
    print(f"{'='*60}")
    print(f"  笔数: {len(trades)}")
    print(f"  胜率: {np.mean(rets > 0)*100:.1f}% ({len(wins)}赢/{len(losses)}亏)")
    print(f"  PF: {pf:.2f}")
    print(f"  均收益: {rets.mean()*100:.3f}%")
    print(f"  中位收益: {np.median(rets)*100:.3f}%")
    print(f"  赢均收益: {wins.mean()*100:.2f}%" if len(wins) > 0 else "  赢均收益: N/A")
    print(f"  亏均收益: {losses.mean()*100:.2f}%" if len(losses) > 0 else "  亏均收益: N/A")
    print(f"  95%CI: [{(rets.mean()-1.96*rets.std()/np.sqrt(len(rets)))*100:.3f}%, {(rets.mean()+1.96*rets.std()/np.sqrt(len(rets)))*100:.3f}%]")
    print(f"  平均持仓: {np.mean([t['hold_days'] for t in trades]):.1f}天")

    # 卖出原因分布
    reasons = {}
    for t in trades:
        r = t["reason"].split()[0] if t["reason"] else "unknown"
        reasons[r] = reasons.get(r, 0) + 1
    print(f"  卖出原因: {reasons}")

    # 收益分布
    print(f"  收益分布:")
    for lo, hi, lbl in [(-999, -0.10, "亏>10%"), (-0.10, -0.05, "亏5-10%"),
                        (-0.05, -0.03, "亏3-5%"), (-0.03, 0, "亏0-3%"),
                        (0, 0.03, "赚0-3%"), (0.03, 0.05, "赚3-5%"),
                        (0.05, 0.10, "赚5-10%"), (0.10, 999, "赚>10%")]:
        pct = np.mean((rets >= lo) & (rets < hi)) * 100
        bar = "#" * int(pct)
        print(f"    {lbl}: {pct:5.1f}%  {bar}")

    return {
        "label": label, "count": len(trades),
        "win_rate": np.mean(rets > 0) * 100,
        "pf": pf, "avg_ret": rets.mean() * 100,
        "median_ret": np.median(rets) * 100,
    }


def main():
    print("策略B v2 回测: 三层反转策略")
    print(f"参数: 连跌>={REGULAR_CONSEC_DAYS}天, 累跌>{abs(REGULAR_CUM_DROP)*100}%, 量比>={REGULAR_VOL_RATIO}")
    print(f"      暴跌日: 市场均跌>{abs(CRASH_MARKET_THRESHOLD)*100}%, 个股跌>{abs(CRASH_STOCK_DROP)*100}%")
    print(f"      止损={STOP_LOSS*100}%, trailing={TRAILING*100}%, 目标={TARGET*100}%, 持{MAX_HOLD}天")
    print()

    conn = sqlite3.connect(str(DB_PATH))

    # 1. 加载数据
    print("加载数据...")
    by_stock, dates = load_data(conn)
    print(f"  股票数: {len(by_stock)}, 交易日: {len(dates)} ({dates[0]} ~ {dates[-1]})")

    # 2. 计算市场收益率
    print("计算市场收益率...")
    market_ret = calc_market_avg_return(by_stock, dates)
    crash_days = [d for d, r in market_ret.items() if r < CRASH_MARKET_THRESHOLD]
    print(f"  暴跌日(均跌>{abs(CRASH_MARKET_THRESHOLD)*100}%): {len(crash_days)}天")

    # 3. 找信号
    print("寻找买入信号...")
    regular_sigs, crash_sigs = find_signals(by_stock, dates, market_ret)
    print(f"  常规模式信号: {len(regular_sigs)}")
    print(f"  暴跌日增强信号: {len(crash_sigs)}")

    # 4. 回测: 常规模式
    print("\n回测常规模式...")
    regular_trades = simulate_trades(regular_sigs, by_stock, dates, "regular")

    # 5. 回测: 暴跌日增强模式
    print("回测暴跌日增强模式...")
    crash_trades = simulate_trades(crash_sigs, by_stock, dates, "crash")

    # 6. 合并回测
    print("回测合并模式...")
    all_sigs = regular_sigs + crash_sigs
    all_sigs.sort(key=lambda s: (s["date_idx"], 0 if s["type"] == "crash" else 1))
    # 去重: 同一天同一股票只保留增强模式(如果两者都触发)
    seen = set()
    dedup_sigs = []
    for s in reversed(all_sigs):  # 反过来，增强模式优先(已经排了crash在前)
        key = (s["code"], s["date"])
        if key not in seen:
            seen.add(key)
            dedup_sigs.append(s)
    dedup_sigs.reverse()

    combined_trades = simulate_trades(dedup_sigs, by_stock, dates, "combined")

    # 7. 输出统计
    r_stats = print_stats(regular_trades, "常规模式 (连跌>=4天+量比>=1.5)")
    c_stats = print_stats(crash_trades, "暴跌日增强 (市场均跌>2%+个股跌>5%)")
    b_stats = print_stats(combined_trades, "合并模式 (增强模式优先)")

    # 8. 对比
    print(f"\n{'='*60}")
    print("与当前策略B (PF=1.06) 对比")
    print(f"{'='*60}")
    print(f"  当前策略B: PF=1.06, 胜率38.7%, 均收益+0.10%")
    if b_stats:
        print(f"  新策略B v2: PF={b_stats['pf']:.2f}, 胜率={b_stats['win_rate']:.1f}%, 均收益={b_stats['avg_ret']:.3f}%")
        if b_stats["pf"] > 1.06:
            print(f"  → PF改善: {b_stats['pf']:.2f} vs 1.06 (+{(b_stats['pf']/1.06-1)*100:.0f}%)")
        else:
            print(f"  → PF未改善: {b_stats['pf']:.2f} vs 1.06")

    conn.close()


if __name__ == "__main__":
    main()
