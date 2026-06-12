"""monitor_live.py — 盘中实时监控

每30秒汇总一次: 持仓盈亏/策略候选/情绪状态/熔断状态
异常高亮: 止损触发/熔断/候选为0/大幅盈亏

用法: uv run python scripts/monitor_live.py
"""
from __future__ import annotations

import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

DB_PATH = Path(__file__).resolve().parents[1] / "data" / "alpha_miner.db"

# ANSI colors
RED = "\033[91m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
BOLD = "\033[1m"
RESET = "\033[0m"


def _conn():
    c = sqlite3.connect(str(DB_PATH), timeout=10)
    c.row_factory = sqlite3.Row
    return c


def _get_account(conn) -> dict:
    r = conn.execute(
        "SELECT * FROM daemon_account WHERE period=3 ORDER BY date DESC LIMIT 1"
    ).fetchone()
    return dict(r) if r else {}


def _get_positions(conn) -> list:
    return [dict(r) for r in conn.execute(
        "SELECT * FROM daemon_positions WHERE status='held' AND period=3 ORDER BY buy_time"
    ).fetchall()]


def _get_realtime_price(codes: list[str]) -> dict:
    if not codes:
        return {}
    try:
        from src.trader.realtime_quote import get_realtime
        return get_realtime(codes)
    except Exception:
        return {}


def _get_emotion() -> dict:
    try:
        from src.trader.daemon_strategies import get_market_emotion
        return get_market_emotion()
    except Exception:
        return {"phase": "?", "can_buy": True}


def _get_circuit_breaker(conn) -> str:
    r = conn.execute(
        "SELECT message FROM daemon_log WHERE log_level='WARNING' "
        "AND message LIKE '%熔断%' AND log_time >= date('now') "
        "ORDER BY log_time DESC LIMIT 1"
    ).fetchone()
    return r[0] if r else ""


def _color_pnl(val: float, fmt: str = "+.1f") -> str:
    s = f"{val:{fmt}}"
    if val > 0:
        return f"{GREEN}{s}{RESET}"
    elif val < 0:
        return f"{RED}{s}{RESET}"
    return s


def scan_once():
    conn = _conn()
    try:
        acct = _get_account(conn)
        positions = _get_positions(conn)
        cb_msg = _get_circuit_breaker(conn)
    finally:
        conn.close()

    now = datetime.now().strftime("%H:%M:%S")
    emotion = _get_emotion()

    # 持仓盈亏
    codes = [p["code"] for p in positions]
    quotes = _get_realtime_price(codes)

    pos_details = []
    total_unrealized = 0.0
    alerts = []

    for p in positions:
        code = p["code"]
        name = p.get("name", code)
        buy_p = p.get("buy_price", 0)
        shares = p.get("shares", 0)
        highest = p.get("highest_price", buy_p)

        q = quotes.get(code, {})
        cur_p = q.get("price", 0)
        chg_pct = q.get("change_pct_calc", 0) or 0

        if cur_p > 0 and buy_p > 0:
            pnl_pct = (cur_p / buy_p - 1) * 100
            unrealized = (cur_p - buy_p) * shares
            total_unrealized += unrealized

            # 更新最高价显示
            if cur_p > highest:
                highest = cur_p

            # trailing 计算
            signal_type = p.get("signal_type", "")
            if "策略A" in signal_type or "首阴" in signal_type:
                strat = "A"
            elif "策略B" in signal_type or "低开" in signal_type or "回踩" in signal_type:
                strat = "B"
            elif "策略C" in signal_type or "趋势" in signal_type:
                strat = "C"
            else:
                strat = "B"

            from src.trader.daemon_config import SELL_PARAMS
            trail_pct = SELL_PARAMS[strat].get("trailing_stop_pct", 0.05)
            phase = emotion.get("phase", "正常")
            if phase in ("冰点", "偏弱", "退潮预警"):
                trail_pct = SELL_PARAMS[strat].get("trailing_frost_pct", trail_pct)
            elif phase == "退潮":
                trail_pct = SELL_PARAMS[strat].get("trailing_ebb_pct", trail_pct)

            trail_trigger = highest * (1 - trail_pct) if highest > 0 else 0
            drawdown_pct = (cur_p / highest - 1) * 100 if highest > 0 else 0

            # 警告
            pnl_str = _color_pnl(pnl_pct)
            detail = f"  {code} {name:<6s} {strat} 买入¥{buy_p:.2f} 现¥{cur_p:.2f} {pnl_str}% 高¥{highest:.2f} trail{trail_pct*100:.0f}%→¥{trail_trigger:.2f}"

            if pnl_pct <= -5:
                alerts.append(f"{RED}[止损警告] {code} {name} {pnl_pct:+.1f}%{RESET}")
            elif drawdown_pct <= -trail_pct * 80:
                alerts.append(f"{YELLOW}[接近trailing] {code} {name} 从高{highest:.2f}回落{drawdown_pct:.1f}%{RESET}")
            elif pnl_pct >= 5:
                detail = f"{GREEN}{detail}{RESET}"

            pos_details.append(detail)
        else:
            pos_details.append(f"  {code} {name} (无实时报价)")

    # 账户汇总
    cash = acct.get("cash", 0)
    daily_pnl = acct.get("daily_pnl", 0)
    mv = acct.get("market_value", 0)
    total = acct.get("total_assets", 0)

    # 熔断状态
    phase = emotion.get("phase", "?")
    can_buy = emotion.get("can_buy", True)

    phase_color = GREEN if phase == "正常" else (YELLOW if phase in ("偏弱", "退潮预警") else RED)

    # 输出
    print(f"\n{BOLD}{'='*70}{RESET}")
    print(f"{BOLD} {now}{RESET}  账户¥{total:,.0f} 现金¥{cash:,.0f} 市值¥{mv:,.0f}")
    print(f"  今日盈亏: {_color_pnl(daily_pnl, '+.0f')}元  持仓{len(positions)}只  "
          f"情绪: {phase_color}{phase}{RESET}  买入: {'✓' if can_buy else f'{RED}✗{RESET}'}")

    if pos_details:
        print(f"  {'─'*66}")
        for d in pos_details:
            print(d)

    # 候选数(轻量查询, 不调用策略函数)
    try:
        conn2 = _conn()
        log_entries = conn2.execute(
            "SELECT message FROM daemon_log WHERE log_time >= date('now') AND "
            "(message LIKE '%策略A%候选%' OR message LIKE '%策略C%趋势牛股v2%') "
            "ORDER BY log_time DESC LIMIT 2"
        ).fetchall()
        conn2.close()
        for entry in log_entries:
            msg = entry[0]
            if "趋势牛股v2" in msg:
                print(f"  {CYAN}[C] {msg}{RESET}")
            else:
                print(f"  {CYAN}[A] {msg}{RESET}")
    except Exception:
        pass

    # 警告
    if alerts:
        print(f"  {'─'*66}")
        for a in alerts:
            print(a)

    if cb_msg:
        print(f"  {RED}[熔断] {cb_msg}{RESET}")

    if not positions:
        print(f"  {YELLOW}(无持仓){RESET}")


def main():
    print(f"{BOLD}Alpha Miner 盘中监控{RESET}  (Ctrl+C退出)")
    try:
        while True:
            scan_once()
            time.sleep(30)
    except KeyboardInterrupt:
        print(f"\n{YELLOW}监控结束{RESET}")


if __name__ == "__main__":
    main()
