"""
策略淘汰机制 — Strategy Elimination Engine v2

修复: 以daemon_positions为主数据源(signal_type更准确)
"""
import sqlite3
from datetime import datetime
from dataclasses import dataclass, field
from typing import Optional
import json
import logging

logger = logging.getLogger(__name__)

DB_PATH = "data/alpha_miner.db"

# === 淘汰阈值 ===
MIN_TRADES_TO_EVAL = 20
MIN_WIN_RATE = 0.45
MIN_PROFIT_FACTOR = 0.80
MAX_CAPITAL_LOSS_PCT = -0.15
EVAL_WINDOW_DAYS = 30

# 策略配置(signal_type from daemon_positions)
STRATEGY_CONFIG = {
    "策略A": {"initial": 30_000, "label": "龙头首阴", "signals": ["策略A", "首阴日内", "龙头首阴"]},
    "策略B": {"initial": 30_000, "label": "首板低开反弹", "signals": ["策略B", "回踩低吸(策略B)", "回踩低吸", "低开反弹(策略B)", "低开反弹", "涨停低吸", "板块补涨", "涨停确认"]},  # [GUARD-BYPASS] 2026-05-26: 完整signal_type
    "策略C": {"initial": 30_000, "label": "趋势牛股", "signals": ["趋势牛股(策略C)", "策略C", "趋势牛股", "缩量反包"]},
}


@dataclass
class StrategyEval:
    strategy: str
    label: str
    total_trades: int = 0
    closed_trades: int = 0
    win_trades: int = 0
    loss_trades: int = 0
    win_rate: float = 0.0
    total_pnl: float = 0.0
    avg_pnl: float = 0.0
    profit_factor: float = 0.0
    initial_capital: float = 0.0
    capital_loss_pct: float = 0.0
    status: str = "active"
    cards: list = field(default_factory=list)
    evaluable: bool = False
    recommendation: str = ""
    details: str = ""


def evaluate_strategy(strategy_name: str, period: Optional[int] = None) -> StrategyEval:
    config = STRATEGY_CONFIG.get(strategy_name)
    if not config:
        raise ValueError(f"未知策略: {strategy_name}")

    conn = sqlite3.connect(DB_PATH)

    if period is None:
        row = conn.execute(
            "SELECT period FROM daemon_account ORDER BY date DESC LIMIT 1"
        ).fetchone()
        period = row[0] if row else 3

    signals = config["signals"]
    placeholders = ",".join(["?"] * len(signals))

    # 主数据源: daemon_positions (signal_type准确, pnl准确)
    positions = conn.execute(
        f"SELECT code, signal_type, buy_price, sell_price, pnl, pnl_pct, status, sell_reason "
        f"FROM daemon_positions WHERE period=? AND signal_type IN ({placeholders})",
        [period] + signals
    ).fetchall()

    # 也统计所有period的累计数据
    all_positions = conn.execute(
        f"SELECT code, signal_type, buy_price, sell_price, pnl, pnl_pct, status, sell_reason "
        f"FROM daemon_positions WHERE signal_type IN ({placeholders})",
        signals
    ).fetchall()

    conn.close()

    def _calc(pos_list):
        # SELECT: code(0), signal_type(1), buy_price(2), sell_price(3), pnl(4), pnl_pct(5), status(6), sell_reason(7)
        closed = [p for p in pos_list if p[6] == "closed"]
        total = len(pos_list)
        if not closed:
            return total, 0, 0, 0, 0.0, 0.0, 0.0, 0.0, 0.0
        wins = [p for p in closed if p[4] is not None and p[4] > 0]
        losses = [p for p in closed if p[4] is not None and p[4] <= 0]
        win_trades = len(wins)
        loss_trades = len(losses)
        win_rate = win_trades / len(closed) if closed else 0
        total_pnl = sum(p[4] or 0 for p in closed)
        avg_pnl = total_pnl / len(closed)
        total_win = sum(p[4] or 0 for p in wins)
        total_loss = abs(sum(p[4] or 0 for p in losses))
        pf = total_win / total_loss if total_loss > 0 else float("inf")
        capital_loss = total_pnl / config["initial"]
        return total, len(closed), win_trades, loss_trades, win_rate, total_pnl, avg_pnl, pf, capital_loss

    # 当前period
    total, closed_n, win_n, loss_n, wr, tpnl, apnl, pf, closs = _calc(positions)
    # 累计所有period(淘汰判定用累计)
    all_total, all_closed, all_win, all_loss, all_wr, all_tpnl, _, all_pf, all_closs = _calc(all_positions)

    result = StrategyEval(
        strategy=strategy_name,
        label=config["label"],
        initial_capital=config["initial"],
        total_trades=all_total,
        closed_trades=all_closed,
        win_trades=all_win,
        loss_trades=all_loss,
        win_rate=all_wr,
        total_pnl=all_tpnl,
        avg_pnl=all_tpnl / all_closed if all_closed > 0 else 0.0,
        profit_factor=all_pf,
        capital_loss_pct=all_closs,
        evaluable=all_closed >= MIN_TRADES_TO_EVAL,
    )

    # 淘汰判定(使用累计数据)
    cards = []
    if result.evaluable:
        if result.win_rate < MIN_WIN_RATE:
            cards.append(f"胜率{result.win_rate:.1%}<{MIN_WIN_RATE:.0%}")
        if 0 < result.profit_factor < MIN_PROFIT_FACTOR:
            cards.append(f"PF={result.profit_factor:.2f}<{MIN_PROFIT_FACTOR}")
        if result.capital_loss_pct < MAX_CAPITAL_LOSS_PCT:
            cards.append(f"资金亏损{result.capital_loss_pct:.1%}>{MAX_CAPITAL_LOSS_PCT:.0%}")
            result.status = "red_card"
        elif cards:
            result.status = "yellow_card"

    result.cards = cards

    # 附带当期+累计信息
    period_info = f"当期{closed_n}笔" if closed_n > 0 else "当期无交易"
    cumulative = f"累计{all_period_str(all_total, all_closed, all_wr, all_pf, all_tpnl)}"

    if not result.evaluable:
        result.recommendation = f"样本不足({all_closed}/{MIN_TRADES_TO_EVAL}), 继续积累 [{cumulative}]"
        result.details = f"{period_info}, {cumulative}"
    elif result.status == "red_card":
        result.recommendation = f"红牌! 建议暂停替换 [{cumulative}]"
        result.details = "; ".join(cards)
    elif result.status == "yellow_card":
        result.recommendation = f"黄牌: 密切监控 [{cumulative}]"
        result.details = "; ".join(cards)
    else:
        result.recommendation = f"运行正常 [{cumulative}]"
        result.details = f"胜率{all_wr:.1%}, PF={all_pf:.2f}, pnl={all_tpnl:.0f}"

    return result


def all_period_str(total, closed, wr, pf, pnl):
    return f"{total}笔/{closed}平仓/胜率{wr:.0%}/PF={pf:.2f}/pnl={pnl:.0f}"


def evaluate_all(period=None):
    return [evaluate_strategy(n, period) for n in STRATEGY_CONFIG]


def generate_elimination_report(period=None):
    conn = sqlite3.connect(DB_PATH)
    if period is None:
        row = conn.execute(
            "SELECT period FROM daemon_account ORDER BY date DESC LIMIT 1"
        ).fetchone()
        period = row[0] if row else 3
    conn.close()

    results = evaluate_all(period)
    actions = []
    summary_parts = []

    for r in results:
        icon = {"active": "🟢", "yellow_card": "🟡", "red_card": "🔴"}.get(r.status, "⚪")
        sample_tag = "" if r.evaluable else f"[样本{r.closed_trades}笔]"
        summary_parts.append(
            f"{icon} {r.strategy}({r.label}): "
            f"{r.closed_trades}笔平仓, 胜率{r.win_rate:.0%}, PF={r.profit_factor:.2f}, "
            f"pnl={r.total_pnl:.0f} {sample_tag} — {r.recommendation}"
        )
        if r.status == "red_card":
            actions.append(f"🔴 {r.strategy}({r.label}): {r.recommendation}")
        elif r.status == "yellow_card":
            actions.append(f"🟡 {r.strategy}({r.label}): {r.recommendation}")

    if not actions:
        actions.append("所有策略样本不足或运行正常，无需操作")

    return {
        "timestamp": datetime.now().isoformat(),
        "period": period,
        "thresholds": {
            "min_trades": MIN_TRADES_TO_EVAL,
            "min_win_rate": MIN_WIN_RATE,
            "min_pf": MIN_PROFIT_FACTOR,
            "max_capital_loss": MAX_CAPITAL_LOSS_PCT,
            "eval_window_days": EVAL_WINDOW_DAYS,
        },
        "strategies": [
            {
                "name": r.strategy,
                "label": r.label,
                "closed_trades": r.closed_trades,
                "win_rate": round(r.win_rate, 3),
                "profit_factor": round(r.profit_factor, 3) if r.profit_factor != float("inf") else "inf",
                "total_pnl": round(r.total_pnl, 1),
                "capital_loss_pct": round(r.capital_loss_pct, 4),
                "status": r.status,
                "evaluable": r.evaluable,
                "recommendation": r.recommendation,
                "details": r.details,
            }
            for r in results
        ],
        "summary": "\n".join(summary_parts),
        "actions": actions,
    }


if __name__ == "__main__":
    report = generate_elimination_report()
    print(json.dumps(report, ensure_ascii=False, indent=2))
