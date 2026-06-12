#!/usr/bin/env python3
"""Daily strategy accounting for paper/shadow promotion decisions.

This script is intentionally research-only. It does not place orders or start
the daemon. It summarizes closed paper trades by strategy_version and records
the result in strategy_performance_daily.
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_ROOT / "data" / "alpha_miner.db"
REPORT_DIR = PROJECT_ROOT / "reports" / "strategy_review"

sys.path.insert(0, str(PROJECT_ROOT))

from src.trader.daemon_config import CURRENT_PERIOD  # noqa: E402
from src.trader.daemon_db import init_tables, infer_strategy_code, get_strategy_metadata  # noqa: E402


@dataclass
class Metrics:
    strategy_code: str
    strategy_version: str
    run_mode: str
    trades: int
    wins: int
    losses: int
    win_rate: float
    gross_profit: float
    gross_loss: float
    profit_factor: float
    expectancy_pct: float
    avg_win_pct: float
    avg_loss_pct: float
    max_loss_pct: float
    net_pnl: float
    recommendation: str


@dataclass
class ShadowMetrics:
    strategy_version: str
    signals: int
    checked_3d: int
    win_rate_3d: float
    profit_factor_3d: float
    expectancy_3d: float
    max_loss_3d: float


def connect(read_only: bool = False) -> sqlite3.Connection:
    if read_only:
        conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro&immutable=1", uri=True)
    else:
        conn = sqlite3.connect(str(DB_PATH), timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()
    return row is not None


def _safe_pf(gross_profit: float, gross_loss: float) -> float:
    if gross_loss == 0:
        return 99.0 if gross_profit > 0 else 0.0
    return gross_profit / abs(gross_loss)


def _recommend(m: Metrics) -> str:
    if m.trades < 30:
        return "keep_sampling"
    if m.profit_factor >= 1.25 and m.expectancy_pct > 0 and m.max_loss_pct >= -8:
        return "eligible_review"
    if m.profit_factor < 1.0 or m.expectancy_pct <= 0:
        return "pause_review"
    return "keep_paper"


def fetch_closed_trades(conn: sqlite3.Connection, as_of: str, period: int) -> list[sqlite3.Row]:
    columns = {
        r["name"] for r in conn.execute("PRAGMA table_info(daemon_positions)").fetchall()
    }
    strategy_code_expr = "strategy_code" if "strategy_code" in columns else "'' AS strategy_code"
    strategy_version_expr = "strategy_version" if "strategy_version" in columns else "'' AS strategy_version"
    run_mode_expr = "run_mode" if "run_mode" in columns else "'paper' AS run_mode"
    return conn.execute(
        f"""
        SELECT code, name, sell_date, signal_type, pnl, pnl_pct,
               {strategy_code_expr}, {strategy_version_expr}, {run_mode_expr}
        FROM daemon_positions
        WHERE status='closed' AND period=? AND sell_date<=?
        ORDER BY sell_date, id
        """,
        (period, as_of),
    ).fetchall()


def build_metrics(rows: list[sqlite3.Row]) -> list[Metrics]:
    grouped: dict[str, list[sqlite3.Row]] = {}
    meta_by_version: dict[str, dict] = {}

    for r in rows:
        strategy_code = r["strategy_code"] or infer_strategy_code(r["signal_type"])
        meta = get_strategy_metadata(strategy_code)
        stored_version = r["strategy_version"]
        version = stored_version or f"legacy_unversioned_{strategy_code or 'unknown'}"
        grouped.setdefault(version, []).append(r)
        meta_by_version[version] = {
            "strategy_code": strategy_code,
            "strategy_version": version,
            "run_mode": r["run_mode"] or ("paper" if not stored_version else meta["run_mode"]),
        }

    metrics: list[Metrics] = []
    for version, items in sorted(grouped.items()):
        pcts = [float(r["pnl_pct"] or 0) for r in items]
        pnls = [float(r["pnl"] or 0) for r in items]
        wins = [x for x in pcts if x > 0]
        losses = [x for x in pcts if x <= 0]
        gross_profit = sum(wins)
        gross_loss = sum(losses)
        avg_win = sum(wins) / len(wins) if wins else 0.0
        avg_loss = sum(losses) / len(losses) if losses else 0.0
        m = Metrics(
            strategy_code=meta_by_version[version]["strategy_code"],
            strategy_version=version,
            run_mode=meta_by_version[version]["run_mode"],
            trades=len(items),
            wins=len(wins),
            losses=len(losses),
            win_rate=len(wins) / len(items) * 100 if items else 0.0,
            gross_profit=gross_profit,
            gross_loss=gross_loss,
            profit_factor=_safe_pf(gross_profit, gross_loss),
            expectancy_pct=sum(pcts) / len(pcts) if pcts else 0.0,
            avg_win_pct=avg_win,
            avg_loss_pct=avg_loss,
            max_loss_pct=min(pcts) if pcts else 0.0,
            net_pnl=sum(pnls),
            recommendation="",
        )
        m.recommendation = _recommend(m)
        metrics.append(m)
    return metrics


def count_shadow(conn: sqlite3.Connection, as_of: str, period: int) -> dict[str, int]:
    if not table_exists(conn, "daemon_shadow_signals"):
        return {}
    rows = conn.execute(
        """
        SELECT strategy_version, COUNT(*) AS cnt
        FROM daemon_shadow_signals
        WHERE period=? AND trade_date<=?
        GROUP BY strategy_version
        """,
        (period, as_of),
    ).fetchall()
    return {r["strategy_version"]: int(r["cnt"]) for r in rows}


def trading_dates(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute(
        "SELECT DISTINCT trade_date FROM daily_price ORDER BY trade_date"
    ).fetchall()
    return [r["trade_date"] for r in rows]


def backfill_shadow_returns(conn: sqlite3.Connection, as_of: str, period: int) -> int:
    """Fill T+1/T+2/T+3 returns for shadow signals when future data exists."""
    if not table_exists(conn, "daemon_shadow_signals"):
        return 0

    dates = trading_dates(conn)
    date_idx = {d: i for i, d in enumerate(dates)}
    rows = conn.execute(
        """
        SELECT id, code, trade_date, trigger_price
        FROM daemon_shadow_signals
        WHERE period=? AND trade_date<=?
          AND trigger_price IS NOT NULL AND trigger_price>0
          AND (
              future_checked_until IS NULL
              OR future_max_ret_3d IS NULL
              OR future_close_ret_3d IS NULL
          )
        """,
        (period, as_of),
    ).fetchall()

    updated = 0
    for r in rows:
        sig_date = r["trade_date"]
        if sig_date not in date_idx:
            continue
        i = date_idx[sig_date]
        params: dict[str, float | str | int | None] = {}
        checked_until = None
        for horizon in (1, 2, 3):
            j = i + horizon
            if j >= len(dates) or dates[j] > as_of:
                params[f"future_max_ret_{horizon}d"] = None
                params[f"future_close_ret_{horizon}d"] = None
                continue
            future_dates = dates[i + 1:j + 1]
            placeholders = ",".join("?" for _ in future_dates)
            hi = conn.execute(
                f"""
                SELECT MAX(high) AS high
                FROM daily_price
                WHERE stock_code=? AND trade_date IN ({placeholders})
                """,
                (r["code"], *future_dates),
            ).fetchone()["high"]
            close_row = conn.execute(
                """
                SELECT close
                FROM daily_price
                WHERE stock_code=? AND trade_date=?
                """,
                (r["code"], dates[j]),
            ).fetchone()
            trigger = float(r["trigger_price"])
            params[f"future_max_ret_{horizon}d"] = (
                round((float(hi) / trigger - 1) * 100, 2) if hi else None
            )
            params[f"future_close_ret_{horizon}d"] = (
                round((float(close_row["close"]) / trigger - 1) * 100, 2)
                if close_row and close_row["close"] else None
            )
            checked_until = dates[j]

        if checked_until is None:
            continue

        conn.execute(
            """
            UPDATE daemon_shadow_signals
            SET future_checked_until=?,
                future_max_ret_1d=?, future_close_ret_1d=?,
                future_max_ret_2d=?, future_close_ret_2d=?,
                future_max_ret_3d=?, future_close_ret_3d=?
            WHERE id=?
            """,
            (
                checked_until,
                params.get("future_max_ret_1d"),
                params.get("future_close_ret_1d"),
                params.get("future_max_ret_2d"),
                params.get("future_close_ret_2d"),
                params.get("future_max_ret_3d"),
                params.get("future_close_ret_3d"),
                r["id"],
            ),
        )
        updated += 1
    conn.commit()
    return updated


def build_shadow_metrics(conn: sqlite3.Connection, as_of: str, period: int) -> list[ShadowMetrics]:
    if not table_exists(conn, "daemon_shadow_signals"):
        return []
    rows = conn.execute(
        """
        SELECT strategy_version, future_close_ret_3d
        FROM daemon_shadow_signals
        WHERE period=? AND trade_date<=?
        """,
        (period, as_of),
    ).fetchall()
    grouped: dict[str, list[float | None]] = {}
    for r in rows:
        grouped.setdefault(r["strategy_version"], []).append(r["future_close_ret_3d"])

    result: list[ShadowMetrics] = []
    for version, values in sorted(grouped.items()):
        checked = [float(v) for v in values if v is not None]
        wins = [v for v in checked if v > 0]
        losses = [v for v in checked if v <= 0]
        gp = sum(wins)
        gl = sum(losses)
        result.append(ShadowMetrics(
            strategy_version=version,
            signals=len(values),
            checked_3d=len(checked),
            win_rate_3d=len(wins) / len(checked) * 100 if checked else 0,
            profit_factor_3d=_safe_pf(gp, gl) if checked else 0,
            expectancy_3d=sum(checked) / len(checked) if checked else 0,
            max_loss_3d=min(checked) if checked else 0,
        ))
    return result


def persist_metrics(conn: sqlite3.Connection, as_of: str, period: int, metrics: list[Metrics]) -> None:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for m in metrics:
        conn.execute(
            """
            INSERT INTO strategy_performance_daily
            (trade_date, period, strategy_code, strategy_version, run_mode,
             trades, wins, losses, win_rate, gross_profit, gross_loss,
             profit_factor, expectancy_pct, avg_win_pct, avg_loss_pct,
             max_loss_pct, net_pnl, recommendation, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(trade_date, period, strategy_version) DO UPDATE SET
                run_mode=excluded.run_mode,
                trades=excluded.trades,
                wins=excluded.wins,
                losses=excluded.losses,
                win_rate=excluded.win_rate,
                gross_profit=excluded.gross_profit,
                gross_loss=excluded.gross_loss,
                profit_factor=excluded.profit_factor,
                expectancy_pct=excluded.expectancy_pct,
                avg_win_pct=excluded.avg_win_pct,
                avg_loss_pct=excluded.avg_loss_pct,
                max_loss_pct=excluded.max_loss_pct,
                net_pnl=excluded.net_pnl,
                recommendation=excluded.recommendation,
                created_at=excluded.created_at
            """,
            (
                as_of, period, m.strategy_code, m.strategy_version, m.run_mode,
                m.trades, m.wins, m.losses, m.win_rate, m.gross_profit,
                m.gross_loss, m.profit_factor, m.expectancy_pct,
                m.avg_win_pct, m.avg_loss_pct, m.max_loss_pct, m.net_pnl,
                m.recommendation, now,
            ),
        )
    conn.commit()


def render_report(
    as_of: str,
    period: int,
    metrics: list[Metrics],
    shadow_counts: dict[str, int],
    shadow_metrics: list[ShadowMetrics],
) -> str:
    lines = [
        f"# Daily Strategy Review - {as_of}",
        "",
        f"Period: {period}",
        "",
        "| Strategy | Mode | Trades | Win% | PF | Expectancy | Net PnL | Max Loss | Shadow | Recommendation |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    if not metrics:
        lines.append("| - | - | 0 | - | - | - | - | - | - | no_closed_trades |")
    for m in metrics:
        lines.append(
            f"| {m.strategy_version} | {m.run_mode} | {m.trades} | "
            f"{m.win_rate:.1f}% | {m.profit_factor:.2f} | "
            f"{m.expectancy_pct:+.2f}% | {m.net_pnl:+.2f} | "
            f"{m.max_loss_pct:+.2f}% | {shadow_counts.get(m.strategy_version, 0)} | "
            f"{m.recommendation} |"
        )

    known_versions = {m.strategy_version for m in metrics}
    for version, cnt in sorted(shadow_counts.items()):
        if version in known_versions:
            continue
        lines.append(f"| {version} | shadow | 0 | - | - | - | - | - | {cnt} | keep_shadow |")

    lines.extend([
        "",
        "## Shadow T+3 Review",
        "",
        "| Strategy | Signals | Checked | Win% | PF | Expectancy | Max Loss |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ])
    if not shadow_metrics:
        lines.append("| - | 0 | 0 | - | - | - | - |")
    for m in shadow_metrics:
        lines.append(
            f"| {m.strategy_version} | {m.signals} | {m.checked_3d} | "
            f"{m.win_rate_3d:.1f}% | {m.profit_factor_3d:.2f} | "
            f"{m.expectancy_3d:+.2f}% | {m.max_loss_3d:+.2f}% |"
        )

    lines.extend([
        "",
        "Promotion rules:",
        "- fewer than 30 closed trades: keep_sampling",
        "- PF >= 1.25, expectancy > 0, max loss >= -8%: eligible_review",
        "- PF < 1 or expectancy <= 0 after 30 trades: pause_review",
    ])
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=date.today().isoformat())
    parser.add_argument("--period", type=int, default=CURRENT_PERIOD)
    parser.add_argument("--no-write", action="store_true", help="Do not persist DB/report output")
    args = parser.parse_args()

    if not args.no_write:
        init_tables()

    conn = connect(read_only=args.no_write)
    try:
        rows = fetch_closed_trades(conn, args.date, args.period)
        metrics = build_metrics(rows)
        shadow_counts = count_shadow(conn, args.date, args.period)
        updated_shadow = 0
        if not args.no_write:
            updated_shadow = backfill_shadow_returns(conn, args.date, args.period)
        shadow_metrics = build_shadow_metrics(conn, args.date, args.period)
        report = render_report(args.date, args.period, metrics, shadow_counts, shadow_metrics)
        print(report)
        if updated_shadow:
            print(f"shadow_returns_updated={updated_shadow}")

        if not args.no_write:
            persist_metrics(conn, args.date, args.period, metrics)
            REPORT_DIR.mkdir(parents=True, exist_ok=True)
            path = REPORT_DIR / f"{args.date}_strategy_review.md"
            path.write_text(report, encoding="utf-8")
            print(f"report_written={path}")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
