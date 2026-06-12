#!/usr/bin/env python3
"""Analyze winners the trading system missed.

This script is intentionally read-only. It answers three questions for a
given trade date:

1. Which stocks rose strongly over the next N trading days?
2. Which of those were actually bought by the daemon on the trade date?
3. What common setup features did the missed winners have?

It does not try to reconstruct historical strategy candidates yet, because
candidate snapshots are not persisted. The next improvement should be a
candidate_snapshot table written by the daemon before filtering.
"""

from __future__ import annotations

import argparse
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from statistics import median


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_ROOT / "data" / "alpha_miner.db"
DEFAULT_OUT_DIR = PROJECT_ROOT / "output" / "review"


@dataclass
class Winner:
    code: str
    name: str
    industry: str
    close: float
    future_close: float
    future_high: float
    ret_close: float
    ret_high: float
    day_ret: float | None
    ma20_dist: float | None
    ma60_dist: float | None
    ret20: float | None
    ret60: float | None
    amount_ratio20: float | None
    turnover: float | None
    bought: bool
    buy_signal: str


def connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def trading_dates(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute("SELECT DISTINCT trade_date FROM daily_price ORDER BY trade_date").fetchall()
    return [r[0] for r in rows]


def resolve_window(dates: list[str], date: str, horizon: int) -> tuple[str, list[str]]:
    if date not in dates:
        raise SystemExit(f"trade date {date} not found in daily_price")
    i = dates.index(date)
    future = dates[i + 1 : i + 1 + horizon]
    if not future:
        raise SystemExit(f"no future trading dates after {date}; cannot evaluate horizon={horizon}")
    return future[-1], future


def stock_name(conn: sqlite3.Connection, code: str) -> str:
    row = conn.execute(
        """
        SELECT name FROM zt_pool WHERE stock_code=? AND name IS NOT NULL AND name!=''
        ORDER BY trade_date DESC LIMIT 1
        """,
        (code,),
    ).fetchone()
    return row[0] if row else code


def stock_industry(conn: sqlite3.Connection, code: str) -> str:
    row = conn.execute(
        "SELECT industry_name FROM stock_industry_mapping WHERE stock_code=? LIMIT 1",
        (code,),
    ).fetchone()
    return row[0] if row else ""


def get_buys(conn: sqlite3.Connection, date: str, period: int) -> dict[str, str]:
    rows = conn.execute(
        """
        SELECT code, signal_type
        FROM daemon_trades
        WHERE action='buy' AND trade_date=? AND period=?
          AND reason NOT LIKE '%作废%' AND reason NOT LIKE '%撤销%'
        """,
        (date, period),
    ).fetchall()
    result: dict[str, str] = {}
    for r in rows:
        result.setdefault(r["code"], r["signal_type"] or "")
    return result


def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()
    return row is not None


def get_candidates(conn: sqlite3.Connection, date: str, period: int) -> dict[str, str]:
    if not table_exists(conn, "daemon_candidate_snapshots"):
        return {}
    rows = conn.execute(
        """
        SELECT code, strategy
        FROM daemon_candidate_snapshots
        WHERE trade_date=? AND period=?
        """,
        (date, period),
    ).fetchall()
    result: dict[str, str] = {}
    for r in rows:
        result.setdefault(r["code"], r["strategy"] or "")
    return result


def history_values(
    conn: sqlite3.Connection, code: str, date: str, lookback: int
) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT trade_date, close, amount
        FROM daily_price
        WHERE stock_code=? AND trade_date<=? AND close IS NOT NULL
        ORDER BY trade_date DESC LIMIT ?
        """,
        (code, date, lookback),
    ).fetchall()


def avg(values: list[float]) -> float | None:
    values = [v for v in values if v is not None]
    return sum(values) / len(values) if values else None


def pct(a: float | None, b: float | None) -> float | None:
    if a is None or b in (None, 0):
        return None
    return (a / b - 1) * 100


def features(conn: sqlite3.Connection, row: sqlite3.Row, date: str) -> dict[str, float | None]:
    code = row["stock_code"]
    close = row["close"]
    pre_close = row["pre_close"]
    hist = history_values(conn, code, date, 80)
    closes = [r["close"] for r in reversed(hist)]
    amounts = [r["amount"] for r in reversed(hist)]

    ma20 = avg(closes[-20:]) if len(closes) >= 20 else None
    ma60 = avg(closes[-60:]) if len(closes) >= 60 else None
    prev20 = closes[-21] if len(closes) >= 21 else None
    prev60 = closes[-61] if len(closes) >= 61 else None
    avg_amount20 = avg(amounts[-21:-1]) if len(amounts) >= 21 else None

    return {
        "day_ret": pct(close, pre_close),
        "ma20_dist": pct(close, ma20),
        "ma60_dist": pct(close, ma60),
        "ret20": pct(close, prev20),
        "ret60": pct(close, prev60),
        "amount_ratio20": (row["amount"] / avg_amount20) if row["amount"] and avg_amount20 else None,
        "turnover": row["turnover_rate"],
    }


def find_winners(
    conn: sqlite3.Connection,
    date: str,
    future_dates: list[str],
    min_return: float,
    period: int,
) -> list[Winner]:
    end_date = future_dates[-1]
    buys = get_buys(conn, date, period)

    rows = conn.execute(
        """
        SELECT d.stock_code, d.close, d.pre_close, d.amount, d.turnover_rate,
               f.close AS future_close,
               (
                 SELECT MAX(high) FROM daily_price h
                 WHERE h.stock_code=d.stock_code
                   AND h.trade_date IN ({})
               ) AS future_high
        FROM daily_price d
        JOIN daily_price f
          ON f.stock_code=d.stock_code AND f.trade_date=?
        WHERE d.trade_date=?
          AND d.close IS NOT NULL AND d.close > 0
          AND f.close IS NOT NULL AND f.close > 0
          AND d.stock_code NOT LIKE '8%'
          AND d.stock_code NOT LIKE '4%'
        """.format(",".join("?" for _ in future_dates)),
        (*future_dates, end_date, date),
    ).fetchall()

    winners: list[Winner] = []
    for r in rows:
        close = float(r["close"])
        future_close = float(r["future_close"])
        future_high = float(r["future_high"] or future_close)
        ret_close = (future_close / close - 1) * 100
        ret_high = (future_high / close - 1) * 100
        if ret_high < min_return:
            continue

        f = features(conn, r, date)
        code = r["stock_code"]
        winners.append(
            Winner(
                code=code,
                name=stock_name(conn, code),
                industry=stock_industry(conn, code),
                close=close,
                future_close=future_close,
                future_high=future_high,
                ret_close=ret_close,
                ret_high=ret_high,
                day_ret=f["day_ret"],
                ma20_dist=f["ma20_dist"],
                ma60_dist=f["ma60_dist"],
                ret20=f["ret20"],
                ret60=f["ret60"],
                amount_ratio20=f["amount_ratio20"],
                turnover=f["turnover"],
                bought=code in buys,
                buy_signal=buys.get(code, ""),
            )
        )

    winners.sort(key=lambda x: x.ret_high, reverse=True)
    return winners


def fmt(v: float | None, digits: int = 1, suffix: str = "%") -> str:
    if v is None:
        return "NA"
    return f"{v:.{digits}f}{suffix}"


def summarize_feature_band(values: list[float | None]) -> str:
    clean = [v for v in values if v is not None]
    if not clean:
        return "NA"
    return f"median={median(clean):.1f}, min={min(clean):.1f}, max={max(clean):.1f}"


def render_report(
    date: str,
    horizon: int,
    end_date: str,
    min_return: float,
    winners: list[Winner],
    top: int,
    candidates: dict[str, str],
) -> str:
    bought = [w for w in winners if w.bought]
    missed = [w for w in winners if not w.bought]
    candidate_winners = [w for w in winners if w.code in candidates]
    candidate_missed = [w for w in missed if w.code in candidates]
    capture = len(bought) / len(winners) * 100 if winners else 0.0
    candidate_capture = len(candidate_winners) / len(winners) * 100 if winners else 0.0

    lines: list[str] = []
    lines.append(f"# Missed Winners Review {date}")
    lines.append("")
    lines.append(f"- Horizon: {horizon} trading days, through {end_date}")
    lines.append(f"- Winner threshold: future high >= {min_return:.1f}%")
    lines.append(f"- Winners: {len(winners)}")
    lines.append(f"- Bought winners: {len(bought)}")
    lines.append(f"- Missed winners: {len(missed)}")
    lines.append(f"- Buy capture rate: {capture:.1f}%")
    if candidates:
        lines.append(f"- Candidate winners: {len(candidate_winners)}")
        lines.append(f"- Candidate capture rate: {candidate_capture:.1f}%")
        lines.append(f"- Candidate-but-not-bought winners: {len(candidate_missed)}")
    else:
        lines.append("- Candidate capture rate: unavailable (no candidate snapshots for this date)")
    lines.append("")

    lines.append("## Missed Winner Feature Summary")
    lines.append("")
    lines.append(f"- day_ret: {summarize_feature_band([w.day_ret for w in missed])}")
    lines.append(f"- ma20_dist: {summarize_feature_band([w.ma20_dist for w in missed])}")
    lines.append(f"- ma60_dist: {summarize_feature_band([w.ma60_dist for w in missed])}")
    lines.append(f"- ret20: {summarize_feature_band([w.ret20 for w in missed])}")
    lines.append(f"- ret60: {summarize_feature_band([w.ret60 for w in missed])}")
    lines.append(f"- amount_ratio20: {summarize_feature_band([w.amount_ratio20 for w in missed])}")
    lines.append("")

    lines.append(f"## Top {top} Missed Winners")
    lines.append("")
    lines.append(
        "| Code | Name | Industry | HighRet | CloseRet | DayRet | MA20 | MA60 | Ret20 | Ret60 | AmtRatio |"
    )
    lines.append("|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|")
    for w in missed[:top]:
        lines.append(
            "| "
            + " | ".join(
                [
                    w.code,
                    w.name,
                    w.industry,
                    fmt(w.ret_high),
                    fmt(w.ret_close),
                    fmt(w.day_ret),
                    fmt(w.ma20_dist),
                    fmt(w.ma60_dist),
                    fmt(w.ret20),
                    fmt(w.ret60),
                    fmt(w.amount_ratio20, 2, "x"),
                ]
            )
            + " |"
        )
    lines.append("")

    lines.append("## Bought Winners")
    lines.append("")
    if not bought:
        lines.append("No bought winners matched the threshold.")
    else:
        lines.append("| Code | Name | Signal | HighRet | CloseRet |")
        lines.append("|---|---|---|---:|---:|")
        for w in bought:
            lines.append(
                f"| {w.code} | {w.name} | {w.buy_signal} | {fmt(w.ret_high)} | {fmt(w.ret_close)} |"
            )
    lines.append("")

    lines.append("## Candidate Winners")
    lines.append("")
    if not candidates:
        lines.append("No candidate snapshot data for this date.")
    elif not candidate_winners:
        lines.append("No winners appeared in candidate snapshots.")
    else:
        lines.append("| Code | Name | Strategy | Bought | HighRet | CloseRet |")
        lines.append("|---|---|---|---:|---:|---:|")
        for w in candidate_winners[:top]:
            lines.append(
                f"| {w.code} | {w.name} | {candidates.get(w.code, '')} | "
                f"{'Y' if w.bought else 'N'} | {fmt(w.ret_high)} | {fmt(w.ret_close)} |"
            )
    lines.append("")

    lines.append("## Interpretation")
    lines.append("")
    lines.append(
        "- If missed winners cluster near MA20/MA60 with moderate volume expansion, Strategy C should learn a value-startup entry."
    )
    lines.append(
        "- If missed winners already had high Ret20/Ret60, the system is late and should avoid chasing rather than add them."
    )
    lines.append(
        "- Candidate coverage is available only after daemon_candidate_snapshots starts collecting data."
    )
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True, help="trade date, e.g. 2026-06-02")
    parser.add_argument("--horizon", type=int, default=3, help="future trading days")
    parser.add_argument("--min-return", type=float, default=5.0, help="future high return threshold")
    parser.add_argument("--top", type=int, default=30, help="top missed winners to print")
    parser.add_argument("--period", type=int, default=3, help="daemon period")
    parser.add_argument("--db-path", type=Path, default=DB_PATH)
    parser.add_argument("--out", type=Path, default=None, help="optional markdown output path")
    args = parser.parse_args()

    conn = connect(args.db_path)
    try:
        dates = trading_dates(conn)
        end_date, future_dates = resolve_window(dates, args.date, args.horizon)
        winners = find_winners(conn, args.date, future_dates, args.min_return, args.period)
        candidates = get_candidates(conn, args.date, args.period)
        report = render_report(
            args.date,
            args.horizon,
            end_date,
            args.min_return,
            winners,
            args.top,
            candidates,
        )
    finally:
        conn.close()

    if args.out:
        out = args.out
    else:
        DEFAULT_OUT_DIR.mkdir(parents=True, exist_ok=True)
        out = DEFAULT_OUT_DIR / f"missed_winners_{args.date}_h{args.horizon}.md"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(report, encoding="utf-8")
    print(report)
    print(f"Saved: {out}")


if __name__ == "__main__":
    main()
