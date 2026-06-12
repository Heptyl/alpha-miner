#!/usr/bin/env python3
"""Evaluate a pre-registered A-share closing-period momentum hypothesis."""

from __future__ import annotations

import argparse
import json
import math
import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
MINUTE_DB = PROJECT_ROOT / "data" / "research_minutes_5m.db"
DAILY_DB = PROJECT_ROOT / "data" / "alpha_miner.db"
OUTPUT = PROJECT_ROOT / "output" / "research" / "strategy_a_tail_momentum.json"
BASE_COST = 0.0025
SPLITS = {
    "discovery": ("2023-01-01", "2023-12-31"),
    "validation": ("2024-01-01", "2024-12-31"),
    "oos": ("2025-01-01", "2026-12-31"),
}


def load_daily_tail_bars(minute_db: Path, daily_db: Path | None = None) -> pd.DataFrame:
    conn = sqlite3.connect(minute_db)
    try:
        frame = pd.read_sql_query(
            """
            SELECT stock_code AS code, trade_date,
                   MAX(CASE WHEN substr(bar_time, 12, 5)='14:30' THEN close END) AS close_1430,
                   MAX(CASE WHEN substr(bar_time, 12, 5)='14:50' THEN close END) AS close_1450,
                   MAX(CASE WHEN substr(bar_time, 12, 5)='14:55' THEN close END) AS close_1455,
                   MAX(CASE WHEN substr(bar_time, 12, 5)='15:00' THEN close END) AS daily_close,
                   SUM(CASE WHEN substr(bar_time, 12, 5)>='14:30' THEN amount ELSE 0 END) AS tail_amount,
                   SUM(amount) AS day_amount,
                   COUNT(*) AS bars
            FROM minute_bars_5m
            GROUP BY stock_code, trade_date
            HAVING bars=48
               AND close_1430>0 AND close_1450>0 AND close_1455>0
               AND daily_close>0
            ORDER BY stock_code, trade_date
            """,
            conn,
        )
    finally:
        conn.close()
    frame["trade_date"] = pd.to_datetime(frame["trade_date"])
    frame["pre_close"] = frame.groupby("code", sort=False)["daily_close"].shift(1)
    return frame


def build_signals(
    frame: pd.DataFrame,
    mode: str = "momentum",
    quantile: float = 0.10,
) -> pd.DataFrame:
    df = frame.sort_values(["code", "trade_date"]).copy()
    market_dates = pd.Index(sorted(df["trade_date"].unique()))
    date_idx = pd.Series(np.arange(len(market_dates)), index=market_dates)
    df["date_idx"] = df["trade_date"].map(date_idx)
    by_code = df.groupby("code", sort=False)

    df["tail_return"] = df["close_1450"] / df["close_1430"] - 1
    df["tail_amount_ratio"] = df["tail_amount"] / df["day_amount"]
    df["tail_rank"] = df.groupby("trade_date")["tail_return"].rank(pct=True)
    df["buy_price"] = df["close_1455"]
    df["buy_change"] = df["buy_price"] / df["pre_close"] - 1
    df["exit_price"] = by_code["close_1455"].shift(-1)
    df["exit_date"] = by_code["trade_date"].shift(-1)
    df["next_is_adjacent"] = df["exit_date"].map(date_idx).eq(df["date_idx"] + 1)
    df["net_return"] = df["exit_price"] / df["buy_price"] - 1 - BASE_COST

    rank_filter = (
        (df["tail_rank"] >= 1 - quantile) & (df["tail_return"] > 0)
        if mode == "momentum"
        else (df["tail_rank"] <= quantile) & (df["tail_return"] < 0)
    )
    eligible = df[
        df["next_is_adjacent"]
        & rank_filter
        & (df["buy_change"].between(-0.09, 0.09))
        & (df["buy_price"].between(3, 200))
        & (df["day_amount"] >= 50_000_000)
    ].copy()
    eligible.sort_values(
        ["trade_date", "tail_return", "tail_amount_ratio", "code"],
        ascending=[True, mode == "reversal", False, True],
        inplace=True,
    )
    return eligible.groupby("trade_date", sort=True).head(3).reset_index(drop=True)


def bootstrap_ci(values: np.ndarray, seed: int = 20260611) -> list[float | None]:
    if len(values) < 2:
        return [None, None]
    rng = np.random.default_rng(seed)
    means = [
        float(rng.choice(values, size=len(values), replace=True).mean())
        for _ in range(2000)
    ]
    return [round(float(v) * 100, 4) for v in np.percentile(means, [2.5, 97.5])]


def metrics(signals: pd.DataFrame, total_cost: float = BASE_COST) -> dict:
    values = (
        signals["net_return"].to_numpy(dtype=float)
        - max(0.0, total_cost - BASE_COST)
    )
    if not len(values):
        return {"trades": 0}
    wins = values[values > 0].sum()
    losses = -values[values < 0].sum()
    equity = np.cumprod(1 + values / 3)
    peak = np.maximum.accumulate(equity)
    return {
        "trades": int(len(values)),
        "days": int(signals["trade_date"].nunique()),
        "mean_pct": round(float(values.mean() * 100), 4),
        "median_pct": round(float(np.median(values) * 100), 4),
        "win_rate_pct": round(float((values > 0).mean() * 100), 2),
        "profit_factor": round(float(wins / losses), 3) if losses else math.inf,
        "ci95_mean_pct": bootstrap_ci(values),
        "max_drawdown_pct": round(float(np.min(equity / peak - 1) * 100), 2),
    }


def run(minute_db: Path, daily_db: Path, output: Path) -> dict:
    frame = load_daily_tail_bars(minute_db, daily_db)
    result = {
        "coverage": {
            "rows": int(len(frame)),
            "stocks": int(frame["code"].nunique()) if not frame.empty else 0,
            "dates": int(frame["trade_date"].nunique()) if not frame.empty else 0,
            "start": frame["trade_date"].min().strftime("%Y-%m-%d") if not frame.empty else None,
            "end": frame["trade_date"].max().strftime("%Y-%m-%d") if not frame.empty else None,
        },
        "hypotheses": {},
    }
    for mode in ("momentum", "reversal"):
        signals = build_signals(frame, mode=mode)
        hypothesis = {
            "signal": (
                "14:30-14:50 return cross-sectional top 10%"
                if mode == "momentum"
                else "14:30-14:50 return cross-sectional bottom 10%"
            ),
            "entry": "14:55 close",
            "exit": "next trading day 14:55 close",
            "max_positions": 3,
            "splits": {},
        }
        for split, (start, end) in SPLITS.items():
            selected = signals[signals["trade_date"].between(start, end)]
            hypothesis["splits"][split] = {
                f"{cost * 100:.2f}%": metrics(selected, cost)
                for cost in (0.0025, 0.0050, 0.0100)
            }
        result["hypotheses"][f"A_tail_{mode}_r1"] = hypothesis

    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--minute-db", type=Path, default=MINUTE_DB)
    parser.add_argument("--daily-db", type=Path, default=DAILY_DB)
    parser.add_argument("--output", type=Path, default=OUTPUT)
    args = parser.parse_args()
    print(json.dumps(run(args.minute_db, args.daily_db, args.output), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
