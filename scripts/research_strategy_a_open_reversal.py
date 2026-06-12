#!/usr/bin/env python3
"""Research a pre-registered opening overreaction reversal strategy."""

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
OUTPUT = PROJECT_ROOT / "output" / "research" / "strategy_a_open_reversal.json"
BASE_COST = 0.0025
SPLITS = {
    "discovery": ("2023-01-01", "2023-12-31"),
    "validation": ("2024-01-01", "2024-12-31"),
    "oos": ("2025-01-01", "2026-12-31"),
}


def load_open_bars(db_path: Path) -> pd.DataFrame:
    conn = sqlite3.connect(db_path)
    try:
        frame = pd.read_sql_query(
            """
            SELECT stock_code AS code, trade_date,
                   MAX(CASE WHEN substr(bar_time,12,5)='09:35' THEN open END) AS open_0930,
                   MAX(CASE WHEN substr(bar_time,12,5)='09:35' THEN close END) AS close_0935,
                   MAX(CASE WHEN substr(bar_time,12,5)='09:50' THEN close END) AS close_0950,
                   MAX(CASE WHEN substr(bar_time,12,5)='09:55' THEN close END) AS close_0955,
                   MAX(CASE WHEN substr(bar_time,12,5)='15:00' THEN close END) AS close_1500,
                   SUM(CASE WHEN substr(bar_time,12,5)<='09:55' THEN amount ELSE 0 END) AS open_amount,
                   SUM(amount) AS day_amount,
                   COUNT(*) AS bars
            FROM minute_bars_5m
            GROUP BY stock_code, trade_date
            HAVING bars=48
               AND open_0930>0 AND close_0935>0 AND close_0950>0
               AND close_0955>0 AND close_1500>0
            ORDER BY stock_code, trade_date
            """,
            conn,
        )
    finally:
        conn.close()
    frame["trade_date"] = pd.to_datetime(frame["trade_date"])
    return frame


def build_features(frame: pd.DataFrame) -> pd.DataFrame:
    df = frame.sort_values(["code", "trade_date"]).copy()
    market_dates = pd.Index(sorted(df["trade_date"].unique()))
    date_idx = pd.Series(np.arange(len(market_dates)), index=market_dates)
    df["date_idx"] = df["trade_date"].map(date_idx)
    by_code = df.groupby("code", sort=False)

    df["prev_close"] = by_code["close_1500"].shift(1)
    df["prev_date"] = by_code["trade_date"].shift(1)
    df["prev_is_adjacent"] = df["prev_date"].map(date_idx).eq(df["date_idx"] - 1)
    df["overnight_return"] = df["open_0930"] / df["prev_close"] - 1
    df["overnight_rank"] = df.groupby("trade_date")["overnight_return"].rank(pct=True)
    df["recovery_return"] = df["close_0950"] / df["close_0935"] - 1
    df["entry_price"] = df["close_0955"]

    df["exit_price_am"] = by_code["close_0955"].shift(-1)
    df["exit_price_pm"] = by_code["close_1500"].shift(-1)
    df["exit_date"] = by_code["trade_date"].shift(-1)
    df["exit_is_adjacent"] = df["exit_date"].map(date_idx).eq(df["date_idx"] + 1)
    df["ret_am"] = df["exit_price_am"] / df["entry_price"] - 1 - BASE_COST
    df["ret_pm"] = df["exit_price_pm"] / df["entry_price"] - 1 - BASE_COST
    return df


def select_signals(
    features: pd.DataFrame,
    require_recovery: bool,
    exit_mode: str,
) -> pd.DataFrame:
    return_column = "ret_am" if exit_mode == "next_0955" else "ret_pm"
    mask = (
        features["prev_is_adjacent"]
        & features["exit_is_adjacent"]
        & (features["overnight_rank"] <= 0.10)
        & (features["overnight_return"].between(-0.09, -0.01))
        & (features["entry_price"].between(3, 200))
        & (features["day_amount"] >= 50_000_000)
    )
    if require_recovery:
        mask &= features["recovery_return"] > 0

    selected = features.loc[
        mask,
        [
            "code", "trade_date", "exit_date", "overnight_return",
            "recovery_return", "open_amount", return_column,
        ],
    ].copy()
    selected.rename(columns={return_column: "net_return"}, inplace=True)
    selected.sort_values(
        ["trade_date", "overnight_return", "recovery_return", "code"],
        ascending=[True, True, False, True],
        inplace=True,
    )
    return selected.groupby("trade_date", sort=True).head(3).reset_index(drop=True)


def bootstrap_ci(values: np.ndarray, seed: int = 20260611) -> list[float | None]:
    if len(values) < 2:
        return [None, None]
    rng = np.random.default_rng(seed)
    means = [
        float(rng.choice(values, size=len(values), replace=True).mean())
        for _ in range(2000)
    ]
    return [round(float(v) * 100, 4) for v in np.percentile(means, [2.5, 97.5])]


def metrics(signals: pd.DataFrame, total_cost: float) -> dict:
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


def run(db_path: Path, output: Path) -> dict:
    frame = load_open_bars(db_path)
    features = build_features(frame)
    result = {
        "coverage": {
            "rows": int(len(frame)),
            "stocks": int(frame["code"].nunique()),
            "dates": int(frame["trade_date"].nunique()),
            "start": frame["trade_date"].min().strftime("%Y-%m-%d"),
            "end": frame["trade_date"].max().strftime("%Y-%m-%d"),
        },
        "hypotheses": {},
    }

    hypotheses = {
        "A_open_reversal_confirmed_r1": (True, "next_0955"),
        "A_open_reversal_unconfirmed_control": (False, "next_0955"),
        "A_open_reversal_confirmed_pm_control": (True, "next_1500"),
    }
    for name, (require_recovery, exit_mode) in hypotheses.items():
        signals = select_signals(features, require_recovery, exit_mode)
        item = {
            "entry": "09:55 close",
            "exit": "next trading day 09:55 close" if exit_mode == "next_0955" else "next trading day 15:00 close",
            "requires_0935_0950_recovery": require_recovery,
            "max_positions": 3,
            "splits": {},
        }
        for split, (start, end) in SPLITS.items():
            sample = signals[signals["trade_date"].between(start, end)]
            item["splits"][split] = {
                f"{cost * 100:.2f}%": metrics(sample, cost)
                for cost in (0.0025, 0.0050, 0.0100)
            }
        result["hypotheses"][name] = item

    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--minute-db", type=Path, default=MINUTE_DB)
    parser.add_argument("--output", type=Path, default=OUTPUT)
    args = parser.parse_args()
    print(json.dumps(run(args.minute_db, args.output), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
