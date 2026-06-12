#!/usr/bin/env python3
"""Research a T+1-compliant overnight reversal strategy for A shares."""

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
OUTPUT = PROJECT_ROOT / "output" / "research" / "strategy_a_overnight_reversal.json"
BASE_COST = 0.0025
SPLITS = {
    "discovery": ("2023-01-01", "2023-12-31"),
    "validation": ("2024-01-01", "2024-12-31"),
    "oos": ("2025-01-01", "2026-12-31"),
}


def load_bars(db_path: Path) -> pd.DataFrame:
    conn = sqlite3.connect(db_path)
    try:
        frame = pd.read_sql_query(
            """
            SELECT stock_code AS code, trade_date,
                   MAX(CASE WHEN substr(bar_time,12,5)='09:35' THEN open END) AS open_0930,
                   MAX(CASE WHEN substr(bar_time,12,5)='09:35' THEN close END) AS close_0935,
                   MAX(CASE WHEN substr(bar_time,12,5)='14:50' THEN close END) AS close_1450,
                   MAX(CASE WHEN substr(bar_time,12,5)='14:55' THEN close END) AS close_1455,
                   MAX(CASE WHEN substr(bar_time,12,5)='15:00' THEN close END) AS close_1500,
                   SUM(amount) AS day_amount,
                   COUNT(*) AS bars
            FROM minute_bars_5m
            GROUP BY stock_code, trade_date
            HAVING bars=48
               AND open_0930>0 AND close_0935>0 AND close_1450>0
               AND close_1455>0 AND close_1500>0
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

    df["intraday_return"] = df["close_1450"] / df["open_0930"] - 1
    df["intraday_rank"] = df.groupby("trade_date")["intraday_return"].rank(pct=True)
    df["liquidity_rank"] = df.groupby("trade_date")["day_amount"].rank(
        pct=True, ascending=False
    )
    df["entry_price"] = df["close_1455"]
    df["exit_price"] = by_code["close_0935"].shift(-1)
    df["exit_date"] = by_code["trade_date"].shift(-1)
    df["exit_is_adjacent"] = df["exit_date"].map(date_idx).eq(df["date_idx"] + 1)
    df["net_return"] = df["exit_price"] / df["entry_price"] - 1 - BASE_COST
    return df


def select_signals(features: pd.DataFrame, mode: str) -> pd.DataFrame:
    common = (
        features["exit_is_adjacent"]
        & (features["entry_price"].between(3, 200))
        & (features["day_amount"] >= 50_000_000)
        & (features["intraday_return"].between(-0.09, 0.09))
    )
    if mode == "loser":
        mask = common & (features["intraday_rank"] <= 0.10)
        sort_columns = ["trade_date", "intraday_return", "day_amount", "code"]
        ascending = [True, True, False, True]
    elif mode == "winner":
        mask = common & (features["intraday_rank"] >= 0.90)
        sort_columns = ["trade_date", "intraday_return", "day_amount", "code"]
        ascending = [True, False, False, True]
    else:
        mask = common & (features["liquidity_rank"] <= 0.10)
        sort_columns = ["trade_date", "day_amount", "code"]
        ascending = [True, False, True]

    selected = features.loc[
        mask,
        [
            "code", "trade_date", "exit_date", "intraday_return",
            "day_amount", "net_return",
        ],
    ].copy()
    selected.sort_values(sort_columns, ascending=ascending, inplace=True)
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
    frame = load_bars(db_path)
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

    for mode in ("loser", "winner", "liquid"):
        signals = select_signals(features, mode)
        item = {
            "entry": "14:55 close",
            "exit": "next trading day 09:35 close",
            "selection": mode,
            "max_positions": 3,
            "splits": {},
        }
        for split, (start, end) in SPLITS.items():
            sample = signals[signals["trade_date"].between(start, end)]
            item["splits"][split] = {
                f"{cost * 100:.2f}%": metrics(sample, cost)
                for cost in (0.0025, 0.0050, 0.0100)
            }
        result["hypotheses"][f"A_overnight_{mode}_r1"] = item

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
