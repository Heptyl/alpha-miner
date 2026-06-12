#!/usr/bin/env python3
"""Research a monthly low-volatility plus trend strategy for A shares."""

from __future__ import annotations

import argparse
import json
import math
import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DAILY_DB = PROJECT_ROOT / "data" / "alpha_miner.db"
MINUTE_DB = PROJECT_ROOT / "data" / "research_minutes_5m.db"
OUTPUT = PROJECT_ROOT / "output" / "research" / "strategy_a_low_volatility.json"
BASE_COST = 0.0025
SPLITS = {
    "discovery": ("2023-01-01", "2023-12-31"),
    "validation": ("2024-01-01", "2024-12-31"),
    "oos": ("2025-01-01", "2026-12-31"),
}


def load_data(daily_db: Path, minute_db: Path) -> pd.DataFrame:
    conn = sqlite3.connect(minute_db)
    conn.execute("ATTACH DATABASE ? AS daily", (str(daily_db),))
    try:
        frame = pd.read_sql_query(
            """
            WITH minute_daily AS (
                SELECT m.stock_code AS code, m.trade_date,
                       MAX(CASE WHEN substr(m.bar_time,12,5)='09:35' THEN m.open END) AS open,
                       MAX(CASE WHEN substr(m.bar_time,12,5)='15:00' THEN m.close END) AS close,
                       SUM(m.amount) AS amount,
                       u.target_year, u.rank_no,
                       COUNT(*) AS bars
                FROM minute_bars_5m m
                JOIN yearly_universe u
                  ON u.stock_code=m.stock_code
                 AND u.target_year=CAST(substr(m.trade_date,1,4) AS INTEGER)
                WHERE m.stock_code LIKE '000%' OR m.stock_code LIKE '001%'
                   OR m.stock_code LIKE '002%' OR m.stock_code LIKE '003%'
                   OR m.stock_code LIKE '600%' OR m.stock_code LIKE '601%'
                   OR m.stock_code LIKE '603%' OR m.stock_code LIKE '605%'
                GROUP BY m.stock_code, m.trade_date
                HAVING bars=48 AND open>0 AND close>0
            ),
            warmup AS (
                SELECT d.stock_code AS code, d.trade_date,
                       d.open, d.close, d.amount,
                       2023 AS target_year, u.rank_no
                FROM daily.daily_price d
                JOIN yearly_universe u
                  ON u.stock_code=d.stock_code AND u.target_year=2023
                WHERE d.trade_date BETWEEN '2022-01-01' AND '2022-12-31'
                  AND d.open>0 AND d.close>0
            )
            SELECT code, trade_date, open, close, amount, target_year, rank_no
            FROM warmup
            UNION ALL
            SELECT code, trade_date, open, close, amount, target_year, rank_no
            FROM minute_daily
            ORDER BY code, trade_date
            """,
            conn,
        )
    finally:
        conn.close()
    frame["trade_date"] = pd.to_datetime(frame["trade_date"])
    return frame


def build_features(frame: pd.DataFrame) -> pd.DataFrame:
    df = frame.sort_values(["code", "trade_date"]).copy()
    by_code = df.groupby("code", sort=False)
    df["pre_close"] = by_code["close"].shift(1)
    raw_return = df["close"] / df["pre_close"] - 1
    df["ret1"] = raw_return.where(raw_return.abs() <= 0.11)
    df["vol60"] = by_code["ret1"].transform(
        lambda s: s.shift(1).rolling(60, min_periods=50).std()
    )
    df["mom120"] = by_code["ret1"].transform(
        lambda s: s.shift(1).rolling(120, min_periods=100).apply(
            lambda values: float(np.nanprod(1 + values) - 1),
            raw=True,
        )
    )
    df["avg_amount20"] = by_code["amount"].transform(
        lambda s: s.shift(1).rolling(20, min_periods=15).mean()
    )
    return df


def monthly_rebalance_dates(features: pd.DataFrame) -> list[pd.Timestamp]:
    dates = pd.Series(sorted(features["trade_date"].unique()))
    table = pd.DataFrame({"trade_date": dates})
    table["month"] = table["trade_date"].dt.to_period("M")
    return table.groupby("month")["trade_date"].min().tolist()


def forward_return(
    stock: pd.DataFrame,
    entry_date: pd.Timestamp,
    hold_days: int,
    date_positions: pd.Series,
) -> tuple[pd.Timestamp, float] | None:
    rows = stock[stock["trade_date"] >= entry_date].head(hold_days)
    if len(rows) < hold_days:
        return None
    expected_entry = date_positions.get(entry_date)
    expected_exit = expected_entry + hold_days - 1 if expected_entry is not None else None
    actual_positions = rows["trade_date"].map(date_positions)
    if (
        expected_entry is None
        or actual_positions.iloc[0] != expected_entry
        or actual_positions.iloc[-1] != expected_exit
        or not np.array_equal(
            actual_positions.to_numpy(),
            np.arange(expected_entry, expected_exit + 1),
        )
    ):
        return None
    first = rows.iloc[0]
    if first["open"] <= 0:
        return None
    gross = first["close"] / first["open"]
    if len(rows) > 1:
        subsequent_returns = (
            rows.iloc[1:]["close"] / rows.iloc[1:]["pre_close"] - 1
        )
        if subsequent_returns.abs().gt(0.11).any():
            return None
        gross *= float(np.prod((1 + subsequent_returns).to_numpy()))
    return rows.iloc[-1]["trade_date"], gross - 1 - BASE_COST


def build_signals(features: pd.DataFrame, mode: str, hold_days: int = 20) -> pd.DataFrame:
    market_dates = pd.Index(sorted(features["trade_date"].unique()))
    date_positions = pd.Series(np.arange(len(market_dates)), index=market_dates)
    by_code = {
        code: group.sort_values("trade_date").reset_index(drop=True)
        for code, group in features.groupby("code", sort=False)
    }
    records = []
    for entry_date in monthly_rebalance_dates(features):
        previous = features[features["trade_date"] < entry_date]
        if previous.empty:
            continue
        signal_date = previous["trade_date"].max()
        cross = previous[previous["trade_date"] == signal_date].copy()
        cross = cross[
            cross["vol60"].notna()
            & cross["mom120"].notna()
            & (cross["avg_amount20"] >= 50_000_000)
            & cross["close"].between(3, 200)
        ]
        if mode == "low_vol_trend":
            cross = cross[cross["mom120"] > 0]
            cross.sort_values(["vol60", "mom120", "code"], ascending=[True, False, True], inplace=True)
        elif mode == "low_vol":
            cross.sort_values(["vol60", "code"], ascending=[True, True], inplace=True)
        else:
            cross = cross[cross["mom120"] > 0]
            cross.sort_values(["mom120", "vol60", "code"], ascending=[False, True, True], inplace=True)

        for row in cross.head(3).itertuples():
            outcome = forward_return(
                by_code[row.code], entry_date, hold_days, date_positions
            )
            if outcome is None:
                continue
            exit_date, net_return = outcome
            records.append({
                "code": row.code,
                "signal_date": signal_date,
                "entry_date": entry_date,
                "exit_date": exit_date,
                "vol60": row.vol60,
                "mom120": row.mom120,
                "net_return": net_return,
            })
    return pd.DataFrame(records)


def bootstrap_ci(values: np.ndarray, seed: int = 20260611) -> list[float | None]:
    if len(values) < 2:
        return [None, None]
    rng = np.random.default_rng(seed)
    means = [float(rng.choice(values, size=len(values), replace=True).mean()) for _ in range(2000)]
    return [round(float(v) * 100, 4) for v in np.percentile(means, [2.5, 97.5])]


def metrics(signals: pd.DataFrame, total_cost: float) -> dict:
    if signals.empty:
        return {"trades": 0}
    values = signals["net_return"].to_numpy(dtype=float) - max(0, total_cost - BASE_COST)
    monthly = signals.groupby("entry_date")["net_return"].mean().to_numpy(
        dtype=float, copy=True
    )
    monthly -= max(0, total_cost - BASE_COST)
    wins, losses = values[values > 0].sum(), -values[values < 0].sum()
    equity = np.cumprod(1 + monthly)
    peak = np.maximum.accumulate(equity)
    return {
        "trades": int(len(values)),
        "months": int(signals["entry_date"].nunique()),
        "mean_pct": round(float(values.mean() * 100), 4),
        "median_pct": round(float(np.median(values) * 100), 4),
        "win_rate_pct": round(float((values > 0).mean() * 100), 2),
        "profit_factor": round(float(wins / losses), 3) if losses else math.inf,
        "ci95_mean_pct": bootstrap_ci(values),
        "portfolio_return_pct": round(float((equity[-1] - 1) * 100), 2),
        "max_drawdown_pct": round(float(np.min(equity / peak - 1) * 100), 2),
    }


def run(daily_db: Path, minute_db: Path, output: Path) -> dict:
    features = build_features(load_data(daily_db, minute_db))
    result = {"hypotheses": {}}
    for mode in ("low_vol_trend", "low_vol", "momentum"):
        signals = build_signals(features, mode)
        item = {"hold_days": 20, "max_positions": 3, "splits": {}}
        for split, (start, end) in SPLITS.items():
            sample = signals[signals["entry_date"].between(start, end)]
            item["splits"][split] = {
                f"{cost * 100:.2f}%": metrics(sample, cost)
                for cost in (0.0025, 0.0050, 0.0100)
            }
        result["hypotheses"][f"A_monthly_{mode}_r1"] = item
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--daily-db", type=Path, default=DAILY_DB)
    parser.add_argument("--minute-db", type=Path, default=MINUTE_DB)
    parser.add_argument("--output", type=Path, default=OUTPUT)
    args = parser.parse_args()
    print(json.dumps(run(args.daily_db, args.minute_db, args.output), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
