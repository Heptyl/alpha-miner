#!/usr/bin/env python3
"""Research the 52-week-high effect in point-in-time liquid A-share universes."""

from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd

from scripts.research_strategy_a_volume_premium import (
    SPLITS,
    bootstrap_ci,
    metrics,
    monthly_calendar,
)


ROOT = Path(__file__).resolve().parents[1]
DAILY_DB = ROOT / "data" / "alpha_miner.db"
MINUTE_DB = ROOT / "data" / "research_minutes_5m.db"
OUTPUT = ROOT / "output" / "research" / "strategy_a_52week_high.json"


def load_daily(daily_db: Path, minute_db: Path) -> pd.DataFrame:
    con = sqlite3.connect(minute_db)
    con.execute("ATTACH DATABASE ? AS daily", (str(daily_db),))
    try:
        frame = pd.read_sql_query(
            """
            WITH minute_daily AS (
                SELECT m.stock_code AS code, m.trade_date,
                       MAX(CASE WHEN substr(m.bar_time,12,5)='09:35' THEN m.open END) AS open,
                       MAX(CASE WHEN substr(m.bar_time,12,5)='15:00' THEN m.close END) AS close,
                       SUM(m.amount) AS amount, u.rank_no, COUNT(*) AS bars
                FROM minute_bars_5m m
                JOIN yearly_universe u
                  ON u.stock_code=m.stock_code
                 AND u.target_year=CAST(substr(m.trade_date,1,4) AS INTEGER)
                WHERE m.stock_code NOT LIKE '4%'
                  AND m.stock_code NOT LIKE '8%'
                  AND m.stock_code NOT LIKE '9%'
                  AND m.stock_code NOT LIKE '200%'
                  AND m.stock_code NOT LIKE '900%'
                GROUP BY m.stock_code, m.trade_date
                HAVING bars=48 AND open>0 AND close>0 AND amount>0
            ),
            warmup AS (
                SELECT d.stock_code AS code, d.trade_date, d.open, d.close,
                       d.amount, u.rank_no
                FROM daily.daily_price d
                JOIN yearly_universe u
                  ON u.stock_code=d.stock_code AND u.target_year=2023
                WHERE d.trade_date BETWEEN '2022-01-01' AND '2022-12-31'
                  AND d.open>0 AND d.close>0 AND d.amount>0
            )
            SELECT * FROM warmup
            UNION ALL
            SELECT code,trade_date,open,close,amount,rank_no FROM minute_daily
            ORDER BY code,trade_date
            """,
            con,
        )
    finally:
        con.close()
    frame["trade_date"] = pd.to_datetime(frame["trade_date"])
    return frame


def build_features(frame: pd.DataFrame) -> pd.DataFrame:
    df = frame.sort_values(["code", "trade_date"]).copy()
    market_dates = pd.Index(sorted(df["trade_date"].unique()))
    position = pd.Series(np.arange(len(market_dates)), index=market_dates)
    by_code = df.groupby("code", sort=False)
    previous_date = by_code["trade_date"].shift(1)
    contiguous = previous_date.map(position).eq(df["trade_date"].map(position) - 1)
    raw_return = (df["close"] / by_code["close"].shift(1) - 1).where(contiguous)
    is_growth = df["code"].str.startswith(("300", "301", "688"))
    df["ret1"] = raw_return.where(raw_return.abs() <= np.where(is_growth, 0.22, 0.11))
    df["adjusted_index"] = (
        df["ret1"].fillna(0).add(1).groupby(df["code"]).cumprod()
    )
    df["adjustment_factor"] = df["adjusted_index"] / df["close"]
    df["adjusted_open"] = df["open"] * df["adjustment_factor"]
    by_code = df.groupby("code", sort=False)
    df["high_240"] = by_code["adjusted_index"].transform(
        lambda values: values.rolling(240, min_periods=200).max()
    )
    df["high_ratio"] = df["adjusted_index"] / df["high_240"]
    df["mom120"] = by_code["adjusted_index"].pct_change(120)
    df["amount20"] = by_code["amount"].transform(
        lambda values: values.rolling(20, min_periods=15).mean()
    )
    return df


def build_signals(features: pd.DataFrame, mode: str) -> pd.DataFrame:
    indexed = {
        code: group.set_index("trade_date")
        for code, group in features.groupby("code", sort=False)
    }
    records = []
    for period in monthly_calendar(features).itertuples(index=False):
        cross = features[features["trade_date"] == period.signal_date].copy()
        cross = cross[
            cross["high_ratio"].notna()
            & cross["mom120"].notna()
            & (cross["amount20"] >= 50_000_000)
            & cross["close"].between(2, 300)
        ]
        if len(cross) < 100:
            continue
        cross["high_pct"] = cross["high_ratio"].rank(pct=True)
        if mode == "near_high":
            selected = cross[cross["high_pct"] >= 0.90].sort_values(
                ["high_ratio", "mom120", "rank_no", "code"],
                ascending=[False, False, True, True],
            )
        elif mode == "near_high_positive":
            selected = cross[
                (cross["high_pct"] >= 0.90) & (cross["mom120"] > 0)
            ].sort_values(
                ["high_ratio", "mom120", "rank_no", "code"],
                ascending=[False, False, True, True],
            )
        elif mode == "far_from_high":
            selected = cross[cross["high_pct"] <= 0.10].sort_values(
                ["high_ratio", "mom120", "rank_no", "code"],
                ascending=[True, True, True, True],
            )
        else:
            raise ValueError(mode)

        for row in selected.head(10).itertuples():
            stock = indexed[row.code]
            if period.entry_date not in stock.index or period.exit_date not in stock.index:
                continue
            entry = float(stock.at[period.entry_date, "adjusted_open"])
            exit_price = float(stock.at[period.exit_date, "adjusted_open"])
            gross = exit_price / entry - 1
            if entry <= 0 or exit_price <= 0 or abs(gross) > 0.60:
                continue
            records.append({
                "code": row.code,
                "signal_date": period.signal_date,
                "entry_date": period.entry_date,
                "exit_date": period.exit_date,
                "high_ratio": float(row.high_ratio),
                "mom120": float(row.mom120),
                "gross_return": gross,
            })
    return pd.DataFrame(records)


def execute(daily_db: Path, minute_db: Path, output: Path) -> dict:
    features = build_features(load_daily(daily_db, minute_db))
    result = {
        "method": {
            "signal": "monthly top/bottom decile of adjusted close / 240-day high",
            "entry": "next month's first trading-day open",
            "exit": "following month's first trading-day open",
            "max_positions": 10,
        },
        "hypotheses": {},
    }
    for mode in ("near_high", "near_high_positive", "far_from_high"):
        signals = build_signals(features, mode)
        result["hypotheses"][mode] = {
            split: {
                f"{cost * 100:.2f}%": metrics(
                    signals[signals["entry_date"].between(start, end)], cost
                )
                for cost in (0.0025, 0.005, 0.01)
            }
            for split, (start, end) in SPLITS.items()
        }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, ensure_ascii=False, indent=2))
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--daily-db", type=Path, default=DAILY_DB)
    parser.add_argument("--minute-db", type=Path, default=MINUTE_DB)
    parser.add_argument("--output", type=Path, default=OUTPUT)
    args = parser.parse_args()
    print(json.dumps(execute(args.daily_db, args.minute_db, args.output), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
