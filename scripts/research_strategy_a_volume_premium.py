#!/usr/bin/env python3
"""Research a monthly abnormal-volume return premium in liquid A shares."""

from __future__ import annotations

import argparse
import json
import math
import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "data" / "research_minutes_5m.db"
OUTPUT = ROOT / "output" / "research" / "strategy_a_volume_premium.json"
SPLITS = {
    "discovery": ("2023-01-01", "2023-12-31"),
    "validation": ("2024-01-01", "2024-12-31"),
    "oos": ("2025-01-01", "2026-12-31"),
}


def load_daily(db_path: Path) -> pd.DataFrame:
    con = sqlite3.connect(db_path)
    try:
        frame = pd.read_sql_query(
            """
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
            ORDER BY m.stock_code, m.trade_date
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
    positions = pd.Series(np.arange(len(market_dates)), index=market_dates)
    by_code = df.groupby("code", sort=False)
    previous_date = by_code["trade_date"].shift(1)
    contiguous = previous_date.map(positions).eq(df["trade_date"].map(positions) - 1)
    raw_return = (df["close"] / by_code["close"].shift(1) - 1).where(contiguous)
    is_growth = df["code"].str.startswith(("300", "301", "688"))
    limit = np.where(is_growth, 0.22, 0.11)
    df["ret1"] = raw_return.where(raw_return.abs() <= limit)
    df["adjusted_index"] = (
        df["ret1"].fillna(0).add(1).groupby(df["code"]).cumprod()
    )
    df["adjustment_factor"] = df["adjusted_index"] / df["close"]
    df["adjusted_open"] = df["open"] * df["adjustment_factor"]
    by_code = df.groupby("code", sort=False)
    df["ret20"] = by_code["adjusted_index"].pct_change(20)
    df["amount5"] = by_code["amount"].transform(
        lambda values: values.rolling(5, min_periods=5).mean()
    )
    df["amount_base60"] = by_code["amount"].transform(
        lambda values: values.shift(5).rolling(60, min_periods=50).median()
    )
    df["amount_ratio"] = df["amount5"] / df["amount_base60"]
    return df


def monthly_calendar(features: pd.DataFrame) -> pd.DataFrame:
    dates = pd.Series(sorted(features["trade_date"].unique()))
    table = pd.DataFrame({"trade_date": dates})
    table["month"] = table["trade_date"].dt.to_period("M")
    grouped = table.groupby("month")["trade_date"]
    return pd.DataFrame({
        "signal_date": grouped.max(),
        "entry_date": grouped.min().shift(-1),
        "exit_date": grouped.min().shift(-2),
    }).dropna().reset_index(drop=True)


def build_signals(features: pd.DataFrame, mode: str) -> pd.DataFrame:
    calendar = monthly_calendar(features)
    indexed = {
        code: group.set_index("trade_date")
        for code, group in features.groupby("code", sort=False)
    }
    records = []
    for period in calendar.itertuples(index=False):
        cross = features[features["trade_date"] == period.signal_date].copy()
        cross = cross[
            cross["amount_ratio"].notna()
            & cross["ret20"].notna()
            & cross["close"].between(2, 300)
            & (cross["amount5"] >= 50_000_000)
        ]
        if len(cross) < 100:
            continue
        cross["volume_pct"] = cross["amount_ratio"].rank(pct=True)
        if mode == "high_volume":
            selected = cross[cross["volume_pct"] >= 0.90].sort_values(
                ["amount_ratio", "rank_no", "code"], ascending=[False, True, True]
            )
        elif mode == "high_volume_positive":
            selected = cross[
                (cross["volume_pct"] >= 0.90) & (cross["ret20"] > 0)
            ].sort_values(
                ["amount_ratio", "ret20", "code"], ascending=[False, False, True]
            )
        elif mode == "low_volume":
            selected = cross[cross["volume_pct"] <= 0.10].sort_values(
                ["amount_ratio", "rank_no", "code"], ascending=[True, True, True]
            )
        else:
            raise ValueError(mode)

        for row in selected.head(10).itertuples():
            stock = indexed[row.code]
            if period.entry_date not in stock.index or period.exit_date not in stock.index:
                continue
            entry = float(stock.at[period.entry_date, "adjusted_open"])
            exit_price = float(stock.at[period.exit_date, "adjusted_open"])
            if entry <= 0 or exit_price <= 0:
                continue
            gross_return = exit_price / entry - 1
            if abs(gross_return) > 0.60:
                continue
            records.append({
                "code": row.code,
                "signal_date": period.signal_date,
                "entry_date": period.entry_date,
                "exit_date": period.exit_date,
                "amount_ratio": float(row.amount_ratio),
                "ret20": float(row.ret20),
                "gross_return": gross_return,
            })
    return pd.DataFrame(records)


def bootstrap_ci(values: np.ndarray, seed: int = 20260611) -> list[float | None]:
    if len(values) < 2:
        return [None, None]
    rng = np.random.default_rng(seed)
    samples = rng.choice(values, size=(3000, len(values)), replace=True).mean(axis=1)
    return [round(float(value * 100), 3) for value in np.percentile(samples, [2.5, 97.5])]


def metrics(signals: pd.DataFrame, cost: float) -> dict:
    if signals.empty:
        return {"trades": 0}
    values = signals["gross_return"].to_numpy() - cost
    monthly = (
        signals.assign(net=values)
        .groupby("entry_date")["net"]
        .mean()
        .sort_index()
    )
    equity = (1 + monthly).cumprod()
    peak = equity.cummax()
    wins = values[values > 0].sum()
    losses = -values[values < 0].sum()
    return {
        "trades": int(len(values)),
        "months": int(len(monthly)),
        "mean_trade_pct": round(float(values.mean() * 100), 3),
        "median_trade_pct": round(float(np.median(values) * 100), 3),
        "win_rate_pct": round(float((values > 0).mean() * 100), 2),
        "profit_factor": round(float(wins / losses), 3) if losses else math.inf,
        "ci95_mean_trade_pct": bootstrap_ci(values),
        "portfolio_return_pct": round(float((equity.iloc[-1] - 1) * 100), 2),
        "max_drawdown_pct": round(float((equity / peak - 1).min() * 100), 2),
        "yearly_returns_pct": {
            str(year): round(float(((1 + group).prod() - 1) * 100), 2)
            for year, group in monthly.groupby(monthly.index.year)
        },
    }


def execute(db_path: Path, output: Path) -> dict:
    features = build_features(load_daily(db_path))
    result = {
        "method": {
            "signal": "top/bottom decile of 5-day amount vs prior 60-day median",
            "entry": "next month's first trading-day open",
            "exit": "following month's first trading-day open",
            "max_positions": 10,
            "costs": [0.0025, 0.005, 0.01],
        },
        "hypotheses": {},
    }
    for mode in ("high_volume", "high_volume_positive", "low_volume"):
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
    parser.add_argument("--db", type=Path, default=DB)
    parser.add_argument("--output", type=Path, default=OUTPUT)
    args = parser.parse_args()
    print(json.dumps(execute(args.db, args.output), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
