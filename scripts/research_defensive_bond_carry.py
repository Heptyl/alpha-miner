#!/usr/bin/env python3
"""Evaluate a government-bond ETF as a defensive capital-management strategy."""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np

from scripts.research_etf_dual_momentum_corrected import download_price


ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "output" / "research" / "defensive_bond_carry.json"
SPLITS = {
    "discovery": ("2013-01-01", "2018-12-31"),
    "validation": ("2019-01-01", "2021-12-31"),
    "oos": ("2022-01-01", "2026-12-31"),
}


def metrics(frame, cost: float) -> dict:
    if len(frame) < 2:
        return {"days": 0}
    returns = frame["close"].pct_change().dropna().to_numpy(copy=True)
    returns[0] -= cost / 2
    returns[-1] -= cost / 2
    equity = np.cumprod(1 + returns)
    peak = np.maximum.accumulate(equity)
    years = len(returns) / 252
    yearly = (
        frame.assign(year=frame["date"].dt.year)
        .groupby("year")["close"]
        .apply(lambda values: float(values.iloc[-1] / values.iloc[0] - 1))
    )
    return {
        "days": int(len(returns)),
        "total_return_pct": round(float((equity[-1] - 1) * 100), 2),
        "cagr_pct": round(float((equity[-1] ** (1 / years) - 1) * 100), 2),
        "max_drawdown_pct": round(float(np.min(equity / peak - 1) * 100), 2),
        "positive_day_pct": round(float((returns > 0).mean() * 100), 2),
        "yearly_returns_pct": {
            str(year): round(value * 100, 2) for year, value in yearly.items()
        },
    }


def execute(output: Path = OUTPUT) -> dict:
    prices = download_price("511010")
    result = {
        "strategy": "hold_511010_government_bond_etf",
        "role": "defensive_capital_management_not_equity_alpha",
        "execution": "buy once and hold; forward-adjusted exchange price",
        "results": {
            f"{cost * 100:.2f}%": {
                split: metrics(
                    prices[prices["date"].between(start, end)].copy(), cost
                )
                for split, (start, end) in SPLITS.items()
            }
            for cost in (0.0015, 0.0030)
        },
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, ensure_ascii=False, indent=2))
    return result


if __name__ == "__main__":
    print(json.dumps(execute(), ensure_ascii=False, indent=2))
