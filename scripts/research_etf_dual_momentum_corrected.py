#!/usr/bin/env python3
"""Point-in-time ETF dual-momentum research using adjusted exchange prices."""

from __future__ import annotations

import argparse
import json
import math
import subprocess
import time
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
CACHE_DIR = ROOT / "data" / "etf_prices_qfq"
OUTPUT = ROOT / "reports" / "etf_momentum_corrected.json"
RISK_ASSETS = ("510300", "510500", "159915")
DEFENSIVE_ASSET = "511260"
MARKETS = {
    "510300": 1,
    "510500": 1,
    "159915": 0,
    "511260": 1,
    "511010": 1,
}
SPLITS = {
    "discovery": ("2013-01-01", "2018-12-31"),
    "validation": ("2019-01-01", "2021-12-31"),
    "oos": ("2022-01-01", "2026-12-31"),
}


def download_price(code: str, refresh: bool = False) -> pd.DataFrame:
    cache = CACHE_DIR / f"{code}_qfq.csv"
    if cache.exists() and not refresh:
        return pd.read_csv(cache, parse_dates=["date"])

    market = MARKETS[code]
    url = (
        "https://push2his.eastmoney.com/api/qt/stock/kline/get"
        f"?secid={market}.{code}"
        "&fields1=f1,f2,f3,f4,f5,f6"
        "&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61"
        "&klt=101&fqt=1&beg=20050101&end=20261231"
    )
    payload = None
    errors = []
    for attempt in range(4):
        for executable in ("curl.exe", "curl"):
            try:
                proc = subprocess.run(
                    [executable, "-L", "-sS", "--max-time", "30", url],
                    capture_output=True,
                    text=True,
                    timeout=40,
                    check=True,
                )
                payload = json.loads(proc.stdout)
                if (payload.get("data") or {}).get("klines"):
                    break
            except Exception as exc:
                errors.append(f"{executable}: {exc}")
        if payload and (payload.get("data") or {}).get("klines"):
            break
        time.sleep(attempt + 1)
    if payload is None:
        raise RuntimeError(f"price download failed for {code}: {errors[-2:]}")
    klines = (payload.get("data") or {}).get("klines") or []
    if not klines:
        raise RuntimeError(f"no exchange-price data returned for {code}")

    rows = []
    for line in klines:
        fields = line.split(",")
        rows.append({
            "date": fields[0],
            "open": float(fields[1]),
            "close": float(fields[2]),
            "high": float(fields[3]),
            "low": float(fields[4]),
            "volume": float(fields[5]),
            "amount": float(fields[6]),
        })
    frame = pd.DataFrame(rows)
    frame["date"] = pd.to_datetime(frame["date"])
    frame.sort_values("date", inplace=True)
    frame.drop_duplicates("date", keep="last", inplace=True)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    frame.to_csv(cache, index=False)
    return frame.reset_index(drop=True)


def validate_prices(prices: dict[str, pd.DataFrame]) -> dict:
    quality = {}
    for code, frame in prices.items():
        returns = frame["close"].pct_change()
        quality[code] = {
            "rows": int(len(frame)),
            "start": frame["date"].min().strftime("%Y-%m-%d"),
            "end": frame["date"].max().strftime("%Y-%m-%d"),
            "duplicate_dates": int(frame["date"].duplicated().sum()),
            "bad_ohlc": int(
                (
                    (frame["low"] > frame[["open", "close", "high"]].min(axis=1))
                    | (frame["high"] < frame[["open", "close", "low"]].max(axis=1))
                    | (frame[["open", "high", "low", "close"]] <= 0).any(axis=1)
                ).sum()
            ),
            "max_abs_close_return_pct": round(float(returns.abs().max() * 100), 3),
        }
    return quality


def build_calendar(prices: dict[str, pd.DataFrame]) -> pd.DataFrame:
    market = prices["510300"][["date"]].copy()
    market["month"] = market["date"].dt.to_period("M")
    grouped = market.groupby("month")["date"]
    calendar = pd.DataFrame({
        "signal_date": grouped.max(),
        "entry_date": grouped.min().shift(-1),
        "exit_date": grouped.min().shift(-2),
    }).dropna()
    return calendar.reset_index(drop=True)


def value_on(frame: pd.DataFrame, date: pd.Timestamp, column: str) -> float | None:
    row = frame[frame["date"] == date]
    return None if row.empty else float(row.iloc[0][column])


def signal_features(
    prices: dict[str, pd.DataFrame], signal_date: pd.Timestamp
) -> dict[str, dict[str, float]]:
    result = {}
    for code in RISK_ASSETS:
        history = prices[code][prices[code]["date"] <= signal_date]
        if len(history) < 252:
            continue
        monthly = (
            history.set_index("date")["close"].resample("ME").last().dropna()
        )
        if len(monthly) < 13:
            continue
        result[code] = {
            "trend": float(history.iloc[-1]["close"] / history.tail(200)["close"].mean() - 1),
            "momentum": float(monthly.iloc[-1] / monthly.iloc[-13] - 1),
        }
    return result


def allocation_for(
    mode: str,
    features: dict[str, dict[str, float]],
    prices: dict[str, pd.DataFrame],
    entry_date: pd.Timestamp,
    defensive: str,
) -> dict[str, float]:
    if mode == "trend":
        selected = [code for code, item in features.items() if item["trend"] > 0]
        if selected:
            return {code: 1 / len(selected) for code in selected}
    elif mode == "momentum":
        selected = [code for code, item in features.items() if item["momentum"] > 0]
        if selected:
            return {code: 1 / len(selected) for code in selected}
    elif mode == "dual":
        selected = [code for code, item in features.items() if item["trend"] > 0]
        if selected:
            best = max(selected, key=lambda code: features[code]["momentum"])
            return {best: 1.0}
    else:
        raise ValueError(mode)

    if defensive == "bond" and value_on(
        prices[DEFENSIVE_ASSET], entry_date, "open"
    ) is not None:
        return {DEFENSIVE_ASSET: 1.0}
    return {"CASH": 1.0}


def turnover(previous: dict[str, float], current: dict[str, float]) -> float:
    old = previous or {"CASH": 1.0}
    return sum(
        abs(current.get(key, 0.0) - old.get(key, 0.0))
        for key in set(old) | set(current)
    ) / 2


def run_strategy(
    prices: dict[str, pd.DataFrame],
    calendar: pd.DataFrame,
    mode: str,
    defensive: str,
    cost: float,
) -> pd.DataFrame:
    records = []
    previous = {"CASH": 1.0}
    for row in calendar.itertuples(index=False):
        features = signal_features(prices, row.signal_date)
        allocation = allocation_for(
            mode, features, prices, row.entry_date, defensive
        )
        portfolio_return = 0.0
        valid = True
        for code, weight in allocation.items():
            if code == "CASH":
                continue
            entry = value_on(prices[code], row.entry_date, "open")
            exit_price = value_on(prices[code], row.exit_date, "open")
            if entry is None or exit_price is None:
                valid = False
                break
            portfolio_return += weight * (exit_price / entry - 1)
        if not valid:
            continue
        traded = turnover(previous, allocation)
        net_return = portfolio_return - traded * cost
        records.append({
            "signal_date": row.signal_date,
            "entry_date": row.entry_date,
            "exit_date": row.exit_date,
            "allocation": json.dumps(allocation, sort_keys=True),
            "turnover": traded,
            "gross_return": portfolio_return,
            "net_return": net_return,
            "risk_on": not any(code in allocation for code in ("CASH", DEFENSIVE_ASSET)),
        })
        previous = allocation
    return pd.DataFrame(records)


def run_benchmark(
    prices: dict[str, pd.DataFrame], calendar: pd.DataFrame, cost: float
) -> pd.DataFrame:
    allocation = {code: 1 / len(RISK_ASSETS) for code in RISK_ASSETS}
    records = []
    previous = {"CASH": 1.0}
    for row in calendar.itertuples(index=False):
        returns = []
        for code in RISK_ASSETS:
            entry = value_on(prices[code], row.entry_date, "open")
            exit_price = value_on(prices[code], row.exit_date, "open")
            if entry is None or exit_price is None:
                returns = []
                break
            returns.append(exit_price / entry - 1)
        if not returns:
            continue
        traded = turnover(previous, allocation)
        records.append({
            "signal_date": row.signal_date,
            "entry_date": row.entry_date,
            "exit_date": row.exit_date,
            "allocation": json.dumps(allocation, sort_keys=True),
            "turnover": traded,
            "gross_return": float(np.mean(returns)),
            "net_return": float(np.mean(returns) - traded * cost),
            "risk_on": True,
        })
        previous = allocation
    return pd.DataFrame(records)


def metrics(trades: pd.DataFrame) -> dict:
    if trades.empty:
        return {"months": 0}
    returns = trades["net_return"].to_numpy()
    equity = np.cumprod(1 + returns)
    peak = np.maximum.accumulate(equity)
    years = len(returns) / 12
    yearly = (
        trades.assign(year=trades["entry_date"].dt.year)
        .groupby("year")["net_return"]
        .apply(lambda values: float(np.prod(1 + values) - 1))
    )
    positive_years = yearly[yearly > 0]
    return {
        "months": int(len(trades)),
        "total_return_pct": round(float((equity[-1] - 1) * 100), 2),
        "cagr_pct": round(float((equity[-1] ** (1 / years) - 1) * 100), 2),
        "max_drawdown_pct": round(float(np.min(equity / peak - 1) * 100), 2),
        "sharpe": round(
            float(np.mean(returns) / np.std(returns, ddof=1) * math.sqrt(12)), 3
        ),
        "annual_turnover_pct": round(float(trades["turnover"].mean() * 1200), 2),
        "risk_on_pct": round(float(trades["risk_on"].mean() * 100), 2),
        "positive_month_pct": round(float((returns > 0).mean() * 100), 2),
        "yearly_returns_pct": {
            str(year): round(value * 100, 2) for year, value in yearly.items()
        },
        "largest_positive_year_share_pct": round(
            float(positive_years.max() / positive_years.sum() * 100), 2
        ) if not positive_years.empty else None,
    }


def execute(refresh: bool = False) -> dict:
    prices = {
        code: download_price(code, refresh=refresh)
        for code in (*RISK_ASSETS, DEFENSIVE_ASSET)
    }
    calendar = build_calendar(prices)
    result = {
        "method": {
            "price": "East Money exchange-traded daily OHLC, forward-adjusted",
            "signal": "month-end adjusted close",
            "execution": "next month's first trading-day adjusted open",
            "holding": "entry open to following month's first trading-day open",
            "costs": [0.0015, 0.0030],
        },
        "data_quality": validate_prices(prices),
        "results": {},
    }
    for defensive in ("bond", "cash"):
        result["results"][defensive] = {}
        for cost in (0.0015, 0.0030):
            cost_key = f"{cost * 100:.2f}%"
            result["results"][defensive][cost_key] = {}
            for mode in ("trend", "momentum", "dual"):
                trades = run_strategy(prices, calendar, mode, defensive, cost)
                result["results"][defensive][cost_key][mode] = {
                    split: metrics(
                        trades[trades["entry_date"].between(start, end)]
                    )
                    for split, (start, end) in SPLITS.items()
                }
                if defensive == "bond" and cost == 0.003 and mode == "dual":
                    out = OUTPUT.with_name("etf_dual_corrected_trades.csv")
                    out.parent.mkdir(parents=True, exist_ok=True)
                    trades.to_csv(out, index=False)
            benchmark = run_benchmark(prices, calendar, cost)
            result["results"][defensive][cost_key]["benchmark"] = {
                split: metrics(
                    benchmark[benchmark["entry_date"].between(start, end)]
                )
                for split, (start, end) in SPLITS.items()
            }
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(json.dumps(result, ensure_ascii=False, indent=2))
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--refresh", action="store_true")
    args = parser.parse_args()
    print(json.dumps(execute(refresh=args.refresh), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
