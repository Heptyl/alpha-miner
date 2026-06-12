#!/usr/bin/env python3
"""Research strategy A/B hypotheses from point-in-time daily prices.

This script is intentionally independent from the trading daemon. It defines
the hypothesis families up front, evaluates them on fixed time splits, and
writes reproducible JSON/Markdown evidence without changing live parameters.
"""

from __future__ import annotations

import argparse
import json
import math
import sqlite3
from dataclasses import asdict, dataclass
from pathlib import Path

import numpy as np
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB = PROJECT_ROOT / "data" / "alpha_miner.db"
DEFAULT_OUTPUT = PROJECT_ROOT / "output" / "research" / "strategy_ab_price_research.json"

ROUND_TRIP_COST = 0.0025  # 10 bps slippage each side + 5 bps taxes/fees.
MIN_CROSS_SECTION = 3500
MIN_VALID_RATIO = 0.95


@dataclass(frozen=True)
class Hypothesis:
    strategy: str
    name: str
    description: str
    hold_days: int
    params: dict[str, float | int | str]


SPLITS = {
    "discovery": ("2022-01-01", "2023-12-31"),
    "validation": ("2024-01-01", "2024-12-31"),
    "oos": ("2025-01-01", "2026-12-31"),
}


HYPOTHESES = [
    Hypothesis(
        strategy="A",
        name="A7_volume_breakout_20d_3d",
        description="收盘突破前20日高点、量比不低于1.5且当日未涨停；次日开盘买入，持3日。",
        hold_days=3,
        params={"signal_family": "volume_breakout", "breakout_days": 20,
                "volume_ratio_min": 1.5, "day_return_max": 0.095},
    ),
    Hypothesis(
        strategy="A",
        name="A8_volume_breakout_20d_5d",
        description="收盘突破前20日高点、量比不低于1.5且当日未涨停；次日开盘买入，持5日。",
        hold_days=5,
        params={"signal_family": "volume_breakout", "breakout_days": 20,
                "volume_ratio_min": 1.5, "day_return_max": 0.095},
    ),
    Hypothesis(
        strategy="A",
        name="A9_volume_breakout_60d_5d",
        description="收盘突破前60日高点、量比不低于1.5且当日未涨停；次日开盘买入，持5日。",
        hold_days=5,
        params={"signal_family": "volume_breakout", "breakout_days": 60,
                "volume_ratio_min": 1.5, "day_return_max": 0.095},
    ),
    Hypothesis(
        strategy="A",
        name="A4_daily_momentum_top10",
        description="非涨停股前一日收益位于全市场前10%，次日开盘买入，满足T+1后收盘卖出。",
        hold_days=1,
        params={"signal_family": "daily_momentum", "return_percentile_min": 0.90,
                "market_up_only": 0, "volume_ratio_min": 0.0},
    ),
    Hypothesis(
        strategy="A",
        name="A5_daily_momentum_top10_market_up",
        description="非涨停股前一日收益位于全市场前10%，仅市场上涨日触发，次日开盘买入，满足T+1后收盘卖出。",
        hold_days=1,
        params={"signal_family": "daily_momentum", "return_percentile_min": 0.90,
                "market_up_only": 1, "volume_ratio_min": 0.0},
    ),
    Hypothesis(
        strategy="A",
        name="A6_daily_momentum_top10_volume",
        description="非涨停股前一日收益位于全市场前10%且成交量高于20日均量，次日开盘买入，满足T+1后收盘卖出。",
        hold_days=1,
        params={"signal_family": "daily_momentum", "return_percentile_min": 0.90,
                "market_up_only": 0, "volume_ratio_min": 1.0},
    ),
    Hypothesis(
        strategy="A",
        name="A1_trend_pullback_3d",
        description="20日上涨10%-40%，仍在MA20上方，单日回撤1%-5%且缩量；次日开盘买入，持3日。",
        hold_days=3,
        params={"mom20_min": 0.10, "mom20_max": 0.40, "day_min": -0.05, "day_max": -0.01,
                "volume_ratio_max": 0.80},
    ),
    Hypothesis(
        strategy="A",
        name="A2_trend_pullback_5d",
        description="与A1相同，固定持有5日，用于检验收益是否具有持续性。",
        hold_days=5,
        params={"mom20_min": 0.10, "mom20_max": 0.40, "day_min": -0.05, "day_max": -0.01,
                "volume_ratio_max": 0.80},
    ),
    Hypothesis(
        strategy="A",
        name="A3_strong_trend_pullback_3d",
        description="更强趋势过滤：20日上涨20%-50%，单日回撤1%-5%且缩量；次日开盘买入，持3日。",
        hold_days=3,
        params={"mom20_min": 0.20, "mom20_max": 0.50, "day_min": -0.05, "day_max": -0.01,
                "volume_ratio_max": 0.80},
    ),
    Hypothesis(
        strategy="B",
        name="B1_first_limit_gap_down_1d",
        description="主板首次涨停后，次日低开2%-6%买入，满足T+1后收盘卖出。",
        hold_days=1,
        params={"gap_min": -0.06, "gap_max": -0.02, "first_limit_only": 1},
    ),
    Hypothesis(
        strategy="B",
        name="B2_first_limit_gap_down_3d",
        description="主板首次涨停后，次日低开2%-6%买入，持3日。",
        hold_days=3,
        params={"gap_min": -0.06, "gap_max": -0.02, "first_limit_only": 1},
    ),
    Hypothesis(
        strategy="B",
        name="B3_any_limit_gap_down_1d",
        description="主板任意涨停后，次日低开2%-6%买入，满足T+1后收盘卖出；用于检验首次涨停过滤价值。",
        hold_days=1,
        params={"gap_min": -0.06, "gap_max": -0.02, "first_limit_only": 0},
    ),
]


def load_prices(db_path: Path) -> pd.DataFrame:
    con = sqlite3.connect(db_path)
    try:
        date_stats = pd.read_sql_query(
            """
            SELECT trade_date,
                   COUNT(DISTINCT stock_code) AS stock_count,
                   1.0 * SUM(
                       open > 0 AND high > 0 AND low > 0 AND close > 0 AND pre_close > 0
                   ) / COUNT(*) AS valid_ratio
            FROM daily_price
            GROUP BY trade_date
            ORDER BY trade_date
            """,
            con,
        )
        recent_universe = date_stats["stock_count"].shift(1).rolling(
            20, min_periods=1
        ).median()
        coverage_floor = np.maximum(MIN_CROSS_SECTION, recent_universe * 0.90)
        valid_dates = date_stats.loc[
            (date_stats["stock_count"] >= coverage_floor)
            & (date_stats["valid_ratio"] >= MIN_VALID_RATIO),
            "trade_date",
        ].tolist()
        if not valid_dates:
            raise RuntimeError("no complete trading dates found")

        placeholders = ",".join("?" for _ in valid_dates)
        prices = pd.read_sql_query(
            f"""
            SELECT stock_code AS code, trade_date,
                   open, high, low, close, pre_close, volume, amount
            FROM daily_price
            WHERE trade_date IN ({placeholders})
              AND open > 0 AND high > 0 AND low > 0 AND close > 0
              AND pre_close > 0 AND volume > 0
            ORDER BY stock_code, trade_date
            """,
            con,
            params=valid_dates,
        )
    finally:
        con.close()

    prices["trade_date"] = pd.to_datetime(prices["trade_date"])
    prices = prices[~prices["code"].str.startswith(("4", "8", "9", "200", "900"))].copy()
    return prices


def build_features(prices: pd.DataFrame) -> pd.DataFrame:
    df = prices.sort_values(["code", "trade_date"]).copy()
    market_dates = pd.Index(sorted(df["trade_date"].unique()))
    date_position = pd.Series(np.arange(len(market_dates)), index=market_dates)
    df["market_date_idx"] = df["trade_date"].map(date_position)
    by_code = df.groupby("code", sort=False)

    df["ret1"] = df["close"] / df["pre_close"] - 1
    df["ret1_percentile"] = df.groupby("trade_date")["ret1"].rank(pct=True)
    df["market_ret1"] = df.groupby("trade_date")["ret1"].transform("mean")
    df["mom20"] = by_code["close"].pct_change(20)
    df["ma20"] = by_code["close"].transform(lambda s: s.rolling(20, min_periods=20).mean())
    df["avg_volume20"] = by_code["volume"].transform(
        lambda s: s.shift(1).rolling(20, min_periods=15).mean()
    )
    df["volume_ratio"] = df["volume"] / df["avg_volume20"]
    df["avg_amount20"] = by_code["amount"].transform(
        lambda s: s.shift(1).rolling(20, min_periods=15).median()
    )
    df["prev_high20"] = by_code["high"].transform(
        lambda s: s.shift(1).rolling(20, min_periods=20).max()
    )
    df["prev_high60"] = by_code["high"].transform(
        lambda s: s.shift(1).rolling(60, min_periods=60).max()
    )
    df["breakout_strength20"] = df["close"] / df["prev_high20"] - 1
    df["breakout_strength60"] = df["close"] / df["prev_high60"] - 1

    main_board = df["code"].str.startswith(("000", "001", "002", "003", "600", "601", "603", "605"))
    df["is_main_board"] = main_board
    df["is_limit_up"] = main_board & (df["ret1"] >= 0.095)
    prev_date = by_code["trade_date"].shift(1)
    prev_is_adjacent = (
        df["market_date_idx"] - prev_date.map(date_position)
    ).eq(1)
    df["prev_limit_up"] = (
        by_code["is_limit_up"].shift(1).fillna(False).astype(bool) & prev_is_adjacent
    )

    for horizon in (1, 3, 5):
        # A-share stocks bought on T cannot be sold until T+1. A one-day hold
        # therefore exits two market dates after the signal date.
        exit_offset = max(2, horizon)
        future_close = by_code["close"].shift(-exit_offset)
        future_date = by_code["trade_date"].shift(-exit_offset)
        next_open = by_code["open"].shift(-1)
        next_date = by_code["trade_date"].shift(-1)
        expected_next_idx = df["market_date_idx"] + 1
        expected_exit_idx = df["market_date_idx"] + exit_offset
        is_contiguous = (
            next_date.map(date_position).eq(expected_next_idx)
            & future_date.map(date_position).eq(expected_exit_idx)
        )
        net_return = future_close / next_open - 1 - ROUND_TRIP_COST
        df[f"net_ret_{horizon}d"] = net_return.where(is_contiguous)
        df[f"buy_date_{horizon}d"] = next_date.where(is_contiguous)
        df[f"exit_date_{horizon}d"] = future_date.where(is_contiguous)

    df["next_open"] = by_code["open"].shift(-1)
    df["next_pre_close"] = by_code["pre_close"].shift(-1)
    next_date = by_code["trade_date"].shift(-1)
    next_is_adjacent = next_date.map(date_position).eq(df["market_date_idx"] + 1)
    df["next_gap"] = (df["next_open"] / df["next_pre_close"] - 1).where(next_is_adjacent)
    return df


def select_signals(df: pd.DataFrame, hypothesis: Hypothesis) -> pd.DataFrame:
    p = hypothesis.params
    liquid = df["avg_amount20"].fillna(0) >= 50_000_000
    executable = df[f"net_ret_{hypothesis.hold_days}d"].notna()

    if p.get("signal_family") == "volume_breakout":
        breakout_days = int(p["breakout_days"])
        prior_high = df[f"prev_high{breakout_days}"]
        mask = (
            liquid
            & executable
            & (df["close"] > prior_high)
            & (df["volume_ratio"] >= float(p["volume_ratio_min"]))
            & (df["ret1"] < float(p["day_return_max"]))
            & (df["ret1"] > 0)
            & (df["close"].between(3, 200))
        )
    elif p.get("signal_family") == "daily_momentum":
        mask = (
            liquid
            & executable
            & ~df["is_limit_up"]
            & (df["ret1_percentile"] >= float(p["return_percentile_min"]))
            & (df["volume_ratio"] >= float(p["volume_ratio_min"]))
            & (df["close"].between(3, 200))
        )
        if int(p["market_up_only"]):
            mask &= df["market_ret1"] > 0
    elif hypothesis.strategy == "A":
        mask = (
            liquid
            & executable
            & df["mom20"].between(float(p["mom20_min"]), float(p["mom20_max"]), inclusive="both")
            & df["ret1"].between(float(p["day_min"]), float(p["day_max"]), inclusive="both")
            & (df["close"] > df["ma20"])
            & (df["volume_ratio"] <= float(p["volume_ratio_max"]))
            & (df["close"].between(3, 200))
        )
    else:
        mask = (
            liquid
            & executable
            & df["is_main_board"]
            & df["is_limit_up"]
            & df["next_gap"].between(float(p["gap_min"]), float(p["gap_max"]), inclusive="both")
            & (df["next_open"].between(3, 200))
        )
        if int(p["first_limit_only"]):
            mask &= ~df["prev_limit_up"]

    columns = [
        "code", "trade_date", f"net_ret_{hypothesis.hold_days}d",
        f"buy_date_{hypothesis.hold_days}d", f"exit_date_{hypothesis.hold_days}d",
    ]
    if hypothesis.strategy == "A":
        columns.extend([
            "mom20", "volume_ratio", "ret1", "ret1_percentile",
            "breakout_strength20", "breakout_strength60",
        ])
    else:
        columns.append("next_gap")
    result = df.loc[mask, columns].copy()
    result.rename(columns={
        f"net_ret_{hypothesis.hold_days}d": "net_return",
        f"buy_date_{hypothesis.hold_days}d": "buy_date",
        f"exit_date_{hypothesis.hold_days}d": "exit_date",
    }, inplace=True)
    if p.get("signal_family") == "volume_breakout":
        result["ranking_score"] = result[f"breakout_strength{int(p['breakout_days'])}"]
    elif p.get("signal_family") == "daily_momentum":
        result["ranking_score"] = result["ret1"]
    return result


def bootstrap_ci(values: np.ndarray, seed: int = 20260610) -> tuple[float, float]:
    if len(values) < 2:
        return (math.nan, math.nan)
    rng = np.random.default_rng(seed)
    means = np.empty(2000)
    for i in range(len(means)):
        means[i] = rng.choice(values, size=len(values), replace=True).mean()
    return tuple(float(v) for v in np.percentile(means, [2.5, 97.5]))


def metrics(signals: pd.DataFrame, extra_cost: float = 0.0) -> dict:
    values = signals["net_return"].dropna().to_numpy(dtype=float) - extra_cost
    if not len(values):
        return {
            "signals": 0, "days": 0, "mean_pct": 0, "median_pct": 0,
            "win_rate_pct": 0, "profit_factor": 0, "ci95_mean_pct": [None, None],
            "max_drawdown_pct": 0, "top_10_profit_share_pct": None,
        }
    wins = values[values > 0].sum()
    losses = -values[values < 0].sum()
    ci_low, ci_high = bootstrap_ci(values)
    equity = np.cumprod(1 + values)
    peaks = np.maximum.accumulate(equity)
    max_drawdown = float(np.min(equity / peaks - 1))
    positive_values = np.sort(values[values > 0])[::-1]
    top_10_profit_share = (
        float(positive_values[:10].sum() / wins * 100) if wins > 0 else None
    )
    return {
        "signals": int(len(values)),
        "days": int(signals["trade_date"].nunique()),
        "mean_pct": round(float(values.mean() * 100), 4),
        "median_pct": round(float(np.median(values) * 100), 4),
        "win_rate_pct": round(float((values > 0).mean() * 100), 2),
        "profit_factor": round(float(wins / losses), 3) if losses > 0 else None,
        "ci95_mean_pct": [round(ci_low * 100, 4), round(ci_high * 100, 4)],
        "p05_pct": round(float(np.percentile(values, 5) * 100), 4),
        "p95_pct": round(float(np.percentile(values, 95) * 100), 4),
        "max_drawdown_pct": round(max_drawdown * 100, 2),
        "top_10_profit_share_pct": (
            round(top_10_profit_share, 2) if top_10_profit_share is not None else None
        ),
    }


def split_signals(signals: pd.DataFrame) -> dict[str, dict]:
    result = {}
    for name, (start, end) in SPLITS.items():
        start_ts, end_ts = pd.Timestamp(start), pd.Timestamp(end)
        result[name] = metrics(signals[signals["trade_date"].between(start_ts, end_ts)])
    return result


def yearly_metrics(signals: pd.DataFrame) -> dict[str, dict]:
    if signals.empty:
        return {}
    return {
        str(year): metrics(group)
        for year, group in signals.groupby(signals["trade_date"].dt.year, sort=True)
    }


def cost_sensitivity(signals: pd.DataFrame) -> dict[str, dict]:
    return {
        f"{total_cost * 100:.2f}%": metrics(
            signals, extra_cost=max(0.0, total_cost - ROUND_TRIP_COST)
        )
        for total_cost in (0.0025, 0.0050, 0.0100)
    }


def apply_capacity(
    signals: pd.DataFrame, strategy: str, signal_family: str = ""
) -> pd.DataFrame:
    if signals.empty:
        return signals.copy()

    max_positions = 3 if strategy == "A" else 1
    active: list[tuple[pd.Timestamp, str]] = []
    selected_rows = []

    for buy_date, group in signals.groupby("buy_date", sort=True):
        active = [(exit_date, code) for exit_date, code in active if exit_date >= buy_date]
        available = max_positions - len(active)
        if available <= 0:
            continue

        held_codes = {code for _, code in active}
        candidates = group[~group["code"].isin(held_codes)].copy()
        if strategy == "A":
            if signal_family == "volume_breakout":
                candidates.sort_values(
                    ["ranking_score", "volume_ratio", "code"],
                    ascending=[False, False, True],
                    inplace=True,
                )
            elif signal_family == "daily_momentum":
                candidates.sort_values(
                    ["ranking_score", "volume_ratio", "code"],
                    ascending=[False, False, True],
                    inplace=True,
                )
            else:
                candidates.sort_values(
                    ["mom20", "volume_ratio", "code"],
                    ascending=[False, True, True],
                    inplace=True,
                )
        else:
            candidates.sort_values(["next_gap", "code"], ascending=[True, True], inplace=True)

        chosen = candidates.head(available)
        selected_rows.extend(chosen.to_dict("records"))
        active.extend((row.exit_date, row.code) for row in chosen.itertuples())

    if not selected_rows:
        return signals.iloc[0:0].copy()
    return pd.DataFrame(selected_rows).sort_values(["buy_date", "code"]).reset_index(drop=True)


def verdict(portfolio_splits: dict[str, dict], portfolio_yearly: dict[str, dict]) -> str:
    validation = portfolio_splits["validation"]
    oos = portfolio_splits["oos"]
    enough = validation["signals"] >= 100 and oos["signals"] >= 100
    positive = (
        validation["mean_pct"] > 0
        and oos["mean_pct"] > 0
        and validation["ci95_mean_pct"][0] is not None
        and validation["ci95_mean_pct"][0] > 0
        and oos["ci95_mean_pct"][0] > 0
    )
    yearly_positive = all(
        portfolio_yearly.get(year, {}).get("mean_pct", 0) > 0
        for year in ("2024", "2025", "2026")
    )
    if enough and positive:
        return "shadow_candidate" if yearly_positive else "unstable"
    if not enough:
        return "insufficient_sample"
    return "reject"


def render_markdown(payload: dict) -> str:
    lines = [
        "# A/B 基础行情研究结果",
        "",
        f"- 数据库：`{payload['database']}`",
        f"- 完整交易日：{payload['data']['dates']}（{payload['data']['start']} 至 {payload['data']['end']}）",
        f"- 股票日记录：{payload['data']['rows']:,}",
        f"- 往返成本假设：{payload['cost_pct']:.2f}%",
        "- 固定切分：2022-2023 发现期；2024 验证期；2025-2026 最终样本外期。",
        "",
        "| 假设 | 验证期容量组合 | 样本外容量组合 | 结论 |",
        "|---|---:|---:|---|",
    ]
    for item in payload["hypotheses"]:
        pval = item["portfolio"]["splits"]["validation"]
        poos = item["portfolio"]["splits"]["oos"]
        lines.append(
            f"| {item['name']} | {pval['signals']} / {pval['mean_pct']:+.3f}% / "
            f"{pval['profit_factor']} | {poos['signals']} / {poos['mean_pct']:+.3f}% / "
            f"{poos['profit_factor']} | {item['verdict']} |"
        )
    lines.extend([
        "",
        "表格中的数字依次为样本数 / 笔均净收益 / PF，已应用 A 最多3只、B 最多1只的持仓约束。",
        "`shadow_candidate` 仍不代表可接入 paper，必须先做实时 shadow 成交验证。",
        "",
    ])
    return "\n".join(lines)


def run(db_path: Path, output_path: Path) -> dict:
    prices = load_prices(db_path)
    features = build_features(prices)
    results = []
    for hypothesis in HYPOTHESES:
        signals = select_signals(features, hypothesis)
        portfolio_signals = apply_capacity(
            signals,
            hypothesis.strategy,
            str(hypothesis.params.get("signal_family", "")),
        )
        split_result = split_signals(signals)
        portfolio_splits = split_signals(portfolio_signals)
        portfolio_yearly = yearly_metrics(portfolio_signals)
        results.append({
            **asdict(hypothesis),
            "splits": split_result,
            "yearly": yearly_metrics(signals),
            "portfolio": {
                "max_positions": 3 if hypothesis.strategy == "A" else 1,
                "selection_rule": (
                    "mom20 descending, volume_ratio ascending"
                    if hypothesis.strategy == "A"
                    else "next_gap ascending"
                ),
                "splits": portfolio_splits,
                "yearly": portfolio_yearly,
                "cost_sensitivity": {
                    name: cost_sensitivity(
                        portfolio_signals[
                            portfolio_signals["trade_date"].between(
                                pd.Timestamp(start), pd.Timestamp(end)
                            )
                        ]
                    )
                    for name, (start, end) in SPLITS.items()
                },
            },
            "verdict": verdict(portfolio_splits, portfolio_yearly),
        })

    payload = {
        "database": str(db_path),
        "cost_pct": ROUND_TRIP_COST * 100,
        "data": {
            "rows": int(len(prices)),
            "dates": int(prices["trade_date"].nunique()),
            "stocks": int(prices["code"].nunique()),
            "start": prices["trade_date"].min().strftime("%Y-%m-%d"),
            "end": prices["trade_date"].max().strftime("%Y-%m-%d"),
        },
        "hypotheses": results,
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    output_path.with_suffix(".md").write_text(render_markdown(payload), encoding="utf-8")
    return payload


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    payload = run(args.db, args.output)
    print(render_markdown(payload))


if __name__ == "__main__":
    main()
