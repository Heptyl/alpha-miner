import numpy as np
import pandas as pd

from scripts.research_etf_dual_momentum_corrected import (
    allocation_for,
    build_calendar,
    signal_features,
    turnover,
)


def _prices(start, periods, value=10.0):
    dates = pd.bdate_range(start, periods=periods)
    close = np.linspace(value, value * 1.2, periods)
    return pd.DataFrame({
        "date": dates,
        "open": close,
        "high": close,
        "low": close,
        "close": close,
        "volume": 1.0,
        "amount": 1.0,
    })


def test_calendar_executes_after_signal_and_exits_one_month_later():
    calendar = build_calendar({"510300": _prices("2023-01-02", 100)})

    assert (calendar["signal_date"] < calendar["entry_date"]).all()
    assert (calendar["entry_date"] < calendar["exit_date"]).all()


def test_signal_features_ignore_prices_after_signal_date():
    base = {
        code: _prices("2022-01-03", 320, value=10 + index)
        for index, code in enumerate(("510300", "510500", "159915"))
    }
    signal_date = base["510300"].iloc[280]["date"]
    before = signal_features(base, signal_date)
    for frame in base.values():
        frame.loc[frame["date"] > signal_date, "close"] *= 100
    after = signal_features(base, signal_date)

    assert before == after


def test_defensive_asset_cannot_be_held_before_listing():
    prices = {
        "511260": _prices("2020-01-02", 10, value=100),
    }
    allocation = allocation_for(
        "dual",
        {},
        prices,
        pd.Timestamp("2019-01-02"),
        defensive="bond",
    )

    assert allocation == {"CASH": 1.0}
    assert turnover({"CASH": 1.0}, {"510300": 1.0}) == 1.0

