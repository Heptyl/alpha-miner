import pandas as pd

from scripts.research_strategy_a_tail_momentum import build_signals


def test_tail_momentum_uses_1450_signal_and_1455_entry_with_t1_exit():
    rows = []
    for day, prices in [
        ("2024-01-02", (10.0, 10.2, 10.25)),
        ("2024-01-03", (10.3, 10.4, 10.5)),
    ]:
        rows.append({
            "code": "600000",
            "trade_date": pd.Timestamp(day),
            "close_1430": prices[0],
            "close_1450": prices[1],
            "close_1455": prices[2],
            "tail_amount": 20_000_000,
            "day_amount": 100_000_000,
            "pre_close": 10.0,
            "daily_close": prices[2],
        })

    signals = build_signals(pd.DataFrame(rows), mode="momentum", quantile=1)

    assert len(signals) == 1
    assert signals.iloc[0]["buy_price"] == 10.25
    assert signals.iloc[0]["exit_price"] == 10.5
    assert signals.iloc[0]["exit_date"] == pd.Timestamp("2024-01-03")


def test_tail_reversal_selects_negative_tail_return():
    rows = []
    for code, first_prices, second_prices in [
        ("600000", (10.2, 10.0, 10.0), (10.0, 10.1, 10.2)),
        ("600001", (10.0, 10.2, 10.2), (10.2, 10.3, 10.3)),
    ]:
        for day, prices in [
            ("2024-01-02", first_prices),
            ("2024-01-03", second_prices),
        ]:
            rows.append({
                "code": code,
                "trade_date": pd.Timestamp(day),
                "close_1430": prices[0],
                "close_1450": prices[1],
                "close_1455": prices[2],
                "tail_amount": 20_000_000,
                "day_amount": 100_000_000,
                "pre_close": 10.0,
                "daily_close": prices[2],
            })

    signals = build_signals(pd.DataFrame(rows), mode="reversal", quantile=0.5)

    assert signals["code"].tolist() == ["600000"]
