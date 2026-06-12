import pandas as pd

from scripts.research_strategy_a_overnight_reversal import build_features, select_signals


def _row(code, date, open_price, close_1450, close_1455, next_open):
    return {
        "code": code,
        "trade_date": pd.Timestamp(date),
        "open_0930": open_price,
        "close_0935": next_open,
        "close_1450": close_1450,
        "close_1455": close_1455,
        "close_1500": close_1455,
        "day_amount": 100_000_000,
    }


def test_overnight_reversal_enters_tail_and_exits_next_morning():
    rows = []
    for index in range(10):
        code = f"6000{index:02d}"
        close_1450 = 9.2 if index == 0 else 9.8 + index * 0.01
        rows.extend([
            _row(code, "2024-01-02", 10, close_1450, close_1450, 10),
            _row(code, "2024-01-03", 10, 10, 10, 10.2),
        ])
    features = build_features(pd.DataFrame(rows))

    signals = select_signals(features, "loser")

    assert signals["code"].tolist() == ["600000"]
    assert signals.iloc[0]["exit_date"] == pd.Timestamp("2024-01-03")
