import pandas as pd

from scripts.research_strategy_a_open_reversal import build_features, select_signals


def _row(code, date, open_price, close_0935, close_0950, close_0955, close_1500):
    return {
        "code": code,
        "trade_date": pd.Timestamp(date),
        "open_0930": open_price,
        "close_0935": close_0935,
        "close_0950": close_0950,
        "close_0955": close_0955,
        "close_1500": close_1500,
        "open_amount": 20_000_000,
        "day_amount": 100_000_000,
    }


def test_confirmed_open_reversal_uses_t1_exit():
    rows = []
    for index in range(10):
        code = f"6000{index:02d}"
        open_price = 9.5 if index == 0 else 9.9 + index * 0.01
        rows.extend([
            _row(code, "2024-01-02", 10, 10, 10, 10, 10),
            _row(code, "2024-01-03", open_price, open_price, open_price + 0.1, open_price + 0.1, open_price + 0.2),
            _row(code, "2024-01-04", 10, 10, 10, 10, 10),
        ])
    frame = pd.DataFrame(rows)
    features = build_features(frame)

    signals = select_signals(features, require_recovery=True, exit_mode="next_0955")

    assert len(signals) == 1
    assert signals.iloc[0]["trade_date"] == pd.Timestamp("2024-01-03")
    assert signals.iloc[0]["exit_date"] == pd.Timestamp("2024-01-04")


def test_recovery_confirmation_rejects_continued_fall():
    frame = pd.DataFrame([
        _row("600000", "2024-01-02", 10, 10, 10, 10, 10),
        _row("600000", "2024-01-03", 9.5, 9.5, 9.3, 9.3, 9.4),
        _row("600000", "2024-01-04", 9.4, 9.5, 9.5, 9.5, 9.5),
    ])
    features = build_features(frame)

    signals = select_signals(features, require_recovery=True, exit_mode="next_0955")

    assert signals.empty
