import pandas as pd

from scripts.research_strategy_a_52week_high import build_features as build_high_features
from scripts.research_strategy_a_volume_premium import build_features as build_volume_features


def _frame(periods=260):
    dates = pd.bdate_range("2022-01-03", periods=periods)
    return pd.DataFrame({
        "code": "600000",
        "trade_date": dates,
        "open": 10.0,
        "close": 10.0,
        "amount": 100_000_000.0,
        "rank_no": 1,
    })


def test_volume_features_remove_corporate_action_jump_from_adjusted_open():
    frame = _frame(100)
    frame.loc[70:, ["open", "close"]] = 5.0

    features = build_volume_features(frame)

    assert abs(features.iloc[-1]["adjusted_open"] - 1.0) < 1e-12


def test_52week_high_uses_clean_return_chain():
    frame = _frame()
    frame.loc[220:, ["open", "close"]] = 5.0

    features = build_high_features(frame)

    assert abs(features.iloc[-1]["high_ratio"] - 1.0) < 1e-12
