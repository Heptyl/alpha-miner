import numpy as np
import pandas as pd

from scripts.research_strategy_a_low_volatility import build_features


def test_momentum_excludes_unadjusted_corporate_action_jump():
    dates = pd.bdate_range("2023-01-02", periods=140)
    close = np.full(len(dates), 10.0)
    close[125:] = 5.0
    frame = pd.DataFrame({
        "code": "600000",
        "trade_date": dates,
        "open": close,
        "close": close,
        "amount": 100_000_000.0,
        "target_year": 2023,
        "rank_no": 1,
    })

    features = build_features(frame)

    assert np.isnan(features.loc[125, "ret1"])
    assert abs(features.iloc[-1]["mom120"]) < 1e-12

