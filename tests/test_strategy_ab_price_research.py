import pandas as pd

from scripts.research_strategy_ab_from_prices import apply_capacity, verdict


def _signal(code, buy_date, exit_date, net_return, **ranking):
    return {
        "code": code,
        "trade_date": pd.Timestamp(buy_date) - pd.Timedelta(days=1),
        "buy_date": pd.Timestamp(buy_date),
        "exit_date": pd.Timestamp(exit_date),
        "net_return": net_return,
        **ranking,
    }


def test_a_capacity_keeps_three_positions_and_uses_fixed_ranking():
    signals = pd.DataFrame([
        _signal("000001", "2024-01-02", "2024-01-04", 0.01, mom20=0.2, volume_ratio=0.7),
        _signal("000002", "2024-01-02", "2024-01-04", 0.01, mom20=0.4, volume_ratio=0.7),
        _signal("000003", "2024-01-02", "2024-01-04", 0.01, mom20=0.3, volume_ratio=0.7),
        _signal("000004", "2024-01-02", "2024-01-04", 0.01, mom20=0.1, volume_ratio=0.7),
        _signal("000005", "2024-01-03", "2024-01-05", 0.01, mom20=0.5, volume_ratio=0.6),
    ])

    selected = apply_capacity(signals, "A")

    assert set(selected["code"]) == {"000001", "000002", "000003"}
    assert "000004" not in set(selected["code"])
    assert "000005" not in set(selected["code"])


def test_b_capacity_releases_slot_after_same_day_exit():
    signals = pd.DataFrame([
        _signal("000001", "2024-01-02", "2024-01-02", 0.01, next_gap=-0.03),
        _signal("000002", "2024-01-03", "2024-01-03", 0.02, next_gap=-0.04),
    ])

    selected = apply_capacity(signals, "B")

    assert selected["code"].tolist() == ["000001", "000002"]


def test_verdict_requires_capacity_confidence_and_yearly_stability():
    splits = {
        "validation": {
            "signals": 150, "mean_pct": 0.4, "ci95_mean_pct": [0.1, 0.7],
        },
        "oos": {
            "signals": 150, "mean_pct": 0.3, "ci95_mean_pct": [0.05, 0.6],
        },
    }
    yearly = {
        "2024": {"mean_pct": 0.4},
        "2025": {"mean_pct": 0.2},
        "2026": {"mean_pct": 0.1},
    }

    assert verdict(splits, yearly) == "shadow_candidate"

    yearly["2025"]["mean_pct"] = -0.1
    assert verdict(splits, yearly) == "unstable"
