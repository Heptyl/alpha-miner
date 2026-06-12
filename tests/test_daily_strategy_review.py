from scripts.daily_strategy_review import build_metrics


def test_unversioned_history_is_not_attributed_to_current_strategy_version():
    rows = [
        {
            "strategy_code": "",
            "signal_type": "暴跌日狙击(策略B)",
            "strategy_version": "",
            "run_mode": "paper",
            "pnl_pct": -1.5,
            "pnl": -150.0,
        }
    ]

    metrics = build_metrics(rows)

    assert len(metrics) == 1
    assert metrics[0].strategy_code == "B"
    assert metrics[0].strategy_version == "legacy_unversioned_B"
    assert metrics[0].run_mode == "paper"
