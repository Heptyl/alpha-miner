"""P1b: TradingBrain must fail closed."""

from unittest.mock import MagicMock, patch

import pytest


@pytest.mark.parametrize(
    ("brain_result", "expected"),
    [
        ({"decision": "buy", "reason": "ok", "confidence": 0.8}, "buy"),
        ({"decision": "pass", "reason": "block", "confidence": 0.8}, "pass"),
        (None, "pass"),
        ({}, "pass"),
        ({"decision": "hold"}, "pass"),
    ],
)
def test_brain_result_contract(brain_result, expected):
    from src.trader.trading_daemon import _get_brain_buy_decision

    brain = MagicMock()
    brain.think_before_buy.return_value = brain_result
    with patch("src.agent.trading_brain.get_brain", return_value=brain):
        result = _get_brain_buy_decision(code="000001", strategy="A")

    assert result["decision"] == expected
    if brain_result is None or not isinstance(brain_result, dict) or expected == "pass" and not brain_result.get("decision"):
        assert result.get("_outcome") == "brain_error_block"


def test_brain_exception_is_blocked():
    from src.trader.trading_daemon import _get_brain_buy_decision

    brain = MagicMock()
    brain.think_before_buy.side_effect = RuntimeError("provider unavailable")
    with patch("src.agent.trading_brain.get_brain", return_value=brain):
        result = _get_brain_buy_decision(code="000001", strategy="B")

    assert result["decision"] == "pass"
    assert result["_outcome"] == "brain_error_block"
    assert "provider unavailable" in result["reason"]
