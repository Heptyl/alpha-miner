from datetime import datetime as RealDateTime
from unittest.mock import patch

from src.trader.daemon_sell_strategies import _check_sell_strategy_a


class AfternoonDateTime(RealDateTime):
    @classmethod
    def now(cls, tz=None):
        return cls(2026, 6, 10, 14, 50, 0, tzinfo=tz)


def _position() -> dict:
    return {
        "code": "600000",
        "name": "测试股",
        "buy_date": "2026-06-09",
        "buy_price": 10.0,
        "highest_price": 10.0,
        "signal_reason": "首阴反包确认 止损¥9.00(首阴低)",
    }


def _quote() -> dict:
    return {
        "price": 10.0,
        "change_pct_calc": 0.0,
    }


def test_strategy_a_does_not_clear_on_first_holding_day_afternoon():
    with patch(
        "src.trader.daemon_sell_strategies.datetime", AfternoonDateTime
    ), patch(
        "src.trader.daemon_sell_strategies._count_trading_days", return_value=1
    ):
        assert _check_sell_strategy_a(_position(), _quote(), {"can_buy": True}) is None


def test_strategy_a_clears_when_max_hold_is_reached():
    with patch(
        "src.trader.daemon_sell_strategies.datetime", AfternoonDateTime
    ), patch(
        "src.trader.daemon_sell_strategies._count_trading_days", return_value=3
    ):
        signal = _check_sell_strategy_a(_position(), _quote(), {"can_buy": True})

    assert signal is not None
    assert signal["type"] == "策略A清仓"
    assert "到期清仓" in signal["reason"]
