from unittest.mock import patch

from src.strategy import strategy_b
from src.trader import trading_daemon


def test_ladder_lookup_does_not_import_akshare():
    with patch("builtins.__import__", wraps=__import__) as importer:
        strategy_b._get_ladder_from_zt_pool()

    assert all(call.args[0] != "akshare" for call in importer.call_args_list)


def test_zb_refresh_starts_background_worker():
    class ImmediateThread:
        def __init__(self, target, **_kwargs):
            self.target = target

        def start(self):
            self.target()

    trading_daemon._last_zb_refresh_time = None
    trading_daemon._zb_refresh_inflight = False
    with patch.object(trading_daemon, "_is_trading_time", return_value=True), \
         patch.object(trading_daemon.threading, "Thread", ImmediateThread), \
         patch("src.data.sources.akshare_zt_pool.fetch_zb_pool", return_value=None):
        trading_daemon._refresh_zb_pool_if_needed()

    assert trading_daemon._last_zb_refresh_time is not None
    assert trading_daemon._zb_refresh_inflight is False


def test_market_perception_returns_fallback_without_waiting():
    class DeferredThread:
        def __init__(self, target, **_kwargs):
            self.target = target

        def start(self):
            pass

    trading_daemon._market_perception_cache = None
    trading_daemon._market_perception_refresh_time = None
    trading_daemon._market_perception_inflight = False
    with patch.object(trading_daemon.threading, "Thread", DeferredThread):
        result = trading_daemon._get_market_perception_nonblocking(23.456)

    assert result["ratio_now"] == 23.5
    assert result["weight_red_pct"] == -1
    assert result["style"] == "未知"
    assert trading_daemon._market_perception_inflight is True


def test_market_perception_reuses_snapshot_with_current_ratio():
    trading_daemon._market_perception_cache = {
        "style": "均衡",
        "weight_red_pct": 50.0,
        "top5_up_sectors": [{"name": "半导体", "pct": 2.0}],
    }
    trading_daemon._market_perception_refresh_time = trading_daemon.datetime.now()
    trading_daemon._market_perception_inflight = False

    result = trading_daemon._get_market_perception_nonblocking(31.0)

    assert result["ratio_now"] == 31.0
    assert result["weight_red_pct"] == 50.0
    assert result["style"] == "均衡"
