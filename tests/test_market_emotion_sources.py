from unittest.mock import patch

from src.trader import market_emotion


def _quote(price, pre_close=10):
    return {"price": price, "yesterday_close": pre_close}


def test_tencent_breadth_chunks_and_counts():
    codes = [f"60{i:04d}" for i in range(4000)]

    def fake_fetch(chunk, timeout):
        assert len(chunk) <= market_emotion.TENCENT_CHUNK_SIZE
        return {
            code: _quote(11 if int(code[-1]) < 4 else 9 if int(code[-1]) < 9 else 10)
            for code in chunk
        }

    with patch.object(market_emotion, "_load_complete_market_universe", return_value=codes), \
         patch("src.trader.realtime_quote._curl_tencent", side_effect=fake_fetch) as fetch:
        result = market_emotion._fetch_breadth_from_tencent()

    assert fetch.call_count == 20
    assert result["up_count"] == 1600
    assert result["down_count"] == 2000
    assert result["flat_count"] == 400
    assert result["coverage"] == 1.0


def test_tencent_breadth_rejects_partial_market():
    codes = [f"60{i:04d}" for i in range(4000)]
    partial = {code: _quote(11) for code in codes[:3000]}
    with patch.object(market_emotion, "_load_complete_market_universe", return_value=codes), \
         patch("src.trader.realtime_quote._curl_tencent", return_value=partial):
        assert market_emotion._fetch_breadth_from_tencent() is None


def test_limit_api_failure_does_not_invalidate_breadth():
    breadth = {
        "up_count": 1000, "down_count": 3000, "flat_count": 50,
        "quote_count": 4050, "universe_count": 4100, "coverage": 0.9878,
    }
    with patch.object(market_emotion, "_fetch_breadth_from_tencent", return_value=breadth), \
         patch.object(market_emotion, "_fetch_limit_count_fast", return_value=-1):
        result = market_emotion._fetch_from_eastmoney()

    assert result["up_count"] == 1000
    assert result["zt_count"] == -1
    assert result["source"] == "tencent_breadth+eastmoney_limits"


def test_missing_core_breadth_returns_none():
    with patch.object(market_emotion, "_fetch_breadth_from_tencent", return_value=None):
        assert market_emotion._fetch_from_eastmoney() is None
