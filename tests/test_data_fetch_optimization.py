"""数据采集优化的回归测试。"""

import threading
from unittest.mock import MagicMock

import pandas as pd
import pytest

from src.data.collector import _fetch_many, _fetch_news_parallel, collect_date
from src.data.sources import akshare_fund_flow, akshare_price
from src.data.storage import Storage


def test_tencent_parser_uses_real_volume_field(monkeypatch):
    parts = [""] * 40
    parts[0] = 'v_sz000001="1'
    parts[3] = "11.11"
    parts[4] = "11.25"
    parts[5] = "11.22"
    parts[30] = "20260814154915"
    parts[33] = "11.23"
    parts[34] = "11.11"
    parts[35] = "11.11/832344/929098438"
    parts[36] = "832344"
    parts[37] = "92910"  # 成交额(万元)，旧代码曾误当成成交量
    parts[38] = "0.43"

    response = MagicMock()
    response.text = "~".join(parts) + ";"
    response.raise_for_status.return_value = None
    session = MagicMock()
    session.get.return_value = response
    monkeypatch.setattr(akshare_price, "_direct_session", lambda retries=3: session)

    result = akshare_price._fetch_tencent_batch(["000001"])

    assert len(result) == 1
    assert result.iloc[0]["volume"] == 832344
    assert result.iloc[0]["amount"] == 929098438
    assert result.iloc[0]["_quote_date"] == "2026-08-14"
    session.close.assert_called_once()


def test_tencent_session_proxy_is_explicitly_configurable(monkeypatch):
    monkeypatch.delenv("ALPHA_MINER_USE_PROXY", raising=False)
    direct = akshare_price._direct_session(retries=1)
    assert direct.trust_env is False
    direct.close()

    monkeypatch.setenv("ALPHA_MINER_USE_PROXY", "true")
    proxied = akshare_price._direct_session(retries=1)
    assert proxied.trust_env is True
    proxied.close()


@pytest.mark.parametrize(
    ("raw", "expected"),
    [("43.72亿", 4_372_000_000), ("-1812.83万", -18_128_300), ("12", 12)],
)
def test_fund_flow_amount_is_normalized_to_yuan(raw, expected):
    assert akshare_fund_flow._parse_amount(raw) == pytest.approx(expected)


def test_fetch_many_runs_concurrently_and_isolates_failures():
    barrier = threading.Barrier(2, timeout=1)

    def fetch_one():
        barrier.wait()
        return pd.DataFrame({"value": [1]})

    def fetch_two():
        barrier.wait()
        return pd.DataFrame({"value": [2]})

    def fetch_broken():
        raise RuntimeError("source down")

    outcomes = _fetch_many(
        {"one": fetch_one, "two": fetch_two, "broken": fetch_broken},
        max_workers=3,
    )

    assert len(outcomes["one"].data) == 1
    assert len(outcomes["two"].data) == 1
    assert isinstance(outcomes["broken"].error, RuntimeError)


def test_news_parallel_deduplicates(monkeypatch):
    barrier = threading.Barrier(2, timeout=1)

    def fake_fetch(stock_code, trade_date):
        barrier.wait()
        return pd.DataFrame([{
            "news_id": "same-news",
            "stock_code": stock_code,
            "title": "same",
            "publish_time": trade_date,
        }])

    monkeypatch.setattr("src.data.collector.akshare_news.fetch", fake_fetch)

    result = _fetch_news_parallel(["000001", "000002"], "2026-08-14", max_workers=2)

    assert len(result) == 1
    assert result.iloc[0]["news_id"] == "same-news"


def test_backfill_never_stamps_live_only_sources_as_history(tmp_path, monkeypatch):
    db = Storage(str(tmp_path / "collector.db"))
    db.init_db()
    def empty(*args, **kwargs):
        return pd.DataFrame()

    monkeypatch.setattr("src.data.collector.akshare_zt_pool.fetch_zt_pool", empty)
    monkeypatch.setattr("src.data.collector.akshare_zt_pool.fetch_zb_pool", empty)
    monkeypatch.setattr("src.data.collector.akshare_zt_pool.fetch_strong_pool", empty)
    monkeypatch.setattr("src.data.collector.akshare_lhb.fetch", empty)
    monkeypatch.setattr("src.data.collector.akshare_concept.fetch", empty)
    monkeypatch.setattr(
        "src.data.collector.akshare_fund_flow.fetch",
        lambda *args, **kwargs: pytest.fail("historical collection called live fund flow"),
    )
    monkeypatch.setattr(
        "src.data.collector.akshare_price.fetch_today",
        lambda *args, **kwargs: pytest.fail("historical collection called live quote"),
    )
    monkeypatch.setattr(
        "src.data.collector.ak.stock_market_activity_legu",
        lambda: pytest.fail("historical collection called live market emotion"),
    )
    monkeypatch.setattr(
        "src.data.collector.akshare_price.fetch_history",
        lambda *args, **kwargs: pd.DataFrame([{
            "stock_code": "000001",
            "trade_date": "2024-06-14",
            "open": 10.0,
            "high": 11.0,
            "low": 9.0,
            "close": 10.5,
            "volume": 1000,
            "amount": 10_000,
            "turnover_rate": 1.0,
        }]),
    )

    results = collect_date("2024-06-14", db=db, mode="backfill")

    assert results["daily_price"] == 1
    assert results["fund_flow"] == 0
    assert results["news"] == 0
    assert results["market_emotion"] == 1
