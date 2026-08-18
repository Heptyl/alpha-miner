"""Dynamic watchlist RAW 5m capture contracts."""

from __future__ import annotations

import sqlite3
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from src.data.minute_backfill import save_minute_bars
from src.data.sources.sina_minutes import MinuteBar, SinaMinuteError
from src.data.storage import Storage
from src.data.user_preferences import add_watch, load_watchlist
from src.data.watchlist_minutes import MAX_WATCHLIST_STOCKS, capture_watchlist_minutes

SHANGHAI = ZoneInfo("Asia/Shanghai")
NOW = datetime(2026, 8, 18, 18, tzinfo=SHANGHAI)
def _bar(code: str, close: float = 10.0) -> MinuteBar:
    return MinuteBar(
        code, "2026-08-18T15:00:00+08:00", 10, max(10, close),
        min(10, close), close, 1000, 10000,
    )
def _setup(tmp_path: Path, codes=("000735", "600613")):
    market, preferences = tmp_path / "working.db", tmp_path / "preferences.db"
    Storage(str(market)).init_db()
    for code in codes:
        add_watch(preferences, code, clock=lambda: NOW)
    return market, preferences
def test_capture_requests_only_current_watchlist_and_remove_stops_future_requests(tmp_path):
    market, preferences = _setup(tmp_path)
    calls: list[str] = []
    def fetch(code: str):
        calls.append(code)
        return [_bar(code)]

    first = capture_watchlist_minutes(market, preferences, fetcher=fetch, sleeper=lambda _: None,
                                      delay_seconds=lambda: 0, clock=lambda: NOW)
    from src.data.user_preferences import remove_watch
    remove_watch(preferences, "000735")
    second = capture_watchlist_minutes(market, preferences, fetcher=fetch, sleeper=lambda _: None,
                                       delay_seconds=lambda: 0, clock=lambda: NOW)
    assert first.status == second.status == "SUCCESS"
    assert calls == ["000735", "600613", "600613"]
    assert [item[0] for item in load_watchlist(preferences)] == ["600613"]
    connection = sqlite3.connect(market)
    assert connection.execute("SELECT COUNT(*) FROM minute_bars_5m").fetchone()[0] == 2
    assert not connection.execute(
        "SELECT 1 FROM sqlite_master WHERE name='watchlist_capture_status'"
    ).fetchone()
    connection.close()
def test_system_and_watchlist_share_one_bar_but_keep_separate_status(tmp_path):
    market, preferences = _setup(tmp_path, ("000735",))
    storage = Storage(str(market))
    storage.execute_write(
        """INSERT INTO minute_capture_items
           (candidate_trade_date,stock_code,features_json,universe_hash,frozen_at,status)
           VALUES('2026-08-18','000735','{}',?,'2026-08-18T16:10:00+08:00','PENDING')""",
        ("a" * 64,),
    )
    save_minute_bars(
        str(market), "2026-08-18", "000735", [_bar("000735")],
        fetched_at="2026-08-18T18:00:00+08:00",
    )
    result = capture_watchlist_minutes(market, preferences, fetcher=lambda code: [_bar(code)],
                                       sleeper=lambda _: None, delay_seconds=lambda: 0, clock=lambda: NOW)
    assert result.successful_codes == 1
    assert storage.execute("SELECT COUNT(*) AS n FROM minute_bars_5m") == [{"n": 1}]
    assert storage.execute("SELECT status FROM minute_capture_items") == [{"status": "SUCCESS"}]
    connection = sqlite3.connect(preferences)
    assert connection.execute(
        "SELECT status,bars_count FROM watchlist_capture_status"
    ).fetchone() == ("SUCCESS", 1)
    connection.close()
def test_first_fetched_at_is_clocked_after_source_response(tmp_path):
    market, preferences = _setup(tmp_path, ("000735",))
    events: list[str] = []
    capture_watchlist_minutes(
        market, preferences,
        fetcher=lambda code: events.append("fetch") or [_bar(code)],
        sleeper=lambda _: None, delay_seconds=lambda: 0,
        clock=lambda: events.append("clock") or NOW,
    )
    assert events == ["fetch", "clock"]
    connection = sqlite3.connect(market)
    fetched_at = connection.execute("SELECT first_fetched_at FROM minute_bars_5m").fetchone()[0]
    connection.close()
    assert fetched_at == "2026-08-18T18:00:00+08:00"
def test_conflict_never_overwrites_and_status_is_personal(tmp_path):
    market, preferences = _setup(tmp_path, ("000735",))
    first = capture_watchlist_minutes(market, preferences, fetcher=lambda code: [_bar(code)],
                                      sleeper=lambda _: None, delay_seconds=lambda: 0, clock=lambda: NOW)
    second = capture_watchlist_minutes(market, preferences, fetcher=lambda code: [_bar(code, 11)],
                                       sleeper=lambda _: None, delay_seconds=lambda: 0, clock=lambda: NOW)
    assert first.status == "SUCCESS" and second.status == "PARTIAL"
    connection = sqlite3.connect(market)
    assert connection.execute("SELECT close FROM minute_bars_5m").fetchone()[0] == 10
    connection.close()
    connection = sqlite3.connect(preferences)
    assert connection.execute("SELECT status FROM watchlist_capture_status").fetchone()[0] == "CONFLICT"
    connection.close()
def test_retry_circuit_and_fixed_small_batch_limit(tmp_path):
    codes = tuple(f"000{i:03d}" for i in range(1, 5))
    market, preferences = _setup(tmp_path, codes)
    calls: list[str] = []
    def fail(code: str):
        calls.append(code)
        raise SinaMinuteError("source malformed")

    result = capture_watchlist_minutes(market, preferences, fetcher=fail, sleeper=lambda _: None,
                                       delay_seconds=lambda: 0, clock=lambda: NOW)
    assert result.status == "PARTIAL" and result.failed_codes == 3
    assert result.circuit_reason == "连续3只股票源错误，已熔断"
    assert calls == list(codes[:3])
    with pytest.raises(ValueError, match=str(MAX_WATCHLIST_STOCKS)):
        capture_watchlist_minutes(market, preferences, max_stocks=MAX_WATCHLIST_STOCKS + 1)
def test_403_circuits_immediately_and_future_bar_records_personal_error(tmp_path):
    market, preferences = _setup(tmp_path, ("000735", "600613"))
    calls: list[str] = []

    def forbidden(code: str):
        calls.append(code)
        raise SinaMinuteError("403", circuit_breaker=True, status_code=403)

    blocked = capture_watchlist_minutes(market, preferences, fetcher=forbidden,
                                        sleeper=lambda _: None, delay_seconds=lambda: 0, clock=lambda: NOW)
    assert blocked.circuit_reason == "403" and calls == ["000735"]
    future_market, future_preferences = _setup(tmp_path / "future", ("000735",))
    future = capture_watchlist_minutes(
        future_market, future_preferences,
        fetcher=lambda code: [MinuteBar(
            code, "2026-08-19T09:35:00+08:00", 10, 10, 10, 10, 1, 10,
        )], sleeper=lambda _: None, delay_seconds=lambda: 0, clock=lambda: NOW,
    )
    assert future.status == "PARTIAL"
    connection = sqlite3.connect(future_preferences)
    status, error = connection.execute(
        "SELECT status,last_error FROM watchlist_capture_status"
    ).fetchone()
    connection.close()
    assert status == "ERROR" and "未来bar" in error
