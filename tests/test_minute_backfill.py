"""Contracts for recoverable Sina RAW five-minute candidate backfill."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest
import requests
from click.testing import CliRunner

from src.data.minute_backfill import (
    MinuteBackfillError,
    MinuteBackfillResult,
    MinuteBarConflictError,
    backfill_latest_minutes,
    freeze_latest_minute_universe,
    load_minute_status,
    save_minute_bars,
)
from src.data.sources.sina_minutes import (
    DATALEN,
    MinuteBar,
    SinaMinuteError,
    fetch_raw_5m,
    market_symbol,
)
from src.data.storage import Storage

SHANGHAI = ZoneInfo("Asia/Shanghai")
NOW = datetime(2026, 8, 17, 18, 0, tzinfo=SHANGHAI)


def _storage(tmp_path: Path, name: str = "minutes.db") -> Storage:
    storage = Storage(str(tmp_path / name))
    storage.init_db()
    storage.init_db()
    return storage


def _seed_candidates(
    storage: Storage,
    codes: tuple[str, ...] = ("000001", "600001"),
    candidate_date: str = "2026-08-17",
) -> None:
    storage.execute_write(
        """
        INSERT INTO limit_up_collection_runs
            (trade_date, attempted_at, price_rows, zt_rows, status, detail)
        VALUES (?, ?, 5000, ?, 'ok', '')
        """,
        (candidate_date, f"{candidate_date} 16:10:00", len(codes)),
    )
    for index, code in enumerate(codes, 1):
        for hour, prefix, extra in ((15, "旧名", 0), (16, "最新名", 1)):
            storage.execute_write(
                """
                INSERT INTO zt_pool (
                    stock_code, trade_date, name, consecutive_zt, amount,
                    turnover_rate, snapshot_time
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    code,
                    candidate_date,
                    f"{prefix}{index}",
                    index + extra,
                    1000.0 * (extra + 1) + index,
                    1.0 + index + extra,
                    f"{candidate_date} {hour}:00:00",
                ),
            )


def _bar(
    code: str,
    *,
    day: str = "2026-08-17",
    index: int = 0,
    close: float = 10.0,
) -> MinuteBar:
    start = datetime.fromisoformat(f"{day}T09:35:00+08:00")
    bar_time = (start + timedelta(minutes=5 * index)).isoformat(timespec="seconds")
    return MinuteBar(
        stock_code=code,
        bar_time=bar_time,
        open=10.0,
        high=max(10.2, close),
        low=min(9.8, close),
        close=close,
        volume=1000.0 + index,
        amount=10000.0 + index,
    )


def _raw_bar(**overrides) -> dict:
    row = {
        "day": "2026-08-17 09:35:00",
        "open": "10.00",
        "high": "10.20",
        "low": "9.80",
        "close": "10.10",
        "volume": "1000",
        "amount": "10000.50",
    }
    row.update(overrides)
    return row


def _payload(rows: list[dict] | None = None, status_code: int = 0) -> dict:
    return {"result": {"status": {"code": status_code}, "data": rows or []}}


class _Response:
    def __init__(self, text: str, status_code: int = 200):
        self.text = text
        self.status_code = status_code


class _Session:
    def __init__(self, response: _Response | Exception):
        self.response = response
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        if isinstance(self.response, Exception):
            raise self.response
        return self.response


@pytest.mark.parametrize(
    ("code", "symbol"),
    [
        ("600613", "sh600613"),
        ("688001", "sh688001"),
        ("000001", "sz000001"),
        ("300001", "sz300001"),
        ("920000", "bj920000"),
    ],
)
def test_market_symbol_is_explicit_and_strict(code, symbol):
    assert market_symbol(code) == symbol
    for invalid in ("60061", "X00613", "120001"):
        with pytest.raises(SinaMinuteError):
            market_symbol(invalid)


def test_direct_sina_adapter_accepts_json_and_jsonp_and_maps_raw_fields():
    payload = _payload([_raw_bar()])
    session = _Session(_Response(json.dumps(payload)))
    rows = fetch_raw_5m("600613", session=session, timeout=7)

    assert len(session.calls) == 1
    assert session.calls[0][1]["params"] == {
        "symbol": "sh600613",
        "scale": "5",
        "ma": "no",
        "datalen": str(DATALEN),
    }
    assert rows == [
        MinuteBar(
            stock_code="600613",
            bar_time="2026-08-17T09:35:00+08:00",
            open=10.0,
            high=10.2,
            low=9.8,
            close=10.1,
            volume=1000.0,
            amount=10000.5,
        )
    ]
    jsonp = _Session(_Response(f"callback({json.dumps(payload)});"))
    assert fetch_raw_5m("600613", session=jsonp) == rows


@pytest.mark.parametrize(
    "response",
    [
        _Response(json.dumps(_payload([_raw_bar(amount=None)]))),
        _Response(json.dumps(_payload([_raw_bar(high="NaN")]))),
        _Response(json.dumps(_payload([_raw_bar(day="2026-08-17 09:33:00")]))),
        _Response(json.dumps(_payload([_raw_bar()], status_code=9))),
        _Response(json.dumps(_payload([]))),
        _Response("not-json"),
    ],
)
def test_direct_sina_adapter_rejects_empty_or_malformed_fields(response):
    with pytest.raises(SinaMinuteError):
        fetch_raw_5m("600613", session=_Session(response))


def test_direct_sina_adapter_classifies_timeout_http_retry_and_403_circuit():
    with pytest.raises(SinaMinuteError) as timeout_error:
        fetch_raw_5m("600613", session=_Session(requests.Timeout("synthetic")))
    assert timeout_error.value.retryable
    for status_code in (429, 500):
        with pytest.raises(SinaMinuteError) as retry_error:
            fetch_raw_5m("600613", session=_Session(_Response("", status_code)))
        assert retry_error.value.retryable
        assert retry_error.value.status_code == status_code
    with pytest.raises(SinaMinuteError) as forbidden:
        fetch_raw_5m("600613", session=_Session(_Response("", 403)))
    assert forbidden.value.circuit_breaker and forbidden.value.status_code == 403


def test_freeze_deduplicates_latest_snapshot_and_rejects_changed_set(tmp_path):
    storage = _storage(tmp_path)
    _seed_candidates(storage)
    first = freeze_latest_minute_universe(storage, frozen_at=NOW)
    repeated = freeze_latest_minute_universe(storage, frozen_at=NOW)

    assert repeated == first
    assert first.stock_codes == ("000001", "600001")
    assert len(first.universe_hash) == 64
    items = storage.execute(
        """
        SELECT stock_code, stock_name, features_json, universe_hash, status
        FROM minute_capture_items ORDER BY stock_code
        """
    )
    assert len(items) == 2 and items[0]["stock_name"] == "最新名1"
    assert json.loads(items[0]["features_json"])["consecutive_zt"] == 2
    assert {item["universe_hash"] for item in items} == {first.universe_hash}
    assert all(item["status"] == "PENDING" for item in items)

    storage.execute_write(
        """
        INSERT INTO zt_pool (stock_code, trade_date, name, snapshot_time)
        VALUES ('300001', '2026-08-17', '集合变化', '2026-08-17 16:01:00')
        """
    )
    with pytest.raises(MinuteBackfillError, match="候选集合.*禁止"):
        freeze_latest_minute_universe(storage, frozen_at=NOW)
    assert storage.execute("SELECT COUNT(*) AS n FROM minute_capture_items") == [
        {"n": 2}
    ]


def test_freeze_rejects_candidate_date_later_than_local_frozen_date(tmp_path):
    storage = _storage(tmp_path)
    _seed_candidates(storage, candidate_date="2026-08-17")
    with pytest.raises(MinuteBackfillError, match="候选交易日晚于冻结时间"):
        freeze_latest_minute_universe(
            storage,
            frozen_at=datetime(2026, 8, 16, 23, 59, tzinfo=SHANGHAI),
        )
    assert storage.execute("SELECT COUNT(*) AS n FROM minute_capture_items") == [
        {"n": 0}
    ]


def test_resume_checkpoint_and_resource_limit_are_deterministic(tmp_path):
    storage = _storage(tmp_path)
    _seed_candidates(storage)
    calls: list[str] = []

    def fetch(code: str) -> list[MinuteBar]:
        calls.append(code)
        return [_bar(code)]

    results = [
        backfill_latest_minutes(
            storage,
            max_stocks=1,
            fetcher=fetch,
            sleeper=lambda _: None,
            delay_seconds=lambda: 0,
            now_fn=lambda: NOW,
        )
        for _ in range(2)
    ]
    done = backfill_latest_minutes(
        storage,
        fetcher=fetch,
        sleeper=lambda _: None,
        delay_seconds=lambda: 0,
        now_fn=lambda: NOW,
    )

    assert calls == ["000001", "600001"]
    assert [result.successful_items for result in results] == [1, 1]
    assert results[0].status == "BATCH_INCOMPLETE"
    assert results[1].status == "SUCCESS"
    assert done.selected_items == 0 and done.status == "SUCCESS"
    status = load_minute_status(storage.db_path)
    assert status.total_items == status.successful_items == 2
    assert status.pending_items == status.error_items == 0
    assert status.bars_count == 2
    assert status.complete_days == 0 and status.partial_days == 2
    with pytest.raises(MinuteBackfillError, match="1到120"):
        backfill_latest_minutes(storage, max_stocks=121)


def test_bar_insert_is_idempotent_conflict_safe_and_marks_partial_day(tmp_path):
    storage = _storage(tmp_path)
    _seed_candidates(storage, ("000001",))
    universe = freeze_latest_minute_universe(storage, frozen_at=NOW)
    bars = [_bar("000001", day="2026-08-14")]
    bars.extend(_bar("000001", index=index) for index in range(48))

    for fetched_at in (
        "2026-08-17T18:00:00+08:00",
        "2026-08-17T19:00:00+08:00",
    ):
        save_minute_bars(
            storage.db_path,
            universe.candidate_trade_date,
            "000001",
            bars,
            fetched_at=fetched_at,
        )
    assert storage.execute("SELECT COUNT(*) AS n FROM minute_bars_5m") == [{"n": 49}]
    first = storage.execute(
        """
        SELECT first_fetched_at, payload_hash, close FROM minute_bars_5m
        WHERE stock_code = '000001' ORDER BY bar_time LIMIT 1
        """
    )[0]
    assert first["first_fetched_at"] == "2026-08-17T18:00:00+08:00"
    item = storage.execute(
        """
        SELECT bars_count, complete_days, partial_days
        FROM minute_capture_items WHERE stock_code = '000001'
        """
    )[0]
    assert item == {"bars_count": 49, "complete_days": 1, "partial_days": 1}

    with pytest.raises(MinuteBarConflictError, match="同键异值"):
        save_minute_bars(
            storage.db_path,
            universe.candidate_trade_date,
            "000001",
            [_bar("000001", day="2026-08-14", close=11.0)],
            fetched_at="2026-08-17T20:00:00+08:00",
        )
    after = storage.execute(
        """
        SELECT first_fetched_at, payload_hash, close FROM minute_bars_5m
        WHERE stock_code = '000001' ORDER BY bar_time LIMIT 1
        """
    )[0]
    assert after == first

    storage.execute_write(
        "UPDATE minute_capture_items SET status = 'ERROR' WHERE stock_code = '000001'"
    )
    conflict_result = backfill_latest_minutes(
        storage,
        fetcher=lambda code: [_bar(code, day="2026-08-14", close=11.0)],
        sleeper=lambda _: None,
        delay_seconds=lambda: 0,
        now_fn=lambda: NOW,
    )
    assert conflict_result.status == "PARTIAL" and conflict_result.conflict_items == 1
    assert storage.execute(
        "SELECT status FROM minute_capture_items WHERE stock_code = '000001'"
    ) == [{"status": "CONFLICT"}]
    calls: list[str] = []
    repeated = backfill_latest_minutes(
        storage,
        fetcher=lambda code: calls.append(code) or [_bar(code)],
        sleeper=lambda _: None,
        delay_seconds=lambda: 0,
        now_fn=lambda: NOW,
    )
    assert repeated.status == "PARTIAL" and repeated.selected_items == 0
    assert calls == []
    assert load_minute_status(storage.db_path).conflict_items == 1


def test_future_source_bar_is_zero_write_and_non_success(tmp_path):
    storage = _storage(tmp_path)
    _seed_candidates(storage, ("000001",))

    result = backfill_latest_minutes(
        storage,
        fetcher=lambda code: [_bar(code, day="2026-08-18")],
        sleeper=lambda _: None,
        delay_seconds=lambda: 0,
        now_fn=lambda: NOW,
    )

    assert result.status == "PARTIAL" and result.successful_items == 0
    assert storage.execute("SELECT COUNT(*) AS n FROM minute_bars_5m") == [{"n": 0}]
    item = storage.execute(
        "SELECT status, bars_count, last_error FROM minute_capture_items"
    )[0]
    assert item["status"] == "ERROR" and item["bars_count"] == 0
    assert "未来bar" in item["last_error"]


def test_retry_partial_and_three_source_errors_trip_circuit(tmp_path):
    storage = _storage(tmp_path)
    codes = ("000001", "000002", "000003", "000004", "000005")
    _seed_candidates(storage, codes)
    attempts: dict[str, int] = {}
    sleeps: list[float] = []

    def fetch(code: str) -> list[MinuteBar]:
        attempts[code] = attempts.get(code, 0) + 1
        if code == "000001" and attempts[code] < 3:
            raise SinaMinuteError("retry", retryable=True)
        if code != "000001":
            raise SinaMinuteError("source malformed")
        return [_bar(code)]

    result = backfill_latest_minutes(
        storage,
        fetcher=fetch,
        sleeper=sleeps.append,
        delay_seconds=lambda: 1.5,
        now_fn=lambda: NOW,
    )

    assert result.status == "PARTIAL"
    assert result.successful_items == 1 and result.failed_items == 3
    assert result.circuit_reason == "连续3只股票源错误，已熔断"
    assert attempts == {"000001": 3, "000002": 1, "000003": 1, "000004": 1}
    assert sleeps == [1.5] * 5
    rows = storage.execute(
        "SELECT stock_code, status, attempts FROM minute_capture_items ORDER BY stock_code"
    )
    assert rows[0] == {"stock_code": "000001", "status": "SUCCESS", "attempts": 3}
    assert [row["status"] for row in rows[1:4]] == ["ERROR", "ERROR", "ERROR"]
    assert rows[4]["status"] == "PENDING"


def test_403_trips_immediate_circuit_and_leaves_remaining_pending(tmp_path):
    storage = _storage(tmp_path)
    _seed_candidates(storage, ("000001", "000002"))
    calls: list[str] = []

    def forbidden(code: str) -> list[MinuteBar]:
        calls.append(code)
        raise SinaMinuteError("403 forbidden", circuit_breaker=True, status_code=403)

    result = backfill_latest_minutes(
        storage,
        fetcher=forbidden,
        sleeper=lambda _: None,
        delay_seconds=lambda: 0,
        now_fn=lambda: NOW,
    )
    assert result.status == "PARTIAL" and result.failed_items == 1
    assert result.circuit_reason == "403 forbidden"
    assert calls == ["000001"]
    states = storage.execute(
        "SELECT stock_code, status FROM minute_capture_items ORDER BY stock_code"
    )
    assert [row["status"] for row in states] == ["ERROR", "PENDING"]


def test_retryable_source_error_stops_after_two_retries(tmp_path):
    storage = _storage(tmp_path)
    _seed_candidates(storage, ("000001",))
    calls = 0

    def unavailable(code: str) -> list[MinuteBar]:
        nonlocal calls
        calls += 1
        raise SinaMinuteError("HTTP 500", retryable=True, status_code=500)

    result = backfill_latest_minutes(
        storage,
        fetcher=unavailable,
        sleeper=lambda _: None,
        delay_seconds=lambda: 0,
        now_fn=lambda: NOW,
    )
    assert calls == 3
    assert result.status == "PARTIAL" and result.failed_items == 1
    assert storage.execute("SELECT attempts FROM minute_capture_items") == [
        {"attempts": 3}
    ]


def test_status_is_mode_ro_and_cli_has_no_time_or_candidate_backdoor(tmp_path, monkeypatch):
    from cli import limit_up

    storage = _storage(tmp_path)
    _seed_candidates(storage)
    freeze_latest_minute_universe(storage, frozen_at=NOW)
    path = Path(storage.db_path)
    before = (hashlib.sha256(path.read_bytes()).hexdigest(), path.stat().st_mtime_ns)
    status = load_minute_status(path)
    result = CliRunner().invoke(limit_up.main, ["minute-status", "--db", str(path)])

    assert status.candidate_trade_date == "2026-08-17"
    assert result.exit_code == 0, result.output
    assert "候选日 2026-08-17" in result.output and "待处理 2" in result.output
    assert (hashlib.sha256(path.read_bytes()).hexdigest(), path.stat().st_mtime_ns) == before

    missing = tmp_path / "missing.db"
    assert load_minute_status(missing).candidate_trade_date is None
    assert not missing.exists()
    no_table = tmp_path / "no-table.db"
    sqlite3.connect(no_table).close()
    no_table_before = hashlib.sha256(no_table.read_bytes()).hexdigest()
    assert load_minute_status(no_table).candidate_trade_date is None
    assert hashlib.sha256(no_table.read_bytes()).hexdigest() == no_table_before

    fake_result = MinuteBackfillResult(
        "2026-08-17", "a" * 64, 2, 1, 1, 0, 1, "SUCCESS"
    )
    monkeypatch.setattr(limit_up, "backfill_latest_minutes", lambda *a, **k: fake_result)
    command = CliRunner().invoke(
        limit_up.main,
        ["backfill-minutes", "--max-stocks", "1", "--db", str(tmp_path / "cli.db")],
    )
    assert command.exit_code == 0, command.output
    assert "RETRO_BACKFILL" in command.output and "first_fetched_at" in command.output
    help_result = CliRunner().invoke(limit_up.main, ["backfill-minutes", "--help"])
    assert help_result.exit_code == 0
    for option in ("--observed-at", "--candidate-date", "--frozen-at"):
        assert option not in help_result.output
    over_limit = CliRunner().invoke(
        limit_up.main,
        ["backfill-minutes", "--max-stocks", "121", "--db", str(tmp_path / "x.db")],
    )
    assert over_limit.exit_code != 0


def test_partial_cli_is_nonzero(tmp_path, monkeypatch):
    from cli import limit_up

    partial = MinuteBackfillResult(
        "2026-08-17",
        "b" * 64,
        3,
        3,
        1,
        2,
        0,
        "PARTIAL",
        "连续3只股票源错误，已熔断",
    )
    monkeypatch.setattr(limit_up, "backfill_latest_minutes", lambda *a, **k: partial)
    result = CliRunner().invoke(
        limit_up.main,
        ["backfill-minutes", "--db", str(tmp_path / "partial.db")],
    )
    assert result.exit_code != 0
    assert "批次未完成" in result.output and "熔断" in result.output

    batch = MinuteBackfillResult(
        "2026-08-17", "c" * 64, 150, 120, 120, 0, 0, "BATCH_INCOMPLETE"
    )
    monkeypatch.setattr(limit_up, "backfill_latest_minutes", lambda *a, **k: batch)
    incomplete = CliRunner().invoke(
        limit_up.main,
        ["backfill-minutes", "--db", str(tmp_path / "batch.db")],
    )
    assert incomplete.exit_code != 0
    assert "BATCH_INCOMPLETE" in incomplete.output
