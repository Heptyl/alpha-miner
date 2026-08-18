"""Synthetic contract tests for forward-only pre-limit snapshot capture."""

from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timedelta
from pathlib import Path

import pytest
import requests
from click.testing import CliRunner

from src.data.prelimit_capture import (
    AUCTION_PHASE,
    OPEN_PHASE,
    CaptureResult,
    PrelimitCaptureError,
    capture_prelimit,
    load_prelimit_pairs,
    load_prelimit_status,
)
from src.data.sources import sina_prelimit
from src.data.sources.sina_prelimit import (
    SOURCE_NAME,
    SinaPrelimitError,
    fetch_all_spot,
)
from src.data.storage import Storage


def _storage(tmp_path: Path, name: str = "prelimit.db") -> Storage:
    storage = Storage(str(tmp_path / name))
    storage.init_db()
    return storage


def _seed_candidate_day(storage: Storage, candidate_date: str = "2026-08-17") -> None:
    storage.execute_write(
        """
        INSERT INTO limit_up_collection_runs
            (trade_date, attempted_at, price_rows, zt_rows, status, detail)
        VALUES (?, ?, 5000, 2, 'ok', '')
        """,
        (candidate_date, f"{candidate_date} 16:10:00"),
    )
    for code, name, snapshot in (
        ("000001", "旧名称", f"{candidate_date} 15:00:00"),
        ("000001", "候选甲", f"{candidate_date} 16:00:00"),
        ("600001", "候选乙", f"{candidate_date} 16:00:00"),
    ):
        storage.execute_write(
            """
            INSERT INTO zt_pool
                (stock_code, trade_date, name, consecutive_zt, snapshot_time)
            VALUES (?, ?, ?, 1, ?)
            """,
            (code, candidate_date, name, snapshot),
        )
    action = date.fromisoformat(candidate_date) + timedelta(days=1)
    while action.weekday() >= 5:
        action += timedelta(days=1)
    action_date = action.isoformat()
    candidates = [
        {
            "stock_code": "000001",
            "stock_name": "候选甲",
            "paper_status": "PLANNED",
            "planned_entry_date": action_date,
            "lifecycle_events": [
                {"status": "PLANNED", "recorded_at": "D", "reason": "冻结"}
            ],
        },
        {
            "stock_code": "600001",
            "stock_name": "候选乙",
            "paper_status": "PLANNED",
            "planned_entry_date": action_date,
            "lifecycle_events": [
                {"status": "PLANNED", "recorded_at": "D", "reason": "冻结"}
            ],
        },
    ]
    generated_at = f"{candidate_date}T16:20:00+08:00"
    plan = {
        "play_id": "attention_reacceleration_open_v1",
        "play_name": "测试",
        "behavior_logic": "逻辑",
        "signal_trade_date": candidate_date,
        "generated_at": generated_at,
        "trigger_rule": "触发",
        "abandon_rule": "放弃",
        "exit_rule": "退出",
        "admission_status": "NOT_ADMITTED",
        "candidate_identity": [
            {"stock_code": item["stock_code"]} for item in candidates
        ],
    }
    plan_hash = hashlib.sha256(
        json.dumps(plan, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    storage.execute_write(
        """
        INSERT INTO play_cards(
            play_id,play_name,behavior_logic,signal_trade_date,candidates_json,
            trigger_rule,abandon_rule,exit_rule,historical_evidence_json,
            paper_status,admission_status,generated_at
        ) VALUES('attention_reacceleration_open_v1','测试','逻辑',?,?,
                 '触发','放弃','退出',?,'PLANNED','NOT_ADMITTED',?)
        """,
        (
            candidate_date,
            json.dumps(candidates, ensure_ascii=False),
            json.dumps({"forward_plan": plan, "forward_plan_hash": plan_hash}, ensure_ascii=False),
            generated_at,
        ),
    )


def _quote(
    code: str,
    name: str,
    *,
    source_time: str,
    price: float = 10.0,
    volume: float = 100.0,
    amount: float = 1000.0,
) -> dict:
    return {
        "stock_code": code,
        "stock_name": name,
        "price": price,
        "open": 9.8,
        "high": 10.1,
        "low": 9.7,
        "volume": volume,
        "amount": amount,
        "bid1": 9.99,
        "ask1": 10.01,
        "source_time": source_time,
        "source": SOURCE_NAME,
    }


def _quotes(source_time: str, *, price: float = 10.0, volume: float = 100.0):
    return [
        _quote("000001", "行情甲", source_time=source_time, price=price, volume=volume),
        _quote(
            "600001",
            "行情乙",
            source_time=source_time,
            price=price + 1,
            volume=volume + 10,
            amount=1100.0,
        ),
        _quote("999999", "非候选", source_time=source_time),
    ]


class _Response:
    def __init__(self, payload, error: Exception | None = None):
        self.payload = payload
        self.error = error

    def raise_for_status(self):
        if self.error:
            raise self.error

    def json(self):
        return self.payload


class _Session:
    def __init__(self, *responses: _Response):
        self.responses = list(responses)
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        if not self.responses:
            raise AssertionError("unexpected extra Sina request")
        return self.responses.pop(0)


def _raw_quote(code: str, *, ticktime: str = "09:25:01") -> dict:
    return {
        "code": code,
        "name": f"股票{code}",
        "trade": "10.20",
        "open": "10.00",
        "high": "10.30",
        "low": "9.90",
        "volume": "1,200",
        "amount": "12345.6",
        "buy": "10.19",
        "sell": "10.20",
        "ticktime": ticktime,
    }


def test_sina_adapter_pages_by_declared_count_deduplicates_and_maps_fields(monkeypatch):
    monkeypatch.setattr(sina_prelimit, "PAGE_SIZE", 2)
    session = _Session(
        _Response("3"),
        _Response([_raw_quote("000001"), _raw_quote("600001")]),
        _Response([_raw_quote("600001"), _raw_quote("600002")]),
    )

    rows = fetch_all_spot(session=session)

    assert len(session.calls) == 3
    assert session.calls[1][1]["params"]["page"] == "1"
    assert session.calls[2][1]["params"]["page"] == "2"
    assert all(call[1]["params"]["node"] == "hs_a" for call in session.calls)
    assert [row["stock_code"] for row in rows] == ["000001", "600001", "600002"]
    assert rows[0] == {
        "stock_code": "000001",
        "stock_name": "股票000001",
        "price": 10.2,
        "open": 10.0,
        "high": 10.3,
        "low": 9.9,
        "volume": 1200.0,
        "amount": 12345.6,
        "bid1": 10.19,
        "ask1": 10.2,
        "source_time": "09:25:01",
        "source": SOURCE_NAME,
    }


def test_sina_adapter_rejects_empty_repeated_and_malformed_pages(monkeypatch):
    monkeypatch.setattr(sina_prelimit, "PAGE_SIZE", 2)
    repeated = [_raw_quote("000001"), _raw_quote("000002")]
    sessions = [
        _Session(_Response("3"), _Response([])),
        _Session(_Response("3"), _Response(repeated), _Response(repeated)),
        _Session(_Response("1"), _Response({"not": "a list"})),
        _Session(_Response("1"), _Response(["not-an-object"])),
        _Session(_Response("1"), _Response([{"code": "bad"}])),
        _Session(_Response("not-a-count")),
        _Session(_Response("0")),
        _Session(_Response([], requests.HTTPError("synthetic source failure"))),
    ]
    for session in sessions:
        with pytest.raises(SinaPrelimitError):
            fetch_all_spot(session=session)


def test_sina_adapter_rejects_declared_market_above_page_bound(monkeypatch):
    monkeypatch.setattr(sina_prelimit, "PAGE_SIZE", 2)
    monkeypatch.setattr(sina_prelimit, "MAX_PAGES", 2)
    with pytest.raises(SinaPrelimitError, match="页数异常"):
        fetch_all_spot(session=_Session(_Response("5")))


def test_two_phases_are_idempotent_frozen_and_pairable(tmp_path):
    storage = _storage(tmp_path)
    _seed_candidate_day(storage)
    # D日盘后结果即使意外存在，也没有成功审计且不得进入早盘候选。
    storage.execute_write(
        """
        INSERT INTO zt_pool
            (stock_code, trade_date, name, consecutive_zt, snapshot_time)
        VALUES ('D_FINAL', '2026-08-18', '未来涨停', 1, '2026-08-18 16:00:00')
        """
    )

    auction = capture_prelimit(
        storage,
        AUCTION_PHASE,
        datetime.fromisoformat("2026-08-18T09:25:00+08:00"),
        lambda: _quotes("09:25:01"),
    )
    repeated = capture_prelimit(
        storage,
        AUCTION_PHASE,
        datetime.fromisoformat("2026-08-18T09:26:00+08:00"),
        lambda: _quotes("09:26:01", price=99.0),
    )
    opening = capture_prelimit(
        storage,
        OPEN_PHASE,
        datetime.fromisoformat("2026-08-18T09:31:00+08:00"),
        lambda: _quotes("09:31:01", price=10.5, volume=150.0),
    )

    assert auction.candidate_trade_date == "2026-08-17"
    assert auction.candidate_count == auction.stored_count == 2
    assert repeated.stored_count == 2
    assert opening.stored_count == 2
    rows = storage.execute(
        """
        SELECT trade_date, candidate_trade_date, phase, stock_code, stock_name,
               price, bid1, ask1, source, observed_at
        FROM prelimit_snapshots
        ORDER BY phase, stock_code
        """
    )
    assert len(rows) == 4
    assert {row["stock_code"] for row in rows} == {"000001", "600001"}
    assert all(row["candidate_trade_date"] == "2026-08-17" for row in rows)
    first = next(
        row
        for row in rows
        if row["phase"] == AUCTION_PHASE and row["stock_code"] == "000001"
    )
    assert first["stock_name"] == "候选甲"
    assert first["price"] == 10.0
    assert first["observed_at"] == "2026-08-18T09:25:00+08:00"
    assert first["source"] == SOURCE_NAME

    pairs = load_prelimit_pairs(storage, "2026-08-18")
    assert len(pairs) == 2
    pair = next(row for row in pairs if row["stock_code"] == "000001")
    assert pair["auction_price"] == 10.0
    assert pair["open_price"] == 10.5
    assert pair["cumulative_volume_delta"] == 50.0
    assert pair["cumulative_amount_delta"] == 0.0
    status = load_prelimit_status(storage.db_path)
    assert status.auction_date == status.open_date == status.paired_date == "2026-08-18"
    assert status.auction_rows == status.open_rows == 2
    assert status.missing_phases == ()


def test_latest_failed_audit_blocks_frozen_card_capture(tmp_path):
    storage = _storage(tmp_path)
    _seed_candidate_day(storage)
    storage.execute_write(
        "INSERT INTO limit_up_collection_runs"
        "(trade_date,attempted_at,price_rows,zt_rows,status) "
        "VALUES('2026-08-17','2026-08-17 16:20:00',5000,50,'failed')"
    )
    with pytest.raises(PrelimitCaptureError, match="最新记录.*成功审计"):
        capture_prelimit(
            storage,
            AUCTION_PHASE,
            datetime.fromisoformat("2026-08-18T09:25:00+08:00"),
            lambda: _quotes("09:25:01"),
        )
    assert storage.execute("SELECT COUNT(*) AS n FROM prelimit_snapshots") == [{"n": 0}]


def test_zero_candidate_plan_is_auditable_noop(tmp_path):
    storage = _storage(tmp_path)
    _seed_candidate_day(storage)
    evidence = json.loads(
        storage.execute("SELECT historical_evidence_json FROM play_cards")[0][
            "historical_evidence_json"
        ]
    )
    evidence["forward_plan"]["candidate_identity"] = []
    evidence["forward_plan_hash"] = hashlib.sha256(
        json.dumps(
            evidence["forward_plan"],
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()
    storage.execute_write(
        "UPDATE play_cards SET candidates_json = '[]', historical_evidence_json = ? "
        "WHERE play_id = 'attention_reacceleration_open_v1'",
        (json.dumps(evidence, ensure_ascii=False),),
    )

    def should_not_fetch():
        raise AssertionError("zero-candidate plan must not call the market source")

    result = capture_prelimit(
        storage,
        AUCTION_PHASE,
        datetime.fromisoformat("2026-08-18T09:25:00+08:00"),
        should_not_fetch,
    )
    assert result.candidate_count == result.stored_count == 0
    assert storage.execute("SELECT COUNT(*) AS n FROM prelimit_snapshots") == [{"n": 0}]


def test_prelimit_rejects_tampered_frozen_card(tmp_path):
    storage = _storage(tmp_path)
    _seed_candidate_day(storage)
    storage.execute_write(
        "UPDATE play_cards SET trigger_rule = 'tampered' "
        "WHERE play_id = 'attention_reacceleration_open_v1'"
    )
    with pytest.raises(PrelimitCaptureError, match="不可验证"):
        capture_prelimit(
            storage,
            AUCTION_PHASE,
            datetime.fromisoformat("2026-08-18T09:25:00+08:00"),
            lambda: _quotes("09:25:01"),
        )


def test_open_capture_advances_paper_entry_after_snapshot_commit(tmp_path, monkeypatch):
    from cli import limit_up

    calls = []

    def capture(storage, phase):
        calls.append("capture_committed")
        return CaptureResult("2026-08-18", "2026-08-17", phase, 1, 1)

    def settle(storage):
        calls.append("settle_entry")
        return []

    monkeypatch.setattr(limit_up, "capture_prelimit", capture)
    monkeypatch.setattr(limit_up, "settle_attention_reacceleration_cards", settle)
    result = CliRunner().invoke(
        limit_up.main,
        ["capture-prelimit", "--phase", "open", "--db", str(tmp_path / "capture.db")],
    )
    assert result.exit_code == 0, result.output
    assert calls == ["capture_committed", "settle_entry"]


def test_open_capture_settlement_failure_is_nonzero(tmp_path, monkeypatch):
    from cli import limit_up

    monkeypatch.setattr(
        limit_up,
        "capture_prelimit",
        lambda storage, phase: CaptureResult("2026-08-18", "2026-08-17", phase, 1, 1),
    )
    monkeypatch.setattr(
        limit_up,
        "settle_attention_reacceleration_cards",
        lambda storage: (_ for _ in ()).throw(ValueError("synthetic entry failure")),
    )
    result = CliRunner().invoke(
        limit_up.main,
        ["capture-prelimit", "--phase", "open", "--db", str(tmp_path / "failure.db")],
    )
    assert result.exit_code != 0
    assert "09:31 PAPER入场推进失败" in result.output

def test_candidate_day_is_exact_previous_market_day_without_stale_fallback(tmp_path):
    stale_storage = _storage(tmp_path, "stale-prior.db")
    _seed_candidate_day(stale_storage, "2026-08-17")
    with pytest.raises(PrelimitCaptureError, match="上一交易日2026-08-18.*禁止回退"):
        capture_prelimit(
            stale_storage,
            AUCTION_PHASE,
            datetime.fromisoformat("2026-08-19T09:26:00+08:00"),
            lambda: _quotes("09:26:01"),
        )
    assert stale_storage.execute("SELECT COUNT(*) AS n FROM prelimit_snapshots") == [
        {"n": 0}
    ]

    monday_storage = _storage(tmp_path, "monday.db")
    _seed_candidate_day(monday_storage, "2026-08-21")
    result = capture_prelimit(
        monday_storage,
        AUCTION_PHASE,
        datetime.fromisoformat("2026-08-24T09:26:00+08:00"),
        lambda: _quotes("09:26:01"),
    )
    assert result.candidate_trade_date == "2026-08-21"
    assert result.stored_count == 2


def test_window_weekend_empty_source_and_missing_candidate_fail_without_write(tmp_path):
    storage = _storage(tmp_path)
    _seed_candidate_day(storage)

    cases = [
        (
            AUCTION_PHASE,
            datetime.fromisoformat("2026-08-18T09:30:00+08:00"),
            lambda: _quotes("09:25:01"),
            "仅允许",
        ),
        (
            AUCTION_PHASE,
            datetime.fromisoformat("2026-08-22T09:25:00+08:00"),
            lambda: _quotes("09:25:01"),
            "周末",
        ),
        (
            AUCTION_PHASE,
            datetime.fromisoformat("2026-10-01T09:25:00+08:00"),
            lambda: _quotes("09:25:01"),
            "交易所公告休市日",
        ),
        (
            AUCTION_PHASE,
            datetime.fromisoformat("2027-01-04T09:25:00+08:00"),
            lambda: _quotes("09:25:01"),
            "2027年交易日历尚未审计",
        ),
        (
            AUCTION_PHASE,
            datetime.fromisoformat("2026-08-18T09:25:00+08:00"),
            lambda: [],
            "为空",
        ),
        (
            AUCTION_PHASE,
            datetime.fromisoformat("2026-08-18T09:25:00+08:00"),
            lambda: [_quote("000001", "只有一只", source_time="09:25:01")],
            "缺少Sina快照",
        ),
    ]
    for phase, observed, fetcher, message in cases:
        with pytest.raises(PrelimitCaptureError, match=message):
            capture_prelimit(storage, phase, observed, fetcher)
    assert storage.execute("SELECT COUNT(*) AS n FROM prelimit_snapshots") == [{"n": 0}]

    empty_storage = _storage(tmp_path, "no-candidates.db")
    with pytest.raises(PrelimitCaptureError, match="成功审计"):
        capture_prelimit(
            empty_storage,
            AUCTION_PHASE,
            datetime.fromisoformat("2026-08-18T09:25:00+08:00"),
            lambda: _quotes("09:25:01"),
        )


def test_stale_source_clock_is_rejected(tmp_path):
    storage = _storage(tmp_path)
    _seed_candidate_day(storage)

    with pytest.raises(PrelimitCaptureError, match="休市或陈旧"):
        capture_prelimit(
            storage,
            OPEN_PHASE,
            datetime.fromisoformat("2026-08-18T09:31:00+08:00"),
            lambda: _quotes("15:00:00"),
        )

    assert storage.execute("SELECT COUNT(*) AS n FROM prelimit_snapshots") == [{"n": 0}]


def test_source_clock_checks_frozen_candidates_not_unrelated_market_rows(tmp_path):
    storage = _storage(tmp_path)
    _seed_candidate_day(storage)
    rows = _quotes("09:26:01")
    rows.append(_quote("888888", "无关陈旧股", source_time="15:30:00"))

    result = capture_prelimit(
        storage,
        AUCTION_PHASE,
        datetime.fromisoformat("2026-08-18T09:26:00+08:00"),
        lambda: rows,
    )
    assert result.stored_count == 2

    blocked = _storage(tmp_path, "candidate-stale.db")
    _seed_candidate_day(blocked)
    candidate_stale = _quotes("09:26:01")
    candidate_stale[0] = _quote("000001", "候选陈旧", source_time="15:30:00")
    with pytest.raises(PrelimitCaptureError, match="000001.*陈旧快照"):
        capture_prelimit(
            blocked,
            AUCTION_PHASE,
            datetime.fromisoformat("2026-08-18T09:26:00+08:00"),
            lambda: candidate_stale,
        )
    assert blocked.execute("SELECT COUNT(*) AS n FROM prelimit_snapshots") == [
        {"n": 0}
    ]


def test_status_and_cli_are_read_only_and_explain_missing_phase(tmp_path):
    from cli import limit_up

    storage = _storage(tmp_path)
    _seed_candidate_day(storage)
    capture_prelimit(
        storage,
        AUCTION_PHASE,
        datetime.fromisoformat("2026-08-18T09:25:00+08:00"),
        lambda: _quotes("09:25:01"),
    )
    path = Path(storage.db_path)
    before = (hashlib.sha256(path.read_bytes()).hexdigest(), path.stat().st_mtime_ns)

    result = CliRunner().invoke(
        limit_up.main,
        ["prelimit-status", "--db", str(path)],
    )

    assert result.exit_code == 0, result.output
    assert "AUCTION_0925：2026-08-18 | 2 行" in result.output
    assert "OPEN_0931：缺失 | 0 行" in result.output
    assert "最新数据日缺段：OPEN_0931" in result.output
    assert (hashlib.sha256(path.read_bytes()).hexdigest(), path.stat().st_mtime_ns) == before


def test_capture_cli_uses_system_time_only_and_surfaces_failure(tmp_path, monkeypatch):
    from cli import limit_up

    calls = []

    def succeed(storage, phase):
        calls.append(phase)
        return CaptureResult(
            trade_date="2026-08-18",
            candidate_trade_date="2026-08-17",
            phase=phase,
            candidate_count=2,
            stored_count=2,
        )

    monkeypatch.setattr(limit_up, "capture_prelimit", succeed)
    result = CliRunner().invoke(
        limit_up.main,
        [
            "capture-prelimit",
            "--phase",
            "auction",
            "--db",
            str(tmp_path / "cli.db"),
        ],
    )
    assert result.exit_code == 0, result.output
    assert calls == [AUCTION_PHASE]
    assert "D-1候选日 2026-08-17" in result.output
    assert "2 \n行" in result.output or "2 行" in result.output
    help_result = CliRunner().invoke(limit_up.main, ["capture-prelimit", "--help"])
    assert help_result.exit_code == 0
    assert "--observed-at" not in help_result.output

    def fail(*args, **kwargs):
        raise PrelimitCaptureError("synthetic empty source")

    monkeypatch.setattr(limit_up, "capture_prelimit", fail)
    failed = CliRunner().invoke(
        limit_up.main,
        [
            "capture-prelimit",
            "--phase",
            "open",
            "--db",
            str(tmp_path / "cli.db"),
        ],
    )
    assert failed.exit_code != 0
    assert "涨停前快照采集失败：synthetic empty source" in failed.output
