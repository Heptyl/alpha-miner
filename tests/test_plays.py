"""Synthetic-data tests for the three-to-four PAPER vertical slice."""

import math
import sqlite3
from dataclasses import replace
from pathlib import Path

import pytest

from src.data.storage import Storage
from src.mining.playbook import load_latest_play_cards, save_play_card
from src.mining.plays import (
    build_theme_new_entrant_diffusion_card,
    build_three_to_four_card,
    load_usable_audit_dates,
    settle_theme_new_entrant_diffusion_cards,
    settle_three_to_four_cards,
)


def _storage(tmp_path: Path) -> Storage:
    storage = Storage(str(tmp_path / "plays.db"))
    storage.init_db()
    return storage


def _price(
    storage: Storage,
    code: str,
    trade_date: str,
    *,
    open_price: float = 9.5,
    high: float = 10.0,
    low: float = 9.0,
    close: float = 10.0,
    volume: float = 100.0,
    snapshot: str = "2026-01-01 16:00:00",
) -> None:
    storage.execute_write(
        """
        INSERT INTO daily_price
            (stock_code, trade_date, open, high, low, close, volume, snapshot_time)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (code, trade_date, open_price, high, low, close, volume, snapshot),
    )


def _zt(
    storage: Storage,
    code: str,
    trade_date: str,
    board: int,
    *,
    open_count: int | None = 0,
    name: str = "测试股",
    snapshot: str = "2026-01-01 16:00:00",
) -> None:
    storage.execute_write(
        """
        INSERT INTO zt_pool
            (stock_code, trade_date, name, consecutive_zt, open_count, snapshot_time)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (code, trade_date, name, board, open_count, snapshot),
    )


def _calendar(storage: Storage, dates: list[str]) -> None:
    for index, trade_date in enumerate(dates):
        _price(storage, "CAL", trade_date, snapshot=f"2026-01-01 15:{index:02d}:00")


def _collection_run(
    storage: Storage,
    trade_date: str,
    status: str,
    attempted_at: str,
) -> None:
    storage.execute_write(
        """
        INSERT INTO limit_up_collection_runs
            (trade_date, attempted_at, price_rows, zt_rows, status, detail)
        VALUES (?, ?, 5000, 50, ?, '')
        """,
        (trade_date, attempted_at, status),
    )


def _pool(
    storage: Storage,
    table: str,
    code: str,
    trade_date: str,
    *,
    name: str,
    industry: str,
    amount: float,
    snapshot: str,
) -> None:
    assert table in {"zt_pool", "strong_pool"}
    storage.execute_write(
        f"""
        INSERT INTO {table}
            (stock_code, trade_date, name, industry, amount, snapshot_time)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (code, trade_date, name, industry, amount, snapshot),
    )


def _h1_signal_data(storage: Storage, previous_date: str, signal_date: str) -> None:
    _calendar(storage, [previous_date, signal_date])
    _collection_run(storage, previous_date, "ok", f"{previous_date} 16:10:00")
    _collection_run(storage, signal_date, "ok", f"{signal_date} 16:10:00")
    for index, code in enumerate(("600101", "600102"), 1):
        _pool(
            storage,
            "zt_pool",
            code,
            previous_date,
            name=f"前日涨停{index}",
            industry="机器人",
            amount=100 + index,
            snapshot=f"{previous_date} 16:0{index}:00",
        )
    for index, code in enumerate(("600101", "600102", "600103"), 1):
        _pool(
            storage,
            "zt_pool",
            code,
            signal_date,
            name=f"当日涨停{index}",
            industry="机器人",
            amount=200 + index,
            snapshot=f"{signal_date} 16:0{index}:00",
        )


def _legacy_day(
    storage: Storage,
    trade_date: str,
    snapshot: str,
    *,
    market_rows: int = 4_000,
    include_strong: bool = True,
) -> None:
    connection = sqlite3.connect(storage.db_path)
    try:
        connection.executemany(
            """
            INSERT INTO daily_price
                (stock_code, trade_date, open, high, low, close, volume, snapshot_time)
            VALUES (?, ?, 10, 10, 10, 10, 100, ?)
            """,
            ((f"{index:06d}", trade_date, snapshot) for index in range(market_rows)),
        )
        connection.executemany(
            """
            INSERT INTO zt_pool
                (stock_code, trade_date, name, industry, amount, snapshot_time)
            VALUES (?, ?, ?, '机器人', 100, ?)
            """,
            (
                ("600101", trade_date, "前日涨停1", snapshot),
                ("600102", trade_date, "前日涨停2", snapshot),
            ),
        )
        if include_strong:
            connection.execute(
                """
                INSERT INTO strong_pool
                    (stock_code, trade_date, name, industry, amount, snapshot_time)
                VALUES ('600299', ?, '前日强势', '机器人', 100, ?)
                """,
                (trade_date, snapshot),
            )
        connection.commit()
    finally:
        connection.close()


def _current_h1_day(storage: Storage, trade_date: str) -> None:
    _collection_run(storage, trade_date, "ok", f"{trade_date} 16:10:00")
    for index, code in enumerate(("600101", "600102", "600103"), 1):
        _pool(
            storage,
            "zt_pool",
            code,
            trade_date,
            name=f"当日涨停{index}",
            industry="机器人",
            amount=200 + index,
            snapshot=f"{trade_date} 16:0{index}:00",
        )
    _pool(
        storage,
        "strong_pool",
        "600201",
        trade_date,
        name="新强势候选",
        industry="机器人",
        amount=300,
        snapshot=f"{trade_date} 16:05:00",
    )
    _price(storage, "600201", trade_date, close=10.0, snapshot=f"{trade_date} 16:06:00")


def test_current_candidates_use_only_signal_day_latest_snapshots(tmp_path):
    storage = _storage(tmp_path)
    _zt(storage, "000002", "2026-01-08", 3, name="乙")
    _zt(storage, "000001", "2026-01-08", 2, name="旧名", snapshot="2026-01-08 15:00:00")
    _zt(storage, "000001", "2026-01-08", 3, name="甲", snapshot="2026-01-08 16:00:00")
    _zt(storage, "000003", "2026-01-08", 4, name="四板")
    _zt(storage, "999999", "2026-01-09", 3, name="未来数据")

    card = build_three_to_four_card(
        storage,
        signal_date="2026-01-08",
        generated_at="2026-01-08T16:10:00+08:00",
    )

    assert card.candidates == [
        {
            "stock_code": "000001",
            "stock_name": "甲",
            "board_count": 3,
            "paper_status": "PLANNED",
        },
        {
            "stock_code": "000002",
            "stock_name": "乙",
            "board_count": 3,
            "paper_status": "PLANNED",
        },
    ]
    assert all(candidate["stock_code"] != "999999" for candidate in card.candidates)


def test_default_date_uses_latest_successfully_audited_day_not_newer_intraday_rows(tmp_path):
    storage = _storage(tmp_path)
    _zt(storage, "COMPLETE", "2026-01-08", 3, name="完整日")
    _collection_run(storage, "2026-01-08", "missing", "2026-01-08 15:50:00")
    _collection_run(storage, "2026-01-08", "ok", "2026-01-08 16:10:00")
    _zt(storage, "INTRADAY", "2026-01-09", 3, name="盘中未闭环")
    _collection_run(storage, "2026-01-09", "unconfirmed", "2026-01-09 11:57:00")

    card = build_three_to_four_card(storage, generated_at="2026-01-09T12:00:00+08:00")

    assert card.signal_trade_date == "2026-01-08"
    assert [candidate["stock_code"] for candidate in card.candidates] == ["COMPLETE"]


def test_default_date_fails_without_successful_collection_audit(tmp_path):
    storage = _storage(tmp_path)
    _zt(storage, "INTRADAY", "2026-01-09", 3)
    _collection_run(storage, "2026-01-09", "unconfirmed", "2026-01-09 11:57:00")

    with pytest.raises(ValueError, match="no successfully audited"):
        build_three_to_four_card(storage)


def test_default_date_rejects_intraday_ok_and_uses_older_post_close_day(tmp_path):
    storage = _storage(tmp_path)
    _zt(storage, "CLOSED", "2026-01-08", 3)
    _collection_run(storage, "2026-01-08", "ok", "2026-01-08 16:10:00")
    _zt(storage, "INTRADAY", "2026-01-09", 3)
    _collection_run(storage, "2026-01-09", "ok", "2026-01-09 11:00:00")

    card = build_three_to_four_card(storage)

    assert card.signal_trade_date == "2026-01-08"
    assert card.candidates[0]["stock_code"] == "CLOSED"


def test_usable_audit_dates_use_latest_attempt_and_post_close_time(tmp_path):
    storage = _storage(tmp_path)
    _collection_run(storage, "2026-01-05", "ok", "2026-01-05 11:00:00")
    assert "2026-01-05" not in load_usable_audit_dates(storage)

    _collection_run(storage, "2026-01-05", "ok", "2026-01-05 16:00:00")
    assert "2026-01-05" in load_usable_audit_dates(storage)

    _collection_run(storage, "2026-01-05", "missing", "2026-01-05 16:10:00")
    assert "2026-01-05" not in load_usable_audit_dates(storage)

    _collection_run(storage, "2026-01-06", "ok", "2026-01-07 09:00:00")
    _collection_run(storage, "2026-01-08", "ok", "2026-01-07 16:00:00")
    usable = load_usable_audit_dates(storage)
    assert "2026-01-06" in usable
    assert "2026-01-08" not in usable

    _collection_run(storage, "2026-01-09", "ok", "2026-01-09 16:00:00")
    _collection_run(storage, "2026-01-09", "missing", "2026-01-09 16:00:00")
    assert "2026-01-09" not in load_usable_audit_dates(storage)


def test_build_validates_generated_at_before_return(tmp_path):
    storage = _storage(tmp_path)
    _zt(storage, "CURRENT", "2026-01-08", 3)

    with pytest.raises(ValueError, match="generated_at"):
        build_three_to_four_card(
            storage,
            signal_date="2026-01-08",
            generated_at="not-a-datetime",
        )


def test_reseal_entry_uses_d_close_and_d_plus_one_open_with_cost(tmp_path):
    storage = _storage(tmp_path)
    _calendar(storage, ["2026-01-05", "2026-01-06", "2026-01-07"])
    _zt(storage, "VALID", "2026-01-05", 3)
    _zt(storage, "VALID", "2026-01-06", 4, open_count=1)
    _price(storage, "VALID", "2026-01-05")
    _price(storage, "VALID", "2026-01-06", open_price=7.0, close=10.0, high=10.0, low=9.0)
    _price(storage, "VALID", "2026-01-07", open_price=11.0, close=30.0, high=31.0, low=10.0)

    card = build_three_to_four_card(
        storage,
        signal_date="2026-01-08",
        generated_at="2026-01-08T16:10:00+08:00",
        total_cost_bps=20,
    )
    evidence = card.historical_evidence

    assert evidence["signal_days"] == 1
    assert evidence["candidate_count"] == 1
    assert evidence["proxy_trigger_count"] == 1
    assert evidence["completed_count"] == 1
    assert evidence["unfinished_count"] == 0
    assert evidence["avg_net_return_pct"] == pytest.approx(9.8)
    assert evidence["win_rate"] == 1.0
    assert "D日涨停收盘价" in evidence["entry_proxy"]
    assert "不是D+1开盘" in evidence["entry_proxy"]


def test_untradable_unresealed_unfinished_and_duplicates_are_counted(tmp_path):
    storage = _storage(tmp_path)
    dates = [
        "2026-01-02",
        "2026-01-05",
        "2026-01-06",
        "2026-01-07",
        "2026-01-08",
        "2026-01-09",
        "2026-01-12",
    ]
    _calendar(storage, dates)

    # Latest snapshots turn DUP into one valid historical candidate and trigger.
    _zt(storage, "DUP", "2026-01-02", 2, snapshot="2026-01-02 15:00:00")
    _zt(storage, "DUP", "2026-01-02", 3, snapshot="2026-01-02 16:00:00")
    _zt(storage, "DUP", "2026-01-05", 4, open_count=0, snapshot="2026-01-05 15:00:00")
    _zt(storage, "DUP", "2026-01-05", 4, open_count=1, snapshot="2026-01-05 16:00:00")
    _price(storage, "DUP", "2026-01-05", high=10, low=10, snapshot="2026-01-05 15:00:00")
    _price(storage, "DUP", "2026-01-05", high=10, low=9, close=10, snapshot="2026-01-05 16:00:00")
    _price(storage, "DUP", "2026-01-06", open_price=9.0)

    _zt(storage, "ONE", "2026-01-06", 3)
    _zt(storage, "ONE", "2026-01-07", 4, open_count=1)
    _price(storage, "ONE", "2026-01-07", high=10, low=10, close=10)

    _zt(storage, "NO_RESEAL", "2026-01-07", 3)
    _zt(storage, "NO_RESEAL", "2026-01-08", 4, open_count=0)
    _price(storage, "NO_RESEAL", "2026-01-08")

    _zt(storage, "NO_EXIT", "2026-01-09", 3)
    _zt(storage, "NO_EXIT", "2026-01-12", 4, open_count=1)
    _price(storage, "NO_EXIT", "2026-01-12", high=10, low=9, close=10)

    card = build_three_to_four_card(storage, signal_date="2026-01-13")
    evidence = card.historical_evidence

    assert evidence["signal_days"] == 4
    assert evidence["candidate_count"] == 4
    assert evidence["proxy_trigger_count"] == 2
    assert evidence["completed_count"] == 1
    assert evidence["unfinished_count"] == 1
    assert evidence["untradable_count"] == 1
    assert evidence["trigger_rate"] == 0.5


def test_empty_candidates_and_zero_history_return_complete_finite_paper_card(tmp_path):
    storage = _storage(tmp_path)
    _zt(storage, "FIRST", "2026-01-08", 1)

    card = build_three_to_four_card(storage, signal_date="2026-01-08")
    evidence = card.historical_evidence

    assert card.candidates == []
    assert card.paper_status == "PLANNED"
    assert card.admission_status == "NOT_ADMITTED"
    assert evidence["metrics_available"] is False
    for key in (
        "trigger_rate",
        "win_rate",
        "avg_net_return_pct",
        "profit_loss_ratio",
        "max_drawdown_pct",
    ):
        assert math.isfinite(evidence[key])
        assert evidence[key] == 0.0


def test_card_can_be_saved_and_loaded_without_implicit_write(tmp_path):
    storage = _storage(tmp_path)
    _zt(storage, "CURRENT", "2026-01-08", 3)

    card = build_three_to_four_card(storage, signal_date="2026-01-08")
    assert storage.execute("SELECT COUNT(*) AS n FROM play_cards") == [{"n": 0}]

    save_play_card(storage, card)

    assert load_latest_play_cards(storage) == [card]


def test_candidate_lifecycle_triggers_then_completes_with_cost(tmp_path):
    storage = _storage(tmp_path)
    dates = ["2026-01-05", "2026-01-06", "2026-01-07"]
    _calendar(storage, dates)
    _zt(storage, "FLOW", "2026-01-05", 3)
    card = build_three_to_four_card(storage, signal_date="2026-01-05")
    save_play_card(storage, card)

    _zt(storage, "FLOW", "2026-01-06", 4, open_count=1)
    _price(storage, "FLOW", "2026-01-06", close=10.0, high=10.0, low=9.0)
    _collection_run(storage, "2026-01-06", "ok", "2026-01-06 16:10:00")

    triggered = settle_three_to_four_cards(storage)
    assert len(triggered) == 1
    candidate = triggered[0].candidates[0]
    assert triggered[0].paper_status == "TRIGGERED"
    assert candidate["paper_status"] == "TRIGGERED"
    assert candidate["entry_trade_date"] == "2026-01-06"
    assert candidate["entry_price"] == 10.0
    assert "exit_price" not in candidate
    save_play_card(storage, triggered[0])

    # A repeated collection of the same audited day does not rewrite the fill.
    before = storage.execute("SELECT candidates_json FROM play_cards")[0]
    assert settle_three_to_four_cards(storage) == []
    assert storage.execute("SELECT candidates_json FROM play_cards")[0] == before

    _price(storage, "FLOW", "2026-01-07", open_price=11.0)
    _collection_run(storage, "2026-01-07", "ok", "2026-01-07 16:10:00")
    completed = settle_three_to_four_cards(storage)

    assert len(completed) == 1
    candidate = completed[0].candidates[0]
    assert completed[0].paper_status == "COMPLETED"
    assert candidate["paper_status"] == "COMPLETED"
    assert candidate["exit_trade_date"] == "2026-01-07"
    assert candidate["exit_price"] == 11.0
    assert candidate["net_return_pct"] == pytest.approx(9.8)
    save_play_card(storage, completed[0])
    final_json = storage.execute("SELECT candidates_json FROM play_cards")[0]
    assert settle_three_to_four_cards(storage) == []
    assert storage.execute("SELECT candidates_json FROM play_cards")[0] == final_json


def test_candidate_lifecycle_records_non_trigger_and_unfilled(tmp_path):
    storage = _storage(tmp_path)
    _calendar(storage, ["2026-01-05", "2026-01-06"])
    for code in ("NO_FOUR", "NO_RESEAL", "ONE_PRICE"):
        _zt(storage, code, "2026-01-05", 3)
    card = build_three_to_four_card(storage, signal_date="2026-01-05")
    save_play_card(storage, card)

    _zt(storage, "NO_RESEAL", "2026-01-06", 4, open_count=0)
    _zt(storage, "ONE_PRICE", "2026-01-06", 4, open_count=1)
    _price(storage, "ONE_PRICE", "2026-01-06", high=10.0, low=10.0, close=10.0)
    _collection_run(storage, "2026-01-06", "ok", "2026-01-06 16:10:00")

    settled = settle_three_to_four_cards(storage)[0]
    outcomes = {
        candidate["stock_code"]: candidate for candidate in settled.candidates
    }
    assert settled.paper_status == "COMPLETED"
    assert outcomes["NO_FOUR"]["paper_status"] == "NOT_TRIGGERED"
    assert "未成为四板" in outcomes["NO_FOUR"]["result_reason"]
    assert outcomes["NO_RESEAL"]["paper_status"] == "NOT_TRIGGERED"
    assert "未开板回封" in outcomes["NO_RESEAL"]["result_reason"]
    assert outcomes["ONE_PRICE"]["paper_status"] == "UNFILLED"
    assert "一字板" in outcomes["ONE_PRICE"]["result_reason"]


def test_unaudited_intraday_rows_do_not_settle_planned_card(tmp_path):
    storage = _storage(tmp_path)
    _calendar(storage, ["2026-01-05", "2026-01-06"])
    _zt(storage, "INTRADAY", "2026-01-05", 3)
    card = build_three_to_four_card(storage, signal_date="2026-01-05")
    save_play_card(storage, card)
    _zt(storage, "INTRADAY", "2026-01-06", 4, open_count=1)
    _price(storage, "INTRADAY", "2026-01-06", high=10.0, low=9.0, close=10.0)
    _collection_run(
        storage,
        "2026-01-06",
        "unconfirmed",
        "2026-01-06 11:57:00",
    )

    assert settle_three_to_four_cards(storage) == []
    loaded = load_latest_play_cards(storage)[0]
    assert loaded.paper_status == "PLANNED"
    assert loaded.candidates[0]["paper_status"] == "PLANNED"


def test_latest_failed_audit_does_not_settle_after_earlier_ok(tmp_path):
    storage = _storage(tmp_path)
    _calendar(storage, ["2026-01-05", "2026-01-06"])
    _zt(storage, "LATE_FAIL", "2026-01-05", 3)
    card = build_three_to_four_card(storage, signal_date="2026-01-05")
    save_play_card(storage, card)
    _zt(storage, "LATE_FAIL", "2026-01-06", 4, open_count=1)
    _price(storage, "LATE_FAIL", "2026-01-06", high=10.0, low=9.0, close=10.0)
    _collection_run(storage, "2026-01-06", "ok", "2026-01-06 16:00:00")
    _collection_run(storage, "2026-01-06", "missing", "2026-01-06 16:10:00")

    assert settle_three_to_four_cards(storage) == []
    loaded = load_latest_play_cards(storage)[0]
    assert loaded.candidates[0]["paper_status"] == "PLANNED"


def test_legacy_candidate_without_status_is_treated_as_planned(tmp_path):
    storage = _storage(tmp_path)
    _calendar(storage, ["2026-01-05", "2026-01-06"])
    _zt(storage, "LEGACY", "2026-01-05", 3)
    built = build_three_to_four_card(storage, signal_date="2026-01-05")
    legacy_candidate = dict(built.candidates[0])
    legacy_candidate.pop("paper_status")
    save_play_card(storage, replace(built, candidates=[legacy_candidate]))
    _zt(storage, "LEGACY", "2026-01-06", 4, open_count=1)
    _price(storage, "LEGACY", "2026-01-06", high=10.0, low=9.0, close=10.0)
    _collection_run(storage, "2026-01-06", "ok", "2026-01-06 16:10:00")

    settled = settle_three_to_four_cards(storage)[0]

    assert settled.paper_status == "TRIGGERED"
    assert settled.candidates[0]["paper_status"] == "TRIGGERED"
    assert settled.candidates[0]["entry_price"] == 10.0


def test_h1_freezes_latest_snapshots_and_preregistered_rank_one(tmp_path):
    storage = _storage(tmp_path)
    previous_date = "2026-01-05"
    signal_date = "2026-01-06"
    _h1_signal_data(storage, previous_date, signal_date)
    _pool(
        storage,
        "zt_pool",
        "600103",
        signal_date,
        name="旧快照",
        industry="错误行业",
        amount=1,
        snapshot=f"{signal_date} 15:00:00",
    )
    _pool(
        storage,
        "strong_pool",
        "600201",
        signal_date,
        name="排名一",
        industry="机器人",
        amount=100,
        snapshot=f"{signal_date} 15:00:00",
    )
    _pool(
        storage,
        "strong_pool",
        "600201",
        signal_date,
        name="排名一新快照",
        industry="机器人",
        amount=300,
        snapshot=f"{signal_date} 16:00:00",
    )
    _pool(
        storage,
        "strong_pool",
        "600202",
        signal_date,
        name="排名二",
        industry="机器人",
        amount=200,
        snapshot=f"{signal_date} 16:00:00",
    )
    _pool(
        storage,
        "strong_pool",
        "600299",
        previous_date,
        name="前日强势",
        industry="机器人",
        amount=500,
        snapshot=f"{previous_date} 16:00:00",
    )
    _pool(
        storage,
        "strong_pool",
        "600299",
        signal_date,
        name="前日强势",
        industry="机器人",
        amount=600,
        snapshot=f"{signal_date} 16:00:00",
    )
    _pool(
        storage,
        "strong_pool",
        "600203",
        signal_date,
        name="ST风险",
        industry="机器人",
        amount=700,
        snapshot=f"{signal_date} 16:00:00",
    )
    _pool(
        storage,
        "strong_pool",
        "830001",
        signal_date,
        name="北交样本",
        industry="机器人",
        amount=800,
        snapshot=f"{signal_date} 16:00:00",
    )
    _price(storage, "600201", signal_date, close=10.0, snapshot=f"{signal_date} 16:01:00")
    _price(storage, "600202", signal_date, close=9.0, snapshot=f"{signal_date} 16:01:00")

    card = build_theme_new_entrant_diffusion_card(
        storage,
        signal_date=signal_date,
        generated_at=f"{signal_date}T16:20:00+08:00",
    )

    assert card.play_id == "theme_new_entrant_diffusion_v1"
    assert card.paper_status == "PLANNED"
    assert card.admission_status == "NOT_ADMITTED"
    assert len(card.candidates) == 1
    candidate = card.candidates[0]
    assert candidate["stock_code"] == "600201"
    assert candidate["stock_name"] == "排名一新快照"
    assert candidate["previous_zt_breadth"] == 2
    assert candidate["current_zt_breadth"] == 3
    assert candidate["signal_amount"] == 300
    assert candidate["signal_close"] == 10.0
    assert candidate["allowed_open_low"] == 9.8
    assert candidate["allowed_open_high"] == 10.5
    assert card.historical_evidence["research_status"] == "DEVELOPMENT_CANDIDATE"
    assert card.historical_evidence["usage_status"] == "PAPER_ONLY"
    assert card.historical_evidence["previous_day_audit_source"] == "EXPLICIT_AUDIT"
    assert card.historical_evidence["independent_signal_days"] == 12
    assert card.historical_evidence["holm_significant"] is False
    save_play_card(storage, card)
    assert load_latest_play_cards(storage) == [card]


def test_h1_does_not_use_stale_previous_day_or_bad_signal_audit(tmp_path):
    storage = _storage(tmp_path)
    _calendar(storage, ["2026-01-05", "2026-01-06", "2026-01-07"])
    _collection_run(storage, "2026-01-05", "ok", "2026-01-05 16:10:00")
    _collection_run(storage, "2026-01-07", "ok", "2026-01-07 16:10:00")
    for code in ("600101", "600102", "600103"):
        _pool(
            storage,
            "zt_pool",
            code,
            "2026-01-07",
            name=code,
            industry="机器人",
            amount=100,
            snapshot="2026-01-07 16:00:00",
        )
    _pool(
        storage,
        "strong_pool",
        "600201",
        "2026-01-07",
        name="候选",
        industry="机器人",
        amount=200,
        snapshot="2026-01-07 16:00:00",
    )

    card = build_theme_new_entrant_diffusion_card(storage, signal_date="2026-01-07")
    assert card.candidates == []
    assert "精确前一交易日2026-01-06缺少可信盘后证据" in card.historical_evidence[
        "empty_reason"
    ]

    with pytest.raises(ValueError, match="no successful collection audit"):
        build_theme_new_entrant_diffusion_card(storage, signal_date="2026-01-06")


def test_h1_accepts_strict_legacy_post_close_previous_day(tmp_path):
    storage = _storage(tmp_path)
    _legacy_day(storage, "2026-08-14", "2026-08-14 16:58:00")
    _current_h1_day(storage, "2026-08-17")

    card = build_theme_new_entrant_diffusion_card(
        storage,
        signal_date="2026-08-17",
        generated_at="2026-08-17T16:20:00+08:00",
    )

    assert [candidate["stock_code"] for candidate in card.candidates] == ["600201"]
    assert (
        card.historical_evidence["previous_day_audit_source"]
        == "LEGACY_POST_CLOSE_SNAPSHOT"
    )
    assert "旧版三表盘后快照" in card.historical_evidence["data_limitations"]


@pytest.mark.parametrize(
    ("snapshot", "market_rows", "include_strong"),
    [
        ("2026-08-14 14:00:00", 4_000, True),
        ("2026-08-15 09:00:00", 4_000, True),
        ("2026-08-14 16:58:00", 3_999, True),
        ("2026-08-14 16:58:00", 4_000, False),
    ],
)
def test_h1_rejects_weak_legacy_previous_day_proof(
    tmp_path,
    snapshot,
    market_rows,
    include_strong,
):
    storage = _storage(tmp_path)
    _legacy_day(
        storage,
        "2026-08-14",
        snapshot,
        market_rows=market_rows,
        include_strong=include_strong,
    )
    _current_h1_day(storage, "2026-08-17")

    card = build_theme_new_entrant_diffusion_card(storage, signal_date="2026-08-17")

    assert card.candidates == []
    assert card.historical_evidence["previous_day_audit_source"] == "UNAVAILABLE"
    assert "缺少可信盘后证据" in card.historical_evidence["empty_reason"]


def test_h1_never_falls_back_when_previous_day_has_failed_audit(tmp_path):
    storage = _storage(tmp_path)
    _legacy_day(storage, "2026-08-14", "2026-08-14 16:58:00")
    _collection_run(storage, "2026-08-14", "missing", "2026-08-14 19:10:00")
    _current_h1_day(storage, "2026-08-17")

    card = build_theme_new_entrant_diffusion_card(storage, signal_date="2026-08-17")

    assert card.candidates == []
    assert card.historical_evidence["previous_day_audit_source"] == "UNAVAILABLE"


def test_h1_signal_day_never_uses_legacy_snapshot_fallback(tmp_path):
    storage = _storage(tmp_path)
    _legacy_day(storage, "2026-08-14", "2026-08-14 16:58:00")
    _legacy_day(storage, "2026-08-17", "2026-08-17 16:58:00")

    with pytest.raises(ValueError, match="no successful collection audit"):
        build_theme_new_entrant_diffusion_card(storage, signal_date="2026-08-17")


def test_h1_requires_acceleration_new_entrant_and_never_backfills_rank_two(tmp_path):
    storage = _storage(tmp_path)
    previous_date = "2026-01-05"
    signal_date = "2026-01-06"
    _h1_signal_data(storage, previous_date, signal_date)
    _pool(
        storage,
        "strong_pool",
        "600201",
        signal_date,
        name="排名一缺收盘",
        industry="机器人",
        amount=300,
        snapshot=f"{signal_date} 16:00:00",
    )
    _pool(
        storage,
        "strong_pool",
        "600202",
        signal_date,
        name="排名二有收盘",
        industry="机器人",
        amount=200,
        snapshot=f"{signal_date} 16:00:00",
    )
    _price(storage, "600202", signal_date, close=9.0)

    card = build_theme_new_entrant_diffusion_card(storage, signal_date=signal_date)

    assert card.candidates == []
    assert "本日0只" in card.historical_evidence["empty_reason"]


def test_h1_requires_breadth_to_strictly_exceed_previous_day(tmp_path):
    storage = _storage(tmp_path)
    previous_date = "2026-01-05"
    signal_date = "2026-01-06"
    _h1_signal_data(storage, previous_date, signal_date)
    _pool(
        storage,
        "zt_pool",
        "600104",
        previous_date,
        name="前日第三只",
        industry="机器人",
        amount=103,
        snapshot=f"{previous_date} 16:04:00",
    )
    _pool(
        storage,
        "strong_pool",
        "600201",
        signal_date,
        name="未加速候选",
        industry="机器人",
        amount=300,
        snapshot=f"{signal_date} 16:00:00",
    )
    _price(storage, "600201", signal_date, close=10.0)

    card = build_theme_new_entrant_diffusion_card(storage, signal_date=signal_date)

    assert card.candidates == []
    assert card.historical_evidence["current_candidate_count"] == 0


def test_h1_lifecycle_enters_d_plus_one_and_exits_d_plus_three(tmp_path):
    storage = _storage(tmp_path)
    previous_date = "2026-01-02"
    signal_date = "2026-01-05"
    _h1_signal_data(storage, previous_date, signal_date)
    _pool(
        storage,
        "strong_pool",
        "600201",
        signal_date,
        name="前向候选",
        industry="机器人",
        amount=300,
        snapshot=f"{signal_date} 16:00:00",
    )
    _price(storage, "600201", signal_date, close=10.0)
    card = build_theme_new_entrant_diffusion_card(storage, signal_date=signal_date)
    save_play_card(storage, card)

    for trade_date in ("2026-01-06", "2026-01-07", "2026-01-08"):
        _price(storage, "CAL2", trade_date)
    _price(
        storage,
        "600201",
        "2026-01-06",
        open_price=10.2,
        high=10.5,
        low=10.0,
    )
    _price(storage, "600201", "2026-01-07", open_price=99.0)
    _price(storage, "600201", "2026-01-08", open_price=11.22)
    _collection_run(storage, "2026-01-06", "ok", "2026-01-06 16:10:00")

    triggered = settle_theme_new_entrant_diffusion_cards(storage)
    assert triggered[0].paper_status == "TRIGGERED"
    candidate = triggered[0].candidates[0]
    assert candidate["entry_trade_date"] == "2026-01-06"
    assert candidate["entry_price"] == 10.2
    save_play_card(storage, triggered[0])

    _collection_run(storage, "2026-01-07", "ok", "2026-01-07 16:10:00")
    assert settle_theme_new_entrant_diffusion_cards(storage) == []

    _collection_run(storage, "2026-01-08", "ok", "2026-01-08 16:10:00")
    completed = settle_theme_new_entrant_diffusion_cards(storage)
    candidate = completed[0].candidates[0]
    assert completed[0].paper_status == "COMPLETED"
    assert candidate["exit_trade_date"] == "2026-01-08"
    assert candidate["exit_price"] == 11.22
    assert candidate["net_return_pct"] == pytest.approx(9.8)


def test_h1_builder_does_not_reselect_from_future_pool(tmp_path):
    storage = _storage(tmp_path)
    previous_date = "2026-01-05"
    signal_date = "2026-01-06"
    _h1_signal_data(storage, previous_date, signal_date)
    _pool(
        storage,
        "strong_pool",
        "600201",
        signal_date,
        name="冻结候选",
        industry="机器人",
        amount=200,
        snapshot=f"{signal_date} 16:00:00",
    )
    _price(storage, "600201", signal_date, close=10.0)
    _pool(
        storage,
        "strong_pool",
        "600999",
        "2026-01-07",
        name="未来高额样本",
        industry="机器人",
        amount=999999,
        snapshot="2026-01-07 16:00:00",
    )
    _price(storage, "600999", "2026-01-07", close=20.0)

    card = build_theme_new_entrant_diffusion_card(storage, signal_date=signal_date)

    assert [candidate["stock_code"] for candidate in card.candidates] == ["600201"]
