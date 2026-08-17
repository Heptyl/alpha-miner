"""涨停历史连续采集、缺失告警与状态审计测试。"""

from datetime import datetime

import pandas as pd
from click.testing import CliRunner

from cli.limit_up import _collect_and_audit, main
from src.data.limit_up_history import (
    CalendarGap,
    LimitUpHistoryHealth,
    assess_limit_up_history,
    evaluate_collection_day,
    record_collection_attempt,
)
from src.data.storage import Storage


def _db(tmp_path) -> Storage:
    db = Storage(str(tmp_path / "history.db"))
    db.init_db()
    return db


def _insert_prices(db: Storage, trade_date: str, count: int = 2) -> None:
    db.insert(
        "daily_price",
        pd.DataFrame(
            [
                {
                    "stock_code": f"{index:06d}",
                    "trade_date": trade_date,
                    "open": 10,
                    "high": 11,
                    "low": 9,
                    "close": 10,
                    "volume": 100,
                    "amount": 1_000,
                }
                for index in range(count)
            ]
        ),
        dedup=True,
    )


def _insert_zt(db: Storage, trade_date: str, count: int = 1) -> None:
    db.insert(
        "zt_pool",
        pd.DataFrame(
            [
                {
                    "stock_code": f"{index:06d}",
                    "trade_date": trade_date,
                    "consecutive_zt": 1,
                }
                for index in range(count)
            ]
        ),
    )


def test_collection_day_distinguishes_missing_unknown_and_weekend(tmp_path):
    db = _db(tmp_path)
    _insert_prices(db, "2026-08-10")

    missing = evaluate_collection_day(db, "2026-08-10", min_market_rows=2)
    unknown = evaluate_collection_day(db, "2026-08-11", min_market_rows=2)
    weekend = evaluate_collection_day(db, "2026-08-15", min_market_rows=2)

    assert missing.status == "missing"
    assert missing.failed
    assert unknown.status == "unconfirmed"
    assert unknown.failed
    assert weekend.status == "skipped"
    assert not weekend.failed


def test_collection_day_flags_abnormal_rows_without_touching_factor_gates(tmp_path):
    db = _db(tmp_path)
    _insert_prices(db, "2026-08-10")
    _insert_zt(db, "2026-08-10", count=3)

    check = evaluate_collection_day(
        db,
        "2026-08-10",
        min_market_rows=2,
        max_zt_rows=2,
    )

    assert check.status == "row_anomaly"
    assert check.zt_rows == 3
    assert "超过异常上限" in check.detail


def test_collection_day_fails_when_market_data_is_incomplete(tmp_path):
    db = _db(tmp_path)
    _insert_prices(db, "2026-08-10", count=1)
    _insert_zt(db, "2026-08-10")

    check = evaluate_collection_day(db, "2026-08-10", min_market_rows=2)
    record_collection_attempt(db, check, attempted_at=datetime(2026, 8, 10, 16))
    health = assess_limit_up_history(db, as_of="2026-08-10", min_market_rows=2)

    assert check.status == "market_incomplete"
    assert check.failed
    assert "全市场行情数据不足" in check.detail
    assert health.failed_attempt_dates == (("2026-08-10", "market_incomplete"),)
    assert health.strict_failure


def test_history_reports_missing_dates_long_gaps_and_current_streak(tmp_path):
    db = _db(tmp_path)
    for trade_date in ("2026-06-01", "2026-08-10", "2026-08-11", "2026-08-12"):
        _insert_prices(db, trade_date)
    for trade_date in ("2026-06-01", "2026-08-10", "2026-08-12"):
        _insert_zt(db, trade_date)

    health = assess_limit_up_history(
        db,
        as_of="2026-08-12",
        min_market_rows=2,
        max_calendar_gap_days=4,
    )

    assert health.latest_price_date == "2026-08-12"
    assert health.latest_zt_date == "2026-08-12"
    assert health.history_days == 3
    assert health.continuous_days == 1
    assert health.missing_dates == ("2026-08-11",)
    assert health.calendar_gaps[-1].previous_date == "2026-06-01"
    assert health.calendar_gaps[-1].next_date == "2026-08-10"


def test_attempt_log_keeps_failure_active_after_a_later_success(tmp_path):
    db = _db(tmp_path)
    _insert_prices(db, "2026-08-11")
    _insert_zt(db, "2026-08-11")

    failed = evaluate_collection_day(db, "2026-08-10", min_market_rows=2)
    passed = evaluate_collection_day(db, "2026-08-11", min_market_rows=2)
    record_collection_attempt(db, failed, attempted_at=datetime(2026, 8, 10, 16))
    record_collection_attempt(db, passed, attempted_at=datetime(2026, 8, 11, 16))

    health = assess_limit_up_history(
        db,
        as_of="2026-08-11",
        min_market_rows=2,
    )

    assert health.tracking_start == "2026-08-10"
    assert health.last_attempt["status"] == "ok"
    assert health.failed_attempt_dates == (("2026-08-10", "unconfirmed"),)
    assert health.strict_failure


def test_retry_closes_a_failed_attempt_for_the_same_date(tmp_path):
    db = _db(tmp_path)
    failed = evaluate_collection_day(db, "2026-08-10", min_market_rows=2)
    record_collection_attempt(db, failed, attempted_at=datetime(2026, 8, 10, 16))
    _insert_prices(db, "2026-08-10")
    _insert_zt(db, "2026-08-10")
    passed = evaluate_collection_day(db, "2026-08-10", min_market_rows=2)
    record_collection_attempt(db, passed, attempted_at=datetime(2026, 8, 10, 17))

    health = assess_limit_up_history(
        db,
        as_of="2026-08-10",
        min_market_rows=2,
    )

    assert health.failed_attempt_dates == ()
    assert not health.strict_failure


def test_collect_and_audit_persists_empty_source_failure(tmp_path, monkeypatch):
    db = _db(tmp_path)
    _insert_prices(db, "2026-08-10")
    monkeypatch.setattr(
        "cli.limit_up.collect_date",
        lambda trade_date, storage: {"zt_pool": 0, "daily_price": 2},
    )
    monkeypatch.setattr(
        "cli.limit_up.evaluate_collection_day",
        lambda storage, trade_date: evaluate_collection_day(
            storage,
            trade_date,
            min_market_rows=2,
        ),
    )

    _, check = _collect_and_audit("2026-08-10", db)
    attempts = db.execute(
        "SELECT trade_date, status, zt_rows FROM limit_up_collection_runs"
    )

    assert check.status == "missing"
    assert attempts == [{"trade_date": "2026-08-10", "status": "missing", "zt_rows": 0}]


def test_collect_and_audit_persists_unexpected_collector_error(tmp_path, monkeypatch):
    db = _db(tmp_path)

    def broken_collector(*args, **kwargs):
        raise RuntimeError("secret-bearing upstream detail must not be persisted")

    monkeypatch.setattr("cli.limit_up.collect_date", broken_collector)

    counts, check = _collect_and_audit("2026-08-10", db)
    attempts = db.execute("SELECT status, detail FROM limit_up_collection_runs")

    assert counts == {}
    assert check.status == "collection_error"
    assert attempts == [
        {"status": "collection_error", "detail": "采集器异常退出（RuntimeError）"}
    ]


def test_status_displays_history_alerts_and_strict_exit(tmp_path, monkeypatch):
    db = _db(tmp_path)
    health = LimitUpHistoryHealth(
        latest_price_date="2026-08-12",
        latest_zt_date="2026-08-12",
        history_days=2,
        continuous_days=1,
        tracking_start="2026-08-10",
        missing_dates=("2026-08-11",),
        abnormal_dates=(("2026-08-12", 201),),
        failed_attempt_dates=(("2026-08-11", "missing"),),
        calendar_gaps=(CalendarGap("2026-06-01", "2026-08-10", 70),),
        last_attempt={
            "trade_date": "2026-08-12",
            "attempted_at": "2026-08-12 16:10:00.000",
            "price_rows": 5_000,
            "zt_rows": 201,
            "status": "row_anomaly",
            "detail": "too many rows",
        },
    )
    monkeypatch.setattr("cli.limit_up.assess_limit_up_history", lambda storage: health)

    result = CliRunner().invoke(
        main,
        ["status", "--db", db.db_path, "--state", str(tmp_path / "missing.json"), "--strict"],
    )

    assert result.exit_code == 1
    assert "缺采告警" in result.output
    assert "行数告警" in result.output
    assert "连续性告警" in result.output
    assert "采集失败告警" in result.output


def test_status_displays_signal_day_milestones_and_clamps_remaining(
    tmp_path, monkeypatch
):
    db = _db(tmp_path)
    health = LimitUpHistoryHealth(
        latest_price_date="2026-08-12",
        latest_zt_date="2026-08-12",
        history_days=13,
        continuous_days=13,
        tracking_start="2026-07-27",
        missing_dates=(),
        abnormal_dates=(),
        failed_attempt_dates=(),
        calendar_gaps=(),
        last_attempt=None,
    )
    monkeypatch.setattr("cli.limit_up.assess_limit_up_history", lambda storage: health)
    state_path = tmp_path / "state.json"
    state_path.write_text(
        '{"dataset_summary": {"signal_dates": 13, "minimum_signal_dates": 40}}',
        encoding="utf-8",
    )

    result = CliRunner().invoke(
        main,
        ["status", "--db", db.db_path, "--state", str(state_path)],
    )

    assert result.exit_code == 0
    assert "当前信号日" in result.output
    assert "13 日" in result.output
    assert "距离40日因子准入" in result.output
    assert "还差 27 日" in result.output
    assert "距离120日正式评估" in result.output
    assert "还差 107 日" in result.output

    state_path.write_text(
        '{"dataset_summary": {"signal_dates": 125, "minimum_signal_dates": 40}}',
        encoding="utf-8",
    )
    complete = CliRunner().invoke(
        main,
        ["status", "--db", db.db_path, "--state", str(state_path)],
    )

    assert complete.exit_code == 0
    assert complete.output.count("还差 0 日") == 2
