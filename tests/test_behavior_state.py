"""Synthetic contracts for the query-only PIT Behavior State reducer."""

from __future__ import annotations

from dataclasses import FrozenInstanceError, replace
from datetime import datetime
from pathlib import Path

import pytest

from src.data.pit import SHANGHAI, PITMode, PointInTimeView
from src.data.storage import Storage
from src.mining.behavior_state import (
    INDUSTRY_PROXY,
    BehaviorStateSpec,
    reduce_behavior_state,
)
from src.mining.experiments import ExperimentSpec


def _storage(tmp_path: Path) -> Storage:
    storage = Storage(str(tmp_path / "behavior.db"))
    storage.init_db()
    return storage


def _audit(storage: Storage, trade_date: str, *, at: str = "16:10:00", status: str = "ok"):
    storage.execute_write(
        "INSERT INTO limit_up_collection_runs(trade_date,attempted_at,price_rows,zt_rows,status) "
        "VALUES(?,?,5000,50,?)",
        (trade_date, f"{trade_date} {at}", status),
    )


def _zt(
    storage: Storage,
    code: str,
    trade_date: str,
    *,
    industry: str = "医药",
    consecutive: int = 1,
    snapshot: str = "16:00:00",
    seal_amount: float | None = None,
    open_count: int = 0,
):
    storage.execute_write(
        "INSERT INTO zt_pool(stock_code,trade_date,name,industry,consecutive_zt,amount,"
        "seal_amount,open_count,snapshot_time) VALUES(?,?,?, ?,?,1000,?,?,?)",
        (
            code,
            trade_date,
            f"股票{code}",
            industry,
            consecutive,
            seal_amount,
            open_count,
            f"{trade_date} {snapshot}",
        ),
    )


def _strong(storage: Storage, code: str, trade_date: str, industry: str = "医药"):
    storage.execute_write(
        "INSERT INTO strong_pool(stock_code,trade_date,name,industry,amount,snapshot_time) "
        "VALUES(?,?,?,?,1000,?)",
        (code, trade_date, f"股票{code}", industry, f"{trade_date} 16:01:00"),
    )


def _zb(storage: Storage, code: str, trade_date: str):
    storage.execute_write(
        "INSERT INTO zb_pool(stock_code,trade_date,amount,snapshot_time) VALUES(?,?,1000,?)",
        (code, trade_date, f"{trade_date} 16:02:00"),
    )


def _reduce(storage: Storage, as_of: datetime, calendar, spec=None):
    return reduce_behavior_state(
        PointInTimeView(storage, as_of, PITMode.FORWARD),
        as_of,
        calendar,
        spec or BehaviorStateSpec(),
    )


def _stock(snapshot, code: str):
    return next(item for item in snapshot.stocks if item.stock_code == code)


def test_trade_day_decay_ignores_weekend_and_spec_is_frozen(tmp_path):
    storage = _storage(tmp_path)
    _audit(storage, "2026-08-14")  # Friday
    _zt(storage, "000001", "2026-08-14")
    spec = BehaviorStateSpec(half_life_trade_days=1)
    snapshot = _reduce(
        storage,
        datetime(2026, 8, 17, 16, 30, tzinfo=SHANGHAI),
        ["2026-08-14", "2026-08-17"],
        spec,
    )
    state = _stock(snapshot, "000001")
    assert state.own_attention == pytest.approx(0.5)
    assert state.decay_age_trade_days == 1
    with pytest.raises(FrozenInstanceError):
        state.own_attention = 99  # type: ignore[misc]


def test_repeat_limit_and_auditable_seal_quality_reinforce_memory(tmp_path):
    storage = _storage(tmp_path)
    for trade_date in ("2026-08-14", "2026-08-17"):
        _audit(storage, trade_date)
    _zt(storage, "000001", "2026-08-14")
    _zt(
        storage,
        "000001",
        "2026-08-17",
        consecutive=2,
        seal_amount=500,
    )
    _zt(storage, "000002", "2026-08-17")
    snapshot = _reduce(
        storage,
        datetime(2026, 8, 17, 16, 30, tzinfo=SHANGHAI),
        ["2026-08-14", "2026-08-17"],
    )
    repeated, single = _stock(snapshot, "000001"), _stock(snapshot, "000002")
    assert repeated.own_attention > single.own_attention
    assert repeated.attention_slope is not None
    assert "recent_limit_memory" in repeated.state_domains
    assert single.negative_pulse is None
    assert "BREAKDOWN_PULSE_UNSUPPORTED" in single.limitations


def test_non_limit_strong_member_receives_only_industry_proxy_diffusion(tmp_path):
    storage = _storage(tmp_path)
    _audit(storage, "2026-08-17")
    _zt(storage, "000001", "2026-08-17", industry="算力")
    _strong(storage, "000002", "2026-08-17", industry="算力")
    snapshot = _reduce(
        storage,
        datetime(2026, 8, 17, 16, 30, tzinfo=SHANGHAI),
        ["2026-08-15", "2026-08-17"],
    )
    own, diffusion = _stock(snapshot, "000001"), _stock(snapshot, "000002")
    assert own.own_attention > 0 and diffusion.own_attention == 0
    assert diffusion.group_attention > 0 and diffusion.diffusion > 0
    assert diffusion.state_domains == ("industry_diffusion_non_limit",)
    assert diffusion.group_provenance == INDUSTRY_PROXY
    assert 0 < diffusion.crowding <= 1
    assert snapshot.groups[0].market_attention_share == pytest.approx(1.0)
    assert 0 < snapshot.groups[0].concentration <= 1
    assert snapshot.state_is_signal is False


def test_post_limit_domain_and_negative_pulse_preserve_missing_semantics(tmp_path):
    storage = _storage(tmp_path)
    for trade_date in ("2026-08-14", "2026-08-17"):
        _audit(storage, trade_date)
    _zt(storage, "000001", "2026-08-14")
    _zt(storage, "000002", "2026-08-14")
    _zb(storage, "000001", "2026-08-17")
    snapshot = _reduce(
        storage,
        datetime(2026, 8, 17, 16, 30, tzinfo=SHANGHAI),
        ["2026-08-14", "2026-08-17"],
    )
    failed, no_failure = _stock(snapshot, "000001"), _stock(snapshot, "000002")
    assert "post_limit_non_limit" in failed.state_domains
    assert failed.negative_pulse is not None and failed.negative_pulse > 0
    assert "ZB_POOL_POST_CLOSE_AUDITED" in failed.provenance
    assert no_failure.negative_pulse is not None  # observable group diffusion stopped
    assert "BREAKDOWN_PULSE_UNSUPPORTED" in no_failure.limitations


def test_input_order_is_irrelevant_and_as_of_is_deterministic(tmp_path):
    storage = _storage(tmp_path)
    for trade_date in ("2026-08-14", "2026-08-17"):
        _audit(storage, trade_date)
    _zt(storage, "000002", "2026-08-17", industry="机器人")
    _zt(storage, "000001", "2026-08-14", industry="机器人")
    as_of = datetime(2026, 8, 17, 16, 30, tzinfo=SHANGHAI)
    first = _reduce(storage, as_of, ["2026-08-14", "2026-08-17"])
    second = _reduce(storage, as_of, ["2026-08-17", "2026-08-14"])
    assert first == second


def test_future_rows_and_intraday_daily_outcomes_never_enter_state(tmp_path):
    storage = _storage(tmp_path)
    _audit(storage, "2026-08-17", at="09:25:00")
    _zt(storage, "000001", "2026-08-17", snapshot="09:30:00", open_count=99)
    _strong(storage, "000002", "2026-08-17")
    as_of = datetime(2026, 8, 17, 9, 31, tzinfo=SHANGHAI)
    snapshot = _reduce(storage, as_of, ["2026-08-14", "2026-08-17"])
    assert snapshot.stocks == ()
    assert "INTRADAY_DAILY_OUTCOMES_EXCLUDED" in snapshot.limitations
    assert "MINUTE_0935_UNSUPPORTED" in snapshot.limitations

    with pytest.raises(ValueError, match="future"):
        _reduce(storage, as_of, ["2026-08-17", "2026-08-18"])


def test_future_snapshot_is_hidden(tmp_path):
    storage = _storage(tmp_path)
    _audit(storage, "2026-08-14")
    _zt(storage, "000001", "2026-08-14", snapshot="23:59:00")
    snapshot = _reduce(
        storage,
        datetime(2026, 8, 14, 16, 30, tzinfo=SHANGHAI),
        ["2026-08-14"],
    )
    assert snapshot.stocks == ()


def test_latest_failed_audit_rejects_day(tmp_path):
    storage = _storage(tmp_path)
    _audit(storage, "2026-08-14")
    _audit(storage, "2026-08-14", at="16:20:00", status="failed")
    _zt(storage, "000001", "2026-08-14")
    snapshot = _reduce(
        storage,
        datetime(2026, 8, 14, 16, 30, tzinfo=SHANGHAI),
        ["2026-08-14"],
    )
    assert snapshot.stocks == ()
    assert "NO_USABLE_POST_CLOSE_AUDITS" in snapshot.limitations


def test_spec_hash_covers_every_parameter_and_validates_values():
    baseline = BehaviorStateSpec()
    assert baseline.spec_hash == BehaviorStateSpec().spec_hash
    for field in baseline.to_payload():
        if field == "model_version":
            changed = replace(baseline, model_version="behavior-state-v2")
        elif field == "lookback_trade_days":
            changed = replace(baseline, lookback_trade_days=21)
        else:
            changed = replace(baseline, **{field: getattr(baseline, field) + 0.1})
        assert changed.spec_hash != baseline.spec_hash
    assert baseline.candidate_identity()["behavior_state_spec_hash"] == baseline.spec_hash
    with pytest.raises(ValueError):
        BehaviorStateSpec(half_life_trade_days=0).validate()

    experiment = ExperimentSpec(
        play_id="future-play",
        behavior_hypothesis="testable",
        universe_rule="limit-up ecosystem",
        decision_boundary="D close",
        prediction="positive drift",
        entry_rule="frozen later",
        exit_rule="frozen later",
        executability_rule="required",
        invalidations=("missing data",),
        market_regime="all",
        development_protocol=("walk forward",),
        adapter_id="future-adapter",
        behavior_state_spec_hash=baseline.spec_hash,
    )
    assert experiment.protocol()["behavior_state_spec_hash"] == baseline.spec_hash


def test_reducer_rejects_pit_lookalike_with_storage_escape_methods():
    class MaliciousView:
        def query(self, *args, **kwargs):
            return None

        def execute(self, *args, **kwargs):
            raise AssertionError("must not execute")

        def _get_conn(self):
            raise AssertionError("must not connect")

        bypass_snapshot = True

    with pytest.raises(TypeError, match="PointInTimeView"):
        reduce_behavior_state(
            MaliciousView(),  # type: ignore[arg-type]
            datetime(2026, 8, 17, 16, 0, tzinfo=SHANGHAI),
            ["2026-08-17"],
            BehaviorStateSpec(),
        )
