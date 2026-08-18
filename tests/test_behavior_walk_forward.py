"""Dual-track contracts for the first behavior-state executable play."""

from dataclasses import replace
from datetime import datetime
from pathlib import Path

import pytest

from src.data.pit import SHANGHAI
from src.data.storage import Storage
from src.mining.behavior_state import BehaviorStateSpec
from src.mining.experiments import AttentionReaccelerationRule, FrozenPartition
from src.mining.playbook import load_latest_play_cards, save_play_card
from src.mining.plays import (
    ATTENTION_REACCELERATION_PLAY_ID,
    attention_reacceleration_candidate,
    build_attention_reacceleration_card,
    evaluate_attention_open_trigger,
    evaluate_attention_reacceleration_development,
    select_attention_reacceleration_states,
    settle_attention_reacceleration_cards,
)


def _storage(tmp_path: Path) -> Storage:
    storage = Storage(str(tmp_path / "walk-forward.db"))
    storage.init_db()
    return storage


def _audit(storage: Storage, trade_date: str, status: str = "ok") -> None:
    storage.execute_write(
        "INSERT INTO limit_up_collection_runs"
        "(trade_date,attempted_at,price_rows,zt_rows,status) VALUES(?,?,5000,50,?)",
        (trade_date, f"{trade_date} 16:10:00", status),
    )


def _price(
    storage: Storage,
    code: str,
    trade_date: str,
    *,
    open_price: float = 10,
    close: float = 10,
    high: float = 10.5,
    low: float = 9.5,
    volume: float = 100,
) -> None:
    storage.execute_write(
        "INSERT INTO daily_price"
        "(stock_code,trade_date,open,high,low,close,volume,snapshot_time) "
        "VALUES(?,?,?,?,?,?,?,?)",
        (code, trade_date, open_price, high, low, close, volume, f"{trade_date} 16:05:00"),
    )


def _zt(storage: Storage, code: str, trade_date: str, industry: str = "算力") -> None:
    storage.execute_write(
        "INSERT INTO zt_pool"
        "(stock_code,trade_date,name,industry,consecutive_zt,amount,snapshot_time) "
        "VALUES(?,?,?,?,1,1000,?)",
        (code, trade_date, f"股票{code}", industry, f"{trade_date} 16:00:00"),
    )


def _strong(storage: Storage, code: str, trade_date: str, industry: str = "算力") -> None:
    storage.execute_write(
        "INSERT INTO strong_pool"
        "(stock_code,trade_date,name,industry,amount,snapshot_time) VALUES(?,?,?,?,1000,?)",
        (code, trade_date, f"股票{code}", industry, f"{trade_date} 16:01:00"),
    )


def _forward_fixture(storage: Storage) -> None:
    for trade_date in ("2026-08-14", "2026-08-17"):
        _audit(storage, trade_date)
        _price(storage, "CAL", trade_date)
    _zt(storage, "000001", "2026-08-14")
    _zt(storage, "000002", "2026-08-17")
    _strong(storage, "000003", "2026-08-17")
    _price(storage, "000003", "2026-08-17", close=10)


def _prelimit(storage: Storage, phase: str, at: str, price: float, volume: float) -> None:
    storage.execute_write(
        "INSERT INTO prelimit_snapshots"
        "(trade_date,candidate_trade_date,observed_at,stock_code,stock_name,phase,"
        "price,open,high,low,volume,amount,source,snapshot_time) "
        "VALUES('2026-08-18','2026-08-17',?,'000003','股票000003',?,?,?,?,?,?,?,"
        "'mock',?)",
        (
            f"2026-08-18 {at}",
            phase,
            price,
            price,
            price + 0.1,
            price - 0.1,
            volume,
            volume * price,
            f"2026-08-18 {at}",
        ),
    )


def test_forward_plan_includes_non_limit_diffusion_and_is_immutable(tmp_path):
    storage = _storage(tmp_path)
    _forward_fixture(storage)
    generated = datetime(2026, 8, 17, 16, 30, tzinfo=SHANGHAI)
    card = build_attention_reacceleration_card(storage, "2026-08-17", generated)
    assert card.play_id == ATTENTION_REACCELERATION_PLAY_ID
    diffusion = next(item for item in card.candidates if item["stock_code"] == "000003")
    assert "industry_diffusion_non_limit" in diffusion["state_domains"]
    assert diffusion["paper_status"] == "PLANNED"
    assert card.historical_evidence["planned_entry_date"] == "2026-08-18"
    assert card.historical_evidence["planned_exit_date"] == "2026-08-20"
    save_play_card(storage, card)

    changed_plan = dict(card.historical_evidence["forward_plan"])
    changed_plan["planned_exit_date"] = "2026-08-21"
    evidence = dict(card.historical_evidence)
    evidence["forward_plan"] = changed_plan
    with pytest.raises(ValueError, match="hash"):
        save_play_card(storage, replace(card, historical_evidence=evidence))
    assert load_latest_play_cards(storage) == [card]
    with pytest.raises(ValueError, match="signal date"):
        build_attention_reacceleration_card(
            storage, "2026-08-17", "2026-08-18T09:00:00+08:00"
        )


def test_forward_lifecycle_trigger_complete_and_idempotent(tmp_path):
    storage = _storage(tmp_path)
    _forward_fixture(storage)
    card = build_attention_reacceleration_card(
        storage, "2026-08-17", "2026-08-17T16:30:00+08:00"
    )
    save_play_card(storage, card)
    _prelimit(storage, "AUCTION_0925", "09:25:00", 10.1, 10)
    _prelimit(storage, "OPEN_0931", "09:31:00", 10.2, 20)
    triggered = settle_attention_reacceleration_cards(
        storage, recorded_at="2026-08-18T16:20:00+08:00"
    )[0]
    assert triggered.paper_status == "TRIGGERED"
    triggered_item = next(
        item for item in triggered.candidates if item["stock_code"] == "000003"
    )
    assert triggered_item["entry_price"] == pytest.approx(10.2)
    assert triggered_item["lifecycle_events"][-1]["auction_observed_at"].endswith("09:25:00")
    assert triggered_item["lifecycle_events"][-1]["open_observed_at"].endswith("09:31:00")
    save_play_card(storage, triggered)
    assert settle_attention_reacceleration_cards(storage) == []

    _audit(storage, "2026-08-20", status="failed")
    pending = settle_attention_reacceleration_cards(
        storage, recorded_at="2026-08-20T16:15:00+08:00"
    )[0]
    pending_item = next(item for item in pending.candidates if item["stock_code"] == "000003")
    assert pending.paper_status == "TRIGGERED"
    assert "DATA_NOT_READY" in pending_item["pending_reason"]
    save_play_card(storage, pending)
    _audit(storage, "2026-08-20")
    _price(storage, "CAL", "2026-08-20")
    _price(storage, "000003", "2026-08-20", open_price=10.5)
    completed = settle_attention_reacceleration_cards(
        storage, recorded_at="2026-08-20T16:20:00+08:00"
    )[0]
    assert completed.paper_status == "COMPLETED"
    completed_item = next(
        item for item in completed.candidates if item["stock_code"] == "000003"
    )
    assert completed_item["net_return_pct"] == pytest.approx(
        (10.5 / 10.2 - 1) * 100 - 0.2
    )
    assert [event["status"] for event in completed_item["lifecycle_events"]] == [
        "PLANNED",
        "TRIGGERED",
        "TRIGGERED",
        "COMPLETED",
    ]
    save_play_card(storage, completed)
    assert settle_attention_reacceleration_cards(storage) == []


def test_settler_rebuilds_each_frozen_genome_rule(tmp_path):
    storage = _storage(tmp_path)
    _forward_fixture(storage)
    rule = replace(AttentionReaccelerationRule(), min_total_attention=0.25)
    card = build_attention_reacceleration_card(
        storage, "2026-08-17", "2026-08-17T16:30:00+08:00", rule=rule
    )
    save_play_card(storage, card)
    _prelimit(storage, "AUCTION_0925", "09:25:00", 10.1, 10)
    _prelimit(storage, "OPEN_0931", "09:31:00", 10.2, 20)
    settled = settle_attention_reacceleration_cards(
        storage, recorded_at="2026-08-18T16:20:00+08:00"
    )
    assert settled and settled[0].historical_evidence["forward_plan"]["rule_hash"] == rule.rule_hash


@pytest.mark.parametrize(
    ("opening", "expected"),
    [
        ({"price": None, "volume": 1, "amount": 1}, "INVALID"),
        ({"price": 10, "high": 10, "low": 10, "volume": 1, "amount": 1}, "UNFILLED"),
        ({"price": 11, "high": 11.1, "low": 10.9, "volume": 1, "amount": 1}, "UNFILLED"),
    ],
)
def test_shared_trigger_preserves_invalid_and_unfilled(opening, expected):
    decision = evaluate_attention_open_trigger(10, opening, AttentionReaccelerationRule())
    assert decision.status == expected


def test_rule_hash_changes_and_high_state_alone_is_not_a_signal():
    baseline = AttentionReaccelerationRule()
    assert baseline.rule_hash != replace(baseline, min_attention_slope=0.1).rule_hash
    from src.mining.behavior_state import BehaviorStateSnapshot, StockBehaviorState

    stock = StockBehaviorState(
        "000001", "股票", None, None, ("recent_limit_memory",), 99, 0, 0, 0,
        0, -0.01, None, (), (),
    )
    snapshot = BehaviorStateSnapshot(
        "2026-08-17T16:30:00+08:00", "2026-08-17", BehaviorStateSpec().spec_hash,
        (stock,), (), (), False,
    )
    assert select_attention_reacceleration_states(snapshot, baseline) == ()


def test_missing_prelimit_pair_is_retained_as_invalid(tmp_path):
    storage = _storage(tmp_path)
    _forward_fixture(storage)
    card = build_attention_reacceleration_card(
        storage, "2026-08-17", "2026-08-17T16:30:00+08:00"
    )
    save_play_card(storage, card)
    _prelimit(storage, "OPEN_0931", "09:31:00", 10.2, 20)
    settled = settle_attention_reacceleration_cards(
        storage, recorded_at="2026-08-18T16:20:00+08:00"
    )[0]
    assert settled.paper_status == "COMPLETED"
    item = next(item for item in settled.candidates if item["stock_code"] == "000003")
    assert item["paper_status"] == "INVALID"
    assert "DATA_NOT_READY" in item["result_reason"]


def test_opening_cumulative_amount_cannot_move_backwards(tmp_path):
    storage = _storage(tmp_path)
    _forward_fixture(storage)
    card = build_attention_reacceleration_card(
        storage, "2026-08-17", "2026-08-17T16:30:00+08:00"
    )
    save_play_card(storage, card)
    _prelimit(storage, "AUCTION_0925", "09:25:00", 10.1, 20)
    _prelimit(storage, "OPEN_0931", "09:31:00", 10.2, 10)
    settled = settle_attention_reacceleration_cards(
        storage, recorded_at="2026-08-18T16:20:00+08:00"
    )[0]
    item = next(item for item in settled.candidates if item["stock_code"] == "000003")
    assert item["paper_status"] == "INVALID"
    assert "累计量额" in item["result_reason"]


def test_retro_walk_forward_freezes_before_daily_open_proxy_and_never_reads_reserved(
    tmp_path,
):
    storage = _storage(tmp_path)
    dates = (
        "2026-08-10", "2026-08-11", "2026-08-12", "2026-08-13",
        "2026-08-14", "2026-08-17", "2026-08-18", "2026-08-19",
    )
    for trade_date in dates:
        _audit(storage, trade_date)
        _price(storage, "CAL", trade_date)
    _zt(storage, "000001", "2026-08-12")
    _zt(storage, "000002", "2026-08-13")
    _strong(storage, "000003", "2026-08-13")
    _price(storage, "000003", "2026-08-13", close=10)
    _price(storage, "000003", "2026-08-14", open_price=10.2)
    _price(storage, "000003", "2026-08-18", open_price=10.5)
    partition = FrozenPartition(
        play_id=ATTENTION_REACCELERATION_PLAY_ID,
        dataset_snapshot_hash="a" * 64,
        audited_dates=dates,
        development_dates=dates[:4],
        embargo_dates=dates[4:7],
        reserved_dates=dates[7:],
    )
    candidate = attention_reacceleration_candidate(partition)
    queries = []
    original = storage.execute

    def spy(sql, params=()):
        queries.append((sql, params))
        return original(sql, params)

    storage.execute = spy  # type: ignore[method-assign]
    evidence = evaluate_attention_reacceleration_development(storage, candidate)
    assert evidence.filled_count >= 1
    assert evidence.completed_signal_days >= 1
    assert "DAILY_OPEN_PROXY" in evidence.data_limitations
    assert "RETRO_DEVELOPMENT_ONLY" in evidence.data_limitations
    assert not any("2026-08-19" in tuple(map(str, params)) for _, params in queries)
