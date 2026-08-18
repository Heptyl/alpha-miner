"""Tests for the single-table, precomputed play-card boundary."""

import hashlib
import json
from dataclasses import replace
from pathlib import Path

import pytest

from src.data.storage import Storage
from src.mining.playbook import (
    PlayCard,
    load_latest_play_cards,
    load_recent_result_cards,
    save_play_card,
)


def _storage(tmp_path: Path) -> Storage:
    storage = Storage(str(tmp_path / "playbook.db"))
    storage.init_db()
    storage.init_db()
    return storage


def _card(**changes) -> PlayCard:
    card = PlayCard(
        play_id="three_to_four",
        play_name="三进四/四进五",
        behavior_logic="连续涨停形成注意力瀑布，盘中回封检验接力一致性。",
        signal_trade_date="2026-08-17",
        candidates=[{"stock_code": "000001", "board_count": 3}],
        trigger_rule="D日盘中封住第四板或按规则回封时模拟委托。",
        abandon_rule="一字板、队列不可达或回封失败时放弃。",
        exit_rule="D+1起遵守T+1，按止损、止盈或失效事件退出。",
        historical_evidence={"signal_days": 40, "win_rate": 0.55, "fill_rate": 0.62},
        paper_status="PAPER",
        admission_status="NOT_ADMITTED",
        generated_at="2026-08-17T16:20:00+08:00",
    )
    return replace(card, **changes)


def test_init_is_idempotent_and_empty_database_returns_no_cards(tmp_path):
    storage = _storage(tmp_path)

    tables = storage.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='play_cards'"
    )
    assert tables == [{"name": "play_cards"}]
    assert load_latest_play_cards(storage) == []


def test_save_round_trip_and_same_play_day_overwrites(tmp_path):
    storage = _storage(tmp_path)
    original = _card()
    save_play_card(storage, original)

    updated = replace(
        original,
        candidates=[{"board_count": 3, "stock_code": "000002"}],
        paper_status="TRIGGERED",
        generated_at="2026-08-17T16:30:00+08:00",
    )
    save_play_card(storage, updated)

    assert storage.execute("SELECT COUNT(*) AS n FROM play_cards") == [{"n": 1}]
    assert load_latest_play_cards(storage) == [updated]

    raw = storage.execute(
        "SELECT candidates_json, historical_evidence_json FROM play_cards"
    )[0]
    assert raw["candidates_json"] == '[{"board_count":3,"stock_code":"000002"}]'
    assert raw["historical_evidence_json"] == json.dumps(
        updated.historical_evidence,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def test_latest_batch_and_play_order_are_deterministic(tmp_path):
    storage = _storage(tmp_path)
    old = _card(signal_trade_date="2026-08-16")
    second = _card(
        play_id="first_board_reseal",
        play_name="首板分歧回封",
        candidates=[],
    )
    save_play_card(storage, old)
    save_play_card(storage, _card())
    save_play_card(storage, second)

    assert load_latest_play_cards(storage, as_of_date="2026-08-16") == [old]
    assert [card.play_id for card in load_latest_play_cards(storage)] == [
        "first_board_reseal",
        "three_to_four",
    ]


def test_forward_plan_rejects_status_rollback_and_entry_rewrite(tmp_path):
    storage = _storage(tmp_path)
    plan = {
        "play_id": "attention",
        "play_name": "三进四/四进五",
        "behavior_logic": "连续涨停形成注意力瀑布，盘中回封检验接力一致性。",
        "signal_trade_date": "2026-08-17",
        "generated_at": "2026-08-17T16:20:00+08:00",
        "trigger_rule": "D日盘中封住第四板或按规则回封时模拟委托。",
        "abandon_rule": "一字板、队列不可达或回封失败时放弃。",
        "exit_rule": "D+1起遵守T+1，按止损、止盈或失效事件退出。",
        "admission_status": "NOT_ADMITTED",
        "candidate_identity": [{"stock_code": "000001"}],
    }
    plan_hash = hashlib.sha256(
        json.dumps(plan, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    triggered_candidate = {
        "stock_code": "000001",
        "paper_status": "TRIGGERED",
        "entry_trade_date": "2026-08-18",
        "entry_price": 10.0,
        "entry_proxy": "OPEN_0931",
        "lifecycle_events": [
            {"status": "PLANNED", "recorded_at": "D", "reason": "frozen"},
            {"status": "TRIGGERED", "recorded_at": "D+1", "reason": "filled"},
        ],
    }
    card = _card(
        play_id="attention",
        candidates=[triggered_candidate],
        historical_evidence={"forward_plan": plan, "forward_plan_hash": plan_hash},
        paper_status="TRIGGERED",
    )
    save_play_card(storage, card)

    rollback = replace(
        card,
        candidates=[
            {
                "stock_code": "000001",
                "paper_status": "PLANNED",
                "lifecycle_events": triggered_candidate["lifecycle_events"][:1],
            }
        ],
        paper_status="PLANNED",
    )
    with pytest.raises(ValueError, match="append-only|backwards"):
        save_play_card(storage, rollback)
    rewritten = dict(triggered_candidate, entry_price=99.0)
    with pytest.raises(ValueError, match="entry"):
        save_play_card(storage, replace(card, candidates=[rewritten]))
    completed_candidate = {
        **triggered_candidate,
        "paper_status": "COMPLETED",
        "exit_trade_date": "2026-08-20",
        "exit_price": 11.0,
        "net_return_pct": 9.8,
        "result_reason": "settled",
        "lifecycle_events": [
            *triggered_candidate["lifecycle_events"],
            {"status": "COMPLETED", "recorded_at": "D+3", "reason": "settled"},
        ],
    }
    completed = replace(card, candidates=[completed_candidate], paper_status="COMPLETED")
    save_play_card(storage, completed)
    for malicious in (
        replace(completed, paper_status="PLANNED"),
        replace(completed, candidates=[dict(completed_candidate, paper_status="UNFILLED")]),
        replace(completed, candidates=[dict(completed_candidate, net_return_pct=99.0)]),
        replace(
            completed,
            historical_evidence={**completed.historical_evidence, "invented": True},
        ),
    ):
        with pytest.raises(ValueError):
            save_play_card(storage, malicious)
    assert load_latest_play_cards(storage) == [completed]


def test_forward_plan_compare_and_swap_rejects_concurrent_last_writer(tmp_path):
    storage = _storage(tmp_path)
    candidate = {
        "stock_code": "000001",
        "paper_status": "PLANNED",
        "lifecycle_events": [{"status": "PLANNED", "recorded_at": "D", "reason": "frozen"}],
    }
    plan = {
        "play_id": "attention",
        "play_name": "并发测试",
        "behavior_logic": "逻辑",
        "signal_trade_date": "2026-08-17",
        "generated_at": "2026-08-17T16:20:00+08:00",
        "trigger_rule": "触发",
        "abandon_rule": "放弃",
        "exit_rule": "退出",
        "admission_status": "NOT_ADMITTED",
        "candidate_identity": [{"stock_code": "000001"}],
    }
    plan_hash = hashlib.sha256(
        json.dumps(plan, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    initial = _card(
        play_id="attention",
        play_name="并发测试",
        behavior_logic="逻辑",
        candidates=[candidate],
        trigger_rule="触发",
        abandon_rule="放弃",
        exit_rule="退出",
        historical_evidence={"forward_plan": plan, "forward_plan_hash": plan_hash},
        paper_status="PLANNED",
    )
    save_play_card(storage, initial)
    winner_candidate = {
        **candidate,
        "paper_status": "INVALID",
        "result_reason": "DATA_NOT_READY",
        "lifecycle_events": [
            *candidate["lifecycle_events"],
            {"status": "INVALID", "recorded_at": "D+1", "reason": "DATA_NOT_READY"},
        ],
    }
    winner = replace(initial, candidates=[winner_candidate], paper_status="COMPLETED")
    desired_candidate = {
        **candidate,
        "paper_status": "TRIGGERED",
        "entry_trade_date": "2026-08-18",
        "entry_price": 10.0,
        "entry_proxy": "OPEN_0931",
        "result_reason": "filled",
        "lifecycle_events": [
            *candidate["lifecycle_events"],
            {"status": "TRIGGERED", "recorded_at": "D+1", "reason": "filled"},
        ],
    }
    desired = replace(initial, candidates=[desired_candidate], paper_status="TRIGGERED")

    class RacingStorage:
        raced = False

        def execute(self, sql, params=()):
            return storage.execute(sql, params)

        def execute_write(self, sql, params=()):
            if not self.raced:
                self.raced = True
                save_play_card(storage, winner)
            storage.execute_write(sql, params)

    with pytest.raises(ValueError, match="concurrent"):
        save_play_card(RacingStorage(), desired)
    assert load_latest_play_cards(storage) == [winner]


def test_non_admitted_paper_card_is_valid_and_not_watch_only(tmp_path):
    storage = _storage(tmp_path)
    card = _card(paper_status="PAPER", admission_status="NOT_ADMITTED")

    save_play_card(storage, card)

    loaded = load_latest_play_cards(storage)[0]
    assert loaded.paper_status == "PAPER"
    assert loaded.admission_status == "NOT_ADMITTED"
    assert "WATCH_ONLY" not in (loaded.paper_status, loaded.admission_status)


def test_recent_result_cards_exclude_latest_plan_and_are_deterministic(tmp_path):
    storage = _storage(tmp_path)
    completed = _card(
        signal_trade_date="2026-08-16",
        paper_status="COMPLETED",
        candidates=[
            {
                "stock_code": "000001",
                "board_count": 3,
                "paper_status": "NOT_TRIGGERED",
            }
        ],
    )
    save_play_card(storage, completed)
    save_play_card(storage, _card(paper_status="PLANNED"))

    assert load_recent_result_cards(
        storage,
        before_date="2026-08-17",
        limit=1,
    ) == [completed]


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"play_name": ""}, "play_name"),
        ({"signal_trade_date": "17-08-2026"}, "signal_trade_date"),
        ({"generated_at": "yesterday"}, "generated_at"),
        ({"candidates": {}}, "candidates"),
        ({"historical_evidence": {}}, "historical_evidence"),
        ({"paper_status": "WATCH_ONLY"}, "paper_status"),
        ({"admission_status": "AUTO_ADMITTED"}, "admission_status"),
        (
            {
                "candidates": [
                    {
                        "stock_code": "000001",
                        "paper_status": "WATCH_ONLY",
                    }
                ]
            },
            "candidate paper_status",
        ),
    ],
)
def test_invalid_cards_are_rejected_before_write(tmp_path, changes, message):
    storage = _storage(tmp_path)

    with pytest.raises(ValueError, match=message):
        save_play_card(storage, _card(**changes))

    assert storage.execute("SELECT COUNT(*) AS n FROM play_cards") == [{"n": 0}]


def test_read_module_has_no_slow_or_external_dependencies():
    source = (
        Path(__file__).resolve().parents[1] / "src" / "mining" / "playbook.py"
    ).read_text(encoding="utf-8")
    forbidden = (
        "EvolutionEngine",
        "BacktestEngine",
        "anthropic",
        "openai",
        "requests",
        "collector",
        "web",
    )
    assert all(term not in source for term in forbidden)
