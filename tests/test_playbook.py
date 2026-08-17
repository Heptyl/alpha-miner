"""Tests for the single-table, precomputed play-card boundary."""

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
