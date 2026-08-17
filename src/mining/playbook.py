"""Minimal persistence contract for the single USER-facing play card."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Protocol

PAPER_STATUSES = frozenset({"PAPER", "PLANNED", "TRIGGERED", "COMPLETED"})
ADMISSION_STATUSES = frozenset({"NOT_ADMITTED", "ADMISSION_ELIGIBLE", "ADMITTED"})
CANDIDATE_PAPER_STATUSES = frozenset(
    {"PLANNED", "NOT_TRIGGERED", "UNFILLED", "TRIGGERED", "COMPLETED"}
)


class PlayCardStorage(Protocol):
    """The small part of ``Storage`` used by this read/write boundary."""

    def execute(self, sql: str, params: tuple = ()) -> list[dict]: ...

    def execute_write(self, sql: str, params: tuple = ()) -> None: ...


@dataclass(frozen=True)
class PlayCard:
    """One precomputed play shown identically for PAPER and admitted research."""

    play_id: str
    play_name: str
    behavior_logic: str
    signal_trade_date: str
    candidates: list[dict[str, Any]]
    trigger_rule: str
    abandon_rule: str
    exit_rule: str
    historical_evidence: dict[str, Any]
    paper_status: str
    admission_status: str
    generated_at: str

    def validate(self) -> None:
        """Reject an incomplete or ambiguous card before any database write."""
        required_text = {
            "play_id": self.play_id,
            "play_name": self.play_name,
            "behavior_logic": self.behavior_logic,
            "trigger_rule": self.trigger_rule,
            "abandon_rule": self.abandon_rule,
            "exit_rule": self.exit_rule,
            "generated_at": self.generated_at,
        }
        for field_name, value in required_text.items():
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{field_name} must be a non-empty string")

        try:
            date.fromisoformat(self.signal_trade_date)
        except (TypeError, ValueError) as exc:
            raise ValueError("signal_trade_date must be YYYY-MM-DD") from exc
        try:
            datetime.fromisoformat(self.generated_at.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError("generated_at must be an ISO-8601 datetime") from exc

        if not isinstance(self.candidates, list):
            raise ValueError("candidates must be a list")
        if not all(isinstance(candidate, dict) for candidate in self.candidates):
            raise ValueError("each candidate must be an object")
        for candidate in self.candidates:
            candidate_status = candidate.get("paper_status")
            if (
                candidate_status is not None
                and candidate_status not in CANDIDATE_PAPER_STATUSES
            ):
                raise ValueError(
                    f"unsupported candidate paper_status: {candidate_status!r}"
                )
        if not isinstance(self.historical_evidence, dict) or not self.historical_evidence:
            raise ValueError("historical_evidence must be a non-empty object")
        if self.paper_status not in PAPER_STATUSES:
            raise ValueError(f"unsupported paper_status: {self.paper_status!r}")
        if self.admission_status not in ADMISSION_STATUSES:
            raise ValueError(f"unsupported admission_status: {self.admission_status!r}")

        _deterministic_json(self.candidates, "candidates")
        _deterministic_json(self.historical_evidence, "historical_evidence")


def _deterministic_json(value: Any, field_name: str) -> str:
    """Serialize JSON in one stable representation and reject NaN/Infinity."""
    try:
        encoded = json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must contain valid JSON values") from exc
    return encoded


def save_play_card(storage: PlayCardStorage, card: PlayCard) -> None:
    """Insert or replace the one card for a play and signal trading day."""
    if not isinstance(card, PlayCard):
        raise TypeError("card must be a PlayCard")
    card.validate()
    storage.execute_write(
        """
        INSERT INTO play_cards (
            play_id, play_name, behavior_logic, signal_trade_date,
            candidates_json, trigger_rule, abandon_rule, exit_rule,
            historical_evidence_json, paper_status, admission_status, generated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(play_id, signal_trade_date) DO UPDATE SET
            play_name = excluded.play_name,
            behavior_logic = excluded.behavior_logic,
            candidates_json = excluded.candidates_json,
            trigger_rule = excluded.trigger_rule,
            abandon_rule = excluded.abandon_rule,
            exit_rule = excluded.exit_rule,
            historical_evidence_json = excluded.historical_evidence_json,
            paper_status = excluded.paper_status,
            admission_status = excluded.admission_status,
            generated_at = excluded.generated_at
        """,
        (
            card.play_id,
            card.play_name,
            card.behavior_logic,
            card.signal_trade_date,
            _deterministic_json(card.candidates, "candidates"),
            card.trigger_rule,
            card.abandon_rule,
            card.exit_rule,
            _deterministic_json(card.historical_evidence, "historical_evidence"),
            card.paper_status,
            card.admission_status,
            card.generated_at,
        ),
    )


def load_latest_play_cards(
    storage: PlayCardStorage,
    as_of_date: str | None = None,
) -> list[PlayCard]:
    """Load the latest precomputed signal-day batch, deterministically sorted."""
    params: tuple = ()
    date_filter = ""
    if as_of_date is not None:
        try:
            date.fromisoformat(as_of_date)
        except (TypeError, ValueError) as exc:
            raise ValueError("as_of_date must be YYYY-MM-DD") from exc
        date_filter = "WHERE signal_trade_date <= ?"
        params = (as_of_date,)

    rows = storage.execute(
        f"""
        SELECT play_id, play_name, behavior_logic, signal_trade_date,
               candidates_json, trigger_rule, abandon_rule, exit_rule,
               historical_evidence_json, paper_status, admission_status, generated_at
        FROM play_cards
        WHERE signal_trade_date = (
            SELECT MAX(signal_trade_date) FROM play_cards {date_filter}
        )
        ORDER BY play_id ASC, play_name ASC
        """,
        params,
    )
    return [_card_from_row(row) for row in rows]


def load_recent_result_cards(
    storage: PlayCardStorage,
    before_date: str | None = None,
    limit: int = 1,
) -> list[PlayCard]:
    """Load recent triggered/terminal PAPER cards for the USER result section."""
    if not isinstance(limit, int) or isinstance(limit, bool) or limit < 1:
        raise ValueError("limit must be a positive integer")
    params: list[Any] = []
    date_filter = ""
    if before_date is not None:
        try:
            date.fromisoformat(before_date)
        except (TypeError, ValueError) as exc:
            raise ValueError("before_date must be YYYY-MM-DD") from exc
        date_filter = "AND signal_trade_date < ?"
        params.append(before_date)
    params.append(limit)
    rows = storage.execute(
        f"""
        SELECT play_id, play_name, behavior_logic, signal_trade_date,
               candidates_json, trigger_rule, abandon_rule, exit_rule,
               historical_evidence_json, paper_status, admission_status, generated_at
        FROM play_cards
        WHERE paper_status IN ('TRIGGERED', 'COMPLETED')
          {date_filter}
        ORDER BY signal_trade_date DESC, play_id ASC, play_name ASC
        LIMIT ?
        """,
        tuple(params),
    )
    return [_card_from_row(row) for row in rows]


def load_pending_play_cards(
    storage: PlayCardStorage,
    play_id: str,
) -> list[PlayCard]:
    """Load pending cards for a programmatic lifecycle worker."""
    if not isinstance(play_id, str) or not play_id.strip():
        raise ValueError("play_id must be a non-empty string")
    rows = storage.execute(
        """
        SELECT play_id, play_name, behavior_logic, signal_trade_date,
               candidates_json, trigger_rule, abandon_rule, exit_rule,
               historical_evidence_json, paper_status, admission_status, generated_at
        FROM play_cards
        WHERE play_id = ?
          AND paper_status IN ('PAPER', 'PLANNED', 'TRIGGERED')
        ORDER BY signal_trade_date ASC, play_id ASC
        """,
        (play_id,),
    )
    return [_card_from_row(row) for row in rows]


def _card_from_row(row: dict[str, Any]) -> PlayCard:
    candidates = json.loads(row["candidates_json"])
    evidence = json.loads(row["historical_evidence_json"])
    card = PlayCard(
        play_id=row["play_id"],
        play_name=row["play_name"],
        behavior_logic=row["behavior_logic"],
        signal_trade_date=row["signal_trade_date"],
        candidates=candidates,
        trigger_rule=row["trigger_rule"],
        abandon_rule=row["abandon_rule"],
        exit_rule=row["exit_rule"],
        historical_evidence=evidence,
        paper_status=row["paper_status"],
        admission_status=row["admission_status"],
        generated_at=row["generated_at"],
    )
    card.validate()
    return card
