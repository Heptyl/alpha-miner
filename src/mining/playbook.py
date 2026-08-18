"""Minimal persistence contract for the single USER-facing play card."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Protocol

PAPER_STATUSES = frozenset(
    {"PAPER", "PLANNED", "TRIGGERED", "COMPLETED", "DATA_NOT_READY"}
)
ADMISSION_STATUSES = frozenset({"NOT_ADMITTED", "ADMISSION_ELIGIBLE", "ADMITTED"})
CANDIDATE_PAPER_STATUSES = frozenset(
    {"PLANNED", "NOT_TRIGGERED", "UNFILLED", "INVALID", "TRIGGERED", "COMPLETED"}
)
_STATUS_RANK = {
    "PLANNED": 0,
    "TRIGGERED": 1,
    "NOT_TRIGGERED": 2,
    "UNFILLED": 2,
    "INVALID": 2,
    "COMPLETED": 3,
}
_CARD_STATUS_RANK = {
    "PAPER": 0,
    "PLANNED": 0,
    "TRIGGERED": 1,
    "DATA_NOT_READY": 2,
    "COMPLETED": 2,
}
_CANDIDATE_RESULT_FIELDS = frozenset(
    {
        "paper_status",
        "lifecycle_events",
        "result_reason",
        "entry_trade_date",
        "entry_price",
        "entry_gap_pct",
        "entry_proxy",
        "exit_trade_date",
        "exit_price",
        "exit_proxy",
        "total_cost_bps",
        "net_return_pct",
        "auction_observed_at",
        "open_observed_at",
        "pending_reason",
    }
)
_TERMINAL_STATUSES = frozenset({"NOT_TRIGGERED", "UNFILLED", "INVALID", "COMPLETED"})
_CARD_DB_COLUMNS = (
    "play_id", "play_name", "behavior_logic", "signal_trade_date", "candidates_json",
    "trigger_rule", "abandon_rule", "exit_rule", "historical_evidence_json",
    "paper_status", "admission_status", "generated_at",
)
_CAS_COLUMNS = _CARD_DB_COLUMNS[1:3] + _CARD_DB_COLUMNS[4:]
_CARD_COLUMNS = ", ".join(_CARD_DB_COLUMNS)
_UPSERT_SQL = f"""INSERT INTO play_cards ({_CARD_COLUMNS})
VALUES ({', '.join('?' for _ in _CARD_DB_COLUMNS)})
ON CONFLICT(play_id, signal_trade_date) DO UPDATE SET
{', '.join(f'{column} = excluded.{column}' for column in _CAS_COLUMNS)}
WHERE {' AND '.join(f'play_cards.{column} = ?' for column in _CAS_COLUMNS)}"""
_FORWARD_ENVELOPE_FIELDS = (
    "play_id", "play_name", "behavior_logic", "signal_trade_date", "generated_at",
    "trigger_rule", "abandon_rule", "exit_rule", "admission_status",
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
        for field_name in (
            "play_id", "play_name", "behavior_logic", "trigger_rule",
            "abandon_rule", "exit_rule", "generated_at",
        ):
            value = getattr(self, field_name)
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
        plan = self.historical_evidence.get("forward_plan")
        plan_hash = self.historical_evidence.get("forward_plan_hash")
        if plan is not None or plan_hash is not None:
            if not isinstance(plan, dict) or not isinstance(plan_hash, str):
                raise ValueError("forward plan and hash must be stored together")
            if canonical_json_hash(plan) != plan_hash:
                raise ValueError("forward_plan_hash does not match the immutable plan")
            envelope = {field: getattr(self, field) for field in _FORWARD_ENVELOPE_FIELDS}
            if any(plan.get(field) != value for field, value in envelope.items()):
                raise ValueError("forward plan envelope differs from the actual card")
            identities = plan.get("candidate_identity")
            if not isinstance(identities, list):
                raise ValueError("forward plan must freeze candidate identity")
            frozen_by_code = {
                str(item.get("stock_code")): item
                for item in identities
                if isinstance(item, dict) and item.get("stock_code")
            }
            actual_codes = {str(item.get("stock_code") or "") for item in self.candidates}
            if actual_codes != set(frozen_by_code):
                raise ValueError("forward plan candidate set differs from the actual card")
            for candidate in self.candidates:
                code = str(candidate.get("stock_code") or "")
                frozen = frozen_by_code.get(code)
                if frozen is None:
                    raise ValueError("candidate is absent from the immutable plan")
                for field, value in frozen.items():
                    if candidate.get(field) != value:
                        raise ValueError(f"candidate immutable field changed: {field}")
                events = candidate.get("lifecycle_events", [])
                if not isinstance(events, list) or not all(isinstance(item, dict) for item in events):
                    raise ValueError("lifecycle_events must be a list of objects")
                _validate_lifecycle_events(events)
                if not events or events[-1].get("status") != candidate.get("paper_status"):
                    raise ValueError("candidate status must equal its latest lifecycle event")


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


def canonical_json_hash(value: Any) -> str:
    """Return the canonical hash used by immutable forward plans."""
    return hashlib.sha256(_deterministic_json(value, "immutable plan").encode()).hexdigest()


def freeze_forward_plan(
    envelope: dict[str, Any],
    candidates: list[dict[str, Any]],
    identity_fields: tuple[str, ...],
    details: dict[str, Any],
) -> tuple[dict[str, Any], str]:
    """Project one canonical immutable plan from card fields and frozen candidates."""
    plan = {
        **envelope,
        **details,
        "candidate_identity": [
            {field: item[field] for field in identity_fields} for item in candidates
        ],
    }
    return plan, canonical_json_hash(plan)


def _validate_lifecycle_events(events: list[dict[str, Any]]) -> None:
    previous = -1
    previous_status = None
    for event in events:
        status = event.get("status")
        if status not in CANDIDATE_PAPER_STATUSES:
            raise ValueError(f"unsupported lifecycle event status: {status!r}")
        rank = _STATUS_RANK[status]
        if rank < previous or (previous_status in _TERMINAL_STATUSES and status != previous_status):
            raise ValueError("candidate lifecycle cannot move backwards or leave a terminal state")
        previous = rank
        previous_status = status


def save_play_card(storage: PlayCardStorage, card: PlayCard) -> None:
    """Insert or replace the one card for a play and signal trading day."""
    if not isinstance(card, PlayCard):
        raise TypeError("card must be a PlayCard")
    card.validate()
    stored = load_play_card(storage, card.play_id, card.signal_trade_date)
    previous = stored or card
    if stored:
        _validate_monotonic_update(previous, card)
    storage.execute_write(_UPSERT_SQL, _card_values(card) + _cas_values(previous))
    if load_play_card(storage, card.play_id, card.signal_trade_date) != card:
        raise ValueError("concurrent PAPER card update conflict")


def _validate_monotonic_update(previous: PlayCard, current: PlayCard) -> None:
    old_hash = previous.historical_evidence.get("forward_plan_hash")
    new_hash = current.historical_evidence.get("forward_plan_hash")
    if old_hash is None and new_hash is None:
        return
    if old_hash != new_hash or previous.generated_at != current.generated_at:
        raise ValueError("frozen PAPER plan cannot be replaced")
    if previous.historical_evidence != current.historical_evidence:
        raise ValueError("forward plan evidence is immutable; results belong to lifecycle events")
    fixed = (
        "play_name",
        "behavior_logic",
        "trigger_rule",
        "abandon_rule",
        "exit_rule",
        "admission_status",
    )
    if any(getattr(previous, field) != getattr(current, field) for field in fixed):
        raise ValueError("frozen PAPER rules cannot be changed during settlement")
    old_card_rank = _CARD_STATUS_RANK[previous.paper_status]
    new_card_rank = _CARD_STATUS_RANK[current.paper_status]
    if new_card_rank < old_card_rank or (
        old_card_rank == 2 and current.paper_status != previous.paper_status
    ):
        raise ValueError("PAPER card status cannot move backwards or change terminal state")
    old_by_code = {str(item.get("stock_code")): item for item in previous.candidates}
    new_by_code = {str(item.get("stock_code")): item for item in current.candidates}
    if old_by_code.keys() != new_by_code.keys():
        raise ValueError("frozen PAPER candidates cannot be changed during settlement")
    for code, old in old_by_code.items():
        new = new_by_code[code]
        old_events = old.get("lifecycle_events", [])
        new_events = new.get("lifecycle_events", [])
        if new_events[: len(old_events)] != old_events:
            raise ValueError(f"candidate {code} lifecycle evidence is append-only")
        old_status = str(old.get("paper_status") or "PLANNED")
        new_status = str(new.get("paper_status") or "PLANNED")
        if _STATUS_RANK[new_status] < _STATUS_RANK[old_status]:
            raise ValueError(f"candidate {code} lifecycle cannot move backwards")
        if old_status in _TERMINAL_STATUSES and new_status != old_status:
            raise ValueError(f"candidate {code} terminal status cannot change")
        old_plan_fields = {key: value for key, value in old.items() if key not in _CANDIDATE_RESULT_FIELDS}
        new_plan_fields = {key: value for key, value in new.items() if key not in _CANDIDATE_RESULT_FIELDS}
        if old_plan_fields != new_plan_fields:
            raise ValueError(f"candidate {code} immutable plan fields cannot change")
        for field in _CANDIDATE_RESULT_FIELDS - {"paper_status", "lifecycle_events"}:
            if field in old and old.get(field) != new.get(field):
                raise ValueError(f"candidate {code} first result field cannot change: {field}")


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

    return _load_cards(
        storage,
        f"""WHERE signal_trade_date = (
            SELECT MAX(signal_trade_date) FROM play_cards {date_filter}
        )
        ORDER BY play_id ASC, play_name ASC""",
        params,
    )


def load_play_card(
    storage: PlayCardStorage, play_id: str, signal_trade_date: str
) -> PlayCard | None:
    """Load and fully validate one frozen card, or return ``None``."""
    cards = _load_cards(
        storage,
        "WHERE play_id = ? AND signal_trade_date = ?",
        (play_id, signal_trade_date),
    )
    if len(cards) > 1:
        raise ValueError("duplicate play card identity")
    return cards[0] if cards else None


def load_play_cards_through(
    storage: PlayCardStorage, play_id: str, cutoff_trade_date: str
) -> list[PlayCard]:
    """Load one play's immutable cards through a frozen market cutoff."""
    if not isinstance(play_id, str) or not play_id.strip():
        raise ValueError("play_id must be a non-empty string")
    try:
        date.fromisoformat(cutoff_trade_date)
    except (TypeError, ValueError) as exc:
        raise ValueError("cutoff_trade_date must be YYYY-MM-DD") from exc
    query = "WHERE play_id=? AND signal_trade_date<=? ORDER BY signal_trade_date,play_id"
    return _load_cards(storage, query, (play_id, cutoff_trade_date))


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
    return _load_cards(
        storage,
        f"""WHERE paper_status IN ('TRIGGERED', 'COMPLETED')
          {date_filter}
        ORDER BY signal_trade_date DESC, play_id ASC, play_name ASC
        LIMIT ?""",
        tuple(params),
    )
def load_pending_play_cards(
    storage: PlayCardStorage,
    play_id: str,
) -> list[PlayCard]:
    """Load pending cards for a programmatic lifecycle worker."""
    if not isinstance(play_id, str) or not play_id.strip():
        raise ValueError("play_id must be a non-empty string")
    return _load_cards(
        storage,
        """WHERE play_id = ?
          AND paper_status IN ('PAPER', 'PLANNED', 'TRIGGERED')
        ORDER BY signal_trade_date ASC, play_id ASC""",
        (play_id,),
    )


def _load_cards(
    storage: PlayCardStorage, clause: str, params: tuple = ()
) -> list[PlayCard]:
    rows = storage.execute(f"SELECT {_CARD_COLUMNS} FROM play_cards {clause}", params)
    return [_card_from_row(row) for row in rows]


def _card_values(card: PlayCard) -> tuple:
    return (
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
    )


def _cas_values(card: PlayCard) -> tuple:
    values = _card_values(card)
    return values[1:3] + values[4:]


def _card_from_row(row: dict[str, Any]) -> PlayCard:
    values = dict(row)
    values["candidates"] = json.loads(values.pop("candidates_json"))
    values["historical_evidence"] = json.loads(values.pop("historical_evidence_json"))
    card = PlayCard(**values)
    card.validate()
    return card
