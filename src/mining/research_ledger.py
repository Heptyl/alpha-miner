"""Append-only research candidate and evidence ledger.

This module deliberately does not expose market-data queries or a holdout
evaluator.  ``open_holdout`` commits the retirement fact before it hands a
single receipt to a caller-provided authorization callback.
"""

from __future__ import annotations

import hashlib
import json
import os
import secrets
import shutil
import sqlite3
import stat
import tempfile
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, TypeVar

from src.data.snapshot_manifest import (
    canonical_json,
    sha256_file,
    sidecar_path,
    validate_pair,
)

SCHEMA_VERSION = 1
EVENT_TYPES = frozenset(
    {"DEVELOPMENT_RESULT", "HOLDOUT_OPENED", "HOLDOUT_RESULT", "EVALUATION_ERROR"}
)
_HEX_DIGITS = frozenset("0123456789abcdef")
_GrantResult = TypeVar("_GrantResult")


class LedgerError(RuntimeError):
    """Base class for fail-closed research-ledger errors."""


class LedgerValidationError(LedgerError, ValueError):
    """Raised when a frozen artifact is incomplete or malformed."""


class LedgerConflict(LedgerError):
    """Raised when an idempotency key or immutable identity conflicts."""


class LineageRetired(LedgerError):
    """Raised when a holdout-opened lineage is reused or reopened."""


class InvalidHoldoutReceipt(LedgerError):
    """Raised for forged, mismatched, or already-consumed receipts."""


@dataclass(frozen=True)
class CandidateSpec:
    """All semantic inputs frozen before formal evidence is recorded."""

    candidate_name: str
    experiment_type: str
    code_text: str
    parameters: Mapping[str, Any]
    data_manifest: Mapping[str, Any]
    cost_model: Mapping[str, Any]
    protocol: Mapping[str, Any]
    parent_hashes: Sequence[str] = ()
    expected_candidate_hash: str | None = None


@dataclass(frozen=True)
class FrozenCandidate:
    candidate_hash: str
    candidate_name: str
    experiment_type: str
    code_text: str
    code_hash: str
    parameters_json: str
    parameters_hash: str
    data_manifest_json: str
    data_hash: str
    dataset_snapshot_hash: str
    cost_model_json: str
    cost_hash: str
    protocol_json: str
    protocol_hash: str
    parent_hashes: tuple[str, ...]
    lineage_roots: tuple[str, ...]
    lineage_hash: str
    frozen_at: str
    schema_version: int


@dataclass(frozen=True)
class EvidenceEvent:
    sequence_id: int
    event_id: str
    idempotency_key: str
    candidate_hash: str
    lineage_hash: str
    event_type: str
    holdout_scope_hash: str | None
    payload: dict[str, Any]
    payload_hash: str
    recorded_at: str


@dataclass(frozen=True)
class HoldoutReceipt:
    """One-use authority created only after HOLDOUT_OPENED is committed."""

    opened_event_id: str
    candidate_hash: str
    lineage_hash: str
    holdout_scope_hash: str
    token: str


@dataclass(frozen=True)
class LineageState:
    candidate_hash: str
    lineage_hash: str
    lineage_roots: tuple[str, ...]
    retired: bool
    event_types: tuple[str, ...]


@dataclass(frozen=True)
class BoundSnapshot:
    source_snapshot_sha256: str
    immutable_path: Path
    latest_trade_date: str


class ResearchLedger:
    """Trusted SQLite boundary for immutable candidates and evidence."""

    def __init__(
        self,
        data_root: str | Path,
        *,
        clock: Callable[[], datetime] | None = None,
    ):
        root = Path(data_root)
        root.mkdir(parents=True, exist_ok=True)
        if root.is_symlink():
            raise LedgerValidationError("data root must not be a symlink")
        self._data_root = root.resolve()
        self._db_path = str(self._data_root / "research_ledger.db")
        self._market_path = self._data_root / "alpha_miner.db"
        self._clock = clock or _utc_now
        self._bound: BoundSnapshot | None = None

    def init_db(self) -> None:
        schema = Path(__file__).with_name("research_ledger_schema.sql").read_text(encoding="utf-8")
        connection = self._connect()
        try:
            connection.executescript(schema)
            connection.commit()
        finally:
            connection.close()

    def bind_active_market(self) -> BoundSnapshot:
        if not Path(self._db_path).is_file():
            raise LedgerValidationError("research ledger must be initialized")
        if self._market_path.is_symlink() or sidecar_path(self._market_path).is_symlink():
            raise LedgerValidationError("active market paths must not be symlinks")
        manifest = validate_pair(self._market_path, sidecar_path(self._market_path))
        snapshot_dir = self._data_root / "research_snapshots"
        if snapshot_dir.is_symlink():
            raise LedgerValidationError("research snapshot directory must not be a symlink")
        snapshot_dir.mkdir(exist_ok=True)
        digest = str(manifest["source_snapshot_sha256"])
        target = snapshot_dir / f"{digest}.db"
        descriptor, name = tempfile.mkstemp(dir=snapshot_dir, suffix=".binding")
        os.close(descriptor)
        Path(name).unlink()
        try:
            shutil.copy2(self._market_path, name)
            copied = Path(name)
            if sha256_file(copied) != digest:
                raise LedgerValidationError("active market changed while binding")
            validate_pair(copied, sidecar_path(self._market_path))
            if target.exists():
                if target.is_symlink() or sha256_file(target) != digest:
                    raise LedgerValidationError("immutable snapshot conflicts with hash")
                copied.unlink()
            else:
                copied.replace(target)
            target.chmod(stat.S_IREAD)
            bound = BoundSnapshot(digest, target, str(manifest["latest_trade_date"]))
            self._register_bound_snapshot(bound, manifest)
            self._bound = bound
            return bound
        finally:
            if Path(name).exists():
                Path(name).unlink()

    def _register_bound_snapshot(self, bound: BoundSnapshot, manifest: Mapping[str, Any]) -> None:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute("SELECT manifest_json,size_bytes,latest_trade_date,published_at FROM dataset_snapshots WHERE source_snapshot_sha256=?", (bound.source_snapshot_sha256,)).fetchone()
            values = (canonical_json(manifest), manifest["size_bytes"], bound.latest_trade_date, manifest["published_at"])
            if row is not None and tuple(row) != values:
                raise LedgerConflict("verified snapshot registry conflict")
            if row is None:
                connection.execute("INSERT INTO dataset_snapshots VALUES (?,?,?,?,?,?,1)", (bound.source_snapshot_sha256, *values, self._now()))
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def freeze_candidate(self, spec: CandidateSpec) -> FrozenCandidate:
        """Insert one immutable candidate, or return an identical frozen row."""
        if self._bound is None:
            raise LedgerValidationError("bind_active_market is required before freeze")
        prepared = _prepare_spec(spec, self._now(), self._bound.source_snapshot_sha256)
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            candidate = self._freeze_candidate_locked(connection, spec, prepared)
            connection.commit()
            return candidate
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def append_development_result(
        self,
        candidate_hash: str,
        result: Mapping[str, Any],
        idempotency_key: str,
    ) -> EvidenceEvent:
        """Append the sole development result for a frozen candidate."""
        return self._append_result(
            candidate_hash,
            "DEVELOPMENT_RESULT",
            result,
            idempotency_key,
            reject_retired=True,
        )

    def open_holdout(
        self,
        candidate_hash: str,
        idempotency_key: str,
        *,
        authorize: Callable[[HoldoutReceipt], _GrantResult] | None = None,
    ) -> HoldoutReceipt | _GrantResult:
        """Retire a lineage, commit, then grant one receipt/callback invocation.

        Repeating this call never returns another receipt, including when the
        idempotency key is identical.  A callback exception does not undo the
        already committed retirement event.
        """
        candidate_hash = _validate_hash(candidate_hash, "candidate_hash")
        key = _validate_text(idempotency_key, "idempotency_key")
        timestamp = self._now()
        token = secrets.token_hex(32)
        payload = {"receipt_token_hash": _sha256_text(token), "schema_version": 1}
        candidate_scope: str | None = None
        payload_json = _canonical_json(payload, "holdout payload")
        payload_hash = _sha256_text(payload_json)

        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            candidate = self._load_candidate_locked(connection, candidate_hash)
            candidate_scope = _candidate_holdout_scope(candidate)
            if not self._has_event_locked(
                connection, candidate_hash, "DEVELOPMENT_RESULT"
            ):
                raise LedgerValidationError(
                    "holdout requires a frozen DEVELOPMENT_RESULT"
                )
            roots = set(candidate.lineage_roots)
            if self._retired_roots_locked(connection) & roots:
                raise LineageRetired("research lineage has already opened holdout")
            if self._event_by_key_locked(connection, key) is not None:
                raise LedgerConflict("idempotency key conflicts with existing evidence")
            if self._event_by_type_locked(
                connection, candidate_hash, "HOLDOUT_OPENED"
            ) is not None:
                raise LineageRetired("candidate holdout has already been opened")
            if connection.execute(
                "SELECT 1 FROM research_evidence WHERE event_type='HOLDOUT_OPENED' "
                "AND holdout_scope_hash=?",
                (candidate_scope,),
            ).fetchone():
                raise LineageRetired("holdout scope has already been opened")
            payload = {
                "holdout_scope_hash": candidate_scope,
                "receipt_token_hash": _sha256_text(token),
                "schema_version": 1,
            }
            payload_json = _canonical_json(payload, "holdout payload")
            payload_hash = _sha256_text(payload_json)
            event = self._insert_event_locked(
                connection,
                candidate,
                "HOLDOUT_OPENED",
                payload_json,
                payload_hash,
                key,
                timestamp,
                candidate_scope,
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

        receipt = HoldoutReceipt(
            opened_event_id=event.event_id,
            candidate_hash=candidate_hash,
            lineage_hash=candidate.lineage_hash,
            holdout_scope_hash=str(candidate_scope),
            token=token,
        )
        return authorize(receipt) if authorize is not None else receipt

    def append_holdout_result(
        self,
        receipt: HoldoutReceipt,
        result: Mapping[str, Any],
        idempotency_key: str,
    ) -> EvidenceEvent:
        return self._append_holdout_terminal(
            receipt, result, idempotency_key, "HOLDOUT_RESULT"
        )

    def append_evaluation_error(
        self,
        receipt: HoldoutReceipt,
        result: Mapping[str, Any],
        idempotency_key: str,
    ) -> EvidenceEvent:
        return self._append_holdout_terminal(
            receipt, result, idempotency_key, "EVALUATION_ERROR"
        )

    def _append_holdout_terminal(
        self,
        receipt: HoldoutReceipt,
        result: Mapping[str, Any],
        idempotency_key: str,
        event_type: str,
    ) -> EvidenceEvent:
        """Consume one genuine receipt with exactly one terminal event."""
        if not isinstance(receipt, HoldoutReceipt):
            raise InvalidHoldoutReceipt("receipt must be a HoldoutReceipt")
        _validate_hash(receipt.opened_event_id, "opened_event_id")
        candidate_hash = _validate_hash(receipt.candidate_hash, "candidate_hash")
        lineage_hash = _validate_hash(receipt.lineage_hash, "lineage_hash")
        scope_hash = _validate_hash(receipt.holdout_scope_hash, "holdout_scope_hash")
        token = _validate_text(receipt.token, "receipt token")
        key = _validate_text(idempotency_key, "idempotency_key")
        timestamp = self._now()
        result_payload = {
            "opened_event_id": receipt.opened_event_id,
            "result": _mapping_copy(result, "holdout result"),
        }
        payload_json = _canonical_json(result_payload, "holdout result")
        payload_hash = _sha256_text(payload_json)

        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            candidate = self._load_candidate_locked(connection, candidate_hash)
            opened_row = connection.execute(
                """
                SELECT * FROM research_evidence
                WHERE event_id = ? AND event_type = 'HOLDOUT_OPENED'
                """,
                (receipt.opened_event_id,),
            ).fetchone()
            if opened_row is None:
                raise InvalidHoldoutReceipt("receipt does not reference HOLDOUT_OPENED")
            opened = _event_from_row(opened_row)
            if (
                opened.candidate_hash != candidate_hash
                or opened.lineage_hash != lineage_hash
                or candidate.lineage_hash != lineage_hash
                or opened.holdout_scope_hash != scope_hash
                or _candidate_holdout_scope(candidate) != scope_hash
                or opened.payload.get("receipt_token_hash") != _sha256_text(token)
            ):
                raise InvalidHoldoutReceipt("receipt does not match the opened lineage")
            existing = self._event_by_key_locked(connection, key)
            if existing is not None:
                event = _event_from_row(existing)
                if _same_event(event, candidate_hash, event_type, payload_hash):
                    connection.commit()
                    return event
                raise LedgerConflict("idempotency key conflicts with existing evidence")
            prior = connection.execute(
                "SELECT * FROM research_evidence WHERE candidate_hash=? "
                "AND event_type IN ('HOLDOUT_RESULT','EVALUATION_ERROR')",
                (candidate_hash,),
            ).fetchone()
            if prior is not None:
                raise InvalidHoldoutReceipt("holdout receipt has already been consumed")
            event = self._insert_event_locked(
                connection,
                candidate,
                event_type,
                payload_json,
                payload_hash,
                key,
                timestamp,
            )
            connection.commit()
            return event
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def load_candidate(self, candidate_hash: str) -> FrozenCandidate:
        candidate_hash = _validate_hash(candidate_hash, "candidate_hash")
        connection = self._connect()
        try:
            return self._load_candidate_locked(connection, candidate_hash)
        finally:
            connection.close()

    def load_development_history(
        self,
        experiment_type: str,
        dataset_snapshot_hash: str,
    ) -> tuple[tuple[FrozenCandidate, EvidenceEvent | None], ...]:
        """Read frozen trials and optional development facts for exact resume."""
        experiment_type = _validate_text(experiment_type, "experiment_type")
        dataset_snapshot_hash = _validate_hash(
            dataset_snapshot_hash, "dataset_snapshot_hash"
        )
        connection = self._connect()
        try:
            _paper_feedback_receipts_locked(connection)
            rows = connection.execute(
                "SELECT candidate.candidate_hash frozen_candidate_hash,evidence.* "
                "FROM research_candidates candidate LEFT JOIN research_evidence evidence "
                "ON evidence.candidate_hash=candidate.candidate_hash "
                "AND evidence.event_type='DEVELOPMENT_RESULT' "
                "WHERE candidate.experiment_type=? AND candidate.dataset_snapshot_hash=? "
                "ORDER BY candidate.frozen_at,candidate.candidate_hash",
                (experiment_type, dataset_snapshot_hash),
            ).fetchall()
            return tuple(
                (self._load_candidate_locked(connection, row["frozen_candidate_hash"]),
                 _event_from_row(row) if row["event_type"] else None)
                for row in rows
            )
        finally:
            connection.close()

    def load_paper_feedback_consumption(self, exclude_snapshot_hash: str | None = None):
        """Load strictly validated, already committed PAPER consumption receipts."""
        connection = self._connect()
        try:
            consumed = {}
            for event, receipt in _paper_feedback_receipts_locked(connection, exclude_snapshot_hash):
                bucket = consumed.setdefault(receipt["execution_hash"],
                                             {"plan_hashes": set(), "event_ids": set()})
                bucket["plan_hashes"].update(receipt["plan_hashes"])
                bucket["event_ids"].add(event.event_id)
            return {key: {name: tuple(sorted(items)) for name, items in value.items()}
                    for key, value in consumed.items()}
        finally:
            connection.close()

    def load_lineage_state(self, candidate_hash: str) -> LineageState:
        candidate = self.load_candidate(candidate_hash)
        connection = self._connect()
        try:
            rows = connection.execute(
                """
                SELECT event_type FROM research_evidence
                WHERE candidate_hash = ? ORDER BY sequence_id
                """,
                (candidate.candidate_hash,),
            ).fetchall()
            retired = bool(
                set(candidate.lineage_roots) & self._retired_roots_locked(connection)
            )
        finally:
            connection.close()
        return LineageState(
            candidate_hash=candidate.candidate_hash,
            lineage_hash=candidate.lineage_hash,
            lineage_roots=candidate.lineage_roots,
            retired=retired,
            event_types=tuple(str(row["event_type"]) for row in rows),
        )

    def _append_result(
        self,
        candidate_hash: str,
        event_type: str,
        result: Mapping[str, Any],
        idempotency_key: str,
        reject_retired: bool,
    ) -> EvidenceEvent:
        candidate_hash = _validate_hash(candidate_hash, "candidate_hash")
        key = _validate_text(idempotency_key, "idempotency_key")
        timestamp = self._now()
        payload_json = _canonical_json(
            _mapping_copy(result, "evidence result"), "evidence result"
        )
        payload_hash = _sha256_text(payload_json)
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            candidate = self._load_candidate_locked(connection, candidate_hash)
            existing = self._event_by_key_locked(connection, key)
            if existing is not None:
                event = _event_from_row(existing)
                if _same_event(event, candidate_hash, event_type, payload_hash):
                    connection.commit()
                    return event
                raise LedgerConflict("idempotency key conflicts with existing evidence")
            if reject_retired and (
                set(candidate.lineage_roots) & self._retired_roots_locked(connection)
            ):
                raise LineageRetired("retired lineage cannot add development evidence")
            prior = self._event_by_type_locked(connection, candidate_hash, event_type)
            if prior is not None:
                prior_event = _event_from_row(prior)
                if prior_event.payload_hash == payload_hash:
                    connection.commit()
                    return prior_event
                raise LedgerConflict(
                    f"candidate already has conflicting {event_type} evidence"
                )
            if event_type == "DEVELOPMENT_RESULT":
                self._validate_paper_consumption_locked(connection, candidate,
                                                        json.loads(payload_json))
            event = self._insert_event_locked(
                connection,
                candidate,
                event_type,
                payload_json,
                payload_hash,
                key,
                timestamp,
            )
            connection.commit()
            return event
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    @staticmethod
    def _validate_paper_consumption_locked(connection, candidate, payload) -> None:
        incoming = _paper_feedback_receipt(payload)
        if incoming is None:
            return
        candidate_execution = json.loads(candidate.parameters_json).get("execution_hash")
        if candidate_execution != incoming["execution_hash"]:
            raise LedgerConflict("PAPER feedback execution differs from frozen candidate")
        if incoming.get("pending"):
            return
        for _event, prior in _paper_feedback_receipts_locked(connection):
            if set(incoming["plan_hashes"]) & set(prior["plan_hashes"]):
                raise LedgerConflict("PAPER feedback plan hash was already consumed")
            if (incoming["execution_hash"] == prior["execution_hash"] and
                    incoming["window_start"] <= prior["window_end"]):
                raise LedgerConflict("PAPER feedback receipt window overlaps or moves backwards")

    def _freeze_candidate_locked(
        self,
        connection: sqlite3.Connection,
        spec: CandidateSpec,
        prepared: dict[str, Any],
    ) -> FrozenCandidate:
        parents = tuple(prepared["parent_hashes"])
        if spec.expected_candidate_hash is not None:
            expected = _validate_hash(
                spec.expected_candidate_hash, "expected_candidate_hash"
            )
            if expected in parents:
                raise LedgerValidationError("candidate cannot be its own parent")
        parent_rows: list[FrozenCandidate] = []
        snapshot_exists = connection.execute(
            """
            SELECT 1 FROM dataset_snapshots
            WHERE source_snapshot_sha256 = ?
            """,
            (prepared["dataset_snapshot_hash"],),
        ).fetchone()
        if snapshot_exists is None:
            raise LedgerValidationError("dataset snapshot is not registered")
        for parent_hash in parents:
            row = connection.execute(
                "SELECT * FROM research_candidates WHERE candidate_hash = ?",
                (parent_hash,),
            ).fetchone()
            if row is None:
                raise LedgerValidationError(f"parent candidate does not exist: {parent_hash}")
            parent_rows.append(_candidate_from_row(row))

        semantic = {
            "schema_version": SCHEMA_VERSION,
            "experiment_type": prepared["experiment_type"],
            "code_hash": prepared["code_hash"],
            "parameters_hash": prepared["parameters_hash"],
            "data_hash": prepared["data_hash"],
            "dataset_snapshot_hash": prepared["dataset_snapshot_hash"],
            "cost_hash": prepared["cost_hash"],
            "protocol_hash": prepared["protocol_hash"],
            "parent_hashes": parents,
        }
        candidate_hash = _sha256_text(_canonical_json(semantic, "candidate identity"))
        if spec.expected_candidate_hash is not None and candidate_hash != expected:
            raise LedgerValidationError("expected_candidate_hash does not match semantics")
        if candidate_hash in parents:
            raise LedgerValidationError("candidate cannot be its own parent")

        roots = tuple(
            sorted(
                {root for parent in parent_rows for root in parent.lineage_roots}
                or {candidate_hash}
            )
        )
        lineage_hash = _sha256_text(_canonical_json(list(roots), "lineage roots"))
        existing = connection.execute(
            "SELECT * FROM research_candidates WHERE candidate_hash = ?",
            (candidate_hash,),
        ).fetchone()
        if existing is not None:
            frozen = _candidate_from_row(existing)
            if _candidate_semantics(frozen) != (
                prepared["experiment_type"],
                prepared["code_text"],
                prepared["parameters_json"],
                prepared["data_manifest_json"],
                prepared["dataset_snapshot_hash"],
                prepared["cost_model_json"],
                prepared["protocol_json"],
                parents,
                roots,
            ):
                raise LedgerConflict("candidate hash conflicts with immutable content")
            return frozen
        if set(roots) & self._retired_roots_locked(connection):
            raise LineageRetired("retired lineage cannot produce a new candidate")

        connection.execute(
            """
            INSERT INTO research_candidates (
                candidate_hash, candidate_name, experiment_type, code_text,
                code_hash, parameters_json, parameters_hash,
                data_manifest_json, data_hash, dataset_snapshot_hash,
                cost_model_json, cost_hash, protocol_json, protocol_hash,
                parent_hashes_json, lineage_roots_json, lineage_hash,
                frozen_at, schema_version
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                candidate_hash,
                prepared["candidate_name"],
                prepared["experiment_type"],
                prepared["code_text"],
                prepared["code_hash"],
                prepared["parameters_json"],
                prepared["parameters_hash"],
                prepared["data_manifest_json"],
                prepared["data_hash"],
                prepared["dataset_snapshot_hash"],
                prepared["cost_model_json"],
                prepared["cost_hash"],
                prepared["protocol_json"],
                prepared["protocol_hash"],
                _canonical_json(list(parents), "parent hashes"),
                _canonical_json(list(roots), "lineage roots"),
                lineage_hash,
                prepared["frozen_at"],
                SCHEMA_VERSION,
            ),
        )
        row = connection.execute(
            "SELECT * FROM research_candidates WHERE candidate_hash = ?",
            (candidate_hash,),
        ).fetchone()
        if row is None:  # pragma: no cover - SQLite insert contract
            raise LedgerError("candidate insert did not persist")
        return _candidate_from_row(row)

    def _insert_event_locked(
        self,
        connection: sqlite3.Connection,
        candidate: FrozenCandidate,
        event_type: str,
        payload_json: str,
        payload_hash: str,
        idempotency_key: str,
        recorded_at: str,
        holdout_scope_hash: str | None = None,
    ) -> EvidenceEvent:
        if event_type not in EVENT_TYPES:
            raise LedgerValidationError(f"unsupported event_type: {event_type}")
        identity = {
            "candidate_hash": candidate.candidate_hash,
            "event_type": event_type,
            "idempotency_key": idempotency_key,
            "payload_hash": payload_hash,
        }
        event_id = _sha256_text(_canonical_json(identity, "event identity"))
        connection.execute(
            """
            INSERT INTO research_evidence (
                event_id, idempotency_key, candidate_hash, lineage_hash,
                event_type, holdout_scope_hash, payload_json, payload_hash, recorded_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event_id,
                idempotency_key,
                candidate.candidate_hash,
                candidate.lineage_hash,
                event_type,
                holdout_scope_hash,
                payload_json,
                payload_hash,
                recorded_at,
            ),
        )
        row = connection.execute(
            "SELECT * FROM research_evidence WHERE event_id = ?", (event_id,)
        ).fetchone()
        if row is None:  # pragma: no cover - SQLite insert contract
            raise LedgerError("evidence insert did not persist")
        return _event_from_row(row)

    def _load_candidate_locked(
        self, connection: sqlite3.Connection, candidate_hash: str
    ) -> FrozenCandidate:
        row = connection.execute(
            "SELECT * FROM research_candidates WHERE candidate_hash = ?",
            (candidate_hash,),
        ).fetchone()
        if row is None:
            raise LedgerValidationError("candidate does not exist")
        return _candidate_from_row(row)

    @staticmethod
    def _event_by_key_locked(
        connection: sqlite3.Connection, idempotency_key: str
    ) -> sqlite3.Row | None:
        return connection.execute(
            "SELECT * FROM research_evidence WHERE idempotency_key = ?",
            (idempotency_key,),
        ).fetchone()

    @staticmethod
    def _event_by_type_locked(
        connection: sqlite3.Connection, candidate_hash: str, event_type: str
    ) -> sqlite3.Row | None:
        return connection.execute(
            """
            SELECT * FROM research_evidence
            WHERE candidate_hash = ? AND event_type = ?
            """,
            (candidate_hash, event_type),
        ).fetchone()

    @staticmethod
    def _has_event_locked(
        connection: sqlite3.Connection, candidate_hash: str, event_type: str
    ) -> bool:
        return (
            ResearchLedger._event_by_type_locked(
                connection, candidate_hash, event_type
            )
            is not None
        )

    @staticmethod
    def _retired_roots_locked(connection: sqlite3.Connection) -> set[str]:
        rows = connection.execute(
            """
            SELECT candidate.lineage_roots_json
            FROM research_evidence AS evidence
            JOIN research_candidates AS candidate
              ON candidate.candidate_hash = evidence.candidate_hash
            WHERE evidence.event_type = 'HOLDOUT_OPENED'
            """
        ).fetchall()
        roots: set[str] = set()
        for row in rows:
            value = json.loads(row["lineage_roots_json"])
            if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
                raise LedgerValidationError("stored lineage roots are invalid")
            roots.update(value)
        return roots

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self._db_path,
            timeout=30,
            isolation_level=None,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        return connection

    def _now(self) -> str:
        value = self._clock()
        if not isinstance(value, datetime) or value.tzinfo is None:
            raise LedgerValidationError("ledger clock must return an aware datetime")
        return value.astimezone(timezone.utc).isoformat(timespec="microseconds")


def _prepare_spec(spec: CandidateSpec, frozen_at: str, snapshot_hash: str) -> dict[str, Any]:
    if not isinstance(spec, CandidateSpec):
        raise LedgerValidationError("spec must be a CandidateSpec")
    candidate_name = _validate_text(spec.candidate_name, "candidate_name")
    experiment_type = _validate_text(spec.experiment_type, "experiment_type")
    code_text = _validate_text(spec.code_text, "code_text")
    parameters_json = _canonical_json(
        _mapping_copy(spec.parameters, "parameters"), "parameters"
    )
    data_manifest_json = _canonical_json(
        _mapping_copy(spec.data_manifest, "data_manifest"), "data_manifest"
    )
    cost_model_json = _canonical_json(
        _mapping_copy(spec.cost_model, "cost_model"), "cost_model"
    )
    protocol_json = _canonical_json(
        _mapping_copy(spec.protocol, "protocol"), "protocol"
    )
    parent_hashes = tuple(
        sorted({_validate_hash(value, "parent_hash") for value in spec.parent_hashes})
    )
    return {
        "candidate_name": candidate_name,
        "experiment_type": experiment_type,
        "code_text": code_text,
        "code_hash": _sha256_text(code_text),
        "parameters_json": parameters_json,
        "parameters_hash": _sha256_text(parameters_json),
        "data_manifest_json": data_manifest_json,
        "data_hash": _sha256_text(data_manifest_json),
        "dataset_snapshot_hash": _validate_hash(snapshot_hash, "dataset_snapshot_hash"),
        "cost_model_json": cost_model_json,
        "cost_hash": _sha256_text(cost_model_json),
        "protocol_json": protocol_json,
        "protocol_hash": _sha256_text(protocol_json),
        "parent_hashes": parent_hashes,
        "frozen_at": frozen_at,
    }


def _candidate_from_row(row: sqlite3.Row) -> FrozenCandidate:
    return FrozenCandidate(
        candidate_hash=str(row["candidate_hash"]),
        candidate_name=str(row["candidate_name"]),
        experiment_type=str(row["experiment_type"]),
        code_text=str(row["code_text"]),
        code_hash=str(row["code_hash"]),
        parameters_json=str(row["parameters_json"]),
        parameters_hash=str(row["parameters_hash"]),
        data_manifest_json=str(row["data_manifest_json"]),
        data_hash=str(row["data_hash"]),
        dataset_snapshot_hash=str(row["dataset_snapshot_hash"]),
        cost_model_json=str(row["cost_model_json"]),
        cost_hash=str(row["cost_hash"]),
        protocol_json=str(row["protocol_json"]),
        protocol_hash=str(row["protocol_hash"]),
        parent_hashes=tuple(json.loads(row["parent_hashes_json"])),
        lineage_roots=tuple(json.loads(row["lineage_roots_json"])),
        lineage_hash=str(row["lineage_hash"]),
        frozen_at=str(row["frozen_at"]),
        schema_version=int(row["schema_version"]),
    )


def _candidate_semantics(candidate: FrozenCandidate) -> tuple[Any, ...]:
    return (
        candidate.experiment_type,
        candidate.code_text,
        candidate.parameters_json,
        candidate.data_manifest_json,
        candidate.dataset_snapshot_hash,
        candidate.cost_model_json,
        candidate.protocol_json,
        candidate.parent_hashes,
        candidate.lineage_roots,
    )


def _event_from_row(row: sqlite3.Row) -> EvidenceEvent:
    payload = json.loads(row["payload_json"])
    if not isinstance(payload, dict):
        raise LedgerValidationError("stored evidence payload must be an object")
    return EvidenceEvent(
        sequence_id=int(row["sequence_id"]),
        event_id=str(row["event_id"]),
        idempotency_key=str(row["idempotency_key"]),
        candidate_hash=str(row["candidate_hash"]),
        lineage_hash=str(row["lineage_hash"]),
        event_type=str(row["event_type"]),
        holdout_scope_hash=(
            str(row["holdout_scope_hash"]) if row["holdout_scope_hash"] else None
        ),
        payload=payload,
        payload_hash=str(row["payload_hash"]),
        recorded_at=str(row["recorded_at"]),
    )


def _same_event(
    event: EvidenceEvent,
    candidate_hash: str,
    event_type: str,
    payload_hash: str,
) -> bool:
    return (
        event.candidate_hash == candidate_hash
        and event.event_type == event_type
        and event.payload_hash == payload_hash
    )


def _candidate_holdout_scope(candidate: FrozenCandidate) -> str:
    try:
        scope = json.loads(candidate.protocol_json)["holdout_scope_hash"]
    except (KeyError, TypeError, json.JSONDecodeError) as exc:
        raise LedgerValidationError("candidate protocol lacks holdout_scope_hash") from exc
    return _validate_hash(scope, "holdout_scope_hash")


def _paper_feedback_receipts_locked(connection, exclude_snapshot_hash=None):
    sql = ("SELECT evidence.*,candidate.dataset_snapshot_hash,candidate.parameters_json "
           "FROM research_evidence evidence "
           "JOIN research_candidates candidate USING(candidate_hash) "
           "WHERE event_type='DEVELOPMENT_RESULT' ORDER BY sequence_id")
    rows = connection.execute(sql).fetchall()
    receipts, used, last_end = [], set(), {}
    for row in rows:
        event = _event_from_row(row)
        receipt = _paper_feedback_receipt(event.payload)
        if receipt is None:
            continue
        execution = json.loads(row["parameters_json"]).get("execution_hash")
        if execution != receipt["execution_hash"]:
            raise LedgerConflict("stored PAPER feedback execution differs from candidate")
        if receipt.get("pending"):
            continue
        plans, execution = set(receipt["plan_hashes"]), receipt["execution_hash"]
        if plans & used or receipt["window_start"] <= last_end.get(execution, "0001-01-01"):
            raise LedgerConflict("stored PAPER feedback receipts overlap or move backwards")
        used.update(plans)
        last_end[execution] = receipt["window_end"]
        if exclude_snapshot_hash != row["dataset_snapshot_hash"]:
            receipts.append((event, receipt))
    return receipts


def _paper_feedback_receipt(payload: Mapping[str, Any]) -> dict[str, Any] | None:
    evolution = payload.get("evolution")
    feedback = evolution.get("forward_feedback") if isinstance(evolution, Mapping) else None
    if not isinstance(feedback, Mapping):
        return None
    state = feedback.get("consumption_state")
    if state not in {"PENDING_THRESHOLD_NOT_CONSUMED", "CONSUMED_ON_APPEND"}:
        raise LedgerValidationError("PAPER feedback consumption state is invalid")
    execution = _validate_hash(feedback.get("execution_hash"), "execution_hash")
    _validate_hash(feedback.get("content_hash"), "feedback content_hash")
    plan_hashes = tuple(feedback.get("plan_hashes", ()))
    if tuple(sorted(set(plan_hashes))) != plan_hashes:
        raise LedgerValidationError("PAPER plan hashes must be sorted and unique")
    for value in plan_hashes:
        _validate_hash(value, "consumed plan_hash")
    start, end = feedback.get("window_start"), feedback.get("window_end")
    if ((start is None) != (end is None) or start is not None and
            date.fromisoformat(start) > date.fromisoformat(end)):
        raise LedgerValidationError("PAPER feedback window is invalid")
    days = feedback.get("completed_signal_days")
    keys = ("execution_hash", "plan_hashes", "content_hash", "window_start",
            "window_end", "completed_signal_days")
    receipt_hash = _sha256_text(_canonical_json(
        {key: feedback.get(key) for key in keys}, "PAPER feedback receipt"))
    if feedback.get("consumption_receipt_hash") != receipt_hash:
        raise LedgerValidationError("PAPER feedback receipt hash is invalid")
    if state == "PENDING_THRESHOLD_NOT_CONSUMED":
        if (type(days) is not int or days >= 5 or feedback.get("adjustment") != 0
                or feedback.get("status") != "INSUFFICIENT"):
            raise LedgerValidationError("pending PAPER feedback state/sample is invalid")
        return {"execution_hash": execution, "pending": True}
    if not plan_hashes or start is None or type(days) is not int or days < 5:
        raise LedgerValidationError("consumed PAPER feedback window/sample is invalid")
    return {"execution_hash": execution, "plan_hashes": plan_hashes,
            "window_start": start, "window_end": end}


def _mapping_copy(value: Mapping[str, Any], field: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise LedgerValidationError(f"{field} must be an object")
    copied = dict(value)
    if not copied:
        raise LedgerValidationError(f"{field} must not be empty")
    return copied


def _canonical_json(value: Any, field: str) -> str:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise LedgerValidationError(f"{field} must contain canonical JSON values") from exc


def _validate_hash(value: Any, field: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or set(value) - _HEX_DIGITS
    ):
        raise LedgerValidationError(f"{field} must be 64 lowercase hex characters")
    return value


def _validate_text(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise LedgerValidationError(f"{field} must be a non-empty string")
    return value


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()
