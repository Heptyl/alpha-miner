"""Contracts for the append-only research evidence ledger."""

from __future__ import annotations

import hashlib
import inspect
import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import Barrier, Lock

import pytest

from src.data.snapshot_manifest import build_manifest, canonical_json, sidecar_path
from src.data.storage import Storage
from src.mining.research_ledger import (
    CandidateSpec,
    HoldoutReceipt,
    InvalidHoldoutReceipt,
    LedgerConflict,
    LedgerValidationError,
    LineageRetired,
    ResearchLedger,
)

FROZEN_AT = "2026-08-17T16:00:00+08:00"
FIXED_NOW = datetime(2026, 8, 17, 16, 0, tzinfo=timezone(timedelta(hours=8)))


def _setup_active(root: Path, close: float = 10) -> None:
    root.mkdir(parents=True, exist_ok=True)
    market = root / "alpha_miner.db"
    Storage(str(market)).init_db()
    connection = sqlite3.connect(market)
    connection.execute(
        "INSERT INTO daily_price(stock_code,trade_date,close) VALUES('000001','2026-08-17',?)",
        (close,),
    )
    connection.commit()
    connection.close()
    manifest = build_manifest(market, published_at="2026-08-17T08:00:00.000000+00:00")
    sidecar_path(market).write_text(canonical_json(manifest), encoding="utf-8")


@pytest.fixture
def ledger_db(tmp_path: Path) -> tuple[Path, ResearchLedger]:
    root = tmp_path / "data"
    _setup_active(root)
    db_path = root / "research_ledger.db"
    ledger = ResearchLedger(root, clock=lambda: FIXED_NOW)
    ledger.init_db()
    ledger.init_db()
    ledger.bind_active_market()
    return db_path, ledger


def _spec(**overrides: object) -> CandidateSpec:
    values: dict[str, object] = {
        "candidate_name": "attention diffusion",
        "experiment_type": "FACTOR",
        "code_text": "def compute(view, as_of): return {'score': 1}",
        "parameters": {"window": 20, "threshold": 2.0},
        "data_manifest": {
            "decision_start": "2026-01-01",
            "decision_end": "2026-08-01",
            "tables": ["daily_price"],
        },
        "cost_model": {"round_trip_bps": 20},
        "protocol": {"mode": "RETRO_DEVELOPMENT", "split": "80/20-frozen"},
        "parent_hashes": (),
    }
    values.update(overrides)
    return CandidateSpec(**values)  # type: ignore[arg-type]


def _freeze_ready(ledger: ResearchLedger, **overrides: object):
    candidate = ledger.freeze_candidate(_spec(**overrides))
    ledger.append_development_result(
        candidate.candidate_hash,
        {"fitness": 0.12, "status": "DEVELOPMENT_CANDIDATE"},
        f"dev:{candidate.candidate_hash}",
    )
    return candidate


def _count(db_path: Path, table: str) -> int:
    connection = sqlite3.connect(db_path)
    try:
        return int(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
    finally:
        connection.close()


def _direct_event(
    connection: sqlite3.Connection,
    candidate_hash: str,
    lineage_hash: str,
    event_type: str,
    key: str,
) -> None:
    payload_json = json.dumps(
        {"direct_contract_probe": key}, sort_keys=True, separators=(",", ":")
    )
    payload_hash = hashlib.sha256(payload_json.encode()).hexdigest()
    event_id = hashlib.sha256(f"event:{key}".encode()).hexdigest()
    connection.execute(
        """
        INSERT INTO research_evidence (
            event_id, idempotency_key, candidate_hash, lineage_hash,
            event_type, payload_json, payload_hash, recorded_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            event_id,
            key,
            candidate_hash,
            lineage_hash,
            event_type,
            payload_json,
            payload_hash,
            FROZEN_AT,
        ),
    )


def test_init_twice_creates_only_empty_ledger_tables(ledger_db, tmp_path):
    db_path, ledger = ledger_db
    (tmp_path / "candidate_pool.jsonl").write_text("legacy", encoding="utf-8")
    (tmp_path / "evolution_state.json").write_text("{}", encoding="utf-8")

    ledger.init_db()

    assert _count(db_path, "research_candidates") == 0
    assert _count(db_path, "research_evidence") == 0


def test_snapshot_registry_is_immutable_and_has_no_production_writer(ledger_db):
    db_path, ledger = ledger_db
    assert not hasattr(ledger, "register_dataset_snapshot")
    connection = sqlite3.connect(db_path)
    try:
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            connection.execute("UPDATE dataset_snapshots SET size_bytes=1")
        connection.rollback()
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            connection.execute("DELETE FROM dataset_snapshots")
    finally:
        connection.close()
    assert _count(db_path, "dataset_snapshots") == 1


def test_bound_hash_snapshot_does_not_drift_when_active_market_is_replaced(tmp_path):
    root = tmp_path / "data"
    _setup_active(root)
    ledger = ResearchLedger(root, clock=lambda: FIXED_NOW)
    ledger.init_db()
    bound = ledger.bind_active_market()
    frozen_bytes = bound.immutable_path.read_bytes()

    replacement = tmp_path / "replacement"
    _setup_active(replacement, close=11)
    (replacement / "alpha_miner.db").replace(root / "alpha_miner.db")
    sidecar_path(replacement / "alpha_miner.db").replace(
        sidecar_path(root / "alpha_miner.db")
    )

    assert bound.immutable_path.read_bytes() == frozen_bytes
    assert hashlib.sha256(frozen_bytes).hexdigest() == bound.source_snapshot_sha256


def test_unregistered_snapshot_rejects_api_and_direct_candidate_insert(ledger_db):
    db_path, ledger = ledger_db
    unbound = ResearchLedger(db_path.parent)
    with pytest.raises(LedgerValidationError, match="bind_active_market"):
        unbound.freeze_candidate(_spec())

    candidate = ledger.freeze_candidate(_spec())
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    try:
        row = connection.execute(
            "SELECT * FROM research_candidates WHERE candidate_hash=?",
            (candidate.candidate_hash,),
        ).fetchone()
        assert row is not None
        columns = tuple(row.keys())
        values = dict(row)
        values["candidate_hash"] = "c" * 64
        values["dataset_snapshot_hash"] = "d" * 64
        placeholders = ",".join("?" for _ in columns)
        with pytest.raises(sqlite3.IntegrityError, match="snapshot is not registered"):
            connection.execute(
                f"INSERT INTO research_candidates ({','.join(columns)}) "
                f"VALUES ({placeholders})",
                tuple(values[column] for column in columns),
            )
    finally:
        connection.close()
    assert _count(db_path, "research_candidates") == 1


def test_default_clock_records_current_aware_utc(tmp_path):
    root = tmp_path / "current"
    _setup_active(root)
    ledger = ResearchLedger(root)
    ledger.init_db()
    ledger.bind_active_market()
    before = datetime.now(timezone.utc)
    candidate = ledger.freeze_candidate(_spec())
    after = datetime.now(timezone.utc)

    recorded = datetime.fromisoformat(candidate.frozen_at)
    assert recorded.tzinfo == timezone.utc
    assert before <= recorded <= after


def test_injected_clock_is_deterministic_and_normalized_to_utc(tmp_path):
    root = tmp_path / "stepping"
    _setup_active(root)
    instants = iter(
        (
            FIXED_NOW,
            FIXED_NOW + timedelta(minutes=10),
            FIXED_NOW + timedelta(minutes=20),
        )
    )
    ledger = ResearchLedger(root, clock=lambda: next(instants))
    ledger.init_db()
    ledger.bind_active_market()
    candidate = ledger.freeze_candidate(_spec())
    event = ledger.append_development_result(
        candidate.candidate_hash, {"fitness": 0.1}, "dev-time"
    )

    assert candidate.frozen_at == "2026-08-17T08:10:00.000000+00:00"
    assert event.recorded_at == "2026-08-17T08:20:00.000000+00:00"


def test_naive_clock_and_public_time_overrides_are_rejected(tmp_path):
    root = tmp_path / "naive"
    _setup_active(root)
    initial = ResearchLedger(root, clock=lambda: FIXED_NOW)
    initial.init_db()
    initial.bind_active_market()
    ledger = ResearchLedger(root, clock=lambda: datetime(2026, 8, 17, 8, 0))
    ledger.bind_active_market()

    with pytest.raises(LedgerValidationError, match="aware datetime"):
        ledger.freeze_candidate(_spec())
    assert "frozen_at" not in CandidateSpec.__dataclass_fields__
    for method_name in (
        "append_development_result",
        "open_holdout",
        "append_holdout_result",
    ):
        parameters = inspect.signature(getattr(ResearchLedger, method_name)).parameters
        assert "recorded_at" not in parameters


def test_candidate_hash_is_canonical_and_name_is_not_semantic(ledger_db):
    db_path, ledger = ledger_db
    first = ledger.freeze_candidate(_spec())
    reordered = ledger.freeze_candidate(
        _spec(
            candidate_name="renamed for display",
            parameters={"threshold": 2.0, "window": 20},
            data_manifest={
                "tables": ["daily_price"],
                "decision_end": "2026-08-01",
                "decision_start": "2026-01-01",
            },
        )
    )

    assert reordered == first
    assert reordered.candidate_hash == first.candidate_hash
    assert reordered.candidate_name == "attention diffusion"
    assert json.loads(first.parameters_json) == {"threshold": 2.0, "window": 20}
    assert first.code_hash == hashlib.sha256(first.code_text.encode()).hexdigest()
    assert _count(db_path, "research_candidates") == 1


@pytest.mark.parametrize(
    ("field", "changed"),
    [
        ("experiment_type", "STRATEGY"),
        ("code_text", "def compute(view, as_of): return {'score': 2}"),
        ("parameters", {"window": 21, "threshold": 2.0}),
        ("data_manifest", {"decision_end": "2026-07-31", "tables": ["daily_price"]}),
        ("cost_model", {"round_trip_bps": 30}),
        ("protocol", {"mode": "RETRO_DEVELOPMENT", "split": "70/30-frozen"}),
    ],
)
def test_each_semantic_input_changes_candidate_hash(ledger_db, field, changed):
    _, ledger = ledger_db
    baseline = ledger.freeze_candidate(_spec())
    changed_candidate = ledger.freeze_candidate(_spec(**{field: changed}))
    assert changed_candidate.candidate_hash != baseline.candidate_hash


def test_parent_changes_hash_and_lineage_is_union(ledger_db):
    _, ledger = ledger_db
    parent_a = ledger.freeze_candidate(_spec(code_text="return 'a'"))
    parent_b = ledger.freeze_candidate(_spec(code_text="return 'b'"))
    child_a = ledger.freeze_candidate(
        _spec(code_text="return 'child'", parent_hashes=(parent_a.candidate_hash,))
    )
    child_b = ledger.freeze_candidate(
        _spec(code_text="return 'child'", parent_hashes=(parent_b.candidate_hash,))
    )
    crossover = ledger.freeze_candidate(
        _spec(
            code_text="return 'cross'",
            parent_hashes=(parent_b.candidate_hash, parent_a.candidate_hash),
        )
    )

    assert child_a.candidate_hash != child_b.candidate_hash
    assert child_a.lineage_roots == (parent_a.candidate_hash,)
    assert crossover.lineage_roots == tuple(
        sorted((parent_a.candidate_hash, parent_b.candidate_hash))
    )


def test_hashes_parents_and_self_cycles_fail_closed(ledger_db):
    _, ledger = ledger_db
    with pytest.raises(LedgerValidationError, match="parent candidate does not exist"):
        ledger.freeze_candidate(_spec(parent_hashes=("b" * 64,)))

    parent = ledger.freeze_candidate(_spec(code_text="return 'parent'"))
    child = ledger.freeze_candidate(
        _spec(code_text="return 'child'", parent_hashes=(parent.candidate_hash,))
    )
    with pytest.raises(LedgerValidationError, match="own parent"):
        ledger.freeze_candidate(
            replace(
                _spec(code_text="return 'child'", parent_hashes=(child.candidate_hash,)),
                expected_candidate_hash=child.candidate_hash,
            )
        )


def test_non_json_values_are_rejected(ledger_db):
    _, ledger = ledger_db
    with pytest.raises(LedgerValidationError, match="canonical JSON"):
        ledger.freeze_candidate(_spec(parameters={"bad": float("nan")}))


def test_development_evidence_is_idempotent_but_never_overwritten(ledger_db):
    db_path, ledger = ledger_db
    candidate = ledger.freeze_candidate(_spec())
    first = ledger.append_development_result(
        candidate.candidate_hash,
        {"b": 2, "a": 1},
        "dev-key",
    )
    repeated = ledger.append_development_result(
        candidate.candidate_hash,
        {"a": 1, "b": 2},
        "dev-key",
    )
    same_payload_new_key = ledger.append_development_result(
        candidate.candidate_hash, {"a": 1, "b": 2}, "another-key"
    )

    assert repeated == first
    assert same_payload_new_key == first
    with pytest.raises(LedgerConflict, match="idempotency key conflicts"):
        ledger.append_development_result(
            candidate.candidate_hash, {"a": 99}, "dev-key"
        )
    with pytest.raises(LedgerConflict, match="conflicting DEVELOPMENT_RESULT"):
        ledger.append_development_result(
            candidate.candidate_hash, {"a": 99}, "different-key"
        )
    assert _count(db_path, "research_evidence") == 1


def test_tables_reject_direct_update_and_delete(ledger_db):
    db_path, ledger = ledger_db
    candidate = _freeze_ready(ledger)
    connection = sqlite3.connect(db_path)
    try:
        for statement in (
            "UPDATE research_candidates SET candidate_name='x'",
            "DELETE FROM research_candidates",
            "UPDATE research_evidence SET payload_json='{}'",
            "DELETE FROM research_evidence",
        ):
            with pytest.raises(sqlite3.IntegrityError, match="append-only"):
                connection.execute(statement)
        connection.rollback()
        with pytest.raises(sqlite3.IntegrityError, match="does not exist"):
            connection.execute(
                """
                INSERT INTO research_evidence (
                    event_id, idempotency_key, candidate_hash, lineage_hash,
                    event_type, payload_json, payload_hash, recorded_at
                ) VALUES (?, ?, ?, ?, 'DEVELOPMENT_RESULT', '{}', ?, ?)
                """,
                ("c" * 64, "bad-parent", "d" * 64, "e" * 64, "f" * 64, FROZEN_AT),
            )
    finally:
        connection.close()
    assert ledger.load_candidate(candidate.candidate_hash) == candidate


def test_direct_sql_rejects_candidate_lineage_mismatch(ledger_db):
    db_path, ledger = ledger_db
    candidate = _freeze_ready(ledger)
    connection = sqlite3.connect(db_path)
    try:
        with pytest.raises(sqlite3.IntegrityError, match="lineage mismatch"):
            _direct_event(
                connection,
                candidate.candidate_hash,
                "b" * 64,
                "HOLDOUT_OPENED",
                "bad-lineage",
            )
    finally:
        connection.close()
    assert _count(db_path, "research_evidence") == 1


def test_direct_sql_holdout_requires_development(ledger_db):
    db_path, ledger = ledger_db
    candidate = ledger.freeze_candidate(_spec())
    connection = sqlite3.connect(db_path)
    try:
        with pytest.raises(sqlite3.IntegrityError, match="requires development"):
            _direct_event(
                connection,
                candidate.candidate_hash,
                candidate.lineage_hash,
                "HOLDOUT_OPENED",
                "no-development",
            )
    finally:
        connection.close()
    assert _count(db_path, "research_evidence") == 0


def test_direct_sql_holdout_result_requires_opened_event(ledger_db):
    db_path, ledger = ledger_db
    candidate = _freeze_ready(ledger)
    connection = sqlite3.connect(db_path)
    try:
        with pytest.raises(sqlite3.IntegrityError, match="requires opened event"):
            _direct_event(
                connection,
                candidate.candidate_hash,
                candidate.lineage_hash,
                "HOLDOUT_RESULT",
                "no-opened-event",
            )
    finally:
        connection.close()
    assert _count(db_path, "research_evidence") == 1


def test_direct_sql_development_rejects_retired_ancestor(ledger_db):
    db_path, ledger = ledger_db
    root = _freeze_ready(ledger, code_text="return 'retired-root'")
    child = ledger.freeze_candidate(
        _spec(code_text="return 'pre-frozen-child'", parent_hashes=(root.candidate_hash,))
    )
    ledger.open_holdout(root.candidate_hash, "open-retired-root")
    connection = sqlite3.connect(db_path)
    try:
        with pytest.raises(sqlite3.IntegrityError, match="retired lineage"):
            _direct_event(
                connection,
                child.candidate_hash,
                child.lineage_hash,
                "DEVELOPMENT_RESULT",
                "late-development",
            )
    finally:
        connection.close()
    assert ledger.load_lineage_state(child.candidate_hash).retired is True


def test_direct_sql_holdout_rejects_any_lineage_root_overlap(ledger_db):
    db_path, ledger = ledger_db
    shared_root = ledger.freeze_candidate(_spec(code_text="return 'shared-root'"))
    other_root = ledger.freeze_candidate(_spec(code_text="return 'other-root'"))
    first = _freeze_ready(
        ledger,
        code_text="return 'first-lineage'",
        parent_hashes=(shared_root.candidate_hash,),
    )
    overlapping = _freeze_ready(
        ledger,
        code_text="return 'overlapping-lineage'",
        parent_hashes=(shared_root.candidate_hash, other_root.candidate_hash),
    )
    assert first.lineage_hash != overlapping.lineage_hash
    ledger.open_holdout(first.candidate_hash, "open-first-lineage")
    connection = sqlite3.connect(db_path)
    try:
        with pytest.raises(sqlite3.IntegrityError, match="root has already been opened"):
            _direct_event(
                connection,
                overlapping.candidate_hash,
                overlapping.lineage_hash,
                "HOLDOUT_OPENED",
                "open-overlap-direct",
            )
    finally:
        connection.close()
    assert _count(db_path, "research_evidence") == 3


def test_holdout_is_committed_before_callback_and_failure_still_retires(ledger_db):
    db_path, ledger = ledger_db
    candidate = _freeze_ready(ledger)
    observed: list[str] = []

    def authorize(receipt: HoldoutReceipt) -> None:
        connection = sqlite3.connect(db_path)
        try:
            observed.extend(
                row[0]
                for row in connection.execute(
                    "SELECT event_type FROM research_evidence ORDER BY sequence_id"
                )
            )
        finally:
            connection.close()
        assert receipt.candidate_hash == candidate.candidate_hash
        raise RuntimeError("simulated evaluator crash")

    with pytest.raises(RuntimeError, match="evaluator crash"):
        ledger.open_holdout(candidate.candidate_hash, "open-key", authorize=authorize)

    assert observed == ["DEVELOPMENT_RESULT", "HOLDOUT_OPENED"]
    assert ledger.load_lineage_state(candidate.candidate_hash).retired is True
    with pytest.raises(LineageRetired):
        ledger.open_holdout(candidate.candidate_hash, "open-key")
    assert _count(db_path, "research_evidence") == 2


def test_retired_ancestor_blocks_mutation_and_crossover(ledger_db):
    _, ledger = ledger_db
    parent = _freeze_ready(ledger, code_text="return 'parent'")
    other = ledger.freeze_candidate(_spec(code_text="return 'other'"))
    ledger.open_holdout(parent.candidate_hash, "open-parent")

    with pytest.raises(LineageRetired):
        ledger.freeze_candidate(
            _spec(code_text="return 'mutation'", parent_hashes=(parent.candidate_hash,))
        )
    with pytest.raises(LineageRetired):
        ledger.freeze_candidate(
            _spec(
                code_text="return 'crossover'",
                parent_hashes=(parent.candidate_hash, other.candidate_hash),
            )
        )
    assert ledger.freeze_candidate(_spec(code_text="return 'parent'")) == parent


@pytest.mark.parametrize("same_candidate", [True, False])
def test_concurrent_overlapping_holdout_only_grants_once(ledger_db, same_candidate):
    db_path, ledger = ledger_db
    root = ledger.freeze_candidate(_spec(code_text="return 'root'"))
    first = _freeze_ready(
        ledger, code_text="return 'child-a'", parent_hashes=(root.candidate_hash,)
    )
    second = first
    if not same_candidate:
        other_root = ledger.freeze_candidate(_spec(code_text="return 'other-root'"))
        second = _freeze_ready(
            ledger,
            code_text="return 'child-b'",
            parent_hashes=(root.candidate_hash, other_root.candidate_hash),
        )
    barrier = Barrier(2)
    grants: list[str] = []
    guard = Lock()

    def attempt(candidate_hash: str, key: str) -> str:
        local = ResearchLedger(db_path.parent)
        local.bind_active_market()
        barrier.wait()

        def granted(_: HoldoutReceipt) -> str:
            with guard:
                grants.append(candidate_hash)
            return "granted"

        try:
            return str(local.open_holdout(candidate_hash, key, authorize=granted))
        except LineageRetired:
            return "retired"

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(
            pool.map(
                lambda pair: attempt(*pair),
                ((first.candidate_hash, "open-a"), (second.candidate_hash, "open-b")),
            )
        )

    assert sorted(results) == ["granted", "retired"]
    assert len(grants) == 1
    connection = sqlite3.connect(db_path)
    try:
        assert connection.execute(
            "SELECT COUNT(*) FROM research_evidence WHERE event_type='HOLDOUT_OPENED'"
        ).fetchone()[0] == 1
    finally:
        connection.close()


def test_holdout_result_requires_genuine_once_only_receipt(ledger_db):
    db_path, ledger = ledger_db
    candidate = _freeze_ready(ledger)
    receipt = ledger.open_holdout(candidate.candidate_hash, "open-key")
    assert isinstance(receipt, HoldoutReceipt)

    with pytest.raises(InvalidHoldoutReceipt):
        ledger.append_holdout_result(
            replace(receipt, token="forged"), {"mean": 0.01}, "result-forged"
        )
    first = ledger.append_holdout_result(
        receipt,
        {"mean": 0.01},
        "result-key",
    )
    repeated = ledger.append_holdout_result(
        receipt, {"mean": 0.01}, "result-key"
    )
    assert repeated == first
    with pytest.raises(InvalidHoldoutReceipt):
        ledger.append_holdout_result(
            replace(receipt, token="forged"), {"mean": 0.01}, "result-key"
        )
    with pytest.raises(InvalidHoldoutReceipt, match="already been consumed"):
        ledger.append_holdout_result(receipt, {"mean": 0.01}, "new-result-key")
    with pytest.raises(InvalidHoldoutReceipt):
        ledger.append_holdout_result(
            replace(receipt, opened_event_id="b" * 64),
            {"mean": 0.01},
            "missing-open",
        )
    assert _count(db_path, "research_evidence") == 3


def test_holdout_requires_development_and_no_raw_query_api(ledger_db):
    _, ledger = ledger_db
    candidate = ledger.freeze_candidate(_spec())
    with pytest.raises(LedgerValidationError, match="DEVELOPMENT_RESULT"):
        ledger.open_holdout(candidate.candidate_hash, "premature")

    forbidden = {
        "query",
        "query_range",
        "execute",
        "_get_conn",
        "holdout_query",
        "holdout_values",
    }
    assert forbidden.isdisjoint(dir(ledger))


def test_ledger_source_has_no_legacy_state_dependency_and_paths_are_fixed():
    source = (Path(__file__).parents[1] / "src/mining/research_ledger.py").read_text(
        encoding="utf-8"
    )
    forbidden_fragments = (
        "candidate_pool." + "jsonl",
        "evolution_state." + "json",
    )
    assert not any(fragment in source for fragment in forbidden_fragments)
    assert tuple(inspect.signature(ResearchLedger).parameters) == ("data_root", "clock")
    assert tuple(inspect.signature(ResearchLedger.bind_active_market).parameters) == (
        "self",
    )
