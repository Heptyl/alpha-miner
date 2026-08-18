"""Opt-in after-close orchestration over replaceable canonical market snapshots."""
from __future__ import annotations

import os
import shutil
import tempfile
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from scripts.activate_data import ActivationConflict, activate_snapshot, activation_lock
from scripts.publish_data import publish_snapshot
from src.data.snapshot_manifest import sidecar_path, validate_pair
from src.data.storage import Storage
from src.data.user_preferences import validate_preferences_path
from src.data.watchlist_minutes import capture_watchlist_minutes
from src.mining.evolution import EvolutionEngine
from src.mining.experiments import FrozenPlayProjection
from src.mining.playbook import save_play_card
from src.mining.plays import (
    build_attention_reacceleration_card,
    load_usable_audit_dates,
    settle_attention_reacceleration_cards,
    settle_theme_new_entrant_diffusion_cards,
    settle_three_to_four_cards,
)

GENERATIONS, POPULATION, WORKERS = 6, 12, 4
@dataclass(frozen=True)
class AfterCloseResult:
    collection_status: str
    collected_rows: int
    canonical_snapshot_hash: str | None
    paper_generated: bool
    projection: FrozenPlayProjection | None = None
    warning: str | None = None
    watch_capture_status: str | None = None
def _copy_canonical(active: Path, working: Path, *, keep_sidecar=False) -> dict[str, object]:
    manifest = validate_pair(active, sidecar_path(active))
    shutil.copy2(active, working)
    working_sidecar = sidecar_path(working)
    shutil.copy2(sidecar_path(active), working_sidecar)
    if validate_pair(working, working_sidecar) != manifest:
        raise ActivationConflict("working market copy differs from canonical identity")
    if not keep_sidecar:
        working_sidecar.unlink()
    return manifest
def _settle_existing(storage: Storage) -> None:
    for settle in (settle_three_to_four_cards, settle_theme_new_entrant_diffusion_cards, settle_attention_reacceleration_cards):
        for card in settle(storage):
            save_play_card(storage, card)
def _develop(active: Path, temporary: Path) -> FrozenPlayProjection:
    engine = EvolutionEngine(
        db_path=str(active), api_client=None,
        mining_log_path=str(temporary / "mining_log.jsonl"), state_path=str(temporary / "evolution_state.json"))
    engine.run(GENERATIONS, POPULATION, True, WORKERS)
    projections = engine.ranked_play_projections(1)
    if not projections:
        raise RuntimeError("development produced no frozen play projection")
    return projections[0]
def _restore_pair(active: Path, stable: Path) -> None:
    staged = active.with_suffix(active.suffix + ".restore")
    staged_sidecar = sidecar_path(staged)
    shutil.copy2(stable, staged)
    shutil.copy2(sidecar_path(stable), staged_sidecar)
    os.replace(staged, active)
    os.replace(staged_sidecar, sidecar_path(active))
    validate_pair(active, sidecar_path(active))
def run_after_close(active_db: str | Path, trade_date: str,
    collect: Callable[[str, Storage], tuple[dict[str, int], object]],
    *, decision_clock: Callable[[], datetime] | None = None,
    preferences_db: str | Path | None = None,
) -> AfterCloseResult:
    """Collect into working storage, then publish research and PAPER in two stages."""
    active = Path(active_db).resolve()
    previous = active.with_name("alpha_miner.previous.db")
    preferences = validate_preferences_path(
        preferences_db or active.parent / "user_preferences.db",
        forbidden_paths=(active, previous, active.parent / "research_ledger.db"),
    )
    with activation_lock(active), tempfile.TemporaryDirectory(prefix="alpha-miner-after-close-") as name:
        temporary = Path(name)
        working, incoming = temporary / "working.db", temporary / "incoming.db"
        initial = _copy_canonical(active, working)
        storage = Storage(str(working))
        counts, check = collect(trade_date, storage)
        if check.status != "ok":
            raise RuntimeError(f"collection audit is not usable: {check.status}")
        if trade_date not in load_usable_audit_dates(storage):
            raise RuntimeError("collection audit is not a usable post-close audit")
        _settle_existing(storage)
        try:
            watch_status = capture_watchlist_minutes(
                working, preferences
            ).status
        except Exception as exc:
            watch_status = f"ERROR:{type(exc).__name__}:{exc}"
        publish_snapshot(working, incoming)
        canonical_one = activate_snapshot(
            incoming, active, previous,
            expected_current_hash=str(initial["source_snapshot_sha256"]), _lock_held=True)
        canonical_one_hash = str(canonical_one["source_snapshot_sha256"])
        stable = temporary / "canonical-one.db"
        if _copy_canonical(active, stable, keep_sidecar=True)["source_snapshot_sha256"] != canonical_one_hash:
            raise ActivationConflict("canonical#1 backup identity changed")
        projection = None
        try:
            projection = _develop(active, temporary)
            if projection.dataset_snapshot_hash != canonical_one_hash:
                raise ValueError("rank-1 projection is not bound to canonical#1")
            working_two, incoming_two = temporary / "paper-working.db", temporary / "paper-incoming.db"
            copied = _copy_canonical(active, working_two)
            if copied["source_snapshot_sha256"] != canonical_one_hash:
                raise ActivationConflict("PAPER working copy is not canonical#1")
            paper_storage = Storage(str(working_two))
            card = build_attention_reacceleration_card(
                paper_storage, signal_date=trade_date,
                generated_at=(decision_clock or (lambda: datetime.now(ZoneInfo("Asia/Shanghai"))))(),
                frozen_projection=projection)
            save_play_card(paper_storage, card)
            publish_snapshot(working_two, incoming_two)
            try:
                canonical_two = activate_snapshot(
                    incoming_two, active, previous,
                    expected_current_hash=canonical_one_hash, _lock_held=True)
            except Exception as exc:
                if not isinstance(exc, ActivationConflict):
                    _restore_pair(active, stable)
                raise
        except ActivationConflict:
            raise
        except Exception as exc:
            validate_pair(active, sidecar_path(active))
            return AfterCloseResult(
                check.status, sum(counts.values()), canonical_one_hash, False,
                projection=projection, warning=f"PAPER_NOT_GENERATED:{type(exc).__name__}:{exc}",
                watch_capture_status=watch_status)
        return AfterCloseResult(
            check.status, sum(counts.values()), str(canonical_two["source_snapshot_sha256"]),
            True, projection, watch_capture_status=watch_status)
