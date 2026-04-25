"""
Candidate factor buffer pool for the evolution engine v2.

Accepted factors don't go straight to production — they spend 5 observation
days in the pool first.  Each daily check result is appended as a JSONL line.
The last entry per candidate name is treated as the current state.

States
------
- pending  : in the pool, accumulating daily checks
- promoted : passed 5 daily checks → ready for production
- rejected : failed 3 consecutive daily checks → discarded
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any


class CandidatePool:
    """JSONL-backed candidate factor buffer pool."""

    # Promotion / rejection thresholds
    _PROMOTION_THRESHOLD: int = 5
    _REJECTION_THRESHOLD: int = 3

    def __init__(self, pool_path: str = "data/candidate_pool.jsonl") -> None:
        self._pool_path = Path(pool_path)
        self._pool_path.parent.mkdir(parents=True, exist_ok=True)
        # In-memory state keyed by candidate name; last entry per name wins.
        self._state: dict[str, dict[str, Any]] = {}
        self._load()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def add_candidate(self, name: str, config: dict, code: str) -> None:
        """Add a new candidate with status *pending* and zeroed counters."""
        if name in self._state:
            raise ValueError(f"Candidate '{name}' already exists in the pool")

        entry: dict[str, Any] = {
            "name": name,
            "config": config,
            "code": code,
            "status": "pending",
            "entry_date": date.today().isoformat(),
            "check_date": date.today().isoformat(),
            "daily_ic": 0.0,
            "days_checked": 0,
            "days_passed": 0,
            "days_failed": 0,
        }
        self._state[name] = entry
        self._save_entry(entry)

    def get_pending(self) -> list[dict]:
        """Return all candidates with status ``pending``."""
        return [v for v in self._state.values() if v["status"] == "pending"]

    def update_candidate(self, name: str, daily_ic: float, passed: bool) -> str:
        """Update a candidate with a daily verification result.

        Returns the new status string (``"pending"`` | ``"promoted"`` | ``"rejected"``).
        """
        if name not in self._state:
            raise KeyError(f"Candidate '{name}' not found in the pool")

        entry = self._state[name]
        entry["daily_ic"] = daily_ic
        entry["days_checked"] += 1
        entry["check_date"] = date.today().isoformat()

        if passed:
            entry["days_passed"] += 1
            entry["days_failed"] = 0
            if entry["days_passed"] >= self._PROMOTION_THRESHOLD:
                entry["status"] = "promoted"
        else:
            entry["days_failed"] += 1
            entry["days_passed"] = 0
            if entry["days_failed"] >= self._REJECTION_THRESHOLD:
                entry["status"] = "rejected"

        self._state[name] = entry
        self._save_entry(entry)
        return entry["status"]

    def get_promoted(self) -> list[dict]:
        """Return all promoted candidates."""
        return [v for v in self._state.values() if v["status"] == "promoted"]

    def get_status(self, name: str) -> dict | None:
        """Get the current status dict of a candidate, or ``None``."""
        return self._state.get(name)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _load(self) -> None:
        """Read the JSONL file and reconstruct in-memory state.

        The last entry for each candidate name wins.
        """
        if not self._pool_path.exists():
            return

        with open(self._pool_path, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                except json.JSONDecodeError:
                    continue
                candidate_name = entry.get("name")
                if candidate_name is not None:
                    self._state[candidate_name] = entry

    def _save_entry(self, entry: dict[str, Any]) -> None:
        """Append a single JSONL line for *entry*."""
        with open(self._pool_path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(entry, ensure_ascii=False) + "\n")
