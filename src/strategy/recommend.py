"""Compatibility entry point for the daily recommendation engine."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_ARCHIVED_PATH = _PROJECT_ROOT / ".archive" / "src" / "strategy" / "recommend.py"

if not _ARCHIVED_PATH.exists():
    raise ImportError(f"Archived recommendation implementation is missing: {_ARCHIVED_PATH}")

_spec = importlib.util.spec_from_file_location(__name__, _ARCHIVED_PATH)
if _spec is None or _spec.loader is None:
    raise ImportError(f"Cannot load recommendation implementation: {_ARCHIVED_PATH}")

_module = importlib.util.module_from_spec(_spec)
sys.modules[__name__] = _module
_spec.loader.exec_module(_module)
