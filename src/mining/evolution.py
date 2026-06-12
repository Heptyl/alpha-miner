"""Compatibility entry point for the factor evolution engine.

The implementation was archived while the public CLI and tests still depended
on this module. Load that implementation under the original module name until
the feature is either formally retired or moved back into the active tree.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_ARCHIVED_PATH = _PROJECT_ROOT / ".archive" / "src" / "mining" / "evolution.py"

if not _ARCHIVED_PATH.exists():
    raise ImportError(f"Archived evolution implementation is missing: {_ARCHIVED_PATH}")

_spec = importlib.util.spec_from_file_location(__name__, _ARCHIVED_PATH)
if _spec is None or _spec.loader is None:
    raise ImportError(f"Cannot load evolution implementation: {_ARCHIVED_PATH}")

_module = importlib.util.module_from_spec(_spec)
sys.modules[__name__] = _module
_spec.loader.exec_module(_module)

# The archived file derives these paths from its own location.
_module.KB_PATH = _PROJECT_ROOT / "knowledge_base" / "theories.yaml"
_module.PROMPTS_DIR = _PROJECT_ROOT / "src" / "mining" / "prompts"
