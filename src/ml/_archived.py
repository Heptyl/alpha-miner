"""Load ML modules that remain public but currently live in ``.archive``."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


def load_archived_module(module_name: str, filename: str) -> None:
    project_root = Path(__file__).resolve().parents[2]
    archived_path = project_root / ".archive" / "src" / "ml" / filename
    if not archived_path.exists():
        raise ImportError(f"Archived ML implementation is missing: {archived_path}")

    spec = importlib.util.spec_from_file_location(module_name, archived_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load ML implementation: {archived_path}")

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
