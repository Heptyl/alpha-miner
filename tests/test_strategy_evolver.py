"""Retirement contract for the untrusted parallel strategy evolver."""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_strategy_evolver_module_is_retired_without_migrating_results():
    assert not (ROOT / "src/strategy/evolver.py").exists()


def test_unified_engine_does_not_import_retired_parallel_paths():
    source = (ROOT / "src/mining/evolution.py").read_text(encoding="utf-8")
    assert "src.strategy.evolver" not in source
    assert "BacktestEngine" not in source
