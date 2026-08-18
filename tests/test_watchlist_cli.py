"""USER watchlist isolation and root CLI contracts."""

from __future__ import annotations

import hashlib
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

import src.data.user_preferences as preferences_module
from src.data.storage import Storage
from src.data.user_preferences import (
    UserPreferenceError,
    add_watch,
    record_capture_status,
    validate_preferences_path,
)

REPO = Path(__file__).resolve().parents[1]
def _fingerprint(path: Path) -> tuple[str, int]:
    return hashlib.sha256(path.read_bytes()).hexdigest(), path.stat().st_mtime_ns
def _run(repo: Path, preferences: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "cli", "watch", "--db", str(preferences), *args],
        cwd=repo, capture_output=True, text=True, encoding="utf-8", timeout=10, check=False,
    )
def test_root_watch_add_list_remove_is_idempotent_and_market_is_unchanged(tmp_path):
    market = tmp_path / "market.db"
    Storage(str(market)).init_db()
    before = _fingerprint(market)
    preferences = tmp_path / "personal" / "user_preferences.db"
    added = _run(REPO, preferences, "add", "000735")
    repeated = _run(REPO, preferences, "add", "000735")
    listed = _run(REPO, preferences, "list")
    assert added.returncode == repeated.returncode == listed.returncode == 0
    assert "已添加：000735" in added.stdout and "已存在：000735" in repeated.stdout
    assert "000735" in listed.stdout

    record_capture_status(preferences, "000735", "SUCCESS", attempts=1, bars_count=2,
                          attempted_at="2026-08-18T18:00:00+08:00")
    assert _run(REPO, preferences, "remove", "000735").returncode == 0
    assert "自选为空" in _run(REPO, preferences, "list").stdout
    connection = sqlite3.connect(preferences)
    assert connection.execute("SELECT COUNT(*) FROM watchlist_capture_status").fetchone()[0] == 0
    connection.close()
    assert _fingerprint(market) == before
def test_watch_rejects_invalid_code_and_missing_list_is_read_only(tmp_path):
    preferences = tmp_path / "missing.db"
    result = _run(REPO, preferences, "add", "123")
    assert result.returncode != 0
    assert "六位A股代码" in (result.stdout + result.stderr)
    assert "自选为空" in _run(REPO, preferences, "list").stdout
    assert not preferences.exists()
def test_default_root_output_contract_does_not_advertise_or_run_watch(tmp_path):
    result = subprocess.run(
        [sys.executable, "-m", "cli", "--help"], cwd=tmp_path,
        env={"PYTHONPATH": str(REPO)}, capture_output=True, text=True,
        encoding="utf-8", timeout=10, check=False,
    )
    assert result.returncode == 0
    assert "play" in result.stdout and "watch" not in result.stdout
    assert not (tmp_path / "data" / "user_preferences.db").exists()
def test_watchlist_has_no_research_selection_dependency():
    for relative in ("evolution.py", "experiments.py", "behavior_state.py", "plays.py"):
        text = (REPO / "src" / "mining" / relative).read_text(encoding="utf-8")
        assert "user_preferences" not in text and "watchlist_minutes" not in text
def test_market_aliases_and_reserved_work_paths_fail_before_any_write(tmp_path, monkeypatch):
    market = tmp_path / "alpha_miner.db"
    Storage(str(market)).init_db()
    before = _fingerprint(market)
    direct = _run(REPO, market, "add", "000735")
    assert direct.returncode != 0 and _fingerprint(market) == before
    alias = tmp_path / "personal-alias.db"
    alias.hardlink_to(market)
    monkeypatch.setattr(preferences_module, "DEFAULT_PROTECTED", (market,))
    monkeypatch.setattr(preferences_module.sqlite3, "connect",
                        lambda *_a, **_k: pytest.fail("alias must fail before SQLite connection"))
    with pytest.raises(UserPreferenceError, match="别名"):
        validate_preferences_path(alias)
    symlink = tmp_path / "personal-link.db"
    real_resolve = Path.resolve
    monkeypatch.setattr(Path, "resolve",
                        lambda self: market if self == symlink else real_resolve(self))
    with pytest.raises(UserPreferenceError):
        validate_preferences_path(symlink)
    for name in ("research_ledger.db", "incoming.db", "working.db"):
        target = tmp_path / name
        with pytest.raises(UserPreferenceError):
            validate_preferences_path(target)
        assert not target.exists()
def test_existing_unidentified_or_mixed_sqlite_is_never_upgraded(tmp_path):
    unidentified = tmp_path / "unidentified.db"
    sqlite3.connect(unidentified).close()
    before = _fingerprint(unidentified)
    with pytest.raises(UserPreferenceError, match="身份"):
        add_watch(unidentified, "000735")
    assert _fingerprint(unidentified) == before
    valid = tmp_path / "valid.db"
    add_watch(valid, "000735")
    connection = sqlite3.connect(valid)
    connection.execute("CREATE TABLE foreign_fact(x)")
    connection.commit()
    connection.close()
    with pytest.raises(UserPreferenceError, match="非偏好表"):
        add_watch(valid, "600613")
def test_initialization_rechecks_identity_and_tables_inside_write_lock(tmp_path, monkeypatch):
    target = tmp_path / "raced.db"
    real_connect = sqlite3.connect
    def racing_connect(path, *args, **kwargs):
        connection = real_connect(path, *args, **kwargs)
        with real_connect(path) as other:
            other.execute("CREATE TABLE external_fact(value TEXT)")
        return connection
    monkeypatch.setattr(preferences_module.sqlite3, "connect", racing_connect)
    with pytest.raises(UserPreferenceError, match="占用"):
        preferences_module.init_preferences(target)
    with real_connect(target) as connection:
        assert connection.execute("PRAGMA application_id").fetchone()[0] == 0
        tables = {row[0] for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")}
    assert tables == {"external_fact"}
