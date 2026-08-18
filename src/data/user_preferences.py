"""Independent USER-owned watchlist preferences; never part of market facts."""

from __future__ import annotations

import re
import sqlite3
from datetime import datetime, timezone
from os.path import samefile
from pathlib import Path
from typing import Callable

SCHEMA_PATH = Path(__file__).with_name("user_preferences_schema.sql")
VALID_PREFIXES = ("00", "30", "60", "68", "4", "8", "9")
PREFERENCE_APP_ID = 1095585874
PREFERENCE_TABLES = {"watchlist", "watchlist_capture_status"}
RESERVED_NAMES = {
    "alpha_miner.db", "alpha_miner.previous.db", "research_ledger.db",
    "incoming.db", "working.db", "paper-incoming.db", "paper-working.db",
    "canonical-one.db",
}
DEFAULT_PROTECTED = (
    Path("data/alpha_miner.db"), Path("data/alpha_miner.previous.db"),
    Path("data/research_ledger.db"), Path("incoming/alpha_miner.db"),
)
class UserPreferenceError(ValueError):
    """Invalid personal preference input."""
def _user_tables(connection: sqlite3.Connection) -> set[str]:
    return {str(row[0]) for row in connection.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
    )}
def validate_stock_code(value: str) -> str:
    code = str(value).strip()
    if not re.fullmatch(r"\d{6}", code) or not code.startswith(VALID_PREFIXES):
        raise UserPreferenceError("股票代码必须是可识别的六位A股代码")
    return code
def validate_preferences_path(
    path: str | Path, *, forbidden_paths: tuple[str | Path, ...] = (),
) -> Path:
    """Reject market/research aliases and any existing non-preference SQLite."""
    target = Path(path).resolve()
    if target.name.lower() in RESERVED_NAMES:
        raise UserPreferenceError("偏好库路径不得指向market、ledger、incoming或working数据库")
    for forbidden in (*DEFAULT_PROTECTED, *forbidden_paths):
        other = Path(forbidden).resolve()
        try:
            alias = target == other or (target.exists() and other.exists() and samefile(target, other))
        except OSError:
            alias = target == other
        if alias:
            raise UserPreferenceError("偏好库路径与受保护数据库相同或互为别名")
    if not target.exists():
        return target
    if not target.is_file():
        raise UserPreferenceError("偏好库路径不是普通文件")
    try:
        with sqlite3.connect(target.as_uri() + "?mode=ro", uri=True) as connection:
            app_id = int(connection.execute("PRAGMA application_id").fetchone()[0])
            tables = _user_tables(connection)
    except sqlite3.DatabaseError as exc:
        raise UserPreferenceError("既有文件不是可信偏好专库") from exc
    if app_id != PREFERENCE_APP_ID or tables != PREFERENCE_TABLES:
        raise UserPreferenceError("既有SQLite缺少偏好专库身份或包含非偏好表")
    return target
def init_preferences(path: str | Path) -> Path:
    target = validate_preferences_path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(target)
    try:
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("BEGIN IMMEDIATE")
        app_id = int(connection.execute("PRAGMA application_id").fetchone()[0])
        tables = _user_tables(connection)
        if app_id == PREFERENCE_APP_ID and tables == PREFERENCE_TABLES:
            pass
        elif app_id or tables:
            raise UserPreferenceError("偏好库初始化期间路径被其他SQLite占用")
        else:
            for statement in SCHEMA_PATH.read_text(encoding="utf-8").split(";"):
                if statement.strip() and "foreign_keys" not in statement:
                    connection.execute(statement)
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()
    return validate_preferences_path(target)
def add_watch(
    path: str | Path,
    stock_code: str,
    *,
    clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
) -> bool:
    code = validate_stock_code(stock_code)
    observed = clock()
    if observed.tzinfo is None:
        raise UserPreferenceError("added_at时钟必须包含时区")
    target = init_preferences(path)
    with sqlite3.connect(target) as connection:
        cursor = connection.execute(
            "INSERT OR IGNORE INTO watchlist(stock_code,added_at) VALUES(?,?)",
            (code, observed.astimezone(timezone.utc).isoformat(timespec="seconds")),
        )
        return cursor.rowcount == 1
def remove_watch(path: str | Path, stock_code: str) -> bool:
    code = validate_stock_code(stock_code)
    target = validate_preferences_path(path)
    if not target.is_file():
        return False
    with sqlite3.connect(target) as connection:
        connection.execute("PRAGMA foreign_keys=ON")
        cursor = connection.execute("DELETE FROM watchlist WHERE stock_code=?", (code,))
        return cursor.rowcount == 1
def load_watchlist(path: str | Path) -> tuple[tuple[str, str], ...]:
    target = validate_preferences_path(path)
    if not target.is_file():
        return ()
    with sqlite3.connect(target.as_uri() + "?mode=ro", uri=True) as connection:
        return tuple((str(code), str(added_at)) for code, added_at in connection.execute(
            "SELECT stock_code,added_at FROM watchlist ORDER BY stock_code"
        ))
def record_capture_status(
    path: str | Path, stock_code: str, status: str, *, attempts: int,
    bars_count: int, attempted_at: str, error: str | None = None,
) -> None:
    if status not in {"SUCCESS", "ERROR", "CONFLICT"}:
        raise UserPreferenceError("非法自选5分钟采集状态")
    target = validate_preferences_path(path)
    if not target.is_file():
        raise UserPreferenceError("偏好专库不存在")
    with sqlite3.connect(target) as connection:
        connection.execute(
            """INSERT INTO watchlist_capture_status
               (stock_code,status,attempts,bars_count,last_attempt_at,last_error)
               SELECT stock_code,?,?,?,?,? FROM watchlist WHERE stock_code=?
               ON CONFLICT(stock_code) DO UPDATE SET status=excluded.status,
               attempts=watchlist_capture_status.attempts+excluded.attempts,
               bars_count=excluded.bars_count,last_attempt_at=excluded.last_attempt_at,
               last_error=excluded.last_error""",
            (status, attempts, bars_count, attempted_at, error, stock_code),
        )
