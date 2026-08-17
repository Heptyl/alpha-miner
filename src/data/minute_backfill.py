"""Recoverable RETRO_BACKFILL of Sina RAW five-minute bars for frozen candidates."""

from __future__ import annotations

import hashlib
import json
import math
import random
import sqlite3
import time
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Protocol
from zoneinfo import ZoneInfo

from src.data.sources.sina_minutes import (
    ADJUST,
    PERIOD,
    SOURCE_NAME,
    MinuteBar,
    SinaMinuteError,
    fetch_raw_5m,
)

MAX_STOCKS_PER_RUN = 120
MAX_RETRIES = 2
SHANGHAI = ZoneInfo("Asia/Shanghai")


class MinuteStorage(Protocol):
    db_path: str

    def execute(self, sql: str, params: tuple = ()) -> list[dict[str, Any]]: ...


class MinuteBackfillError(RuntimeError):
    """Raised for a frozen-universe or immutable-bar contract violation."""


class MinuteBarConflictError(MinuteBackfillError):
    """Raised when an immutable bar key reappears with a different payload."""


@dataclass(frozen=True)
class FrozenUniverse:
    candidate_trade_date: str
    universe_hash: str
    stock_codes: tuple[str, ...]


@dataclass(frozen=True)
class MinuteBackfillResult:
    candidate_trade_date: str
    universe_hash: str
    total_items: int
    selected_items: int
    successful_items: int
    failed_items: int
    already_successful: int
    status: str
    circuit_reason: str | None = None
    conflict_items: int = 0


@dataclass(frozen=True)
class MinuteStatus:
    candidate_trade_date: str | None
    universe_hash: str | None
    total_items: int
    pending_items: int
    successful_items: int
    error_items: int
    conflict_items: int
    bars_count: int
    complete_days: int
    partial_days: int
    last_attempt_at: str | None


Fetcher = Callable[[str], list[MinuteBar]]


def freeze_latest_minute_universe(
    storage: MinuteStorage,
    *,
    frozen_at: datetime | None = None,
) -> FrozenUniverse:
    """Freeze the latest audited zt_pool set once; a changed repeat is rejected."""
    audit_rows = storage.execute(
        """
        SELECT MAX(run.trade_date) AS trade_date
        FROM limit_up_collection_runs AS run
        WHERE run.status = 'ok'
          AND EXISTS (
              SELECT 1 FROM zt_pool AS zt WHERE zt.trade_date = run.trade_date
          )
        """
    )
    candidate_date = audit_rows[0].get("trade_date") if audit_rows else None
    if not candidate_date:
        raise MinuteBackfillError("没有成功审计且含涨停候选的交易日")

    rows = storage.execute(
        "SELECT * FROM zt_pool WHERE trade_date = ? ORDER BY stock_code, snapshot_time",
        (candidate_date,),
    )
    latest: dict[str, dict[str, Any]] = {}
    for row in rows:
        code = str(row.get("stock_code") or "")
        if not code:
            continue
        previous = latest.get(code)
        if previous is None or str(row.get("snapshot_time") or "") >= str(
            previous.get("snapshot_time") or ""
        ):
            latest[code] = row
    if not latest:
        raise MinuteBackfillError("最新成功审计日没有可冻结候选")

    codes = tuple(sorted(latest))
    universe_hash = _hash_json(
        {"candidate_trade_date": str(candidate_date), "stock_codes": codes}
    )
    frozen_time = _as_shanghai_time(frozen_at)
    if str(candidate_date) > frozen_time.date().isoformat():
        raise MinuteBackfillError(
            "候选交易日晚于冻结时间的本地日期，禁止污染冻结集合"
        )
    frozen_text = frozen_time.isoformat(timespec="seconds")
    connection = _write_connection(storage.db_path)
    try:
        connection.execute("BEGIN IMMEDIATE")
        existing = connection.execute(
            """
            SELECT stock_code, universe_hash
            FROM minute_capture_items
            WHERE candidate_trade_date = ?
            ORDER BY stock_code
            """,
            (candidate_date,),
        ).fetchall()
        if existing:
            existing_codes = tuple(str(row["stock_code"]) for row in existing)
            existing_hashes = {str(row["universe_hash"]) for row in existing}
            if existing_codes != codes or existing_hashes != {universe_hash}:
                raise MinuteBackfillError(
                    "已冻结候选集合与当前最新快照不同，禁止悄悄变更"
                )
        else:
            for code in codes:
                row = latest[code]
                connection.execute(
                    """
                    INSERT INTO minute_capture_items (
                        candidate_trade_date, stock_code, stock_name, features_json,
                        universe_hash, frozen_at, status
                    ) VALUES (?, ?, ?, ?, ?, ?, 'PENDING')
                    """,
                    (
                        candidate_date,
                        code,
                        str(row.get("name") or ""),
                        _features_json(row),
                        universe_hash,
                        frozen_text,
                    ),
                )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()
    return FrozenUniverse(str(candidate_date), universe_hash, codes)


def backfill_latest_minutes(
    storage: MinuteStorage,
    *,
    max_stocks: int = MAX_STOCKS_PER_RUN,
    fetcher: Fetcher = fetch_raw_5m,
    sleeper: Callable[[float], None] = time.sleep,
    delay_seconds: Callable[[], float] = lambda: random.uniform(1.5, 2.5),
    now_fn: Callable[[], datetime] = lambda: datetime.now(SHANGHAI),
) -> MinuteBackfillResult:
    """Resume pending/error items with bounded retries and per-stock transactions."""
    if isinstance(max_stocks, bool) or not 1 <= max_stocks <= MAX_STOCKS_PER_RUN:
        raise MinuteBackfillError(
            f"max_stocks必须在1到{MAX_STOCKS_PER_RUN}之间"
        )
    universe = freeze_latest_minute_universe(storage, frozen_at=now_fn())
    items = storage.execute(
        """
        SELECT stock_code, status
        FROM minute_capture_items
        WHERE candidate_trade_date = ?
        ORDER BY CASE status WHEN 'SUCCESS' THEN 1 ELSE 0 END, stock_code
        """,
        (universe.candidate_trade_date,),
    )
    resumable_codes = [
        str(row["stock_code"])
        for row in items
        if row["status"] in {"PENDING", "ERROR"}
    ]
    pending_codes = resumable_codes[:max_stocks]
    already_successful = sum(row["status"] == "SUCCESS" for row in items)
    existing_conflicts = sum(row["status"] == "CONFLICT" for row in items)
    successes = 0
    failures = 0
    new_conflicts = 0
    consecutive_source_errors = 0
    circuit_reason: str | None = None
    request_count = 0

    for code in pending_codes:
        request_attempts = 0
        try:
            while True:
                if request_count:
                    delay = float(delay_seconds())
                    if delay < 0:
                        raise MinuteBackfillError("请求间隔不能为负数")
                    sleeper(delay)
                request_count += 1
                request_attempts += 1
                try:
                    bars = fetcher(code)
                    if not bars:
                        raise SinaMinuteError("Sina 5分钟数据为空")
                    _validate_fetched_bars(code, bars)
                    break
                except SinaMinuteError as exc:
                    if exc.circuit_breaker:
                        raise
                    if exc.retryable and request_attempts <= MAX_RETRIES:
                        continue
                    raise

            fetched_at = _as_shanghai_time(now_fn()).isoformat(timespec="seconds")
            save_minute_bars(
                storage.db_path,
                universe.candidate_trade_date,
                code,
                bars,
                fetched_at=fetched_at,
                request_attempts=request_attempts,
            )
            successes += 1
            consecutive_source_errors = 0
        except SinaMinuteError as exc:
            failures += 1
            consecutive_source_errors += 1
            _mark_item_error(
                storage.db_path,
                universe.candidate_trade_date,
                code,
                str(exc),
                attempts=request_attempts,
                attempted_at=_as_shanghai_time(now_fn()).isoformat(timespec="seconds"),
            )
            if exc.circuit_breaker:
                circuit_reason = str(exc)
                break
            if consecutive_source_errors >= 3:
                circuit_reason = "连续3只股票源错误，已熔断"
                break
        except MinuteBarConflictError as exc:
            failures += 1
            new_conflicts += 1
            _mark_item_error(
                storage.db_path,
                universe.candidate_trade_date,
                code,
                str(exc),
                attempts=request_attempts,
                attempted_at=_as_shanghai_time(now_fn()).isoformat(timespec="seconds"),
                status="CONFLICT",
            )
        except MinuteBackfillError as exc:
            failures += 1
            _mark_item_error(
                storage.db_path,
                universe.candidate_trade_date,
                code,
                str(exc),
                attempts=request_attempts,
                attempted_at=_as_shanghai_time(now_fn()).isoformat(timespec="seconds"),
            )

    conflict_items = existing_conflicts + new_conflicts
    if failures or circuit_reason or conflict_items:
        status = "PARTIAL"
    elif len(resumable_codes) > len(pending_codes):
        status = "BATCH_INCOMPLETE"
    else:
        status = "SUCCESS"
    return MinuteBackfillResult(
        candidate_trade_date=universe.candidate_trade_date,
        universe_hash=universe.universe_hash,
        total_items=len(items),
        selected_items=len(pending_codes),
        successful_items=successes,
        failed_items=failures,
        already_successful=already_successful,
        status=status,
        circuit_reason=circuit_reason,
        conflict_items=conflict_items,
    )


def save_minute_bars(
    db_path: str,
    candidate_trade_date: str,
    stock_code: str,
    bars: list[MinuteBar],
    *,
    fetched_at: str,
    request_attempts: int = 1,
) -> None:
    """Atomically persist immutable bars and advance exactly one item checkpoint."""
    if not bars:
        raise MinuteBackfillError("禁止保存空的5分钟数据")
    _validate_fetched_bars(stock_code, bars)
    fetched_time = _parse_shanghai_time(fetched_at, "first_fetched_at")
    for bar in bars:
        if _parse_shanghai_time(bar.bar_time, "bar_time") > fetched_time:
            raise MinuteBackfillError(
                f"{stock_code}包含晚于first_fetched_at的未来bar，禁止写入"
            )
    connection = _write_connection(db_path)
    try:
        connection.execute("BEGIN IMMEDIATE")
        item = connection.execute(
            """
            SELECT 1 FROM minute_capture_items
            WHERE candidate_trade_date = ? AND stock_code = ?
            """,
            (candidate_trade_date, stock_code),
        ).fetchone()
        if item is None:
            raise MinuteBackfillError("补采股票不在冻结候选中")

        prepared = [(bar, _bar_payload_hash(bar)) for bar in bars]
        for bar, payload_hash in prepared:
            existing = connection.execute(
                """
                SELECT payload_hash FROM minute_bars_5m
                WHERE source = ? AND stock_code = ? AND period = ?
                  AND adjust = ? AND bar_time = ?
                """,
                (bar.source, stock_code, bar.period, bar.adjust, bar.bar_time),
            ).fetchone()
            if existing is not None and str(existing["payload_hash"]) != payload_hash:
                raise MinuteBarConflictError(
                    f"{stock_code} {bar.bar_time}同键异值，禁止覆盖"
                )
        for bar, payload_hash in prepared:
            connection.execute(
                """
                INSERT INTO minute_bars_5m (
                    source, stock_code, period, adjust, bar_time,
                    open, high, low, close, volume, amount,
                    first_fetched_at, payload_hash
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(source, stock_code, period, adjust, bar_time) DO NOTHING
                """,
                (
                    bar.source,
                    stock_code,
                    bar.period,
                    bar.adjust,
                    bar.bar_time,
                    bar.open,
                    bar.high,
                    bar.low,
                    bar.close,
                    bar.volume,
                    bar.amount,
                    fetched_at,
                    payload_hash,
                ),
            )
        counts = connection.execute(
            """
            SELECT substr(bar_time, 1, 10) AS trade_date, COUNT(*) AS n
            FROM minute_bars_5m
            WHERE source = ? AND stock_code = ? AND period = ? AND adjust = ?
            GROUP BY substr(bar_time, 1, 10)
            """,
            (SOURCE_NAME, stock_code, PERIOD, ADJUST),
        ).fetchall()
        bars_count = sum(int(row["n"]) for row in counts)
        complete_days = sum(int(row["n"]) == 48 for row in counts)
        partial_days = sum(int(row["n"]) != 48 for row in counts)
        connection.execute(
            """
            UPDATE minute_capture_items
            SET status = 'SUCCESS', attempts = attempts + ?, last_error = NULL,
                bars_count = ?, complete_days = ?, partial_days = ?,
                last_attempt_at = ?
            WHERE candidate_trade_date = ? AND stock_code = ?
            """,
            (
                request_attempts,
                bars_count,
                complete_days,
                partial_days,
                fetched_at,
                candidate_trade_date,
                stock_code,
            ),
        )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def load_minute_status(db_path: str | Path) -> MinuteStatus:
    """Read the latest frozen universe through SQLite mode=ro without init/write."""
    path = Path(db_path).resolve()
    if not path.is_file():
        return _empty_status()
    connection = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=2)
    connection.row_factory = sqlite3.Row
    try:
        table = connection.execute(
            """
            SELECT 1 FROM sqlite_master
            WHERE type = 'table' AND name = 'minute_capture_items'
            """
        ).fetchone()
        if table is None:
            return _empty_status()
        latest = connection.execute(
            "SELECT MAX(candidate_trade_date) FROM minute_capture_items"
        ).fetchone()[0]
        if latest is None:
            return _empty_status()
        row = connection.execute(
            """
            SELECT MIN(universe_hash) AS universe_hash,
                   COUNT(*) AS total_items,
                   SUM(status = 'PENDING') AS pending_items,
                   SUM(status = 'SUCCESS') AS successful_items,
                   SUM(status = 'ERROR') AS error_items,
                   SUM(status = 'CONFLICT') AS conflict_items,
                   SUM(bars_count) AS bars_count,
                   SUM(complete_days) AS complete_days,
                   SUM(partial_days) AS partial_days,
                   MAX(last_attempt_at) AS last_attempt_at
            FROM minute_capture_items
            WHERE candidate_trade_date = ?
            """,
            (latest,),
        ).fetchone()
        return MinuteStatus(
            candidate_trade_date=str(latest),
            universe_hash=str(row["universe_hash"]),
            total_items=int(row["total_items"] or 0),
            pending_items=int(row["pending_items"] or 0),
            successful_items=int(row["successful_items"] or 0),
            error_items=int(row["error_items"] or 0),
            conflict_items=int(row["conflict_items"] or 0),
            bars_count=int(row["bars_count"] or 0),
            complete_days=int(row["complete_days"] or 0),
            partial_days=int(row["partial_days"] or 0),
            last_attempt_at=row["last_attempt_at"],
        )
    finally:
        connection.close()


def _features_json(row: dict[str, Any]) -> str:
    excluded = {"stock_code", "trade_date", "snapshot_time", "name"}
    features = {key: row[key] for key in sorted(row) if key not in excluded}
    return json.dumps(features, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _bar_payload_hash(bar: MinuteBar) -> str:
    payload = asdict(bar)
    return _hash_json(payload)


def _hash_json(value: Any) -> str:
    encoded = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _validate_fetched_bars(stock_code: str, bars: list[MinuteBar]) -> None:
    seen: set[str] = set()
    for bar in bars:
        if not isinstance(bar, MinuteBar):
            raise SinaMinuteError("5分钟适配器返回了非MinuteBar对象")
        if bar.stock_code != stock_code:
            raise SinaMinuteError("5分钟bar股票代码与请求代码不一致")
        if bar.source != SOURCE_NAME or bar.period != PERIOD or bar.adjust != ADJUST:
            raise SinaMinuteError("当前仅允许Sina RAW 5m数据")
        try:
            parsed_time = datetime.fromisoformat(bar.bar_time)
        except ValueError as exc:
            raise SinaMinuteError("5分钟bar_time格式异常") from exc
        if parsed_time.tzinfo is None or parsed_time.astimezone(SHANGHAI).utcoffset() != parsed_time.utcoffset():
            raise SinaMinuteError("5分钟bar_time必须是Asia/Shanghai时间")
        if parsed_time.second != 0 or parsed_time.minute % 5 != 0:
            raise SinaMinuteError("5分钟bar_time不在5分钟边界")
        for value in (
            bar.open,
            bar.high,
            bar.low,
            bar.close,
            bar.volume,
            bar.amount,
        ):
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise SinaMinuteError("5分钟bar包含非数值字段")
            if not math.isfinite(float(value)) or float(value) < 0:
                raise SinaMinuteError("5分钟bar包含非负有限数约束之外的字段")
        if bar.high < max(bar.open, bar.close, bar.low) or bar.low > min(
            bar.open, bar.close, bar.high
        ):
            raise SinaMinuteError("5分钟bar的OHLC关系异常")
        if bar.bar_time in seen:
            raise SinaMinuteError("5分钟bar_time重复")
        seen.add(bar.bar_time)


def _mark_item_error(
    db_path: str,
    candidate_trade_date: str,
    stock_code: str,
    error: str,
    *,
    attempts: int,
    attempted_at: str,
    status: str = "ERROR",
) -> None:
    connection = _write_connection(db_path)
    try:
        connection.execute(
            """
            UPDATE minute_capture_items
            SET status = ?, attempts = attempts + ?, last_error = ?,
                last_attempt_at = ?
            WHERE candidate_trade_date = ? AND stock_code = ?
            """,
            (
                status,
                attempts,
                error[:500],
                attempted_at,
                candidate_trade_date,
                stock_code,
            ),
        )
        connection.commit()
    finally:
        connection.close()


def _write_connection(db_path: str) -> sqlite3.Connection:
    connection = sqlite3.connect(db_path, timeout=30)
    connection.row_factory = sqlite3.Row
    return connection


def _as_shanghai_time(value: datetime | None) -> datetime:
    observed = value or datetime.now(SHANGHAI)
    if observed.tzinfo is None:
        return observed.replace(tzinfo=SHANGHAI)
    return observed.astimezone(SHANGHAI)


def _parse_shanghai_time(value: str, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise MinuteBackfillError(f"{field}不是合法ISO时间") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != SHANGHAI.utcoffset(parsed):
        raise MinuteBackfillError(f"{field}必须是Asia/Shanghai时间")
    return parsed.astimezone(SHANGHAI)


def _empty_status() -> MinuteStatus:
    return MinuteStatus(None, None, 0, 0, 0, 0, 0, 0, 0, 0, None)
