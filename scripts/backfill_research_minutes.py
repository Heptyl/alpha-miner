#!/usr/bin/env python3
"""Backfill historical A-share 5-minute bars for strategy research.

The yearly universe is selected using the previous year's average daily amount,
so historical research does not use future liquidity information. Data is kept
in a separate SQLite database and every stock/year task is resumable.
"""

from __future__ import annotations

import argparse
import signal
import socket
import sqlite3
import time
from datetime import datetime
from pathlib import Path

import baostock as bs


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DAILY_DB = PROJECT_ROOT / "data" / "alpha_miner.db"
MINUTE_DB = PROJECT_ROOT / "data" / "research_minutes_5m.db"
VALID_PREFIXES = ("000", "001", "002", "003", "300", "301", "600", "601", "603", "605", "688", "689")
FIELDS = "date,time,code,open,high,low,close,volume,amount,adjustflag"


class NetworkOperationTimeout(TimeoutError):
    pass


def _alarm_handler(signum, frame) -> None:
    raise NetworkOperationTimeout("baostock network operation timed out")


def with_alarm(seconds: int, function, *args, **kwargs):
    previous = signal.signal(signal.SIGALRM, _alarm_handler)
    signal.alarm(seconds)
    try:
        return function(*args, **kwargs)
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, previous)


def connect_minute_db(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path, timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS yearly_universe (
            target_year INTEGER NOT NULL,
            rank_no INTEGER NOT NULL,
            stock_code TEXT NOT NULL,
            source_year INTEGER NOT NULL,
            avg_daily_amount REAL NOT NULL,
            PRIMARY KEY (target_year, stock_code)
        );

        CREATE TABLE IF NOT EXISTS minute_bars_5m (
            stock_code TEXT NOT NULL,
            bar_time TEXT NOT NULL,
            trade_date TEXT NOT NULL,
            open REAL NOT NULL,
            high REAL NOT NULL,
            low REAL NOT NULL,
            close REAL NOT NULL,
            volume REAL NOT NULL,
            amount REAL NOT NULL,
            adjustflag TEXT NOT NULL,
            source TEXT NOT NULL DEFAULT 'baostock',
            PRIMARY KEY (stock_code, bar_time)
        ) WITHOUT ROWID;

        CREATE INDEX IF NOT EXISTS idx_minute_bars_date
        ON minute_bars_5m(trade_date, stock_code, bar_time);

        CREATE TABLE IF NOT EXISTS backfill_progress (
            target_year INTEGER NOT NULL,
            stock_code TEXT NOT NULL,
            status TEXT NOT NULL,
            row_count INTEGER NOT NULL DEFAULT 0,
            error_message TEXT NOT NULL DEFAULT '',
            updated_at TEXT NOT NULL,
            PRIMARY KEY (target_year, stock_code)
        );
        """
    )
    return conn


def is_a_share(code: str) -> bool:
    return len(code) == 6 and code.startswith(VALID_PREFIXES)


def build_yearly_universe(
    daily_db: Path,
    target_year: int,
    size: int,
) -> list[tuple[str, float]]:
    source_year = target_year - 1
    conn = sqlite3.connect(daily_db)
    try:
        rows = conn.execute(
            """
            SELECT stock_code, AVG(amount) AS avg_amount
            FROM daily_price
            WHERE trade_date BETWEEN ? AND ?
              AND amount > 0
            GROUP BY stock_code
            HAVING COUNT(*) >= 60
            ORDER BY avg_amount DESC
            """,
            (f"{source_year}-01-01", f"{source_year}-12-31"),
        ).fetchall()
    finally:
        conn.close()

    result = []
    for code, avg_amount in rows:
        if is_a_share(str(code)):
            result.append((str(code), float(avg_amount)))
        if len(result) >= size:
            break
    if len(result) < size:
        raise RuntimeError(
            f"{target_year} universe only has {len(result)} eligible stocks from {source_year}"
        )
    return result


def save_universe(
    conn: sqlite3.Connection,
    target_year: int,
    universe: list[tuple[str, float]],
) -> None:
    source_year = target_year - 1
    conn.executemany(
        """
        INSERT INTO yearly_universe
        (target_year, rank_no, stock_code, source_year, avg_daily_amount)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(target_year, stock_code) DO UPDATE SET
            rank_no=excluded.rank_no,
            source_year=excluded.source_year,
            avg_daily_amount=excluded.avg_daily_amount
        """,
        [
            (target_year, rank_no, code, source_year, avg_amount)
            for rank_no, (code, avg_amount) in enumerate(universe, start=1)
        ],
    )
    conn.commit()


def to_baostock_code(code: str) -> str:
    return f"sh.{code}" if code.startswith(("600", "601", "603", "605", "688", "689")) else f"sz.{code}"


def fetch_stock_year(code: str, year: int) -> list[tuple]:
    rs = bs.query_history_k_data_plus(
        to_baostock_code(code),
        FIELDS,
        start_date=f"{year}-01-01",
        end_date=f"{year}-12-31",
        frequency="5",
        adjustflag="3",
    )
    if rs.error_code != "0":
        raise RuntimeError(rs.error_msg)

    rows = []
    while rs.next():
        row = rs.get_row_data()
        if not row or not row[1]:
            continue
        raw_time = row[1]
        bar_time = datetime.strptime(raw_time[:14], "%Y%m%d%H%M%S").strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        rows.append(
            (
                code,
                bar_time,
                row[0],
                float(row[3]),
                float(row[4]),
                float(row[5]),
                float(row[6]),
                float(row[7] or 0),
                float(row[8] or 0),
                row[9],
            )
        )
    return rows


def task_complete(conn: sqlite3.Connection, year: int, code: str) -> bool:
    row = conn.execute(
        "SELECT status FROM backfill_progress WHERE target_year=? AND stock_code=?",
        (year, code),
    ).fetchone()
    return bool(row and row[0] == "complete")


def save_task(
    conn: sqlite3.Connection,
    year: int,
    code: str,
    rows: list[tuple],
) -> None:
    conn.executemany(
        """
        INSERT OR REPLACE INTO minute_bars_5m
        (stock_code, bar_time, trade_date, open, high, low, close,
         volume, amount, adjustflag)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    conn.execute(
        """
        INSERT INTO backfill_progress
        (target_year, stock_code, status, row_count, error_message, updated_at)
        VALUES (?, ?, 'complete', ?, '', datetime('now', 'localtime'))
        ON CONFLICT(target_year, stock_code) DO UPDATE SET
            status='complete',
            row_count=excluded.row_count,
            error_message='',
            updated_at=excluded.updated_at
        """,
        (year, code, len(rows)),
    )
    conn.commit()


def save_failure(conn: sqlite3.Connection, year: int, code: str, message: str) -> None:
    conn.execute(
        """
        INSERT INTO backfill_progress
        (target_year, stock_code, status, row_count, error_message, updated_at)
        VALUES (?, ?, 'failed', 0, ?, datetime('now', 'localtime'))
        ON CONFLICT(target_year, stock_code) DO UPDATE SET
            status='failed',
            error_message=excluded.error_message,
            updated_at=excluded.updated_at
        """,
        (year, code, message[:500]),
    )
    conn.commit()


def run(args: argparse.Namespace) -> int:
    socket.setdefaulttimeout(args.network_timeout)
    conn = connect_minute_db(args.output)
    tasks: list[tuple[int, str]] = []
    try:
        for year in args.years:
            universe = build_yearly_universe(args.daily_db, year, args.universe_size)
            save_universe(conn, year, universe)
            year_codes = [code for code, _ in universe]
            if args.limit:
                year_codes = year_codes[: args.limit]
            tasks.extend((year, code) for code in year_codes)

        pending = [
            task for task in tasks
            if args.force or not task_complete(conn, task[0], task[1])
        ]
        pending = [
            task for index, task in enumerate(pending)
            if index % args.shard_count == args.shard_index
        ]
        print(
            f"tasks={len(tasks)} pending={len(pending)} "
            f"shard={args.shard_index}/{args.shard_count} output={args.output}",
            flush=True,
        )
        if not pending:
            return 0

        login = None
        last_login_error = ""
        for attempt in range(1, args.retries + 1):
            try:
                login = with_alarm(args.network_timeout, bs.login)
                if login.error_code == "0":
                    break
                last_login_error = login.error_msg
            except Exception as exc:
                last_login_error = str(exc)
            time.sleep(min(5, attempt))
        if login is None or login.error_code != "0":
            raise RuntimeError(f"baostock login failed: {last_login_error}")
        try:
            total_rows = 0
            failures = 0
            started = time.monotonic()
            for index, (year, code) in enumerate(pending, start=1):
                last_error = ""
                for attempt in range(1, args.retries + 1):
                    try:
                        rows = with_alarm(
                            args.network_timeout,
                            fetch_stock_year,
                            code,
                            year,
                        )
                        save_task(conn, year, code, rows)
                        total_rows += len(rows)
                        last_error = ""
                        break
                    except Exception as exc:
                        last_error = str(exc)
                        time.sleep(min(5, attempt))
                if last_error:
                    failures += 1
                    save_failure(conn, year, code, last_error)

                if index % args.progress_every == 0 or index == len(pending):
                    elapsed = max(time.monotonic() - started, 0.001)
                    print(
                        f"progress={index}/{len(pending)} rows={total_rows} "
                        f"failed={failures} tasks_per_min={index / elapsed * 60:.1f}",
                        flush=True,
                    )
        finally:
            bs.logout()
    finally:
        conn.close()
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--daily-db", type=Path, default=DAILY_DB)
    parser.add_argument("--output", type=Path, default=MINUTE_DB)
    parser.add_argument("--years", type=int, nargs="+", default=[2023, 2024, 2025, 2026])
    parser.add_argument("--universe-size", type=int, default=500)
    parser.add_argument("--limit", type=int, default=0, help="Limit stocks per year for pilot runs")
    parser.add_argument("--retries", type=int, default=3)
    parser.add_argument("--network-timeout", type=int, default=20)
    parser.add_argument("--progress-every", type=int, default=10)
    parser.add_argument("--shard-count", type=int, default=1)
    parser.add_argument("--shard-index", type=int, default=0)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()
    if args.shard_count < 1:
        parser.error("--shard-count must be >= 1")
    if not 0 <= args.shard_index < args.shard_count:
        parser.error("--shard-index must be in [0, shard-count)")
    return args


if __name__ == "__main__":
    raise SystemExit(run(parse_args()))
