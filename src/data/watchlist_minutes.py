"""Best-effort RAW 5m capture for the independent personal watchlist."""

from __future__ import annotations

import random
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from src.data.minute_backfill import (
    MinuteBackfillError,
    MinuteBarConflictError,
    MinuteRequestState,
    fetch_minute_bars_with_retry,
    persist_raw_5m_bars,
)
from src.data.sources.sina_minutes import MinuteBar, SinaMinuteError, fetch_raw_5m
from src.data.user_preferences import load_watchlist, record_capture_status

MAX_WATCHLIST_STOCKS = 20
SHANGHAI = ZoneInfo("Asia/Shanghai")
@dataclass(frozen=True)
class WatchlistCaptureResult:
    active_codes: int
    selected_codes: int
    successful_codes: int
    failed_codes: int
    status: str
    circuit_reason: str | None = None
def _observed_at(clock: Callable[[], datetime]) -> str:
    observed = clock()
    if observed.tzinfo is None:
        raise ValueError("watchlist capture clock必须包含时区")
    return observed.astimezone(SHANGHAI).isoformat(timespec="seconds")
def capture_watchlist_minutes(
    market_db: str | Path,
    preferences_db: str | Path,
    *,
    max_stocks: int = MAX_WATCHLIST_STOCKS,
    fetcher: Callable[[str], list[MinuteBar]] = fetch_raw_5m,
    sleeper: Callable[[float], None] = time.sleep,
    delay_seconds: Callable[[], float] = lambda: random.uniform(1.5, 2.5),
    clock: Callable[[], datetime] = lambda: datetime.now(SHANGHAI),
) -> WatchlistCaptureResult:
    """Capture only the watchlist snapshot, keeping bars in market and status personal."""
    if isinstance(max_stocks, bool) or not 1 <= max_stocks <= MAX_WATCHLIST_STOCKS:
        raise ValueError(f"max_stocks必须在1到{MAX_WATCHLIST_STOCKS}之间")
    items = load_watchlist(preferences_db)
    codes = [item[0] for item in items]
    selected = codes[:max_stocks]
    successes = failures = consecutive_errors = 0
    circuit_reason = None
    request_state = MinuteRequestState()
    for code in selected:
        try:
            bars = fetch_minute_bars_with_retry(code, fetcher=fetcher, sleeper=sleeper,
                                                 delay_seconds=delay_seconds, state=request_state)
            attempted = _observed_at(clock)
            count, _, _ = persist_raw_5m_bars(str(market_db), code, bars, fetched_at=attempted)
            record_capture_status(preferences_db, code, "SUCCESS", attempts=request_state.attempts,
                                  bars_count=count, attempted_at=attempted)
            successes += 1
            consecutive_errors = 0
        except MinuteBarConflictError as exc:
            failures += 1
            record_capture_status(preferences_db, code, "CONFLICT", attempts=request_state.attempts,
                                  bars_count=0, attempted_at=_observed_at(clock), error=str(exc))
        except SinaMinuteError as exc:
            failures += 1
            consecutive_errors += 1
            record_capture_status(preferences_db, code, "ERROR", attempts=request_state.attempts,
                                  bars_count=0, attempted_at=_observed_at(clock), error=str(exc))
            if exc.circuit_breaker or consecutive_errors >= 3:
                circuit_reason = str(exc) if exc.circuit_breaker else "连续3只股票源错误，已熔断"
                break
        except MinuteBackfillError as exc:
            failures += 1
            record_capture_status(preferences_db, code, "ERROR", attempts=request_state.attempts,
                                  bars_count=0, attempted_at=_observed_at(clock), error=str(exc))
    status = "EMPTY" if not codes else (
        "PARTIAL" if failures or circuit_reason or len(codes) > len(selected) else "SUCCESS"
    )
    return WatchlistCaptureResult(
        len(codes), len(selected), successes, failures, status, circuit_reason,
    )
