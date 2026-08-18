"""Forward-only 09:25/09:31 evidence capture for a frozen D-1 universe."""

from __future__ import annotations

import math
import sqlite3
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any, Callable, Protocol
from zoneinfo import ZoneInfo

from src.data.sources.sina_prelimit import fetch_all_spot
from src.mining.playbook import load_play_card

AUCTION_PHASE = "AUCTION_0925"
OPEN_PHASE = "OPEN_0931"
PHASES = frozenset({AUCTION_PHASE, OPEN_PHASE})
_SHANGHAI = ZoneInfo("Asia/Shanghai")

# Audited against the SSE 2026 closure notice (上证公告〔2025〕45号):
# https://www.sse.com.cn/disclosure/dealinstruc/closed/
# Unsupported years fail closed until their exchange notice is added.
MARKET_CLOSURES: dict[int, frozenset[str]] = {
    2026: frozenset(
        {
            "2026-01-01",
            "2026-01-02",
            "2026-01-03",
            "2026-01-04",
            "2026-02-14",
            "2026-02-15",
            "2026-02-16",
            "2026-02-17",
            "2026-02-18",
            "2026-02-19",
            "2026-02-20",
            "2026-02-21",
            "2026-02-22",
            "2026-02-23",
            "2026-02-28",
            "2026-04-04",
            "2026-04-05",
            "2026-04-06",
            "2026-05-01",
            "2026-05-02",
            "2026-05-03",
            "2026-05-04",
            "2026-05-05",
            "2026-05-09",
            "2026-06-19",
            "2026-06-20",
            "2026-06-21",
            "2026-09-20",
            "2026-09-25",
            "2026-09-26",
            "2026-09-27",
            "2026-10-01",
            "2026-10-02",
            "2026-10-03",
            "2026-10-04",
            "2026-10-05",
            "2026-10-06",
            "2026-10-07",
            "2026-10-10",
        }
    )
}


class CaptureStorage(Protocol):
    def execute(self, sql: str, params: tuple = ()) -> list[dict[str, Any]]: ...

    def execute_write(self, sql: str, params: tuple = ()) -> None: ...


class PrelimitCaptureError(RuntimeError):
    """Raised before an empty, stale, or out-of-window capture can be written."""


@dataclass(frozen=True)
class CaptureResult:
    trade_date: str
    candidate_trade_date: str
    phase: str
    candidate_count: int
    stored_count: int


@dataclass(frozen=True)
class PrelimitStatus:
    auction_date: str | None
    auction_rows: int
    open_date: str | None
    open_rows: int
    paired_date: str | None
    latest_date: str | None
    missing_phases: tuple[str, ...]


QuoteFetcher = Callable[[], list[dict[str, Any]]]


def capture_prelimit(
    storage: CaptureStorage,
    phase: str,
    observed_at: datetime | None = None,
    fetch_quotes: QuoteFetcher = fetch_all_spot,
) -> CaptureResult:
    """Capture one phase after freezing candidates from the latest audited D-1."""
    if phase not in PHASES:
        raise PrelimitCaptureError(f"不支持的竞价阶段：{phase}")
    observed = observed_at or datetime.now().astimezone()
    if not isinstance(observed, datetime):
        raise PrelimitCaptureError("observed_at必须是datetime")
    if observed.tzinfo is None:
        observed = observed.astimezone()
    _validate_capture_time(phase, observed)
    trade_date = observed.date().isoformat()

    candidate_date, candidates = _load_frozen_candidates(storage, trade_date)
    if not candidates:
        return CaptureResult(trade_date, candidate_date, phase, 0, 0)
    quotes = fetch_quotes()
    if not isinstance(quotes, list) or not quotes:
        raise PrelimitCaptureError("Sina全市场快照为空，禁止写入空成功")
    quote_by_code: dict[str, dict[str, Any]] = {}
    for quote in quotes:
        if not isinstance(quote, dict):
            raise PrelimitCaptureError("Sina标准化快照包含非对象行")
        code = str(quote.get("stock_code") or "")
        if not code:
            raise PrelimitCaptureError("Sina标准化快照缺少stock_code")
        if code in quote_by_code:
            raise PrelimitCaptureError(f"Sina标准化快照代码重复：{code}")
        quote_by_code[code] = quote

    missing = sorted(code for code in candidates if code not in quote_by_code)
    if missing:
        preview = "、".join(missing[:8])
        raise PrelimitCaptureError(f"冻结候选缺少Sina快照：{preview}")
    _validate_source_clock(phase, [quote_by_code[code] for code in candidates])

    observed_text = observed.isoformat(timespec="seconds")
    snapshot_time = datetime.now().astimezone().isoformat(timespec="seconds")
    values: list[Any] = []
    placeholders = []
    for code in sorted(candidates):
        quote = quote_by_code[code]
        _validate_normalized_quote(quote, code)
        values.extend(
            (
                trade_date,
                candidate_date,
                observed_text,
                code,
                candidates[code] or str(quote.get("stock_name") or ""),
                phase,
                quote["price"],
                quote["open"],
                quote["high"],
                quote["low"],
                quote["volume"],
                quote["amount"],
                quote.get("bid1"),
                quote.get("ask1"),
                str(quote["source"]),
                snapshot_time,
            )
        )
        placeholders.append("(" + ",".join("?" for _ in range(16)) + ")")

    storage.execute_write(
        """
        INSERT INTO prelimit_snapshots (
            trade_date, candidate_trade_date, observed_at, stock_code, stock_name,
            phase, price, open, high, low, volume, amount, bid1, ask1, source,
            snapshot_time
        ) VALUES
        """
        + ",".join(placeholders)
        + " ON CONFLICT(trade_date, phase, stock_code) DO NOTHING",
        tuple(values),
    )
    stored = storage.execute(
        """
        SELECT COUNT(*) AS n FROM prelimit_snapshots
        WHERE trade_date = ? AND phase = ?
        """,
        (trade_date, phase),
    )[0]["n"]
    return CaptureResult(
        trade_date=trade_date,
        candidate_trade_date=candidate_date,
        phase=phase,
        candidate_count=len(candidates),
        stored_count=int(stored),
    )


def load_prelimit_pairs(
    storage: CaptureStorage,
    trade_date: str | None = None,
) -> list[dict[str, Any]]:
    """Pair the two evidence snapshots without inventing a trading rule."""
    date_filter = ""
    params: tuple[Any, ...] = ()
    if trade_date is not None:
        date_filter = "AND auction.trade_date = ?"
        params = (trade_date,)
    return storage.execute(
        f"""
        SELECT auction.trade_date,
               auction.candidate_trade_date,
               auction.stock_code,
               auction.stock_name,
               auction.observed_at AS auction_observed_at,
               auction.price AS auction_price,
               opening.observed_at AS open_observed_at,
               opening.price AS open_price,
               opening.volume - auction.volume AS cumulative_volume_delta,
               opening.amount - auction.amount AS cumulative_amount_delta
        FROM prelimit_snapshots AS auction
        JOIN prelimit_snapshots AS opening
          ON opening.trade_date = auction.trade_date
         AND opening.stock_code = auction.stock_code
         AND opening.phase = '{OPEN_PHASE}'
        WHERE auction.phase = '{AUCTION_PHASE}'
          {date_filter}
        ORDER BY auction.trade_date, auction.stock_code
        """,
        params,
    )


def load_prelimit_status(db_path: str | Path) -> PrelimitStatus:
    """Read phase completeness through SQLite mode=ro without initializing data."""
    path = Path(db_path).resolve()
    if not path.is_file():
        return _empty_status()
    connection = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=2)
    connection.row_factory = sqlite3.Row
    try:
        table = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='prelimit_snapshots'"
        ).fetchone()
        if table is None:
            return _empty_status()
        rows = connection.execute(
            """
            SELECT trade_date, phase, COUNT(*) AS n
            FROM prelimit_snapshots
            GROUP BY trade_date, phase
            ORDER BY trade_date, phase
            """
        ).fetchall()
    finally:
        connection.close()
    return _status_from_rows([dict(row) for row in rows])


def _load_frozen_candidates(
    storage: CaptureStorage,
    trade_date: str,
) -> tuple[str, dict[str, str]]:
    candidate_date = _previous_trade_date(date.fromisoformat(trade_date)).isoformat()
    rows = storage.execute(
        """
        SELECT status, attempted_at
        FROM limit_up_collection_runs
        WHERE trade_date = ?
        ORDER BY attempted_at DESC, id DESC
        LIMIT 1
        """,
        (candidate_date,),
    )
    if not rows or rows[0].get("status") != "ok" or not is_post_close_attempt(
        candidate_date, rows[0].get("attempted_at")
    ):
        raise PrelimitCaptureError(
            f"上一交易日{candidate_date}最新记录不是可用盘后成功审计，禁止回退或采集"
        )
    try:
        card = load_play_card(
            storage, "attention_reacceleration_open_v1", candidate_date
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise PrelimitCaptureError("冻结attention玩法卡不可验证") from exc
    if card is None:
        raise PrelimitCaptureError("缺少唯一冻结的attention PAPER玩法卡候选")
    candidates = {
        str(item.get("stock_code")): str(item.get("stock_name") or "")
        for item in card.candidates
        if isinstance(item, dict)
        and item.get("paper_status") == "PLANNED"
        and str(item.get("planned_entry_date") or "") == trade_date
        and str(item.get("stock_code") or "")
    }
    return candidate_date, candidates


def is_post_close_attempt(trade_date: str, value: Any) -> bool:
    try:
        attempted = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        event_date = date.fromisoformat(trade_date)
    except (TypeError, ValueError):
        return False
    if attempted.tzinfo is not None:
        attempted = attempted.astimezone(_SHANGHAI).replace(tzinfo=None)
    return attempted.date() > event_date or (
        attempted.date() == event_date and attempted.time() >= time(15, 40)
    )


def _validate_capture_time(phase: str, observed: datetime) -> None:
    _validate_trade_date(observed.date())
    start, end = _phase_window(phase)
    observed_time = observed.time().replace(tzinfo=None)
    if not start <= observed_time <= end:
        raise PrelimitCaptureError(
            f"{phase}仅允许在{start.strftime('%H:%M')}–{end.strftime('%H:%M')}采集"
        )


def _validate_trade_date(observed_date: date) -> None:
    closures = MARKET_CLOSURES.get(observed_date.year)
    if closures is None:
        raise PrelimitCaptureError(
            f"{observed_date.year}年交易日历尚未审计，禁止采集"
        )
    if observed_date.weekday() >= 5:
        raise PrelimitCaptureError("观察日不是交易日：周末禁止采集")
    if observed_date.isoformat() in closures:
        raise PrelimitCaptureError("观察日是交易所公告休市日，禁止采集")


def _previous_trade_date(observed_date: date) -> date:
    candidate = observed_date - timedelta(days=1)
    while True:
        closures = MARKET_CLOSURES.get(candidate.year)
        if closures is None:
            raise PrelimitCaptureError(
                f"{candidate.year}年交易日历尚未审计，无法确定上一交易日"
            )
        if candidate.weekday() < 5 and candidate.isoformat() not in closures:
            return candidate
        candidate -= timedelta(days=1)


def _validate_source_clock(phase: str, quotes: list[dict[str, Any]]) -> None:
    start, end = _phase_window(phase)
    for quote in quotes:
        value = str(quote.get("source_time") or "")
        try:
            parsed = time.fromisoformat(value)
        except ValueError as exc:
            raise PrelimitCaptureError("Sina源时间缺失或格式错误") from exc
        if not start <= parsed <= end:
            code = str(quote.get("stock_code") or "未知代码")
            raise PrelimitCaptureError(
                f"{code}的Sina源时间不在{phase}窗口，可能是休市或陈旧快照"
            )


def _phase_window(phase: str) -> tuple[time, time]:
    if phase == AUCTION_PHASE:
        return time(9, 20), time(9, 29, 59)
    return time(9, 30), time(9, 35, 59)


def _validate_normalized_quote(quote: dict[str, Any], code: str) -> None:
    for field in ("price", "open", "high", "low", "volume", "amount"):
        value = quote.get(field)
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            raise PrelimitCaptureError(f"{code}的{field}字段异常")
        if not math.isfinite(float(value)) or float(value) < 0:
            raise PrelimitCaptureError(f"{code}的{field}不是非负有限数")
    if not str(quote.get("source") or ""):
        raise PrelimitCaptureError(f"{code}缺少source")


def _status_from_rows(rows: list[dict[str, Any]]) -> PrelimitStatus:
    if not rows:
        return _empty_status()
    by_date: dict[str, dict[str, int]] = {}
    for row in rows:
        by_date.setdefault(str(row["trade_date"]), {})[str(row["phase"])] = int(row["n"])
    auction_dates = [date for date, phases in by_date.items() if AUCTION_PHASE in phases]
    open_dates = [date for date, phases in by_date.items() if OPEN_PHASE in phases]
    paired_dates = [
        date
        for date, phases in by_date.items()
        if AUCTION_PHASE in phases and OPEN_PHASE in phases
    ]
    latest_date = max(by_date)
    latest_phases = by_date[latest_date]
    missing = tuple(phase for phase in (AUCTION_PHASE, OPEN_PHASE) if phase not in latest_phases)
    auction_date = max(auction_dates) if auction_dates else None
    open_date = max(open_dates) if open_dates else None
    return PrelimitStatus(
        auction_date=auction_date,
        auction_rows=by_date[auction_date][AUCTION_PHASE] if auction_date else 0,
        open_date=open_date,
        open_rows=by_date[open_date][OPEN_PHASE] if open_date else 0,
        paired_date=max(paired_dates) if paired_dates else None,
        latest_date=latest_date,
        missing_phases=missing,
    )


def _empty_status() -> PrelimitStatus:
    return PrelimitStatus(
        auction_date=None,
        auction_rows=0,
        open_date=None,
        open_rows=0,
        paired_date=None,
        latest_date=None,
        missing_phases=(AUCTION_PHASE, OPEN_PHASE),
    )
