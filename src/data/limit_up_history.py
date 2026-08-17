"""涨停历史采集健康度与审计记录。

本模块只评价数据是否持续、完整地落库，不参与因子评分或准入判断。
交易日以数据库中达到全市场覆盖门槛的 ``daily_price`` 日期为事实日历；
另外用自然日间隔暴露“行情和涨停池同时停采”的长断档。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from src.data.storage import Storage
from src.data.trading_calendar import is_weekend

MIN_MARKET_ROWS = 1_000
MIN_LIMIT_UP_ROWS = 1
MAX_LIMIT_UP_ROWS = 200
MAX_CALENDAR_GAP_DAYS = 4

_FAILED_STATUSES = {
    "missing",
    "market_incomplete",
    "row_anomaly",
    "unconfirmed",
    "collection_error",
}


@dataclass(frozen=True)
class CollectionCheck:
    """一次目标日采集完成后的可调度结果。"""

    trade_date: str
    price_rows: int
    zt_rows: int
    status: str
    detail: str

    @property
    def failed(self) -> bool:
        return self.status in _FAILED_STATUSES


@dataclass(frozen=True)
class CalendarGap:
    """两个相邻已采涨停日期之间的自然日断档。"""

    previous_date: str
    next_date: str
    calendar_days: int


@dataclass(frozen=True)
class LimitUpHistoryHealth:
    """涨停历史的只读健康快照。"""

    latest_price_date: str | None
    latest_zt_date: str | None
    history_days: int
    continuous_days: int
    tracking_start: str | None
    missing_dates: tuple[str, ...]
    abnormal_dates: tuple[tuple[str, int], ...]
    failed_attempt_dates: tuple[tuple[str, str], ...]
    calendar_gaps: tuple[CalendarGap, ...]
    last_attempt: dict | None

    @property
    def strict_failure(self) -> bool:
        """活动采集期存在未闭环问题时供调度器返回非零。"""
        active_missing = self._active_dates(self.missing_dates)
        active_abnormal = self._active_dates(date for date, _ in self.abnormal_dates)
        active_failed = self._active_dates(date for date, _ in self.failed_attempt_dates)
        return bool(active_missing or active_abnormal or active_failed)

    def _active_dates(self, dates) -> tuple[str, ...]:
        if not self.tracking_start:
            return ()
        return tuple(date for date in dates if date >= self.tracking_start)


def count_rows_for_date(db: Storage, table: str, trade_date: str) -> int:
    """按股票去重统计目标日行数，避免重复快照放大健康值。"""
    rows = db.execute(
        f"SELECT COUNT(DISTINCT stock_code) AS n FROM [{table}] WHERE trade_date = ?",
        (trade_date,),
    )
    return int(rows[0]["n"] or 0) if rows else 0


def evaluate_collection_day(
    db: Storage,
    trade_date: str,
    *,
    min_market_rows: int = MIN_MARKET_ROWS,
    min_zt_rows: int = MIN_LIMIT_UP_ROWS,
    max_zt_rows: int = MAX_LIMIT_UP_ROWS,
) -> CollectionCheck:
    """评价目标日采集是否足以作为连续涨停历史的一天。"""
    if is_weekend(trade_date):
        return CollectionCheck(trade_date, 0, 0, "skipped", "周末，不要求采集")

    price_rows = count_rows_for_date(db, "daily_price", trade_date)
    zt_rows = count_rows_for_date(db, "zt_pool", trade_date)
    if zt_rows > max_zt_rows:
        return CollectionCheck(
            trade_date,
            price_rows,
            zt_rows,
            "row_anomaly",
            f"涨停池 {zt_rows} 行，超过异常上限 {max_zt_rows}",
        )
    if zt_rows < min_zt_rows:
        if price_rows >= min_market_rows:
            detail = f"全市场行情 {price_rows} 行，但涨停池缺失"
            status = "missing"
        else:
            detail = (
                f"行情仅 {price_rows} 行且涨停池为空，无法确认是休市还是采集失败"
            )
            status = "unconfirmed"
        return CollectionCheck(trade_date, price_rows, zt_rows, status, detail)

    if price_rows < min_market_rows:
        return CollectionCheck(
            trade_date,
            price_rows,
            zt_rows,
            "market_incomplete",
            f"全市场行情数据不足：仅 {price_rows} 行，最低要求 {min_market_rows} 行；涨停池已落库",
        )
    return CollectionCheck(trade_date, price_rows, zt_rows, "ok", "涨停池与全市场行情已落库")


def record_collection_attempt(
    db: Storage,
    check: CollectionCheck,
    *,
    attempted_at: datetime | None = None,
) -> None:
    """追加一次采集审计；不覆盖旧尝试，便于定位重试过程。"""
    timestamp = (attempted_at or datetime.now()).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    db.execute_write(
        "INSERT INTO limit_up_collection_runs "
        "(trade_date, attempted_at, price_rows, zt_rows, status, detail) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (
            check.trade_date,
            timestamp,
            check.price_rows,
            check.zt_rows,
            check.status,
            check.detail,
        ),
    )


def assess_limit_up_history(
    db: Storage,
    *,
    as_of: str | None = None,
    min_market_rows: int = MIN_MARKET_ROWS,
    min_zt_rows: int = MIN_LIMIT_UP_ROWS,
    max_zt_rows: int = MAX_LIMIT_UP_ROWS,
    max_calendar_gap_days: int = MAX_CALENDAR_GAP_DAYS,
) -> LimitUpHistoryHealth:
    """计算缺采、行数异常、长断档与当前连续采集天数。"""
    end_date = as_of or datetime.now().strftime("%Y-%m-%d")
    price_rows = db.execute(
        "SELECT trade_date, COUNT(DISTINCT stock_code) AS n "
        "FROM daily_price WHERE trade_date <= ? GROUP BY trade_date ORDER BY trade_date",
        (end_date,),
    )
    zt_rows = db.execute(
        "SELECT trade_date, COUNT(DISTINCT stock_code) AS n "
        "FROM zt_pool WHERE trade_date <= ? GROUP BY trade_date ORDER BY trade_date",
        (end_date,),
    )

    market_dates = {
        str(row["trade_date"]): int(row["n"] or 0)
        for row in price_rows
        if int(row["n"] or 0) >= min_market_rows and not is_weekend(str(row["trade_date"]))
    }
    zt_counts = {
        str(row["trade_date"]): int(row["n"] or 0)
        for row in zt_rows
        if not is_weekend(str(row["trade_date"]))
    }
    expected_dates = sorted(market_dates)
    captured_dates = sorted(zt_counts)
    missing_dates = tuple(date for date in expected_dates if date not in zt_counts)
    abnormal_dates = tuple(
        (date, count)
        for date, count in sorted(zt_counts.items())
        if count < min_zt_rows or count > max_zt_rows
    )
    abnormal_set = {date for date, _ in abnormal_dates}

    gaps = []
    for previous, current in zip(captured_dates, captured_dates[1:]):
        distance = (datetime.fromisoformat(current) - datetime.fromisoformat(previous)).days
        if distance > max_calendar_gap_days:
            gaps.append(CalendarGap(previous, current, distance))

    continuous_days = 0
    for index in range(len(expected_dates) - 1, -1, -1):
        date = expected_dates[index]
        if date in missing_dates or date in abnormal_set:
            break
        if index < len(expected_dates) - 1:
            next_date = expected_dates[index + 1]
            distance = (datetime.fromisoformat(next_date) - datetime.fromisoformat(date)).days
            if distance > max_calendar_gap_days:
                break
        continuous_days += 1

    attempts = db.execute(
        "SELECT trade_date, attempted_at, price_rows, zt_rows, status, detail "
        "FROM limit_up_collection_runs ORDER BY attempted_at DESC, id DESC LIMIT 1"
    )
    tracking_rows = db.execute(
        "SELECT MIN(trade_date) AS d FROM limit_up_collection_runs WHERE status != 'skipped'"
    )
    tracking_start = (
        str(tracking_rows[0]["d"])
        if tracking_rows and tracking_rows[0].get("d")
        else None
    )
    failed_attempt_rows = db.execute(
        "SELECT run.trade_date, run.status FROM limit_up_collection_runs AS run "
        "JOIN (SELECT trade_date, MAX(id) AS id FROM limit_up_collection_runs "
        "GROUP BY trade_date) AS latest ON latest.id = run.id "
        "WHERE run.status IN "
        "('missing', 'market_incomplete', 'row_anomaly', 'unconfirmed', 'collection_error') "
        "ORDER BY run.trade_date"
    )

    return LimitUpHistoryHealth(
        latest_price_date=max(market_dates) if market_dates else None,
        latest_zt_date=max(zt_counts) if zt_counts else None,
        history_days=len(captured_dates),
        continuous_days=continuous_days,
        tracking_start=tracking_start,
        missing_dates=missing_dates,
        abnormal_dates=abnormal_dates,
        failed_attempt_dates=tuple(
            (str(row["trade_date"]), str(row["status"])) for row in failed_attempt_rows
        ),
        calendar_gaps=tuple(gaps),
        last_attempt=attempts[0] if attempts else None,
    )
