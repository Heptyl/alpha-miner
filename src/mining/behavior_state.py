"""Deterministic PIT reducer for behavioral state in the limit-up ecosystem."""

from __future__ import annotations

import hashlib
import json
import math
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, time
from typing import Any, Iterable

from src.data.pit import SHANGHAI, PointInTimeView

POST_CLOSE = time(15, 40)
INDUSTRY_PROXY = "INDUSTRY_PROXY"


@dataclass(frozen=True)
class BehaviorStateSpec:
    """Frozen experimental parameters; defaults are not universal truths."""

    model_version: str = "behavior-state-v1"
    lookback_trade_days: int = 20
    half_life_trade_days: float = 5.0
    limit_up_weight: float = 1.0
    repeat_limit_weight: float = 0.5
    consecutive_board_weight: float = 0.25
    seal_quality_weight: float = 0.25
    industry_diffusion_weight: float = 0.5
    failed_board_decay_weight: float = 0.75
    diffusion_stop_decay_weight: float = 0.25
    breakdown_decay_weight: float = 0.5

    def validate(self) -> None:
        if not isinstance(self.model_version, str) or not self.model_version.strip():
            raise ValueError("model_version must be non-empty")
        if isinstance(self.lookback_trade_days, bool) or not 2 <= self.lookback_trade_days <= 250:
            raise ValueError("lookback_trade_days must be an integer in [2, 250]")
        if not math.isfinite(self.half_life_trade_days) or self.half_life_trade_days <= 0:
            raise ValueError("half_life_trade_days must be finite and positive")
        for field in (
            "limit_up_weight",
            "repeat_limit_weight",
            "consecutive_board_weight",
            "seal_quality_weight",
            "industry_diffusion_weight",
            "failed_board_decay_weight",
            "diffusion_stop_decay_weight",
            "breakdown_decay_weight",
        ):
            value = getattr(self, field)
            if not math.isfinite(value) or value < 0:
                raise ValueError(f"{field} must be finite and non-negative")

    def to_payload(self) -> dict[str, Any]:
        self.validate()
        return {
            "model_version": self.model_version,
            "lookback_trade_days": self.lookback_trade_days,
            "half_life_trade_days": self.half_life_trade_days,
            "limit_up_weight": self.limit_up_weight,
            "repeat_limit_weight": self.repeat_limit_weight,
            "consecutive_board_weight": self.consecutive_board_weight,
            "seal_quality_weight": self.seal_quality_weight,
            "industry_diffusion_weight": self.industry_diffusion_weight,
            "failed_board_decay_weight": self.failed_board_decay_weight,
            "diffusion_stop_decay_weight": self.diffusion_stop_decay_weight,
            "breakdown_decay_weight": self.breakdown_decay_weight,
        }

    @property
    def spec_hash(self) -> str:
        encoded = json.dumps(
            self.to_payload(), sort_keys=True, separators=(",", ":"), allow_nan=False
        )
        return hashlib.sha256(encoded.encode()).hexdigest()

    def candidate_identity(self) -> dict[str, Any]:
        return {"behavior_state_spec": self.to_payload(), "behavior_state_spec_hash": self.spec_hash}


@dataclass(frozen=True)
class StockBehaviorState:
    stock_code: str
    name: str
    industry: str | None
    group_provenance: str | None
    state_domains: tuple[str, ...]
    own_attention: float
    group_attention: float
    diffusion: float
    crowding: float
    decay_age_trade_days: int | None
    attention_slope: float | None
    negative_pulse: float | None
    provenance: tuple[str, ...]
    limitations: tuple[str, ...]


@dataclass(frozen=True)
class GroupBehaviorState:
    industry: str
    provenance: str
    attention: float
    crowding: float
    market_attention_share: float
    concentration: float
    event_count: int
    repeat_event_ratio: float


@dataclass(frozen=True)
class BehaviorStateSnapshot:
    as_of: str
    decision_trade_date: str
    spec_hash: str
    stocks: tuple[StockBehaviorState, ...]
    groups: tuple[GroupBehaviorState, ...]
    limitations: tuple[str, ...]
    state_is_signal: bool = False


def reduce_behavior_state(
    view: PointInTimeView,
    as_of: datetime,
    trade_calendar: Iterable[str | date],
    spec: BehaviorStateSpec,
) -> BehaviorStateSnapshot:
    """Batch-read a decision-bound view, then reduce without entry/exit semantics."""
    if type(view) is not PointInTimeView:
        raise TypeError("view must be an exact decision-bound PointInTimeView")
    spec.validate()
    decision_at = _local_time(as_of)
    calendar = _calendar(trade_calendar, decision_at.date())
    decision_date = calendar[-1]
    window = calendar[-spec.lookback_trade_days :]
    positions = {value: index for index, value in enumerate(calendar)}
    start = window[0].isoformat()
    frames = {
        table: view.query(table, decision_at, where="trade_date >= ?", params=(start,))
        for table in ("limit_up_collection_runs", "zt_pool", "zb_pool", "strong_pool")
    }
    records = {
        table: _visible_records(frame.to_dict("records"), set(window), decision_at)
        for table, frame in frames.items()
    }
    usable_audits = _usable_audit_dates(records["limit_up_collection_runs"])
    limitations = {"INDUSTRY_PROXY_NOT_PIT_CONCEPT", "MINUTE_0935_UNSUPPORTED"}
    if decision_at.date() == decision_date and decision_at.time() < POST_CLOSE:
        limitations.add("INTRADAY_DAILY_OUTCOMES_EXCLUDED")
    if not usable_audits:
        limitations.add("NO_USABLE_POST_CLOSE_AUDITS")

    excluded = sum(
        row.get("trade_date") not in usable_audits
        for table in ("zt_pool", "zb_pool", "strong_pool")
        for row in records[table]
    )
    if excluded:
        limitations.add(f"UNAUDITED_POOL_ROWS_EXCLUDED:{excluded}")
    zt_rows = _latest_rows(records["zt_pool"], usable_audits)
    zb_rows = _latest_rows(records["zb_pool"], usable_audits)
    strong_rows = _latest_rows(records["strong_pool"], usable_audits)
    return _reduce_rows(
        decision_at,
        decision_date,
        calendar,
        positions,
        zt_rows,
        zb_rows,
        strong_rows,
        spec,
        limitations,
    )


def _reduce_rows(
    decision_at: datetime,
    decision_date: date,
    calendar: tuple[date, ...],
    positions: dict[date, int],
    zt_rows: list[dict[str, Any]],
    zb_rows: list[dict[str, Any]],
    strong_rows: list[dict[str, Any]],
    spec: BehaviorStateSpec,
    snapshot_limitations: set[str],
) -> BehaviorStateSnapshot:
    impulses: dict[str, list[tuple[date, float, str]]] = defaultdict(list)
    groups: dict[str, list[tuple[str, date, float]]] = defaultdict(list)
    names: dict[str, tuple[date, str]] = {}
    industries: dict[str, tuple[date, str]] = {}
    stock_limitations: dict[str, set[str]] = defaultdict(set)
    repeat_counts: dict[str, int] = defaultdict(int)
    group_event_counts: dict[str, int] = defaultdict(int)
    group_repeat_counts: dict[str, int] = defaultdict(int)
    limited_by_date: set[tuple[str, date]] = set()

    for row in sorted(zt_rows, key=_row_order):
        code, event_date = str(row["stock_code"]), date.fromisoformat(str(row["trade_date"]))
        industry = str(row.get("industry") or "").strip()
        impulse = spec.limit_up_weight
        if repeat_counts[code]:
            impulse += spec.repeat_limit_weight
        consecutive = _number(row.get("consecutive_zt"))
        if consecutive is not None and consecutive > 1:
            impulse += (consecutive - 1) * spec.consecutive_board_weight
        amount, seal_amount = _number(row.get("amount")), _number(row.get("seal_amount"))
        if amount and amount > 0 and seal_amount is not None:
            impulse += min(max(seal_amount / amount, 0.0), 1.0) * spec.seal_quality_weight
        else:
            stock_limitations[code].add("SEAL_QUALITY_UNAVAILABLE")
        impulses[code].append((event_date, impulse, industry))
        limited_by_date.add((code, event_date))
        repeat_counts[code] += 1
        if industry:
            groups[industry].append((code, event_date, impulse))
            group_event_counts[industry] += 1
            if repeat_counts[code] > 1:
                group_repeat_counts[industry] += 1
            industries[code] = max(industries.get(code, (date.min, "")), (event_date, industry))
        else:
            stock_limitations[code].add("INDUSTRY_MISSING")
        name = str(row.get("name") or "").strip()
        if name:
            names[code] = max(names.get(code, (date.min, "")), (event_date, name))

    strong_latest: dict[str, dict[str, Any]] = {}
    for row in sorted(strong_rows, key=_row_order):
        code, event_date = str(row["stock_code"]), date.fromisoformat(str(row["trade_date"]))
        strong_latest[code] = row
        industry = str(row.get("industry") or "").strip()
        if industry:
            industries[code] = max(industries.get(code, (date.min, "")), (event_date, industry))
        name = str(row.get("name") or "").strip()
        if name:
            names[code] = max(names.get(code, (date.min, "")), (event_date, name))

    current_index = positions[decision_date]
    own_now = {code: _attention(events, positions, current_index, spec) for code, events in impulses.items()}
    group_now = {
        industry: sum(_weighted(value, current_index - positions[event_date], spec) for _, event_date, value in events)
        for industry, events in groups.items()
    }
    global_attention = sum(group_now.values())
    crowding_components = {
        industry: _crowding(industry, events, group_now, own_now, global_attention, group_event_counts, group_repeat_counts)
        for industry, events in groups.items()
    }
    crowding_by_group = {industry: values[0] for industry, values in crowding_components.items()}
    universe = set(impulses)
    universe.update(
        code
        for code, row in strong_latest.items()
        if str(row.get("industry") or "").strip() in group_now
    )
    failed_by_stock: dict[str, float] = defaultdict(float)
    for row in zb_rows:
        code, event_date = str(row["stock_code"]), date.fromisoformat(str(row["trade_date"]))
        failed_by_stock[code] += _weighted(
            spec.failed_board_decay_weight,
            current_index - positions[event_date],
            spec,
        )

    stocks = []
    for code in sorted(universe):
        industry = industries.get(code, (date.min, ""))[1] or None
        own = own_now.get(code, 0.0)
        group_total = group_now.get(industry or "", 0.0)
        group_other = max(0.0, group_total - own)
        diffusion = group_other * spec.industry_diffusion_weight
        domains = []
        if code in impulses:
            domains.append("recent_limit_memory")
            if (code, decision_date) not in limited_by_date:
                domains.append("post_limit_non_limit")
        if code in strong_latest and (code, decision_date) not in limited_by_date and group_other > 0:
            domains.append("industry_diffusion_non_limit")
        latest_positive = max((item[0] for item in impulses.get(code, ())), default=None)
        age = current_index - positions[latest_positive] if latest_positive else None
        previous_index = current_index - 1
        slope = None
        if previous_index >= 0:
            own_previous = _attention(impulses.get(code, ()), positions, previous_index, spec)
            group_previous = _group_attention(groups.get(industry or "", ()), positions, previous_index, spec)
            slope = own + diffusion - (
                own_previous + max(0.0, group_previous - own_previous) * spec.industry_diffusion_weight
            )
        negatives = failed_by_stock.get(code, 0.0)
        if industry and groups.get(industry):
            latest_group_date = max(item[1] for item in groups[industry])
            group_age = current_index - positions[latest_group_date]
            if group_age > 0:
                negatives += _weighted(spec.diffusion_stop_decay_weight, group_age, spec)
        local_limitations = set(stock_limitations.get(code, ()))
        local_limitations.add("BREAKDOWN_PULSE_UNSUPPORTED")
        if slope is None:
            local_limitations.add("ATTENTION_SLOPE_UNAVAILABLE")
        provenance = {"ZT_POOL_POST_CLOSE_AUDITED"} if code in impulses else set()
        if code in strong_latest or industry:
            provenance.add(INDUSTRY_PROXY)
        if code in failed_by_stock:
            provenance.add("ZB_POOL_POST_CLOSE_AUDITED")
        stocks.append(
            StockBehaviorState(
                stock_code=code,
                name=names.get(code, (date.min, ""))[1],
                industry=industry,
                group_provenance=INDUSTRY_PROXY if industry else None,
                state_domains=tuple(domains),
                own_attention=own,
                group_attention=group_other,
                diffusion=diffusion,
                crowding=crowding_by_group.get(industry or "", 0.0),
                decay_age_trade_days=age,
                attention_slope=slope,
                negative_pulse=negatives if negatives > 0 else None,
                provenance=tuple(sorted(provenance)),
                limitations=tuple(sorted(local_limitations)),
            )
        )

    group_states = tuple(
        GroupBehaviorState(
            industry=industry,
            provenance=INDUSTRY_PROXY,
            attention=group_now[industry],
            crowding=crowding_by_group[industry],
            market_attention_share=crowding_components[industry][1],
            concentration=crowding_components[industry][3],
            event_count=group_event_counts[industry],
            repeat_event_ratio=group_repeat_counts[industry] / group_event_counts[industry],
        )
        for industry in sorted(groups)
    )
    return BehaviorStateSnapshot(
        as_of=decision_at.isoformat(timespec="seconds"),
        decision_trade_date=decision_date.isoformat(),
        spec_hash=spec.spec_hash,
        stocks=tuple(stocks),
        groups=group_states,
        limitations=tuple(sorted(snapshot_limitations)),
    )


def _calendar(values: Iterable[str | date], maximum: date) -> tuple[date, ...]:
    parsed = sorted({_date(value) for value in values})
    if not parsed or parsed[-1] > maximum:
        raise ValueError("trade_calendar must be non-empty and contain no future date")
    return tuple(parsed)


def _visible_records(rows: list[dict[str, Any]], window: set[date], as_of: datetime) -> list[dict[str, Any]]:
    visible = []
    for row in rows:
        trade_date = _date(str(row.get("trade_date") or ""))
        if trade_date not in window or trade_date > as_of.date():
            continue
        if trade_date == as_of.date() and as_of.time() < POST_CLOSE:
            continue
        visible.append(row)
    return visible


def _usable_audit_dates(rows: list[dict[str, Any]]) -> set[str]:
    latest: dict[str, dict[str, Any]] = {}
    for row in rows:
        key = str(row.get("trade_date") or "")
        rank = (str(row.get("attempted_at") or ""), int(row.get("id") or 0))
        current = latest.get(key)
        if current is None or rank > (
            str(current.get("attempted_at") or ""), int(current.get("id") or 0)
        ):
            latest[key] = row
    usable = set()
    for trade_date, row in latest.items():
        attempted = _local_time(datetime.fromisoformat(str(row.get("attempted_at"))))
        event_date = date.fromisoformat(trade_date)
        if row.get("status") == "ok" and (
            attempted.date() > event_date
            or (attempted.date() == event_date and attempted.time() >= POST_CLOSE)
        ):
            usable.add(trade_date)
    return usable


def _latest_rows(rows: list[dict[str, Any]], usable_dates: set[str]) -> list[dict[str, Any]]:
    latest: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        trade_date, code = str(row.get("trade_date") or ""), str(row.get("stock_code") or "")
        if trade_date not in usable_dates or not code:
            continue
        key = trade_date, code
        if key not in latest or str(row.get("snapshot_time") or "") > str(
            latest[key].get("snapshot_time") or ""
        ):
            latest[key] = row
    return list(latest.values())


def _attention(events, positions: dict[date, int], target: int, spec: BehaviorStateSpec) -> float:
    return sum(
        _weighted(value, target - positions[event_date], spec)
        for event_date, value, _ in events
        if positions[event_date] <= target
    )


def _group_attention(events, positions: dict[date, int], target: int, spec: BehaviorStateSpec) -> float:
    return sum(
        _weighted(value, target - positions[event_date], spec)
        for _, event_date, value in events
        if positions[event_date] <= target
    )


def _weighted(value: float, age: int, spec: BehaviorStateSpec) -> float:
    return value * 2 ** (-age / spec.half_life_trade_days)


def _crowding(industry, events, group_now, own_now, global_attention, counts, repeats) -> tuple[float, float, float, float]:
    group_attention = group_now[industry]
    share = group_attention / global_attention if global_attention else 0.0
    repeat_ratio = repeats[industry] / counts[industry]
    concentration = max((own_now.get(code, 0.0) for code, _, _ in events), default=0.0)
    concentration = concentration / group_attention if group_attention else 0.0
    score = min(1.0, max(0.0, (share + repeat_ratio + concentration) / 3))
    return score, share, repeat_ratio, concentration


def _row_order(row: dict[str, Any]) -> tuple[str, str, str]:
    return str(row.get("trade_date") or ""), str(row.get("stock_code") or ""), str(row.get("snapshot_time") or "")


def _date(value: str | date) -> date:
    return value if isinstance(value, date) else date.fromisoformat(value)


def _local_time(value: datetime) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError("as_of must be datetime")
    return value.replace(tzinfo=SHANGHAI) if value.tzinfo is None else value.astimezone(SHANGHAI)


def _number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None
