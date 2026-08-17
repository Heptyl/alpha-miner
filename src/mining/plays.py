"""Programmatic builders for the three initial play families.

The implemented PAPER plays share this module rather than creating parallel
engines or one module per play.
"""

from __future__ import annotations

import math
import sqlite3
from collections import defaultdict
from dataclasses import replace
from datetime import date, datetime, time
from typing import Any
from zoneinfo import ZoneInfo

from src.data.limit_up_history import (
    MAX_LIMIT_UP_ROWS,
    MIN_LIMIT_UP_ROWS,
)
from src.mining.playbook import PlayCard, PlayCardStorage, load_pending_play_cards

THREE_TO_FOUR_PLAY_ID = "three_to_four_reseal"
THEME_NEW_ENTRANT_PLAY_ID = "theme_new_entrant_diffusion_v1"
TERMINAL_CANDIDATE_STATUSES = frozenset({"NOT_TRIGGERED", "UNFILLED", "COMPLETED"})

_ORDINARY_STOCK_PREFIXES = (
    "000",
    "001",
    "002",
    "003",
    "300",
    "301",
    "600",
    "601",
    "603",
    "605",
    "688",
    "689",
)
_SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")
_POST_CLOSE_AUDIT_TIME = time(15, 40)
LEGACY_MIN_MARKET_ROWS = 4_000


def build_theme_new_entrant_diffusion_card(
    storage: PlayCardStorage,
    signal_date: str | None = None,
    generated_at: str | datetime | None = None,
    total_cost_bps: float = 20,
) -> PlayCard:
    """Build the frozen D-close H1 PAPER plan without reading future prices."""
    signal_date = _resolve_signal_date(storage, signal_date)
    total_cost_bps = _validate_total_cost_bps(total_cost_bps)
    if signal_date not in load_usable_audit_dates(storage):
        raise ValueError(f"signal date {signal_date} has no successful collection audit")

    previous_date = _previous_market_date(storage, signal_date)
    empty_reason = ""
    previous_day_audit_source = "UNAVAILABLE"
    if previous_date is None:
        empty_reason = "缺少精确前一交易日，无法判断行业涨停宽度是否加速"
    else:
        resolved_source = _previous_day_audit_source(storage, previous_date)
        if resolved_source is None:
            empty_reason = f"精确前一交易日{previous_date}缺少可信盘后证据，未生成候选"
        else:
            previous_day_audit_source = resolved_source

    current_zt = _latest_pool_rows(storage, "zt_pool", signal_date)
    current_strong = _latest_pool_rows(storage, "strong_pool", signal_date)
    previous_zt = (
        _latest_pool_rows(storage, "zt_pool", previous_date) if previous_date else []
    )
    previous_strong = (
        _latest_pool_rows(storage, "strong_pool", previous_date) if previous_date else []
    )
    if not empty_reason and not current_zt:
        empty_reason = "本日成功审计但涨停池为空，无法形成行业宽度信号"
    if not empty_reason and not current_strong:
        empty_reason = "本日强势池为空，本日0只符合条件的候选"

    candidates: list[dict[str, Any]] = []
    accelerated_industries: dict[str, tuple[int, int]] = {}
    if not empty_reason:
        previous_breadth = _industry_breadth(previous_zt)
        current_breadth = _industry_breadth(current_zt)
        accelerated_industries = {
            industry: (previous_breadth.get(industry, 0), breadth)
            for industry, breadth in current_breadth.items()
            if breadth >= 3 and breadth > previous_breadth.get(industry, 0)
        }
        current_zt_codes = {str(row["stock_code"]) for row in current_zt}
        previous_strong_codes = {str(row["stock_code"]) for row in previous_strong}
        ranked_by_industry: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in current_strong:
            code = str(row.get("stock_code") or "")
            name = str(row.get("name") or "")
            industry = str(row.get("industry") or "").strip()
            amount = _finite_float(row.get("amount"))
            if (
                industry not in accelerated_industries
                or code in current_zt_codes
                or code in previous_strong_codes
                or _is_st_stock(name)
                or not _is_ordinary_stock(code)
                or amount is None
            ):
                continue
            ranked_by_industry[industry].append(row)

        for industry in sorted(ranked_by_industry):
            ranked = sorted(
                ranked_by_industry[industry],
                key=lambda row: (-float(row["amount"]), str(row["stock_code"])),
            )
            top = ranked[0]
            code = str(top["stock_code"])
            signal_close = _signal_close(storage, code, signal_date)
            # The preregistered rank is never backfilled with rank 2.
            if signal_close is None:
                continue
            previous_count, current_count = accelerated_industries[industry]
            candidates.append(
                {
                    "stock_code": code,
                    "stock_name": str(top.get("name") or ""),
                    "industry": industry,
                    "paper_status": "PLANNED",
                    "signal_close": signal_close,
                    "allowed_open_low": round(signal_close * 0.98, 4),
                    "allowed_open_high": round(signal_close * 1.05, 4),
                    "previous_zt_breadth": previous_count,
                    "current_zt_breadth": current_count,
                    "signal_amount": float(top["amount"]),
                    "selection_reason": (
                        f"{industry}涨停宽度{previous_count}→{current_count}加速；"
                        "本日新进入强势池；行业成交额排名1"
                    ),
                    "abandon_conditions": (
                        "D+1开盘相对D收盘低于-2%或高于+5%、涨停开盘、无有效报价均不成交"
                    ),
                }
            )

    if not candidates and not empty_reason:
        empty_reason = "本日0只同时满足宽度加速、新进入强势池和行业成交额排名1"
    evidence = {
        "research_status": "DEVELOPMENT_CANDIDATE",
        "usage_status": "PAPER_ONLY",
        "independent_signal_days": 12,
        "development_mean_net_return_pct": 0.5530,
        "development_ci95_pct": [-0.9914, 2.0835],
        "holm_significant": False,
        "late_period_mean_net_return_pct": 0.1299,
        "current_candidate_count": len(candidates),
        "previous_day_audit_source": previous_day_audit_source,
        "total_cost_bps": total_cost_bps,
        "entry_proxy": "候选在D日收盘冻结；D+1开盘仅按预设[-2%, +5%]区间判断PAPER成交",
        "exit_proxy": "固定D+3开盘，即D+1入场后的第二个后续市场开盘",
        "data_limitations": (
            "development仅12个独立收益日，95%区间跨0且Holm校正不显著；"
            "后段均值仅+0.1299%，不能展示为胜率优势或实盘发现。"
            "前一交易日若标记LEGACY_POST_CLOSE_SNAPSHOT，仅表示旧版三表盘后快照"
            "通过完整性门槛，并非补写或伪造显式采集审计。"
        ),
        "empty_reason": empty_reason,
    }
    card = PlayCard(
        play_id=THEME_NEW_ENTRANT_PLAY_ID,
        play_name="热点扩散新强势成员（H1）",
        behavior_logic=(
            "行业涨停宽度相对精确前一交易日继续扩张时，注意力可能扩散到尚未涨停、"
            "但新进入强势池且成交额领先的普通股票。"
        ),
        signal_trade_date=signal_date,
        candidates=candidates,
        trigger_rule=(
            "D日收盘冻结候选；D+1开盘相对D收盘位于[-2%, +5%]且非涨停开盘时，"
            "按开盘价记录PAPER模拟买入，不得使用D+1数据重选。"
        ),
        abandon_rule=(
            "D+1开盘低于-2%或高于+5%、涨停开盘、无报价即记录未成交；"
            "不回填同一行业排名2及以后股票。"
        ),
        exit_rule=(
            "固定D+3开盘模拟卖出，即D+1入场后的第二个后续市场开盘；"
            f"完整往返扣除{total_cost_bps:g}bp成本。"
        ),
        historical_evidence=evidence,
        paper_status="PLANNED",
        admission_status="NOT_ADMITTED",
        generated_at=_resolve_generated_at(generated_at),
    )
    card.validate()
    return card


def build_three_to_four_card(
    storage: PlayCardStorage,
    signal_date: str | None = None,
    generated_at: str | datetime | None = None,
    total_cost_bps: float = 20,
) -> PlayCard:
    """Build, but do not persist, the three-to-four tradable-reseal PAPER card.

    ``signal_date`` is D-1. Current candidates come only from that day's latest
    ``zt_pool`` snapshots. Historical outcomes use strictly earlier rows.
    """
    signal_date = _resolve_signal_date(storage, signal_date)
    if not isinstance(total_cost_bps, (int, float)) or not math.isfinite(total_cost_bps):
        raise ValueError("total_cost_bps must be a finite number")
    if total_cost_bps < 0:
        raise ValueError("total_cost_bps must be non-negative")

    current_rows = storage.execute(
        """
        SELECT stock_code, name, consecutive_zt, snapshot_time
        FROM zt_pool
        WHERE trade_date = ?
        ORDER BY stock_code, snapshot_time
        """,
        (signal_date,),
    )
    current_latest = _dedupe_latest(current_rows, ("stock_code",))
    candidates = sorted(
        (
            {
                "stock_code": str(row["stock_code"]),
                "stock_name": str(row.get("name") or ""),
                "board_count": 3,
                "paper_status": "PLANNED",
            }
            for row in current_latest.values()
            if _as_int(row.get("consecutive_zt")) == 3
        ),
        key=lambda item: (item["stock_code"], item["stock_name"]),
    )

    evidence = _build_three_to_four_evidence(storage, signal_date, float(total_cost_bps))
    card = PlayCard(
        play_id=THREE_TO_FOUR_PLAY_ID,
        play_name="三进四可成交回封",
        behavior_logic=(
            "连续三板形成全市场注意力瀑布；第四板经历分歧后回封，"
            "用于检验接力资金重新达成一致是否存在成本后优势。"
        ),
        signal_trade_date=signal_date,
        candidates=candidates,
        trigger_rule=(
            "D-1仅列三板候选；下一市场交易日D成为四板、open_count>=1，"
            "且非一字板并有成交量时，按D日涨停收盘价代理模拟PAPER打板入场。"
        ),
        abandon_rule=(
            "D日未成为四板、未开板回封、字段不足、一字板、无量或封单队列不可达均放弃，"
            "不得改用D+1开盘追入。"
        ),
        exit_rule=(
            "遵守T+1，入场后的下一市场交易日D+1以开盘价代理退出；"
            f"完整往返收益扣除{float(total_cost_bps):g}bp成本。"
        ),
        historical_evidence=evidence,
        paper_status="PLANNED",
        admission_status="NOT_ADMITTED",
        generated_at=_resolve_generated_at(generated_at),
    )
    card.validate()
    return card


def settle_three_to_four_cards(
    storage: PlayCardStorage,
    total_cost_bps: float = 20,
) -> list[PlayCard]:
    """Advance pending PAPER candidates using only successfully audited days.

    The returned cards are changed copies; callers own persistence so the
    collection command can surface any write failure as a non-zero exit.
    """
    fallback_cost = _validate_total_cost_bps(total_cost_bps)
    cards = load_pending_play_cards(storage, THREE_TO_FOUR_PLAY_ID)
    if not cards:
        return []

    successful_dates = load_usable_audit_dates(storage)
    market_dates = [
        str(row["trade_date"])
        for row in storage.execute(
            "SELECT DISTINCT trade_date FROM daily_price ORDER BY trade_date"
        )
    ]
    changed_cards: list[PlayCard] = []
    for card in cards:
        entry_date = _next_market_date(market_dates, card.signal_trade_date)
        updated_candidates = [
            _settle_candidate(
                storage,
                candidate,
                entry_date,
                market_dates,
                successful_dates,
                _card_total_cost_bps(card, fallback_cost),
            )
            for candidate in card.candidates
        ]
        paper_status = _card_paper_status(updated_candidates, entry_date, successful_dates)
        if updated_candidates == card.candidates and paper_status == card.paper_status:
            continue
        updated_card = replace(
            card,
            candidates=updated_candidates,
            paper_status=paper_status,
        )
        updated_card.validate()
        changed_cards.append(updated_card)
    return changed_cards


def settle_theme_new_entrant_diffusion_cards(
    storage: PlayCardStorage,
    total_cost_bps: float = 20,
) -> list[PlayCard]:
    """Advance frozen H1 candidates without selecting from forward data."""
    fallback_cost = _validate_total_cost_bps(total_cost_bps)
    cards = load_pending_play_cards(storage, THEME_NEW_ENTRANT_PLAY_ID)
    if not cards:
        return []
    successful_dates = load_usable_audit_dates(storage)
    market_dates = [
        str(row["trade_date"])
        for row in storage.execute(
            "SELECT DISTINCT trade_date FROM daily_price ORDER BY trade_date"
        )
    ]
    changed_cards: list[PlayCard] = []
    for card in cards:
        entry_date = _next_market_date(market_dates, card.signal_trade_date)
        cost_bps = _card_total_cost_bps(card, fallback_cost)
        updated_candidates = [
            _settle_theme_candidate(
                storage,
                candidate,
                entry_date,
                market_dates,
                successful_dates,
                cost_bps,
            )
            for candidate in card.candidates
        ]
        paper_status = _card_paper_status(updated_candidates, entry_date, successful_dates)
        if updated_candidates == card.candidates and paper_status == card.paper_status:
            continue
        updated = replace(card, candidates=updated_candidates, paper_status=paper_status)
        updated.validate()
        changed_cards.append(updated)
    return changed_cards


def _settle_theme_candidate(
    storage: PlayCardStorage,
    candidate: dict[str, Any],
    entry_date: str | None,
    market_dates: list[str],
    successful_dates: set[str],
    total_cost_bps: float,
) -> dict[str, Any]:
    updated = dict(candidate)
    status = str(updated.get("paper_status") or "PLANNED")
    updated["paper_status"] = status
    if status in TERMINAL_CANDIDATE_STATUSES:
        return updated
    code = str(updated.get("stock_code") or "")
    if status == "PLANNED":
        if entry_date is None or entry_date not in successful_dates:
            return updated
        row = _latest_stock_date_row(
            storage,
            "daily_price",
            "stock_code, trade_date, open, high, low, close, snapshot_time",
            code,
            entry_date,
        )
        entry_price = _positive_float(row.get("open")) if row else None
        signal_close = _positive_float(updated.get("signal_close"))
        if entry_price is None or signal_close is None:
            return _terminal_candidate(updated, "UNFILLED", entry_date, "D+1开盘无有效报价")
        gap_pct = (entry_price / signal_close - 1) * 100
        if gap_pct < -2 or gap_pct > 5:
            return _terminal_candidate(
                updated,
                "UNFILLED",
                entry_date,
                f"D+1开盘缺口{gap_pct:+.4f}%超出[-2%, +5%]",
            )
        if _is_one_price_row(row):
            return _terminal_candidate(updated, "UNFILLED", entry_date, "D+1涨停式一字开盘，代理不可成交")
        updated.update(
            {
                "paper_status": "TRIGGERED",
                "entry_trade_date": entry_date,
                "entry_price": entry_price,
                "entry_proxy": "D+1开盘价",
                "entry_gap_pct": gap_pct,
                "result_reason": "预冻结候选在允许开盘区间内，已记录PAPER模拟买入",
            }
        )

    if updated["paper_status"] != "TRIGGERED":
        return updated
    recorded_entry_date = updated.get("entry_trade_date")
    entry_price = _positive_float(updated.get("entry_price"))
    if not isinstance(recorded_entry_date, str) or entry_price is None:
        raise ValueError(f"TRIGGERED candidate {code!r} is missing its entry proxy")
    first_following = _next_market_date(market_dates, recorded_entry_date)
    exit_date = (
        _next_market_date(market_dates, first_following) if first_following else None
    )
    if exit_date is None or exit_date not in successful_dates:
        return updated
    exit_row = _latest_stock_date_row(
        storage,
        "daily_price",
        "stock_code, trade_date, open, snapshot_time",
        code,
        exit_date,
    )
    exit_price = _positive_float(exit_row.get("open")) if exit_row else None
    if exit_price is None:
        return updated
    net_return_pct = (exit_price / entry_price - 1) * 100 - total_cost_bps / 100
    updated.update(
        {
            "paper_status": "COMPLETED",
            "exit_trade_date": exit_date,
            "exit_price": exit_price,
            "exit_proxy": "D+3开盘价",
            "total_cost_bps": total_cost_bps,
            "net_return_pct": net_return_pct,
            "result_reason": "已按D+3开盘价完成PAPER模拟卖出",
        }
    )
    return updated


def _settle_candidate(
    storage: PlayCardStorage,
    candidate: dict[str, Any],
    entry_date: str | None,
    market_dates: list[str],
    successful_dates: set[str],
    total_cost_bps: float,
) -> dict[str, Any]:
    updated = dict(candidate)
    status = str(updated.get("paper_status") or "PLANNED")
    updated["paper_status"] = status
    if status in TERMINAL_CANDIDATE_STATUSES:
        return updated

    code = str(updated.get("stock_code") or "")
    if status == "PLANNED":
        if entry_date is None or entry_date not in successful_dates:
            return updated
        zt_row = _latest_stock_date_row(
            storage,
            "zt_pool",
            "stock_code, trade_date, consecutive_zt, open_count, snapshot_time",
            code,
            entry_date,
        )
        if zt_row is None or _as_int(zt_row.get("consecutive_zt")) != 4:
            return _terminal_candidate(
                updated,
                "NOT_TRIGGERED",
                entry_date,
                "D日未成为四板",
            )
        open_count = _as_int(zt_row.get("open_count"))
        if open_count is None:
            return _terminal_candidate(
                updated,
                "UNFILLED",
                entry_date,
                "D日开板次数字段不足，代理不可成交",
            )
        if open_count < 1:
            return _terminal_candidate(
                updated,
                "NOT_TRIGGERED",
                entry_date,
                "D日四板但未开板回封",
            )
        price_row = _latest_stock_date_row(
            storage,
            "daily_price",
            "stock_code, trade_date, high, low, close, volume, snapshot_time",
            code,
            entry_date,
        )
        entry_price = _tradable_reseal_close(price_row)
        if entry_price is None:
            return _terminal_candidate(
                updated,
                "UNFILLED",
                entry_date,
                _unfilled_reason(price_row),
            )
        updated.update(
            {
                "paper_status": "TRIGGERED",
                "entry_trade_date": entry_date,
                "entry_price": entry_price,
                "entry_proxy": "D日涨停收盘价",
                "result_reason": "D日四板开板回封，PAPER代理成交",
            }
        )

    if updated["paper_status"] != "TRIGGERED":
        return updated
    recorded_entry_date = updated.get("entry_trade_date")
    entry_price = _positive_float(updated.get("entry_price"))
    if not isinstance(recorded_entry_date, str) or entry_price is None:
        raise ValueError(f"TRIGGERED candidate {code!r} is missing its entry proxy")
    exit_date = _next_market_date(market_dates, recorded_entry_date)
    if exit_date is None or exit_date not in successful_dates:
        return updated
    exit_row = _latest_stock_date_row(
        storage,
        "daily_price",
        "stock_code, trade_date, open, snapshot_time",
        code,
        exit_date,
    )
    exit_price = _positive_float(exit_row.get("open")) if exit_row else None
    if exit_price is None:
        return updated
    net_return_pct = (exit_price / entry_price - 1) * 100 - total_cost_bps / 100
    updated.update(
        {
            "paper_status": "COMPLETED",
            "exit_trade_date": exit_date,
            "exit_price": exit_price,
            "exit_proxy": "D+1开盘价",
            "total_cost_bps": total_cost_bps,
            "net_return_pct": net_return_pct,
            "result_reason": "已按D+1开盘价完成PAPER模拟卖出",
        }
    )
    return updated


def _terminal_candidate(
    candidate: dict[str, Any],
    status: str,
    result_date: str,
    reason: str,
) -> dict[str, Any]:
    candidate.update(
        {
            "paper_status": status,
            "result_trade_date": result_date,
            "result_reason": reason,
        }
    )
    return candidate


def _latest_stock_date_row(
    storage: PlayCardStorage,
    table: str,
    columns: str,
    stock_code: str,
    trade_date: str,
) -> dict[str, Any] | None:
    if table not in {"zt_pool", "daily_price"}:
        raise ValueError(f"unsupported lifecycle table: {table}")
    rows = storage.execute(
        f"""
        SELECT {columns}
        FROM {table}
        WHERE stock_code = ? AND trade_date = ?
        ORDER BY snapshot_time DESC
        LIMIT 1
        """,
        (stock_code, trade_date),
    )
    return rows[0] if rows else None


def load_usable_audit_dates(storage: PlayCardStorage) -> set[str]:
    """Return dates whose latest collection audit is successful and post-close."""
    rows = storage.execute(
        """
        SELECT r.trade_date, r.attempted_at, r.status
        FROM limit_up_collection_runs AS r
        WHERE r.id = (
            SELECT newer.id
            FROM limit_up_collection_runs AS newer
            WHERE newer.trade_date = r.trade_date
            ORDER BY newer.attempted_at DESC, newer.id DESC
            LIMIT 1
        )
        ORDER BY r.trade_date
        """
    )
    usable: set[str] = set()
    for row in rows:
        trade_date = str(row.get("trade_date") or "")
        if row.get("status") != "ok" or not _is_post_close_attempt(
            trade_date, row.get("attempted_at")
        ):
            continue
        usable.add(trade_date)
    return usable


def _previous_day_audit_source(
    storage: PlayCardStorage,
    trade_date: str,
) -> str | None:
    """Resolve explicit audit first; only an entirely audit-less day may use legacy proof."""
    audit_rows = storage.execute(
        "SELECT 1 AS found FROM limit_up_collection_runs WHERE trade_date = ? LIMIT 1",
        (trade_date,),
    )
    if audit_rows:
        return "EXPLICIT_AUDIT" if trade_date in load_usable_audit_dates(storage) else None
    return "LEGACY_POST_CLOSE_SNAPSHOT" if _has_legacy_post_close_snapshot(
        storage, trade_date
    ) else None


def _has_legacy_post_close_snapshot(
    storage: PlayCardStorage,
    trade_date: str,
) -> bool:
    requirements = {
        "daily_price": (LEGACY_MIN_MARKET_ROWS, None),
        "zt_pool": (MIN_LIMIT_UP_ROWS, MAX_LIMIT_UP_ROWS),
        "strong_pool": (1, None),
    }
    try:
        for table, (minimum, maximum) in requirements.items():
            rows = storage.execute(
                f"SELECT stock_code, snapshot_time FROM {table} WHERE trade_date = ?",
                (trade_date,),
            )
            distinct_codes = {str(row.get("stock_code") or "") for row in rows}
            distinct_codes.discard("")
            if len(distinct_codes) < minimum:
                return False
            if maximum is not None and len(distinct_codes) > maximum:
                return False
            if not all(
                _is_same_day_post_close_snapshot(trade_date, row.get("snapshot_time"))
                for row in rows
            ):
                return False
    except (KeyError, TypeError, ValueError, sqlite3.Error):
        return False
    return True


def _is_same_day_post_close_snapshot(trade_date: str, snapshot_time: Any) -> bool:
    try:
        signal_day = date.fromisoformat(trade_date)
        parsed = datetime.fromisoformat(str(snapshot_time).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return False
    if parsed.tzinfo is None:
        local = parsed.replace(tzinfo=_SHANGHAI_TZ)
    else:
        local = parsed.astimezone(_SHANGHAI_TZ)
    return (
        local.date() == signal_day
        and local.time().replace(tzinfo=None) >= _POST_CLOSE_AUDIT_TIME
    )


def _is_post_close_attempt(trade_date: str, attempted_at: Any) -> bool:
    try:
        signal_day = date.fromisoformat(trade_date)
        parsed = datetime.fromisoformat(str(attempted_at).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return False
    if parsed.tzinfo is None:
        local = parsed.replace(tzinfo=_SHANGHAI_TZ)
    else:
        local = parsed.astimezone(_SHANGHAI_TZ)
    if local.date() > signal_day:
        return True
    if local.date() < signal_day:
        return False
    return local.time().replace(tzinfo=None) >= _POST_CLOSE_AUDIT_TIME


def _previous_market_date(storage: PlayCardStorage, trade_date: str) -> str | None:
    rows = storage.execute(
        "SELECT MAX(trade_date) AS trade_date FROM daily_price WHERE trade_date < ?",
        (trade_date,),
    )
    value = rows[0].get("trade_date") if rows else None
    return str(value) if value else None


def _latest_pool_rows(
    storage: PlayCardStorage,
    table: str,
    trade_date: str,
) -> list[dict[str, Any]]:
    if table == "zt_pool":
        columns = "stock_code, name, amount, industry, snapshot_time"
    elif table == "strong_pool":
        columns = "stock_code, name, amount, industry, snapshot_time"
    else:
        raise ValueError(f"unsupported pool table: {table}")
    rows = storage.execute(
        f"""
        SELECT {columns}
        FROM {table}
        WHERE trade_date = ?
        ORDER BY stock_code, snapshot_time
        """,
        (trade_date,),
    )
    return list(_dedupe_latest(rows, ("stock_code",)).values())


def _industry_breadth(rows: list[dict[str, Any]]) -> dict[str, int]:
    codes: dict[str, set[str]] = defaultdict(set)
    for row in rows:
        industry = str(row.get("industry") or "").strip()
        code = str(row.get("stock_code") or "")
        if industry and code:
            codes[industry].add(code)
    return {industry: len(stock_codes) for industry, stock_codes in codes.items()}


def _signal_close(
    storage: PlayCardStorage,
    stock_code: str,
    trade_date: str,
) -> float | None:
    row = _latest_stock_date_row(
        storage,
        "daily_price",
        "stock_code, trade_date, close, snapshot_time",
        stock_code,
        trade_date,
    )
    return _positive_float(row.get("close")) if row else None


def _is_st_stock(name: str) -> bool:
    return "ST" in name.upper()


def _is_ordinary_stock(code: str) -> bool:
    return len(code) == 6 and code.isdigit() and code.startswith(_ORDINARY_STOCK_PREFIXES)


def _is_one_price_row(row: dict[str, Any] | None) -> bool:
    if row is None:
        return False
    open_price = _positive_float(row.get("open"))
    high = _positive_float(row.get("high"))
    low = _positive_float(row.get("low"))
    return (
        open_price is not None
        and high is not None
        and low is not None
        and math.isclose(open_price, high, rel_tol=1e-9, abs_tol=1e-9)
        and math.isclose(high, low, rel_tol=1e-9, abs_tol=1e-9)
    )


def _next_market_date(market_dates: list[str], trade_date: str) -> str | None:
    return next((value for value in market_dates if value > trade_date), None)


def _card_paper_status(
    candidates: list[dict[str, Any]],
    entry_date: str | None,
    successful_dates: set[str],
) -> str:
    statuses = {str(candidate.get("paper_status") or "PLANNED") for candidate in candidates}
    if "PLANNED" in statuses:
        return "PLANNED"
    if "TRIGGERED" in statuses:
        return "TRIGGERED"
    if not candidates and (entry_date is None or entry_date not in successful_dates):
        return "PLANNED"
    return "COMPLETED"


def _validate_total_cost_bps(value: float) -> float:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise ValueError("total_cost_bps must be a finite number")
    parsed = float(value)
    if not math.isfinite(parsed) or parsed < 0:
        raise ValueError("total_cost_bps must be a non-negative finite number")
    return parsed


def _card_total_cost_bps(card: PlayCard, fallback: float) -> float:
    value = card.historical_evidence.get("total_cost_bps", fallback)
    return _validate_total_cost_bps(value)


def _unfilled_reason(row: dict[str, Any] | None) -> str:
    if row is None:
        return "D日行情字段不足，代理不可成交"
    if _positive_float(row.get("volume")) is None:
        return "D日无量或成交量字段不足，代理不可成交"
    high = _positive_float(row.get("high"))
    low = _positive_float(row.get("low"))
    close = _positive_float(row.get("close"))
    if None in (high, low, close):
        return "D日价格字段不足，代理不可成交"
    if math.isclose(high, low, rel_tol=1e-9, abs_tol=1e-9):
        return "D日一字板，代理不可成交"
    return "D日队列代理不可达，未成交"


def _resolve_signal_date(storage: PlayCardStorage, signal_date: str | None) -> str:
    if signal_date is None:
        signal_date = None
        for audited_date in sorted(load_usable_audit_dates(storage), reverse=True):
            rows = storage.execute(
                "SELECT 1 AS found FROM zt_pool WHERE trade_date = ? LIMIT 1",
                (audited_date,),
            )
            if rows:
                signal_date = audited_date
                break
        if not signal_date:
            raise ValueError("no successfully audited zt_pool signal trading date")
    try:
        parsed = date.fromisoformat(signal_date)
    except (TypeError, ValueError) as exc:
        raise ValueError("signal_date must be YYYY-MM-DD") from exc
    if parsed.isoformat() != signal_date:
        raise ValueError("signal_date must be YYYY-MM-DD")
    return signal_date


def _resolve_generated_at(value: str | datetime | None) -> str:
    if value is None:
        return datetime.now().astimezone().isoformat(timespec="seconds")
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, str):
        return value
    raise ValueError("generated_at must be an ISO-8601 datetime")


def _build_three_to_four_evidence(
    storage: PlayCardStorage,
    signal_date: str,
    total_cost_bps: float,
) -> dict[str, Any]:
    zt_rows = storage.execute(
        """
        SELECT stock_code, trade_date, name, consecutive_zt, open_count, snapshot_time
        FROM zt_pool
        WHERE trade_date < ?
        ORDER BY trade_date, stock_code, snapshot_time
        """,
        (signal_date,),
    )
    price_rows = storage.execute(
        """
        SELECT stock_code, trade_date, open, high, low, close, volume, snapshot_time
        FROM daily_price
        WHERE trade_date < ?
        ORDER BY trade_date, stock_code, snapshot_time
        """,
        (signal_date,),
    )
    zt_latest = _dedupe_latest(zt_rows, ("trade_date", "stock_code"))
    price_latest = _dedupe_latest(price_rows, ("trade_date", "stock_code"))
    market_dates = sorted({str(row["trade_date"]) for row in price_latest.values()})
    date_index = {trade_date: index for index, trade_date in enumerate(market_dates)}

    historical_signals = sorted(
        (
            row
            for row in zt_latest.values()
            if _as_int(row.get("consecutive_zt")) == 3
        ),
        key=lambda row: (str(row["trade_date"]), str(row["stock_code"])),
    )
    signal_days = {str(row["trade_date"]) for row in historical_signals}
    proxy_trigger_count = 0
    completed_count = 0
    unfinished_count = 0
    untradable_count = 0
    net_returns_by_signal_day: dict[str, list[float]] = defaultdict(list)

    for signal in historical_signals:
        sample_signal_date = str(signal["trade_date"])
        code = str(signal["stock_code"])
        index = date_index.get(sample_signal_date)
        if index is None or index + 1 >= len(market_dates):
            unfinished_count += 1
            continue

        entry_date = market_dates[index + 1]
        entry_zt = zt_latest.get((entry_date, code))
        if entry_zt is None or _as_int(entry_zt.get("consecutive_zt")) != 4:
            continue
        open_count = _as_int(entry_zt.get("open_count"))
        if open_count is None:
            untradable_count += 1
            continue
        if open_count < 1:
            continue

        entry_price_row = price_latest.get((entry_date, code))
        entry_price = _tradable_reseal_close(entry_price_row)
        if entry_price is None:
            untradable_count += 1
            continue

        proxy_trigger_count += 1
        if index + 2 >= len(market_dates):
            unfinished_count += 1
            continue
        exit_date = market_dates[index + 2]
        if exit_date >= signal_date:
            unfinished_count += 1
            continue
        exit_row = price_latest.get((exit_date, code))
        exit_price = _positive_float(exit_row.get("open")) if exit_row else None
        if exit_price is None:
            unfinished_count += 1
            continue

        net_return_pct = (exit_price / entry_price - 1) * 100 - total_cost_bps / 100
        net_returns_by_signal_day[sample_signal_date].append(net_return_pct)
        completed_count += 1

    daily_returns = [
        sum(net_returns_by_signal_day[day]) / len(net_returns_by_signal_day[day])
        for day in sorted(net_returns_by_signal_day)
    ]
    metrics_available = bool(daily_returns)
    wins = [value for value in daily_returns if value > 0]
    losses = [value for value in daily_returns if value <= 0]
    win_rate = len(wins) / len(daily_returns) if daily_returns else 0.0
    avg_net_return = sum(daily_returns) / len(daily_returns) if daily_returns else 0.0
    profit_loss_ratio = _profit_loss_ratio(wins, losses)

    return {
        "signal_days": len(signal_days),
        "candidate_count": len(historical_signals),
        "proxy_trigger_count": proxy_trigger_count,
        "completed_count": completed_count,
        "unfinished_count": unfinished_count,
        "untradable_count": untradable_count,
        "trigger_rate": proxy_trigger_count / len(historical_signals) if historical_signals else 0.0,
        "win_rate": win_rate,
        "avg_net_return_pct": avg_net_return,
        "profit_loss_ratio": profit_loss_ratio,
        "max_drawdown_pct": _max_drawdown_pct(daily_returns),
        "total_cost_bps": total_cost_bps,
        "metrics_available": metrics_available,
        "entry_proxy": "D日四板开板回封后，以D日涨停收盘价代理PAPER入场；不是D+1开盘",
        "exit_proxy": "遵守T+1，以入场后下一市场交易日D+1开盘价代理退出",
        "data_limitations": (
            "日线zt_pool的open_count与OHLCV仅是盘中回封和可成交性的保守代理；"
            "无法还原真实封单队列、委托延迟和部分成交。指标按信号日等权；"
            "无已完成信号日时统计指标置0且metrics_available=false。"
        ),
    }


def _dedupe_latest(
    rows: list[dict[str, Any]],
    key_fields: tuple[str, ...],
) -> dict[tuple[str, ...], dict[str, Any]]:
    latest: dict[tuple[str, ...], dict[str, Any]] = {}
    for row in rows:
        key = tuple(str(row[field]) for field in key_fields)
        existing = latest.get(key)
        if existing is None or str(row.get("snapshot_time") or "") >= str(
            existing.get("snapshot_time") or ""
        ):
            latest[key] = row
    return latest


def _as_int(value: Any) -> int | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(parsed):
        return None
    return int(parsed)


def _positive_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(parsed) or parsed <= 0:
        return None
    return parsed


def _finite_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _tradable_reseal_close(row: dict[str, Any] | None) -> float | None:
    if row is None:
        return None
    high = _positive_float(row.get("high"))
    low = _positive_float(row.get("low"))
    close = _positive_float(row.get("close"))
    volume = _positive_float(row.get("volume"))
    if None in (high, low, close, volume):
        return None
    if math.isclose(high, low, rel_tol=1e-9, abs_tol=1e-9):
        return None
    return close


def _profit_loss_ratio(wins: list[float], losses: list[float]) -> float:
    if not wins or not losses:
        return 0.0
    avg_win = sum(wins) / len(wins)
    avg_loss = abs(sum(losses) / len(losses))
    return avg_win / avg_loss if avg_loss > 0 else 0.0


def _max_drawdown_pct(daily_returns: list[float]) -> float:
    equity = 1.0
    peak = 1.0
    maximum = 0.0
    for value in daily_returns:
        equity *= 1 + value / 100
        peak = max(peak, equity)
        if peak > 0:
            maximum = max(maximum, (peak - equity) / peak * 100)
    return maximum
