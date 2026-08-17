"""Programmatic builders for the three initial play families.

Only the three-to-four tradable-reseal PAPER play is implemented today. Future
initial plays belong in this module rather than in parallel engines or modules.
"""

from __future__ import annotations

import math
from collections import defaultdict
from dataclasses import replace
from datetime import date, datetime
from typing import Any

from src.mining.playbook import PlayCard, PlayCardStorage, load_pending_play_cards

THREE_TO_FOUR_PLAY_ID = "three_to_four_reseal"
TERMINAL_CANDIDATE_STATUSES = frozenset({"NOT_TRIGGERED", "UNFILLED", "COMPLETED"})


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

    successful_dates = {
        str(row["trade_date"])
        for row in storage.execute(
            """
            SELECT DISTINCT trade_date
            FROM limit_up_collection_runs
            WHERE status = 'ok'
            ORDER BY trade_date
            """
        )
    }
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
        rows = storage.execute(
            """
            SELECT MAX(r.trade_date) AS trade_date
            FROM limit_up_collection_runs AS r
            WHERE r.status = 'ok'
              AND EXISTS (
                  SELECT 1 FROM zt_pool AS z WHERE z.trade_date = r.trade_date
              )
            """
        )
        signal_date = rows[0].get("trade_date") if rows else None
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
