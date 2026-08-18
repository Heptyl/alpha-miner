"""Programmatic builders for the three initial play families.

The implemented PAPER plays share this module rather than creating parallel
engines or one module per play.
"""

from __future__ import annotations

import math
import random
import sqlite3
from collections import defaultdict
from dataclasses import dataclass, replace
from datetime import date, datetime, time, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from src.data.limit_up_history import (
    MAX_LIMIT_UP_ROWS,
    MIN_LIMIT_UP_ROWS,
)
from src.data.pit import SHANGHAI, PITMode, PointInTimeView
from src.data.prelimit_capture import MARKET_CLOSURES, is_post_close_attempt
from src.mining.behavior_state import (
    BehaviorStateSnapshot,
    BehaviorStateSpec,
    StockBehaviorState,
    reduce_behavior_state,
)
from src.mining.experiments import (
    AttentionReaccelerationRule,
    DevelopmentEvidence,
    ExperimentSpec,
    FrozenPartition,
    FrozenPlayProjection,
    PlayCandidate,
    PlayGenome,
    canonical_mapping,
    play_execution_hash,
)
from src.mining.playbook import (
    PlayCard,
    PlayCardStorage,
    canonical_json_hash,
    freeze_forward_plan,
    load_pending_play_cards,
    load_play_cards_through,
)

THREE_TO_FOUR_PLAY_ID = "three_to_four_reseal"
THEME_NEW_ENTRANT_PLAY_ID = "theme_new_entrant_diffusion_v1"
ATTENTION_REACCELERATION_PLAY_ID = "attention_reacceleration_open_v1"
TERMINAL_CANDIDATE_STATUSES = frozenset(
    {"NOT_TRIGGERED", "UNFILLED", "INVALID", "COMPLETED"}
)
_PAPER_RECEIPT_FIELDS = (
    "execution_hash", "plan_hashes", "content_hash", "window_start",
    "window_end", "completed_signal_days",
)

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


@dataclass(frozen=True)
class OpenTriggerDecision:
    status: str
    reason: str
    entry_price: float | None = None
    gap_pct: float | None = None


def attention_reacceleration_candidate(
    partition: FrozenPartition,
    behavior_spec: BehaviorStateSpec | None = None,
    rule: AttentionReaccelerationRule | None = None,
) -> PlayCandidate:
    """Freeze the complete behavior-state play semantics before outcomes are read."""
    behavior_spec = behavior_spec or BehaviorStateSpec()
    rule = rule or AttentionReaccelerationRule(total_cost_bps=partition.total_cost_bps)
    behavior_spec.validate()
    rule.validate()
    if partition.play_id != ATTENTION_REACCELERATION_PLAY_ID:
        raise ValueError("partition belongs to another play")
    if rule.total_cost_bps != partition.total_cost_bps:
        raise ValueError("rule cost must match frozen partition")
    spec = rule.experiment_spec(behavior_spec.spec_hash)
    return _play_candidate(
        "注意力再加速开盘PAPER",
        spec,
        {
            "behavior_state": behavior_spec.to_payload(),
            "behavior_state_spec_hash": behavior_spec.spec_hash,
            "play_rule": rule.to_payload(),
            "play_rule_hash": rule.rule_hash,
        },
        rule.total_cost_bps,
        partition,
    )


def attention_genome_candidate(
    partition: FrozenPartition,
    genome: PlayGenome,
    parent_hashes: tuple[str, ...] = (),
) -> PlayCandidate:
    """Project one frozen genome into the shared executable attention adapter."""
    genome.validate()
    candidate = attention_reacceleration_candidate(
        partition, genome.behavior_spec, genome.rule
    )
    parameters = candidate.parameters
    parameters.update({
        "play_genome": genome.to_payload(),
        "genome_hash": genome.genome_hash,
        "execution_hash": genome.execution_hash,
    })
    return replace(
        candidate,
        candidate_name=f"attention:{genome.prediction_id}:{genome.genome_hash[:10]}",
        parameters_json=canonical_mapping(parameters, "parameters"),
        parent_hashes=parent_hashes,
    )


def _play_candidate(name, spec, parameters, cost_bps, partition):
    candidate = PlayCandidate(
        candidate_name=name,
        spec=spec,
        parameters_json=canonical_mapping(parameters, "parameters"),
        cost_model_json=canonical_mapping(
            {
                "commission_bps": None,
                "model_scope": "LUMP_SUM_PROXY_NOT_ITEMIZED",
                "round_trip_bps": cost_bps,
                "slippage_bps": None,
                "stamp_tax_bps": None,
            },
            "cost_model",
        ),
        partition=partition,
    )
    candidate.validate()
    return candidate


def prepare_attention_reacceleration_partition(
    storage: PlayCardStorage,
    dataset_snapshot_hash: str,
    total_cost_bps: float = 20,
) -> FrozenPartition:
    """Freeze metadata-only development/reserved dates before reading returns."""
    return FrozenPartition.chronological(
        ATTENTION_REACCELERATION_PLAY_ID,
        dataset_snapshot_hash,
        tuple(sorted(load_usable_audit_dates(storage))),
        _validate_total_cost_bps(total_cost_bps),
    )


def load_attention_paper_feedback(
    storage: PlayCardStorage, cutoff_trade_date: str, consumed=None
) -> dict[str, dict[str, Any]]:
    """Summarize only complete, validated FORWARD cards by exact execution identity."""
    grouped, receipt_sources, seen_plan_hashes = defaultdict(list), {}, set()
    consumed = consumed or {}
    cutoff_end = datetime.combine(date.fromisoformat(cutoff_trade_date), time.max,
                                  tzinfo=_SHANGHAI_TZ)
    cards = load_play_cards_through(storage, ATTENTION_REACCELERATION_PLAY_ID,
                                    cutoff_trade_date)
    for card in cards:
        plan = card.historical_evidence["forward_plan"]
        behavior = BehaviorStateSpec(**plan["behavior_state_spec"])
        rule = _attention_rule_from_payload(plan["rule"])
        if (plan["behavior_state_spec_hash"] != behavior.spec_hash or
                plan["rule_hash"] != rule.rule_hash or rule.total_cost_bps != 20):
            raise ValueError("FORWARD card behavior/rule/cost hash mismatch")
        plan_hash = card.historical_evidence["forward_plan_hash"]
        if plan_hash in seen_plan_hashes:
            raise ValueError("duplicate FORWARD plan hash")
        seen_plan_hashes.add(plan_hash)
        if (card.paper_status != "COMPLETED" or not card.candidates or any(
                item.get("paper_status") not in TERMINAL_CANDIDATE_STATUSES
                for item in card.candidates)):
            continue
        planned_dates = (card.signal_trade_date, plan["planned_entry_date"],
                         plan["planned_exit_date"])
        if any(date.fromisoformat(value) > cutoff_end.date() for value in planned_dates):
            raise ValueError("FORWARD result crosses the frozen feedback cutoff")
        codes = [str(item.get("stock_code") or "") for item in card.candidates]
        if not all(codes) or len(codes) != len(set(codes)):
            raise ValueError("FORWARD result stock codes must be non-empty and unique")
        results = []
        for item in card.candidates:
            status = str(item["paper_status"])
            result = {"stock_code": str(item["stock_code"]), "status": status}
            event_times = [_feedback_event_time(event["recorded_at"])
                           for event in item["lifecycle_events"]]
            if event_times != sorted(event_times) or any(t > cutoff_end for t in event_times):
                raise ValueError("FORWARD lifecycle time is non-monotonic or after cutoff")
            if status == "COMPLETED":
                value = item.get("net_return_pct")
                if not isinstance(value, (int, float)) or not math.isfinite(value):
                    raise ValueError("completed FORWARD result lacks finite net return")
                prices = (item.get("entry_price"), item.get("exit_price"))
                if (any(_positive_float(price) is None for price in prices)
                        or item.get("entry_trade_date") != plan["planned_entry_date"]
                        or item.get("exit_trade_date") != plan["planned_exit_date"]
                        or float(item.get("total_cost_bps", -1)) != 20):
                    raise ValueError("completed FORWARD result cost differs from frozen 20bp")
                expected = (float(prices[1]) / float(prices[0]) - 1) * 100 - 0.20
                if not math.isclose(float(value), expected, rel_tol=0, abs_tol=1e-9):
                    raise ValueError("completed FORWARD net return differs from frozen prices")
                result["net_return_pct"] = float(value)
            results.append(result)
        execution_hash = play_execution_hash(behavior.to_payload(), rule.to_payload())
        prior = consumed.get(execution_hash, {})
        receipt_sources[execution_hash] = tuple(prior.get("event_ids", ()))
        grouped.setdefault(execution_hash, [])
        if plan_hash in prior.get("plan_hashes", ()):
            continue
        grouped[execution_hash].append({"signal_trade_date": card.signal_trade_date,
            "plan_hash": plan_hash,
            "results": sorted(results, key=lambda value: value["stock_code"])})
    return {key: _paper_feedback(key, cutoff_trade_date, facts, receipt_sources[key])
            for key, facts in grouped.items()}


def empty_paper_feedback(execution_hash: str, cutoff_trade_date: str) -> dict[str, Any]:
    return _paper_feedback(execution_hash, cutoff_trade_date, [])


def _paper_feedback(execution_hash, cutoff_trade_date, facts, receipt_sources=()):
    facts = sorted(facts, key=lambda item: (item["signal_trade_date"], item["plan_hash"]))
    counts = {status: 0 for status in sorted(TERMINAL_CANDIDATE_STATUSES)}
    execution_days = counts.copy()
    daily_returns: list[float] = []
    for fact in facts:
        returns = []
        for result in fact["results"]:
            counts[result["status"]] += 1
            if result["status"] == "COMPLETED":
                returns.append(result["net_return_pct"])
        statuses = {result["status"] for result in fact["results"]}
        priority = "INVALID", "UNFILLED", "COMPLETED", "NOT_TRIGGERED"
        execution_days[next(status for status in priority if status in statuses)] += 1
        if returns:
            daily_returns.append(sum(returns) / len(returns))
    completed_days = len(daily_returns)
    mean = sum(daily_returns) / completed_days if completed_days else None
    cap = _paper_feedback_cap(completed_days)
    adjustment = max(-cap, min(cap, float(mean or 0) * 0.04))
    feedback = {
        "execution_hash": execution_hash, "cutoff_trade_date": cutoff_trade_date,
        "plan_hashes": sorted({fact["plan_hash"] for fact in facts}),
        "content_hash": canonical_json_hash({"cutoff": cutoff_trade_date, "facts": facts}),
        "status_counts": counts, "status_signal_days": execution_days,
        "completed_signal_days": completed_days, "mean_net_return_pct": mean,
        "max_drawdown_pct": _max_drawdown_pct(daily_returns) if daily_returns else None,
        "adjustment": adjustment,
        "status": "ADAPTIVE_DEVELOPMENT_FEEDBACK" if completed_days >= 5 else "INSUFFICIENT",
        "window_start": facts[0]["signal_trade_date"] if facts else None,
        "window_end": facts[-1]["signal_trade_date"] if facts else None,
        "receipt_sources": sorted(receipt_sources),
        "consumption_state": ("CONSUMED_ON_APPEND" if completed_days >= 5
                              else "PENDING_THRESHOLD_NOT_CONSUMED"),
    }
    feedback["consumption_receipt_hash"] = canonical_json_hash({
        key: feedback[key] for key in _PAPER_RECEIPT_FIELDS})
    _validate_paper_feedback(feedback)
    return feedback


def _validate_paper_feedback(feedback) -> None:
    hashes = [feedback[key] for key in ("execution_hash", "content_hash",
              "consumption_receipt_hash")] + feedback["plan_hashes"] + feedback["receipt_sources"]
    if (not all(isinstance(value, str) and len(value) == 64 and
                not set(value) - set("0123456789abcdef") for value in hashes) or
            feedback["plan_hashes"] != sorted(set(feedback["plan_hashes"])) or
            feedback["receipt_sources"] != sorted(set(feedback["receipt_sources"]))):
        raise ValueError("forward feedback hashes are invalid or duplicated")
    date.fromisoformat(feedback["cutoff_trade_date"])
    for counts in (feedback["status_counts"], feedback["status_signal_days"]):
        if set(counts) != TERMINAL_CANDIDATE_STATUSES or any(
                type(value) is not int or value < 0 for value in counts.values()):
            raise ValueError("forward feedback counts are invalid")
    days = feedback["completed_signal_days"]
    if type(days) is not int:
        raise ValueError("forward feedback completed-day count is invalid")
    mean, drawdown = feedback["mean_net_return_pct"], feedback["max_drawdown_pct"]
    if (not 0 <= days <= len(feedback["plan_hashes"])
            or (days == 0) != (mean is None and drawdown is None)
            or any(value is not None and not math.isfinite(value) for value in (mean, drawdown))
            or feedback["adjustment"] != max(-_paper_feedback_cap(days), min(
                _paper_feedback_cap(days), float(mean or 0) * 0.04))
            or sum(feedback["status_signal_days"].values()) != len(feedback["plan_hashes"])):
        raise ValueError("forward feedback metrics or day aggregation is invalid")
    expected = "INSUFFICIENT" if days < 5 else "ADAPTIVE_DEVELOPMENT_FEEDBACK"
    start, end = feedback["window_start"], feedback["window_end"]
    expected_receipt = canonical_json_hash({key: feedback[key]
                                            for key in _PAPER_RECEIPT_FIELDS})
    consumed = "PENDING_THRESHOLD_NOT_CONSUMED" if days < 5 else "CONSUMED_ON_APPEND"
    if (feedback["status"] != expected or feedback["consumption_state"] != consumed
            or (start is None) != (end is None)
            or start is not None and date.fromisoformat(start) > date.fromisoformat(end)
            or feedback["consumption_receipt_hash"] != expected_receipt):
        raise ValueError("forward feedback state, window, or receipt is invalid")
    canonical_json_hash(feedback)


def _paper_feedback_cap(days: int) -> float:
    return 0.0 if days < 5 else 0.05 if days < 20 else 0.1 if days < 40 else 0.2


def _attention_rule_from_payload(payload: dict[str, Any]) -> AttentionReaccelerationRule:
    values = dict(payload)
    values["allowed_state_domains"] = tuple(values["allowed_state_domains"])
    return AttentionReaccelerationRule(**values)


def _feedback_event_time(value: Any) -> datetime:
    if not isinstance(value, str):
        raise ValueError("FORWARD lifecycle recorded_at must be ISO-8601 text")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("FORWARD lifecycle recorded_at must include timezone")
    return parsed.astimezone(_SHANGHAI_TZ)


def select_attention_reacceleration_states(
    snapshot: BehaviorStateSnapshot,
    rule: AttentionReaccelerationRule,
) -> tuple[StockBehaviorState, ...]:
    """Single deterministic state-to-candidate rule used by both execution tracks."""
    rule.validate()
    eligible = []
    allowed = set(rule.allowed_state_domains)
    for state in snapshot.stocks:
        total_attention = state.own_attention + state.diffusion
        if (
            allowed.intersection(state.state_domains)
            and state.attention_slope is not None
            and state.attention_slope > rule.min_attention_slope
            and total_attention >= rule.min_total_attention
            and state.diffusion >= rule.min_diffusion_attention
            and state.crowding <= rule.max_crowding
            and (
                state.decay_age_trade_days is None
                or state.decay_age_trade_days <= rule.max_decay_age_trade_days
            )
        ):
            eligible.append(state)
    return tuple(
        sorted(
            eligible,
            key=lambda item: (
                -float(item.attention_slope or 0),
                -(item.own_attention + item.diffusion),
                item.stock_code,
            ),
        )[: rule.max_candidates]
    )


def evaluate_attention_open_trigger(
    signal_close: Any,
    observation: dict[str, Any] | None,
    rule: AttentionReaccelerationRule,
) -> OpenTriggerDecision:
    """Shared, side-effect-free executability decision for forward and retro paths."""
    rule.validate()
    close = _positive_float(signal_close)
    if close is None or observation is None:
        return OpenTriggerDecision("INVALID", "DATA_NOT_READY：缺D收盘或D+1开盘证据")
    price = _positive_float(observation.get("price", observation.get("open")))
    if price is None:
        return OpenTriggerDecision("INVALID", "DATA_NOT_READY：无有效D+1开盘成交价代理")
    gap = (price / close - 1) * 100
    volume = _positive_float(observation.get("volume"))
    amount = _positive_float(observation.get("amount"))
    if volume is None or ("amount" in observation and amount is None):
        return OpenTriggerDecision("UNFILLED", "D+1无有效成交量额代理", gap_pct=gap)
    low = _positive_float(observation.get("low"))
    high = _positive_float(observation.get("high"))
    if low is not None and high is not None and math.isclose(low, high, abs_tol=1e-9):
        return OpenTriggerDecision("UNFILLED", "D+1一字/单一价格代理不可成交", gap_pct=gap)
    if gap < rule.entry_gap_low_pct or gap > rule.entry_gap_high_pct:
        return OpenTriggerDecision(
            "UNFILLED",
            f"D+1开盘gap {gap:+.4f}%超出冻结区间",
            gap_pct=gap,
        )
    return OpenTriggerDecision("TRIGGERED", "满足冻结再加速开盘条件", price, gap)


def build_attention_reacceleration_card(
    storage: PlayCardStorage,
    signal_date: str | None = None,
    generated_at: str | datetime | None = None,
    behavior_spec: BehaviorStateSpec | None = None,
    rule: AttentionReaccelerationRule | None = None,
    frozen_projection: FrozenPlayProjection | None = None,
) -> PlayCard:
    """Freeze one real-time, post-close FORWARD PAPER plan without future reads."""
    signal_date = _resolve_signal_date(storage, signal_date)
    if signal_date not in load_usable_audit_dates(storage):
        raise ValueError(f"signal date {signal_date} has no usable post-close audit")
    generated_text = _resolve_generated_at(generated_at)
    if datetime.fromisoformat(generated_text.replace("Z", "+00:00")).tzinfo is None:
        raise ValueError("FORWARD PAPER decision_time must include a timezone")
    decision_at = _local_datetime(generated_text)
    if decision_at.date().isoformat() != signal_date:
        raise ValueError("FORWARD PAPER decision_time must be on the signal date")
    if decision_at.time() < _POST_CLOSE_AUDIT_TIME:
        raise ValueError("FORWARD PAPER plan requires a post-close decision_time")
    research_identity = None
    if frozen_projection is not None:
        frozen_projection.validate()
        genome = PlayGenome.from_payload(frozen_projection.genome)
        if behavior_spec is not None or rule is not None:
            raise ValueError("frozen projection is the sole PAPER rule identity")
        behavior_spec, rule = genome.behavior_spec, genome.rule
        research_identity = frozen_projection.to_payload()
    behavior_spec = behavior_spec or BehaviorStateSpec()
    rule = rule or AttentionReaccelerationRule()
    snapshot, closes = _behavior_inputs(storage, signal_date, decision_at, behavior_spec)
    selected = select_attention_reacceleration_states(snapshot, rule)
    entry_date, exit_date = _planned_market_dates(signal_date, 3)
    candidates: list[dict[str, Any]] = []
    for state in selected:
        row = closes.get(state.stock_code)
        signal_close = _positive_float(row.get("close")) if row else None
        candidate_status = "PLANNED" if signal_close is not None else "INVALID"
        event = {
            "status": candidate_status,
            "recorded_at": generated_text,
            "reason": (
                "候选与动作规则已在decision_time冻结"
                if candidate_status == "PLANNED"
                else "DATA_NOT_READY：D收盘价缺失"
            ),
        }
        candidates.append(
            {
                "stock_code": state.stock_code,
                "stock_name": state.name,
                "industry": state.industry,
                "state_domains": list(state.state_domains),
                "own_attention": state.own_attention,
                "diffusion_attention": state.diffusion,
                "attention_slope": state.attention_slope,
                "signal_close": signal_close,
                "planned_entry_date": entry_date,
                "planned_exit_date": exit_date,
                "paper_status": candidate_status,
                "selection_reason": (
                    f"{'+'.join(state.state_domains)}；attention="
                    f"{state.own_attention + state.diffusion:.4f}；"
                    f"slope={float(state.attention_slope or 0):+.4f}"
                ),
                "lifecycle_events": [event],
            }
        )
    data_ready = "NO_USABLE_POST_CLOSE_AUDITS" not in snapshot.limitations
    card_status = "DATA_NOT_READY" if not data_ready else "PLANNED"
    empty_reason = (
        "DATA_NOT_READY：行为状态缺少可用盘后审计"
        if not data_ready
        else "本日0只同时满足冻结attention与正向reacceleration条件"
    )
    envelope = {
        "play_id": ATTENTION_REACCELERATION_PLAY_ID,
        "play_name": "注意力再加速开盘",
        "behavior_logic": "近期涨停记忆或行业扩散注意力出现正向再加速时，冻结次日可成交开盘实验",
        "signal_trade_date": signal_date,
        "generated_at": generated_text,
        "trigger_rule": (
            f"{entry_date}须同时具备09:25与09:31快照；09:31相对D收盘gap位于"
            f"[{rule.entry_gap_low_pct:g}%, {rule.entry_gap_high_pct:g}%]且非一字、有有效量额"
        ),
        "abandon_rule": "缺任一快照、时钟异常、量额倒退记INVALID；gap越界、一字或无量记UNFILLED",
        "exit_rule": f"{exit_date}开盘PAPER模拟卖出；完整往返成本{rule.total_cost_bps:g}bp",
        "admission_status": "NOT_ADMITTED",
    }
    plan, plan_hash = freeze_forward_plan(
        envelope,
        candidates,
        ("stock_code", "state_domains", "signal_close", "selection_reason"),
        {
            "decision_time": generated_text,
            "behavior_state_spec": behavior_spec.to_payload(),
            "behavior_state_spec_hash": behavior_spec.spec_hash,
            "rule": rule.to_payload(),
            "rule_hash": rule.rule_hash,
            "planned_entry_date": entry_date,
            "planned_exit_date": exit_date,
            **({"research_identity": research_identity} if research_identity else {}),
        },
    )
    evidence = {
        "research_status": "DEVELOPMENT_CANDIDATE",
        "usage_status": "PAPER_ONLY",
        "forward_plan": plan,
        "forward_plan_hash": plan_hash,
        "decision_time": generated_text,
        "planned_entry_date": entry_date,
        "planned_exit_date": exit_date,
        "current_candidate_count": len(candidates),
        "total_cost_bps": rule.total_cost_bps,
        "state_limitations": list(snapshot.limitations),
        "historical_development": {
            "status": "PENDING_RECOMPUTE",
            "holdout_status": "HOLDOUT_NOT_OPENED",
        },
        "empty_reason": empty_reason if not candidates else "",
        "data_limitations": (
            "FORWARD PAPER只使用decision_time前证据；09:25/09:31缺段会保留DATA_NOT_READY/INVALID；"
            "PAPER未准入，实盘仓位0。"
        ),
    }
    card = PlayCard(
        **envelope,
        candidates=candidates,
        historical_evidence=evidence,
        paper_status=card_status,
    )
    card.validate()
    return card


def evaluate_attention_reacceleration_development(
    storage: PlayCardStorage,
    candidate: PlayCandidate,
) -> DevelopmentEvidence:
    """Walk forward only over the frozen development dates; reserved stays unread."""
    candidate.validate()
    if candidate.spec.adapter_id != ATTENTION_REACCELERATION_PLAY_ID:
        raise ValueError("unsupported attention adapter candidate")
    parameters = candidate.parameters
    behavior_spec = BehaviorStateSpec(**parameters["behavior_state"])
    rule = _attention_rule_from_payload(parameters["play_rule"])
    if parameters["behavior_state_spec_hash"] != behavior_spec.spec_hash:
        raise ValueError("behavior state spec hash mismatch")
    if parameters["play_rule_hash"] != rule.rule_hash:
        raise ValueError("attention play rule hash mismatch")
    if tuple(sorted(load_usable_audit_dates(storage))) != candidate.partition.audited_dates:
        raise ValueError("immutable snapshot audit dates differ from frozen partition")
    stats = _evaluate_attention_dates(
        storage,
        candidate.partition.development_dates,
        behavior_spec,
        rule,
        reserved_start=(
            candidate.partition.reserved_dates[0]
            if candidate.partition.reserved_dates
            else None
        ),
    )
    return DevelopmentEvidence.from_walk_forward(
        candidate.partition,
        stats,
        rule.total_cost_bps,
        ["DAILY_OPEN_PROXY", "RETRO_DEVELOPMENT_ONLY", "reserved holdout未打开且未读取"],
        "无已完成独立信号日；保留负证据且指标为缺失",
    )


def theme_new_entrant_candidate(
    partition: FrozenPartition,
    total_cost_bps: float = 20,
) -> PlayCandidate:
    """Return the fully frozen, development-only H1 experiment semantics."""
    cost = _validate_total_cost_bps(total_cost_bps)
    if cost != partition.total_cost_bps:
        raise ValueError("cost model must match the frozen statistical plan")
    spec = ExperimentSpec(
        play_id=THEME_NEW_ENTRANT_PLAY_ID,
        behavior_hypothesis="行业涨停宽度加速后，注意力扩散至新进入强势池的未涨停成员",
        universe_rule=(
            "D与精确前一交易日池按股票最新快照去重；D行业涨停宽度>=3且严格增加；"
            "候选为D强势非涨停、非前日强势、非ST普通股，每行业amount降序/code升序取1"
        ),
        decision_boundary="D日收盘后且最新采集审计成功；不得读取D+1数据选股",
        prediction="候选在D+1可成交开盘后至D+3开盘存在正的成本后收益",
        entry_rule="D+1开盘，开盘缺口位于[-2%,+5%]",
        exit_rule="D+3开盘，即入场后的第二个后续市场开盘",
        executability_rule="无报价、涨停/一字开盘或缺口越界记UNFILLED，不得静默删除",
        invalidations=(
            "缺少可信D或精确前一交易日收盘审计",
            "候选缺D收盘或D+1/D+3开盘报价",
            "D+1开盘缺口越界或一字不可成交",
        ),
        market_regime="通用行业注意力扩散，不硬编码行业",
        development_protocol=(
            "冻结可用信号日期后按时间80% development/20% reserved",
            "仅development逐信号日等权，reserved保持未打开",
            "收益扣完整往返成本且保留不可成交负证据",
        ),
        adapter_id=THEME_NEW_ENTRANT_PLAY_ID,
    )
    return _play_candidate(
        "热点扩散新强势成员H1",
        spec,
        {
            "breadth_min": 3,
            "entry_gap_pct": [-2.0, 5.0],
            "exit_market_days_after_signal": 3,
            "rank_per_industry": 1,
            "rank_rule": "amount_desc_code_asc",
            "require_new_strong_entrant": True,
        },
        cost,
        partition,
    )


def prepare_theme_new_entrant_partition(
    storage: PlayCardStorage,
    dataset_snapshot_hash: str,
    total_cost_bps: float = 20,
) -> FrozenPartition:
    """Freeze audit-date metadata without reading any price or return value."""
    return FrozenPartition.chronological(
        THEME_NEW_ENTRANT_PLAY_ID,
        dataset_snapshot_hash,
        tuple(sorted(load_usable_audit_dates(storage))),
        _validate_total_cost_bps(total_cost_bps),
    )


def select_theme_new_entrant_candidates(
    storage: PlayCardStorage,
    signal_date: str,
) -> tuple[list[dict[str, Any]], str, str]:
    """Apply the single H1 universe/rank rule used by PAPER and research."""
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
    previous_zt = _latest_pool_rows(storage, "zt_pool", previous_date) if previous_date else []
    previous_strong = (
        _latest_pool_rows(storage, "strong_pool", previous_date) if previous_date else []
    )
    if not empty_reason and not current_zt:
        empty_reason = "本日成功审计但涨停池为空，无法形成行业宽度信号"
    if not empty_reason and not current_strong:
        empty_reason = "本日强势池为空，本日0只符合条件的候选"

    candidates: list[dict[str, Any]] = []
    if not empty_reason:
        previous_breadth = _industry_breadth(previous_zt)
        current_breadth = _industry_breadth(current_zt)
        accelerated = {
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
                industry not in accelerated
                or code in current_zt_codes
                or code in previous_strong_codes
                or _is_st_stock(name)
                or not _is_ordinary_stock(code)
                or amount is None
            ):
                continue
            ranked_by_industry[industry].append(row)

        for industry in sorted(ranked_by_industry):
            top = sorted(
                ranked_by_industry[industry],
                key=lambda row: (-float(row["amount"]), str(row["stock_code"])),
            )[0]
            code = str(top["stock_code"])
            signal_close = _signal_close(storage, code, signal_date)
            if signal_close is None:  # Rank 2 is deliberately not backfilled.
                continue
            previous_count, current_count = accelerated[industry]
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
    return candidates, empty_reason, previous_day_audit_source


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

    candidates, empty_reason, previous_day_audit_source = (
        select_theme_new_entrant_candidates(storage, signal_date)
    )
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


def evaluate_theme_new_entrant_development(
    storage: PlayCardStorage,
    candidate: PlayCandidate,
) -> DevelopmentEvidence:
    """Recompute H1 on the development partition of an immutable snapshot."""
    candidate.validate()
    if candidate.spec.adapter_id != THEME_NEW_ENTRANT_PLAY_ID:
        raise ValueError("unsupported H1 adapter candidate")
    partition = candidate.partition
    parameters = candidate.parameters
    cost_bps = _validate_total_cost_bps(candidate.cost_model["round_trip_bps"])
    if parameters != theme_new_entrant_candidate(partition, cost_bps).parameters:
        raise ValueError("H1 parameters do not match the frozen adapter semantics")

    audited_dates = tuple(sorted(load_usable_audit_dates(storage)))
    if audited_dates != partition.audited_dates:
        raise ValueError("immutable snapshot audit dates differ from frozen partition")
    development_dates = partition.development_dates
    reserved_start = partition.reserved_dates[0] if partition.reserved_dates else None
    stats = _evaluate_theme_dates(
        storage, development_dates, cost_bps, reserved_start=reserved_start
    )
    return DevelopmentEvidence.from_walk_forward(
        partition,
        stats,
        cost_bps,
        ["仅为development证据；reserved holdout未打开，不能称为发现或准入"],
        "没有已完成的独立信号日收益，指标保持缺失而非伪造为零",
    )


def evaluate_theme_new_entrant_holdout(
    storage: PlayCardStorage,
    candidate: PlayCandidate,
) -> dict[str, Any]:
    """Read and summarize the already-opened reserved partition exactly once."""
    candidate.validate()
    partition = candidate.partition
    stats = _evaluate_theme_dates(
        storage,
        partition.reserved_dates,
        _validate_total_cost_bps(candidate.cost_model["round_trip_bps"]),
    )
    return summarize_theme_holdout_statistics(stats, partition)


def summarize_theme_holdout_statistics(
    stats: dict[str, Any], partition: FrozenPartition
) -> dict[str, Any]:
    """Apply the preregistered signal-day bootstrap and admission gates."""
    stats = dict(stats)
    daily_returns = stats.pop("daily_returns")
    ci_low, ci_high, p_value = _moving_block_bootstrap(daily_returns, partition)
    wins = [value for value in daily_returns if value > 0]
    losses = [value for value in daily_returns if value <= 0]
    fill_signal_days = int(stats.pop("fill_signal_days"))
    completed = len(daily_returns)
    mean = sum(daily_returns) / completed if completed else None
    enough = (
        completed >= partition.min_completed_signal_days
        and fill_signal_days >= partition.min_fill_signal_days
    )
    significant = p_value is not None and p_value <= 0.05
    approved = bool(
        enough
        and mean is not None
        and mean >= partition.min_mean_net_return_pct
        and ci_low is not None
        and ci_low > partition.min_ci_lower_pct
        and significant
    )
    decision = (
        "INSUFFICIENT_SAMPLE"
        if not enough
        else "ADMISSION_APPROVED_PENDING_PUBLICATION"
        if approved
        else "REJECTED"
    )
    return {
        **stats,
        "completed_signal_days": completed,
        "fill_signal_days": fill_signal_days,
        "wins": len(wins),
        "mean_net_return_pct": mean,
        "win_rate": len(wins) / completed if completed else None,
        "profit_loss_ratio": _profit_loss_ratio(wins, losses) if completed else None,
        "max_drawdown_pct": _max_drawdown_pct(daily_returns) if completed else None,
        "ci95_pct": [ci_low, ci_high],
        "bootstrap_p_one_sided": p_value,
        "holm_significant": significant,
        "multiplicity": {"family_size": 1, "rule": "HOLM_FAMILY_1"},
        "terminal_decision": decision,
    }


def _evaluate_theme_dates(
    storage: PlayCardStorage,
    signal_dates: tuple[str, ...],
    cost_bps: float,
    *,
    reserved_start: str | None = None,
) -> dict[str, Any]:
    market_dates = _market_dates(storage)
    indexes = {value: index for index, value in enumerate(market_dates)}
    invalidations: dict[str, int] = defaultdict(int)
    returns_by_signal: dict[str, list[float]] = defaultdict(list)
    candidate_count = filled_count = unfilled_count = signal_days = 0
    filled_dates: set[str] = set()
    for signal_date in signal_dates:
        index = indexes.get(signal_date)
        if index is None or index + 3 >= len(market_dates):
            invalidations["INCOMPLETE_EXIT_HORIZON"] += 1
            continue
        entry_date, exit_date = market_dates[index + 1], market_dates[index + 3]
        if reserved_start is not None and exit_date >= reserved_start:
            invalidations["OUTCOME_TOUCHES_RESERVED"] += 1
            continue
        selected, reason, _source = select_theme_new_entrant_candidates(storage, signal_date)
        signal_days += 1
        if not selected:
            invalidations["NO_ELIGIBLE_CANDIDATE"] += 1
            if reason:
                invalidations["SELECTION_EVIDENCE_INCOMPLETE"] += 1
            continue
        candidate_count += len(selected)
        for item in selected:
            code = str(item["stock_code"])
            entry = _latest_stock_date_row(
                storage,
                "daily_price",
                "stock_code, trade_date, open, high, low, snapshot_time",
                code,
                entry_date,
            )
            entry_price = _positive_float(entry.get("open")) if entry else None
            signal_close = _positive_float(item.get("signal_close"))
            gap = (entry_price / signal_close - 1) * 100 if entry_price and signal_close else None
            if gap is None:
                invalidations["MISSING_ENTRY_QUOTE"] += 1
            elif gap < -2 or gap > 5 or _is_one_price_row(entry):
                invalidations["ENTRY_NOT_EXECUTABLE"] += 1
            else:
                filled_count += 1
                filled_dates.add(signal_date)
                exit_row = _latest_stock_date_row(
                    storage,
                    "daily_price",
                    "stock_code, trade_date, open, snapshot_time",
                    code,
                    exit_date,
                )
                exit_price = _positive_float(exit_row.get("open")) if exit_row else None
                if exit_price is None:
                    invalidations["MISSING_EXIT_QUOTE"] += 1
                else:
                    returns_by_signal[signal_date].append(
                        (exit_price / entry_price - 1) * 100 - cost_bps / 100
                    )
                continue
            unfilled_count += 1
    return {
        "signal_days": signal_days,
        "candidate_count": candidate_count,
        "filled_count": filled_count,
        "unfilled_count": unfilled_count,
        "fill_signal_days": len(filled_dates),
        "invalidations": dict(sorted(invalidations.items())),
        "daily_returns": [
            sum(values) / len(values) for _, values in sorted(returns_by_signal.items())
        ],
    }


def _moving_block_bootstrap(
    values: list[float], partition: FrozenPartition
) -> tuple[float | None, float | None, float | None]:
    if not values:
        return None, None, None
    rng = random.Random(partition.bootstrap_seed)
    size, block = len(values), partition.moving_block_length
    means = []
    for _ in range(partition.bootstrap_iterations):
        sample: list[float] = []
        while len(sample) < size:
            start = rng.randrange(size)
            sample.extend(values[(start + offset) % size] for offset in range(block))
        means.append(sum(sample[:size]) / size)
    means.sort()
    low = means[int(0.025 * (len(means) - 1))]
    high = means[int(0.975 * (len(means) - 1))]
    p_value = (1 + sum(value <= 0 for value in means)) / (len(means) + 1)
    return low, high, p_value


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
    return _settle_daily_open_cards(
        storage, THREE_TO_FOUR_PLAY_ID, total_cost_bps, _settle_candidate
    )


def settle_theme_new_entrant_diffusion_cards(
    storage: PlayCardStorage,
    total_cost_bps: float = 20,
) -> list[PlayCard]:
    """Advance frozen H1 candidates without selecting from forward data."""
    return _settle_daily_open_cards(
        storage, THEME_NEW_ENTRANT_PLAY_ID, total_cost_bps, _settle_theme_candidate
    )


def _settle_daily_open_cards(storage, play_id, total_cost_bps, settle_candidate):
    fallback_cost = _validate_total_cost_bps(total_cost_bps)
    cards = load_pending_play_cards(storage, play_id)
    successful_dates = load_usable_audit_dates(storage)
    market_dates = _market_dates(storage)
    changed_cards: list[PlayCard] = []
    for card in cards:
        entry_date = _next_market_date(market_dates, card.signal_trade_date)
        cost_bps = _card_total_cost_bps(card, fallback_cost)
        updated_candidates = [
            settle_candidate(storage, item, entry_date, market_dates, successful_dates, cost_bps)
            for item in card.candidates
        ]
        paper_status = _card_paper_status(updated_candidates, entry_date, successful_dates)
        updated = _updated_card(card, updated_candidates, paper_status)
        if updated:
            changed_cards.append(updated)
    return changed_cards


def settle_attention_reacceleration_cards(
    storage: PlayCardStorage,
    rule: AttentionReaccelerationRule | None = None,
    recorded_at: str | datetime | None = None,
) -> list[PlayCard]:
    """Append forward evidence to frozen attention plans after audited days."""
    if rule is not None:
        rule.validate()
    recorded_text = _resolve_generated_at(recorded_at)
    cards = load_pending_play_cards(storage, ATTENTION_REACCELERATION_PLAY_ID)
    successful_dates = load_usable_audit_dates(storage)
    changed_cards = []
    for card in cards:
        plan = card.historical_evidence.get("forward_plan")
        if not isinstance(plan, dict):
            raise ValueError("stored attention plan is missing")
        frozen_rule = _attention_rule_from_payload(plan["rule"])
        if plan.get("rule_hash") != frozen_rule.rule_hash or (
            rule is not None and rule.rule_hash != frozen_rule.rule_hash
        ):
            raise ValueError("stored attention plan does not match settler rule")
        updated_candidates = [
            _settle_attention_candidate(
                storage, item, successful_dates, frozen_rule, recorded_text
            )
            for item in card.candidates
        ]
        paper_status = _card_paper_status(
            updated_candidates,
            str(plan.get("planned_entry_date") or ""),
            successful_dates,
            triggered_first=True,
        )
        updated = _updated_card(card, updated_candidates, paper_status)
        if updated:
            changed_cards.append(updated)
    return changed_cards


def _updated_card(card, candidates, paper_status):
    if candidates == card.candidates and paper_status == card.paper_status:
        return None
    updated = replace(card, candidates=candidates, paper_status=paper_status)
    updated.validate()
    return updated


def _settle_attention_candidate(
    storage: PlayCardStorage,
    candidate: dict[str, Any],
    successful_dates: set[str],
    rule: AttentionReaccelerationRule,
    recorded_at: str,
) -> dict[str, Any]:
    updated = dict(candidate)
    updated["lifecycle_events"] = list(candidate.get("lifecycle_events", []))
    status = str(updated.get("paper_status") or "PLANNED")
    if status in TERMINAL_CANDIDATE_STATUSES:
        return updated
    code = str(updated.get("stock_code") or "")
    entry_date = str(updated.get("planned_entry_date") or "")
    if status == "PLANNED":
        auction = _latest_prelimit_row(storage, code, entry_date, "AUCTION_0925")
        opening = _latest_prelimit_row(storage, code, entry_date, "OPEN_0931")
        if auction is None and opening is None:
            return updated
        if auction is None or opening is None:
            return _append_candidate_event(
                updated, "INVALID", recorded_at, "DATA_NOT_READY：09:25/09:31快照未配对"
            )
        clock_error = _prelimit_clock_error(auction, opening, entry_date)
        if clock_error:
            return _append_candidate_event(updated, "INVALID", recorded_at, clock_error)
        auction_volume = _finite_float(auction.get("volume"))
        opening_volume = _finite_float(opening.get("volume"))
        auction_amount = _finite_float(auction.get("amount"))
        opening_amount = _finite_float(opening.get("amount"))
        if (
            None in (auction_volume, opening_volume, auction_amount, opening_amount)
            or opening_volume < auction_volume
            or opening_amount < auction_amount
        ):
            return _append_candidate_event(
                updated, "INVALID", recorded_at, "DATA_NOT_READY：09:31累计量额小于09:25或字段缺失"
            )
        decision = evaluate_attention_open_trigger(updated.get("signal_close"), opening, rule)
        event_evidence = {
            "auction_observed_at": str(auction.get("observed_at")),
            "open_observed_at": str(opening.get("observed_at")),
        }
        updated = _append_candidate_event(
            updated, decision.status, recorded_at, decision.reason, event_evidence
        )
        if decision.status != "TRIGGERED":
            return updated
        updated.update(
            {
                "entry_trade_date": entry_date,
                "entry_price": decision.entry_price,
                "entry_gap_pct": decision.gap_pct,
                "entry_proxy": "OPEN_0931可成交快照代理",
                **event_evidence,
            }
        )
    if updated.get("paper_status") != "TRIGGERED":
        return updated
    exit_date = str(updated.get("planned_exit_date") or "")
    if exit_date not in successful_dates:
        audit = _latest_audit_row(storage, exit_date)
        if audit is not None:
            reason = (
                f"DATA_NOT_READY：固定退出日最新审计为"
                f"{audit.get('status') or 'UNKNOWN'}，退出保持待结算"
            )
            return _append_pending_event(updated, recorded_at, reason)
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
        return _append_candidate_event(
            updated, "INVALID", recorded_at, "DATA_NOT_READY：固定退出日开盘报价缺失"
        )
    entry_price = _positive_float(updated.get("entry_price"))
    if entry_price is None:
        raise ValueError("TRIGGERED attention candidate is missing its frozen entry")
    net = (exit_price / entry_price - 1) * 100 - rule.total_cost_bps / 100
    updated.update(
        {
            "exit_trade_date": exit_date,
            "exit_price": exit_price,
            "exit_proxy": "D+3日线开盘代理",
            "total_cost_bps": rule.total_cost_bps,
            "net_return_pct": net,
        }
    )
    return _append_candidate_event(updated, "COMPLETED", recorded_at, "固定D+3开盘已完成PAPER结算")


def _append_candidate_event(
    candidate: dict[str, Any],
    status: str,
    recorded_at: str,
    reason: str,
    evidence: dict[str, Any] | None = None,
) -> dict[str, Any]:
    current = str(candidate.get("paper_status") or "PLANNED")
    if current == status:
        return candidate
    candidate["paper_status"] = status
    candidate.setdefault("result_reason", reason)
    event = {"status": status, "recorded_at": recorded_at, "reason": reason}
    if evidence:
        event.update(evidence)
    candidate.setdefault("lifecycle_events", []).append(event)
    return candidate


def _append_pending_event(
    candidate: dict[str, Any], recorded_at: str, reason: str
) -> dict[str, Any]:
    events = candidate.setdefault("lifecycle_events", [])
    if events and events[-1].get("status") == "TRIGGERED" and events[-1].get("reason") == reason:
        return candidate
    candidate.setdefault("pending_reason", reason)
    events.append({"status": "TRIGGERED", "recorded_at": recorded_at, "reason": reason})
    return candidate


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
        if row.get("status") != "ok" or not is_post_close_attempt(
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


def _planned_market_dates(signal_date: str, exit_horizon: int) -> tuple[str, str]:
    current = date.fromisoformat(signal_date)
    closures = MARKET_CLOSURES.get(current.year)
    if closures is None:
        raise ValueError(f"exchange calendar for {current.year} is not audited")
    dates = []
    while len(dates) < exit_horizon:
        current += timedelta(days=1)
        year_closures = MARKET_CLOSURES.get(current.year)
        if year_closures is None:
            raise ValueError(f"exchange calendar for {current.year} is not audited")
        if current.weekday() < 5 and current.isoformat() not in year_closures:
            dates.append(current.isoformat())
    return dates[0], dates[-1]


def _local_datetime(value: str | datetime) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00")) if isinstance(value, str) else value
    return parsed.replace(tzinfo=SHANGHAI) if parsed.tzinfo is None else parsed.astimezone(SHANGHAI)


def _behavior_inputs(
    storage: PlayCardStorage,
    signal_date: str,
    decision_at: datetime,
    spec: BehaviorStateSpec,
) -> tuple[BehaviorStateSnapshot, dict[str, dict[str, Any]]]:
    calendar = [
        str(row["trade_date"])
        for row in storage.execute(
            "SELECT DISTINCT trade_date FROM daily_price WHERE trade_date <= ? ORDER BY trade_date",
            (signal_date,),
        )
    ]
    if not calendar or calendar[-1] != signal_date:
        raise ValueError("DATA_NOT_READY: signal date is absent from the market calendar")
    view = PointInTimeView(storage, decision_at, PITMode.FORWARD)
    frame = view.query("daily_price", decision_at, where="trade_date = ?", params=(signal_date,))
    closes = {
        str(key[0]): row
        for key, row in _dedupe_latest(frame.to_dict("records"), ("stock_code",)).items()
    }
    return reduce_behavior_state(view, decision_at, calendar, spec), closes


def _evaluate_attention_dates(
    storage: PlayCardStorage,
    signal_dates: tuple[str, ...],
    behavior_spec: BehaviorStateSpec,
    rule: AttentionReaccelerationRule,
    *,
    reserved_start: str | None,
) -> dict[str, Any]:
    market_dates = _market_dates(storage)
    indexes = {value: index for index, value in enumerate(market_dates)}
    returns_by_signal: dict[str, list[float]] = defaultdict(list)
    invalidations: dict[str, int] = defaultdict(int)
    candidate_count = filled_count = unfilled_count = signal_days = 0
    for signal_date in signal_dates:
        index = indexes.get(signal_date)
        if index is None or index + rule.exit_market_days_after_signal >= len(market_dates):
            invalidations["INCOMPLETE_EXIT_HORIZON"] += 1
            continue
        entry_date = market_dates[index + 1]
        exit_date = market_dates[index + rule.exit_market_days_after_signal]
        if reserved_start is not None and exit_date >= reserved_start:
            invalidations["OUTCOME_TOUCHES_RESERVED"] += 1
            continue
        decision_at = datetime.combine(
            date.fromisoformat(signal_date), time(23, 59), tzinfo=SHANGHAI
        )
        try:
            snapshot, closes = _behavior_inputs(
                storage, signal_date, decision_at, behavior_spec
            )
            selected = select_attention_reacceleration_states(snapshot, rule)
        except (ValueError, RuntimeError):
            invalidations["STATE_DATA_NOT_READY"] += 1
            signal_days += 1
            continue
        # Candidate identity is fully frozen here, before either future date is queried.
        frozen = tuple(
            (state.stock_code, _positive_float(closes.get(state.stock_code, {}).get("close")))
            for state in selected
        )
        signal_days += 1
        candidate_count += len(frozen)
        if not frozen:
            invalidations["NO_REACCELERATION_CANDIDATE"] += 1
        for code, signal_close in frozen:
            entry_row = _latest_stock_date_row(
                storage,
                "daily_price",
                "stock_code, trade_date, open, high, low, volume, snapshot_time",
                code,
                entry_date,
            )
            decision = evaluate_attention_open_trigger(signal_close, entry_row, rule)
            if decision.status != "TRIGGERED":
                unfilled_count += 1
                invalidations[decision.status] += 1
                continue
            filled_count += 1
            exit_row = _latest_stock_date_row(
                storage,
                "daily_price",
                "stock_code, trade_date, open, snapshot_time",
                code,
                exit_date,
            )
            exit_price = _positive_float(exit_row.get("open")) if exit_row else None
            if exit_price is None or decision.entry_price is None:
                invalidations["INVALID_MISSING_EXIT"] += 1
                continue
            returns_by_signal[signal_date].append(
                (exit_price / decision.entry_price - 1) * 100 - rule.total_cost_bps / 100
            )
    return {
        "signal_days": signal_days,
        "candidate_count": candidate_count,
        "filled_count": filled_count,
        "unfilled_count": unfilled_count,
        "invalidations": dict(sorted(invalidations.items())),
        "daily_returns": [
            sum(values) / len(values) for _, values in sorted(returns_by_signal.items())
        ],
    }


def _latest_prelimit_row(
    storage: PlayCardStorage, code: str, trade_date: str, phase: str
) -> dict[str, Any] | None:
    rows = storage.execute(
        """
        SELECT * FROM prelimit_snapshots
        WHERE stock_code = ? AND trade_date = ? AND phase = ?
        ORDER BY snapshot_time DESC LIMIT 1
        """,
        (code, trade_date, phase),
    )
    return rows[0] if rows else None


def _latest_audit_row(storage: PlayCardStorage, trade_date: str) -> dict[str, Any] | None:
    rows = storage.execute(
        """
        SELECT status, attempted_at FROM limit_up_collection_runs
        WHERE trade_date = ? ORDER BY attempted_at DESC, id DESC LIMIT 1
        """,
        (trade_date,),
    )
    return rows[0] if rows else None


def _prelimit_clock_error(
    auction: dict[str, Any], opening: dict[str, Any], trade_date: str
) -> str | None:
    try:
        auction_time = _local_datetime(str(auction["observed_at"]))
        opening_time = _local_datetime(str(opening["observed_at"]))
        auction_snapshot = _local_datetime(str(auction["snapshot_time"]))
        opening_snapshot = _local_datetime(str(opening["snapshot_time"]))
    except (KeyError, TypeError, ValueError):
        return "DATA_NOT_READY：快照时间戳无效"
    expected = date.fromisoformat(trade_date)
    if any(value.date() != expected for value in (auction_time, opening_time)):
        return "DATA_NOT_READY：快照不属于计划交易日"
    if not time(9, 20) <= auction_time.time() <= time(9, 29, 59):
        return "DATA_NOT_READY：09:25快照时钟越界"
    if not time(9, 30) <= opening_time.time() <= time(9, 35, 59):
        return "DATA_NOT_READY：09:31快照时钟越界"
    if auction_snapshot < auction_time or opening_snapshot < opening_time:
        return "DATA_NOT_READY：快照可用时间早于观测时间"
    return None


def _card_paper_status(
    candidates: list[dict[str, Any]],
    entry_date: str | None,
    successful_dates: set[str],
    *,
    triggered_first: bool = False,
) -> str:
    statuses = {str(candidate.get("paper_status") or "PLANNED") for candidate in candidates}
    if triggered_first and "TRIGGERED" in statuses:
        return "TRIGGERED"
    if "PLANNED" in statuses:
        return "PLANNED"
    if "TRIGGERED" in statuses:
        return "TRIGGERED"
    if not candidates and (entry_date is None or entry_date not in successful_dates):
        return "PLANNED"
    return "COMPLETED"


def _market_dates(storage: PlayCardStorage) -> list[str]:
    return [
        str(row["trade_date"])
        for row in storage.execute("SELECT DISTINCT trade_date FROM daily_price ORDER BY trade_date")
    ]


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
