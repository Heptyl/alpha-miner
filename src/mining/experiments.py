"""Immutable contracts for executable development-only play experiments."""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import asdict, dataclass, replace
from datetime import date
from typing import Any, Mapping

from src.mining.behavior_state import BehaviorStateSpec

DEVELOPMENT_ONLY = "DEVELOPMENT_ONLY"
HOLDOUT_NOT_OPENED = "HOLDOUT_NOT_OPENED"
HOLDOUT_TERMINAL = frozenset(
    {
        "ADMISSION_APPROVED_PENDING_PUBLICATION",
        "REJECTED",
        "INSUFFICIENT_SAMPLE",
        "EVALUATION_ERROR",
    }
)


def canonical_mapping(value: Mapping[str, Any], field: str) -> str:
    if not isinstance(value, Mapping) or not value:
        raise ValueError(f"{field} must be a non-empty mapping")
    try:
        return json.dumps(
            dict(value), ensure_ascii=False, sort_keys=True, separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must contain canonical JSON values") from exc


def _text(value: str, field: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be non-empty")


@dataclass(frozen=True)
class FrozenPartition:
    play_id: str
    dataset_snapshot_hash: str
    audited_dates: tuple[str, ...]
    development_dates: tuple[str, ...]
    embargo_dates: tuple[str, ...]
    reserved_dates: tuple[str, ...]
    max_forward_horizon: int = 3
    primary_metric: str = "mean_net_return_pct"
    total_cost_bps: float = 20.0
    bootstrap_seed: int = 20260818
    bootstrap_iterations: int = 10_000
    moving_block_length: int = 5
    family_size: int = 1
    multiplicity_rule: str = "HOLM_FAMILY_1"
    min_completed_signal_days: int = 40
    min_fill_signal_days: int = 40
    min_mean_net_return_pct: float = 0.20
    min_ci_lower_pct: float = 0.0

    @classmethod
    def chronological(
        cls, play_id: str, dataset_snapshot_hash: str,
        audited_dates: tuple[str, ...], total_cost_bps: float,
    ) -> FrozenPartition:
        split = int(len(audited_dates) * 0.8)
        embargo_start = max(0, split - 3)
        partition = cls(
            play_id=play_id,
            dataset_snapshot_hash=dataset_snapshot_hash,
            audited_dates=audited_dates,
            development_dates=audited_dates[:embargo_start],
            embargo_dates=audited_dates[embargo_start:split],
            reserved_dates=audited_dates[split:],
            total_cost_bps=total_cost_bps,
        )
        partition.validate()
        return partition

    def validate(self) -> None:
        _text(self.play_id, "play_id")
        if len(self.dataset_snapshot_hash) != 64 or any(
            char not in "0123456789abcdef" for char in self.dataset_snapshot_hash
        ):
            raise ValueError("dataset_snapshot_hash must be lowercase SHA256")
        groups = (self.development_dates, self.embargo_dates, self.reserved_dates)
        for values in (self.audited_dates, *groups):
            if tuple(sorted(set(values))) != values:
                raise ValueError("partition dates must be unique and sorted")
            for value in values:
                date.fromisoformat(value)
        if tuple(value for group in groups for value in group) != self.audited_dates:
            raise ValueError("partition groups must exactly cover audited_dates")
        if not math.isfinite(self.total_cost_bps) or self.total_cost_bps < 0:
            raise ValueError("total_cost_bps must be a finite non-negative value")
        expected = (3, "mean_net_return_pct", 20260818, 10_000, 5, 1)
        actual = (
            self.max_forward_horizon,
            self.primary_metric,
            self.bootstrap_seed,
            self.bootstrap_iterations,
            self.moving_block_length,
            self.family_size,
        )
        if actual != expected or self.multiplicity_rule != "HOLM_FAMILY_1":
            raise ValueError("holdout statistical plan is fixed")
        if (self.min_completed_signal_days, self.min_fill_signal_days) != (40, 40):
            raise ValueError("holdout minimum signal-day counts are fixed at 40")
        if (self.min_mean_net_return_pct, self.min_ci_lower_pct) != (0.20, 0.0):
            raise ValueError("holdout effect gates are fixed")

    def to_dict(self) -> dict[str, Any]:
        self.validate()
        payload = asdict(self)
        for field in ("audited_dates", "development_dates", "embargo_dates", "reserved_dates"):
            payload[field] = list(payload[field])
        payload["partition_hash"] = _hash(payload)
        payload["holdout_scope_hash"] = _hash(
            {
                "play_id": self.play_id,
                "dataset_snapshot_hash": self.dataset_snapshot_hash,
                "reserved_dates": list(self.reserved_dates),
                "entry_horizon": 1,
                "exit_horizon": self.max_forward_horizon,
            }
        )
        return payload

    @property
    def partition_hash(self) -> str:
        return str(self.to_dict()["partition_hash"])

    @property
    def holdout_scope_hash(self) -> str:
        return str(self.to_dict()["holdout_scope_hash"])


def _hash(value: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        dict(value), ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
    )
    return hashlib.sha256(encoded.encode()).hexdigest()


def play_execution_hash(
    behavior_state: Mapping[str, Any], play_rule: Mapping[str, Any]
) -> str:
    """Hash only executable state/rule semantics, excluding narrative provenance."""
    return _hash({"behavior_state": dict(behavior_state), "play_rule": dict(play_rule)})


@dataclass(frozen=True)
class ExperimentSpec:
    play_id: str
    behavior_hypothesis: str
    universe_rule: str
    decision_boundary: str
    prediction: str
    entry_rule: str
    exit_rule: str
    executability_rule: str
    invalidations: tuple[str, ...]
    market_regime: str
    development_protocol: tuple[str, ...]
    adapter_id: str
    behavior_state_spec_hash: str | None = None

    def validate(self) -> None:
        for field in (
            "play_id", "behavior_hypothesis", "universe_rule", "decision_boundary",
            "prediction", "entry_rule", "exit_rule", "executability_rule",
            "market_regime", "adapter_id",
        ):
            _text(getattr(self, field), field)
        for field in ("invalidations", "development_protocol"):
            values = getattr(self, field)
            if not isinstance(values, tuple) or not values:
                raise ValueError(f"{field} must be a non-empty tuple")
            for value in values:
                _text(value, field)
        if self.behavior_state_spec_hash is not None and (
            len(self.behavior_state_spec_hash) != 64
            or any(char not in "0123456789abcdef" for char in self.behavior_state_spec_hash)
        ):
            raise ValueError("behavior_state_spec_hash must be lowercase SHA256")

    def protocol(self) -> dict[str, Any]:
        self.validate()
        payload = {
            "play_id": self.play_id,
            "behavior_hypothesis": self.behavior_hypothesis,
            "universe_rule": self.universe_rule,
            "decision_boundary": self.decision_boundary,
            "prediction": self.prediction,
            "entry_rule": self.entry_rule,
            "exit_rule": self.exit_rule,
            "executability_rule": self.executability_rule,
            "invalidations": list(self.invalidations),
            "market_regime": self.market_regime,
            "development_protocol": list(self.development_protocol),
            "adapter_id": self.adapter_id,
        }
        if self.behavior_state_spec_hash is not None:
            payload["behavior_state_spec_hash"] = self.behavior_state_spec_hash
        return payload


@dataclass(frozen=True)
class AttentionReaccelerationRule:
    """Frozen play thresholds shared by retro and forward execution."""

    min_total_attention: float = 0.5
    min_attention_slope: float = 0.0
    min_diffusion_attention: float = 0.0
    max_crowding: float = 1.0
    max_decay_age_trade_days: int = 20
    entry_gap_low_pct: float = -2.0
    entry_gap_high_pct: float = 5.0
    exit_market_days_after_signal: int = 3
    total_cost_bps: float = 20.0
    max_candidates: int = 5
    allowed_state_domains: tuple[str, ...] = (
        "recent_limit_memory",
        "post_limit_non_limit",
        "industry_diffusion_non_limit",
    )

    def validate(self) -> None:
        numeric = (
            self.min_total_attention, self.min_attention_slope,
            self.min_diffusion_attention, self.max_crowding,
            self.entry_gap_low_pct, self.entry_gap_high_pct, self.total_cost_bps,
        )
        if any(isinstance(value, bool) or not math.isfinite(value) for value in numeric):
            raise ValueError("attention play thresholds must be finite numbers")
        if min(self.min_total_attention, self.min_attention_slope, self.min_diffusion_attention) < 0:
            raise ValueError("attention thresholds must be non-negative")
        if not 0 <= self.max_crowding <= 1:
            raise ValueError("max_crowding must be in [0, 1]")
        if self.max_decay_age_trade_days not in {3, 5, 10, 20}:
            raise ValueError("max decay age is outside the preregistered set")
        if self.entry_gap_low_pct >= self.entry_gap_high_pct:
            raise ValueError("entry gap bounds must be increasing")
        if self.exit_market_days_after_signal != 3 or self.total_cost_bps < 0:
            raise ValueError("exit horizon is D+3 and total cost must be non-negative")
        if isinstance(self.max_candidates, bool) or not 1 <= self.max_candidates <= 20:
            raise ValueError("max_candidates must be an integer in [1, 20]")
        if not self.allowed_state_domains or not set(self.allowed_state_domains) <= _STATE_DOMAINS:
            raise ValueError("state domains must be a non-empty preregistered subset")
        if len(set(self.allowed_state_domains)) != len(self.allowed_state_domains):
            raise ValueError("state domains must be unique")

    def to_payload(self) -> dict[str, Any]:
        self.validate()
        payload = asdict(self)
        payload["allowed_state_domains"] = list(self.allowed_state_domains)
        return payload

    @property
    def rule_hash(self) -> str:
        return _hash(self.to_payload())

    def experiment_spec(self, behavior_state_spec_hash: str) -> ExperimentSpec:
        """Build the one frozen executable-play protocol for this rule."""
        return ExperimentSpec(
            play_id="attention_reacceleration_open_v1",
            behavior_hypothesis=(
                "涨停显著性形成的近期注意力记忆，在自身或行业扩散注意力重新加速时，"
                "可能延续至下一交易日可成交开盘"
            ),
            universe_rule=(
                "recent_limit_memory、post_limit_non_limit与industry_diffusion_non_limit三域；"
                "按attention slope、总attention、代码排序冻结，不要求D日涨停"
            ),
            decision_boundary="D日15:40后成功审计；候选冻结后才允许读取D+1及D+3结果",
            prediction="D+1开盘可成交后至D+3开盘存在正的20bp成本后收益",
            entry_rule=(
                f"D+1开盘相对D收盘gap位于[{self.entry_gap_low_pct:g}%,"
                f"{self.entry_gap_high_pct:g}%]且有成交代理"
            ),
            exit_rule="固定D+3开盘，即入场后的第二个后续交易日开盘",
            executability_rule="缺快照/报价为INVALID；越界、一字或无量为UNFILLED",
            invalidations=("行为状态或D收盘证据不完整", "D+1开盘快照缺失、陈旧或累计量额倒退", "D+3退出报价缺失"),
            market_regime="涨停生态注意力形成、强化、扩散与衰减",
            development_protocol=(
                "逐信号日重建PIT状态并先冻结候选",
                "daily open仅为DAILY_OPEN_PROXY/RETRO_DEVELOPMENT_ONLY",
                "按信号日先等权；reserved保持未读取",
            ),
            adapter_id="attention_reacceleration_open_v1",
            behavior_state_spec_hash=behavior_state_spec_hash,
        )


_GENOME_BOUNDS = {
    "lookback_trade_days": (10, 20, 30),
    "half_life_trade_days": (3.0, 5.0, 8.0),
    "limit_up_weight": (0.75, 1.0, 1.25),
    "repeat_limit_weight": (0.25, 0.5, 0.75),
    "consecutive_board_weight": (0.1, 0.25, 0.5),
    "seal_quality_weight": (0.1, 0.25, 0.5),
    "industry_diffusion_weight": (0.25, 0.5, 0.75),
    "failed_board_decay_weight": (0.5, 0.75, 1.0),
    "diffusion_stop_decay_weight": (0.1, 0.25, 0.5),
    "breakdown_decay_weight": (0.25, 0.5, 0.75),
    "min_total_attention": (0.25, 0.5, 1.0),
    "min_attention_slope": (0.0, 0.05, 0.1),
    "min_diffusion_attention": (0.0, 0.25, 0.5),
    "max_crowding": (0.5, 0.75, 1.0),
    "max_decay_age_trade_days": (3, 5, 10, 20),
    "entry_gap_low_pct": (-3.0, -2.0, -1.0),
    "entry_gap_high_pct": (3.0, 5.0, 7.0),
    "max_candidates": (3, 5, 10),
}
_STATE_DOMAINS = frozenset(("recent_limit_memory", "post_limit_non_limit", "industry_diffusion_non_limit"))
_DOMAIN_OPTIONS = (
    ("recent_limit_memory",), ("post_limit_non_limit",),
    ("industry_diffusion_non_limit",), ("post_limit_non_limit", "recent_limit_memory"),
    ("industry_diffusion_non_limit", "recent_limit_memory"),
    ("industry_diffusion_non_limit", "post_limit_non_limit"), tuple(sorted(_STATE_DOMAINS)),
)


def play_genome_search_space() -> dict[str, list[Any]]:
    """Canonical preregistered axes included in the search-family identity."""
    axes = {field: list(values) for field, values in _GENOME_BOUNDS.items()}
    return {**axes, "allowed_state_domains": [list(values) for values in _DOMAIN_OPTIONS]}


@dataclass(frozen=True)
class PlayGenome:
    """One bounded, executable attention-play identity."""

    theory_id: str
    prediction_id: str
    evidence_grade: str
    behavior_spec: BehaviorStateSpec
    rule: AttentionReaccelerationRule
    theory_provenance: tuple[tuple[str, str, str], ...] = ()

    def validate(self) -> None:
        for field in ("theory_id", "prediction_id", "evidence_grade"):
            _text(getattr(self, field), field)
        if self.evidence_grade not in {"ACADEMIC_FOUNDATION", "THEORY_DERIVED", "HEURISTIC"}:
            raise ValueError("evidence grade is not recognized")
        self.behavior_spec.validate()
        self.rule.validate()
        provenance = self.theory_provenance or (
            (self.theory_id, self.prediction_id, self.evidence_grade),
        )
        if provenance != tuple(sorted(set(provenance))):
            raise ValueError("theory provenance must be sorted and unique")
        if (self.theory_id, self.prediction_id, self.evidence_grade) not in provenance:
            raise ValueError("primary theory must be present in provenance")
        if any(len(item) != 3 or any(not value for value in item) for item in provenance):
            raise ValueError("theory provenance is incomplete")
        values = {**self.behavior_spec.to_payload(), **self.rule.to_payload()}
        for field, allowed in _GENOME_BOUNDS.items():
            if values[field] not in allowed:
                raise ValueError(f"{field} is outside the preregistered set")
        domains = self.rule.allowed_state_domains
        if domains != tuple(sorted(domains)):
            raise ValueError("state domains must be a sorted preregistered subset")
        if self.rule.exit_market_days_after_signal != 3 or self.rule.total_cost_bps != 20:
            raise ValueError("genome fixes D+3 exit and 20bp total cost")

    def to_payload(self) -> dict[str, Any]:
        self.validate()
        provenance = self.theory_provenance or ((self.theory_id, self.prediction_id, self.evidence_grade),)
        return {
            "theory_id": self.theory_id,
            "prediction_id": self.prediction_id,
            "evidence_grade": self.evidence_grade,
            "behavior_state": self.behavior_spec.to_payload(),
            "play_rule": self.rule.to_payload(),
            "theory_provenance": [list(item) for item in provenance],
        }

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any]) -> PlayGenome:
        rule = dict(payload["play_rule"])
        rule["allowed_state_domains"] = tuple(rule["allowed_state_domains"])
        genome = cls(
            str(payload["theory_id"]),
            str(payload["prediction_id"]),
            str(payload["evidence_grade"]),
            BehaviorStateSpec(**payload["behavior_state"]),
            AttentionReaccelerationRule(**rule),
            tuple(tuple(item) for item in payload.get("theory_provenance", ())),
        )
        genome.validate()
        return genome

    @property
    def genome_hash(self) -> str:
        return _hash(self.to_payload())

    @property
    def execution_hash(self) -> str:
        return play_execution_hash(
            self.execution_payload["behavior_state"], self.execution_payload["play_rule"]
        )

    @property
    def execution_payload(self) -> dict[str, Any]:
        return {"behavior_state": self.behavior_spec.to_payload(), "play_rule": self.rule.to_payload()}

    @property
    def mutable_fields(self) -> tuple[str, ...]:
        return tuple(_GENOME_BOUNDS) + ("allowed_state_domains",)

    def mutate(self, field: str, direction: int = 1) -> PlayGenome:
        """Move exactly one axis within its preregistered finite set."""
        if direction not in {-1, 1}:
            raise ValueError("mutation direction must be -1 or 1")
        if field == "allowed_state_domains":
            values = _DOMAIN_OPTIONS
            current = tuple(self.rule.allowed_state_domains)
            rule = replace(self.rule, allowed_state_domains=values[(values.index(current) + direction) % len(values)])
            return replace(self, rule=rule)
        if field not in _GENOME_BOUNDS:
            raise ValueError("mutation field is not preregistered")
        target = self.behavior_spec if hasattr(self.behavior_spec, field) else self.rule
        values = _GENOME_BOUNDS[field]
        value = values[(values.index(getattr(target, field)) + direction) % len(values)]
        changed = replace(target, **{field: value})
        updates = {"behavior_spec": changed} if target is self.behavior_spec else {"rule": changed}
        return replace(self, **updates)


@dataclass(frozen=True)
class FrozenPlayProjection:
    """Ranked immutable ledger identity safe to project into a PAPER plan."""

    rank: int
    fitness: float
    candidate_hash: str
    lineage_hash: str
    dataset_snapshot_hash: str
    search_family_hash: str
    genome_hash: str
    execution_hash: str
    genome: Mapping[str, Any]
    research_status: str
    usage_status: str
    holdout_status: str
    admission_status: str

    def validate(self) -> None:
        if isinstance(self.rank, bool) or self.rank < 1 or not math.isfinite(self.fitness):
            raise ValueError("projection rank/fitness is invalid")
        hashes = (self.candidate_hash, self.lineage_hash, self.dataset_snapshot_hash,
                  self.search_family_hash, self.genome_hash, self.execution_hash)
        if any(len(value) != 64 or set(value) - set("0123456789abcdef") for value in hashes):
            raise ValueError("projection identity requires six lowercase SHA256 hashes")
        genome = PlayGenome.from_payload(self.genome)
        if genome.genome_hash != self.genome_hash or genome.execution_hash != self.execution_hash:
            raise ValueError("projection genome identity mismatch")
        if (
            self.research_status, self.usage_status,
            self.holdout_status, self.admission_status,
        ) != ("DEVELOPMENT_CANDIDATE", "PAPER_ONLY", "HOLDOUT_NOT_OPENED", "NOT_ADMITTED"):
            raise ValueError("projection cannot elevate development-only status")

    def to_payload(self) -> dict[str, Any]:
        self.validate()
        payload = asdict(self)
        payload["genome"] = PlayGenome.from_payload(self.genome).to_payload()
        return payload


@dataclass(frozen=True)
class PlayCandidate:
    candidate_name: str
    spec: ExperimentSpec
    parameters_json: str
    cost_model_json: str
    partition: FrozenPartition
    parent_hashes: tuple[str, ...] = ()

    def validate(self) -> None:
        _text(self.candidate_name, "candidate_name")
        self.spec.validate()
        self.partition.validate()
        for field in ("parameters_json", "cost_model_json"):
            value = getattr(self, field)
            try:
                decoded = json.loads(value)
            except (TypeError, json.JSONDecodeError) as exc:
                raise ValueError(f"{field} must be canonical JSON") from exc
            if canonical_mapping(decoded, field) != value:
                raise ValueError(f"{field} must be canonical JSON")
        if not isinstance(self.parent_hashes, tuple):
            raise ValueError("parent_hashes must be a tuple")

    @property
    def parameters(self) -> dict[str, Any]:
        self.validate()
        return json.loads(self.parameters_json)

    @property
    def cost_model(self) -> dict[str, Any]:
        self.validate()
        return json.loads(self.cost_model_json)


@dataclass(frozen=True)
class DevelopmentEvidence:
    coverage_start: str | None
    coverage_end: str | None
    signal_days: int
    candidate_count: int
    filled_count: int
    unfilled_count: int
    completed_signal_days: int
    wins: int
    mean_net_return_pct: float | None
    win_rate: float | None
    profit_loss_ratio: float | None
    max_drawdown_pct: float | None
    total_cost_bps: float
    invalidation_counts: Mapping[str, int]
    data_limitations: tuple[str, ...]
    research_status: str = DEVELOPMENT_ONLY
    holdout_status: str = HOLDOUT_NOT_OPENED

    @classmethod
    def from_walk_forward(
        cls, partition: FrozenPartition, stats: Mapping[str, Any], cost_bps: float,
        limitations: list[str], empty_limitation: str,
    ) -> DevelopmentEvidence:
        returns = list(stats["daily_returns"])
        wins = [value for value in returns if value > 0]
        losses = [value for value in returns if value <= 0]
        if not returns:
            limitations.append(empty_limitation)
        ratio = 0.0
        if wins and losses:
            ratio = (sum(wins) / len(wins)) / abs(sum(losses) / len(losses))
        equity = peak = 1.0
        drawdown = 0.0
        for value in returns:
            equity *= 1 + value / 100
            peak = max(peak, equity)
            drawdown = max(drawdown, (peak - equity) / peak * 100)
        evidence = cls(
            coverage_start=partition.development_dates[0] if partition.development_dates else None,
            coverage_end=partition.development_dates[-1] if partition.development_dates else None,
            signal_days=stats["signal_days"], candidate_count=stats["candidate_count"],
            filled_count=stats["filled_count"], unfilled_count=stats["unfilled_count"],
            completed_signal_days=len(returns), wins=len(wins),
            mean_net_return_pct=sum(returns) / len(returns) if returns else None,
            win_rate=len(wins) / len(returns) if returns else None,
            profit_loss_ratio=ratio if returns else None,
            max_drawdown_pct=drawdown if returns else None,
            total_cost_bps=cost_bps, invalidation_counts=stats["invalidations"],
            data_limitations=tuple(limitations),
        )
        evidence.to_payload()
        return evidence

    def to_payload(self) -> dict[str, Any]:
        if self.signal_days < 0 or min(
            self.candidate_count, self.filled_count, self.unfilled_count,
            self.completed_signal_days, self.wins,
        ) < 0:
            raise ValueError("development counts must be non-negative")
        for value in (
            self.mean_net_return_pct, self.win_rate, self.profit_loss_ratio,
            self.max_drawdown_pct, self.total_cost_bps,
        ):
            if value is not None and (isinstance(value, bool) or not math.isfinite(value)):
                raise ValueError("development metrics must be finite")
        if self.research_status != DEVELOPMENT_ONLY or self.holdout_status != HOLDOUT_NOT_OPENED:
            raise ValueError("development evidence cannot claim holdout or admission")
        payload = dict(self.__dict__)
        payload["invalidation_counts"] = dict(sorted(self.invalidation_counts.items()))
        payload["data_limitations"] = list(self.data_limitations)
        canonical_mapping(payload, "development evidence")
        return payload


@dataclass(frozen=True)
class HoldoutEvaluation:
    opened: bool
    status: str
    summary: str
    payload: Mapping[str, Any]

    def validate(self) -> None:
        _text(self.status, "holdout status")
        _text(self.summary, "holdout summary")
        if self.opened and self.status not in HOLDOUT_TERMINAL:
            raise ValueError("opened holdout must have a terminal decision")
        if not self.opened and self.status not in {
            "NOT_OPENED_IMMATURE",
            "READY_NOT_OPENED",
        }:
            raise ValueError("non-opened holdout status is invalid")
        canonical_mapping(self.payload, "holdout payload")
