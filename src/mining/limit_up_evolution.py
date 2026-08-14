"""Limit-up specific evolution with executable next-open trading semantics.

Unlike generic cross-sectional IC mining, this module evaluates a T0 limit-up
event as an actual A-share workflow: select after close, buy at T1 open only when
tradable, and sell no earlier than T2 because of T+1 settlement.
"""

from __future__ import annotations

import json
import math
import random
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

from src.data.storage import Storage
from src.factors.base import dedup_latest
from src.factors.formula.limit_up import build_limit_up_features

GENE_FEATURES = (
    "board_height",
    "seal_stability",
    "seal_ratio",
    "first_seal",
    "turnover_quality",
    "liquidity_rank",
    "market_heat",
    "sector_breadth",
    "capital_confirmation",
    "break_risk",
)

FEATURE_LABELS = {
    "board_height": "连板高度",
    "seal_stability": "封板稳定",
    "seal_ratio": "封单占流通盘",
    "first_seal": "首次封板早",
    "turnover_quality": "换手适中",
    "liquidity_rank": "成交活跃",
    "market_heat": "市场涨停热度",
    "sector_breadth": "同行业涨停扩散",
    "capital_confirmation": "主力资金确认",
    "break_risk": "开板风险",
}


def describe_genome(genome: "LimitUpGenome", limit: int = 4) -> str:
    """Return the dominant, human-readable structure of a genome."""
    ranked = sorted(genome.weights.items(), key=lambda item: abs(item[1]), reverse=True)
    parts = []
    for feature, weight in ranked[:limit]:
        operator = "+" if weight >= 0 else "-"
        parts.append(f"{operator}{FEATURE_LABELS.get(feature, feature)}×{abs(weight):.2f}")
    return " ".join(parts).lstrip("+")


@dataclass
class LimitUpGenome:
    name: str
    weights: dict[str, float]
    min_board: int = 1
    max_board: int = 6
    max_open_count: int = 6
    min_entry_gap: float = -4.0
    max_entry_gap: float = 5.0
    holding_days: int = 1
    top_n: int = 3
    source: str = "seed"
    generation: int = 0
    parents: list[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, value: dict) -> "LimitUpGenome":
        return cls(**value)


@dataclass
class TradeStats:
    trades: int = 0
    signal_days: int = 0
    win_rate: float = 0.0
    avg_return: float = 0.0
    median_return: float = 0.0
    pnl_ratio: float = 0.0
    p10: float = 0.0
    worst_drawdown: float = 0.0


@dataclass
class LimitUpEvaluation:
    genome: LimitUpGenome
    train: TradeStats
    validation: TradeStats
    test: TradeStats
    fitness: float
    accepted: bool = False
    rejection_reasons: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "genome": asdict(self.genome),
            "train": asdict(self.train),
            "validation": asdict(self.validation),
            "test": asdict(self.test),
            "fitness": self.fitness,
            "accepted": self.accepted,
            "rejection_reasons": self.rejection_reasons,
        }


@dataclass
class LimitUpOutcome:
    evaluations: list[LimitUpEvaluation]
    dataset_summary: dict
    best: LimitUpEvaluation | None


@dataclass
class LimitUpActionCard:
    stock_code: str
    stock_name: str
    action: str
    score: float
    entry_rule: str
    exit_rule: str
    position_rule: str
    reasons: list[str]


class LimitUpEvolutionEngine:
    """Evolve structural limit-up factor combinations and lock a chronological test set."""

    def __init__(
        self,
        db_path: str = "data/alpha_miner.db",
        state_path: str = "data/limit_up_evolution.json",
        min_market_rows: int = 100,
        min_signal_dates: int = 40,
        seed: int = 20260814,
    ):
        self.db = Storage(db_path)
        self.db.init_db()
        self.state_path = Path(state_path)
        self.min_market_rows = min_market_rows
        self.min_signal_dates = min_signal_dates
        self.random = random.Random(seed)

    def run(self, generations: int = 5, population_size: int = 24) -> LimitUpOutcome:
        if generations < 1 or population_size < 4:
            raise ValueError("generations must be >=1 and population_size must be >=4")
        events, summary = self.build_event_dataset()
        if events.empty:
            return LimitUpOutcome([], summary, None)

        population = self._initial_population(population_size)
        all_evaluations: list[LimitUpEvaluation] = []
        seen: set[str] = set()
        for generation in range(1, generations + 1):
            generation_evaluations = []
            for genome in population:
                genome.generation = generation
                signature = self._signature(genome)
                if signature in seen:
                    continue
                seen.add(signature)
                evaluation = self._evaluate(genome, events, summary)
                generation_evaluations.append(evaluation)
                all_evaluations.append(evaluation)
            generation_evaluations.sort(key=lambda item: item.fitness, reverse=True)
            population = self._next_population(generation_evaluations, population_size, generation)

        all_evaluations.sort(key=lambda item: item.fitness, reverse=True)
        best = next((item for item in all_evaluations if item.accepted), None)
        if best is None and all_evaluations:
            best = all_evaluations[0]
        outcome = LimitUpOutcome(all_evaluations, summary, best)
        self._save(outcome)
        return outcome

    def build_event_dataset(self) -> tuple[pd.DataFrame, dict]:
        old_mode = self.db.backtest_mode
        self.db.backtest_mode = True
        try:
            price = self.db.query("daily_price", datetime.now(), bypass_snapshot=True)
            price = dedup_latest(price)
            if price.empty:
                return pd.DataFrame(), {"error": "daily_price empty"}
            counts = price.groupby("trade_date")["stock_code"].nunique()
            dates = sorted(counts[counts >= self.min_market_rows].index.tolist())
            price_index = price.set_index(["trade_date", "stock_code"]).sort_index()
            date_position = {date: index for index, date in enumerate(dates)}

            zt = self.db.query("zt_pool", datetime.now(), bypass_snapshot=True)
            zt = dedup_latest(zt)
            frames: list[pd.DataFrame] = []
            usable_dates: list[str] = []
            for signal_date in sorted(zt["trade_date"].unique()) if not zt.empty else []:
                position = date_position.get(signal_date)
                if position is None or position + 3 >= len(dates):
                    continue
                path_dates = dates[position : position + 4]
                if any(self._calendar_gap(path_dates[i], path_dates[i + 1]) > 4 for i in range(3)):
                    continue

                signal_rows = zt[zt["trade_date"] == signal_date]
                universe = signal_rows["stock_code"].astype(str).unique().tolist()
                as_of = datetime.strptime(signal_date, "%Y-%m-%d").replace(hour=15)
                features = build_limit_up_features(universe, as_of, self.db)
                if features.empty:
                    continue
                raw = signal_rows.drop_duplicates("stock_code", keep="last").set_index("stock_code")
                features["stock_name"] = raw.get("name", pd.Series("", index=raw.index)).reindex(
                    features.index
                )
                features["signal_date"] = signal_date

                signal_price = self._price_slice(price_index, signal_date)
                buy_date = path_dates[1]
                buy_price = self._price_slice(price_index, buy_date)
                features["buy_date"] = buy_date
                features["entry_gap"] = (
                    buy_price["open"].reindex(features.index)
                    / signal_price["close"].reindex(features.index)
                    - 1
                ) * 100
                features["unbuyable"] = (features["entry_gap"] >= 9.5) | (
                    buy_price["high"]
                    .reindex(features.index)
                    .eq(buy_price["low"].reindex(features.index))
                    & (features["entry_gap"] >= 9.0)
                )
                entry_open = buy_price["open"].reindex(features.index).replace(0, np.nan)
                for holding_days in (1, 2):
                    exit_date = path_dates[1 + holding_days]
                    exit_price = self._price_slice(price_index, exit_date)
                    features[f"return_{holding_days}"] = (
                        exit_price["close"].reindex(features.index) / entry_open - 1
                    ) * 100
                    lows = []
                    for path_date in path_dates[1 : 2 + holding_days]:
                        lows.append(
                            self._price_slice(price_index, path_date)["low"].reindex(features.index)
                        )
                    features[f"drawdown_{holding_days}"] = (
                        pd.concat(lows, axis=1).min(axis=1) / entry_open - 1
                    ) * 100

                features = features[features["entry_gap"].notna()]
                if not features.empty:
                    frames.append(features.reset_index())
                    usable_dates.append(signal_date)

            events = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
            unique_dates = sorted(set(usable_dates))
            summary = {
                "calendar_dates": len(dates),
                "signal_dates": len(unique_dates),
                "events": len(events),
                "first_signal_date": unique_dates[0] if unique_dates else None,
                "last_signal_date": unique_dates[-1] if unique_dates else None,
                "minimum_signal_dates": self.min_signal_dates,
                "data_ready": len(unique_dates) >= self.min_signal_dates,
            }
            return events, summary
        finally:
            self.db.backtest_mode = old_mode

    @staticmethod
    def _price_slice(price_index: pd.DataFrame, date: str) -> pd.DataFrame:
        try:
            return price_index.xs(date, level="trade_date")
        except KeyError:
            return pd.DataFrame(columns=price_index.columns)

    @staticmethod
    def _calendar_gap(left: str, right: str) -> int:
        return (datetime.fromisoformat(right) - datetime.fromisoformat(left)).days

    def _evaluate(
        self,
        genome: LimitUpGenome,
        events: pd.DataFrame,
        summary: dict,
    ) -> LimitUpEvaluation:
        dates = sorted(events["signal_date"].unique())
        train_end = max(1, int(len(dates) * 0.60))
        validation_end = max(train_end + 1, int(len(dates) * 0.80))
        validation_end = min(validation_end, max(len(dates) - 1, train_end + 1))
        train_dates = set(dates[:train_end])
        validation_dates = set(dates[train_end:validation_end])
        test_dates = set(dates[validation_end:])

        train = self._trade_stats(events[events["signal_date"].isin(train_dates)], genome)
        validation = self._trade_stats(events[events["signal_date"].isin(validation_dates)], genome)
        test = self._trade_stats(events[events["signal_date"].isin(test_dates)], genome)
        fitness = self._fitness(train, validation)
        reasons = self._rejection_reasons(train, validation, test, summary)
        return LimitUpEvaluation(
            genome=genome,
            train=train,
            validation=validation,
            test=test,
            fitness=round(fitness, 6),
            accepted=not reasons,
            rejection_reasons=reasons,
        )

    def _trade_stats(self, events: pd.DataFrame, genome: LimitUpGenome) -> TradeStats:
        if events.empty:
            return TradeStats()
        eligible = events[
            events["board_count"].between(genome.min_board, genome.max_board)
            & (events["open_count"] <= genome.max_open_count)
            & events["entry_gap"].between(genome.min_entry_gap, genome.max_entry_gap)
            & ~events["unbuyable"].astype(bool)
        ].copy()
        return_column = f"return_{genome.holding_days}"
        drawdown_column = f"drawdown_{genome.holding_days}"
        eligible = eligible.dropna(subset=[return_column])
        if eligible.empty:
            return TradeStats()
        eligible["gene_score"] = self.score_frame(eligible, genome)
        selected = (
            eligible.sort_values(["signal_date", "gene_score"], ascending=[True, False])
            .groupby("signal_date", group_keys=False)
            .head(genome.top_n)
        )
        returns = selected[return_column].astype(float).to_numpy()
        wins = returns[returns > 0]
        losses = returns[returns <= 0]
        avg_win = float(wins.mean()) if len(wins) else 0.0
        avg_loss = abs(float(losses.mean())) if len(losses) else 0.0
        return TradeStats(
            trades=len(selected),
            signal_days=selected["signal_date"].nunique(),
            win_rate=float((returns > 0).mean()),
            avg_return=float(returns.mean()),
            median_return=float(np.median(returns)),
            pnl_ratio=(avg_win / avg_loss) if avg_loss else (999.0 if avg_win else 0.0),
            p10=float(np.percentile(returns, 10)),
            worst_drawdown=float(selected[drawdown_column].min()),
        )

    @staticmethod
    def score_frame(frame: pd.DataFrame, genome: LimitUpGenome) -> pd.Series:
        score = pd.Series(0.0, index=frame.index)
        for feature, weight in genome.weights.items():
            if feature in frame:
                score = score + pd.to_numeric(frame[feature], errors="coerce").fillna(0) * weight
        return score

    @staticmethod
    def _segment_score(stats: TradeStats) -> float:
        if stats.trades == 0:
            return -10.0
        pnl_term = math.log(max(stats.pnl_ratio, 0.05)) * 0.20
        sample_term = min(stats.signal_days, 12) / 60
        return (
            stats.avg_return / 5
            + (stats.win_rate - 0.5) * 2
            + pnl_term
            + stats.median_return / 10
            + sample_term
        )

    def _fitness(self, train: TradeStats, validation: TradeStats) -> float:
        score = self._segment_score(train) * 0.4 + self._segment_score(validation) * 0.6
        score -= abs(train.avg_return - validation.avg_return) / 15
        # Sparse variants can look spectacular on one validation trade. Penalize
        # the exact shortfall against the promotion sample floor so evolution
        # learns toward broader evidence before it optimizes headline return.
        score -= max(0, 30 - train.trades) * 0.08
        score -= max(0, 10 - validation.trades) * 0.25
        return score

    def _rejection_reasons(
        self,
        train: TradeStats,
        validation: TradeStats,
        test: TradeStats,
        summary: dict,
    ) -> list[str]:
        reasons = []
        if not summary.get("data_ready"):
            reasons.append(
                f"可用涨停信号日不足: {summary.get('signal_dates', 0)}<{self.min_signal_dates}"
            )
        if train.trades < 30 or validation.trades < 10 or test.trades < 10:
            reasons.append(
                f"样本不足: train/validation/test={train.trades}/{validation.trades}/{test.trades}"
            )
        if min(train.avg_return, validation.avg_return, test.avg_return) <= 0:
            reasons.append("训练/验证/测试至少一段平均收益非正")
        if test.win_rate < 0.52:
            reasons.append(f"锁定测试胜率不足: {test.win_rate:.1%}<52%")
        if test.pnl_ratio < 1.05:
            reasons.append(f"锁定测试盈亏比不足: {test.pnl_ratio:.2f}<1.05")
        return reasons

    def _initial_population(self, population_size: int) -> list[LimitUpGenome]:
        seeds = [
            LimitUpGenome(
                "zt_first_board_seal_confirm",
                self._weights(
                    seal_stability=0.25,
                    seal_ratio=0.20,
                    first_seal=0.15,
                    turnover_quality=0.15,
                    sector_breadth=0.15,
                    capital_confirmation=0.10,
                    break_risk=-0.20,
                ),
                min_board=1,
                max_board=1,
                max_open_count=3,
                max_entry_gap=4,
                top_n=4,
            ),
            LimitUpGenome(
                "zt_relay_core_quality",
                self._weights(
                    board_height=0.20,
                    seal_stability=0.20,
                    seal_ratio=0.15,
                    turnover_quality=0.10,
                    sector_breadth=0.15,
                    capital_confirmation=0.10,
                    liquidity_rank=0.10,
                    break_risk=-0.25,
                ),
                min_board=2,
                max_board=5,
                max_open_count=4,
                max_entry_gap=5,
                top_n=3,
            ),
            LimitUpGenome(
                "zt_leader_cluster_confirmation",
                self._weights(
                    board_height=0.20,
                    first_seal=0.15,
                    liquidity_rank=0.15,
                    sector_breadth=0.25,
                    capital_confirmation=0.15,
                    seal_stability=0.10,
                    break_risk=-0.20,
                ),
                min_board=1,
                max_board=6,
                max_open_count=5,
                max_entry_gap=3,
                top_n=3,
            ),
            LimitUpGenome(
                "zt_low_risk_liquid_board",
                self._weights(
                    seal_stability=0.25,
                    seal_ratio=0.20,
                    turnover_quality=0.20,
                    liquidity_rank=0.20,
                    sector_breadth=0.15,
                    break_risk=-0.35,
                ),
                min_board=1,
                max_board=3,
                max_open_count=2,
                max_entry_gap=2,
                top_n=5,
                holding_days=2,
            ),
        ]
        while len(seeds) < population_size:
            weights = {feature: self.random.uniform(-0.25, 0.35) for feature in GENE_FEATURES}
            weights["break_risk"] = -abs(weights["break_risk"])
            seeds.append(
                LimitUpGenome(
                    name=f"zt_structural_{len(seeds) + 1}",
                    weights=self._normalize(weights),
                    min_board=self.random.choice([1, 1, 2]),
                    max_board=self.random.choice([3, 4, 6]),
                    max_open_count=self.random.choice([2, 4, 6]),
                    max_entry_gap=self.random.choice([2.0, 3.0, 5.0]),
                    holding_days=self.random.choice([1, 2]),
                    top_n=self.random.choice([2, 3, 5]),
                    source="structural_exploration",
                )
            )
        return seeds[:population_size]

    @staticmethod
    def _weights(**values: float) -> dict[str, float]:
        return LimitUpEvolutionEngine._normalize(
            {feature: values.get(feature, 0.0) for feature in GENE_FEATURES}
        )

    @staticmethod
    def _normalize(weights: dict[str, float]) -> dict[str, float]:
        scale = sum(abs(value) for value in weights.values()) or 1.0
        return {key: round(value / scale, 6) for key, value in weights.items()}

    def _next_population(
        self,
        evaluations: list[LimitUpEvaluation],
        population_size: int,
        generation: int,
    ) -> list[LimitUpGenome]:
        if not evaluations:
            return self._initial_population(population_size)
        parent_count = max(2, len(evaluations) // 3)
        parents = [item.genome for item in evaluations[:parent_count]]
        children: list[LimitUpGenome] = []
        while len(children) < population_size:
            if len(children) % 3 == 0 and len(parents) >= 2:
                left, right = self.random.sample(parents, 2)
                child = self._crossover(left, right, generation, len(children))
            else:
                child = self._mutate(self.random.choice(parents), generation, len(children))
            children.append(child)
        return children

    def _mutate(self, parent: LimitUpGenome, generation: int, index: int) -> LimitUpGenome:
        weights = {
            feature: weight + self.random.gauss(0, 0.08)
            for feature, weight in parent.weights.items()
        }
        weights["break_risk"] = -abs(weights.get("break_risk", 0))
        min_board = max(1, min(3, parent.min_board + self.random.choice([-1, 0, 0, 1])))
        max_board = max(
            min_board,
            min(8, parent.max_board + self.random.choice([-1, 0, 1])),
        )
        return LimitUpGenome(
            name=f"zt_evo_g{generation + 1}_m{index + 1}",
            weights=self._normalize(weights),
            min_board=min_board,
            max_board=max_board,
            max_open_count=max(0, min(10, parent.max_open_count + self.random.choice([-1, 0, 1]))),
            min_entry_gap=parent.min_entry_gap,
            max_entry_gap=max(1, min(8, parent.max_entry_gap + self.random.choice([-1, 0, 1]))),
            holding_days=self.random.choice([parent.holding_days, 1, 2]),
            top_n=max(1, min(6, parent.top_n + self.random.choice([-1, 0, 1]))),
            source="mutation",
            generation=generation + 1,
            parents=[parent.name],
        )

    def _crossover(
        self,
        left: LimitUpGenome,
        right: LimitUpGenome,
        generation: int,
        index: int,
    ) -> LimitUpGenome:
        weights = {
            feature: (left.weights.get(feature, 0) + right.weights.get(feature, 0)) / 2
            for feature in GENE_FEATURES
        }
        return LimitUpGenome(
            name=f"zt_evo_g{generation + 1}_x{index + 1}",
            weights=self._normalize(weights),
            min_board=min(left.min_board, right.min_board),
            max_board=max(left.max_board, right.max_board),
            max_open_count=min(left.max_open_count, right.max_open_count),
            min_entry_gap=max(left.min_entry_gap, right.min_entry_gap),
            max_entry_gap=min(left.max_entry_gap, right.max_entry_gap),
            holding_days=self.random.choice([left.holding_days, right.holding_days]),
            top_n=min(left.top_n, right.top_n),
            source="crossover",
            generation=generation + 1,
            parents=[left.name, right.name],
        )

    @staticmethod
    def _signature(genome: LimitUpGenome) -> str:
        return json.dumps(
            {
                "weights": {key: round(value, 4) for key, value in sorted(genome.weights.items())},
                "min_board": genome.min_board,
                "max_board": genome.max_board,
                "max_open_count": genome.max_open_count,
                "min_entry_gap": genome.min_entry_gap,
                "max_entry_gap": genome.max_entry_gap,
                "holding_days": genome.holding_days,
                "top_n": genome.top_n,
            },
            sort_keys=True,
        )

    def _save(self, outcome: LimitUpOutcome) -> None:
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "version": 1,
            "updated_at": datetime.now().isoformat(),
            "dataset_summary": outcome.dataset_summary,
            "best": outcome.best.to_dict() if outcome.best else None,
            "top_evaluations": [item.to_dict() for item in outcome.evaluations[:20]],
        }
        temporary = self.state_path.with_suffix(self.state_path.suffix + ".tmp")
        temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        temporary.replace(self.state_path)

    def action_cards(self, date: str | None = None, top_n: int = 8) -> list[LimitUpActionCard]:
        if not self.state_path.exists():
            raise FileNotFoundError("请先运行涨停专用进化")
        state = json.loads(self.state_path.read_text(encoding="utf-8"))
        best = state.get("best")
        if not best:
            return []
        evaluation = LimitUpEvaluation(
            genome=LimitUpGenome.from_dict(best["genome"]),
            train=TradeStats(**best["train"]),
            validation=TradeStats(**best["validation"]),
            test=TradeStats(**best["test"]),
            fitness=best["fitness"],
            accepted=best["accepted"],
            rejection_reasons=best.get("rejection_reasons", []),
        )
        if date is None:
            rows = self.db.execute("SELECT MAX(trade_date) AS d FROM zt_pool")
            date = rows[0]["d"] if rows else None
        if not date:
            return []
        as_of = datetime.strptime(date, "%Y-%m-%d").replace(hour=23, minute=59)
        old_mode = self.db.backtest_mode
        self.db.backtest_mode = True
        try:
            rows = self.db.execute(
                "SELECT stock_code, name FROM zt_pool WHERE trade_date = ? ORDER BY snapshot_time",
                (date,),
            )
            names = {row["stock_code"]: row.get("name", "") for row in rows}
            universe = sorted(names)
            features = build_limit_up_features(universe, as_of, self.db)
        finally:
            self.db.backtest_mode = old_mode
        if features.empty:
            return []
        genome = evaluation.genome
        eligible = features[
            features["board_count"].between(genome.min_board, genome.max_board)
            & (features["open_count"] <= genome.max_open_count)
        ].copy()
        eligible["score"] = self.score_frame(eligible, genome)
        eligible = eligible.nlargest(top_n, "score")
        cards = []
        for code, row in eligible.iterrows():
            contributions = sorted(
                (
                    (FEATURE_LABELS[feature], float(row.get(feature, 0)) * weight)
                    for feature, weight in genome.weights.items()
                ),
                key=lambda item: abs(item[1]),
                reverse=True,
            )
            reasons = [f"{label}{value:+.3f}" for label, value in contributions[:4]]
            if row.get("break_risk", 0) >= 0.75:
                action = "AVOID"
            elif evaluation.accepted:
                action = "CONDITIONAL_BUY"
            else:
                action = "WATCH_ONLY"
            cards.append(
                LimitUpActionCard(
                    stock_code=code,
                    stock_name=names.get(code, ""),
                    action=action,
                    score=float(row["score"]),
                    entry_rule=(
                        f"次日开盘涨幅 {genome.min_entry_gap:.0f}%~{genome.max_entry_gap:.0f}%，"
                        "且非一字涨停；否则放弃"
                    ),
                    exit_rule=f"买入后第 {genome.holding_days} 个完整交易日收盘退出（遵守 T+1）",
                    position_rule="单票不超过 10%，最多 3 只；未通过锁定测试时仓位为 0",
                    reasons=reasons,
                )
            )
        return cards
