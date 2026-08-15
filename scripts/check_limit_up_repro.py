"""Print a canonical digest for cross-runtime limit-up evolution diagnostics."""

from __future__ import annotations

import hashlib
import json

import numpy as np
import pandas as pd

from src.mining.limit_up_evolution import GENE_FEATURES, LimitUpEvolutionEngine


def _digest(frame: pd.DataFrame) -> str:
    payload = frame.to_csv(index=False, lineterminator="\n", float_format="%.10f")
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def main() -> None:
    engine = LimitUpEvolutionEngine()
    events, summary = engine.build_event_dataset()
    columns = [
        "signal_date",
        "stock_code",
        *GENE_FEATURES,
        "board_count",
        "open_count",
        "entry_gap",
        "return_1",
        "return_2",
        "drawdown_1",
        "drawdown_2",
    ]
    canonical = events[columns].copy()
    numeric_columns = canonical.select_dtypes(include=[np.number]).columns
    canonical[numeric_columns] = canonical[numeric_columns].round(10)
    canonical = canonical.sort_values(["signal_date", "stock_code"]).reset_index(drop=True)
    column_digests = {
        column: _digest(canonical[["signal_date", "stock_code", column]])
        for column in columns[2:]
    }
    engine.active_features = set(summary.get("active_features", GENE_FEATURES))
    population = [engine._sanitize_genome(item) for item in engine._initial_population(32)]
    seen: set[str] = set()
    evolution_trace = []
    for generation in range(1, 9):
        evaluations = []
        for genome in population:
            genome.generation = generation
            signature = engine._signature(genome)
            if signature in seen:
                continue
            seen.add(signature)
            evaluations.append(engine._evaluate(genome, events, summary))
        evaluations.sort(key=lambda item: item.fitness, reverse=True)
        evolution_trace.append(
            [
                {
                    "name": item.genome.name,
                    "fitness": item.fitness,
                    "train": round(item.train.avg_return, 8),
                    "validation": round(item.validation.avg_return, 8),
                }
                for item in evaluations[:5]
            ]
        )
        population = [
            engine._sanitize_genome(item)
            for item in engine._next_population(evaluations, 32, generation)
        ]
    print(
        json.dumps(
            {
                "pandas": pd.__version__,
                "numpy": np.__version__,
                "rows": len(canonical),
                "signal_dates": summary.get("signal_dates"),
                "digest": _digest(canonical),
                "columns": column_digests,
                "evolution_trace": evolution_trace,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
