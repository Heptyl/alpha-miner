"""涨停板结构因子、可成交标签和操作闸门测试。"""

from datetime import datetime

import pandas as pd

from src.data.storage import Storage
from src.factors.formula.limit_up import build_limit_up_features
from src.mining.limit_up_evolution import (
    LimitUpEvolutionEngine,
    LimitUpGenome,
    TradeStats,
    describe_genome,
    describe_rule,
)


def _build_limit_up_db(path) -> Storage:
    db = Storage(str(path))
    db.init_db()
    dates = pd.bdate_range("2024-01-02", periods=9).strftime("%Y-%m-%d").tolist()
    price_rows = []
    for day_index, date in enumerate(dates):
        for code, step in (("000001", 1.03), ("000002", 0.97), ("000003", 1.0), ("000004", 1.0)):
            close = 10 * (step**day_index)
            price_rows.append(
                {
                    "stock_code": code,
                    "trade_date": date,
                    "open": close * 0.995,
                    "high": close * 1.02,
                    "low": close * 0.98,
                    "close": close,
                    "pre_close": close / step,
                    "volume": 1000,
                    "amount": 10_000_000 + day_index,
                    "turnover_rate": 10,
                }
            )
    db.insert("daily_price", pd.DataFrame(price_rows))

    zt_rows = []
    for date in dates[:6]:
        zt_rows.extend(
            [
                {
                    "stock_code": "000001",
                    "name": "good",
                    "trade_date": date,
                    "consecutive_zt": 2,
                    "amount": 80_000_000,
                    "industry": "AI",
                    "circulation_mv": 1_000_000_000,
                    "total_mv": 2_000_000_000,
                    "turnover_rate": 15,
                    "seal_amount": 40_000_000,
                    "first_seal_time": "093000",
                    "last_seal_time": "093000",
                    "open_count": 2,
                    "zt_stats": "2/2",
                },
                {
                    "stock_code": "000002",
                    "name": "bad",
                    "trade_date": date,
                    "consecutive_zt": 1,
                    "amount": 20_000_000,
                    "industry": "Other",
                    "circulation_mv": 2_000_000_000,
                    "total_mv": 3_000_000_000,
                    "turnover_rate": 38,
                    "seal_amount": 1_000_000,
                    "first_seal_time": "145500",
                    "last_seal_time": "145900",
                    "open_count": 7,
                    "zt_stats": "1/1",
                },
                {
                    "stock_code": "000003",
                    "name": "peer",
                    "trade_date": date,
                    "consecutive_zt": 1,
                    "amount": 30_000_000,
                    "industry": "AI",
                    "circulation_mv": 1_500_000_000,
                    "total_mv": 2_500_000_000,
                    "turnover_rate": 18,
                    "seal_amount": 20_000_000,
                    "first_seal_time": "100000",
                    "last_seal_time": "100000",
                    "open_count": 0,
                    "zt_stats": "1/1",
                },
            ]
        )
    db.insert("zt_pool", pd.DataFrame(zt_rows))
    return db


def test_new_limit_up_features_are_structural_and_interpretable(tmp_path):
    db = _build_limit_up_db(tmp_path / "limit.db")
    db.backtest_mode = True
    values = build_limit_up_features(
        ["000001", "000002", "000003"],
        datetime(2024, 1, 2, 15),
        db,
    )

    assert values.loc["000001", "seal_strength"] > values.loc["000002", "seal_strength"]
    assert values.loc["000001", "relay_quality"] > values.loc["000002", "relay_quality"]
    assert values.loc["000001", "break_risk"] < values.loc["000002", "break_risk"]
    assert values.loc["000001", "sector_breadth"] > values.loc["000002", "sector_breadth"]
    assert values.loc["000001", "reseal_quality"] > values.loc["000003", "reseal_quality"]


def test_genome_description_and_mutation_bounds(tmp_path):
    engine = LimitUpEvolutionEngine(
        db_path=str(tmp_path / "limit.db"),
        state_path=str(tmp_path / "state.json"),
    )
    parent = LimitUpGenome(
        name="seed",
        weights={"seal_stability": 0.6, "break_risk": -0.4},
        min_board=3,
        max_board=3,
        min_open_count=1,
        max_open_count=3,
    )

    assert "封板稳定" in describe_genome(parent)
    assert "-开板风险" in describe_genome(parent)
    assert "T0开板1-3次" in describe_rule(parent)
    for index in range(100):
        child = engine._mutate(parent, generation=1, index=index)
        assert child.max_board >= child.min_board
        assert child.max_open_count >= child.min_open_count


def test_fitness_penalizes_one_trade_validation_winners(tmp_path):
    engine = LimitUpEvolutionEngine(
        db_path=str(tmp_path / "limit.db"),
        state_path=str(tmp_path / "state.json"),
    )
    train = TradeStats(
        trades=30, signal_days=10, win_rate=0.6, avg_return=1, median_return=1, pnl_ratio=1.5
    )
    broad = TradeStats(
        trades=10, signal_days=3, win_rate=0.6, avg_return=1, median_return=1, pnl_ratio=1.5
    )
    sparse = TradeStats(
        trades=1, signal_days=1, win_rate=1, avg_return=6, median_return=6, pnl_ratio=999
    )

    assert engine._fitness(train, broad) > engine._fitness(train, sparse)


def test_event_dataset_uses_next_open_and_t_plus_one_exit(tmp_path):
    db = _build_limit_up_db(tmp_path / "limit.db")
    db.insert(
        "daily_price",
        pd.DataFrame(
            [
                {
                    "stock_code": code,
                    "trade_date": "2024-01-06",
                    "open": 10,
                    "high": 11,
                    "low": 9,
                    "close": 10,
                    "volume": 1000,
                    "amount": 10_000,
                }
                for code in ("000001", "000002", "000003", "000004")
            ]
        ),
    )
    db.insert(
        "zt_pool",
        pd.DataFrame(
            [
                {
                    "stock_code": "000001",
                    "trade_date": "2024-01-06",
                    "consecutive_zt": 1,
                }
            ]
        ),
    )
    engine = LimitUpEvolutionEngine(
        db_path=str(tmp_path / "limit.db"),
        state_path=str(tmp_path / "state.json"),
        min_market_rows=4,
        min_signal_dates=2,
    )
    events, summary = engine.build_event_dataset()

    assert summary["signal_dates"] >= 3
    assert "reseal_quality" in summary["active_features"]
    assert "market_heat" in summary["inactive_features"]
    assert summary["source_coverage"]["seal_amount"] == 1.0
    assert summary["excluded_non_trading_dates"] == ["2024-01-06"]
    assert "2024-01-06" not in set(events["signal_date"])
    good = events[events["stock_code"] == "000001"]
    bad = events[events["stock_code"] == "000002"]
    assert good["return_1"].mean() > 0
    assert bad["return_1"].mean() < 0
    assert (good["buy_date"] > good["signal_date"]).all()
    tied_genome = LimitUpGenome(name="tied", weights={}, top_n=2)
    assert engine._trade_stats(events, tied_genome) == engine._trade_stats(
        events.sample(frac=1, random_state=7), tied_genome
    )


def test_unvalidated_evolution_only_outputs_watch_or_avoid(tmp_path):
    _build_limit_up_db(tmp_path / "limit.db")
    state_path = tmp_path / "state.json"
    engine = LimitUpEvolutionEngine(
        db_path=str(tmp_path / "limit.db"),
        state_path=str(state_path),
        min_market_rows=4,
        min_signal_dates=40,
    )
    outcome = engine.run(generations=2, population_size=6)

    assert outcome.best is not None
    assert outcome.best.genome.name.startswith("zt_")
    assert not outcome.best.accepted
    cards = engine.action_cards(date="2024-01-09", top_n=2)
    assert cards
    assert {card.action for card in cards} <= {"WATCH_ONLY", "AVOID"}
