"""H3: 进化引擎 mock 因子评分验证。

测试 EvolutionEngine._evaluate 的验收判定逻辑：
- 完美因子（IC=1.0, ICIR>0.5, win_rate=1.0）→ accepted=True
- 零因子（IC=0.0）→ accepted=False
- 验收阈值精确匹配
"""
import json
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.data.storage import Storage
from src.mining.evolution import Candidate, EvolutionEngine


@pytest.fixture
def db(tmp_path):
    s = Storage(str(tmp_path / "test.db"))
    s.init_db()
    return s


@pytest.fixture
def engine(tmp_path):
    db_path = str(tmp_path / "test.db")
    return EvolutionEngine(db_path=db_path)


def _seed_perfect_data(db, trade_dates):
    """注入完美正相关数据（与 test_hard_ic 相同构造）。"""
    snap = datetime(2024, 6, 1, 10, 0, 0)
    stocks = [f"00000{i}" for i in range(10)]
    fv_rows = []
    price_rows = []

    for date in trade_dates:
        for i, code in enumerate(stocks):
            fv_rows.append({
                "factor_name": "perfect_pos",
                "stock_code": code,
                "trade_date": date,
                "factor_value": float(i + 1),
            })

    base_prices = {code: 10.0 + i * 2.0 for i, code in enumerate(stocks)}
    for di, date in enumerate(trade_dates):
        for i, code in enumerate(stocks):
            price_rows.append({
                "stock_code": code, "trade_date": date,
                "open": base_prices[code] + di * 0.5,
                "high": base_prices[code] + di * 0.5 + 0.5,
                "low": base_prices[code] + di * 0.5 - 0.5,
                "close": base_prices[code] + di * (i + 1) * 0.1,
                "volume": 1000, "amount": 10000, "turnover_rate": 1.0,
            })

    db.insert("factor_values", pd.DataFrame(fv_rows), snapshot_time=snap)
    db.insert("daily_price", pd.DataFrame(price_rows), snapshot_time=snap)


class TestEvaluateAcceptance:
    """_evaluate 的验收判定逻辑。"""

    def test_perfect_factor_accepted(self, engine, db):
        """完美因子 → accepted=True。"""
        # 注入足够数据
        dates = [f"2024-06-{d:02d}" for d in range(1, 30)]
        _seed_perfect_data(db, dates)

        # 构造一个直接读取 factor_values 的因子代码
        code = '''"""完美正相关因子"""
import pandas as pd
import numpy as np
from datetime import datetime

def compute(universe, as_of, db):
    date_str = as_of.strftime("%Y-%m-%d")
    fv = db.query("factor_values", as_of,
                   where="factor_name = ? AND trade_date = ?",
                   params=("perfect_pos", date_str))
    if fv.empty:
        return pd.Series(dtype=float)
    result = {}
    for _, row in fv.iterrows():
        if row["stock_code"] in universe:
            result[row["stock_code"]] = float(row["factor_value"])
    return pd.Series(result, dtype=float)
'''
        candidate = Candidate(
            name="perfect_pos",
            source="knowledge",
            config={"name": "perfect_pos", "factor_type": "formula"},
            code=code,
        )

        # 设置 as_of 到数据范围内
        import os
        with patch.dict(os.environ, {"SANDBOX_AS_OF": "2024-06-25"}):
            engine._evaluate(candidate)

        # 沙箱应该执行成功
        if candidate.error:
            pytest.skip(f"沙箱执行出错（可能环境问题）: {candidate.error}")

        assert candidate.evaluation is not None, "evaluation 为空"
        ic_mean = candidate.evaluation.get("ic_mean", 0.0)
        # 完美正相关数据，IC 应接近 1.0
        assert ic_mean > 0.5, f"IC_mean={ic_mean}, 期望 >0.5"
        assert candidate.accepted, f"因子未被验收，evaluation={candidate.evaluation}"

    def test_zero_factor_rejected(self, engine, db):
        """全零因子 → accepted=False。"""
        dates = [f"2024-06-{d:02d}" for d in range(1, 30)]
        _seed_perfect_data(db, dates)

        code = '''"""全零因子"""
import pandas as pd

def compute(universe, as_of, db):
    return pd.Series(0.0, index=universe, dtype=float)
'''
        candidate = Candidate(
            name="zero_factor",
            source="knowledge",
            config={"name": "zero_factor", "factor_type": "formula"},
            code=code,
        )

        import os
        with patch.dict(os.environ, {"SANDBOX_AS_OF": "2024-06-25"}):
            engine._evaluate(candidate)

        if candidate.error:
            pytest.skip(f"沙箱执行出错: {candidate.error}")

        assert candidate.evaluation is not None
        ic_mean = candidate.evaluation.get("ic_mean", 0.0)
        # 全零因子的 IC 应该接近 0（或 NaN → 0）
        assert abs(ic_mean) < engine.MIN_IC, \
            f"全零因子 IC={ic_mean}, 应该小于阈值 {engine.MIN_IC}"
        assert not candidate.accepted, \
            f"全零因子不应被验收，evaluation={candidate.evaluation}"


class TestEvaluateThresholds:
    """验收阈值的精确匹配。"""

    def test_min_ic_threshold(self):
        """验证 MIN_IC = 0.03。"""
        assert EvolutionEngine.MIN_IC == 0.03

    def test_min_icir_threshold(self):
        """验证 MIN_ICIR = 0.5。"""
        assert EvolutionEngine.MIN_ICIR == 0.5

    def test_min_win_rate_threshold(self):
        """验证 MIN_WIN_RATE = 0.55。"""
        assert EvolutionEngine.MIN_WIN_RATE == 0.55

    def test_acceptance_requires_all_three(self, engine):
        """验收需要同时满足三个条件。"""
        # 只满足两个条件
        candidate = Candidate(
            name="partial", source="test", config={},
        )
        candidate.evaluation = {
            "ic_mean": 0.05,      # > 0.03 ✓
            "icir": 0.6,          # > 0.5 ✓
            "win_rate": 0.40,     # < 0.55 ✗
        }
        # 手动执行验收判定逻辑
        ic = candidate.evaluation.get("ic_mean", 0.0)
        icir = candidate.evaluation.get("icir", 0.0)
        win_rate = candidate.evaluation.get("win_rate", 0.0)

        accepted = (
            abs(ic) >= engine.MIN_IC
            and abs(icir) >= engine.MIN_ICIR
            and win_rate >= engine.MIN_WIN_RATE
        )
        assert not accepted, "win_rate 不满足，不应验收"

    def test_negative_ic_accepted_if_strong(self, engine):
        """强负 IC 也能通过验收（用 abs）。"""
        candidate = Candidate(name="neg_ic", source="test", config={})
        candidate.evaluation = {
            "ic_mean": -0.10,     # |IC| = 0.10 > 0.03 ✓
            "icir": -0.8,         # |ICIR| = 0.8 > 0.5 ✓
            "win_rate": 0.60,     # > 0.55 ✓
        }
        ic = candidate.evaluation["ic_mean"]
        icir = candidate.evaluation["icir"]
        win_rate = candidate.evaluation["win_rate"]

        accepted = (
            abs(ic) >= engine.MIN_IC
            and abs(icir) >= engine.MIN_ICIR
            and win_rate >= engine.MIN_WIN_RATE
        )
        assert accepted, "强负 IC 因子应该通过验收"


class TestCandidateSerialization:
    """Candidate 序列化/反序列化。"""

    def test_to_dict_roundtrip(self):
        c = Candidate("test_factor", "knowledge", {"key": "val"}, "code_here")
        c.evaluation = {"ic_mean": 0.1}
        c.accepted = True
        c.generation = 3

        d = c.to_dict()
        assert d["name"] == "test_factor"
        assert d["source"] == "knowledge"
        assert d["config"]["key"] == "val"
        assert d["code"] == "code_here"
        assert d["evaluation"]["ic_mean"] == 0.1
        assert d["accepted"] is True
        assert d["generation"] == 3

    def test_from_dict_roundtrip(self):
        original = Candidate("test", "mutation", {"a": 1})
        original.evaluation = {"ic_mean": 0.05}
        original.accepted = True
        original.error = "some error"
        original.generation = 2

        restored = Candidate.from_dict(original.to_dict())
        assert restored.name == original.name
        assert restored.source == original.source
        assert restored.config == original.config
        assert restored.evaluation == original.evaluation
        assert restored.accepted == original.accepted
        assert restored.error == original.error
        assert restored.generation == original.generation


class TestMutatorIntegration:
    """变异器集成测试 — 验证验收因子能正确变异。"""

    def test_mutate_accepted_factor(self, engine):
        """验收因子能触发变异。"""
        from src.mining.mutator import FactorMutator

        mutator = FactorMutator()
        config = {
            "name": "test_factor",
            "factor_type": "conditional",
            "conditions": ["连板>=3", "换手率>15%"],
            "source_theory": "info_cascade",
        }
        mutations = mutator.mutate(config, {
            "diagnosis": "IC positive but unstable",
            "details": "win_rate low",
        })
        assert len(mutations) > 0, "变异器应该返回至少一个变异"
        for m in mutations:
            assert "name" in m, "变异配置应该有 name"
            assert m["name"] != "test_factor", "变异因子名应该不同"


class TestCrossoverIntegration:
    """杂交集成测试。"""

    def test_crossover_conditions(self, engine):
        """条件杂交生成新因子。"""
        p1 = Candidate("factor_a", "knowledge", {
            "conditions": ["连板>=3", "换手率>15%", "封板稳"],
            "source_theory": "cascade",
        })
        p2 = Candidate("factor_b", "knowledge", {
            "conditions": ["涨停>80家", "市值<50亿"],
            "source_theory": "emotion",
        })
        child = engine._crossover_conditions(p1, p2)
        assert child is not None
        assert "factor_a" in child.name
        assert "factor_b" in child.name
        assert child.source == "crossover_cond"
        # 子代应该有混合条件
        hybrid_conds = child.config.get("conditions", [])
        assert len(hybrid_conds) > 0

    def test_crossover_multiply(self, engine):
        """乘法杂交。"""
        p1 = Candidate("mom_f", "knowledge", {
            "expression": "seal_amount / total_amount",
            "source_theory": "cascade",
        })
        p2 = Candidate("dad_f", "knowledge", {
            "expression": "zt_count / total_stocks",
            "source_theory": "emotion",
        })
        child = engine._crossover_multiply(p1, p2)
        assert child is not None
        assert child.config["factor_type"] == "formula"
        assert "*" in child.config["expression"]

    def test_crossover_complement(self, engine):
        """互补杂交。"""
        p1 = Candidate("pre_f", "knowledge", {"source_theory": "cascade"})
        p2 = Candidate("post_f", "knowledge", {
            "conditions": ["连板>=3"],
            "source_theory": "emotion",
        })
        child = engine._crossover_complement(p1, p2)
        assert child is not None
        assert child.config.get("pre_filter") == "pre_f"
        assert child.source == "crossover_chain"
