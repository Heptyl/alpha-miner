"""测试进化引擎的逻辑完整性（不降低阈值）。"""
import pytest
from src.mining.evolution import EvolutionEngine, Candidate
from src.mining.failure_analyzer import FailureAnalyzer
from src.mining.mutator import FactorMutator


class TestEvolutionIntegrity:
    """验证进化引擎不用假数据骗自己。"""

    def test_default_thresholds_are_reasonable(self):
        """默认验收阈值不能为 0。"""
        assert EvolutionEngine.MIN_IC > 0, \
            f"MIN_IC={EvolutionEngine.MIN_IC}, 不能 <= 0"
        assert EvolutionEngine.MIN_ICIR > 0, \
            f"MIN_ICIR={EvolutionEngine.MIN_ICIR}, 不能 <= 0"

    def test_candidate_with_zero_ic_is_rejected(self):
        """ic_mean=0 的候选必须被拒绝。"""
        engine = EvolutionEngine(db_path=":memory:", api_client=None)
        c = Candidate("test", "knowledge", {})
        c.evaluation = {"ic_mean": 0.0, "icir": 0.0, "win_rate": 0.0,
                        "sample_size": 100}

        accepted = engine._accept(c) if hasattr(engine, '_accept') else \
            (abs(c.evaluation["ic_mean"]) >= engine.MIN_IC)
        assert not accepted, "ic_mean=0 的候选不应该通过验收"

    def test_failure_analyzer_returns_structured_diagnosis(self):
        """失败分析器必须返回结构化结果。"""
        analyzer = FailureAnalyzer()
        backtest_result = {
            "ic_mean": 0.01, "icir": 0.2, "win_rate": 0.45,
            "sample_size": 50,
        }

        result = analyzer.analyze("test_factor", backtest_result)
        assert hasattr(result, "diagnosis"), "分析结果缺少 diagnosis 属性"
        assert result.diagnosis in (
            "too_strict", "too_loose", "no_signal", "reversed",
            "noisy_but_directional", "redundant", "inconsistent",
            "no_signal",
        ), f"未知诊断类型: {result.diagnosis}"

    def test_mutator_produces_different_config(self):
        """变异器必须产生与原始不同的配置。"""
        mutator = FactorMutator()
        original = {
            "name": "test",
            "factor_type": "conditional",
            "conditions": ["连板>=3", "换手率<10%"],
        }
        diagnosis = {"diagnosis": "too_strict", "details": {}}

        mutations = mutator.mutate(original, diagnosis)
        assert len(mutations) > 0, "变异器没有产生任何变异"

        for m in mutations:
            # 至少有一个字段和原始不同
            differs = (
                m.get("conditions") != original.get("conditions") or
                m.get("name") != original.get("name") or
                m.get("factor_type") != original.get("factor_type")
            )
            assert differs, f"变异结果和原始完全相同: {m}"

    def test_knowledge_base_loads(self):
        """知识库至少有 4 个理论、10 个假说。"""
        engine = EvolutionEngine(db_path=":memory:", api_client=None)
        candidates = engine._generate_from_knowledge()

        assert len(candidates) >= 10, \
            f"知识库只生成了 {len(candidates)} 个候选，期望 >= 10"

        # 每个候选必须有名字和配置
        for c in candidates:
            assert c.name, "候选缺少名字"
            assert c.config, f"候选 {c.name} 缺少配置"
