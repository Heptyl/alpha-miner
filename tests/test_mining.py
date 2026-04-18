"""Mining 模块测试 — FailureAnalyzer + FactorMutator。"""

import numpy as np
import pandas as pd
import pytest

from src.mining.failure_analyzer import FailureAnalyzer, FailureDiagnosis
from src.mining.mutator import FactorMutator


# ============================================================
# FailureAnalyzer Tests
# ============================================================

class TestFailureAnalyzer:
    def test_too_strict(self):
        """样本量极少 → too_strict。"""
        analyzer = FailureAnalyzer()
        result = analyzer.analyze("test_factor", {
            "ic_mean": 0.005,
            "icir": 0.1,
            "avg_sample_per_day": 3,
            "max_correlation": 0.2,
            "ic_series": pd.Series([0.01, -0.01, 0.0]),
        })
        assert result.diagnosis == "too_strict"
        assert "放宽" in result.suggestion

    def test_too_loose(self):
        """样本量极大但无信号 → too_loose。"""
        analyzer = FailureAnalyzer()
        result = analyzer.analyze("test_factor", {
            "ic_mean": 0.005,
            "icir": 0.1,
            "avg_sample_per_day": 150,
            "max_correlation": 0.2,
            "ic_series": pd.Series([0.01, -0.01, 0.0]),
        })
        assert result.diagnosis == "too_loose"
        assert "收紧" in result.suggestion

    def test_no_signal(self):
        """正常样本但无信号。"""
        analyzer = FailureAnalyzer()
        result = analyzer.analyze("test_factor", {
            "ic_mean": 0.005,
            "icir": 0.1,
            "avg_sample_per_day": 30,
            "max_correlation": 0.2,
            "ic_series": pd.Series([0.01, -0.01, 0.0]),
        })
        assert result.diagnosis == "no_signal"

    def test_reversed(self):
        """IC 为负 → reversed。"""
        analyzer = FailureAnalyzer()
        result = analyzer.analyze("test_factor", {
            "ic_mean": -0.05,
            "icir": -1.2,
            "avg_sample_per_day": 30,
            "max_correlation": 0.2,
            "ic_series": pd.Series([-0.05, -0.04, -0.06]),
        })
        assert result.diagnosis == "reversed"
        assert "反转" in result.suggestion

    def test_redundant(self):
        """高相关 → redundant。"""
        analyzer = FailureAnalyzer()
        result = analyzer.analyze("test_factor", {
            "ic_mean": 0.05,
            "icir": 1.5,
            "avg_sample_per_day": 30,
            "max_correlation": 0.85,
            "most_correlated_factor": "other_factor",
            "ic_series": pd.Series([0.05, 0.04, 0.06] * 5),
        })
        assert result.diagnosis == "redundant"
        assert "other_factor" in result.suggestion

    def test_noisy_but_directional(self):
        """IC > 0 但 ICIR < 0.5 且 IC 正比例 > 60%。"""
        analyzer = FailureAnalyzer()
        ic_series = pd.Series([0.05, -0.02, 0.04, 0.01, -0.01, 0.03, 0.02, 0.06, -0.01, 0.04])
        result = analyzer.analyze("test_factor", {
            "ic_mean": 0.02,
            "icir": 0.3,
            "avg_sample_per_day": 30,
            "max_correlation": 0.2,
            "ic_series": ic_series,
        })
        assert result.diagnosis == "noisy_but_directional"
        assert "regime" in result.suggestion


# ============================================================
# FactorMutator Tests
# ============================================================

class TestFactorMutator:
    @pytest.fixture
    def base_config(self):
        return {
            "name": "test_factor",
            "factor_type": "conditional",
            "lookback_days": 5,
            "conditions": [
                {"name": "cond1", "table": "zt_pool", "column": "consecutive_zt", "operator": ">=", "value": 3},
                {"name": "cond2", "table": "daily_price", "column": "turnover_rate", "operator": "<", "value": 10},
                {"name": "cond3", "table": "daily_price", "column": "volume", "operator": ">", "value": 5000},
            ],
        }

    def test_mutate_too_strict(self, base_config):
        """too_strict → 放宽 + 移除条件。"""
        mutator = FactorMutator()
        mutations = mutator.mutate(base_config, {"diagnosis": "too_strict"})
        assert len(mutations) >= 2
        # 放宽版
        assert "loose" in mutations[0]["name"]
        # 移除条件版
        assert "less_cond" in mutations[1]["name"]
        assert len(mutations[1]["conditions"]) < len(base_config["conditions"])

    def test_mutate_too_loose(self, base_config):
        """too_loose → 收紧 + 增加条件。"""
        mutator = FactorMutator()
        mutations = mutator.mutate(base_config, {"diagnosis": "too_loose"})
        assert len(mutations) >= 2
        assert "tight" in mutations[0]["name"]
        assert "extra_cond" in mutations[1]["name"]

    def test_mutate_reversed(self, base_config):
        """reversed → 反转。"""
        mutator = FactorMutator()
        mutations = mutator.mutate(base_config, {"diagnosis": "reversed"})
        assert len(mutations) >= 1
        assert mutations[0].get("reverse") is True

    def test_mutate_noisy(self, base_config):
        """noisy → 加 regime 过滤。"""
        mutator = FactorMutator()
        mutations = mutator.mutate(base_config, {"diagnosis": "noisy_but_directional"})
        assert len(mutations) >= 2
        assert "regime_filter" in mutations[0]
        assert "regime_filter" in mutations[1]

    def test_mutate_preserves_original(self, base_config):
        """变异不应修改原始配置。"""
        mutator = FactorMutator()
        original_conditions = list(base_config["conditions"])
        mutator.mutate(base_config, {"diagnosis": "too_strict"})
        assert base_config["conditions"] == original_conditions
        assert base_config["name"] == "test_factor"

    def test_loosen_thresholds_values(self, base_config):
        """验证放宽阈值的数值变化。"""
        mutator = FactorMutator()
        result = mutator._loosen_thresholds(base_config, ratio=0.8)
        # cond1: >= 3 → >= 2.4
        assert result["conditions"][0]["value"] == pytest.approx(2.4)
        # cond2: < 10 → < 12
        assert result["conditions"][1]["value"] == pytest.approx(12.0)
        # cond3: > 5000 → > 4000
        assert result["conditions"][2]["value"] == pytest.approx(4000.0)

    def test_tighten_thresholds_values(self, base_config):
        """验证收紧阈值的数值变化。"""
        mutator = FactorMutator()
        result = mutator._tighten_thresholds(base_config, ratio=1.2)
        # cond1: >= 3 → >= 3.6
        assert result["conditions"][0]["value"] == pytest.approx(3.6)
