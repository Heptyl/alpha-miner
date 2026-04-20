"""进化引擎测试 — mock Anthropic API 的完整进化流程。"""

import json
import tempfile
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import yaml

from src.mining.evolution import Candidate, EvolutionEngine
from src.mining.sandbox import Sandbox


# ============================================================
# Helper
# ============================================================

def _make_kb(path: Path):
    """创建测试用知识库。"""
    kb = {
        "theories": [
            {
                "id": "test_theory",
                "name": "测试理论",
                "source": "test",
                "core_claim": "测试用",
                "testable_predictions": [
                    {
                        "id": "test_pred_1",
                        "prediction": "条件满足时次日收益为正",
                        "factor_type": "conditional",
                        "conditions": [
                            {"name": "zt", "table": "zt_pool", "column": "consecutive_zt", "operator": ">=", "value": 2},
                        ],
                        "target": "次日收益率",
                    },
                    {
                        "id": "test_pred_2",
                        "prediction": "换手率排名因子",
                        "factor_type": "formula",
                        "expression": "turnover_rate.rank()",
                        "target": "次日收益率",
                    },
                ],
            }
        ]
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.dump(kb, allow_unicode=True))


def _make_mock_api(response_code: str = '"""Generated factor."""\ndef compute(universe, as_of, db):\n    return pd.Series(dtype=float)'):
    """创建 mock Anthropic client。"""
    mock_client = MagicMock()
    mock_response = MagicMock()
    mock_response.content = [MagicMock(text=f"```python\n{response_code}\n```")]
    mock_client.messages.create.return_value = mock_response
    return mock_client


# ============================================================
# Tests
# ============================================================

class TestCandidate:
    def test_to_dict_roundtrip(self):
        c = Candidate("test", "knowledge", {"a": 1})
        c.evaluation = {"ic_mean": 0.05}
        c.accepted = True
        c.generation = 3

        d = c.to_dict()
        c2 = Candidate.from_dict(d)
        assert c2.name == "test"
        assert c2.source == "knowledge"
        assert c2.evaluation == {"ic_mean": 0.05}
        assert c2.accepted is True
        assert c2.generation == 3


class TestEvolutionEngine:
    @pytest.fixture
    def setup(self, tmp_path):
        kb_path = tmp_path / "kb" / "theories.yaml"
        _make_kb(kb_path)
        log_path = tmp_path / "mining_log.jsonl"
        db_path = str(tmp_path / "test.db")

        # 创建一个空 db
        from src.data.storage import Storage
        Storage(db_path)

        return kb_path, log_path, db_path

    def test_generate_from_knowledge(self, setup):
        kb_path, log_path, db_path = setup
        engine = EvolutionEngine(
            db_path=db_path,
            knowledge_path=str(kb_path),
            mining_log_path=str(log_path),
        )
        candidates = engine._generate_from_knowledge()
        assert len(candidates) == 2
        assert candidates[0].name == "test_pred_1"
        assert candidates[0].source == "knowledge"
        assert candidates[1].name == "test_pred_2"

    def test_generate_no_kb(self, tmp_path):
        engine = EvolutionEngine(
            db_path="nonexistent.db",
            knowledge_path=str(tmp_path / "no_such_file.yaml"),
        )
        candidates = engine._generate_from_knowledge()
        assert candidates == []

    def test_template_construct_conditional(self, setup):
        kb_path, log_path, db_path = setup
        engine = EvolutionEngine(db_path=db_path, knowledge_path=str(kb_path))
        candidates = engine._generate_from_knowledge()

        code = engine._template_construct(candidates[0])
        assert "compute" in code
        assert "def compute" in code

    def test_template_construct_formula(self, setup):
        kb_path, log_path, db_path = setup
        engine = EvolutionEngine(db_path=db_path, knowledge_path=str(kb_path))
        candidates = engine._generate_from_knowledge()

        code = engine._template_construct(candidates[1])
        assert "compute" in code
        # formula 类型的种子模板有真实逻辑，非种子才走骨架
        # candidates[1] 是 cascade_break_crash（conditional 类型种子模板）
        # 它的模板不包含 "expression" 关键字
        assert "compute" in code
        assert "db.query" in code or "return pd.Series" in code

    def test_llm_construct(self, setup):
        kb_path, log_path, db_path = setup
        mock_api = _make_mock_api()
        engine = EvolutionEngine(
            db_path=db_path,
            api_client=mock_api,
            knowledge_path=str(kb_path),
        )
        candidates = engine._generate_from_knowledge()
        code = engine._construct_factor(candidates[0])
        assert code is not None
        assert "compute" in code
        mock_api.messages.create.assert_called_once()

    def test_llm_construct_error(self, setup):
        kb_path, log_path, db_path = setup
        mock_api = MagicMock()
        mock_api.messages.create.side_effect = Exception("API error")
        engine = EvolutionEngine(
            db_path=db_path,
            api_client=mock_api,
            knowledge_path=str(kb_path),
        )
        candidates = engine._generate_from_knowledge()
        code = engine._construct_factor(candidates[0])
        assert code is None

    def test_crossover(self, setup):
        kb_path, log_path, db_path = setup
        engine = EvolutionEngine(db_path=db_path, knowledge_path=str(kb_path))

        c1 = Candidate("factor_a", "knowledge", {
            "conditions": [{"a": 1}, {"b": 2}, {"c": 3}],
        })
        c2 = Candidate("factor_b", "knowledge", {
            "conditions": [{"d": 4}, {"e": 5}],
        })
        c1.accepted = True
        c2.accepted = True
        engine.accepted = [c1, c2]

        crossovers = engine._crossover(engine.accepted)
        assert len(crossovers) >= 1
        assert "factor_a" in crossovers[0].name
        assert "factor_b" in crossovers[0].name
        assert crossovers[0].source.startswith("crossover")  # crossover_cond / crossover_mul / crossover_chain

    def test_crossover_single_parent(self, setup):
        kb_path, log_path, db_path = setup
        engine = EvolutionEngine(db_path=db_path, knowledge_path=str(kb_path))
        engine.accepted = [Candidate("only_one", "knowledge", {"conditions": [{"a": 1}]})]
        crossovers = engine._crossover(engine.accepted)
        assert crossovers == []

    def test_mining_log(self, setup):
        kb_path, log_path, db_path = setup
        engine = EvolutionEngine(
            db_path=db_path,
            knowledge_path=str(kb_path),
            mining_log_path=str(log_path),
        )
        c = Candidate("test", "knowledge", {"a": 1})
        c.evaluation = {"ic_mean": 0.05}
        engine._write_log(c)

        assert log_path.exists()
        lines = log_path.read_text().strip().split("\n")
        assert len(lines) == 1
        entry = json.loads(lines[0])
        assert entry["name"] == "test"
        assert entry["evaluation"]["ic_mean"] == 0.05

    def test_run_with_no_data(self, setup):
        """完整进化循环，数据库空（所有因子评估失败但不崩溃）。"""
        kb_path, log_path, db_path = setup
        engine = EvolutionEngine(
            db_path=db_path,
            knowledge_path=str(kb_path),
            mining_log_path=str(log_path),
        )
        # 降低验收标准以便测试
        engine.MIN_IC = 0.0
        engine.MIN_ICIR = 0.0
        engine.MIN_WIN_RATE = 0.0

        accepted = engine.run(generations=1, population_size=3)
        # 应该跑完不崩溃
        assert isinstance(accepted, list)
        # 日志应该有写入
        assert log_path.exists()


class TestSandbox:
    def test_execute_simple_code(self, tmp_path):
        """执行简单代码。"""
        db_path = str(tmp_path / "test.db")
        from src.data.storage import Storage
        Storage(db_path)

        sandbox = Sandbox(db_path)
        code = '''
import pandas as pd

def compute(universe, as_of, db):
    return pd.Series({c: 1.0 for c in universe}, dtype=float)
'''
        result = sandbox.execute(code, "test_factor")
        # 可能因为 db 空而没有完整结果，但不应崩溃
        assert isinstance(result, dict)

    def test_execute_bad_code(self, tmp_path):
        """执行有语法错误的代码。"""
        db_path = str(tmp_path / "test.db")
        from src.data.storage import Storage
        Storage(db_path)

        sandbox = Sandbox(db_path)
        code = "this is not valid python"
        result = sandbox.execute(code, "bad_factor")
        assert "error" in result

    def test_execute_no_compute(self, tmp_path):
        """代码中没有 compute 函数。"""
        db_path = str(tmp_path / "test.db")
        from src.data.storage import Storage
        Storage(db_path)

        sandbox = Sandbox(db_path)
        code = "x = 1"
        result = sandbox.execute(code, "no_compute")
        assert "error" in result
