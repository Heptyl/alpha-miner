"""测试模板生成的因子代码是否有真实逻辑。"""
import pytest
import pandas as pd
from datetime import datetime

from src.data.storage import Storage
from src.mining.evolution import EvolutionEngine, Candidate


@pytest.fixture
def db(tmp_path):
    db = Storage(str(tmp_path / "test.db"))
    db.init_db()
    snap = datetime(2024, 6, 13, 10, 0, 0)

    # 涨停池数据
    db.insert("zt_pool", pd.DataFrame([
        {"stock_code": "000001", "trade_date": "2024-06-14",
         "consecutive_zt": 3, "amount": 50000, "circulation_mv": 200000,
         "open_count": 0, "zt_stats": "3/3"},
        {"stock_code": "000002", "trade_date": "2024-06-14",
         "consecutive_zt": 1, "amount": 20000, "circulation_mv": 100000,
         "open_count": 1, "zt_stats": "1/1"},
    ]), snapshot_time=snap)

    # 价格数据
    db.insert("daily_price", pd.DataFrame([
        {"stock_code": "000001", "trade_date": "2024-06-14",
         "open": 10, "high": 11, "low": 9.5, "close": 11,
         "volume": 1000000, "amount": 11000000, "turnover_rate": 8.0},
        {"stock_code": "000002", "trade_date": "2024-06-14",
         "open": 5, "high": 5.5, "low": 4.8, "close": 5.5,
         "volume": 500000, "amount": 2750000, "turnover_rate": 3.0},
    ]), snapshot_time=snap)

    return db


class TestTemplateConstruct:
    """模板生成的代码必须有真实逻辑，不能返回空 Series。"""

    def test_template_code_is_not_empty(self):
        """生成的代码不能只是 return pd.Series(dtype=float)。"""
        engine = EvolutionEngine(db_path=":memory:", api_client=None)

        candidate = Candidate(
            name="cascade_momentum",
            source="knowledge",
            config={
                "factor_type": "conditional",
                "conditions": ["首次涨停", "封板未开过", "流通市值>20亿"],
                "target": "次日收益率",
                "prediction": "首次涨停后封单稳定，次日高开概率高",
            },
        )

        code = engine._template_construct(candidate)
        assert code is not None, "模板生成返回 None"
        assert len(code) > 50, f"代码太短({len(code)}字符)，疑似空实现"

        # 关键断言: 不能只有空返回
        assert "return pd.Series(dtype=float)" not in code, \
            "模板生成的代码只返回空 Series，没有真实逻辑"
        assert "return pd.Series(0.0" not in code or "db.query" in code, \
            "模板代码没有查询数据库，不可能有真实计算"

    def test_template_covers_all_seed_types(self):
        """知识库的 12 个种子假说，模板至少能覆盖 conditional 和 formula 两种类型。"""
        engine = EvolutionEngine(db_path=":memory:", api_client=None)

        conditional_candidate = Candidate(
            name="test_conditional",
            source="knowledge",
            config={
                "factor_type": "conditional",
                "conditions": ["连板>=3", "换手率<10%"],
                "target": "次日收益率",
            },
        )
        formula_candidate = Candidate(
            name="test_formula",
            source="knowledge",
            config={
                "factor_type": "formula",
                "expression": "涨停数 / 跌停数",
                "target": "次日收益率",
            },
        )

        code_c = engine._template_construct(conditional_candidate)
        code_f = engine._template_construct(formula_candidate)

        assert code_c is not None, "conditional 类型模板返回 None"
        assert code_f is not None, "formula 类型模板返回 None"
        assert "db.query" in code_c, "conditional 模板没有数据查询"
        assert "db.query" in code_f, "formula 模板没有数据查询"

    def test_template_code_is_executable(self, db):
        """模板生成的代码能在沙箱中执行不报错。"""
        engine = EvolutionEngine(db_path=db.db_path, api_client=None)

        candidate = Candidate(
            name="small_cap_trap",
            source="knowledge",
            config={
                "factor_type": "conditional",
                "conditions": ["流通市值<20亿", "换手率<10%", "连板>=3"],
                "target": "未来3日最大跌幅",
            },
        )

        code = engine._template_construct(candidate)
        assert code is not None

        # 尝试执行
        namespace = {}
        try:
            exec(code, namespace)
            assert "compute" in namespace, "生成的代码没有定义 compute 函数"
        except SyntaxError as e:
            pytest.fail(f"模板代码语法错误: {e}")
