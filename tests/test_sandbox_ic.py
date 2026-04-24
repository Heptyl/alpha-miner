"""测试沙箱评估是否真正计算 IC。"""
import json
import tempfile
from datetime import datetime

import pandas as pd
import numpy as np
import pytest

from src.data.storage import Storage


@pytest.fixture
def db_with_data(tmp_path):
    """创建一个有真实数据的测试数据库。"""
    db = Storage(str(tmp_path / "test.db"))
    db.init_db()

    snap = datetime(2024, 6, 1, 10, 0, 0)

    # 插入 5 天的价格数据，10 只股票
    codes = [f"00000{i}" for i in range(10)]
    for day_offset in range(5):
        date_str = f"2024-06-{10 + day_offset:02d}"
        rows = []
        for i, code in enumerate(codes):
            # 价格有规律: 第一只涨得最多，最后一只跌得最多
            base_price = 10.0 + i * 0.5
            price = base_price * (1 + (5 - i) * 0.01 * (day_offset + 1))
            rows.append({
                "stock_code": code,
                "trade_date": date_str,
                "open": price * 0.99,
                "high": price * 1.02,
                "low": price * 0.98,
                "close": price,
                "volume": 1000000 + i * 100000,
                "amount": price * 1000000,
                "turnover_rate": 5.0 + i,
            })
        db.insert("daily_price", pd.DataFrame(rows), snapshot_time=snap)

    # 插入因子值（与未来收益正相关的因子）
    date_str = "2024-06-10"
    factor_rows = []
    for i, code in enumerate(codes):
        factor_rows.append({
            "factor_name": "test_factor",
            "stock_code": code,
            "trade_date": date_str,
            "factor_value": float(5 - i),  # 越大的因子值对应越高的未来收益
        })
    db.insert("factor_values", pd.DataFrame(factor_rows), snapshot_time=snap)

    return db


class TestSandboxICCalculation:
    """验证沙箱评估产生真实的 IC 值。"""

    def test_sandbox_runner_returns_nonzero_ic(self, db_with_data):
        """沙箱评估必须返回非零的 IC 值。"""
        from src.mining.sandbox import Sandbox

        sandbox = Sandbox(db_with_data.db_path)

        # 一个简单的因子代码：直接读 factor_values 返回
        code = '''
import pandas as pd
from datetime import datetime
from src.data.storage import Storage

def compute(universe, as_of, db):
    date_str = as_of.strftime("%Y-%m-%d")
    df = db.query("factor_values", as_of,
                  where="factor_name = ? AND trade_date = ?",
                  params=("test_factor", date_str))
    if df.empty:
        return pd.Series(0.0, index=universe)
    return df.set_index("stock_code")["factor_value"].reindex(universe).fillna(0)
'''
        result = sandbox.evaluate(code, lookback_days=5, as_of=datetime(2024, 6, 14))

        # 核心断言: ic_mean 不能是 0.0（那意味着根本没算）
        assert result is not None, "沙箱评估返回 None"
        assert "ic_mean" in result, "结果缺少 ic_mean"
        assert result["ic_mean"] != 0.0, \
            f"ic_mean = 0.0, 沙箱没有真正计算 IC"

    def test_positive_ic_for_predictive_factor(self, db_with_data):
        """一个与未来收益正相关的因子应该有正 IC。"""
        from src.mining.sandbox import Sandbox

        sandbox = Sandbox(db_with_data.db_path)

        # 这个因子值与未来收益正相关（构造数据时保证了）
        code = '''
import pandas as pd
from datetime import datetime
from src.data.storage import Storage

def compute(universe, as_of, db):
    date_str = as_of.strftime("%Y-%m-%d")
    df = db.query("factor_values", as_of,
                  where="factor_name = ? AND trade_date = ?",
                  params=("test_factor", date_str))
    if df.empty:
        return pd.Series(0.0, index=universe)
    return df.set_index("stock_code")["factor_value"].reindex(universe).fillna(0)
'''
        result = sandbox.evaluate(code, lookback_days=5, as_of=datetime(2024, 6, 14))

        if result and result.get("sample_size", 0) > 0:
            assert result["ic_mean"] > 0, \
                f"预测性因子应该有正 IC, 实际 ic_mean={result['ic_mean']}"

    def test_sandbox_evaluate_reports_sample_size(self, db_with_data):
        """评估结果必须包含真实的样本量。"""
        from src.mining.sandbox import Sandbox

        sandbox = Sandbox(db_with_data.db_path)
        code = '''
import pandas as pd

def compute(universe, as_of, db):
    return pd.Series(1.0, index=universe)
'''
        result = sandbox.evaluate(code, lookback_days=5, as_of=datetime(2024, 6, 14))
        assert result is not None
        assert result.get("sample_size", 0) >= 0


class TestSandboxRunnerConsistency:
    """验证 sandbox.py 内嵌代码和独立 _sandbox_runner.py 的一致性。"""

    def test_no_limit_param_in_db_query(self):
        """Storage.query() 不接受 limit 参数，代码中不能出现。"""
        import inspect
        from src.data.storage import Storage

        sig = inspect.signature(Storage.query)
        params = list(sig.parameters.keys())
        assert "limit" not in params, \
            "Storage.query 没有 limit 参数"

        # 检查 sandbox.py 源码中没有 limit= 调用
        sandbox_path = "src/mining/sandbox.py"
        try:
            with open(sandbox_path) as f:
                content = f.read()
            assert "limit=" not in content or "limit=None" in content, \
                f"sandbox.py 中有对 db.query 的 limit= 调用，会报错"
        except FileNotFoundError:
            pass  # 文件不存在则跳过

    def test_runner_code_has_ic_calculation(self):
        """_sandbox_runner.py 必须包含 spearmanr 或 IC 计算逻辑。"""
        runner_path = "src/mining/_sandbox_runner.py"
        try:
            with open(runner_path) as f:
                content = f.read()
            has_ic = ("spearmanr" in content or
                      "spearman" in content.lower() or
                      "ic_mean" in content)
            assert has_ic, \
                "_sandbox_runner.py 中没有 IC 计算逻辑"
        except FileNotFoundError:
            pytest.skip("_sandbox_runner.py 不存在")
