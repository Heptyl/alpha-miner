# Alpha Miner — 代码质量加固

> **目标**: 不加新功能，只修现有代码的 bug 和假实现
> **方法**: 先写测试暴露问题 → 再修代码通过测试 → 循环直到全绿
> **规则**: 不得删除或降低任何测试断言来通过测试

---

## 工作模式（必须严格遵守）

```
1. 读完本文档全部内容后再开始
2. 每一轮: 运行 uv run pytest tests/ -v -m "not live" 2>&1 | tail -30
3. 如果有 FAILED: 修复代码（不是修改测试），然后重跑
4. 如果全部 PASSED: 进入下一轮新测试
5. 每修一个 bug，用一句话说明改了什么、为什么
6. 禁止:
   - 删除或注释掉失败的测试
   - 降低断言阈值（如 assert >= 8 改成 assert >= 0）
   - 用 pass / return None / return pd.Series(dtype=float) 等空实现骗过测试
   - 用 pytest.mark.skip 跳过失败测试
   - 说"测试通过"但不贴 pytest 输出
7. 每完成一轮，贴出完整的 pytest 最后 30 行输出
8. 全部 8 轮完成后，运行最终验收（本文档末尾），贴出完整输出
```

---

## 第一轮: 沙箱 IC 评估修复

### 问题

`src/mining/_sandbox_runner.py` 的评估逻辑只返回 `ic_mean: 0.0`，没有真正计算 Spearman IC。导致进化引擎永远无法通过验收（除非把阈值降到 0，测试里确实这么干了）。

### 写测试

创建 `tests/test_sandbox_ic.py`:

```python
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
        result = sandbox.evaluate(code, lookback_days=5)

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
        result = sandbox.evaluate(code, lookback_days=5)

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
        result = sandbox.evaluate(code, lookback_days=5)
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
```

### 修什么

1. `src/mining/_sandbox_runner.py`: 加入真实的多日 Spearman IC 计算（参考 `ic_tracker.py` 已有的 `_compute_spearman_ic` 逻辑）
2. `src/mining/sandbox.py`: 如果内嵌了 RUNNER_CODE，同步更新或删除内嵌版改为只用独立文件
3. 确保 `sandbox.evaluate()` 返回的 `ic_mean` 是真实计算的值

### 验收

```bash
uv run pytest tests/test_sandbox_ic.py -v
```

---

## 第二轮: 模板因子生成修复

### 问题

`EvolutionEngine._template_construct()` 生成的代码里 `compute` 直接返回空 Series，无 LLM 时进化引擎完全无效。

### 写测试

创建 `tests/test_template_factors.py`:

```python
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
```

### 修什么

`src/mining/evolution.py` 的 `_template_construct()` 方法：为知识库中的 conditional 和 formula 两种类型各写一个真实的代码模板。conditional 类型至少要查 zt_pool + daily_price 做条件判断，formula 类型至少要做一个数学计算。

### 验收

```bash
uv run pytest tests/test_template_factors.py -v
```

---

## 第三轮: 外部依赖安全

### 问题

`akshare_news.py` 硬依赖 `~/a-share-sentiment/scripts/fin_sentiment`，该路径在 WSL2 环境下不一定存在。

### 写测试

创建 `tests/test_external_deps.py`:

```python
"""测试外部依赖的容错。"""
import pytest
import importlib
import sys


class TestExternalDependencies:
    """确保所有模块在缺少外部依赖时不会崩溃。"""

    def test_news_import_without_fin_sentiment(self):
        """akshare_news 在没有 fin_sentiment 时仍可导入。"""
        # 临时移除 fin_sentiment 的路径
        original_path = sys.path.copy()
        try:
            # 强制重新导入
            if "src.data.sources.akshare_news" in sys.modules:
                del sys.modules["src.data.sources.akshare_news"]
            if "fin_sentiment" in sys.modules:
                del sys.modules["fin_sentiment"]

            # 移除可能包含 fin_sentiment 的路径
            sys.path = [p for p in sys.path
                        if "a-share-sentiment" not in p]

            # 导入不应该崩溃
            try:
                import src.data.sources.akshare_news as news_mod
                # 应该有 fallback 情感分析
                assert hasattr(news_mod, "_sentiment"), \
                    "akshare_news 缺少 _sentiment 函数"
            except ImportError as e:
                if "fin_sentiment" in str(e):
                    pytest.fail(
                        "akshare_news 在没有 fin_sentiment 时导入失败。"
                        "需要加 try/except fallback。"
                    )
                raise
        finally:
            sys.path = original_path

    def test_sentiment_fallback_produces_valid_score(self):
        """情感分析 fallback 必须返回 0-1 之间的值。"""
        from src.data.sources.akshare_news import _sentiment

        score = _sentiment("这是一条测试新闻")
        assert isinstance(score, float), f"情感分数类型错误: {type(score)}"
        assert 0.0 <= score <= 1.0, f"情感分数越界: {score}"

    def test_sentiment_handles_empty_input(self):
        """情感分析处理空输入不崩溃。"""
        from src.data.sources.akshare_news import _sentiment

        assert _sentiment("") == 0.5 or isinstance(_sentiment(""), float)
        assert _sentiment(None) == 0.5 or isinstance(_sentiment(None), float)

    def test_all_source_modules_importable(self):
        """所有数据源模块都可以无错导入。"""
        modules = [
            "src.data.sources.akshare_price",
            "src.data.sources.akshare_zt_pool",
            "src.data.sources.akshare_lhb",
            "src.data.sources.akshare_fund_flow",
            "src.data.sources.akshare_concept",
            "src.data.sources.akshare_news",
        ]
        for mod_name in modules:
            try:
                importlib.import_module(mod_name)
            except ImportError as e:
                pytest.fail(f"{mod_name} 导入失败: {e}")

    def test_narrative_modules_importable(self):
        """叙事引擎模块都可以无错导入。"""
        modules = [
            "src.narrative.news_classifier",
            "src.narrative.script_engine",
            "src.narrative.replay_engine",
        ]
        for mod_name in modules:
            try:
                importlib.import_module(mod_name)
            except ImportError as e:
                pytest.fail(f"{mod_name} 导入失败: {e}")
```

### 修什么

`src/data/sources/akshare_news.py`: 把 `from fin_sentiment import analyze_sentiment` 包在 `try/except` 里，失败时 fallback 到内置简单情感分析（关键词匹配或固定返回 0.5）。

### 验收

```bash
uv run pytest tests/test_external_deps.py -v
```

---

## 第四轮: 进化引擎验收标准真实性

### 问题

`tests/test_mining.py` 中的 `test_run_with_no_data` 把验收阈值 `MIN_IC` 降到 0 来通过测试。这掩盖了引擎的真实问题。

### 写测试

创建 `tests/test_evolution_integrity.py`:

```python
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
        candidate = Candidate("test", "knowledge", {
            "conditions": ["连板>=3"],
        })
        candidate.evaluation = {
            "ic_mean": 0.01, "icir": 0.2, "win_rate": 0.45,
            "sample_size": 50,
        }

        result = analyzer.analyze(candidate)
        assert isinstance(result, dict), f"分析结果不是 dict: {type(result)}"
        assert "diagnosis" in result, "分析结果缺少 diagnosis 字段"
        assert result["diagnosis"] in (
            "too_strict", "too_loose", "no_signal", "reversed",
            "noisy_but_directional", "redundant", "inconsistent",
            "unknown",
        ), f"未知诊断类型: {result['diagnosis']}"

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
```

### 修什么

1. 如果现有测试中有降低阈值的 hack，恢复阈值到正常值，改为用 mock 数据让因子真正有 IC
2. `FailureAnalyzer.analyze()`: 确保参数签名匹配（可能是 `analyze(candidate)` 或 `analyze(candidate, backtest_result)`，统一一下）
3. `FactorMutator.mutate()`: 确保返回的变异配置确实与原始不同

### 验收

```bash
uv run pytest tests/test_evolution_integrity.py -v
```

---

## 第五轮: 数据层健壮性

### 写测试

创建 `tests/test_data_robustness.py`:

```python
"""测试数据层的边界情况。"""
import pytest
import pandas as pd
from datetime import datetime
from src.data.storage import Storage


@pytest.fixture
def db(tmp_path):
    db = Storage(str(tmp_path / "test.db"))
    db.init_db()
    return db


class TestStorageRobustness:
    """数据层边界情况。"""

    def test_insert_duplicate_data(self, db):
        """重复插入相同数据不应报错（幂等性）。"""
        snap = datetime(2024, 6, 14, 10, 0, 0)
        df = pd.DataFrame([{
            "stock_code": "000001", "trade_date": "2024-06-14",
            "open": 10, "high": 11, "low": 9.5, "close": 11,
            "volume": 1000000, "amount": 11000000, "turnover_rate": 8.0,
        }])

        db.insert("daily_price", df, snapshot_time=snap)
        # 第二次插入不应崩溃
        try:
            db.insert("daily_price", df, snapshot_time=snap)
        except Exception as e:
            # 可以报错但不能是未处理异常
            assert "UNIQUE" in str(e) or "duplicate" in str(e).lower(), \
                f"非预期错误: {e}"

    def test_query_empty_table(self, db):
        """查询空表返回空 DataFrame，不崩溃。"""
        result = db.query("daily_price", datetime(2024, 6, 14))
        assert isinstance(result, pd.DataFrame)
        assert result.empty

    def test_query_nonexistent_date(self, db):
        """查询不存在的日期返回空，不崩溃。"""
        snap = datetime(2024, 6, 14, 10, 0, 0)
        db.insert("daily_price", pd.DataFrame([{
            "stock_code": "000001", "trade_date": "2024-06-14",
            "open": 10, "high": 11, "low": 9.5, "close": 11,
            "volume": 1000000, "amount": 11000000, "turnover_rate": 8.0,
        }]), snapshot_time=snap)

        result = db.query("daily_price", datetime(2024, 6, 14),
                          where="trade_date = ?", params=("2024-01-01",))
        assert result.empty

    def test_insert_empty_dataframe(self, db):
        """插入空 DataFrame 不崩溃，返回 0。"""
        count = db.insert("daily_price", pd.DataFrame())
        assert count == 0

    def test_all_tables_exist(self, db):
        """schema.sql 定义的所有表都存在。"""
        import sqlite3
        conn = sqlite3.connect(db.db_path)
        tables = [r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()]
        conn.close()

        expected = [
            "daily_price", "zt_pool", "zb_pool", "strong_pool",
            "lhb_detail", "fund_flow", "concept_mapping", "concept_daily",
            "news", "market_emotion", "factor_values", "ic_series",
            "drift_events", "regime_state", "mining_log",
            "market_scripts", "replay_log",
        ]
        for t in expected:
            assert t in tables, f"表 {t} 不存在"


class TestNewsTableColumns:
    """验证 news 表有分类相关的列。"""

    def test_news_has_type_column(self, db):
        """news 表必须有 news_type 列。"""
        import sqlite3
        conn = sqlite3.connect(db.db_path)
        cols = [r[1] for r in conn.execute(
            "PRAGMA table_info(news)"
        ).fetchall()]
        conn.close()

        assert "news_type" in cols, \
            "news 表缺少 news_type 列"

    def test_news_has_confidence_column(self, db):
        """news 表必须有 classify_confidence 列。"""
        import sqlite3
        conn = sqlite3.connect(db.db_path)
        cols = [r[1] for r in conn.execute(
            "PRAGMA table_info(news)"
        ).fetchall()]
        conn.close()

        assert "classify_confidence" in cols, \
            "news 表缺少 classify_confidence 列"
```

### 修什么

根据测试失败情况修复。常见问题: schema.sql 没定义某些表或列、insert 对空 DataFrame 报错、重复插入处理不当。

### 验收

```bash
uv run pytest tests/test_data_robustness.py -v
```

---

## 第六轮: 因子计算健壮性

### 写测试

创建 `tests/test_factor_edge_cases.py`:

```python
"""测试因子在边界情况下不崩溃。"""
import pytest
import pandas as pd
from datetime import datetime
from src.data.storage import Storage
from src.factors.registry import FactorRegistry


@pytest.fixture
def empty_db(tmp_path):
    db = Storage(str(tmp_path / "test.db"))
    db.init_db()
    return db


class TestFactorEdgeCases:
    """所有因子在极端输入下不崩溃。"""

    def test_all_factors_handle_empty_universe(self, empty_db):
        """空 universe 不崩溃。"""
        registry = FactorRegistry()
        factors = registry.list_factors()
        as_of = datetime(2024, 6, 14, 15, 0, 0)

        for name in factors:
            factor = registry.get_factor(name)
            result = factor.compute([], as_of, empty_db)
            assert isinstance(result, pd.Series), \
                f"因子 {name} 空 universe 返回类型错误: {type(result)}"

    def test_all_factors_handle_empty_db(self, empty_db):
        """空数据库不崩溃，返回全零。"""
        registry = FactorRegistry()
        factors = registry.list_factors()
        universe = ["000001", "000002", "000003"]
        as_of = datetime(2024, 6, 14, 15, 0, 0)

        for name in factors:
            factor = registry.get_factor(name)
            try:
                result = factor.compute(universe, as_of, empty_db)
                assert isinstance(result, pd.Series), \
                    f"因子 {name} 返回类型错误"
                assert len(result) == len(universe) or result.empty, \
                    f"因子 {name} 返回长度不匹配"
            except Exception as e:
                pytest.fail(f"因子 {name} 在空数据库上崩溃: {e}")

    def test_all_factors_handle_single_stock(self, empty_db):
        """只有一只股票时不崩溃。"""
        registry = FactorRegistry()
        factors = registry.list_factors()
        universe = ["000001"]
        as_of = datetime(2024, 6, 14, 15, 0, 0)

        snap = datetime(2024, 6, 13, 10, 0, 0)
        empty_db.insert("zt_pool", pd.DataFrame([{
            "stock_code": "000001", "trade_date": "2024-06-14",
            "consecutive_zt": 1, "amount": 50000, "circulation_mv": 200000,
            "open_count": 0, "zt_stats": "1/1",
        }]), snapshot_time=snap)

        for name in factors:
            factor = registry.get_factor(name)
            try:
                result = factor.compute(universe, as_of, empty_db)
                assert isinstance(result, pd.Series)
            except Exception as e:
                pytest.fail(f"因子 {name} 单只股票崩溃: {e}")

    def test_factor_registry_no_duplicates(self):
        """因子注册表没有重复因子。"""
        registry = FactorRegistry()
        names = registry.list_factors()
        assert len(names) == len(set(names)), \
            f"存在重复因子: {[n for n in names if names.count(n) > 1]}"

    def test_factor_count_at_least_9(self):
        """至少有 9 个因子（5 公式 + 4 叙事）。"""
        registry = FactorRegistry()
        names = registry.list_factors()
        assert len(names) >= 9, \
            f"因子数量 {len(names)} < 9"
```

### 验收

```bash
uv run pytest tests/test_factor_edge_cases.py -v
```

---

## 第七轮: CLI 冒烟测试

### 写测试

创建 `tests/test_cli_smoke.py`:

```python
"""CLI 命令冒烟测试：确保所有命令至少不崩溃。"""
import subprocess
import pytest


def run_cli(args: list[str], timeout: int = 30) -> subprocess.CompletedProcess:
    """运行 CLI 命令并捕获输出。"""
    cmd = ["uv", "run", "python", "-m", "cli"] + args
    return subprocess.run(
        cmd, capture_output=True, text=True, timeout=timeout
    )


class TestCLISmoke:
    """每个 CLI 命令至少能跑起来不报 Python 错误。"""

    def test_cli_help(self):
        r = run_cli(["--help"])
        assert r.returncode == 0, f"--help 失败: {r.stderr}"

    def test_collect_help(self):
        r = run_cli(["collect", "--help"])
        assert r.returncode == 0, f"collect --help 失败: {r.stderr}"

    def test_report_help(self):
        r = run_cli(["report", "--help"])
        assert r.returncode == 0, f"report --help 失败: {r.stderr}"

    def test_mine_help(self):
        r = run_cli(["mine", "--help"])
        assert r.returncode == 0, f"mine --help 失败: {r.stderr}"

    def test_drift_help(self):
        r = run_cli(["drift", "--help"])
        assert r.returncode == 0, f"drift --help 失败: {r.stderr}"

    def test_backtest_help(self):
        r = run_cli(["backtest", "--help"])
        assert r.returncode == 0, f"backtest --help 失败: {r.stderr}"

    def test_script_help(self):
        """script 命令存在且可用。"""
        r = run_cli(["script", "--help"])
        assert r.returncode == 0, f"script --help 失败: {r.stderr}"

    def test_replay_help(self):
        """replay 命令存在且可用。"""
        r = run_cli(["replay", "--help"])
        assert r.returncode == 0, f"replay --help 失败: {r.stderr}"

    def test_report_brief_no_crash(self):
        """report --brief 无数据时不崩溃。"""
        r = run_cli(["report", "--brief"])
        # 允许非零退出（无数据），但不能是 Python traceback
        if r.returncode != 0:
            assert "Traceback" not in r.stderr, \
                f"report --brief Python 崩溃:\n{r.stderr[-500:]}"

    def test_script_no_crash(self):
        """script 无数据时不崩溃。"""
        r = run_cli(["script"])
        if r.returncode != 0:
            assert "Traceback" not in r.stderr, \
                f"script Python 崩溃:\n{r.stderr[-500:]}"
```

### 验收

```bash
uv run pytest tests/test_cli_smoke.py -v
```

---

## 第八轮: 新闻分类器验证

### 写测试

创建 `tests/test_news_classifier_rules.py`:

```python
"""测试新闻分类器的规则准确性。"""
import pytest
from src.narrative.news_classifier import NewsClassifier, NewsType


@pytest.fixture
def classifier():
    return NewsClassifier(llm_client=None)


class TestNewsClassifierRules:
    """规则引擎分类准确性。"""

    def test_theme_ignite(self, classifier):
        """政策/突破类新闻应分类为 theme_ignite。"""
        result = classifier.classify(
            "国务院发布人工智能新政策，AI赛道迎来颠覆性变革",
            "这是一项划时代的政策..."
        )
        assert result["news_type"] == NewsType.THEME_IGNITE or \
               result["news_type"] == "theme_ignite", \
            f"政策新闻应为 theme_ignite, 实际: {result['news_type']}"

    def test_negative(self, classifier):
        """处罚/退市类新闻应分类为 negative。"""
        result = classifier.classify(
            "某公司因财务造假被证监会处罚",
            "违规行为严重，面临退市风险"
        )
        assert result["news_type"] == NewsType.NEGATIVE or \
               result["news_type"] == "negative", \
            f"处罚新闻应为 negative, 实际: {result['news_type']}"

    def test_catalyst_real(self, classifier):
        """中标/业绩类新闻应分类为 catalyst_real。"""
        result = classifier.classify(
            "公司中标5亿元大单，净利润增长200%",
            ""
        )
        assert result["news_type"] == NewsType.CATALYST_REAL or \
               result["news_type"] == "catalyst_real", \
            f"中标新闻应为 catalyst_real, 实际: {result['news_type']}"

    def test_noise(self, classifier):
        """无关新闻应分类为 noise。"""
        result = classifier.classify(
            "今天天气不错",
            "适合出去走走"
        )
        assert result["news_type"] == NewsType.NOISE or \
               result["news_type"] == "noise", \
            f"无关新闻应为 noise, 实际: {result['news_type']}"

    def test_confidence_between_0_and_1(self, classifier):
        """置信度必须在 0-1 之间。"""
        result = classifier.classify("测试新闻标题", "测试内容")
        assert 0.0 <= result["confidence"] <= 1.0, \
            f"置信度越界: {result['confidence']}"

    def test_method_is_rule(self, classifier):
        """无 LLM 时分类方法必须是 rule。"""
        result = classifier.classify("测试", "")
        assert result["method"] == "rule", \
            f"无 LLM 时方法应为 rule, 实际: {result['method']}"

    def test_empty_input(self, classifier):
        """空输入不崩溃。"""
        result = classifier.classify("", "")
        assert result is not None
        assert "news_type" in result
```

### 验收

```bash
uv run pytest tests/test_news_classifier_rules.py -v
```

---

## 最终验收

全部 8 轮完成后，运行以下命令并贴出完整输出:

```bash
# 1. 全量测试
uv run pytest tests/ -v -m "not live" 2>&1 | tail -40

# 2. 测试数量检查
uv run pytest tests/ -m "not live" --co -q 2>&1 | tail -3

# 3. 零失败确认
uv run pytest tests/ -m "not live" -x 2>&1 | tail -5
```

期望结果:
- 所有测试 PASSED，0 FAILED
- 总测试数 >= 130（原 105 + 新增 ~30）
- `-x`（首次失败停止）模式跑到底，说明全绿

**如果任何一条命令有 FAILED，回去修代码，不要修改测试，直到全绿。**
