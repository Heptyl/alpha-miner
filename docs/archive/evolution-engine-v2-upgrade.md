# 进化引擎 v2 升级 — GLM 执行文档

> **执行原则**：每个 Step 完成后必须跑测试确认通过，再进入下一步。不要跳步。
> **代码风格**：严格遵循项目现有风格（type hints、docstring、dedup_latest）。
> **关键约束**：所有数据访问必须通过 `db.query(table, as_of)` 做时间隔离，禁止未来数据。

---

## 背景：当前进化引擎的 5 个结构性问题

1. **沙箱 IC 是假的**：`_sandbox_runner.py` 返回 `ic_mean=0.0`，因子永远过不了验收
2. **IC 不分 regime**：全市场平均 IC 掩盖了因子在特定 regime 下的真实表现
3. **权重硬编码**：`daily_brief.py` 的 `REGIME_WEIGHTS` 是手写固定值，不随数据更新
4. **进化不利用历史**：每次 `evolve` 从知识库重新开始，不知道哪些假说已反复失败
5. **回测 universe 不分层**：固定取前 50 只股票，小市值因子测不出来

---

## Step 1：真实回测器（替换假沙箱 IC）

### 目标

让进化引擎能真正计算多日 IC，返回逐日 IC 序列。

### 创建 `src/mining/backtester.py`

```python
"""因子真实回测器 — 在历史数据上逐日计算因子值并算 Spearman IC。

替代 _sandbox_runner.py 中的假 IC 评估。
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Optional

import numpy as np
import pandas as pd
from scipy import stats as scipy_stats

from src.data.storage import Storage


@dataclass
class BacktestResult:
    """回测结果。"""
    factor_name: str
    ic_mean: float = 0.0
    icir: float = 0.0
    win_rate: float = 0.0
    pnl_ratio: float = 0.0
    sample_per_day: float = 0.0
    total_days: int = 0
    ic_series: list = field(default_factory=list)  # [{date, ic, regime, zt_count}]
    error: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "ic_mean": self.ic_mean,
            "icir": self.icir,
            "win_rate": self.win_rate,
            "pnl_ratio": self.pnl_ratio,
            "sample_per_day": self.sample_per_day,
            "total_days": self.total_days,
            "ic_series": self.ic_series,
            "error": self.error,
        }


class FactorBacktester:
    """在历史数据上逐日回测因子 IC。

    与 ICTracker 的区别：
    - ICTracker 从 factor_values 表读已算好的值
    - FactorBacktester 接收一个 compute 函数，现场算因子值再算 IC
    - FactorBacktester 同时记录每天的 regime 和涨停数，用于后续分段分析
    """

    def __init__(self, db: Storage):
        self.db = db

    def run(
        self,
        compute_fn,  # callable: compute(universe, as_of, db) -> pd.Series
        factor_name: str = "unknown",
        lookback_days: int = 60,
        forward_days: int = 1,
    ) -> BacktestResult:
        """在最近 lookback_days 个交易日上逐日回测。

        流程：
        1. 获取交易日历（从 daily_price 表）
        2. 对每个交易日 T：
           a. 构建 universe（当日有行情且成交额 top 500 的活跃股）
           b. 调用 compute_fn 算因子值
           c. 取 T+forward_days 的收益率
           d. 算 Spearman IC
           e. 记录当天的 regime 和涨停数
        3. 汇总统计
        """
        result = BacktestResult(factor_name=factor_name)

        try:
            trade_dates = self._get_trade_dates(lookback_days + forward_days * 3)
        except Exception as e:
            result.error = f"获取交易日历失败: {e}"
            return result

        if len(trade_dates) < 10:
            result.error = f"交易日数据不足: {len(trade_dates)} 天"
            return result

        # 只在有足够前瞻数据的日期上回测
        test_dates = trade_dates[:len(trade_dates) - forward_days]
        if len(test_dates) > lookback_days:
            test_dates = test_dates[-lookback_days:]

        ic_records = []
        sample_sizes = []

        for i, date_str in enumerate(test_dates):
            as_of = datetime.strptime(date_str, "%Y-%m-%d").replace(hour=15)

            # 构建 universe（按成交额取 top 500 活跃股）
            universe = self._get_universe(as_of, date_str)
            if len(universe) < 20:
                continue

            # 计算因子值
            try:
                factor_values = compute_fn(universe, as_of, self.db)
            except Exception:
                continue

            if factor_values is None or factor_values.empty:
                continue
            factor_values = factor_values.dropna()
            if len(factor_values) < 10:
                continue

            # 计算未来收益
            future_idx = trade_dates.index(date_str) + forward_days
            if future_idx >= len(trade_dates):
                continue
            future_date = trade_dates[future_idx]
            forward_returns = self._get_forward_returns(date_str, future_date)
            if forward_returns.empty:
                continue

            # Spearman IC
            common = factor_values.index.intersection(forward_returns.index)
            if len(common) < 10:
                continue
            fv = factor_values.loc[common].astype(float)
            fr = forward_returns.loc[common].astype(float)
            mask = fv.notna() & fr.notna()
            fv, fr = fv[mask], fr[mask]
            if len(fv) < 10:
                continue

            ic, _ = scipy_stats.spearmanr(fv, fr)
            if np.isnan(ic):
                continue

            regime, zt_count = self._get_day_context(as_of, date_str)
            ic_records.append({
                "date": date_str,
                "ic": round(float(ic), 6),
                "regime": regime,
                "zt_count": zt_count,
                "sample_size": len(fv),
            })
            sample_sizes.append(len(fv))

        if not ic_records:
            result.error = "无有效 IC 样本"
            return result

        ic_values = np.array([r["ic"] for r in ic_records])
        result.ic_mean = float(np.mean(ic_values))
        result.icir = float(np.mean(ic_values) / np.std(ic_values)) if np.std(ic_values) > 0 else 0.0
        result.win_rate = float((ic_values > 0).sum() / len(ic_values))
        pos_mean = float(ic_values[ic_values > 0].mean()) if (ic_values > 0).any() else 0.0
        neg_mean = float(np.abs(ic_values[ic_values < 0].mean())) if (ic_values < 0).any() else 1.0
        result.pnl_ratio = pos_mean / neg_mean if neg_mean > 0 else 0.0
        result.sample_per_day = float(np.mean(sample_sizes))
        result.total_days = len(ic_records)
        result.ic_series = ic_records
        return result

    def _get_trade_dates(self, days: int) -> list[str]:
        """从 daily_price 获取最近的交易日列表。"""
        end = datetime.now()
        df = self.db.query_range("daily_price", end, lookback_days=days * 2)
        if df.empty:
            return []
        dates = sorted(df["trade_date"].unique())
        return dates[-days:] if len(dates) > days else dates

    def _get_universe(self, as_of: datetime, date_str: str) -> list[str]:
        """获取当日 universe — 按成交额取 top 500 活跃股。"""
        df = self.db.query("daily_price", as_of,
                           where="trade_date = ?", params=(date_str,))
        if df.empty:
            return []
        if "amount" in df.columns:
            df = df.sort_values("amount", ascending=False)
        return df["stock_code"].head(500).tolist()

    def _get_forward_returns(self, current_date: str, future_date: str) -> pd.Series:
        """计算 current_date → future_date 的收益率。"""
        cur_as_of = datetime.strptime(current_date, "%Y-%m-%d").replace(hour=15)
        fut_as_of = datetime.strptime(future_date, "%Y-%m-%d").replace(hour=15)
        cur_df = self.db.query("daily_price", cur_as_of,
                               where="trade_date = ?", params=(current_date,))
        fut_df = self.db.query("daily_price", fut_as_of,
                               where="trade_date = ?", params=(future_date,))
        if cur_df.empty or fut_df.empty:
            return pd.Series(dtype=float)
        cur_p = cur_df.drop_duplicates("stock_code").set_index("stock_code")["close"]
        fut_p = fut_df.drop_duplicates("stock_code").set_index("stock_code")["close"]
        common = cur_p.index.intersection(fut_p.index)
        if len(common) == 0:
            return pd.Series(dtype=float)
        return (fut_p.loc[common] - cur_p.loc[common]) / cur_p.loc[common]

    def _get_day_context(self, as_of: datetime, date_str: str) -> tuple[str, int]:
        """获取当天的 regime 和涨停数。"""
        from src.drift.regime import RegimeDetector
        try:
            regime = RegimeDetector(self.db).detect(as_of).regime
        except Exception:
            regime = "unknown"
        try:
            zt_df = self.db.query("zt_pool", as_of,
                                  where="trade_date = ?", params=(date_str,))
            zt_count = len(zt_df) if not zt_df.empty else 0
        except Exception:
            zt_count = 0
        return regime, zt_count
```

### 测试文件 `tests/test_backtester.py`

写测试验证：
- `test_run_with_simple_factor`：有数据时返回有效结果（ic_series 非空）
- `test_run_with_empty_db`：空数据库返回 error 而非崩溃
- `test_ic_series_has_regime`：每条 IC 记录包含 date/ic/regime/zt_count
- `test_backtest_result_to_dict`：序列化格式正确

### 改造 `src/mining/evolution.py`

**只改 `_evaluate` 方法**，替换为使用 `FactorBacktester` 做真实多日 IC 计算。
同时新增 `_extract_compute_fn(self, code: str)` 辅助方法，从代码字符串中提取 compute 函数。

沙箱仍然先跑一次验证代码能执行，然后 backtester 做真实 IC。

运行 `pytest tests/ -v -m "not live"` 确认全部通过。

---

## Step 2：因子手术台（Regime-IC 分段分析）

### 目标

把逐日 IC 序列按 regime、涨停数、时间段拆开，回答"因子在什么条件下有效"。

### 创建 `src/mining/surgery_table.py`

核心类 `FactorSurgeryTable`，方法 `analyze(ic_series, factor_name) -> SurgeryReport`。

数据结构：
- `RegimeIC`：单个 regime 下的 IC 统计（ic_mean, icir, sample_days, effective）
- `EmotionIC`：按涨停数分组（strong>60 / normal 20-60 / weak<20）
- `TimeSegmentIC`：前半段 vs 后半段，检测衰减
- `SurgeryReport`：完整报告，含诊断结论和建议

分段逻辑：
1. **按 regime 拆**：groupby regime 算各组 IC 均值和 ICIR
2. **按涨停数拆**：分 3 档（>60 / 20-60 / <20）
3. **按时间拆**：前后各半对比
4. **黄金窗口检测**：IC 连续 ≥3 天超过阈值 1.5 倍，标记窗口起止日期和对应 regime

诊断逻辑（优先级从高到低）：
1. `universally_effective`：整体 IC > 0.03 且 ICIR > 0.5
2. `regime_dependent`：某个 regime 下有效 → 记录 best_regime
3. `emotion_dependent`：某个涨停数区间有效 → 记录 best_emotion
4. `time_decayed`：前期 IC > 0.03，近期 IC < 0.015
5. `no_signal`：所有维度都无效

### 测试文件 `tests/test_surgery_table.py`

写测试验证 5 种诊断结论 + 黄金窗口检测 + 空数据安全。

运行测试确认通过。

---

## Step 3：改造失败分析器 + 变异器

### 目标

诊断和变异从"盲猜"变成"基于手术台数据的定向操作"。

### 改造 `src/mining/failure_analyzer.py`

`analyze()` 新增可选参数 `surgery_report`。有手术台报告时，直接用手术台的诊断（`regime_dependent` / `emotion_dependent` / `time_decayed`），跳过旧的整体 IC 判断逻辑。无手术台报告时，退回旧逻辑不变。

### 改造 `src/mining/mutator.py`

`mutate()` 新增对手术台诊断类型的处理：

- `regime_dependent` → `_add_regime_filter(config, target_regime)`，regime 值来自手术台的 `best_regime`
- `emotion_dependent` → 新方法 `_add_zt_count_filter(config, min_zt, max_zt)`，根据手术台的 `best_emotion` 推导阈值
- `time_decayed` → 缩短 lookback 到 0.3x + 反转方向各出一个变异

### 改造 `src/mining/evolution.py` 的 `_mutate_accepted`

在变异前先跑手术台分析，用手术台结果喂给 failure_analyzer 和 mutator。

运行测试确认通过。

---

## Step 4：动态 Regime 权重

### 目标

`daily_brief.py` 的因子权重不再硬编码，而是从历史 IC 表现动态算。

### 改造 `src/drift/daily_brief.py`

在 `DailyBrief` 类中新增 `_compute_dynamic_regime_weights(as_of, regime) -> dict[str, float]`：

逻辑：
1. 对每个注册因子，取 `ic_tracker.current_status()` 的 IC 均值
2. `base_weight = abs(ic)`
3. 如果 IC < -0.01（当前 regime 下为负），权重设 0（禁用）
4. 乘以 `REGIME_WEIGHTS` 中的硬编码值作为先验调整（数据不足时 fallback）

在 `build_candidates()` 中调用动态权重替换 `REGIME_WEIGHTS.get()`。

**保留 `REGIME_WEIGHTS` 字典作为 fallback**，不要删除。

---

## Step 5：候选因子缓冲池

### 目标

验收通过不直接上线，先在缓冲池观察 5 天。

### 新建 `src/mining/candidate_pool.py`

`CandidatePool` 类，JSONL 文件存储：
- `add_candidate(candidate)`：进池，状态 pending
- `get_pending() -> list[dict]`：获取待验证列表
- `update_candidate(name, daily_ic, passed) -> status`：更新每日验证结果
  - 连续 5 天达标 → promoted
  - 连续 3 天不达标 → rejected

### 改造进化引擎

验收通过的因子调用 `pool.add_candidate()` 而非直接计入 `self.accepted`。

---

## Step 6：进化引擎历史反馈

### 目标

避开反复失败的死胡同，偏向成功方向。

### 改造 `evolution.py` 的 `_generate_from_knowledge()`

读取 `mining_log.jsonl`，统计每个假说名的失败次数。失败 ≥3 次的假说跳过，打日志说明。

新增辅助方法 `_get_historical_failures() -> dict[str, int]`。

---

## Step 7：CLI 集成

### 新增 `mine surgery` 子命令

```bash
python -m cli mine surgery --factor consecutive_board --days 60
```

输出手术台报告：整体 IC → regime 拆分 → 涨停数拆分 → 时间段对比 → 黄金窗口 → 诊断 + 建议。

在 `cli/mine.py` 的 subparsers 中新增 `surgery` 子命令，实现 `cmd_surgery(args)` 函数。

---

## 验收清单

```bash
# 1. 全部测试通过（预期 > 110 个）
uv run pytest tests/ -v -m "not live"

# 2. 新模块测试
uv run pytest tests/test_backtester.py tests/test_surgery_table.py -v

# 3. 手术台 CLI
uv run python -m cli mine surgery --factor consecutive_board --days 30
# 应输出 regime 拆分表 + 诊断结论，不应崩溃

# 4. 进化引擎真实 IC
uv run python -m cli mine test-seeds
# IC 列不应全是 N/A 或 0.0

# 5. 候选池文件
ls data/candidate_pool.jsonl
```

---

## 文件变更清单

| 操作 | 文件 | Step |
|------|------|------|
| 新建 | `src/mining/backtester.py` | 1 |
| 新建 | `src/mining/surgery_table.py` | 2 |
| 新建 | `src/mining/candidate_pool.py` | 5 |
| 新建 | `tests/test_backtester.py` | 1 |
| 新建 | `tests/test_surgery_table.py` | 2 |
| 修改 | `src/mining/evolution.py` | 1, 3, 5, 6 |
| 修改 | `src/mining/failure_analyzer.py` | 3 |
| 修改 | `src/mining/mutator.py` | 3 |
| 修改 | `src/drift/daily_brief.py` | 4 |
| 修改 | `cli/mine.py` | 7 |
