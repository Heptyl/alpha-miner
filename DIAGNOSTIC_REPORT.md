# Alpha Miner 代码诊断报告
> 生成时间: 2026-04-30
> 最后更新: 2026-04-30
> 诊断范围: 数据层 + 因子层 + 报告层 + 流水线

---

## 修复进度总览

| 编号 | 问题 | 优先级 | 状态 |
|------|------|--------|------|
| D1 | INSERT 无去重 | P0 | 待修 |
| D2 | fund_flow 25页上限 | P1 | 待修 |
| D3 | market_emotion 每次追加不去重 | P0 | 待修 |
| D4 | concept_daily 只从 4/22 开始 | P2 | 待修 |
| D5 | lhb_detail net_amount 全为0 | P0 | **已修复** ✓ |
| D6 | daily_price today 模式只拉重点股 | P2 | 待修 |
| F1 | lhb_institution 因子全为0 | P0 | **已修复** ✓ |
| F2 | zt_dt_ratio IC 无法计算 | P1 | 待修 |
| F3 | ic_series 无去重 | P0 | 待修 |
| F4 | leader_clarity/theme_lifecycle IC 相同 | P2 | 待修 |
| F5 | narrative_velocity IC 为负 | P3 | 观察 |
| R1 | 日报"无市场情绪数据" — snapshot_time+date_str | P0 | **已修复** ✓ |
| R2 | 日报"无行情数据" | P0 | **已修复** ✓ |
| R3 | regime_state 只有2条 | P1 | 待修 |
| R4 | 市场剧本简陋 | P1 | 待修 |
| R5 | cron 缺质量校验 | P2 | 待修 |

---

## 一、数据层 (6个问题)

### BUG-D1: 数据重复插入 — INSERT 无去重/UPSERT
- **文件**: `src/data/storage.py`
- **函数**: `Storage.insert()` (行 148-192)
- **根因**: `insert()` 默认 `dedup=False`，每次调用都是 `df.to_sql(..., if_exists="append")`。
  即使 `dedup=True` 也只按 `trade_date` DELETE 再 INSERT，不按 `stock_code` 去重。
  结果：**每次 cron 运行都往 factor_values、ic_series、market_emotion 追加全量新行**。
- **表现**:
  - factor_values 2026-04-21 每股3份（跑3次）
  - ic_series 同因子同天 3~8 条
  - market_emotion 同天多条
- **建议修复**:
  1. `factor_values` 写入时 dedup=True（collector.py 调用 backtest.py 的 `compute_today()`）
  2. `ic_series` 的 `_persist_ic()` 改为先 DELETE 同因子同天同 forward_days 再 INSERT
  3. `market_emotion` 聚合前先 DELETE 同 trade_date 旧数据
  4. `concept_daily` 同理
- **修复优先级**: P0（影响所有下游计算）

### BUG-D2: fund_flow 只拿 1250 只 — 硬编码上限
- **文件**: `src/data/sources/akshare_fund_flow.py`
- **函数**: `_fetch_ths_rank()` (行 114)
- **根因**: `max_pages = min(total_pages, 25)` 硬编码最多 25 页 × 50 = 1250 只。
  全市场 ~5200 只需要 104 页。
- **表现**: fund_flow 始终只有 ~1250 只，覆盖不到中小票
- **建议修复**: 改为 `max_pages = total_pages` 或 `min(total_pages, 120)`，接受 ~2min 采集时间
- **修复优先级**: P1

### BUG-D3: market_emotion 每次聚合都追加不去重
- **文件**: `src/data/collector.py`
- **函数**: `_aggregate_market_emotion()` (行 231)
- **根因**: `db.insert("market_emotion", emotion_df)` 没有 dedup=True，每次采集都追加一行。
  PRIMARY KEY 是 `(trade_date, snapshot_time)`，snapshot_time 每次不同所以不冲突。
- **表现**: market_emotion 表 30 条 / 22 天，有重复
- **建议修复**: insert 前先 `DELETE FROM market_emotion WHERE trade_date = ?`，或用 `db.insert(..., dedup=True)`
- **修复优先级**: P0

### BUG-D4: concept_daily 只从 4/22 开始
- **文件**: `src/data/collector.py`
- **函数**: `_aggregate_concept_daily()` (行 271-314)
- **根因**: 不是代码 bug。`_aggregate_concept_daily` 依赖 `concept_mapping` 表。
  concept_mapping 是从 akshare_concept.fetch() 获取的，可能 4/22 之前没跑过概念映射采集，
  或者 concept_mapping 表为空导致聚合直接 return。
- **建议修复**: 确认 backfill 流程是否包含 concept_mapping 回填；如果是，检查历史数据
- **修复优先级**: P2

### ~~BUG-D5: lhb_detail net_amount 全为0~~ ✓ 已修复
- **文件**: `src/data/sources/akshare_lhb.py`
- **根因**: `fetch()` 返回的 DataFrame 带一个 `_row_idx` 列（第61行），但 DB schema 中 `lhb_detail` 表没有该列，
  导致 `db.insert()` 抛异常。之前某版本（无 `_row_idx`）的 fetch 写入了 net_amount=0 的脏数据。
- **修复内容** (2026-04-30):
  1. 移除 `_row_idx` 列，改用 `(stock_code, reason)` 去重
  2. `save()` 写入前先 DELETE 当天旧数据
  3. 重采 04-22/04-23/04-13/03-11/03-12 的数据，net_amount 已恢复正常
  4. 同时修复了 lhb_detail 的重复写入问题（04-22 从 385→77 条）

### BUG-D6: daily_price "today" 模式只拉重点股票
- **文件**: `src/data/sources/akshare_price.py`
- **函数**: `fetch_today()`
- **根因**: today 模式从 zt_pool + strong_pool + lhb_detail 获取重点代码列表，
  只拉这些票的实时行情。如果这些采集器部分失败，daily_price 也会缺失。
  backfill 模式才是全量。
- **表现**: 早期某些交易日 daily_price 不完整
- **建议修复**: 确认 today 模式后是否需要补一次全量快照（如尾盘 15:10 后）
- **修复优先级**: P2

---

## 二、因子层 (5个问题)

### ~~BUG-F1: lhb_institution 因子全为0~~ ✓ 已修复
- **文件**: `src/factors/formula/lhb_institution.py`
- **根因**: 两层问题叠加：
  1. `akshare_lhb.py` 写入 DB 失败（`_row_idx` 列不存在），导致 lhb_detail 里 net_amount 全为0
  2. 因子计算中 `groupby("stock_code")["net_amount"].sum()` 会对同一股票多条记录重复求和（3倍膨胀）
- **修复内容** (2026-04-30):
  1. `akshare_lhb.py`: 移除 `_row_idx`，save() 写入前先删旧数据
  2. `lhb_institution.py`: `groupby.sum()` 改为 `drop_duplicates(subset=["stock_code"])` 取第一条

### BUG-F2: zt_dt_ratio 只存 "market" — 设计正确，但 IC 计算不对
- **文件**: `src/factors/formula/zt_ratio.py`
- **函数**: `ZtDtRatioFactor.compute()` (行 56)
- **根因**: 这是 **市场级因子** (`factor_type = "market"`)，返回 `pd.Series([ratio], index=["market"])`。
  所以 factor_values 里 stock_code="market" 是设计如此。
  **问题**: ICTracker 计算截面 IC 时，用 factor_values 和 daily_price 的 forward_return 做 Spearman 相关。
  但 "market" 这个 stock_code 不在 daily_price 里，所以 **IC 计算时 zt_dt_ratio 的有效样本 = 0**。
  所有个股的 zt_dt_ratio 值都是 0（只有 "market" 有值），导致 IC 无法计算。
- **建议修复**:
  1. 方案A: 市场级因子不参与截面 IC 计算（ICTracker 跳过 factor_type=market 的因子）
  2. 方案B: zt_dt_ratio 改为个股级因子（每只股票都赋予相同的市场 zt/dt 比值）
- **修复优先级**: P1

### BUG-F3: IC 重复写入 + ic_series 无去重
- **文件**: `src/drift/ic_tracker.py`
- **函数**: `ICTracker._persist_ic()` (行 153-170)
- **根因**: `_persist_ic()` 直接 `db.insert("ic_series", df)` 无去重。
  每次 `run_ic_pipeline()` 或 `DriftReport.generate()` 调用都会追加。
  ic_series 的 PK 是 `(factor_name, trade_date, forward_days, snapshot_time)`，
  snapshot_time 每次不同所以每次都写入新行。
- **表现**: ic_series 88 条但很多重复
- **建议修复**: `_persist_ic()` 写入前先 DELETE 同因子同天同 forward_days
  ```python
  conn.execute(
      "DELETE FROM ic_series WHERE factor_name = ? AND forward_days = ?",
      (factor_name, forward_days)
  )
  ```
- **修复优先级**: P0

### BUG-F4: leader_clarity 和 theme_lifecycle IC 完全相同
- **文件**: `src/factors/narrative/leader_clarity.py` + `src/factors/narrative/theme_lifecycle.py`
- **根因**: 两个因子的 **输入数据不同**（leader_clarity 用 amount 排名，theme_lifecycle 用 zt_count 评分），
  但如果 concept_mapping 数据缺失或 zt_pool 的 amount 列为空，两个因子都会走 `return pd.Series(0.0, ...)` 路径，
  产生全 0 值 → IC 相同（都是 NaN 或 0）。
  **这不是代码逻辑重复，而是数据不充分导致退化为同一结果**。
- **建议修复**: 确保 concept_mapping 有数据后重新计算因子值
- **修复优先级**: P2（依赖 D4 修复）

### BUG-F5: narrative_velocity IC 为负 — 需确认因子方向
- **文件**: `src/factors/narrative/factor.py`（narrative_velocity.py）
- **函数**: `NarrativeVelocityFactor.compute()` (行 65-77)
- **根因**: IC 为负说明因子值越高 → 未来收益越低。可能原因：
  1. 新闻多的股票已经被充分定价（注意力效应）
  2. 好消息/坏消息的权重设置有问题：`good_realize = -0.5` 是负权重，
     但 "利好兑现" 确实该给负分，这没毛病
  3. 数据量太少（<20 交易日），IC 不稳定
- **建议修复**: 数据量积累到 40+ 交易日后重新评估；当前不修改因子逻辑
- **修复优先级**: P3（观察）

---

## 三、报告层 + 流水线 (5个问题)

### ~~BUG-R1: 日报显示 "无市场情绪数据" — date_str 用 as_of~~ ✓ 已修复
- **文件**: `src/drift/daily_report.py`
- **根因**: `generate()` 接收 `report_date` 参数，生成正确的 `date_str`，
  但 6 个 `_section_*()` 方法都只用 `as_of` 自己 `strftime("%Y-%m-%d")` 生成日期。
  当 cron 在晚间运行时 `as_of=datetime.now()` 可能是 23:xx，
  但数据日期应该是 `report_date` 参数指定的交易日。
- **修复内容** (2026-04-30):
  1. 6 个 `_section_*()` 方法统一签名 `(self, as_of, date_str)` 接收 `date_str` 参数
  2. 删除各方法内部的 `date_str = as_of.strftime(...)` 自行生成
  3. `generate()` 调用时统一传入 `date_str`（来自 `report_date` 或 `as_of.strftime()`）

### ~~BUG-R2: 日报显示 "无行情数据"~~ ✓ 已修复
- **根因**: 与 R1 相同，已一并修复

### BUG-R3: regime_state 只有 4/22 的 2 条 — 流水线不跑 regime 持久化
- **文件**: `src/pipeline/runner.py` + `scripts/daily_run.sh`
- **根因**: `daily_run.sh` 第3步调 `python -m cli.drift --date $DATE`，
  而 `cli/drift.py` 只调 `DriftReport.generate()` 输出文本，**不调 `run_regime_pipeline()`**。
  `run_regime_pipeline()` 在 `src/pipeline/runner.py` 里，只有 `run_full_pipeline()` 才会调。
  但 `daily_run.sh` 没有调用 `run_full_pipeline()`。
  
  Regime 检测本身（`RegimeDetector.detect()`）在日报生成时被调用了（`_section_market` 行 90），
  但检测结果没有持久化到 `regime_state` 表。
- **建议修复**:
  1. `daily_run.sh` 增加 `uv run python -c "from src.pipeline.runner import run_regime_pipeline; run_regime_pipeline()"`
  2. 或在 `DriftReport.generate()` 中加入 regime 持久化逻辑
- **修复优先级**: P1

### BUG-R4: 市场剧本极其简陋 — context 不足
- **文件**: `src/narrative/script_engine.py`（未直接查看，但从行为推断）
- **根因**: 剧本生成时的 prompt 模板可能没有传入足够的上下文数据（涨停梯队、连板详情、炸板率等），
  或者 prompt 中依赖的数据查询受 snapshot_time 过滤影响（同 R1 bug），查不到数据。
- **建议修复**: 确认 script_engine.py 的数据查询是否也有 snapshot_time 过滤问题
- **修复优先级**: P1（依赖 R1 修复）

### BUG-R5: cron 缺少质量校验
- **文件**: `~/.hermes/scripts/alpha_miner_cron.py`
- **根因**: 脚本只判断交易日，采集后不校验数据量。如果采集部分失败（如 zt_pool=0），
  后续因子计算和报告都基于不完整数据。
- **建议修复**: 在 cron prompt 中增加数据校验步骤：
  ```
  采集完成后检查：
  - daily_price 数量 > 3000
  - zt_pool 数量 > 0 (交易日)
  - factor_values 写入行数 > 1000
  如果异常则告警
  ```
- **修复优先级**: P2

---

## 四、核心根因总结

| 根因 | 影响范围 | 修复难度 | 状态 |
|------|---------|---------|------|
| **1. INSERT 无去重** | factor_values/ic_series/market_emotion 全部重复 | 中 | 待修 |
| **2. date_str 传参错误** | 日报查不到数据 | 低 | **已修复** ✓ |
| **3. lhb_detail _row_idx 导致写入失败** | net_amount 全0 → lhb_institution 因子失效 | 低 | **已修复** ✓ |
| **4. 流水线断裂** | regime 不持久化、IC 重复计算 | 低 | 待修 |
| **5. 数据源限制** | fund_flow 1250 上限 | 中 | 待修 |
| **6. 数据量不足** | IC 不稳定、因子退化为 0 | 时间解决 | 观察 |

---

## 五、修复执行顺序

1. ~~**[P0] 修复 date_str 传参** → 解决日报"无数据"问题~~ ✓
2. ~~**[P0] lhb_institution 诊断+修复** → 解决 net_amount 全0~~ ✓
3. **[P0] INSERT 去重** → 解决 factor_values/ic_series/market_emotion 重复
4. **[P0] 清理 DB 重复数据** → 依赖 #3
5. **[P1] daily_run.sh 增加 regime 持久化** → 解决 regime_state 为空
6. **[P1] fund_flow 去掉 25 页上限** → 解决数据覆盖不全
7. **[P1] zt_dt_ratio IC 计算兼容** → 市场级因子不参与截面 IC
8. **[P2] 增加数据校验** → cron 质量保障
9. **[P2] 数据回填** → 补齐缺失的历史数据
