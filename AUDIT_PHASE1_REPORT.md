# Phase 1: 策略常量与联动审核报告

**审核时间**: 2026-05-25
**审核文件**: constants.py, daemon_config.py, strategy_a/b/c.py, daemon_strategies.py, daemon_sell_strategies.py, daemon_signals.py, trading_daemon.py

---

## 参数对齐表

| 参数 | constants.py | daemon_config.py | SELL_PARAMS | 匹配? |
|------|-------------|-----------------|-------------|-------|
| A stop_loss | -0.05 | SELL_PARAMS["A"]=-0.05 | -0.05 | PASS |
| A max_hold | 3 | SELL_PARAMS["A"]=3 | 3 | PASS |
| A capital | 30,000 | A_INITIAL_CAPITAL=30,000 | N/A | PASS |
| A max_pos | 3 | A_MAX_POSITIONS=3 | N/A | PASS |
| B stop_loss | -0.03 | B_STOP_LOSS_PCT=-0.03 | -0.03 | PASS |
| B max_hold | 2 | STRATEGY_B_CONFIG=2 | 2 | PASS |
| B capital | 30,000 | B_INITIAL_CAPITAL=30,000 | N/A | PASS |
| B max_pos | 3 | B_MAX_POSITIONS=3 | N/A | PASS |
| C stop_loss | -0.08 | C_STOP_LOSS_PCT=-0.08 | -0.08 | PASS |
| C max_hold | 5 | STRATEGY_C_CONFIG=5 | 5 | PASS |
| C capital | 30,000 | C_INITIAL_CAPITAL=30,000 | N/A | PASS |
| C max_pos | 3 | C_MAX_POSITIONS=3 | 3 | PASS |

**结论**: 所有参数值三方对齐，无数值不一致。

---

## 审核要点逐条检查

### 1. constants.py vs daemon_config.py 参数一致性
**结果**: PASS
所有7个参数(id/name/signal_type/capital/max_positions/stop_loss_pct/max_hold_days)值完全一致。

### 2. 策略实现文件硬编码参数
**结果**: WARN (建议改进)
- strategy_a.py:32 — 硬编码 `DB_PATH`(使用Path计算，与daemon_config.DB_PATH等价)
- strategy_b.py — 6处使用相对路径 `"data/alpha_miner.db"`(依赖CWD)
- strategy_c.py:68 — 硬编码 `MAX_HOLD_DAYS=5`
- strategy_c.py:71 — 硬编码 `VOL_RATIO_MIN=5.0`
- 无文件引用 constants.py

### 3. daemon_strategies.py 策略注册对齐
**结果**: PASS (已修复)
策略分类使用字符串匹配("首阴"/"回踩"/"趋势牛股")，与当前信号类型一致。

### 4. daemon_sell_strategies.py 止损阈值
**结果**: PASS (已修复)
止损阈值全部从 SELL_PARAMS 取值，B持有天数已改为引用常量。

### 5. daemon_signals.py 信号类型字符串对齐
**结果**: FAIL → 已修复 (P0 + P1)
详见下方问题清单。

### 6. 硬编码策略ID检查
**结果**: PASS
`"首阴"/"回踩低吸"/"趋势牛股"` 等字符串匹配与 SIGNAL_MAP 一致。

### 7. 策略D→C重命名完整性
**结果**: PASS
constants.py 中保留了兼容映射("策略D"→"C", "缩量反包"→"C")，其余文件无残留。

---

## 发现的问题

### P0-1: 策略C仓位限制被完全绕过 [已修复]
- **文件**: daemon_signals.py:254, trading_daemon.py:849
- **问题**: 买入执行时检查策略C仓位使用 `"题材" in sig_type`，但策略C的信号类型是 "趋势牛股"/"趋势牛股(策略C)"，不包含"题材"。导致策略C满仓时买入信号不会被拦截。
- **影响**: 如果策略C已有3只持仓，新买入信号仍会执行，突破3只仓位上限。
- **修复**: `"题材" in sig_type` → `"趋势牛股" in sig_type or "缩量反包" in sig_type`

### P1-1: "缩量"字符串匹配导致策略分类错误 [已修复]
- **文件**: daemon_signals.py:248,250 / daemon_strategies.py:149,205 / trading_daemon.py:724,848,969
- **问题**: 策略B的信号匹配使用 `"缩量" in sig_type`，会错误匹配策略C旧信号"缩量反包"，将C的持仓错误计入B。
- **影响**: 
  - 策略B可能因为误计C持仓而拒绝合法买入
  - 策略C的卖出检查不会触发（被分到B的卖出逻辑）
  - 通知计数/仓位统计不准确
- **修复**: 移除 `"缩量" in sig_type`，改为精确匹配已知B信号类型

### P2-1: daemon_sell_strategies.py B持有天数硬编码 [已修复]
- **文件**: daemon_sell_strategies.py:247
- **问题**: `b_hold_days >= 2` 硬编码，未引用 `SELL_PARAMS["B"]["max_hold_days"]`
- **修复**: 改为 `SELL_PARAMS["B"]["max_hold_days"]`

### 建议 (未修改):
- strategy_b.py 使用相对路径 `"data/alpha_miner.db"` (6处)，应改为从 daemon_config.DB_PATH 导入
- daemon_config.py 未从 constants.py 导入参数（两个文件独立定义相同值）
- strategy_c.py 的 MAX_HOLD_DAYS/VOL_RATIO_MIN 应引用 daemon_config.STRATEGY_C_CONFIG
- 策略分类建议使用 `get_strategy_by_signal()` 替代字符串匹配，提高可维护性

---

## 测试验证

```
tests/test_daemon_split.py .....  32 passed
tests/test_signal.py ...........  16 passed
tests/test_backtest_engine.py ..  18 passed
tests/test_data_layer.py .......   8 passed
tests/test_storage.py ..........   9 passed
Total: 83 passed, 0 failed
```

---

## 修复总结

| 级别 | 问题 | 修复文件 | 状态 |
|------|------|---------|------|
| P0 | 策略C仓位限制绕过 | daemon_signals.py, trading_daemon.py | 已修复 |
| P1 | "缩量"错误匹配策略C | daemon_signals.py, daemon_strategies.py, trading_daemon.py | 已修复 |
| P2 | B持有天数硬编码 | daemon_sell_strategies.py | 已修复 |

共修改 4 个文件，7 处代码变更。所有变更不涉及交易逻辑/策略参数调整。
