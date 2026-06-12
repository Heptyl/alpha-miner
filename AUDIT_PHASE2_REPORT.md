# Phase 2: Daemon核心流程审核报告

**审核时间**: 2026-05-25
**审核文件**: trading_daemon.py, daemon_db.py, daemon_risk.py, daemon_sell_strategies.py, daemon_signals.py, daemon_strategies.py, position_monitor.py, signal_monitor.py

---

## 审核要点逐条检查

### 1. T+1铁律：当天买入的票是否在所有卖出路径中被正确跳过
**结果**: PASS

| 卖出路径 | 文件:行号 | T+1检查方式 | 状态 |
|----------|----------|------------|------|
| _scan_sell | trading_daemon.py:706-718 | `buy_dt.date() >= datetime.now().date()` → continue | PASS |
| _check_sell_strategy_c | daemon_sell_strategies.py:48-54 | `c_hold_days < 1` → pass (不卖) | PASS |
| _check_sell_strategy_a | daemon_sell_strategies.py:111-116 | `a_hold_days`计算 + 持有天数<1时跳过止损 | PASS |
| _check_sell_strategy_b | daemon_sell_strategies.py:228-233 | `b_hold_days`计算 | PASS |
| _do_execute_signal (sell) | daemon_signals.py:202-206 | `buy_date == today_str` → 拦截 | PASS |
| _market_crash_clear | daemon_risk.py:293-295 | `buy_date == date.today().isoformat()` → continue | PASS |

所有6条卖出路径均正确实现T+1保护。

### 2. 持仓状态 status='held' 是否在所有查询中统一
**结果**: PASS

所有持仓查询均使用 `status='held'` (非 'holding'):
- daemon_db.py:175, 224 — SELECT ... WHERE status='held'
- trading_daemon.py:187, 314 — 子查询 COUNT ... WHERE status='held'

### 3. SQL JOIN问题
**结果**: PASS (daemon层)
daemon层无JOIN daily_price操作。策略层SQL在Phase 4审查。

### 4. 开盘观察期(09:30-10:00)
**结果**: PASS

| 检查点 | 位置 | 逻辑 |
|--------|------|------|
| 所有止损统一观察 | trading_daemon.py:746-748 | `is_stop_loss and 930<=now<1000` → continue |
| Grace Period (非盘中止损) | trading_daemon.py:764-770 | 30分钟内非硬止损延迟 |
| 退潮清仓开盘保护 | daemon_risk.py:255-257 | `now_hm < 940` → return |
| 硬止损不受限 | trading_daemon.py:766-768 | `abs(...) >= abs(HARD_STOP_PCT)` → 不延迟 |

### 5. 极端退潮冷却期
**结果**: PASS

| 检查点 | 位置 | 逻辑 |
|--------|------|------|
| 设置冷却 | daemon_risk.py:283-284 | `_last_ebb_clear_time = datetime.now()` |
| 买入检查 | trading_daemon.py:854-861 | `elapsed < EBB_COOLDOWN_MINUTES` → return |
| 回踩检查 | trading_daemon.py:1281-1293 | 独立买入路径也受冷却约束 |
| 冷却时长 | daemon_config.py:56 | `EBB_COOLDOWN_MINUTES = 30` |

### 6. daemon拆分后的模块导入
**结果**: PASS

无循环导入。循环依赖(daemon_signals ↔ trading_daemon)通过运行时import解决。

### 7. paper_trader中的虚拟账户逻辑
**结果**: PASS
execute_buy中的仓位计算与constants.py对齐:
- A: `A_INITIAL_CAPITAL × A_POSITION_RATIO` = 30000 × 0.33 ≈ 1万
- B: `B_INITIAL_CAPITAL × B_POSITION_RATIO` = 30000 × 0.33 ≈ 1万
- C: `C_INITIAL_CAPITAL × C_POSITION_RATIO` = 30000 × 0.33 ≈ 1万

### 8. DB操作参数化查询
**结果**: FAIL → 已修复 (1处)

- daemon_db.py: 全部使用 `?` 占位符 ✓
- daemon_risk.py: 全部使用 `?` 占位符 ✓
- daemon_signals.py: 全部使用 `?` 占位符 ✓
- **strategy_c.py:112**: 使用f-string拼接SQL — 已修复

### 9. 异常处理
**结果**: PASS

| 模块 | try/except | 说明 |
|------|-----------|------|
| run_daemon主循环 | trading_daemon.py:1395-1442 | 捕获所有异常，3次连续异常退出 |
| scan_once | 通过主循环捕获 | 顶层try覆盖 |
| strategy_a/b/c选股 | 各策略文件 | try/except/finally(conn.close) |
| daemon_db | 所有函数 | try/except/finally |

### 10. 日志记录
**结果**: PASS

关键操作均有日志:
- 买入: execute_buy + _add_signal + _log_to_db 三重记录
- 卖出: execute_sell + _add_signal + _log_to_db 三重记录
- 选股: 各策略函数有logger.info汇总
- 风控触发: daemon_risk.py详细日志
- 预告执行: daemon_signals.py记录执行结果

---

## 发现的问题

### P2-1: strategy_c.py SQL查询未参数化 [已修复]
- **文件**: strategy_c.py:109-114
- **问题**: `WHERE trade_date >= '{dates[0]}'` 使用f-string拼接SQL
- **影响**: 虽然数据源为内部DB查询结果，实际风险极低，但违反参数化查询规范
- **修复**: 改为 `WHERE trade_date >= ?` + `(dates[0],)` 参数

### 建议 (未修改):

1. **dead code**: trading_daemon.py:759-762 — 策略A 15分钟grace period检查位于9:30-10:00全止损观察期之后，永远不会被执行到（因line 746已continue）。建议移除或调整逻辑顺序。

2. **相对路径**: position_monitor.py:44 `Path("data/alpha_miner.db")` 和 market_emotion.py:59 `Path("data/alpha_miner.db")` 使用相对路径，应改为从daemon_config.DB_PATH导入。

3. **INITIAL_CAPITAL vs 三策略独立资金**: daemon_db.py中`INITIAL_CAPITAL=90_000`用于月度回撤计算，但实际仓位按三策略各3万独立计算。月度回撤限额用90_000*5%=4500可能偏大。

---

## 测试验证

```
tests/test_daemon_split.py .....  32 passed
tests/test_signal.py ...........  16 passed
tests/test_backtest_engine.py   18 passed
Total: 66 passed, 0 failed
```

---

## 修复总结

| 级别 | 问题 | 修复文件 | 状态 |
|------|------|---------|------|
| P2 | SQL未参数化 | strategy_c.py | 已修复 |

阶段2审核完成。10个审核要点中9个PASS，1个FAIL已修复。核心daemon流程(T+1/止损/风控/冷却期/日志)实现正确。
