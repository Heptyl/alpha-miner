# Alpha Miner 系统全面审核汇总

**审核日期**: 2026-05-25
**审核范围**: 全系统 ~29000行Python代码
**审核方法**: 6阶段逐层审核，按AUDIT_PLAN.md顺序执行

---

## 总览

| 阶段 | 重点 | 发现问题 | 已修复 | 建议 |
|------|------|---------|--------|------|
| 1. 策略常量与联动 | 参数对齐+信号匹配 | 3 | 3 | 4 |
| 2. Daemon核心流程 | T+1/止损/风控/日志 | 1 | 1 | 3 |
| 3. 数据层 | API防护/路径/缓存 | 0 | 0 | 3 |
| 4. 策略实现 | 选股/止损/回测 | 0 | 0 | 2 |
| 5. 代码质量 | lint/重复/命名 | 0 | 0 | 2 |
| 6. 测试覆盖 | 覆盖率/关键路径 | 0 | 0 | 4 |
| **总计** | | **4** | **4** | **18** |

---

## 按严重程度分级

### P0 (关键bug — 已修复)
| # | 问题 | 影响 | 修复 |
|---|------|------|------|
| 1 | 策略C仓位限制被完全绕过 | daemon_signals.py中`"题材" in sig_type`永远不匹配，策略C买入不受3只上限约束 | 改为`"趋势牛股" in sig_type or "缩量反包" in sig_type` |
| 2 | `"缩量"`字符串匹配错误 | 6处代码用`"缩量" in sig_type`匹配策略B，但会错误匹配策略C旧信号"缩量反包" | 移除`"缩量"`匹配，改为精确匹配已知B信号类型 |

### P1 (重要不一致 — 已修复)
| # | 问题 | 影响 | 修复 |
|---|------|------|------|
| 3 | trading_daemon.py通知计数"题材"不匹配 | 策略C通知计数不准确 | 同P0-1修复 |
| 4 | strategy_c.py SQL未参数化 | f-string拼接SQL，违反安全规范 | 改为参数化查询 |

### P2 (小问题 — 已修复)
| # | 问题 | 修复 |
|---|------|------|
| 5 | daemon_sell_strategies.py B持有天数硬编码 | 改为引用`SELL_PARAMS["B"]["max_hold_days"]` |

### 建议 (未修改，共18项)

**高优先级建议:**
1. 为daemon_sell_strategies/daemon_risk添加单元测试
2. strategy_b.py的6处相对DB路径改为从daemon_config导入
3. position_monitor.py/market_emotion.py的相对DB路径
4. 数据采集后缺少条数校验

**中优先级建议:**
5. daemon_log表添加定期清理机制
6. trading_daemon.py:759-762死代码清理
7. 回测引擎添加交易成本计算
8. 运行`uvx ruff check src/ --fix`修复518个可自动修复的lint问题

**低优先级建议:**
9. 策略文件中重复的涨停豁免判断提取公共函数
10. daemon_config.py从constants.py导入参数(统一SSOT)
11. 修复7个预先存在的测试失败
12. 月度回撤用90万总额计算可能偏大

---

## 修改文件清单

| 文件 | 修改数 | 修改内容 |
|------|--------|---------|
| src/trader/daemon_signals.py | 2处 | 策略C仓位检查修复 + 策略B"缩量"匹配移除 |
| src/trader/daemon_strategies.py | 2处 | 策略B"缩量"匹配移除 |
| src/trader/trading_daemon.py | 3处 | 卖出分发/通知计数/选股分类"缩量"修复 |
| src/trader/daemon_sell_strategies.py | 1处 | B持有天数改用常量引用 |
| src/strategy/strategy_c.py | 1处 | SQL参数化 |

**共修改5个文件，9处代码变更。**

---

## 测试验证

```
pytest tests/ -x --tb=short (排除5个预先存在的broken tests)
66 passed (核心测试), 0 failed
390 passed (全量), 7 pre-existing failed, 14 pre-existing errors
```

本次修改未引入任何回归。

---

## 审核报告文件

- [AUDIT_PHASE1_REPORT.md](AUDIT_PHASE1_REPORT.md) — 策略常量与联动
- [AUDIT_PHASE2_REPORT.md](AUDIT_PHASE2_REPORT.md) — Daemon核心流程
- [AUDIT_PHASE3_REPORT.md](AUDIT_PHASE3_REPORT.md) — 数据层
- [AUDIT_PHASE4_REPORT.md](AUDIT_PHASE4_REPORT.md) — 策略实现
- [AUDIT_PHASE5_REPORT.md](AUDIT_PHASE5_REPORT.md) — 代码质量
- [AUDIT_PHASE6_REPORT.md](AUDIT_PHASE6_REPORT.md) — 测试覆盖
