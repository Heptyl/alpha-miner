# Phase 3: 数据层审核报告

**审核时间**: 2026-05-25

---

## 审核要点逐条检查

### 1. 东财API pitfall: clist pz=5500盘中只返回100条的限制
**结果**: PASS
- market_emotion.py:47 — `pz=5500`仅用于交叉验证，标注`[GUARD-BYPASS]`
- market_emotion.py:111 — 明确注释"修复: clist pz限制100条不可用, 改用ulist多指数"
- 盘中涨停/跌停用`pz=500`，安全

### 2. 数据验证: 每次拉取后是否验证条数
**结果**: FAIL (建议)

collector.py中各数据源拉取后只检查异常，不验证条数：
- collector.py:52-54 — zt_pool拉取后无条数校验
- collector.py:61-63 — zb_pool同上
- collector.py:79-82 — daily_price同上

**建议**: 添加最小条数校验，如`if count < 10: logger.warning("数据可能不完整")`

### 3. WSL网络适配
**结果**: PASS
- market_emotion.py:36-40 — 完整浏览器UA
- market_emotion.py:62-68 — `_get_session()` Session复用
- realtime_quote.py:16 — 使用Windows curl兼容WSL
- market_emotion.py:492-496 — requests失败时降级到curl

### 4. 交叉验证逻辑
**结果**: PASS
- market_emotion.py:127-206 — 使用比例对比而非min/max保守值
- market_emotion.py:179-181 — 计算ratio和delta进行验证
- market_emotion.py:184-206 — 阈值验证(5%/15%)，非简单min/max

### 5. DB路径
**结果**: FAIL (建议)

| 文件 | 路径方式 | 问题 |
|------|---------|------|
| daemon_config.py:17 | `PROJECT_ROOT / "data" / "alpha_miner.db"` | 绝对路径 ✓ |
| strategy_a.py:32 | `Path(__file__)... / "data" / "alpha_miner.db"` | 绝对路径 ✓ |
| strategy_c.py:64 | `Path(__file__)... / "data" / "alpha_miner.db"` | 绝对路径 ✓ |
| strategy_b.py (6处) | `"data/alpha_miner.db"` | **相对路径** ✗ |
| position_monitor.py:44 | `Path("data/alpha_miner.db")` | **相对路径** ✗ |
| market_emotion.py:59 | `Path("data/alpha_miner.db")` | **相对路径** ✗ |
| storage.py:21 | `db_path: str = "data/alpha_miner.db"` | **相对路径默认值** ✗ |

**影响**: 如果CWD不是项目根目录，这些模块会找不到DB。daemon从项目根启动所以实际不会触发，但单元测试或独立调用时可能出错。

### 6. daily_price查询period过滤
**结果**: N/A
daily_price表没有period字段，使用trade_date过滤。daemon相关表(daemon_positions等)正确使用period过滤。

### 7. daemon_log超时防护
**结果**: FAIL (建议)
- daemon_db.py:27 — 连接timeout=30s，但DELETE操作无时间限制
- trading_daemon.py:1543 — `DELETE FROM daemon_log`无WHERE条件，大表时可能锁表
- **建议**: 添加定期清理(如保留最近30天)，或添加超时保护

### 8. 缓存过期机制
**结果**: PASS
- intraday_cache.py:54-57 — `max_minutes`参数控制过期
- intraday_cache.py:62-64 — 日期变更自动清缓存
- intraday_cache.py:69-76 — `_cleanup()`移除过期数据
- intraday_cache.py:81-88 — `get_series()`按时间窗口过滤

---

## 修复总结

本阶段无代码修改。所有FAIL项均为架构/规范建议，不涉及联动不一致或bug。

| 级别 | 问题 | 状态 |
|------|------|------|
| 建议 | 数据拉取后缺少条数校验 | 未修改 |
| 建议 | 4个文件使用相对DB路径 | 未修改 |
| 建议 | daemon_log无超时防护/定期清理 | 未修改 |
