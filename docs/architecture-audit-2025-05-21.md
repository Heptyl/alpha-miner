# Alpha Miner 系统架构审计报告

日期: 2026-05-21
状态: 已完成 (5/6完成, 1项待做)

---

## 审计结论

核心交易逻辑（策略A PF=1.86、策略B PF=1.29）是好的，问题不在策略在工程结构。
工程债务已到影响开发效率和可靠性的临界点——每次改bug要动2976行文件，每次参数变更要同步4层。

---

## 数据总览

| 模块 | 文件数 | 总行数 | 说明 |
|------|--------|--------|------|
| src/trader/ | 22 | 14,381 | 交易核心 |
| src/strategy/ | 18 | 6,571 | 策略/因子/回测 |
| scripts/ | 96 | - | 工具脚本（41个冗余） |
| web/pages_new/ | 10 | - | 当前页面（活跃） |
| web/pages/ | 18 | - | 旧页面（应废弃） |

---

## P0 级问题（必须解决）

### 1. trading_daemon.py 是2976行的"上帝对象"

**现状**: 38个函数，承担7类职责：
- 数据库管理（get_conn/init_tables/get_account）
- 交易执行（execute_buy/execute_sell）
- 信号检测（check_buy_signals/check_sell_signals）
- 风控（止损/退潮/集中度/日限亏）
- 通知推送
- 30+硬编码常量
- A/B/C三策略调度

**风险**: 修改隔离度为零。改策略A可能影响B/C。5-19"过期候选"bug就是这个结构导致的（修A没查B，因为A/B逻辑交织）。

**建议拆分方案**:
```
trading_daemon.py（调度器，<300行）
  ├── config.py        — 常量和参数
  ├── db.py            — 数据库操作
  ├── executor.py      — 买卖执行
  ├── risk.py          — 风控检查
  ├── notifier.py      — 通知推送
  └── strategies/
      ├── strategy_a.py — 策略A独立scan
      ├── strategy_b.py — 策略B独立scan
      └── strategy_c.py — 策略C独立scan
```

**前提**: 等自动化测试覆盖后再做，否则拆分过程可能引入新bug。

### 2. scan_once() 746行巨型函数

**现状**: A/B/C三策略的全部扫描、买卖信号检测、升级逻辑平铺在一个函数里。

**风险**: 和P0-1相同，修改隔离度为零。

**建议**: 按策略拆成独立scan方法（scan_strategy_a/scan_strategy_b/scan_strategy_c），主循环统一调度。这个可以先于大拆分做。

### 3. 30+硬编码常量在可执行代码里

**现状**: INITIAL_CAPITAL/MAX_POSITIONS/止损参数全部写在trading_daemon.py顶部，被web层和test层直接import。改参数要动2976行的核心文件。

**风险**: 参数变更高频遗漏（5-17到5-18连续三次50000→70000同步遗漏就是这个问题）。

**建议**: **这个可以先做**。半天搞定。抽取到独立的src/trader/config.py或params.py，daemon.py只import不定义。

---

## P1 级问题（应该解决）

### 4. 双页面系统并行维护

- web/pages/ (18文件) — 旧版，app.py已加SystemExit
- web/pages_new/ (10文件) — 新版，活跃维护
- pages/部分文件在5-19还被修改过

**建议**: 确认pages_new/功能完整后，删除web/pages/和web/app.py。

### 5. scripts/目录41个冗余脚本（占42%）

- 16个backfill脚本功能重叠（all/smart/parallel/3year/baostock等）
- 18个backtest脚本残留版本迭代（v2/v3/v4/ultimate/final/diagnose）
- 7个fill脚本含v1~v4迭代痕迹

**建议**: 保留最终版，其余移到scripts/archive/。

---

## P2 级问题（后续优化）

### 6. strategy模块和trader模块的耦合

- strategy_b.py直接import src.strategy.dragon_score（跨模块）
- trading_daemon.py直接import src.strategy.strategy_a（跨层）
- trading_daemon.py import signal_monitor的私有函数（_get_daily_data等）

**建议**: trader通过统一接口调策略 `strategy.scan(market_data) -> candidates`，不关心策略内部实现。

---

## 执行优先级

|| 序号 | 任务 | 风险 | 工作量 | 状态 ||
||------|------|------|--------|------|
|| 1 | 常量抽取到daemon_config.py | 低 | 半天 | **已完成** ||
|| 2 | 删旧页面(标记废弃) | 低 | 半小时 | **已完成** ||
|| 3 | 冗余脚本归档(36个) | 低 | 半小时 | **已完成** ||
|| 4 | scan_once拆分(746行→52行+5方法) | 中 | 1天 | **已完成** ||
|| 5 | daemon大拆分(db/executor/risk/notifier) | 高 | 2-3天 | 等测试覆盖后 ||
|| 6 | strategy-trader解耦 | 中 | 1天 | 重构时顺带做 ||

---

## 模块依赖关系图

```
数据层:  daily_price(DB) / zt_pool(DB) / realtime_quote(API)
              ↓
策略层:  strategy_a.py / strategy_b.py / dragon_score.py
              ↓
调度层:  trading_daemon.py（1691行, 已拆出5个子模块, scan_once保留原样待后续拆分）

子模块:
  - daemon_config.py (187行) — 纯常量
  - daemon_db.py (305行) — DB/账户/持仓/日志
  - daemon_signals.py (280行) — 预告信号管理
  - daemon_notifier.py (120行) — 通知推送
  - daemon_strategies.py (314行) — 策略候选/回踩
  - daemon_risk.py (343行) — 风控/退潮保护
              ↓
执行层:  execute_buy / execute_sell / risk_manager
              ↓
展示层:  data_service.py → streamlit pages_new/
              ↓
通知层:  notify_trade.py → send_weixin_direct → 微信

跨层问题:
  - daemon直接import strategy_a（应通过接口）
  - daemon直接import signal_monitor私有函数（应通过公开API）
  - web层直接import daemon常量（应通过config模块）
```

---

## trading_daemon.py 核心函数清单

### 数据层(6)
- `_get_conn()` / `init_tables()` / `get_account()` / `get_held_positions()` / `_update_account_value()` / `_log_to_db()`

### 交易执行(6)
- `execute_sell()` / `execute_buy()` / `_calc_commission()` / `_calc_shares()` / `_execute_pending_signals()` / `_do_execute_signal()`

### 信号检测(4)
- `check_buy_signals()` / `check_sell_signals()` / `get_ml_candidates()` / `get_strategy_c_candidates()`

### B策略(4)
- `_get_b_watchlist()` / `_market_crash_clear()` / `_check_b_pullback_realtime()` / `_try_upgrade_positions()`

### 风控(7)
- `_check_market_sentiment()` / `_check_industry_concentration()` / `_check_consecutive_losses()` / `_check_monthly_drawdown()` / `_reset_daily_pnl()` / `_is_trading_time()` / `_is_grace_period()`

### 信号队列(3)
- `_read_pending_signals()` / `_write_pending_signals()` / `_add_signal()`

### 通知(2)
- `_send_batch_notifications()` / `_send_trade_notification()`

### 主循环(2)
- `scan_once()` — 746行（行1963-2709）
- `run_daemon()` — 守护进程主入口

### 被外部引用(12处)
- web/pages_new/2_trading.py — SELL_PARAMS
- web/pages_new/6_evaluator.py — B_STOP_LOSS_PCT
- web/pages/15_realtime_monitor.py — get_ml_candidates
- web/state.py — get_ml_candidates
- web/services/data_service.py — INITIAL_CAPITAL, get_strategy_c_candidates
- tests/test_data_integrity.py — 7处导入
