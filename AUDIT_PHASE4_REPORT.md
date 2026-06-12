# Phase 4: 策略实现审核报告

**审核时间**: 2026-05-25

---

## 审核要点逐条检查

### 1. 策略A龙头评分权重总和
**结果**: PASS
- strategy_a.py:160-186 — 连板30 + 梯队20 + 封板20 + 市值15 + 形态15 = **100分** ✓

### 2. 策略A次日高开2%+翻红确认
**结果**: PASS
- strategy_a.py:203 — `buy_target = round(f_pc * 1.02, 2)` 设置目标价
- trading_daemon.py:1083 — `if open_price_a < yclose * 1.02:` 确认高开>=2%
- trading_daemon.py:1089 — `if current_price <= open_price_a:` 确认翻红(现价>开盘价)
- trading_daemon.py:1095-1099 — weak票不买，confirmed/watch可买
- 确认逻辑完整实现在daemon买入流程中 ✓

### 3. 策略A首阴低×0.98止损
**结果**: PASS
- strategy_a.py:202 — `stop_loss = round(f_low * 0.98, 2)` 计算止损价
- daemon_sell_strategies.py:127-146 — 从signal_reason解析止损价，兜底-5%
- 两处实现一致 ✓

### 4. 策略B真首板过滤(candidates+watchlist)
**结果**: PASS
- strategy_b.py:714-758 (candidates) — 预计算10天窗口+双向检查
- strategy_b.py:970-1004 (watchlist) — 同样逻辑
- 两处均已实现 ✓

### 5. 策略B consecutive_zt=1处理
**结果**: PASS
- strategy_b.py:731 — `consecutive_zt <= 1` 过滤
- strategy_b.py:742-758 — 10天历史检查排除"首板但历史涨停过"
- 正确区分了consecutive_zt=1与真首板 ✓

### 6. 策略C MA5>MA20>MA60多头排列
**结果**: PASS
- strategy_c.py:158-161 — 计算MA5/MA20/MA60
- strategy_c.py:164 — `if not (ma5 > ma20 > ma60): continue` 严格检查 ✓

### 7. 策略C金叉+MACD>0+RSI50-70+量比>=5
**结果**: PASS (参数已更新为v2)
- strategy_c.py:68-71 — `VOL_RATIO_MIN=5.0, RSI_LOW=50, RSI_HIGH=70`
- strategy_c.py:167-173 — MACD>0 (EMA12>EMA26) ✓
- strategy_c.py:176-183 — RSI 50-70 ✓
- strategy_c.py:185-188 — 量比>=5 ✓
- 注: 旧版参数(量比>=1.2, RSI50-80)已在v2中更新

### 8. 止损参数匹配标的波动率
**结果**: 建议
- A: -5%(兜底) / 首阴低×0.98(实际) — 短线龙头波动大，-5%合理
- B: -3% — 首板回踩波动小，-3%合理
- C: -8% — 趋势股波动大，-8%合理(ATR止损已测试，43笔0%胜率被废弃)
- **结论**: 固定止损是经过回测验证的设计选择，非bug

### 9. 回测引擎交易成本
**结果**: 建议
- backtest_engine.py — 未找到交易成本扣除代码
- daemon_config.py:59-60 — `COMMISSION_RATE=0.00025, STAMP_DUTY_RATE=0.0005`
- strategy_c.py注释提及"含交易成本0.125%"，但引擎本身未实现
- **建议**: 在backtest_engine中添加可选的成本扣除参数
- **影响**: 不影响实盘(daemon中execute_buy/sell已包含成本)，仅影响回测准确性

### 10. 策略A/B/C候选+监控两档
**结果**: PASS
- A: candidates返回tier分档(confirmed/watch/weak) ✓
- B: candidates(已回踩) + watchlist(待回踩) 两函数独立 ✓
- C: candidates返回tier分档(hot/normal/watch) + watchlist ✓

---

## 修复总结

本阶段无代码修改。所有审核要点均PASS或为设计建议。

| 级别 | 问题 | 状态 |
|------|------|------|
| 建议 | 回测引擎未实现交易成本 | 未修改(设计改进) |
| 建议 | 固定止损vs动态ATR止损 | 未修改(已验证的设计选择) |
