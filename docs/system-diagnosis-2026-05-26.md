# Alpha-Miner 系统诊断报告 (2026-05-26)

## 一、strategy_c.py 设计缺陷

1. **全量内存计算，无增量/缓存机制**
   - `get_strategy_c_candidates()` 每次拉80天全A股日K线到内存（~40万行），按股票分组逐只计算MA/EMA/RSI
   - daemon盘中15秒轮询一次，每次重复全量计算，CPU和DB压力大

2. **EMA计算有初始化偏差**
   - 第168-171行 EMA 用 `closes[0]` 作种子值，不同股票起始点不同（60天vs80天），EMA收敛不一致
   - MACD信号不可比，标准做法应预热至少26天再取信号

3. **RSI 只用14天窗口但取了15天数据**
   - 第178行 `closes[-15:]` 取15天算14个delta，暴露指标计算没有统一封装
   - MA/EMA/RSI/MACD各策略各实现一遍，容易不一致

4. **分档逻辑不参与过滤但影响排序**
   - watch档（量比<5）不会被买（daemon只对 hot/normal 做实时检测），但仍占位

5. **循环内逐条SQL查票名**
   - 第244-251行每只候选执行2次SQL查名称，N只候选=2N次查询，应批量拉取

---

## 二、基本面数据空白

1. **financial_summary 覆盖严重不足**
   - 只覆盖426只，A股非ST非科创板约4000只，覆盖率仅~10%
   - 策略C v2的基本面评分对大部分票无数据

2. **缺失关键表**
   - stock_fundamentals（PE/PB/市值）：fundamentals.py 存在但未创建表
   - cash_flow_stmt / balance_sheet：Phase 1要新建，目前不存在
   - industry_daily：行业景气度数据完全没有

3. **基本面排雷器名不副实**
   - `src/screener/fundamental.py` 实际只检查价格/成交量/波动率/ST状态
   - 完全没有PE/ROE/净利润/解禁/减持等真正的基本面检查
   - 注释承认"当前版本是基于价格和成交的初步排雷"

4. **同比计算依赖列名匹配**
   - fundamental.py 第233行用字符串拼接构造去年同期列名，经常匹配失败导致 yoy=None

---

## 三、Daemon 系统潜在 Bug

1. **连接管理不统一，存在泄露风险**
   - `_get_conn()` 多处调用，有些 try/finally 关闭，有些手动 open/close 不在 finally 里
   - 异常时会泄露连接

2. **策略C盘中买入路径永远为空**
   - `_scan_buy` 第854行 `strategy_c_cands = []` 硬编码为空
   - 第866-909行的策略C买入分支是死代码

3. **退潮冷却期检查访问模块内部变量**
   - 通过 `import src.trader.daemon_risk as _risk_mod` 读 `_risk_mod._last_ebb_clear_time`
   - daemon被重启（cron拉起）时变量重置为None，冷却期保护失效

4. **仓位检查与预告计数不同步**
   - `_notice_count` 是方法内局部变量，计数包括已有持仓+本轮预告
   - 已执行预告的票已变持仓，如果上一轮预告执行失败（涨停买不到），仓位被虚占

5. **Grace Period 逻辑重复且冲突**
   - 三层保护逻辑交叉（9:30-10:00止损观望 + Grace Period + 策略A专用15分钟缓冲）
   - 第一层条件更宽，可能拦截本应由策略特定逻辑处理的信号

---

## 四、Phase 1-3 任务书补充建议

### Phase 1 补充
1. 采集要加 timeout 和断点续采逻辑（akshare响应时间波动大）
2. 缺少 stock_fundamentals（PE/PB/市值）表，应纳入Phase 1
3. industry_daily 需要个股-行业映射表，当前 concept_mapping 是概念板块非行业

### Phase 2 补充
1. 评分权重 30/30/20/20 是拍脑袋的，应先用历史数据验证各因子IC值
2. 造假检测验证案例（康美已退市）要换成近两年有数据的案例
3. 缺少评分衰减机制（财报滞后性），3个月前的权重应低于1个月前

### Phase 3 补充
1. 半仓止盈在1万块模拟盘不实用（100股无法精确减半），改为全仓止盈或阶梯
2. 30天持仓与现有5天到期清仓矛盾，需明确是替换还是新增
3. 回测基准偏乐观，先跑出实际数据再定阈值
4. v2接口需与 strategy_c.py 提前约定好返回dict字段

---

## 五、优先级排序

| 优先级 | 问题 | 行动 |
|--------|------|------|
| P0 | daemon策略C买入路径死代码 | 清理或修正 _scan_buy 中的 strategy_c_cands |
| P0 | 连接泄露风险 | 统一用 context manager 管理 _get_conn() |
| P1 | strategy_c 全量计算性能 | 增加缓存/增量计算 |
| P1 | financial_summary 覆盖率 426/4000 | Phase 1 采集前先跑断点续采逻辑 |
| P2 | 基本面排雷器名不副实 | Phase 2完成后改造 screener |
| P2 | 指标计算重复实现 | 提取公共 indicators.py |
