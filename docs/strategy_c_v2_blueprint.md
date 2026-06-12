# 策略C重构蓝图：基本面驱动 + 技术面择时

## 一、设计理念

**旧策略C**：纯量价动量（量比>=5 → 买入 → 持5天）= 赌短期惯性
**新策略C**：先选好公司 → 再找买点 → 持到逻辑兑现

学术基础：
1. Piotroski (2000) "Value Investing: The Use of Historical Financial Statement Information
   to Separate Winners from Losers", Journal of Accounting Research
   - 9项财务指标打分(F-Score 0-9)，高分组合年化超额收益13.4%
   - 核心逻辑：盈利能力+杠杆/流动性+运营效率

2. Greenblatt "Magic Formula" (2006)
   - 资本回报率(ROC) + 盈利收益率(EY) 双排名
   - 年化收益30.8%（回测），实盘需降低预期

3. Novy-Marx (2013) "The Other Side of Value: The Gross Profitability Premium"
   - 毛利率高的公司有显著超额收益
   - 与价值因子互补

## 二、选股流程（四层漏斗）

### 第1层：行业景气度（宏观→中观）
**数据源**：akshare行业资金流向 + 行业涨跌幅 + 政策新闻

评分维度：
- 行业近20日资金净流入（主力净买入）
- 行业近20日涨幅排名（前30%加分）
- 政策支撑（news表is_policy=1的行业新闻数量）
- 排除夕阳行业（房地产链、传统煤炭等）

输出：景气行业TOP 10

### 第2层：公司基本面评分（F-Score变体）
**数据源**：akshare财报接口 → financial_summary表

评分维度（满分100分）：

A. 盈利能力（30分）
- ROE > 8% (+10分)
- 净利率 > 10% (+10分)  
- 毛利率 > 30% (+10分)

B. 成长性（30分）
- 营收同比增长 > 15% (+10分)
- 净利润同比增长 > 20% (+10分)
- 连续2个报告期增长 (+10分)

C. 财务健康（20分）
- 资产负债率 < 50% (+10分)
- 经营现金流为正 (+10分)

D. 造假预警（扣分项，-20分）
- 应收账款增速远超营收增速 (>2倍) → -10分
- 净利润与经营现金流严重背离 → -10分

E. 增持/机构信号（20分）
- 大股东/高管增持 (+10分)
- 机构调研次数TOP (+10分)

输出：基本面评分 >= 60分的股票池（预计50-100只）

### 第3层：技术面择时（买点确认）
**在基本面合格的基础上，用技术面找买点**

触发条件（满足任一）：
- 量比 >= 3（放量启动，不必>=5那么极端）
- 突破20日均线 + 站上5日均线
- MACD金叉
- 缩量回调至支撑位（20日/60日均线）

过滤条件（排除）：
- 当日涨幅 > 7%（追高不行）
- 连续3天大涨后（获利盘太多）
- 大盘情绪冰点（涨停<20，跌停>50）

### 第4层：仓位管理
- 单只股票不超过总仓位20%
- 同行业不超过总仓位40%
- 总仓位根据大盘情绪调整（冰点30%/正常60%/高温90%）

## 三、持仓与卖出逻辑

### 持仓周期
- 目标持仓：10-30个交易日（不是5天短线）
- 基本面没有恶化就拿着

### 卖出条件
1. 止损：买入价 -8%（给足空间，基本面好的票波动大）
2. 止盈：
   - 目标收益 +15%（到目标减半仓）
   - 趋势破坏（跌破20日均线+量缩）
3. 基本面恶化：
   - 业绩预告大幅低于预期
   - 大股东大幅减持
   - 负面政策新闻
4. 时间止损：持仓超过30天未达目标，评估是否继续

## 四、数据采集需求

### 需要新增的数据表
| 表名 | 数据源 | 用途 |
|------|--------|------|
| stock_fundamental | akshare: stock_financial_analysis_indicator_em | 基本面评分原始数据 |
| industry_ranking | akshare: stock_board_industry_spot_em | 行业景气度 |
| stock_holder_trade | akshare: stock_shareholder_change_ths | 增持/减持 |
| cash_flow_stmt | akshare: stock_cash_flow_sheet_by_report_em | 造假检测（经营现金流） |
| balance_sheet | akshare: stock_balance_sheet_by_report_em | 造假检测（应收账款） |
| institution_research | akshare: stock_research_report_em | 机构关注度 |

### 已有可复用的数据
| 表名 | 用途 |
|------|------|
| financial_summary | 盈利能力/成长性（需扩充字段） |
| fund_flow | 资金流向 |
| holder_change | 增持/减持 |
| news + is_policy | 政策支撑 |
| market_emotion | 大盘情绪 |
| daily_price | 技术面择时 |
| concept_mapping | 概念/行业归属 |

## 五、实施步骤

### Phase 1：数据底座（Claude Code执行）
1. 扩展financial_summary表，增加更多字段（经营现金流/应收账款/总资产等）
2. 新建stock_fundamental表，用akshare拉取全量A股基本面指标
3. 新建industry_ranking表，拉取行业资金流向和涨跌幅
4. 新建cash_flow_stmt和balance_sheet表，用于造假检测
5. 每张表拉完后验证条数（数据铁律）

### Phase 2：评分引擎（Claude Code执行）
1. 实现 fundamental_scorer.py — 输入stock_code → 输出F-Score(0-100)
2. 实现 fraud_detector.py — 输入stock_code → 输出造假风险(高/中/低)
3. 实现 industry_scorer.py — 输入行业 → 输出景气度评分
4. 单元测试 + 用已知股票验证（如造假股：康美药业/瑞幸 vs 好公司：茅台/宁德）

### Phase 3：选股Pipeline（Claude Code执行）
1. 实现 strategy_c_v2.py — 四层漏斗选股
2. 整合到daemon系统（替换旧strategy_c.py）
3. 回测框架：用2022-2025历史数据回测
4. 回测指标：年化收益/最大回撤/夏普/胜率/PF

### Phase 4：验证与上线
1. 模拟盘运行1个月
2. 对比旧策略C的表现
3. 确认没问题后切换实盘

## 六、与旧策略的关系

- 旧strategy_c.py保留为strategy_c_legacy.py（备份）
- 新策略C在同一仓位（3万/3只）下运行
- 技术面择时部分复用量比计算逻辑（但阈值从>=5降到>=3）
- daemon系统集成方式不变（StrategyRegistry注册）

## 七、验收标准

1. 基本面评分器准确率：已知好公司>=70分，已知烂公司<=30分
2. 造假检测：能识别出康美药业/獐子岛等历史造假案例
3. 回测年化收益 > 15%（考虑交易成本后）
4. 最大回撤 < 20%
5. 持仓期间不出现基本面地雷（业绩暴雷/造假曝光）
