# A股模拟交易框架调研报告

## 一、候选框架对比

### 1. Microsoft Qlib ⭐⭐⭐⭐⭐ (最推荐)

- **GitHub**: microsoft/qlib (15k+ stars)
- **来源**: 微软亚洲研究院，学术论文支撑
- **论文**: "Qlib: An AI-oriented Quantitative Investment Platform" (ACM SIGKDD 2023)
- **核心能力**:
  - 因子挖掘 (Alpha158/Alpha360 内置)
  - ML模型训练 (LightGBM/XGBoost/Transformer等)
  - 回测引擎 (基于真实行情的模拟交易)
  - 组合优化 (风险模型+仓位管理)
- **A股支持**: 原生支持，内置A股数据采集(cn_data)
- **模拟盘**: 支持out-of-sample回测，可做paper trading
- **与我们的契合度**: ★★★★★
  - 我们的Alpha158因子库就是Qlib论文提出的
  - 我们的LightGBM模型与Qlib完全兼容
  - 可以直接用Qlib的回测引擎替代我们的walk_forward
- **安装**: `pip install pyqlib`
- **缺点**: 学习曲线较陡，文档以英文为主

### 2. RQAlpha (米筐) ⭐⭐⭐⭐

- **GitHub**: ricequant/rqalpha (5k+ stars)
- **来源**: 米筐科技（国内量化平台RiceQuant）
- **核心能力**:
  - 事件驱动回测引擎
  - 支持A股/期货
  - 策略编写灵活 (类似实盘API)
  - 可扩展数据源
- **A股支持**: 原生，专为A股设计
- **模拟盘**: 支持模拟交易(rqalpha-mod-simulation)
- **与我们的契合度**: ★★★★
  - 回测引擎成熟，适合做paper trading
  - 需要把我们的ML信号接入
- **安装**: `pip install rqalpha`
- **缺点**: 维护频率下降，部分功能文档不足

### 3. Backtrader ⭐⭐⭐

- **GitHub**: mementum/backtrader (13k+ stars)
- **核心能力**:
  - 经典Python回测框架
  - 事件驱动架构
  - 丰富的指标库(ta-lib集成)
  - 社区活跃，教程多
- **A股支持**: 可用，但需要自己接入数据源
- **模拟盘**: 支持paper trading模式
- **与我们的契合度**: ★★★
  - 通用框架，需要较多适配工作
  - 与ML模型集成需要自己写
- **缺点**: 项目已停止维护(2021年后无更新)

### 4. Zipline (zipline-reloaded) ⭐⭐⭐

- **来源**: Quantopian开源，社区维护版
- **A股支持**: 需要自定义bundle接入A股数据
- **模拟盘**: 支持paper trading
- **缺点**: A股适配工作量大，不如Qlib

### 5. vnpy ⭐⭐

- **GitHub**: vnpy/vnpy (24k+ stars)
- **核心能力**: 全栈量化交易（CTA/价差/期权等）
- **A股支持**: 通过CTP接口
- **模拟盘**: 支持模拟账户
- **缺点**: 过于庞大，主要用于期货/CTA策略，不太适合股票选股

## 二、学术论文支撑

### 核心论文

1. **Alpha158** (Qlib内置因子集)
   - 论文: "Qlib: An AI-oriented Quantitative Investment Platform"
   - 我们已实现53个Alpha158因子

2. **Factor Investment** (因子投资)
   - 书籍: 《主动投资组合管理》(Grinold & Kahn)
   - 书籍: 《量化投资:策略与技术》(丁鹏)

3. **Walk-Forward Optimization** (滚动优化)
   - 论文: "Walk-Forward Analysis" (Robert Pardo)
   - 我们已实现140轮WF验证

4. **LightGBM for Financial Prediction**
   - 论文: "LightGBM: A Highly Efficient Gradient Boosting Decision Tree" (NeurIPS 2017)
   - 已被广泛应用于量化因子预测

5. **Paper Trading Validation** (模拟盘验证)
   - 理论: 任何量化策略在实盘前必须通过out-of-sample验证
   - 最佳实践: 至少3个月的模拟盘稳定盈利

## 三、推荐方案

### 方案A: Qlib回测引擎 (学术级严谨)

用Qlib的回测引擎替代我们的walk_forward:
1. 安装Qlib
2. 把我们的Alpha158因子+LightGBM模型迁移到Qlib格式
3. 用Qlib的`backtest()`函数做out-of-sample回测
4. 结果更可信（经过学术验证的引擎）

### 方案B: 自研模拟盘 (轻量灵活)

基于已有的daily_price数据，自己实现模拟交易:
1. 优点: 完全可控，与现有系统无缝集成
2. 缺点: 需要自己处理分红/除权/停牌等边界情况
3. 参考: Qlib的回测引擎源码作为参照

### 方案C: RQAlpha回测 (国内标准)

用RQAlpha的成熟回测引擎:
1. 把ML信号封装为RQAlpha策略
2. 用A股真实数据回测
3. 绩效报告直接生成

## 四、最终推荐

**先用方案B(自研轻量模拟盘)快速上线，同时参考Qlib源码确保逻辑正确。**

理由:
1. 我们的系统已经完整（数据+因子+模型+UI），不需要引入重量级框架
2. 模拟盘逻辑其实不复杂: 信号→买入→持有→卖出→统计
3. 关键是要用**真实行情数据**验证，而不是回测假设
4. Qlib的核心价值在因子挖掘和模型训练，这部分我们已经做完了
5. 回测引擎部分，Qlib的逻辑也是: 每日信号→下单→撮合→记录，我们可以照着实现

**但必须遵守Qlib论文中的最佳实践:**
1. 严格的时间隔离（不使用未来数据）✓ 我们已实现
2. 行业中性化截面标准化 ✓ 我们已实现  
3. Walk-Forward验证 ✓ 我们已实现
4. 交易成本假设: 手续费万2.5 + 印花税千1（卖出）= 需要加入
5. 滑点假设: 0.1%（开盘价可能无法精确成交）= 需要加入
6. 涨跌停限制: 涨停无法买入，跌停无法卖出 = 需要加入
