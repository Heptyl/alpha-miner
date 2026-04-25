# Alpha Miner 详细文档

README 的补充文档，涵盖完整架构细节、配置说明和技术实现。

## 完整目录结构

```
alpha-miner/
├── cli/                    # CLI 入口 (python -m cli <command>)
│   ├── __main__.py         #   子命令路由 (collect/report/mine/drift/backtest/script/replay/strategy)
│   ├── collect.py          #   数据采集 (--today / --backfill N)
│   ├── report.py           #   日报 + 盘后决策简报 (--brief) + 市场剧本
│   ├── mine.py             #   因子进化挖掘 (LLM 驱动)
│   ├── drift.py            #   漂移检测
│   ├── backtest.py         #   单因子回测
│   ├── replay.py           #   复盘引擎 CLI
│   └── strategy.py         #   策略管理 (list/backtest/evolve/scan)
├── src/
│   ├── data/               # 数据层
│   │   ├── schema.sql      #   SQLite 表结构 (20 张表)
│   │   ├── storage.py      #   Storage: 时间隔离查询 + BacktestStorage (回测模式)
│   │   ├── collector.py    #   CollectorManager 调度器
│   │   ├── backfill_price.py#  历史价格回填 (baostock)
│   │   └── sources/        #   akshare 采集器
│   │       ├── akshare_price.py       #  日线行情 (stock_zh_a_daily)
│   │       ├── akshare_zt_pool.py     #  涨停/跌停/炸板/强势股池
│   │       ├── akshare_lhb.py         #  龙虎榜明细
│   │       ├── akshare_fund_flow.py   #  资金流向 (同花顺全市场排名)
│   │       ├── akshare_concept.py     #  概念板块映射 + 日聚合
│   │       └── akshare_news.py        #  新闻 + 金融情感引擎 + 自动分类
│   ├── factors/            # 因子库 (9 因子)
│   │   ├── base.py         #   BaseFactor / ConditionalFactor / CrossFactor
│   │   ├── registry.py     #   FactorRegistry 自动扫描注册
│   │   ├── formula/        #   公式因子 (5)
│   │   │   ├── zt_ratio.py              # 涨停/(涨停+跌停)
│   │   │   ├── consecutive_board.py     # 连板数 × (1 - 开板率)
│   │   │   ├── main_flow_intensity.py   # 主力净流入 / 成交额
│   │   │   ├── turnover_rank.py         # 换手率百分位排名
│   │   │   └── lhb_institution.py       # 龙虎榜机构净买入额
│   │   └── narrative/      #   叙事因子 (4)
│   │       ├── theme_lifecycle.py       # 题材生命周期
│   │       ├── narrative_velocity.py    # 新闻类型加权 3 日变化率
│   │       ├── theme_crowding.py        # 题材拥挤度
│   │       └── leader_clarity.py        # 龙头清晰度
│   ├── narrative/           # 叙事引擎
│   │   ├── news_classifier.py          #  新闻分类器 (规则+LLM, 7 类)
│   │   ├── script_engine.py            #  市场剧本引擎
│   │   ├── replay_engine.py            #  复盘引擎
│   │   └── prompts/                    #  LLM Prompt 模板
│   │       ├── market_script.md        #  剧本生成
│   │       ├── replay.md               #  复盘分析
│   │       └── news_classify.md        #  新闻分类
│   ├── drift/              # 漂移检测 + 决策输出
│   │   ├── ic_tracker.py   #   IC 追踪: Spearman IC / ICIR / 胜率 / 盈亏比
│   │   ├── cusum.py        #   CUSUM 变点检测 (递归)
│   │   ├── regime.py       #   市场状态分类
│   │   ├── daily_brief.py  #   盘后决策简报 (三大交付物)
│   │   ├── daily_report.py #   传统日报
│   │   └── report.py       #   漂移报告汇总
│   ├── mining/             # 进化引擎 v2
│   │   ├── evolution.py    #   EvolutionEngine: 假说→回测→手术台→验收/变异
│   │   ├── backtester.py   #   FactorBacktester: 真实逐日 Spearman IC 回测
│   │   ├── surgery_table.py#   FactorSurgeryTable: 三分段诊断 + 黄金窗口
│   │   ├── failure_analyzer.py #  失败因子诊断 (整合手术台)
│   │   ├── mutator.py      #   定向变异 (11 种诊断 → 9 种变异操作)
│   │   ├── candidate_pool.py # 候选池 (5 天观察期状态机)
│   │   ├── sandbox.py      #   沙箱执行器
│   │   ├── _sandbox_runner.py # 沙箱子进程 (BacktestStorage, 500 universe)
│   │   └── prompts/        #   LLM Prompt
│   │       ├── explore.md  #   探索新假说
│   │       ├── construct.md #  假说→代码翻译
│   │       └── analyze.md  #   失败分析
│   ├── strategy/           # 策略子系统
│   │   ├── schema.py       #   Strategy / EntryRule / ExitRule / PositionRule / Trade / StrategyReport
│   │   ├── backtest_engine.py #  回测引擎 (T+1 / 多仓位 / regime 分组)
│   │   ├── loader.py       #   YAML 策略加载
│   │   ├── evolver.py      #   参数网格搜索进化器
│   │   └── store.py        #   SQLite 持久化
│   └── pipeline/           # IC 管线
│       └── runner.py       #   批量 IC 计算 + 持久化
├── factors/                # 进化产出的因子代码
│   ├── cascade_momentum.py
│   ├── seal_decay_warning.py
│   ├── emotion_reversal.py
│   ├── strong_emotion_board_alpha.py
│   ├── cascade_break_crash.py
│   └── weak_emotion_avoid.py
├── knowledge_base/
│   ├── theories.yaml       #   行为金融学理论库 (12 个假说)
│   └── strategies.yaml     #   预置策略 (5 个)
├── config/
│   ├── factors.yaml        #   因子注册表
│   └── settings.yaml       #   全局配置
├── scripts/
│   ├── daily_run.sh        #   每日 7 步完整流程
│   ├── hourly_mine.sh      #   定时进化挖掘
│   └── compute_factors.py  #   因子计算脚本
├── tests/                  # 288 tests
└── pyproject.toml          # uv 项目配置 (Python >= 3.11)
```

## 因子详细说明

### zt_ratio (市场级)

涨停/(涨停+跌停) 比率。市场级情绪因子，反映当日多空力量对比。

### consecutive_board (股票级)

连板天数 × (1 - 开板率)。接力情绪指标，连板天数越高且开板次数越少，分数越高。

### main_flow_intensity (股票级)

主力净流入 / 成交额。衡量大资金参与强度，正值为主力净买入。

### turnover_rank (股票级)

换手率在近 N 日的百分位排名。反映资金关注度，过高可能过热。

### lhb_institution (股票级)

龙虎榜机构净买入额排名。专业资金信号，机构买入越多排名越靠前。

### theme_lifecycle (股票级)

根据题材内连续涨停天数判断生命周期阶段：萌芽 (1-2天) → 爆发 (3-5天) → 衰退 (5天+)。

### narrative_velocity (股票级)

新闻数量 3 日变化率，按 7 类新闻类型加权。theme_ignite (3.0) 权重最高，noise (0.0) 不计入。

### theme_crowding (股票级)

1 - max(题材涨停占比 × 5)。题材涨停占比越高分数越低，反拥挤逻辑。

### leader_clarity (股票级)

题材内龙头成交额 / 第二名成交额。龙头辨识度越清晰，题材持续性越好。

## 叙事引擎

### 新闻分类器

7 类新闻标签，规则引擎优先 + LLM fallback：

| 类型 | 权重 | 说明 |
|------|------|------|
| theme_ignite | 3.0 | 题材点燃 (政策/技术突破) |
| catalyst_real | 2.0 | 实质性催化剂 (业绩/中标) |
| theme_ferment | 1.5 | 题材发酵 (讨论增多) |
| catalyst_expect | 1.0 | 预期性催化剂 |
| good_realize | -0.5 | 利好兑现 (见光死) |
| negative | -2.0 | 负面事件 |
| noise | 0.0 | 无关噪音 |

分类流程：先走规则引擎（关键词+正则匹配），置信度低于阈值时 fallback 到 LLM。高置信度直接跳过 LLM 节省成本。

### 市场剧本引擎

每日生成结构化剧本：

- **市场快照**：regime、涨停/跌停、连板梯队、热门题材、龙虎榜、资金流向
- **题材判定**：每个热门题材的生命周期阶段 + 操作判定
- **明日策略**：关注/回避列表 + 仓位建议
- **风险提示**：情绪极端、题材拥挤等

```bash
python -m cli script --date 2024-06-15          # 规则版
python -m cli script --date 2024-06-15 --llm     # LLM 增强版
python -m cli script --date 2024-06-15 --save    # 存入数据库
```

### 复盘引擎

对比昨日剧本预测 vs 今日实际：

- **regime 准确率**：昨日预测的 regime 是否命中
- **题材命中/错过**：关注列表中哪些题材今日爆发
- **异常事件检测**：极端牛市、恐慌性抛售、冰点行情
- **教训 + 调整建议**

```bash
python -m cli replay --date 2024-06-16          # 复盘
python -m cli replay --date 2024-06-16 --llm     # LLM 增强复盘
python -m cli replay --date 2024-06-16 --save    # 存入数据库
python -m cli replay --stats                      # 准确率统计
```

## 进化引擎 v2

v2 核心升级：用真实 IC 回测替换假沙箱评分，引入因子手术台做结构化诊断，定向变异替代随机变异，候选池做观察期过滤。

### 整体流程

```
知识库种子 (theories.yaml, 12 个假说)
    ↓ LLM/模板 → 代码翻译
因子代码 → FactorBacktester (逐日 Spearman IC, ic_series 带 regime/zt_count)
    ↓ ic_series → FactorSurgeryTable (三分段分析)
    ↓
  验收通过?
    ├── YES → CandidatePool (5 天观察期 → promoted 入库)
    └── NO  → FailureAnalyzer (整合手术台报告)
              ↓ 诊断 (11 种类型)
              FactorMutator (定向变异, 9 种操作)
              ↓ 变异因子 → 重新回测
```

### Step 1: 真实回测器 (FactorBacktester)

`src/mining/backtester.py`

替代原来沙箱中的假评分，用真实市场数据逐日计算 Spearman IC。

**核心方法**：`run(factor_config, universe, start_date, end_date) → BacktestResult`

**BacktestResult 结构**：

```python
@dataclass
class BacktestResult:
    ic_mean: float          # 全期 IC 均值
    ic_std: float           # IC 标准差
    icir: float             # IC 均值 / IC 标准差
    win_rate: float         # IC > 0 的比例
    pnl_ratio: float        # 正 IC 均值 / |负 IC 均值|
    sample_days: int        # 有效交易日数
    ic_series: list[dict]   # 逐日 IC 明细
```

**ic_series 每条记录**：

| 字段 | 说明 |
|------|------|
| date | 交易日期 |
| ic | 当日 Spearman IC |
| fwd_return_mean | 当日 forward return 均值 |
| regime | 当日市场状态 |
| zt_count | 当日涨停数 |

**连续段过滤**：过滤非连续交易段（间隔>3自然日），避免跨月 fwd return 失真。

**CLI 调用**：

```bash
python -m cli mine surgery --factor consecutive_board --days 60
```

### Step 2: 因子手术台 (FactorSurgeryTable)

`src/mining/surgery_table.py`

对因子 IC 做三分段结构化分析，回答"这个因子在什么条件下有效"。

**三分段**：

| 维度 | 分段方式 | 指标 |
|------|---------|------|
| Regime | board_rally / theme_rotation / low_volume / normal | IC 均值, ICIR, 有效天数 |
| 情绪 | 按涨停数分桶 (weak/normal/strong) | IC 均值, ICIR |
| 时间 | 前1/3 vs 中1/3 vs 后1/3 | IC 趋势 (衰减/稳定/增强) |

**SurgeryReport 结构**：

```python
@dataclass
class SurgeryReport:
    overall: OverallIC           # 全局 IC/ICIR/胜率
    regime_breakdown: list[RegimeIC]    # regime 分段
    emotion_breakdown: list[EmotionIC]  # 情绪分段
    time_breakdown: TimeIC              # 时间分段
    golden_window: GoldenWindow | None  # 黄金窗口
    diagnosis: str                      # 5 种诊断之一
    details: dict                       # 诊断详情
```

**5 种诊断**：

| 诊断 | 含义 | 触发条件 |
|------|------|---------|
| robust | 全局有效 | 全局 IC > 0.03 且 ICIR > 0.5 |
| regime_dependent | 仅特定 regime 有效 | 某 regime ICIR > 1.0 但其他 ≤ 0.5 |
| emotion_dependent | 仅特定情绪有效 | 某情绪桶 ICIR > 1.0 但其他 ≤ 0.5 |
| time_decayed | IC 时间衰减 | 前1/3 IC > 后1/3 IC × 2 |
| no_signal | 无有效信号 | 所有分段 ICIR ≤ 0.5 |

**黄金窗口**：自动检测最佳 regime + 情绪组合。

### Step 3: 失败分析器 + 定向变异

`src/mining/failure_analyzer.py` + `src/mining/mutator.py`

失败分析器整合手术台报告，输出 11 种诊断：

| 诊断 | 触发 | 变异策略 |
|------|------|---------|
| too_strict | 样本过少 | 放宽阈值 (×0.8) 或移除最弱条件 |
| too_loose | 信号太频繁 | 收紧阈值 (×1.2) 或补充条件 |
| reversed | IC 方向反向 | 反转方向 |
| wrong_direction | IC 显著为负 | 反转方向 + 扩大窗口 |
| noisy_but_directional | IC 正但不稳定 | 加 regime 过滤 |
| regime_dependent | 仅特定 regime 有效 | 加 regime 前置条件 (多变体) |
| emotion_dependent | 仅特定情绪有效 | 加涨停数过滤 |
| time_decayed | IC 衰减 | 缩短窗口 + 反转 |
| redundant | 与已有因子高相关 | 差异化 |
| inconsistent | IC 波动大 | 平滑 + regime 过滤 |
| no_signal | IC 全段无效 | 方向反转 + 窗口调整 |

**9 种变异操作** (每个变异都带 `mutation_type` 字段)：

| 操作 | mutation_type | 说明 |
|------|--------------|------|
| `_loosen_thresholds` | loosen_thresholds | 阈值 × ratio (默认 0.8) |
| `_tighten_thresholds` | tighten_thresholds | 阈值 × ratio (默认 1.2) |
| `_remove_weakest_condition` | remove_condition | 移除最后一个条件 |
| `_add_condition_from_knowledge` | add_condition | 从知识库补充条件 |
| `_reverse_direction` | reverse_direction | 反转 direction 字段 + 设 reverse=True |
| `_add_regime_filter` | regime_filter | 添加 regime 前置条件 |
| `_change_lookback` | change_lookback | 调整 lookback_days |
| `_add_smoothing` | smoothing | 添加平滑窗口 |
| `_add_zt_count_filter` | zt_count_filter | 添加涨停数区间过滤 |
| `_differentiate_from` | differentiate | 与相关因子做差异化 |

### Step 4: 动态 Regime 权重

`src/drift/daily_brief.py`

盘后简报中 Regime → 因子权重从历史 IC 动态计算：

```
动态权重 = 某因子在当前 regime 的近 N 天 IC 均值
```

fallback 逻辑：动态权重无法计算时（数据不足）回退到硬编码权重表。

### Step 5: 候选因子缓冲池

`src/mining/candidate_pool.py`

验收通过的因子不直接入库，先进候选池做 5 天观察期：

**状态机**：

```
new → observing (首次达标)
observing → promoted (连续 5 天达标)
observing → rejected (任意一天不达标)
```

**CandidateFactor 数据结构**：

| 字段 | 说明 |
|------|------|
| name | 因子名 |
| config | 因子配置 |
| first_seen | 首次达标日期 |
| ic_history | 逐日 IC 记录 |
| status | new / observing / promoted / rejected |

### Step 6: 历史反馈

`src/mining/evolution.py` 的 `_get_historical_failures()` 方法。

进化引擎记住每个假说的失败历史。连续失败 ≥3 次的假说自动跳过，避免无限循环浪费。

### Step 7: CLI 集成

`cli/mine.py` 新增 `surgery` 子命令：

```bash
# 对指定因子做手术台分析
python -m cli mine surgery --factor consecutive_board --days 60

# 指定日期范围
python -m cli mine surgery --factor theme_lifecycle --start 2026-01-01 --end 2026-03-31
```

输出：SurgeryReport 的格式化文本（三分段 IC + 诊断 + 建议）。

## 策略子系统

### 预置策略 (5 个)

| 策略 | 入场条件 | 出场 | 来源假说 |
|------|---------|------|---------|
| 首板打板_龙头确认 | 连板≥1 + 换手排名≥30% | 止盈7% / 止损3% / 3天 | info_cascade + theme_lifecycle |
| 题材发酵_跟风低吸 | 换手排名≥30% + 非连板 | 止盈5% / 止损4% / 2天 | theme_lifecycle |
| 情绪冰点_反弹首板 | 首板 + 换手排名≥50% | 止盈10% / 止损5% / 5天 | emotion_regime |
| 三班组回避 | 连板≥2 + 换手排名<20% | 止盈3% / 止损2% / 1天 | three_shift |
| 连板接力_情绪共振 | 连板≥2 + 换手排名≥50%, board_rally | 止盈8% / 止损4% / 2天 / 追踪止损3% | herd_effect |

策略定义在 `knowledge_base/strategies.yaml`，支持 YAML ↔ dataclass 序列化。

### 回测引擎

- **T+1 约束**：当日买入次日才能卖出，符合 A 股规则
- **多仓位管理**：最大持仓数限制，等权分配
- **出场条件**：止盈 / 止损 / 最大持仓天数 / 条件出场 (优先级递减)
- **Regime 分组统计**：按市场状态分组统计胜率和收益
- **持久化**：strategy_defs / strategy_reports / strategy_trades 3 张表

### 策略进化器

网格搜索参数空间，多目标优化：

```bash
python -m cli.strategy evolve \
    --name "首板打板_龙头确认" \
    --start 2026-01-01 --end 2026-03-31 \
    --objective sharpe --top 5
```

优化目标：`sharpe` / `win_rate` / `profit_loss_ratio` / `composite`

## 盘后决策简报 (DailyBrief)

`python -m cli report --brief` 生成三大交付物：

### 交付物一：市场温度计
- Regime 自动识别 → 5 种市场状态
- 情绪 5 级判定：涨停+跌停+炸板率+连板高度 → 极弱/弱/中性/偏强/强
- 建议仓位：极弱 0% → 强 80%
- 有效因子列表 + IC + 趋势

### 交付物二：候选决策卡片 (Top N)
- 评分公式：`score = sum(fv × |ic| × regime_weight) / sum(|ic| × regime_weight) × 10`
- 因子贡献进度条：一眼看到选股理由
- 反向视角：自动列出所有负面因子
- 建议：>7 买入 / 5-7 观望 / <5 回避

### 交付物三：持仓风险预警
- 三班组检测（小市值+低换手+无题材 → 天地板风险）
- 资金流背离（超大单买+大单卖）
- 换手率安全线 / 题材拥挤度

```bash
python -m cli report --brief                                    # 标准简报
python -m cli report --brief --holdings 000001,600519 --top 5  # 指定持仓
python -m cli report --brief --strategy-scan                   # 含策略扫描信号
```

### 情绪 → 仓位映射

| 情绪 | 建议 | 仓位 |
|------|------|------|
| 极弱 | 休息 | 0% |
| 弱 | 谨慎 | 20% |
| 中性 | 可操作 | 40% |
| 偏强 | 积极 | 60% |
| 强 | 重仓 | 80% |

### Regime → 因子权重

| Regime | 加权因子 | 策略提示 |
|--------|---------|---------|
| 连板潮 | consecutive_board, leader_clarity | 优先看龙头辨识度和封板质量 |
| 题材轮动 | theme_crowding, narrative_velocity, theme_lifecycle | 优先看叙事，不追高位连板 |
| 地量 | 无 (空仓等待) | 因子信号稀疏 |
| 普涨跌 | main_flow_intensity, turnover_rank | 系统性主导，因子选股能力下降 |

## 漂移检测

| 模块 | 功能 |
|------|------|
| IC Tracker | 滚动 Spearman IC → ICIR / 胜率 / 盈亏比 / 趋势 |
| CUSUM | 递归变点检测，识别因子 IC 结构性断裂 |
| Regime | 市场状态分类 (连板潮 / 题材轮动 / 地量 / 普涨跌 / 正常) |

## 数据库 (20 张表)

数据层：

| 表 | 用途 |
|----|------|
| daily_price | 日 K 线 |
| zt_pool | 涨停池 |
| zb_pool | 炸板池 |
| strong_pool | 强势股 |
| lhb_detail | 龙虎榜明细 |
| fund_flow | 资金流向 |
| concept_mapping | 板块概念映射 |
| concept_daily | 概念每日聚合 |
| news | 新闻 + 情感 + 分类 |
| market_emotion | 市场情绪指标 |

因子层：

| 表 | 用途 |
|----|------|
| factor_values | 因子计算结果 |
| ic_series | IC 时序追踪 |
| drift_events | 漂移事件记录 |
| regime_state | 市场状态 |

策略层：

| 表 | 用途 |
|----|------|
| strategy_defs | 策略定义 (YAML 序列化 + 元数据) |
| strategy_reports | 回测报告 (胜率/夏普/回撤/盈亏比) |
| strategy_trades | 交易记录 (入场/出场/收益/regime) |

日志层：

| 表 | 用途 |
|----|------|
| mining_log | 因子进化挖掘日志 |
| market_scripts | 市场剧本 |
| replay_log | 复盘记录 |

## 测试 (288 tests)

### 硬断言测试 (47 个)

三层端到端验证，手工构造数据集 + 精确期望值：

| 测试文件 | 覆盖 | 数量 |
|----------|------|------|
| test_hard_narrative_factors | 16 个叙事因子的手工数值计算对比 | 16 |
| test_hard_ic | IC 端到端 (完美正/负相关、手工 Spearman、持久化、边界) | 7 |
| test_hard_evolution | 进化引擎验收判定、阈值、序列化、变异/杂交 | 13 |
| test_template_factors | 进化引擎 11 个种子模板因子可执行性 | 11 |

### v2 新增测试

| 测试文件 | 覆盖 |
|----------|------|
| test_backtester | FactorBacktester 逐日 IC 计算、ic_series 结构、连续段过滤 (4 tests) |
| test_surgery_table | 因子手术台三分段分析、诊断分类、黄金窗口检测 (24 tests) |
| test_evolution_integrity | 进化完整性：阈值非零、IC=0 拒绝、知识库加载 (5 tests) |

### 其他测试文件

| 测试文件 | 覆盖 |
|----------|------|
| test_formula_factors | 5 个公式因子计算 |
| test_narrative_factors | 4 个叙事因子 |
| test_storage | Storage 时间隔离 |
| test_time_isolation | 时间隔离完整性 |
| test_data_layer | 数据采集层 |
| test_drift | 漂移检测 |
| test_mining | 因子挖掘 |
| test_sandbox_ic | 沙箱 IC 计算 |
| test_news_classifier | 新闻分类器 |
| test_script_engine | 剧本引擎 |
| test_replay_engine | 复盘引擎 |
| test_backtest_engine | 策略回测引擎 |
| test_strategy_schema | 策略数据结构 |
| test_strategy_loader | 策略加载 |
| test_strategy_evolver | 策略进化器 |
| test_strategy_store | 策略持久化 |
| test_daily_report | 日报生成 |
| test_external_deps | 外部依赖 |
| test_factor_robustness | 因子鲁棒性 |
| test_cli_smoke | CLI 冒烟测试 |

### 已修复的生产 Bug

- **validate_no_future**: `publish_time` 含时分秒与 `as_of` 日期字符串比较误报 → 截取前10字符
- **_sandbox_runner ICIR**: IC 标准差为 0 时返回 0.0 导致验收失败 → 返回 999.0
- **回测时间隔离**: sandbox 用 snapshot_time 导致回测数据为空 → 新增 BacktestStorage 改用 trade_date
- **涨停因子字段**: seal_times/open_times/seal_amount 不存在 → 改用 open_count/amount

## 技术要点

- **时间隔离**：Storage 层严格按 snapshot_time 隔离；回测场景用 BacktestStorage + trade_date
- **因子注册**：FactorRegistry 自动扫描 `src/factors/` 下 BaseFactor 子类，CLI 无需硬编码因子列表
- **CUSUM 变点**：递归分割 + 标准化累积偏差，阈值可调
- **市场状态**：多信号投票，置信度最高的 regime 胜出
- **情感引擎**：金融关键词规则引擎替代 snownlp，针对 A 股语料优化
- **新闻分类**：规则引擎 + LLM fallback，高置信度跳过 LLM 节省成本
- **LLM 可选**：所有模块 llm_client=None 时走规则路径，系统照常运行
- **LLM Client**：三级 fallback (env → openclaw.json → hermes auth.json)

## 配置说明

### config/settings.yaml

```yaml
database:
  path: "data/alpha_miner.db"

api:
  anthropic:
    api_key: "YOUR_KEY_HERE"
    model: "claude-sonnet-4-20250514"

collector:
  retry_count: 3
  retry_delay: 2
  request_timeout: 30

acceptance:
  min_ic: 0.03
  min_icir: 0.5
  min_win_rate: 0.55
  min_pnl_ratio: 1.2

evolution:
  default_generations: 10
  default_population: 10
  sandbox_timeout: 60
```

### config/factors.yaml

因子注册表，每个因子定义 name/class/module/description/factor_type/lookback_days。FactorRegistry 启动时自动扫描加载。

### knowledge_base/theories.yaml

行为金融学理论库，包含 12 个假说 (前景理论、信息瀑布、处置效应、羊群效应等)，为进化引擎提供假说来源。

## 项目文件

| 文件 | 用途 |
|------|------|
| BUILD_LOG.md | 完整构建过程记录 |
| CLAUDE.md | Claude Code 协作指南 |
| DOCS.md | 本文档 |
| evolution-engine-v2-upgrade.md | 进化引擎 v2 升级规划文档 |
| config/factors.yaml | 因子注册配置 |
| config/settings.yaml | 全局配置 |
| knowledge_base/theories.yaml | 行为金融学理论库 (12 个假说) |
| knowledge_base/strategies.yaml | 预置策略定义 (5 个) |
