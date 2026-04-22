# Alpha Miner

基于行为金融学的 A 股超短线因子挖掘框架。

## 架构

```
alpha-miner/
├── cli/                    # CLI 命令入口 (python -m cli <command>)
│   ├── __main__.py         #   子命令路由 (collect / report / mine / drift / backtest / script / replay / strategy)
│   ├── collect.py          #   数据采集
│   ├── report.py           #   日报 + 盘后决策简报 + 市场剧本 + 复盘
│   ├── mine.py             #   因子进化挖掘 (LLM 驱动)
│   ├── drift.py            #   漂移报告
│   ├── backtest.py         #   单因子回测
│   ├── replay.py           #   复盘 CLI
│   └── strategy.py         #   策略管理 CLI (list / backtest / evolve / scan)
├── src/
│   ├── data/               # 数据层
│   │   ├── schema.sql      #   SQLite 表结构 (20 张表)
│   │   ├── storage.py      #   Storage: 时间隔离查询、快照管理
│   │   ├── collector.py    #   CollectorManager 调度器
│   │   └── sources/        #   采集器 (akshare)
│   │       ├── akshare_price.py       #  日线行情
│   │       ├── akshare_zt_pool.py     #  涨停/跌停池
│   │       ├── akshare_lhb.py         #  龙虎榜
│   │       ├── akshare_fund_flow.py   #  资金流向
│   │       ├── akshare_concept.py     #  概念板块
│   │       └── akshare_news.py        #  新闻 + 金融情感引擎 + 自动分类
│   ├── factors/            # 因子库 (9 因子)
│   │   ├── base.py         #   BaseFactor / ConditionalFactor / CrossFactor
│   │   ├── registry.py     #   FactorRegistry 自动注册
│   │   ├── formula/        #   公式因子 (5)
│   │   │   ├── zt_ratio.py              # 涨停/跌停比率 (市场级)
│   │   │   ├── consecutive_board.py     # 连板数 (股票级)
│   │   │   ├── main_flow_intensity.py   # 主力净流入强度
│   │   │   ├── turnover_rank.py         # 换手率百分位排名
│   │   │   └── lhb_institution.py       # 龙虎榜机构净买入
│   │   └── narrative/      #   叙事因子 (4)
│   │       ├── theme_lifecycle.py       # 题材生命周期 (萌芽→爆发→衰退)
│   │       ├── narrative_velocity.py    # 新闻加权 3 日变化率 (V2: 类型加权)
│   │       ├── theme_crowding.py        # 题材拥挤度 (反拥挤)
│   │       └── leader_clarity.py        # 龙头清晰度
│   ├── narrative/           # 叙事引擎 (Phase 3)
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
│   │   ├── regime.py       #   市场状态: board_rally / theme_rotation / low_volume / broad_move
│   │   ├── daily_brief.py  #   盘后决策简报 (三大交付物)
│   │   ├── daily_report.py #   传统日报
│   │   └── report.py       #   漂移报告汇总
│   └── mining/             # 进化引擎
│       ├── evolution.py    #   EvolutionEngine: 假说生成 → 代码 → 沙箱 → IC 验收
│       ├── mutator.py      #   假说变异 (交叉/参数微调/理论切换)
│       ├── failure_analyzer.py #  失败因子诊断
│       ├── sandbox.py      #   沙箱执行器
│       ├── _sandbox_runner.py # 沙箱子进程 (安全隔离)
│       └── prompts/        #   LLM Prompt 模板
│           ├── explore.md  #   探索新假说
│           ├── construct.md #  假说→代码翻译
│           └── analyze.md  #   失败分析
├── knowledge_base/         # 知识库
│   └── theories.yaml       #   行为金融学理论库 (前景理论/信息瀑布/羊群效应等)
├── reports/                # 产出报告 [调试阶段，后续加入 .gitignore]
├── tests/                  # 105 tests
└── pyproject.toml          # uv 项目配置
```

## 因子体系

### 公式因子 (Formula)

| 因子 | 级别 | 逻辑 |
|------|------|------|
| zt_ratio | 市场 | 涨停数 / 跌停数，情绪方向 |
| consecutive_board | 股票 | 连板天数 × (1 - 开板率) |
| main_flow_intensity | 股票 | 主力净流入 / 流通市值 |
| turnover_rank | 股票 | 换手率在近 N 日的百分位 |
| lhb_institution | 股票 | 龙虎榜机构净买入额排名 |

### 叙事因子 (Narrative)

| 因子 | 级别 | 逻辑 |
|------|------|------|
| theme_lifecycle | 股票 | 题材涨停数阶段判断 (萌芽→爆发→衰退) |
| narrative_velocity | 股票 | 新闻类型加权 3 日变化率（V2: 7 类加权） |
| theme_crowding | 股票 | 1 - max(题材涨停占比 × 5)，反拥挤 |
| leader_clarity | 股票 | 题材内龙头成交额 / 第二名成交额 |

## 叙事引擎 (Phase 3)

### 新闻分类器

7 类新闻标签，规则引擎优先 + LLM fallback：

| 类型 | 权重 | 说明 |
|------|------|------|
| theme_ignite | 3.0 | 题材点燃（政策/技术突破） |
| catalyst_real | 2.0 | 实质性催化剂（业绩/中标） |
| theme_ferment | 1.5 | 题材发酵（讨论增多） |
| catalyst_expect | 1.0 | 预期性催化剂 |
| good_realize | -0.5 | 利好兑现（见光死） |
| negative | -2.0 | 负面事件 |
| noise | 0.0 | 无关噪音 |

采集新闻时自动分类，填充 `news_type` + `classify_confidence` 列。

### 市场剧本引擎

每日生成结构化剧本：

- **市场快照**：regime、涨停/跌停、连板梯队、热门题材、龙虎榜、资金流向
- **题材判定**：每个热门题材的生命周期阶段 + 操作判定
- **明日策略**：关注/回避列表 + 仓位建议
- **风险提示**：情绪极端、题材拥挤等

```bash
python -m cli script --date 2024-06-15          # 规则版（默认）
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

## 进化引擎

LLM 驱动的因子挖掘闭环：

```
知识库 (theories.yaml)
    ↓ 探索假说
假说配置 (name / prediction / conditions)
    ↓ 代码翻译 (Anthropic API)
因子代码 (compute(universe, as_of, db))
    ↓ 沙箱执行
IC 验收 (Spearman IC > 0.03 且 ICIR > 0.5)
    ↓ 通过
因子入库 (注册到 FactorRegistry)
    ↓ 失败
失败分析 → 变异 → 重试
```

- LLM 接口：Z.AI Anthropic 兼容端点 (`glm-4-plus`)
- 沙箱：子进程隔离执行，预注入 `pd/datetime/Storage`
- Prompt 模板：`explore` → `construct` → `analyze` 三阶段

## 策略子系统 (Phase 4)

策略 = 入场条件 + 出场条件 + 仓位管理的完整交易规则。

### 架构

```
src/strategy/
├── schema.py           # Strategy / Trade / StrategyReport 数据结构 + YAML 序列化
├── backtest_engine.py  # 回测引擎 (T+1 / 多仓位 / regime 分组统计)
├── loader.py           # YAML 策略加载 + 预置策略库
├── evolver.py          # 参数网格搜索进化器
└── store.py            # SQLite 持久化 (3 张新表)
strategies/             # 8 个预置策略 YAML
    ├── 首板打板_龙头确认.yaml
    ├── 连板接力_二板定龙头.yaml
    ├── 题材轮动_新题材首板.yaml
    └── ...
```

### 预置策略库 (8 个)

| 策略 | 逻辑 | 标签 |
|------|------|------|
| 首板打板_龙头确认 | 题材首板 + 龙头清晰度 + 主力流入 | 首板, 打板 |
| 连板接力_二板定龙头 | 二连板 + 题材热度 + 换手充分 | 连板, 接力 |
| 龙头低吸_分歧转一致 | 龙头清晰度 > 2 + 换手排名前 30% | 低吸, 龙头 |
| 题材轮动_新题材首板 | 叙事速度 > 0 + 题材生命周期 = 萌芽 | 题材轮动 |
| 地量反弹_超跌首板 | 换手率极低 + 涨停/跌停比 > 5 | 地量反弹 |
| 强势股回踩_二波 | 连板 > 3 + 回调不破关键位 | 二波 |
| 早盘竞价_量比选股 | 开盘量比 + 主题热点 | 竞价 |
| 打板防守_三班组避雷 | 三班组条件反向过滤 | 防守 |

### 回测引擎

- **T+1 约束**：当日买入次日才能卖出，符合 A 股规则
- **多仓位管理**：最大持仓数限制，等权分配
- **出场条件**：止盈 / 止损 / 最大持仓天数 / 条件出场（优先级递减）
- **Regime 分组统计**：按市场状态分组统计胜率和收益

### 策略进化器

网格搜索参数空间，多目标优化：

```bash
python -m cli.strategy evolve \
    --name "首板打板_龙头确认" \
    --start 2026-01-01 --end 2026-03-31 \
    --objective sharpe --top 5
```

优化目标：`sharpe` / `win_rate` / `profit_loss_ratio` / `composite`

### 持久化 (3 张新表)

| 表 | 用途 |
|----|------|
| strategy_defs | 策略定义（YAML 序列化 + 元数据） |
| strategy_reports | 回测报告（胜率/夏普/回撤/盈亏比） |
| strategy_trades | 交易记录（入场/出场/收益/regime） |

### CLI

```bash
python -m cli.strategy list                                        # 列出预置策略
python -m cli.strategy backtest --name "首板打板_龙头确认" --start 2026-01-01 --end 2026-03-31
python -m cli.strategy evolve --name "首板打板_龙头确认" --start 2026-01-01 --end 2026-03-31
python -m cli.strategy scan --date 2026-04-14                      # 当日信号扫描
```

### DailyBrief 整合

盘后简报新增第四交付物 — 策略扫描信号：

```bash
python -m cli report --brief --strategy-scan
```

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

### 情绪级别 → 仓位映射

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
| 地量 | 无（空仓等待） | 因子信号稀疏 |
| 普涨跌 | main_flow_intensity, turnover_rank | 系统性主导，因子选股能力下降 |

## 漂移检测

| 模块 | 功能 |
|------|------|
| IC Tracker | 滚动 Spearman IC → ICIR / 胜率 / 盈亏比 / 趋势 |
| CUSUM | 递归变点检测，识别因子 IC 结构性断裂 |
| Regime | 市场状态分类 (连板潮 / 题材轮动 / 地量 / 普涨跌 / 正常) |

## 数据库 (20 张表)

daily_price, zt_pool, zb_pool, strong_pool, lhb_detail, fund_flow, concept_mapping, concept_daily, news, market_emotion, factor_values, ic_series, drift_events, regime_state, mining_log, market_scripts, replay_log, strategy_defs, strategy_reports, strategy_trades

## 技术要点

- **时间隔离**: Storage 层严格按 snapshot_time 隔离，因子计算只看到 as_of 之前的数据，杜绝未来函数
- **因子注册**: FactorRegistry 自动扫描 `src/factors/` 下的 BaseFactor 子类，CLI 无需硬编码因子列表
- **CUSUM 变点**: 递归分割 + 标准化累积偏差，阈值可调
- **市场状态**: 多信号投票，置信度最高的 regime 胜出
- **情感引擎**: 金融关键词规则引擎替代 snownlp，针对 A 股语料优化
- **新闻分类**: 规则引擎 + LLM fallback，高置信度跳过 LLM 节省成本
- **LLM 可选**: 所有模块 llm_client=None 时走规则路径，系统照常运行
- **LLM Client**: 三级 fallback (env → openclaw.json → hermes auth.json)

## Quick Start

```bash
# 安装
uv sync

# 跑测试 (171 tests)
uv run pytest tests/ -v --ignore=tests/test_collect_live.py

# 每日完整流程 (7 步)
bash scripts/daily_run.sh

# 分步执行
uv run python -m cli collect --today             # 1. 采集数据
uv run python -m cli backtest --compute-today     # 2. 计算因子
uv run python -m cli drift --date $DATE           # 3. 漂移检测
uv run python -m cli mine evolve                  # 4. 因子进化
uv run python -m cli report --date $DATE          # 5. 日报
uv run python -m cli script --date $DATE --save   # 6. 市场剧本
uv run python -m cli replay --date $DATE --save   # 7. 复盘

# 盘后决策简报
uv run python -m cli report --brief
uv run python -m cli report --brief --holdings 000001,600519 --top 5

# 复盘统计
uv run python -m cli replay --stats
```

## 项目文件

| 文件 | 用途 |
|------|------|
| `alpha-miner-steps-wsl2.md` | 原始开发步骤记录 |
| `factor-mining-v2.md` | 进化引擎设计文档 |
| `narrative-strategy-upgrade.md` | 叙事引擎设计文档 (Phase 3) |
| `strategy-backtest-upgrade.md` | 策略子系统设计文档 (Phase 4) |
| `BUILD_LOG.md` | 构建日志 |
| `CLAUDE.md` | Claude Code 协作指南 |

## 待完成

- [ ] 数据自动填充（交易日 15:40 后 cron 采集）
- [ ] 因子 IC 实盘验证（当前 DB 空，需数据积累）
- [ ] 历史相似形态胜率（交付物二的补充）
- [ ] 策略实盘信号推送（Telegram）
- [ ] Web UI（当前 CLI 输出，后续 Excalidraw 可视化）
- [ ] 贝叶斯参数优化（替代网格搜索）

## License

MIT
