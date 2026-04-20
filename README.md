# Alpha Miner

基于行为金融学的 A 股超短线因子挖掘框架。

## 架构

```
alpha-miner/
├── cli/                    # CLI 命令入口 (python -m cli <command>)
│   ├── __main__.py         #   子命令路由 (collect / report / mine / drift / backtest)
│   ├── collect.py          #   数据采集
│   ├── report.py           #   日报 + 盘后决策简报 (--brief)
│   ├── mine.py             #   因子进化挖掘 (LLM 驱动)
│   ├── drift.py            #   漂移报告
│   └── backtest.py         #   单因子回测
├── src/
│   ├── data/               # 数据层
│   │   ├── schema.sql      #   SQLite 表结构 (20+ 表)
│   │   ├── storage.py      #   Storage: 时间隔离查询、快照管理
│   │   ├── collector.py    #   CollectorManager 调度器
│   │   └── sources/        #   采集器 (akshare)
│   │       ├── akshare_price.py       #  日线行情
│   │       ├── akshare_zt_pool.py     #  涨停/跌停池
│   │       ├── akshare_lhb.py         #  龙虎榜
│   │       ├── akshare_fund_flow.py   #  资金流向
│   │       ├── akshare_concept.py     #  概念板块
│   │       └── akshare_news.py        #  新闻 + 金融情感引擎
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
│   │       ├── narrative_velocity.py    # 新闻数量 3 日变化率
│   │       ├── theme_crowding.py        # 题材拥挤度 (反拥挤)
│   │       └── leader_clarity.py        # 龙头清晰度
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
├── tests/                  # 69 tests
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
| narrative_velocity | 股票 | 今日新闻数 / 3 日前新闻数 - 1 |
| theme_crowding | 股票 | 1 - max(题材涨停占比 × 5)，反拥挤 |
| leader_clarity | 股票 | 题材内龙头成交额 / 第二名成交额 |

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

- LLM 接口：Z.AI Anthropic 兼容端点 (`claude-3-5-sonnet`)
- 沙箱：子进程隔离执行，预注入 `pd/datetime/Storage`
- Prompt 模板：`explore` → `construct` → `analyze` 三阶段

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

## 技术要点

- **时间隔离**: Storage 层严格按 snapshot_time 隔离，因子计算只看到 as_of 之前的数据，杜绝未来函数
- **因子注册**: FactorRegistry 自动扫描 `src/factors/` 下的 BaseFactor 子类，CLI 无需硬编码因子列表
- **CUSUM 变点**: 递归分割 + 标准化累积偏差，阈值可调
- **市场状态**: 多信号投票，置信度最高的 regime 胜出
- **情感引擎**: 金融关键词规则引擎替代 snownlp，针对 A 股语料优化
- **LLM Client**: 三级 fallback (env → openclaw.json → hermes auth.json)

## Quick Start

```bash
# 安装
uv sync

# 跑测试 (69 tests)
uv run pytest tests/ -v -m "not live"

# 采集数据
uv run python -m cli collect --today
uv run python -m cli collect --backfill --start 2024-01-01

# 因子进化挖掘
uv run python -m cli mine evolve --generations 3 --population 5

# 盘后决策简报
uv run python -m cli report --brief
uv run python -m cli report --brief --holdings 000001,600519 --top 5

# 漂移报告
uv run python -m cli drift --date 2024-06-15
```

## 项目文件

| 文件 | 用途 |
|------|------|
| `alpha-miner-steps-wsl2.md` | 原始开发步骤记录 |
| `factor-mining-v2.md` | 进化引擎设计文档 |
| `BUILD_LOG.md` | 构建日志 |
| `CLAUDE.md` | Claude Code 协作指南 |

## 待完成

- [ ] 数据自动填充（交易日 15:40 后 cron 采集）
- [ ] 因子 IC 实盘验证（当前 DB 空，需数据积累）
- [ ] 历史相似形态胜率（交付物二的补充）
- [ ] Web UI（当前 CLI 输出，后续 Excaildraw 可视化）
- [ ] Telegram 推送集成

## License

MIT
