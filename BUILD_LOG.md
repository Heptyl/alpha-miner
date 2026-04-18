# Alpha Miner — 构建过程记录

## 项目概述

A 股超短线因子挖掘系统。自动采集数据 → 计算因子 → 漂移检测 → 因子进化（LLM 辅助）→ 日报输出。

## 技术栈

- **语言**: Python 3.11
- **包管理**: uv
- **数据库**: SQLite（时间隔离查询，snapshot_time 防未来数据泄露）
- **数据源**: akshare（涨停/跌停/龙虎榜/资金流/概念板块/新闻）
- **因子**: 公式因子 5 个 + 叙事因子 4 个 + 可扩展条件/交叉组合
- **进化**: 知识库种子 → 模板/LLM 生成 → 沙箱执行 → IC 验收 → 失败分析 → 变异

## 构建步骤

### Phase 1: 基础设施 (Step 1-6)

| Step | 内容 | 关键文件 |
|------|------|----------|
| 1 | 项目骨架 | `pyproject.toml`, `CLAUDE.md`, 目录结构 |
| 2 | 数据库层 | `src/data/schema.sql`, `src/data/storage.py` — snapshot_time 时间隔离 |
| 3 | 因子基类 | `src/factors/base.py` — BaseFactor/ConditionalFactor/CrossFactor, FutureDataError 校验 |
| 4 | 数据采集器 | `src/data/sources/akshare_*.py` — price/zt_pool/zb_pool/strong_pool/lhb/fund_flow |
| 5 | 扩展采集 | concept_mapping, news, market_emotion 聚合, concept_daily 聚合 |
| 6 | 采集 CLI | `cli/collect.py` — `--date/--backfill/--today` |

### Phase 2: 因子体系 (Step 7-8)

**公式因子** (`src/factors/formula/`):
- `zt_dt_ratio` — 涨停/跌停比，市场情绪温度计
- `consecutive_board` — 连板高度，打板空间感知
- `main_flow_intensity` — 主力资金强度（大单净流入/成交额）
- `turnover_rank` — 换手率排名，热度衡量
- `lhb_institution` — 龙虎榜机构净买入额

**叙事因子** (`src/factors/narrative/`):
- `narrative_velocity` — 个股新闻加速度
- `theme_crowding` — 题材拥挤度（涨停股所属概念集中度）
- `theme_lifecycle` — 题材生命周期（萌芽/爆发/衰退）
- `leader_clarity` — 龙头清晰度（连板最高股的概念唯一性）

**组合机制**: 条件因子(ConditionalFactor) + 交叉因子(CrossFactor) 支持任意组合。

### Phase 3: 漂移检测 (Step 9-10)

| 模块 | 功能 |
|------|------|
| `ic_tracker.py` | Spearman IC 序列 → 滚动 ICIR/胜率/PnL比 → healthy/warning/dead 状态判断 |
| `regime.py` | CUSUM 变点检测 → 4 种市场状态识别（连板潮/题材轮动/地量/普涨普跌） |
| `report.py` | 因子漂移告警生成 |

### Phase 4: 进化引擎 (Step 11-13)

| Step | 内容 | 关键文件 |
|------|------|----------|
| 11 | 漂移报告 + backtest CLI | `cli/backtest.py`, `cli/drift.py` |
| 12 | 知识库 + 失败分析 + 变异器 | `src/mining/knowledge_base.py`, `failure_analyzer.py`, `factor_mutator.py` |
| 13 | 进化引擎 + 沙箱 + LLM | `src/mining/evolution.py` — 候选生成/交叉/沙箱执行/IC 验收 |

进化流程:
```
知识库种子 → 候选因子代码（模板或 LLM 生成）
    → 沙箱执行 compute() → IC 验收（|mean IC| > 0.02 且 ICIR > 0.5）
    → 失败分析 → 变异（放宽阈值/收紧阈值/反转方向/去噪）
    → 下一代
```

### Phase 5: 产出与流程 (Step 14-15)

| Step | 内容 | 关键文件 |
|------|------|----------|
| 14 | 挖掘 CLI | `cli/mine.py` — test-seeds/evolve/mutate/history |
| 15 | 日报 + 每日流程 | `src/drift/daily_report.py`, `cli/report.py`, `scripts/daily_run.sh` |

日报 6 个板块: 市场概况 → 因子排名 → 漂移预警 → 挖掘结果 → 明日候选（regime 调权）→ 系统状态

## Bug 修复 (穿插在各 Step 之间)

| # | 问题 | 修复方案 |
|---|------|----------|
| 1 | backfill 用了实时接口 | `fetch_history()` 走 `stock_zh_a_daily`，`fetch_today()` 走 `stock_zh_a_spot_em` |
| 2 | dt_count 硬编码 0 | 从 daily_price 查 pct_change < -9.5% |
| 3 | FactorRegistry 单例泄露 | `_factors` 从类变量改实例变量 |
| 4 | lhb_detail 无去重 | `UNIQUE(stock_code, trade_date, buy_depart, sell_depart)` |
| 5 | concept 每天全量拉 | 缓存 7 天 TTL，失败 fallback 读旧数据 |
| 6 | 因子 compute 无去重 | `dedup_latest()` — 按 key_cols 分组保留最新 snapshot_time |

## 项目结构

```
alpha-miner/
├── cli/                    # 命令行入口
│   ├── collect.py          # 数据采集
│   ├── backtest.py         # 回测 + 因子计算
│   ├── drift.py            # 漂移检测
│   ├── mine.py             # 因子进化
│   └── report.py           # 日报生成
├── config/
│   ├── factors.yaml        # 因子注册配置
│   └── mining_config.yaml  # 进化参数
├── scripts/
│   └── daily_run.sh        # 每日流程脚本
├── src/
│   ├── data/               # 数据层
│   │   ├── schema.sql      # 建表语句
│   │   ├── storage.py      # SQLite ORM（时间隔离查询）
│   │   ├── collector.py    # 采集调度器
│   │   └── sources/        # 各数据源适配器
│   ├── factors/            # 因子层
│   │   ├── base.py         # 基类 + dedup_latest
│   │   ├── registry.py     # 因子注册表
│   │   ├── formula/        # 公式因子 (5)
│   │   └── narrative/      # 叙事因子 (4)
│   ├── drift/              # 漂移检测层
│   │   ├── ic_tracker.py   # IC 追踪
│   │   ├── regime.py       # 市场状态识别
│   │   ├── report.py       # 漂移告警
│   │   └── daily_report.py # 日报生成器
│   └── mining/             # 进化层
│       ├── knowledge_base.py    # 领域知识库
│       ├── failure_analyzer.py  # 失败模式分析
│       ├── factor_mutator.py    # 因子变异
│       └── evolution.py         # 进化引擎 + 沙箱
├── tests/                  # 62 个测试（+7 live 网络测试）
├── pyproject.toml
└── CLAUDE.md
```

## 测试

```bash
# 单元测试（62 个，排除网络）
uv run pytest tests/ -v -m "not live"

# 含网络测试（需要 akshare 连通）
uv run pytest tests/ -v
```

## 日常使用

```bash
# 每日流程（推荐 15:40 后运行）
bash scripts/daily_run.sh

# 或手动分步
python -m cli.collect --today          # 采集
python -m cli.backtest --compute-today # 计算因子
python -m cli.drift --date $DATE       # 漂移检测
python -m cli.mine evolve              # 因子进化
python -m cli.report --date $DATE      # 日报
```

## 回填验证

尝试了 `--backfill 5`，因 WSL2 网络环境（akshare 接口不稳定）部分源拉取失败（daily_price、fund_flow、concept_mapping），但 zt_pool/zb_pool/strong_pool/lhb_detail/market_emotion 正常入库。非交易日的日期返回空数据属于正常行为。

完整回填建议在稳定网络环境下运行，或分多次执行。
