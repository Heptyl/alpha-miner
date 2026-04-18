# Alpha Miner

基于行为金融学的 A 股超短线因子挖掘框架。

## 架构

```
alpha-miner/
├── cli/                    # CLI 命令入口
│   ├── collect.py          #   数据采集 (python -m cli.collect)
│   └── drift.py            #   漂移报告 (python -m cli.drift)
├── src/
│   ├── data/               # 数据层
│   │   ├── schema.sql      #   SQLite 表结构 (20+ 表)
│   │   ├── storage.py      #   Storage 类: 时间隔离查询、快照管理
│   │   └── collectors/     #   采集器: daily_price / zt_pool / lhb / fund_flow / concept / news
│   ├── factors/            # 因子库
│   │   ├── base.py         #   BaseFactor / ConditionalFactor / CrossFactor
│   │   ├── registry.py     #   FactorRegistry 自动注册
│   │   ├── formula/        #   公式因子 (5)
│   │   │   ├── zt_ratio.py         #  涨停/跌停比率 (市场级)
│   │   │   ├── consecutive_board.py # 连板数 (股票级)
│   │   │   ├── main_flow_intensity.py # 主力净流入强度
│   │   │   ├── turnover_rank.py    #  换手率百分位排名
│   │   │   └── lhb_institution.py  #  龙虎榜机构净买入
│   │   └── narrative/      #   叙事因子 (4)
│   │       ├── theme_lifecycle.py   # 题材生命周期 (萌芽→爆发→衰退)
│   │       ├── narrative_velocity.py # 新闻数量 3 日变化率
│   │       ├── theme_crowding.py    # 题材拥挤度 (反拥挤)
│   │       └── leader_clarity.py    # 龙头清晰度
│   └── drift/              # 漂移检测
│       ├── ic_tracker.py   #   IC 追踪: Spearman IC / ICIR / 胜率 / 盈亏比
│       ├── cusum.py        #   CUSUM 变点检测 (递归)
│       ├── regime.py       #   市场状态: board_rally / theme_rotation / low_volume / broad_move
│       └── report.py       #   漂移报告汇总
├── tests/                  # 31 tests, 全绿
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

### 漂移检测 (Drift)

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

## Quick Start

```bash
# 安装
uv sync

# 跑测试
uv run pytest tests/ -v -m "not live"

# 采集数据
uv run python -m cli.collect --today
uv run python -m cli.collect --backfill --start 2024-01-01

# 漂移报告
uv run python -m cli.drift --date 2024-06-15
```

## 待完成

- [ ] Step 11: 回测 CLI (单因子回测 + compute-today)
- [ ] Step 12: 知识库 + 进化基础 (factor-mining-v2)
- [ ] Step 13: 进化引擎 + LLM 集成
- [ ] Step 14: 挖掘 CLI
- [ ] Step 15: 日报 + 每日流程

## License

MIT
