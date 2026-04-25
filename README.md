# Alpha Miner

基于行为金融学的 A 股超短线因子挖掘系统。

> 完整文档见 [DOCS.md](DOCS.md)

## 架构

```
alpha-miner/
├── cli/                    # CLI 入口 (python -m cli <command>)
│   ├── collect.py          #   数据采集
│   ├── report.py           #   日报 + 盘后简报 + 市场剧本
│   ├── mine.py             #   因子进化挖掘 + 手术台 CLI
│   ├── drift.py            #   漂移检测
│   ├── backtest.py         #   单因子回测
│   ├── replay.py           #   复盘引擎
│   └── strategy.py         #   策略管理 (list/backtest/evolve/scan)
├── src/
│   ├── data/               # 数据层 (Storage + 6 个 akshare 采集器)
│   ├── factors/            # 因子库 (5 公式 + 4 叙事)
│   ├── narrative/          # 叙事引擎 (新闻分类/剧本/复盘)
│   ├── drift/              #   漂移检测 + 决策输出 (含动态 Regime 权重)
│   ├── mining/             #   进化引擎 v2 (手术台+真实IC+定向变异+候选池)
│   ├── strategy/           #   策略子系统 (回测/进化/持久化)
│   └── pipeline/           #   IC 管线 (批量计算 + 持久化)
├── factors/                # 进化产出的因子代码 (6 个已验收)
├── knowledge_base/         # theories.yaml (12 假说) + strategies.yaml (5 策略)
├── config/                 # factors.yaml + settings.yaml
├── scripts/                # daily_run.sh + hourly_mine.sh
├── tests/                  # 288 tests
└── pyproject.toml          # uv 项目配置 (Python >= 3.11)
```

## 因子体系

### 公式因子 (5)

| 因子 | 级别 | 逻辑 |
|------|------|------|
| zt_ratio | 市场 | 涨停/(涨停+跌停)，情绪方向 |
| consecutive_board | 股票 | 连板天数 × (1 - 开板率) |
| main_flow_intensity | 股票 | 主力净流入 / 成交额 |
| turnover_rank | 股票 | 换手率百分位排名 |
| lhb_institution | 股票 | 龙虎榜机构净买入额排名 |

### 叙事因子 (4)

| 因子 | 级别 | 逻辑 |
|------|------|------|
| theme_lifecycle | 股票 | 题材涨停阶段 → 生命周期分数 (萌芽→爆发→衰退) |
| narrative_velocity | 股票 | 新闻类型加权 3 日变化率 (7 类) |
| theme_crowding | 股票 | 1 - max(题材涨停占比 × 5)，反拥挤 |
| leader_clarity | 股票 | 龙头成交额 / 第二名成交额 |

验收标准：IC > 0.03, ICIR > 0.5, 胜率 > 55%, 盈亏比 > 1.2

## 进化引擎 v2

```
知识库种子 (12 假说)
    ↓ LLM/模板 → 代码翻译
因子代码 → 真实回测 (FactorBacktester, 逐日 Spearman IC)
    ↓ 带 regime/zt_count 的 ic_series
因子手术台 (三分段分析 + 黄金窗口 + 诊断)
    ↓ 验收通过 → 候选池 (5天观察期)
    ↓ 失败 → 定向变异 (手术台驱动) → 重试
```

核心升级：
- **真实回测器**：替换假沙箱 IC，逐日计算 Spearman IC，带 regime/zt_count 上下文
- **因子手术台**：regime/情绪/时间三分段 IC 分析 + 黄金窗口检测 + 5种诊断
- **定向变异**：基于手术台诊断做 regime 过滤/情绪过滤/方向反转/窗口调整
- **候选池**：5 天观察期，连续达标才入库
- **历史反馈**：失败≥3次的假说自动跳过
- **动态权重**：Regime 权重从历史 IC 动态计算，硬编码值作 fallback

CLI 手术台：

```bash
python -m cli mine surgery --factor consecutive_board --days 60
```

## 叙事引擎

### 新闻分类器 (7 类)

| 类型 | 权重 | 说明 |
|------|------|------|
| theme_ignite | 3.0 | 题材点燃 (政策/技术突破) |
| catalyst_real | 2.0 | 实质性催化剂 (业绩/中标) |
| theme_ferment | 1.5 | 题材发酵 |
| catalyst_expect | 1.0 | 预期性催化剂 |
| good_realize | -0.5 | 利好兑现 (见光死) |
| negative | -2.0 | 负面事件 |
| noise | 0.0 | 无关噪音 |

### 市场剧本 + 复盘

每日生成剧本 (市场快照→题材判定→明日策略→风险提示)，次日复盘验证 (regime 准确率/题材命中/异常检测)。

```bash
python -m cli script --date $DATE [--llm] --save    # 剧本
python -m cli replay --date $DATE [--llm] --save    # 复盘
python -m cli replay --stats                         # 准确率统计
```

## 策略子系统

5 个预置策略，定义在 `knowledge_base/strategies.yaml`：

| 策略 | 来源假说 |
|------|---------|
| 首板打板_龙头确认 | info_cascade + theme_lifecycle |
| 题材发酵_跟风低吸 | theme_lifecycle |
| 情绪冰点_反弹首板 | emotion_regime |
| 三班组回避 | three_shift |
| 连板接力_情绪共振 | herd_effect |

```bash
python -m cli strategy list                                          # 列出策略
python -m cli strategy backtest --name "首板打板_龙头确认" --start 2026-01-01 --end 2026-03-31
python -m cli strategy evolve --name "首板打板_龙头确认" --start 2026-01-01 --objective sharpe
python -m cli strategy scan --date 2026-04-14                        # 当日信号扫描
```

## 盘后决策简报 (DailyBrief)

`python -m cli report --brief` 生成三大交付物：

1. **市场温度计** — Regime 识别 + 情绪 5 级判定 + 建议仓位 (极弱 0% → 强 80%)
2. **候选决策卡片** — Top N 评分 + 因子贡献进度条 + 反向视角
3. **持仓风险预警** — 三班组检测 / 资金流背离 / 换手率安全线 / 题材拥挤度

## 漂移检测

| 模块 | 功能 |
|------|------|
| IC Tracker | 滚动 Spearman IC → ICIR / 胜率 / 盈亏比 / 趋势 |
| CUSUM | 递归变点检测，因子 IC 结构性断裂 |
| Regime | 市场状态 (连板潮 / 题材轮动 / 地量 / 普涨跌 / 正常) |

## 测试 (288 tests)

硬断言测试 47 个 + 手术台测试 24 个 + 回测器测试 4 个 + 进化完整性测试 5 个。覆盖叙事因子/IC端到端/进化引擎/模板因子/手术台诊断/定向变异。

## Quick Start

```bash
uv sync                                          # 安装
uv run pytest tests/ -v --ignore=tests/test_collect_live.py  # 测试
bash scripts/daily_run.sh                        # 每日 7 步完整流程 (交易日 15:40 后)

# 分步执行
uv run python -m cli collect --today             # 1. 采集
uv run python -m cli backtest --compute-today     # 2. 因子计算
uv run python -m cli drift --date $DATE           # 3. 漂移检测
uv run python -m cli mine evolve                  # 4. 因子进化
uv run python -m cli mine surgery --factor X --days 60  # 5. 手术台
uv run python -m cli report --date $DATE          # 6. 日报
uv run python -m cli script --date $DATE --save   # 7. 剧本

uv run python -m cli report --brief               # 盘后简报
```

## License

MIT
