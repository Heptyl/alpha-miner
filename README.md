# Alpha Miner

基于行为金融学的 A 股超短线因子挖掘系统。

> 完整文档见 [DOCS.md](DOCS.md)

## 架构

```
alpha-miner/
├── cli/                    # CLI 入口 (python -m cli <command>)
│   ├── collect.py          #   数据采集
│   ├── report.py           #   日报 + 盘后简报 + 市场剧本
│   ├── mine.py             #   因子进化挖掘
│   ├── drift.py            #   漂移检测
│   ├── backtest.py         #   单因子回测
│   ├── replay.py           #   复盘引擎
│   └── strategy.py         #   策略管理 (list/backtest/evolve/scan)
├── src/
│   ├── data/               # 数据层 (Storage + 6 个 akshare 采集器)
│   ├── factors/            # 因子库 (5 公式 + 4 叙事)
│   ├── narrative/          # 叙事引擎 (新闻分类/剧本/复盘)
│   ├── drift/              # 漂移检测 + 决策输出
│   ├── mining/             # 进化引擎 (假说→代码→沙箱→IC 验收)
│   ├── strategy/           # 策略子系统 (回测/进化/持久化)
│   └── pipeline/           # IC 管线 (批量计算 + 持久化)
├── factors/                # 进化产出的因子代码 (6 个已验收)
├── knowledge_base/         # theories.yaml (12 假说) + strategies.yaml (5 策略)
├── config/                 # factors.yaml + settings.yaml
├── scripts/                # daily_run.sh + hourly_mine.sh
├── tests/                  # 261 tests
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

## 进化引擎

```
知识库 (theories.yaml, 12 假说)
    ↓ LLM 探索假说
假说配置 → 代码翻译 → 沙箱执行 (BacktestStorage, trade_date 隔离)
    ↓ IC 验收通过 → 因子入库
    ↓ 失败 → 诊断 → 变异 → 重试
```

LLM 接口：Z.AI Anthropic 兼容端点。沙箱子进程隔离，Prompt 三阶段：explore → construct → analyze。已产出 6 个因子存于 `factors/` 目录。

## 策略子系统

5 个预置策略，定义在 `knowledge_base/strategies.yaml`：

| 策略 | 来源假说 |
|------|---------|
| 首板打板_龙头确认 | info_cascade + theme_lifecycle |
| 题材发酵_跟风低吸 | theme_lifecycle |
| 情绪冰点_反弹首板 | emotion_regime |
| 三班组回避 | three_shift |
| 连板接力_情绪共振 | herd_effect |

回测引擎支持 T+1 约束、多仓位管理、regime 分组统计。进化器支持网格搜索多目标优化。

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

| 情绪 | 建议 | 仓位 |
|------|------|------|
| 极弱 | 休息 | 0% |
| 弱 | 谨慎 | 20% |
| 中性 | 可操作 | 40% |
| 偏强 | 积极 | 60% |
| 强 | 重仓 | 80% |

## 漂移检测

| 模块 | 功能 |
|------|------|
| IC Tracker | 滚动 Spearman IC → ICIR / 胜率 / 盈亏比 / 趋势 |
| CUSUM | 递归变点检测，因子 IC 结构性断裂 |
| Regime | 市场状态 (连板潮 / 题材轮动 / 地量 / 普涨跌 / 正常) |

## 数据库 (20 张表)

daily_price, zt_pool, zb_pool, strong_pool, lhb_detail, fund_flow, concept_mapping, concept_daily, news, market_emotion, factor_values, ic_series, drift_events, regime_state, mining_log, market_scripts, replay_log, strategy_defs, strategy_reports, strategy_trades

## 测试 (261 tests)

硬断言测试 47 个：手工构造数据集 + 精确期望值，覆盖叙事因子 (16) / IC 端到端 (7) / 进化引擎 (13) / 模板因子 (11)。

已修复生产 Bug：validate_no_future 误报、ICIR 除零、回测 snapshot_time 隔离失败、涨停因子字段名错误。

## 技术要点

- **时间隔离**：Storage 层 snapshot_time 隔离；回测用 BacktestStorage + trade_date
- **因子注册**：FactorRegistry 自动扫描 BaseFactor 子类
- **情感引擎**：金融关键词规则引擎替代 snownlp
- **新闻分类**：规则引擎 + LLM fallback，高置信度跳过 LLM
- **LLM 可选**：llm_client=None 走纯规则路径，系统照常运行

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
uv run python -m cli report --date $DATE          # 5. 日报
uv run python -m cli script --date $DATE --save   # 6. 剧本
uv run python -m cli replay --date $DATE --save   # 7. 复盘

uv run python -m cli report --brief               # 盘后简报
```

## License

MIT
