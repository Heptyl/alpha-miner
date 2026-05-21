# Alpha Miner — Claude Code 执行指令（WSL2）

> **环境**：Windows 11 + WSL2 (Ubuntu) + Claude Code
> **执行方式**：在 WSL2 终端中 `cd` 到项目目录，启动 `claude`，逐条发送指令
> **原则**：每步完成并测试通过后，再发下一步

---

## Step 0: 环境准备（你手动执行）

```bash
# === WSL2 终端 ===

# 创建项目目录
mkdir -p ~/projects/alpha-miner
cd ~/projects/alpha-miner

# 把三个 md 文件从 Windows 侧复制过来
cp /mnt/c/Users/41523/Downloads/factor-drift-system-plan.md .
cp /mnt/c/Users/41523/Downloads/factor-mining-v2.md .
# （本文件你自己参照用，不需要复制进去）

# 如果已有 Windows 侧跑过的项目，可以整体复制
# cp -r /mnt/c/Users/41523/Projects/alpha-miner/* .

# 确认工具链
python3 --version   # 需要 3.11+
pip3 --version
git --version

# 安装 uv（推荐，比 pip 快）
curl -LsSf https://astral.sh/uv/install.sh | sh
source ~/.bashrc

# 初始化 git
git init

# 启动 Claude Code
claude
```

以下所有 Step 的内容，是你在 `claude` 交互界面中发送的指令。

---

## Step 1: 项目骨架

```
请阅读当前目录下的 factor-drift-system-plan.md 和 factor-mining-v2.md，理解整个项目。

本项目运行在 WSL2 Ubuntu 环境。

现在只做一件事：创建项目骨架。
1. 按 factor-drift-system-plan.md 创建完整目录结构（所有文件夹和 __init__.py）
2. 额外创建 knowledge_base/ 目录（来自 factor-mining-v2.md）
3. 创建 CLAUDE.md（从计划中提取，注意：环境是 WSL2 Ubuntu，不是 Windows）
4. 创建 pyproject.toml（从计划提取依赖清单）
5. 创建 config/settings.yaml 模板（API key 用占位符 YOUR_KEY_HERE）
6. 创建 config/factors.yaml（从计划提取完整因子注册表）

不要写任何业务逻辑代码。完成后 tree 展示目录结构。
```

---

## Step 2: 数据库层

```
继续。实现数据库层。

创建 src/data/schema.sql：从计划提取全部建表和建索引语句。

创建 src/data/storage.py：
- Storage 类，接收 db_path 参数（默认 data/alpha_miner.db）
- init_db()：执行 schema.sql 建表
- query(table, as_of, where="", params=())：自动注入 WHERE snapshot_time < as_of
- query_range(table, as_of, lookback_days, date_col="trade_date")：查 as_of 前 N 日数据
- insert(table, df)：DataFrame 写入，自动添加 snapshot_time 列

创建 tests/test_storage.py：
- test_init_db：建表成功
- test_insert_adds_snapshot_time：插入后每行都有 snapshot_time
- test_query_time_isolation：插入两条数据（snapshot_time 分别为 T1 和 T2），用 as_of=T2 查询只返回 T1 的数据
- test_query_range：验证日期范围过滤

安装依赖：uv sync 或 pip install -e ".[dev]"
运行测试：pytest tests/test_storage.py -v
```

---

## Step 3: 因子基类 + 注册

```
继续。实现因子框架。

创建 src/factors/base.py：
- FutureDataError 异常类
- BaseFactor 抽象基类：name, factor_type, description, lookback_days
- 抽象方法 compute(universe: list[str], as_of: datetime, db: Storage) -> pd.Series
- validate_no_future(as_of, data, date_col) 方法

同时实现 factor-mining-v2.md 中的两个新基类：
- ConditionalFactor(BaseFactor)：多条件组合因子，含 Condition 类
- CrossFactor(BaseFactor)：两个已有因子的交叉

创建 src/factors/registry.py：
- FactorRegistry 类
- load_from_yaml(yaml_path)：读取 config/factors.yaml
- get_factor(name) -> BaseFactor
- list_factors() -> list[str]

创建 tests/test_time_isolation.py：
- MockFactor 测试 as_of 隔离
- validate_no_future 异常测试
- ConditionalFactor 基础测试

运行 pytest tests/test_time_isolation.py -v
```

---

## Step 4: 数据采集 — 稳定接口

```
继续。实现数据采集器，先做 4 个最稳定的数据源。

创建 src/data/collector.py：统一调度器，逐个调用数据源，单源失败不影响整体。

创建 src/data/sources/akshare_price.py：日K线（stock_zh_a_daily）
创建 src/data/sources/akshare_zt_pool.py：涨停池+炸板池+强势股（3个接口合一个文件）
创建 src/data/sources/akshare_lhb.py：龙虎榜（stock_lhb_detail_em，参数用 start_date/end_date）
创建 src/data/sources/akshare_fund_flow.py：资金流向

每个源统一接口：fetch(trade_date) -> DataFrame, save(df, db)
网络超时重试 3 次，间隔 2 秒。非交易日返回空 DataFrame。

创建 tests/test_collect_live.py（标记 @pytest.mark.live，默认跳过）：
- 用最近一个交易日测试每个源能拉到数据

实际运行一次：拉取最近交易日数据，确认入库成功。
```

---

## Step 5: 数据采集 — 剩余数据源 + 市场情绪

```
继续。实现剩余数据源。

创建 src/data/sources/akshare_concept.py：
- 板块概念映射（stock_board_concept_name_em）
- 不稳定接口，做好 fallback（异常时读缓存表旧数据）

创建 src/data/sources/akshare_news.py：
- stock_news_em，用 snownlp 算 sentiment_score
- news_id 用 title+publish_time 的 hash 去重

在 collector.py 中补充聚合逻辑：
- 采集完成后自动生成 market_emotion 记录（涨停数、跌停数、最高板、情绪级别）
- 自动聚合 concept_daily（每个概念当日涨停数、龙头等）

测试：完整运行 collect_date 采集一天数据，检查所有表都有数据。
```

---

## Step 6: CLI 采集命令

```
继续。实现 CLI 入口。

创建 cli/collect.py：
- 用 click 框架
- python -m cli.collect --date 2024-06-15
- python -m cli.collect --backfill 60
- python -m cli.collect --today
- 用 rich 美化输出

创建 cli/__main__.py。

测试：python -m cli.collect --backfill 5

Phase 1 完成。运行 pytest tests/ -v 确认全部通过。
```

---

## Step 7: 公式因子（5 个）

```
继续。进入 Phase 2。实现 5 个公式化量价因子。

每个继承 BaseFactor，compute 开头调用 validate_no_future()，只通过 db.query/query_range 取数据。

1. src/factors/formula/zt_ratio.py — 涨停/(涨停+跌停)，市场级因子
2. src/factors/formula/board_height.py — 连板高度加权分
3. src/factors/formula/seal_success.py — 涨停/(涨停+炸板)
4. src/factors/formula/turnover_quantile.py — 换手率全市场分位数，个股级
5. src/factors/formula/fund_flow_diverge.py — 超大单与大单方向背离

每个写完立即写对应测试（tests/test_factors.py），用固定数据验证。
全部完成运行 pytest tests/test_factors.py -v。
```

---

## Step 8: 叙事因子（4 个）

```
继续。实现 4 个叙事因子。

1. src/factors/narrative/theme_lifecycle.py — 题材连续涨停天数→生命周期阶段分数
2. src/factors/narrative/narrative_velocity.py — 新闻数量 3 日变化率
3. src/factors/narrative/theme_crowding.py — 题材涨停占比，拥挤度越高分数越低
4. src/factors/narrative/leader_clarity.py — 龙头成交额/第二名成交额

每个写完测试。全部完成后：
- pytest tests/ -v 全量
- 测试 FactorRegistry 能加载全部 9 个因子

Phase 2 完成。
```

---

## Step 9: IC 追踪器

```
继续。进入 Phase 3。实现因子有效性追踪。

创建 src/drift/ic_tracker.py — ICTracker 类：
- compute_ic_series(factor_name, start_date, end_date, forward_days=1, window=20)
  每日截面 Spearman(因子值, 未来收益) → 滚动 IC/ICIR/胜率/盈亏比
- current_status(factor_name, window=20)
  返回 latest_ic, ic_20d_avg, icir, trend, status

创建 tests/test_drift.py 用模拟数据测试。运行测试。
```

---

## Step 10: CUSUM + Regime

```
继续。

创建 src/drift/cusum.py — CUSUM 变点检测。
创建 src/drift/regime.py — 市场状态识别（board_rally/theme_rotation/low_volume/broad_move）。

补充 tests/test_drift.py。运行全部测试。
```

---

## Step 11: 漂移报告 CLI

```
继续。

创建 src/drift/report.py — 汇总所有因子状态 + 漂移事件。
创建 cli/drift.py — python -m cli.drift --date 2024-06-15（rich 格式化输出）
创建 cli/backtest.py：
- python -m cli.backtest --compute-today（计算今日所有因子值）
- python -m cli.backtest --factor zt_dt_ratio --start 2024-01-01 --end 2024-06-30（单因子回测）

Phase 3 完成。pytest tests/ -v 全量通过。
```

---

## Step 12: 知识库 + 进化基础

```
继续。进入 Phase 4（因子挖掘 v2 方案）。

请阅读 factor-mining-v2.md。

Step A: 创建 knowledge_base/theories.yaml
- 从 factor-mining-v2.md 提取完整的 theories 定义
- 包含 4 个理论（info_cascade, three_shift, theme_lifecycle, emotion_regime）
- 每个理论含 2-3 个 testable_predictions
- 总计约 12 个种子假说

Step B: 创建 src/mining/failure_analyzer.py
- FailureAnalyzer 类，分析因子回测失败原因
- 6 种失败模式的诊断逻辑（too_strict, too_loose, no_signal, reversed, noisy, redundant）
- 每种模式给出结构化建议

Step C: 创建 src/mining/mutator.py
- FactorMutator 类，根据失败原因做定向变异
- 变异操作：阈值调整、条件增删、方向反转、regime 过滤、因子杂交

写测试验证分析器和变异器的逻辑。运行测试。
```

---

## Step 13: 进化引擎 + LLM 集成

```
继续。

创建 src/mining/evolution.py — EvolutionEngine 类：
- run(generations, population_size)：完整进化循环
- _generate_from_knowledge()：从 theories.yaml 生成第一代
- _construct_factor(candidate)：用 Anthropic API 将假说翻译为代码（LLM 做工程师，不做研究员）
- _evaluate(code)：沙箱执行 + IC 回测
- _crossover(accepted)：有效因子杂交

创建 src/mining/prompts/ 目录下的 prompt 模板：
- construct.md：工程师角色（temperature=0.1）
- analyze.md：分析师角色（temperature=0.5）
- explore.md：研究员角色（temperature=0.9，仅知识库耗尽后用）

创建 src/mining/sandbox.py：
- 子进程执行生成的因子代码
- 60 秒超时
- 捕获异常返回错误信息

创建 tests/test_mining.py：
- mock Anthropic API 测试整个进化流程
- 测试 EvalAgent 验收标准
- 测试 mining_log 写入

运行测试。
```

---

## Step 14: 挖掘 CLI

```
继续。

创建 cli/mine.py：
- python -m cli.mine evolve --generations 10 --population 10
  完整进化循环
- python -m cli.mine test-seeds
  只测试知识库中所有种子假说（不进化，快速验证哪些理论成立）
- python -m cli.mine mutate --factor cascade_momentum --rounds 5
  对指定因子做变异探索
- python -m cli.mine history
  查看历史挖掘记录
- python -m cli.mine lineage --factor xxx
  查看因子家谱

Phase 4 完成。运行全部测试。
```

---

## Step 15: 日报 + 每日流程

```
继续。进入 Phase 5。

创建日报生成器（扩展 src/drift/report.py）：
- 市场概况
- 有效因子排名
- 漂移预警
- 今日挖掘结果
- 明日候选标的（有效因子加权打分，regime 调权，失效因子不参与）
- 系统状态

创建 cli/report.py：
- python -m cli.report --date 2024-06-15
- 终端 rich 输出 + 保存到 reports/YYYY-MM-DD.txt

创建 scripts/daily_run.sh：
#!/bin/bash
set -e
DATE=$(date +%Y-%m-%d)
echo "===== Alpha Miner Daily Run: $DATE ====="
python -m cli.collect --today
python -m cli.backtest --compute-today
python -m cli.drift --date $DATE
python -m cli.mine evolve --generations 3 --population 5
python -m cli.report --date $DATE

chmod +x scripts/daily_run.sh

Phase 5 完成。
```

---

## Step 16: 数据回填 + 首次完整验证

```
这是最关键的一步。之前所有步骤只是搭框架，现在要灌真实数据验证。

1. 回填 120 个交易日数据：
   python -m cli.collect --backfill 120
   （这会比较慢，大约 10-30 分钟，耐心等）

2. 对 9 个初始因子计算 120 天的历史因子值：
   python -m cli.backtest --compute-all --start 2025-11-01 --end 2026-04-17
   （每天用 T-1 数据算 T 日因子值，严格时间隔离）

3. 计算每个因子的 IC 序列：
   python -m cli.drift --date 2026-04-17
   输出每个因子的 IC均值、ICIR、胜率、盈亏比

4. 运行种子假说测试：
   python -m cli.mine test-seeds
   看知识库里 12 个假说哪些在历史数据上成立

5. 输出完整报告，列出：
   - 哪些因子有效（IC>0.03）
   - 哪些假说被验证
   - 哪些假说被否定
   - 建议下一步重点探索方向
```

---

## Step 17: 首次进化运行

```
基于 Step 16 的结果，开始真正的因子进化。

python -m cli.mine evolve --generations 10 --population 10

运行完成后输出：
1. 共评估了多少因子
2. 通过验收的因子列表（名称、IC、ICIR、胜率、来源理论）
3. 因子之间的相关性矩阵
4. 失败因子的主要失败模式分布
5. 建议：哪些理论方向值得继续深挖

最后生成一份完整日报：python -m cli.report --date 今天
```

---

## 后续：持续运行

每次新开 Claude Code 会话，发送：

```
cd ~/projects/alpha-miner && claude
```

然后：

```
请阅读 CLAUDE.md。项目已完成 Phase 1-5。
查看 mining_log 表了解之前的挖掘历史。
运行 python -m cli.mine evolve --generations 10 --population 10 继续因子进化。
```

或者设置 crontab 每日自动执行：

```bash
# 编辑 crontab
crontab -e

# 添加：每个交易日 15:40 自动运行（北京时间，WSL2 可能需要调时区）
40 15 * * 1-5 cd ~/projects/alpha-miner && bash scripts/daily_run.sh >> logs/daily_$(date +\%Y\%m\%d).log 2>&1
```

---

## 与 Windows 版的主要差异

| 项目 | Windows 版 | WSL2 版 |
|------|-----------|---------|
| 路径 | C:\Projects\alpha-miner | ~/projects/alpha-miner |
| Shell | PowerShell | bash |
| 定时任务 | daily_run.bat + 任务计划 | daily_run.sh + crontab |
| Python | python | python3（或 python 如果已配别名） |
| 包管理 | pip | uv（推荐）或 pip |
| 文件复制 | copy | cp /mnt/c/... |
