# Alpha Miner — 项目指南

## 可选角色入口

直接启动的普通 Claude 会话不要求选择角色，可以按用户要求正常处理工程任务。只有通过
`.\scripts\agent.ps1 <pm|rd|user> -Cli claude` 或显式选择 `alpha-miner-pm`、
`alpha-miner-rd`、`alpha-miner-user` Agent 时，才读取并执行 `AGENT_ROLES.md`；角色会话内不得切换或临时越权。

## 项目概述
A股量化因子挖掘系统。基于行为金融学理论，从市场数据中挖掘、验证、进化短线交易因子。

## 环境
- **OS**: Windows 11（直接用 uv + .venv 跑；CLAUDE.md 旧版写的 WSL2 已过时）
- **Python**: >= 3.11
- **包管理**: uv（Windows 上 `uv sync` 需先把 TEMP 指到项目内目录，规避 DLP 拦截）
- **数据库**: SQLite (data/alpha_miner.db)
- **编码**: Windows 终端跑 pytest/CLI 需 `$env:PYTHONUTF8='1'`

## 项目结构
```
src/
  data/          — 数据采集与存储
    sources/     — 各数据源适配器 (akshare)
  factors/       — 因子计算
    formula/     — 公式因子 (纯数学变换)
    narrative/   — 叙事因子 (文本/情绪驱动)
  drift/         — 因子漂移检测与IC追踪
  mining/        — 因子进化引擎
    prompts/     — LLM prompt模板
  narrative/     — 叙事引擎 (新闻分类/剧本/复盘)
  strategy/      — 策略子系统 (回测/进化/持久化/推荐/信号/风控)
  pipeline/      — IC管线 (批量计算+持久化)
cli/             — 命令行入口
config/          — 配置文件
knowledge_base/  — 理论知识库
tests/           — 测试
```

## 核心约束
1. **时间隔离**: 所有因子计算必须通过 `db.query(as_of=...)` 取数据，确保不使用未来数据
2. **单源失败容忍**: 数据采集器中某个源失败不影响其他源
3. **因子验收标准**: IC > 0.03, ICIR > 0.5, 胜率 > 55%, 盈亏比 > 1.2

## 数据库表
- `daily_price` — 日K线 (stock_zh_a_daily)
- `zt_pool` — 涨停池
- `zb_pool` — 炸板池
- `strong_pool` — 强势股
- `lhb_detail` — 龙虎榜明细
- `fund_flow` — 资金流向
- `concept_mapping` — 板块概念映射
- `concept_daily` — 概念每日聚合
- `news` — 新闻+情绪
- `market_emotion` — 市场情绪指标
- `factor_values` — 因子计算结果
- `ic_series` — IC时序追踪
- `drift_events` — 漂移事件记录
- `regime_state` — 市场状态
- `mining_log` — 挖掘日志
- `market_scripts` — 市场剧本
- `replay_log` — 复盘记录
- `strategy_defs` — 策略定义
- `strategy_reports` — 回测报告
- `strategy_trades` — 交易记录

## 常用命令
```bash
# 主控制台（推荐入口：Web UI，免手动敲命令；双击 dashboard.bat 亦可）
uv run python scripts/dashboard.py --open

# 每日完整流程（跨平台，含第8步审视简报）
uv run python -m cli daily

# 安装依赖
uv sync

# 运行测试
pytest tests/ -v

# 数据采集
python -m cli.collect --today
python -m cli.collect --backfill 60

# 因子回测
python -m cli.backtest --compute-today

# 漂移报告
python -m cli.drift --date 2024-06-15

# 因子进化
python -m cli.mine evolve --generations 10 --population 10

# 日报
python -m cli.report --date 2024-06-15
```

## 开发原则
- 每个因子必须调用 `validate_no_future()` 检查
- 数据只通过 Storage 类的 `query/query_range` 方法获取
- 网络请求统一重试3次，间隔2秒
- 非交易日返回空DataFrame
- 所有CLI用click框架，rich美化输出
- **因子中文展示统一走 `src/factors/naming.py`**（映射表 `config/factor_aliases.yaml`：
  cn/desc/detail/note），任何 UI/简报/推送不得硬编码因子中文名

## Harness Plugin（推荐）

项目已集成 [claude-code-harness](https://github.com/Chachamaru127/claude-code-harness) 插件，
提供 Plan→Work→Review 工作流、自动代码审查、Plans.md 任务管理等功能。

### 首次安装（新开发者必做）

```bash
# 1. 添加 marketplace 并安装插件
claude plugin marketplace add Chachamaru127/claude-code-harness
claude plugin install claude-code-harness@claude-code-harness-marketplace

# 2. 重新加载插件
/reload-plugins
```

### 常用 Harness 指令

| 指令 | 用途 |
|------|------|
| `/harness-plan` | 创建任务到 Plans.md |
| `/harness-work` | 从 Plans.md 取任务执行 |
| `/harness-review` | 代码审查 |
| `/harness-sync` | 检查 Plans.md 与实现的对齐 |
| `/breezing` | 并行团队执行多个任务 |

### 项目级配置

`.claude/settings.json` 已配置好插件启用和权限规则，提交到 git。
其他开发者安装插件后无需额外配置即可使用。
