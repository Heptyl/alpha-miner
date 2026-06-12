# Alpha Miner — 项目指南

## 项目概述
A股量化因子挖掘系统。基于行为金融学理论，从市场数据中挖掘、验证、进化短线交易因子。

## 环境
- **OS**: WSL2 Ubuntu (Linux)
- **Python**: 3.12+
- **包管理**: uv
- **数据库**: SQLite (data/alpha_miner.db)

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

## 常用命令
```bash
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

## 多Agent协作：Hermes(总指挥) + Claude Code(执行员)

### 角色分工

**Hermes（总指挥）** — 负责"做什么"
- 市场研判：每日盘前给出市场情绪判断 + 操作方向
- 策略决策：决定启用/禁用哪个策略，资金分配
- 风控红线：设定当日最大亏损、清仓阈值
- 异常裁决：极端行情下的应急决策
- **不碰代码**：只给策略意图和风控参数，不指定具体实现

**Claude Code（执行员）** — 负责"怎么做并且做对"
- 代码实现：按Hermes指令编写/修改策略代码
- 质量把控：代码审查、测试、确保无bug上线
- 数据验证：回测结果验证，确保无未来数据泄漏
- 执行反馈：向Hermes报告执行结果和异常
- **不碰决策**：不自行决定买卖方向、仓位大小、策略启用

### 协作规则

1. **代码质量一票否决** — Claude Code认为代码有风险时，可拒绝上线并说明理由
2. **变更需双向确认** — Hermes的指令Claude确认可执行，Claude的实现Hermes确认符合意图
3. **每笔交易可追溯** — 决策链路全程留痕：Hermes指令 → Claude实现 → 执行结果
4. **风控不妥协** — 无论Hermes如何要求，风控红线（止损/仓位限制）不可突破
5. **数据先行** — 任何策略调整必须先有回测数据支撑，不接受"感觉"式决策

### MCP 工具

| 工具 | 用途 |
|------|------|
| sqlite-tools | 直接查询 alpha_miner.db |
| code-understanding | 深度理解代码架构 |
| knowledge-graph | 图谱化持久记忆 |

### 自定义 Skills

| Skill | 用途 |
|-------|------|
| `/a-share-backtest-review` | 回测结果审查 |
| `/trading-risk-check` | 交易风控检查 |
| `/strategy-postmortem` | 策略复盘 |
