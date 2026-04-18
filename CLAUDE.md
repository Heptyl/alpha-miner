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
