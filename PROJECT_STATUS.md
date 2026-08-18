# Alpha Miner 项目状态看板

> 面向 PM / 项目负责人。快照日期：2026-08-18；运行与数据证据截至 2026-08-17。唯一架构源规范见 [ARCHITECTURE.md](ARCHITECTURE.md)。S4-C、S3-B2a 与 S3-B2b 已完成 code-only 冻结并推送到 `origin/main`；以下不代表 X 盘、服务器、计划任务或数据库已经部署。

## 一句话结论

产品骨架已收敛为免费数据 → PIT 离线实验 → PAPER 玩法卡 → `python -m cli` 只读展示；market/USER 投影与独立 append-only 研究账本采用物理隔离。该 B 方案仍未部署或接入正式引擎，因此当前没有正式发现或已证明统计优势。

## Git 与发布状态

| 项目 | 当前事实 |
|---|---|
| 本地 HEAD | 本地 `main` 与 `origin/main` 一致；精确哈希用 `git rev-parse HEAD` 与 `git rev-parse origin/main` 核验。 |
| 远端基线 | 本次 code-only 提交与此前 8 个本地提交均已随 `main` 推送；已安装的 `AlphaMiner-LimitUpHistory` 计划任务 runner 仍运行旧部署基线 `a182e67`。 |
| 代码冻结 | 本批代码冻结状态以 `git log`、`git status`、HEAD 与 `origin/main` 实时核验；尚未 deploy、同步 X 盘/服务器或触碰真实数据库。 |
| 真实运行库 | 已有 1 张 `three_to_four_reseal / 2026-08-17` 卡，状态 `PLANNED / NOT_ADMITTED`；本轮文档与精简工作未写数据库。 |

## 本地冻结批次

| 批次 | 已有事实 | 当前边界 |
|---|---|---|
| B1 竞价/开盘快照 | 9:25 与 09:31 免费前向采集基础已本地冻结，候选按精确前一交易日冻结。 | 代码已随 `main` 推送；未安装新任务或部署。 |
| B2 五分钟 RAW | 两表式候选 checkpoint 与 5 分钟历史补采已本地冻结。 | `RETRO_BACKFILL` 不能冒充当时可见；未形成玩法或胜率；代码已推送但未部署。 |
| PIT 研究诚信 | `PointInTimeView` 与通用/涨停 development-only 语义已推送；本批新增独立追加式候选/血统/证据原语，并绑定不可变 market hash 快照。 | 原语尚未接 EvolutionEngine/Backtester/LimitUp，也未实现正式 holdout evaluator，只能称 `DEVELOPMENT_CANDIDATE`。 |
| H1 题材新进入者 | `theme_new_entrant_diffusion_v1` 的前向 PAPER 构建与审计约束已本地冻结。 | development 仅 12 个独立收益日；D+3 OPEN 成本后均值 `+0.5530%`，95% CI `[-0.9914%, 2.0835%]`，Holm 不显著，不能称发现；代码已推送但未部署。 |
| S2-A / S3-B1 精简 | 10 个零引用旧旁路删除，共 3031 LOC；PM 已接受删除安全并已本地冻结。 | 已随 `main` 推送；删除安全不等于全树静态检查全绿。 |
| S2-B USER 收口 | 默认 `python -m cli` 只读 play；帮助只公开 play、`zt status`、持仓 report 三条路径；15 项定向测试通过，真实 PowerShell 中文捕获通过，数据库 SHA 前后不变。 | 已随 `main` 推送但未部署；USER 角色日常只使用默认入口。 |
| S3-B2a 旧产品面退役 | 删除 7 个旧 CLI/dashboard/cron 文件共 1708 LOC，并从根路由移除 `recommend`、`signal`、`strategy`、`query`；`zt collect` 停止新建 three_to_four，但仍先结算历史卡并继续结算/生成 H1。 | 已完成 code-only 推送但未部署或同步 X 盘；持仓 report、底层 RecommendEngine/SignalEngine/StrategyEvolver、旧卡历史和结算均保留。 |
| S3-B2b 远程旧链收缩 | 本地删除 5 个旧 daily/drift/replay 与每日/小时包装，根路由统一退役 `daily/backtest/drift/script/replay`；持仓 report 与底层回测、漂移、剧本、复盘正确性组件保留。远程 allowlist 删除 collect、limit-up evolve、daily，唯一通用 evolve 明示为 `DEVELOPMENT_ONLY`。 | 已完成 code-only 推送但未执行 remote action、同步 X 盘/服务器、修改调度或真实库；定向 `133 passed`，全仓离线 `574 passed / 7 deselected`。 |
| S4 文档收口 | S4-A 只读审计得到 32 个 tracked Markdown、7552 行；S4-B 已将 14 份旧文档的 6597 行迁移/删除，主真相收敛到六份文档。 | PM 已验收并随 `main` 推送；未部署。 |

## 最近联合回归

当前 B 方案工作树仅执行一次 `uv run pytest -q -m "not live"`：exit 0，`614 passed / 7 deselected / 5 warnings`；pytest 报告耗时 618.12 秒，外层计时 631 秒。本批账本、激活、Storage、PIT 与架构定向组合为 `68 passed`，相关 Ruff 通过。

- 5 个 warnings：SnowNLP 的 `codecs.open()` 弃用警告 2 个；`src/drift/ic_tracker.py` 的 `Mean of empty slice` 3 个。
- 全树 Ruff 仍为 exit 1、289 个既有问题；其中 248 个可自动修复，另有 23 个可选 unsafe 修复，本轮未修改。
- 因此可以说“全仓离线 pytest 通过”，不能说“全部静态检查全绿”。

## 下一步

1. 独立账本尚未接入 EvolutionEngine/Backtester/LimitUp；接线与正式 holdout evaluator 必须另立任务，当前不得称正式发现。
2. 冻结状态以 HEAD/origin 核验；部署、同步 X 盘/服务器、修改计划任务或数据库仍需独立执行，本批没有触碰真实运行库。
