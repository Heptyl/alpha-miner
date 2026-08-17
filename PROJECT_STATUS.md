# Alpha Miner 项目状态看板

> 面向 PM / 项目负责人。快照日期：2026-08-18；运行与数据证据截至 2026-08-17。唯一架构源规范见 [ARCHITECTURE.md](ARCHITECTURE.md)。S4-C、S3-B2a 与 S3-B2b 已完成 code-only 冻结并推送到 `origin/main`；以下不代表 X 盘、服务器、计划任务或数据库已经部署。

## 一句话结论

产品骨架已收敛为免费数据 → PIT 离线实验 → PAPER 玩法卡 → `python -m cli` 只读展示；B1/B2、PIT、H1、精简与 USER/真相源收口现已随 `main` 完成 code-only 推送。当前没有已证明统计优势。S3-B2b 全仓离线 pytest 已通过，仍有全树 Ruff 存量问题；运行环境尚未部署本次代码。

## Git 与发布状态

| 项目 | 当前事实 |
|---|---|
| 本地 HEAD | 本地 `main` 与 `origin/main` 一致；精确哈希用 `git rev-parse HEAD` 与 `git rev-parse origin/main` 核验。 |
| 远端基线 | 本次 code-only 提交与此前 8 个本地提交均已随 `main` 推送；已安装的 `AlphaMiner-LimitUpHistory` 计划任务 runner 仍运行旧部署基线 `a182e67`。 |
| 工作树 | S4-C、S3-B2a 与 S3-B2b 已合并为一个 code-only 冻结提交；提交并推送后 worktree 与 index clean。未 deploy 或同步 X 盘/服务器。 |
| 真实运行库 | 已有 1 张 `three_to_four_reseal / 2026-08-17` 卡，状态 `PLANNED / NOT_ADMITTED`；本轮文档与精简工作未写数据库。 |

## 本地冻结批次

| 批次 | 已有事实 | 当前边界 |
|---|---|---|
| B1 竞价/开盘快照 | 9:25 与 09:31 免费前向采集基础已本地冻结，候选按精确前一交易日冻结。 | 代码已随 `main` 推送；未安装新任务或部署。 |
| B2 五分钟 RAW | 两表式候选 checkpoint 与 5 分钟历史补采已本地冻结。 | `RETRO_BACKFILL` 不能冒充当时可见；未形成玩法或胜率；代码已推送但未部署。 |
| PIT 研究诚信 | `PointInTimeView` 与通用/涨停 development-only 语义已本地冻结；旧引擎不得宣称 accepted。 | 一次性 holdout 审计账本尚未实现，只能称 `DEVELOPMENT_CANDIDATE`；代码已推送但未部署。 |
| H1 题材新进入者 | `theme_new_entrant_diffusion_v1` 的前向 PAPER 构建与审计约束已本地冻结。 | development 仅 12 个独立收益日；D+3 OPEN 成本后均值 `+0.5530%`，95% CI `[-0.9914%, 2.0835%]`，Holm 不显著，不能称发现；代码已推送但未部署。 |
| S2-A / S3-B1 精简 | 10 个零引用旧旁路删除，共 3031 LOC；PM 已接受删除安全并已本地冻结。 | 已随 `main` 推送；删除安全不等于全树静态检查全绿。 |
| S2-B USER 收口 | 默认 `python -m cli` 只读 play；帮助只公开 play、`zt status`、持仓 report 三条路径；15 项定向测试通过，真实 PowerShell 中文捕获通过，数据库 SHA 前后不变。 | 已随 `main` 推送但未部署；USER 角色日常只使用默认入口。 |
| S3-B2a 旧产品面退役 | 删除 7 个旧 CLI/dashboard/cron 文件共 1708 LOC，并从根路由移除 `recommend`、`signal`、`strategy`、`query`；`zt collect` 停止新建 three_to_four，但仍先结算历史卡并继续结算/生成 H1。 | 已完成 code-only 推送但未部署或同步 X 盘；持仓 report、底层 RecommendEngine/SignalEngine/StrategyEvolver、旧卡历史和结算均保留。 |
| S3-B2b 远程旧链收缩 | 本地删除 5 个旧 daily/drift/replay 与每日/小时包装，根路由统一退役 `daily/backtest/drift/script/replay`；持仓 report 与底层回测、漂移、剧本、复盘正确性组件保留。远程 allowlist 删除 collect、limit-up evolve、daily，唯一通用 evolve 明示为 `DEVELOPMENT_ONLY`。 | 已完成 code-only 推送但未执行 remote action、同步 X 盘/服务器、修改调度或真实库；定向 `133 passed`，全仓离线 `574 passed / 7 deselected`。 |
| S4 文档收口 | S4-A 只读审计得到 32 个 tracked Markdown、7552 行；S4-B 已将 14 份旧文档的 6597 行迁移/删除，主真相收敛到六份文档。 | PM 已验收并随 `main` 推送；未部署。 |

## 最近联合回归

S3-B2b 在当前共享脏工作树仅执行一次 `uv run pytest -q -m "not live"`：exit 0，`574 passed / 7 deselected / 5 warnings`；pytest 报告耗时 445.77 秒，外层计时 453.90 秒。此前本批定向组合为 `133 passed / 1 warning`，耗时 105.13 秒。

- 5 个 warnings：SnowNLP 的 `codecs.open()` 弃用警告 2 个；`src/drift/ic_tracker.py` 的 `Mean of empty slice` 3 个。
- 全树 Ruff 仍为 exit 1、289 个既有问题；其中 248 个可自动修复，另有 23 个可选 unsafe 修复，本轮未修改。
- 因此可以说“全仓离线 pytest 通过”，不能说“全部静态检查全绿”。

## 下一步

1. 请负责人核对冻结提交的 25 路径、`574 passed / 7 deselected` 回归和 `origin/main == HEAD`；本次仅完成代码推送。
2. 如需同步 X 盘、服务器、计划任务或数据库，必须另行授权并与本次 code-only 推送分离；持仓 report 与底层研究正确性组件继续保留。
