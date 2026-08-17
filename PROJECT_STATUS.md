# Alpha Miner 项目状态看板

> 面向 PM / 项目负责人。快照日期：2026-08-18；运行与数据证据截至 2026-08-17。唯一架构源规范见 [ARCHITECTURE.md](ARCHITECTURE.md)。本次三批已完成本地冻结，以下均不是已发布声明。

## 一句话结论

产品骨架已收敛为免费数据 → PIT 离线实验 → PAPER 玩法卡 → `python -m cli` 只读展示；B1/B2、PIT、H1、精简与 USER/真相源收口已分三批本地冻结，尚未 push。当前没有已证明统计优势。全仓离线 pytest 已通过，仍有全树 Ruff 存量问题。

## Git 与发布状态

| 项目 | 当前事实 |
|---|---|
| 本地 HEAD | 当前 HEAD 为本次第 3 个冻结提交；精确哈希见 `git log -3`，比 `origin/main` 多 8 个本地提交。 |
| 远端基线 | `origin/main = a182e67`；已安装的 `AlphaMiner-LimitUpHistory` 计划任务 runner 仍以此为已发布基线。 |
| 工作树 | B1/B2、PIT、H1、S2-A/S3-B1、S2-B、S4-B 和 S5 契约同步已分三批本地提交；提交完成后工作树与 index clean，未 push 或 deploy。 |
| 真实运行库 | 已有 1 张 `three_to_four_reseal / 2026-08-17` 卡，状态 `PLANNED / NOT_ADMITTED`；本轮文档与精简工作未写数据库。 |

## 本地冻结批次

| 批次 | 已有事实 | 当前边界 |
|---|---|---|
| B1 竞价/开盘快照 | 9:25 与 09:31 免费前向采集基础已本地冻结，候选按精确前一交易日冻结。 | 未发布、未安装新任务；未 push。 |
| B2 五分钟 RAW | 两表式候选 checkpoint 与 5 分钟历史补采已本地冻结。 | `RETRO_BACKFILL` 不能冒充当时可见；未形成玩法或胜率；未 push。 |
| PIT 研究诚信 | `PointInTimeView` 与通用/涨停 development-only 语义已本地冻结；旧引擎不得宣称 accepted。 | 一次性 holdout 审计账本尚未实现，只能称 `DEVELOPMENT_CANDIDATE`；未 push。 |
| H1 题材新进入者 | `theme_new_entrant_diffusion_v1` 的前向 PAPER 构建与审计约束已本地冻结。 | development 仅 12 个独立收益日；D+3 OPEN 成本后均值 `+0.5530%`，95% CI `[-0.9914%, 2.0835%]`，Holm 不显著，不能称发现；未 push。 |
| S2-A / S3-B1 精简 | 10 个零引用旧旁路删除，共 3031 LOC；PM 已接受删除安全并已本地冻结。 | 未 push；删除安全不等于全树静态检查全绿。 |
| S2-B USER 收口 | 默认 `python -m cli` 只读 play；帮助只公开 play、`zt status`、持仓 report 三条路径；15 项定向测试通过，真实 PowerShell 中文捕获通过，数据库 SHA 前后不变。 | PM 已接受并已本地冻结，未 push；USER 角色日常只使用默认入口。 |
| S4 文档收口 | S4-A 只读审计得到 32 个 tracked Markdown、7552 行；S4-B 已将 14 份旧文档的 6597 行迁移/删除，主真相收敛到六份文档。 | PM 已验收并已本地冻结，未 push。 |

## 最近联合回归

S5-C 在当前共享脏工作树仅执行一次 `uv run pytest -q -m "not live"`：exit 0，`555 passed / 7 deselected / 5 warnings`；pytest 报告耗时 450.34 秒，外层计时 456.80 秒。此前 7 项失败经严格契约同步后全部归零。

- 5 个 warnings：SnowNLP 的 `codecs.open()` 弃用警告 2 个；`src/drift/ic_tracker.py` 的 `Mean of empty slice` 3 个。
- 全树 Ruff 仍为 exit 1、289 个既有问题；其中 248 个可自动修复，另有 23 个可选 unsafe 修复，本轮未修改。
- 因此可以说“全仓离线 pytest 通过”，不能说“全部静态检查全绿”。

## 下一步

1. 请负责人通过 `git log -3` 与精确文件范围验收三批本地提交；保持未 push。
2. 冻结验收后，再做 USER 16 行输出收口及 S3-B2；在此之前不继续扩大精简或玩法范围。
