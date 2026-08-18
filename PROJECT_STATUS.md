# Alpha Miner 项目状态看板

> 面向 PM / 项目负责人。快照日期：2026-08-18；运行与数据证据截至 2026-08-17。唯一架构源规范见 [ARCHITECTURE.md](ARCHITECTURE.md)。S4-C、S3-B2a 与 S3-B2b 已完成 code-only 冻结并推送到 `origin/main`；以下不代表 X 盘、服务器、计划任务或数据库已经部署。

## 一句话结论

产品骨架已收敛为免费数据 → PIT 离线实验 → PAPER 玩法卡 → `python -m cli` 只读展示；market/USER 投影与独立 append-only 研究账本采用物理隔离。当前工作树已把 H1 接入唯一 `EvolutionEngine` 的 development 纵切，但未部署、未打开 holdout，因此仍没有正式发现或已证明统计优势。

## Git 与发布状态

| 项目 | 当前事实 |
|---|---|
| 本地 HEAD | 本地 `main` 与 `origin/main` 一致；精确哈希用 `git rev-parse HEAD` 与 `git rev-parse origin/main` 核验。 |
| 远端基线 | 本次 code-only 提交与此前 8 个本地提交均已随 `main` 推送；已安装的 `AlphaMiner-LimitUpHistory` 计划任务 runner 仍运行旧部署基线 `a182e67`。 |
| 代码冻结 | 本批代码冻结状态以 `git log`、`git status`、HEAD 与 `origin/main` 实时核验；尚未 deploy、同步 X 盘/服务器或触碰真实数据库。 |
| 真实运行库 | 已有 1 张 `three_to_four_reseal / 2026-08-17` 卡，状态 `PLANNED / NOT_ADMITTED`；S6-B 仅以 SQLite `mode=ro&immutable=1` 做一次状态 smoke，SHA/大小/mtime 前后不变。 |

## 本地冻结批次

| 批次 | 已有事实 | 当前边界 |
|---|---|---|
| B1 竞价/开盘快照 | 9:25 与 09:31 免费前向采集基础已本地冻结，候选按精确前一交易日冻结。 | 代码已随 `main` 推送；未安装新任务或部署。 |
| B2 五分钟 RAW | 两表式候选 checkpoint 与 5 分钟历史补采已本地冻结。 | `RETRO_BACKFILL` 不能冒充当时可见；未形成玩法或胜率；代码已推送但未部署。 |
| PIT 研究诚信 | `PointInTimeView` 与通用/涨停 development-only 语义已推送；独立追加式候选/血统/证据原语已绑定不可变 market hash 快照。 | 当前工作树仅接入 H1 adapter；FactorBacktester/LimitUp 尚未迁移。一次性 holdout evaluator 仅在合成库验证，真实 holdout 从未打开，只能称 `DEVELOPMENT_CANDIDATE`。 |
| S3-B3-2A 统一玩法纵切 | 当前工作树中，`EvolutionEngine` 首次以 H1 完成“绑定不可变 snapshot → 收益读取前冻结完整候选 → 重算 development → 追加证据”，并在同一 snapshot 上继续执行 factor hypothesis 的失败诊断、变异和 resume；PAPER builder 与 evaluator 共用选择规则。空壳 CandidatePool 和不可信 StrategyEvolver 已删除。 | 尚未 commit/push/deploy；factor 仅为 `HYPOTHESIS_ONLY / DEVELOPMENT_ONLY`，holdout 调用为 0、accepted 恒 false，不写 active market/`play_cards`，不能称发现。相关组合 `131 passed`。 |
| S3-B3-2B 一次性 holdout 闸门 | 当前工作树已冻结 H1 的精确分区、20bp 成本代理、bootstrap/Holm 与最小样本/效果门槛；显式 evaluator 先提交 `HOLDOUT_OPENED` 才读 reserved，并将结果或普通异常追加为唯一终态。 | 未接 CLI/自动任务/发布者；真实数据仅 1 个合格审计日，真实 open 调用为 0。合成定向组合 `95 passed`；即使通过也仅为 `ADMISSION_APPROVED_PENDING_PUBLICATION`，USER 仍未准入。 |
| S6-0 行为金融契约 | 当前工作树把唯一链路纠偏为行为金融理论 → 涨停生态观测 → Behavior State → 走步/PAPER → development反馈；涨停是强观测而非当日候选硬门槛。 | 尚未commit/push/deploy；知识库区分学术基础、理论推导与HEURISTIC，不能据此准入。 |
| S6-B Behavior State | 当前工作树新增唯一纯逻辑reducer：冻结衰减/强化参数，只从decision-bound PIT视图批量计算 attention memory、diffusion、crowding、decay；状态不直接产生买卖信号。 | 分钟09:35和历史真实题材映射明确unsupported。真实只读smoke为2026-08-17、171个股票状态、56个industry proxy组；相关组合 `60 passed`。 |
| S6-C 双轨行为玩法 | 当前工作树用同一冻结候选/触发规则接入 `attention_reacceleration_open_v1`：RETRO逐日PIT重建且只用日线开盘代理；FORWARD在D收盘冻结计划，D+1配对09:25/09:31证据后单向推进并于D+3结算。 | 尚未commit/push/deploy或写真实库；缺数据、未成交、负收益都保留，实盘仓位0，holdout读取仍为0，不能称发现。 |
| S6-D1 完整玩法演化 | 当前工作树让默认 `EvolutionEngine.run` 按冻结 search family 对 attention 完整玩法基因执行确定性代际搜索；相同执行参数合并理论来源且只评估一次，候选、父血统、变异原因、失败族、fitness 与负证据追加到独立 ledger，resume 不依赖旧 JSON。 | 尚未commit/push/deploy或运行真实 evolve；只使用 development，旧 factor 不占玩法 population，holdout 与 active market/`play_cards` 写入仍为0，所有输出均未准入。 |
| S6-D2 PAPER受限强化 | 当前工作树只读取不可变 snapshot 内、与 execution hash 精确匹配且在 development cutoff 前完整结算的 attention PAPER；COMPLETED 由冻结价格重算，收益与互斥执行日分开聚合，5/20/40 日分段限制 adjustment；首次有效反馈通过 DEVELOPMENT_RESULT 原子消费计划 hash，跨 family 不重复。 | 尚未commit/push/deploy或运行真实 evolve；少于5个完成信号日调整为0且不消费，PAPER仅为自适应development反馈，不是独立验证、holdout或准入。 |
| S6-E2/E3 盘后闭环与动态自选 | 当前工作树增加默认关闭的隔离盘后编排，以及独立 `user_preferences.db` 自选偏好和 working market RAW 5m 补留；自选状态与系统候选状态分离，bar 复用同一不可变键。 | 尚未commit/push/deploy、未安装任务、未初始化真实偏好库；自选不进入 Behavior State、fitness、排名或准入，只支持 5m。 |
| H1 题材新进入者 | `theme_new_entrant_diffusion_v1` 的前向 PAPER 构建与审计约束已本地冻结。 | development 仅 12 个独立收益日；D+3 OPEN 成本后均值 `+0.5530%`，95% CI `[-0.9914%, 2.0835%]`，Holm 不显著，不能称发现；代码已推送但未部署。 |
| S2-A / S3-B1 精简 | 10 个零引用旧旁路删除，共 3031 LOC；PM 已接受删除安全并已本地冻结。 | 已随 `main` 推送；删除安全不等于全树静态检查全绿。 |
| S2-B USER 收口 | 默认 `python -m cli` 只读 play；帮助只公开 play、`zt status`、持仓 report 三条路径；15 项定向测试通过，真实 PowerShell 中文捕获通过，数据库 SHA 前后不变。 | 已随 `main` 推送但未部署；USER 角色日常只使用默认入口。 |
| S3-B2a 旧产品面退役 | 删除 7 个旧 CLI/dashboard/cron 文件共 1708 LOC，并从根路由移除 `recommend`、`signal`、`strategy`、`query`；`zt collect` 停止新建 three_to_four，但仍先结算历史卡并继续结算/生成 H1。 | 已完成 code-only 推送但未部署或同步 X 盘；持仓 report、底层 RecommendEngine/SignalEngine/StrategyEvolver、旧卡历史和结算均保留。 |
| S3-B2b 远程旧链收缩 | 本地删除 5 个旧 daily/drift/replay 与每日/小时包装，根路由统一退役 `daily/backtest/drift/script/replay`；持仓 report 与底层回测、漂移、剧本、复盘正确性组件保留。远程 allowlist 删除 collect、limit-up evolve、daily，唯一通用 evolve 明示为 `DEVELOPMENT_ONLY`。 | 已完成 code-only 推送但未执行 remote action、同步 X 盘/服务器、修改调度或真实库；定向 `133 passed`，全仓离线 `574 passed / 7 deselected`。 |
| S4 文档收口 | S4-A 只读审计得到 32 个 tracked Markdown、7552 行；S4-B 已将 14 份旧文档的 6597 行迁移/删除，主真相收敛到六份文档。 | PM 已验收并随 `main` 推送；未部署。 |

## 最近联合回归

当前统一纵切与一次性 holdout 闸门工作树仅执行一次有效的 `uv run pytest -q -m "not live"`：exit 0，`627 passed / 7 deselected / 5 warnings`；pytest 报告耗时 555.49 秒，外层计时 562.29 秒。本批 2B 定向组合为 `95 passed`，2A/2B 相关联合组合为 `140 passed`，相关 Ruff 通过。

- 5 个 warnings：SnowNLP 的 `codecs.open()` 弃用警告 2 个；`src/drift/ic_tracker.py` 的 `Mean of empty slice` 3 个。
- 全树 Ruff 仍为 exit 1、289 个既有问题；其中 248 个可自动修复，另有 23 个可选 unsafe 修复，本轮未修改。
- 因此可以说“全仓离线 pytest 通过”，不能说“全部静态检查全绿”。

## 下一步

1. 先独立验收 S6-D2 的精确匹配、development cutoff、分段上限与结构化 why；不扩新引擎或产品面。
2. 冻结状态以 HEAD/origin 核验；commit/push、部署、同步 X 盘/服务器、修改计划任务或数据库仍需独立授权，本批没有触碰真实运行库。
