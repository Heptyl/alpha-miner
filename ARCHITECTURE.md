# Alpha Miner 当前架构

> 唯一当前架构源规范，2026-08-18。其他文档与本文冲突时，以本文为准。

## 一句话目标

Alpha Miner 以行为金融理论为核心，在唯一的涨停板生态研究场景中持续形成可证伪、可执行的 PAPER 玩法，主动告诉负责人下一交易日的模拟动作，并从 development 结果中继续进化。

涨停是极强的注意力/显著性观测，不是顶层架构或今日候选硬门槛；系统不是每日股票打分器，不以 `WATCH_ONLY` 代替动作，不连接券商，也不再增加平行模块。

## 唯一闭环

```text
行为金融理论 → 可证伪假说 → 涨停生态观测
  → Behavior State（行为状态）→ 可执行玩法
  → 走步/PAPER 证据 → development 反馈演化
```

holdout 是冻结血统的一次性时间外裁决，不反馈演化。Windows 免费前向采集并发布带时间戳数据；服务器程序完成状态计算、走步回放、失败诊断和验证，再由单一发布者产生 USER 只读的 PAPER 投影。

## 四个“一”

| 原则 | 不可变含义 |
|---|---|
| 一个引擎 | `EvolutionEngine` 是唯一离线研究编排器；因子、策略和涨停只是同一引擎内的实验类型。 |
| 一个 USER 数据契约 | USER 只读 market SQLite 中的行情与 `play_cards` 投影；研究证据写入独立、追加式 `research_ledger.db`。过去“一个数据库”的物理单库实现已退役。 |
| 一个 USER 入口 | 只读预计算玩法卡，目标 p95 `<=5秒`；不得触发采集、临时回测、网页搜索、LLM 或进化。 |
| 一种玩法卡 | 负责人只看玩法、行为逻辑、候选、模拟动作、放弃、卖出、历史证据和 PAPER/准入状态。 |

Windows 负责免费数据的前向采集与发布；服务器负责离线回测和慢实验。慢任务永远不阻塞 USER。

## 唯一逻辑层：Behavior State / 行为状态层

Behavior State 是唯一新增逻辑层，不是新引擎、CLI、数据库或状态文件。它把涨停生态观测归纳为四种可计算状态：`attention memory`（注意力记忆）、`diffusion`（扩散）、`crowding`（拥挤）和 `decay`（衰减）。

- 近期涨停记忆池不局限于今日或 D-1，按交易日衰减；重复涨停、连板、封板质量和题材扩散强化记忆，炸板、退潮和破位加速衰减。窗口、半衰期和强化/衰减参数必须作为冻结实验参数验证，不能写成永恒真理。
- 涨停是注意力与显著性冲击的核心观测；量价、竞价、分钟、题材和新闻只用于解释涨停生态的形成、强化、扩散与衰减，不能脱离该场景另造产品。
- 行为状态不等于买入信号：高 attention 不能自动买入，必须叠加转折或再加速、可成交约束、明确入场/退出和完整成本。
- 候选可来自近期涨停股、涨停后趋势仍存股，以及注意力扩散影响的尚未涨停同题材股；主要买点不要求当日涨停。

## 数据运行、发布与研究证据

- 服务器本地 market SQLite/WAL 是行情与 USER 投影运行库；可替换但只向 USER 提供只读契约。独立 `research_ledger.db` 只追加候选、血统与证据，market 发布/激活绝不迁移或覆盖账本。
- X 盘只承载代码同步和只读一致性快照，不承担运行库或并发写入。
- Windows 采集结果发布时必须用 SQLite backup API 生成一致性副本；canonical manifest 的 SHA256 必须等于 active market DB 实际字节哈希。服务器先执行 `PRAGMA quick_check`，再以可恢复状态机替换运行库，并保留 `alpha_miner.previous.db` 供回滚。
- 研究开始前，账本只从固定 active market DB 与 canonical manifest 绑定数据，并复制为 SHA256 命名的不可变研究快照；候选只能引用该账本实例已绑定的快照，不能由调用者注入路径或哈希。
- Evolution 不得直接写 active market 的 `play_cards`；正式获准证据只能由单一发布者投影进下一版 canonical market 快照，避免日更替换覆盖研究结果。
- 对外快照同样由 backup API 产生；USER 只读预计算结果，采集、发布、回测和进化等慢任务不得进入 USER 请求路径。

## 大模型与程序分工

- 大模型只从行为金融知识、市场叙事和失败族中提出可证伪假说，必要时做一次失败反思。
- 程序负责行为状态、行情、参数搜索、走步回测、候选、去重、成本、可成交模拟、锁定测试、development 反馈和状态推进；holdout 结果不得进入演化。
- 禁止逐股票、逐参数或逐候选调用大模型；玩法数字和状态只能由程序产生。

## USER 只做三个前向玩法

1. **注意力再加速的竞价买点**：近期涨停记忆候选在 D 日 9:25 观察竞价，09:31 仅在状态转折/再加速且代理可成交时 PAPER 模拟买入。
2. **记忆股或扩散股的趋势/回调/冲板前买点**：候选包括涨停后趋势仍存股和尚未涨停的同题材扩散股，用 1/5 分钟量价、VWAP 与真实成交验证。
3. **拥挤衰减/反向瀑布的卖出回避**：高位炸板、资金背离、题材退潮或破位时自动产生 PAPER 卖出/回避动作。

其余玩法、新市场、新采集端、推荐打分、虚拟手机和券商连接全部暂停。USER 的主要买点必须在涨停前或涨停后的趋势延续阶段，不要求买入日涨停；盘中封板/回封只用于研究触发质量和成交审计，不是默认 USER 买点。

## 前向时间线与数据硬门槛

| 时刻 | 程序动作 |
|---|---|
| D 收盘后 | 只用当时已知的涨停生态与 Behavior State 冻结候选和完整计划，不产生当天回看买入。 |
| D+1 09:25 | 只对冻结玩法卡候选保存带时间戳竞价快照，不从当日最终涨停池反选。 |
| D+1 09:31 | 首个纵切用配对的可成交快照代理判断 gap、一字与量额；满足冻结规则才记录 PAPER 模拟买入。 |
| D+1 盘中 | 分钟 VWAP 和 1/5 分钟序列尚未进入首个玩法；缺失必须标注，不得伪造。 |
| D+3 | 按冻结计划以开盘代理模拟卖出并计入 20bp 总成本。 |

首个 `attention_reacceleration_open_v1` 的最小数据门槛是成对的 9:25/09:31 带时间戳快照、非陈旧源时钟、单调累计量额与有效成交价代理；缺失只产生 `DATA_NOT_READY/INVALID/UNFILLED`。日线开盘只能用于 `RETRO_DEVELOPMENT_ONLY` 并标记 `DAILY_OPEN_PROXY`，不得用盘后日线、`open_count` 或最终封板结果冒充前向盘中证据。分钟 VWAP 与连续 1/5 分钟量价是后续强化门槛，当前明确 unsupported。

## PAPER 与实盘准入

未准入也必须产生明确的 PAPER 模拟动作并自动记录结果，不能退化为 `WATCH_ONLY`；卡片必须显示 `PAPER/未准入`，不能伪装成实盘建议。研究证据与实盘准入完全分离，既有准入门槛不得放宽，未准入仓位仍为 0。

证据必须双轨运行：`RETRO_DEVELOPMENT` 只用于历史筛选；`FORWARD PAPER` 必须在真实 `decision_time` 冻结候选、触发、放弃、入场、退出与成本。现有单表契约锁死计划 hash，结算只能单向追加生命周期事件，拒绝候选/规则/generated_at 改写、状态回退或成交价重写。成功、失败、无效和未成交都保留并只反馈 development；holdout 永不反馈演化。

## 真正的锁定测试

- 训练集用于拟合，验证集用于选优、诊断和变异；锁定测试不参与 fitness、排序、提示词或重试。
- 开发结束只冻结一个候选及其代码、参数、数据边界、成本、滑点和可成交模型；锁定测试只评估一次。
- 测试一经读取，该研究血统即退役；失败候选及后代不得复用，下一轮等待新的时间外窗口。
- 独立性按**信号日**处理；同日多股票是相关样本，必须同时报告信号日和交易样本。
- 收益必须计入佣金、印花税、滑点、冲击成本、涨跌停、停牌、队列、延迟和不可成交，不能静默删样本。

H1 的锁定测试在候选冻结前固定完整审计日期、development/embargo/reserved 日期、最长前向期、20bp 总成本代理、主指标、bootstrap seed/次数/block、family size、Holm 规则与最小样本/效果门槛。成熟度不足时不得打开或读取 reserved 收益；获一次性内部授权后，账本必须先提交 `HOLDOUT_OPENED` 才能首次读取，普通异常写入唯一 `EVALUATION_ERROR`，硬崩溃保持 `INCONCLUSIVE_CRASH` 且永不重开。同一血统或同一冻结窗口只允许打开一次。通过也只到 `ADMISSION_APPROVED_PENDING_PUBLICATION`，在单一发布者投影前不得向 USER 宣称准入或发现。

## 现有实现归一与迁移

`EvolutionEngine.run` 的默认预算只用于完整 `attention_reacceleration_open_v1` 玩法基因：初代合并知识库理论来源，后续在预注册有界参数集内按代选择和定向变异。首次运行冻结 search axes、fitness、seed、代数、种群、最大试验数、实现哈希与 multiplicity；同一 snapshot 只能按完全相同协议续跑。相同执行参数只评估一次，但全部 theory/prediction/evidence-grade provenance 随候选写入 append-only ledger。所有候选保持 `DEVELOPMENT_CANDIDATE / PAPER_ONLY / HOLDOUT_NOT_OPENED / NOT_ADMITTED`；默认 evolve 不打开 holdout、不写 active market 或 `play_cards`，旧 factor hypothesis 不占用玩法 population 预算。

已结算 FORWARD PAPER 只作为 `ADAPTIVE_DEVELOPMENT_FEEDBACK`：必须与 execution hash 精确匹配，COMPLETED 收益须由冻结入退场价和 20bp 重算一致；收益按独立信号日等权，执行诊断按 `INVALID > UNFILLED > COMPLETED > NOT_TRIGGERED` 投影为互斥日状态，并受 5/20/40 日分段上限约束。少于 5 日调整为 0 且不消费；首次有效批次的计划 hash、窗口和 receipt 随 DEVELOPMENT_RESULT 原子追加，后续 search family 不得重复消费。反馈不能读取 reserved/holdout、重复累加或证明某一理论来源，PAPER 也永不升级为准入证据。


`EvolutionEngine` KEEP + ADAPT，成为唯一引擎。H1 与首个 Behavior State adapter `attention_reacceleration_open_v1` 都先绑定 hash 命名的不可变 market snapshot、冻结完整玩法候选，再按独立信号日重算并追加 `DEVELOPMENT_ONLY / HOLDOUT_NOT_OPENED` 证据；同一次运行继续在同一 snapshot 上执行可续跑的 factor hypothesis 搜索、失败诊断与定向变异。因子假说只标记 `HYPOTHESIS_ONLY / DEVELOPMENT_ONLY`，不是完整玩法或正式证据。引擎不写 active market 或 `play_cards`，也不宣称发现；新玩法的历史 evaluator、前向 builder 与 settler 共用同一候选/触发纯函数。

空壳 `CandidatePool` 与不可信 `StrategyEvolver` 已 RETIRE，旧 JSON 不迁移且不再是真相源；`LimitUpEvolutionEngine` 暂按 legacy research 保留，待能力迁入统一引擎后退役。`FailureAnalyzer` 保留为统一诊断组件，反馈结果由 `play_cards` 追加并回流统一证据链。不得新增独立 CLI 或第四套引擎。

1. **先补数据基础**：Windows 连续前向采集竞价与分钟数据，单库保存；USER 继续只读一种玩法卡。
2. **再做前向 PAPER**：唯一引擎按上述三个玩法回测并每天自动推进模拟结果，不用盘后数据反推买点。
3. **最后退役旧实现**：结果等价、无唯一数据且至少一个发布周期无需回退后，删除平行引擎、入口和状态文件。

工程完成标准是三个玩法都走完“假说→回测→前向 PAPER→结果→再进化”，不是强行得到高胜率或准入结论。

架构冻结点：行为状态、走步回放、进化强化三个纵切完成后停止架构扩张，后续只增加数据覆盖和预注册实验，不再增加逻辑层、引擎、CLI、数据库或平行产品名。
