# Alpha Miner

基于行为金融学的 A 股超短线因子挖掘系统。

按身份阅读，避免从实现细节中自行拼接结论：

| 身份 | 先看 | 用途 |
|------|------|------|
| 股票分析用户 | [USER_GUIDE.md](USER_GUIDE.md) | 一条日常命令、操作卡解释、空仓条件 |
| PM / 项目负责人 | [PROJECT_STATUS.md](PROJECT_STATUS.md) | 当前进度、证据、风险、下一步 |
| 开发维护者 | [DOCS.md](DOCS.md) | 完整架构、数据口径与技术实现 |

## 架构

```
alpha-miner/
├── cli/                    # CLI 入口 (python -m cli <command>)
│   ├── collect.py          #   数据采集
│   ├── report.py           #   日报 + 盘后简报 + 市场剧本
│   ├── mine.py             #   因子进化挖掘 + 手术台 CLI
│   ├── drift.py            #   漂移检测
│   ├── backtest.py         #   单因子回测
│   ├── limit_up.py         #   涨停专项进化 + 用户操作卡
│   ├── replay.py           #   复盘引擎
│   ├── strategy.py         #   策略管理 (list/backtest/evolve/scan)
│   ├── signal.py           #   选股信号
│   ├── recommend.py        #   个股推荐
│   └── query.py            #   数据查询
├── src/
│   ├── data/               # 数据层 (Storage + 7 个数据源采集器)
│   ├── factors/            # 因子库 (10 公式 + 4 叙事)
│   ├── narrative/          # 叙事引擎 (新闻分类/剧本/复盘)
│   ├── drift/              #   漂移检测 + 决策输出 (含动态 Regime 权重)
│   ├── mining/             #   通用 IC 进化 + 涨停事件专用进化
│   ├── strategy/           #   策略子系统 (回测/进化/持久化/推荐/信号/风控)
│   └── pipeline/           #   IC 管线 (批量计算 + 持久化)
├── factors/                # 进化产出的因子代码 (6 个已验收)
├── knowledge_base/         # theories.yaml (12 假说) + strategies.yaml (5 策略)
├── config/                 # factors.yaml + settings.yaml.example + recommend.yaml
├── scripts/                # 日常任务 + Windows/SSH 远程计算与数据发布
├── tests/                  # 404 passed + 2 skipped
└── pyproject.toml          # uv 项目配置 (Python >= 3.11)
```

## 因子体系

### 基础公式因子 (5)

| 因子 | 级别 | 逻辑 |
|------|------|------|
| zt_dt_ratio | 市场 | 涨停/(涨停+跌停)，情绪方向 |
| consecutive_board | 股票 | 连板天数 × (1 - 开板率) |
| main_flow_intensity | 股票 | 主力净流入 / 成交额 |
| turnover_rank | 股票 | 换手率百分位排名 |
| lhb_institution | 股票 | 龙虎榜机构净买入额排名 |

### 涨停结构因子 (5)

| 因子 | 角色 | 可操作含义 |
|------|------|------------|
| zt_seal_strength | alpha | 早封、少炸板、封单承接强，进入候选排序 |
| zt_relay_quality | alpha | 连板高度与封板、换手、板块、资金共同确认 |
| zt_sector_breadth | alpha | 同行业多股涨停，区分板块共振与孤立涨停 |
| zt_capital_confirmation | alpha | 主力净流入相对成交额确认涨停强度 |
| zt_break_risk | filter | 晚封、多次炸板、封单弱时降权或直接回避 |

### 叙事因子 (4)

| 因子 | 级别 | 逻辑 |
|------|------|------|
| theme_lifecycle | 股票 | 题材涨停阶段 → 生命周期分数 (萌芽→爆发→衰退) |
| narrative_velocity | 股票 | 新闻类型加权 3 日变化率 (7 类) |
| theme_crowding | 股票 | 1 - max(题材涨停占比 × 5)，反拥挤 |
| leader_clarity | 股票 | 龙头成交额 / 第二名成交额 |

验收标准：IC > 0.03, ICIR > 0.5, 胜率 > 55%, 盈亏比 > 1.2

## 因子如何变成操作

因子本身只负责描述和排序，不能直接等同于买入。涨停专项使用一条固定、可审计的链路：

```text
T0 收盘涨停事件
  → 封板/接力/板块/资金/风险结构评分
  → 次日开盘可成交检查（非一字板，涨幅在候选阈值内）
  → 锁定测试通过才允许 CONDITIONAL_BUY
  → 单票≤10%、最多3只
  → 买入后第1或第2个完整交易日收盘退出（遵守 T+1）
```

训练、验证、锁定测试任一段不达标时，输出只能是 `WATCH_ONLY` 或 `AVOID`，仓位为 0。
日常用户只运行：

```powershell
uv run python -m cli zt daily              # 盘后采集→算因子→次日操作卡
uv run python -m cli zt daily --skip-collect  # 数据已有定时任务更新
uv run python -m cli zt status             # 一眼查看数据与实盘闸门
```

## 进化引擎 v2

```
知识库种子 (12 假说)
    ↓ LLM/模板 → 代码翻译
因子代码 → 隔离加载 → 真实回测 (FactorBacktester, 逐日 Spearman IC)
    ↓ 带 regime/zt_count 的 ic_series
因子手术台 (三分段分析 + 黄金窗口 + 诊断)
    ↓ 验收通过 → 候选池 (5天观察期)
    ↓ 失败 → 定向变异 (手术台驱动) → 下一代 checkpoint
frontier 为空 → 历史最佳失败者重启种群 → 继续探索
```

核心升级：
- **真实回测器**：替换假沙箱 IC，逐日计算 Spearman IC，带 regime/zt_count 上下文
- **因子手术台**：regime/情绪/时间三分段 IC 分析 + 黄金窗口检测 + 5种诊断
- **定向变异**：基于手术台诊断做 regime 过滤/情绪过滤/方向反转/窗口调整
- **候选池**：5 天观察期，连续达标才入库
- **持续进化**：保存代数/frontier/已测试签名，续跑不重算；空种群从历史失败者自动复苏
- **并行评估**：`--workers N` 并行回测候选；本地默认 1，服务器建议 8–16
- **动态权重**：Regime 权重从历史 IC 动态计算，硬编码值作 fallback

CLI 手术台：

```bash
python -m cli mine surgery --factor consecutive_board --days 60
```

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

## 策略子系统

5 个预置策略，定义在 `knowledge_base/strategies.yaml`：

| 策略 | 来源假说 |
|------|---------|
| 首板打板_龙头确认 | info_cascade + theme_lifecycle |
| 题材发酵_跟风低吸 | theme_lifecycle |
| 情绪冰点_反弹首板 | emotion_regime |
| 三班组回避 | three_shift |
| 连板接力_情绪共振 | herd_effect |

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

## 漂移检测

| 模块 | 功能 |
|------|------|
| IC Tracker | 滚动 Spearman IC → ICIR / 胜率 / 盈亏比 / 趋势 |
| CUSUM | 递归变点检测，因子 IC 结构性断裂 |
| Regime | 市场状态 (连板潮 / 题材轮动 / 地量 / 普涨跌 / 正常) |

## 测试（404 passed + 2 skipped）

覆盖数据采集、14 个注册因子、涨停可成交标签/结构进化/操作闸门、IC 端到端、手术台、
策略、漂移、报告和 Windows UTF-8 可移植性。2026-08-14 全量回归通过。

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
uv run python -m cli mine surgery --factor X --days 60  # 5. 手术台
uv run python -m cli report --date $DATE          # 6. 日报
uv run python -m cli script --date $DATE --save   # 7. 剧本

uv run python -m cli report --brief               # 盘后简报
```

## 远程计算

服务器工作区由本地私有配置指定，`data/alpha_miner.db` 是权威运行库。
不要从 Windows 直接打开正在运行的 SQLite/WAL；X 盘只承担代码同步、数据库发布和快照读取。

```text
行情源 ──(服务器有行情出口)──────────────→ 服务器 collect ─┐
行情源 ──→ Windows collect ─→ SQLite backup ─→ 原子激活 ─┤
                                                        ↓
                                              服务器本地 SQLite
                                                        ↓
                                  8–16 workers 并行回测/持续进化
                                                        ↓
                                state + mining log + candidate pool
```

```powershell
# 首次复制模板并填写 SSH 用户、服务器地址和远程目录；本地文件不会进入 Git
Copy-Item config\remote.example.ps1 config\remote.local.ps1

# 首次部署（只有首次需要 -SeedData）
.\scripts\remote_compute.ps1 -Action sync -SeedData
.\scripts\remote_compute.ps1 -Action build

# 日常操作：代码同步、服务器直采、进化
.\scripts\remote_compute.ps1 -Action sync
.\scripts\remote_compute.ps1 -Action collect
.\scripts\remote_compute.ps1 -Action evolve
.\scripts\remote_compute.ps1 -Action evolve-limit-up
.\scripts\remote_compute.ps1 -Action snapshot

# 服务器行情出口不可用时：Windows 采集后发布一致性数据库
uv run python -m cli collect --today
.\scripts\remote_compute.ps1 -Action publish-data
```

若服务器不能访问 Docker Hub，`build` 会由 Windows 下载 Linux CPython 和锁定依赖到
`.server-runtime`，服务器自动使用离线运行时，无需服务器 root 权限或公网访问。

默认进化参数为 10 代、每代 16 个候选、16 个并发 worker。可在服务器上通过
`ALPHA_MINER_GENERATIONS`、`ALPHA_MINER_POPULATION`、`ALPHA_MINER_WORKERS` 调整。
一致性快照位于 `X:\alpha-miner\reports\alpha_miner.snapshot.db`。
如果服务器行情访问依赖代理，设置标准 `HTTP_PROXY`/`HTTPS_PROXY`，并设置
`ALPHA_MINER_USE_PROXY=1` 让腾讯会话继承代理。若暂时无法直采，在 Windows 完成
`collect` 后运行 `publish-data`；它通过
SQLite backup API 上传一致性副本，在服务器校验后原子替换运行库，并保留上一版。

## License

MIT
