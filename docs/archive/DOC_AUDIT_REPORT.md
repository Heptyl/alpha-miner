# Alpha Miner 文档审查报告

> 审查日期: 2026-05-21
> 审查范围: README.md / CLAUDE.md / DOCS.md / .gitignore / config/ / 实际代码结构
> 审查人: Hermes Agent

---

## 一、README.md 问题清单

### 1.1 因子名错误
- **文件**: README.md 第 41 行
- **当前**: `| zt_ratio | 市场 | 涨停/(涨停+跌停)，情绪方向 |`
- **实际**: 代码中因子名为 `zt_dt_ratio`（见 `src/factors/formula/zt_ratio.py: name = "zt_dt_ratio"`，`config/factors.yaml` 也注册为 `zt_dt_ratio`）
- **严重程度**: 🔴 必须修
- **修复**: 改为 `| zt_dt_ratio | 市场 | 涨停/(涨停+跌停)，情绪方向 |`

### 1.2 数据源采集器数量错误
- **文件**: README.md 第 20 行
- **当前**: `│   ├── data/               # 数据层 (Storage + 6 个 akshare 采集器)`
- **实际**: `src/data/sources/` 下有 7 个采集器（6 个 akshare + 1 个 fundamentals.py 新浪基本面）
- **严重程度**: 🟡 建议修
- **修复**: 改为 `Storage + 7 个数据源采集器 (6 akshare + 1 新浪基本面)`

### 1.3 测试数量错误
- **文件**: README.md 第 31 行, 第 143-145 行
- **当前**: `├── tests/                  # 288 tests` 和 `## 测试 (288 tests)`
- **实际**: `grep -r "def test_" tests/ | wc -l` = **349 个**（34 个测试文件）
- **严重程度**: 🔴 必须修
- **修复**: 更新为 349 tests（或运行后确认精确数字）

### 1.4 CLI 目录树不完整 — 缺少 signal/query/recommend 命令
- **文件**: README.md 第 11-18 行
- **当前**: 目录树列出 collect/report/mine/drift/backtest/replay/strategy 共 7 个 CLI 文件
- **实际**: `cli/` 下有 11 个文件：collect, report, mine, drift, backtest, replay, strategy, **signal**, **query**, **recommend**, **__main__**
- **严重程度**: 🟡 建议修
- **修复**: 添加 `signal.py`, `query.py`, `recommend.py` 到目录树

### 1.5 文档未覆盖 signal/recommend/query 三个 CLI 命令
- **文件**: README.md 全文
- **当前**: 无任何关于 `python -m cli signal`、`python -m cli recommend`、`python -m cli query` 的说明
- **实际**: 这三个命令已在 `cli/__main__.py` 注册，有独立 CLI 文件且 help 文本完整
- **严重程度**: 🟡 建议修
- **修复**: 添加这三个命令的简要说明和使用示例

### 1.6 config/ 目录树不完整
- **文件**: README.md 第 29 行
- **当前**: `├── config/                 # factors.yaml + settings.yaml`
- **实际**: config/ 下有 `factors.yaml`, `settings.yaml.example`, `recommend.yaml`, `factor_aliases.yaml`（未 track）
- **严重程度**: 🟢 可选
- **修复**: 改为 `factors.yaml + settings.yaml.example + recommend.yaml`

### 1.7 Python 版本不一致
- **文件**: README.md 第 32 行 vs CLAUDE.md 第 8 行
- **当前**: README 写 `Python >= 3.11`，CLAUDE.md 写 `Python: 3.12+`
- **实际**: `pyproject.toml` 写 `requires-python = ">=3.11"`，mypy 配置 `python_version = "3.11"`
- **严重程度**: 🟡 建议修
- **修复**: CLAUDE.md 应改为 `Python >= 3.11`，与 pyproject.toml 一致

### 1.8 策略子系统目录树缺少多个模块
- **文件**: README.md 第 25 行
- **当前**: `│   ├── strategy/           #   策略子系统 (回测/进化/持久化)`
- **实际**: `src/strategy/` 下有 17 个 .py 文件，包含：schema, backtest_engine, loader, evolver, store, **adaptive_weights**, **chase_protection**, **feedback**, **llm_analysis**, **push**, **recommend**, **review**, **signal**, **technical**, **win_rate_backtest** 等远超"回测/进化/持久化"的范畴
- **严重程度**: 🟡 建议修
- **修复**: 改为 `策略子系统 (回测/进化/持久化/推荐/信号/风控/推送)`

---

## 二、CLAUDE.md 问题清单

### 2.1 数据库表列表不完整（缺少 5 张表）
- **文件**: CLAUDE.md 第 34-49 行
- **当前**: 15 张表
- **实际**: `schema.sql` 有 **20 张表**，缺少：
  - `market_scripts` — 市场剧本
  - `replay_log` — 复盘记录
  - `strategy_defs` — 策略定义
  - `strategy_reports` — 回测报告
  - `strategy_trades` — 交易记录
- **严重程度**: 🔴 必须修
- **修复**: 添加上述 5 张表

### 2.2 项目结构描述不完整
- **文件**: CLAUDE.md 第 12-27 行
- **当前**: 未列出 `narrative/`、`strategy/`、`pipeline/` 子目录
- **实际**: `src/` 下有 narrative/（叙事引擎）、strategy/（策略子系统）、pipeline/（IC 管线）
- **严重程度**: 🟡 建议修
- **修复**: 添加 narrative/, strategy/, pipeline/ 到项目结构

### 2.3 Python 版本与 pyproject.toml 不一致
- **文件**: CLAUDE.md 第 8 行
- **当前**: `Python: 3.12+`
- **实际**: `pyproject.toml` 写 `requires-python = ">=3.11"`
- **严重程度**: 🟡 建议修

### 2.4 常用命令缺少 signal/recommend/query
- **文件**: CLAUDE.md 第 51-74 行
- **当前**: 只列了 collect/report/backtest/drift/mine/report 六个命令
- **实际**: 还有 `signal`（选股信号）、`recommend`（个股推荐）、`query`（数据查询）三个活跃命令
- **严重程度**: 🟡 建议修

### 2.5 开发原则缺少关键约束
- **文件**: CLAUDE.md 第 76-81 行
- **当前**: 5 条原则
- **建议补充**:
  - 策略回测必须遵守 T+1 约束
  - LLM 调用是可选的（`llm_client=None` 时走规则路径）
  - 因子注册通过 FactorRegistry 自动扫描，CLI 无需硬编码
- **严重程度**: 🟢 可选

### 2.6 Harness Plugin 描述
- **文件**: CLAUDE.md 第 83-112 行
- **当前**: 占了 CLAUDE.md 约 25% 的篇幅
- **实际**: 这是第三方工具的说明，不应占据 AI 协作指南如此大比例
- **严重程度**: 🟢 可选
- **建议**: 可精简或移到单独文档

---

## 三、DOCS.md 问题清单

### 3.1 因子名错误（同 README）
- **文件**: DOCS.md 第 35 行, 第 104 行
- **当前**: `zt_ratio.py` 和 `### zt_ratio (市场级)`
- **实际**: 因子名为 `zt_dt_ratio`
- **严重程度**: 🔴 必须修
- **修复**: 改为 `zt_dt_ratio`（注意：文件名 `zt_ratio.py` 是对的，但因子名/标题应为 `zt_dt_ratio`）

### 3.2 测试数量错误
- **文件**: DOCS.md 第 98 行, 第 511 行
- **当前**: `tests/                  # 288 tests` 和 `## 测试 (288 tests)`
- **实际**: 349 个测试
- **严重程度**: 🔴 必须修

### 3.3 测试文件列表不完整
- **文件**: DOCS.md 第 532-556 行
- **当前**: 列出约 19 个测试文件
- **实际**: 有 34 个测试文件，缺少：`test_recommend.py`, `test_recommend_pipeline.py`, `test_signal.py`, `test_win_rate_backtest.py` 等
- **严重程度**: 🟡 建议修

### 3.4 CLI 目录树缺少 signal/query/recommend
- **文件**: DOCS.md 第 9-17 行
- **当前**: 只列出 7 个 CLI 文件
- **实际**: 有 11 个文件（含 __main__.py, signal.py, query.py, recommend.py）
- **严重程度**: 🟡 建议修
- **修复**: 添加 signal.py, query.py, recommend.py

### 3.5 完整目录树缺少 strategy/ 子模块细节
- **文件**: DOCS.md 第 73-78 行
- **当前**: `strategy/` 下只列出 schema/backtest_engine/loader/evolver/store 5 个文件
- **实际**: 有 17 个 .py 文件，缺少：adaptive_weights, chase_protection, feedback, llm_analysis, push, recommend, review, signal, technical, win_rate_backtest
- **严重程度**: 🟡 建议修

### 3.6 目录树缺 data/ 子模块
- **文件**: DOCS.md 第 18-30 行
- **当前**: 列出 data/ 下的文件但缺少 `trading_calendar.py` 和 `sources/fundamentals.py`
- **实际**: 这两个文件存在于 `src/data/`
- **严重程度**: 🟡 建议修

### 3.7 配置示例中的 API 配置过时
- **文件**: DOCS.md 第 577-603 行
- **当前**: `config/settings.yaml` 示例使用 `anthropic.api_key` + `model: "claude-sonnet-4-20250514"`
- **实际**: `config/settings.yaml.example` 使用 `deepseek.api_key` + `model: "deepseek-v4-flash"`
- **严重程度**: 🔴 必须修
- **修复**: 更新示例配置与 settings.yaml.example 一致

### 3.8 配置说明缺 recommend.yaml
- **文件**: DOCS.md 第 605-607 行
- **当前**: 只提到 factors.yaml
- **实际**: 还有 `recommend.yaml`（个股推荐配置，含因子权重/技术参数/筛选条件）
- **严重程度**: 🟡 建议修

### 3.9 config/ 目录树缺少 recommend.yaml
- **文件**: DOCS.md 第 91-93 行
- **当前**: `config/` 下只列 factors.yaml 和 settings.yaml
- **实际**: 还有 `settings.yaml.example` 和 `recommend.yaml`
- **严重程度**: 🟡 建议修

### 3.10 文档未覆盖 signal/recommend/query CLI
- **文件**: DOCS.md 全文
- **当前**: 无任何关于 signal、recommend、query 命令的文档
- **严重程度**: 🟡 建议修

### 3.11 snownlp 描述与技术要点矛盾
- **文件**: DOCS.md 第 570 行
- **当前**: `情感引擎：金融关键词规则引擎替代 snownlp，针对 A 股语料优化`
- **实际**: `src/data/sources/akshare_news.py` 仍在使用 `from snownlp import SnowNLP`（第 10 行）
- **严重程度**: 🟡 建议修
- **修复**: 要么代码确实已替代了 snownlp（需要验证），要么更新文档说明

---

## 四、.gitignore 问题清单

### 4.1 logs/ 目录未被忽略
- **文件**: .gitignore
- **当前**: 无 `logs/` 排除规则
- **实际**: `git ls-files` 显示 `logs/hourly/mine_*.log` 和 `logs/mining_cron.log` 已被提交
- **严重程度**: 🔴 必须修
- **修复**: 添加 `/logs/` 到 .gitignore，并 `git rm --cached` 已提交的日志文件

### 4.2 signals/ 目录未被忽略
- **文件**: .gitignore
- **当前**: 无 `signals/` 排除规则
- **实际**: `git ls-files` 显示 `signals/2026-04-24.txt` 等已提交（运行时生成的信号输出）
- **严重程度**: 🔴 必须修
- **修复**: 添加 `signals/` 到 .gitignore，并 `git rm --cached` 已提交的信号文件

### 4.3 reports/ 目录排除规则冗余
- **文件**: .gitignore 第 9-11 行
- **当前**: 第 9 行注释掉了 `# reports/`，第 11 行有 `reports/`
- **实际**: 但 `reports/2026-04-20_brief.txt` 已被 git track（在 .gitignore 规则生效前已提交）
- **严重程度**: 🟡 建议修
- **修复**: `git rm --cached reports/2026-04-20_brief.txt`，删除第 9 行注释

### 4.4 scripts/ 中的临时脚本未被排除
- **文件**: .gitignore
- **当前**: 无 scripts/ 排除规则
- **实际**: `scripts/` 下有大量临时调试脚本（30+ 个），如 `_check_db.py`, `_quick_recommend.py`, `backfill_sina.py`, `test_akshare.py` 等。这些不是核心功能的一部分
- **严重程度**: 🟢 可选
- **建议**: 考虑只保留 `daily_run.sh`, `hourly_mine.sh`, `compute_factors.py` 等核心脚本，其余移除或添加到 .gitignore

### 4.5 uv.lock 已被忽略但不应被忽略
- **文件**: .gitignore 第 8 行
- **当前**: `uv.lock` 在忽略列表
- **实际**: `uv.lock` 是锁定依赖版本的关键文件，`uv` 官方建议提交到 git
- **严重程度**: 🟡 建议修
- **修复**: 从 .gitignore 移除 `uv.lock`，并 `git add uv.lock`

### 4.6 config/settings.yaml 排除正确
- **文件**: .gitignore 第 15 行
- **当前**: `config/settings.yaml` 被排除
- **实际**: 有 `config/settings.yaml.example` 作为模板，不含敏感信息。正确做法。
- **严重程度**: ✅ 正确

---

## 五、config/ 目录问题清单

### 5.1 factor_aliases.yaml 未被 git track
- **文件**: config/factor_aliases.yaml
- **当前**: `git status` 显示为 Untracked
- **实际**: 这是因子中文别名映射，用于微信推送，不含敏感信息
- **严重程度**: 🟡 建议修
- **修复**: `git add config/factor_aliases.yaml`

### 5.2 factors.yaml 因子名正确
- **文件**: config/factors.yaml
- **当前**: 第 4 行 `name: zt_dt_ratio` — 与代码一致 ✅
- **严重程度**: ✅ 正确

### 5.3 config/settings.yaml.example 与文档配置示例不一致
- **文件**: config/settings.yaml.example
- **当前**: 使用 deepseek API 配置
- **DOCS.md**: 使用 anthropic/claude 配置
- **严重程度**: 已在 3.7 中记录

---

## 六、pyproject.toml / 未提交改动

### 6.1 tushare 依赖已添加但未提交
- **文件**: pyproject.toml（已修改未提交）
- **当前**: diff 显示添加了 `"tushare>=1.4.29"`
- **实际**: `grep -r "tushare" src/ cli/` 无任何引用。tushare 在代码中完全未使用
- **严重程度**: 🟡 建议修
- **修复**: 要么移除这个无用依赖，要么如果计划使用则提交

### 6.2 snownlp 依赖已过时？
- **文件**: pyproject.toml 第 11 行
- **当前**: `snownlp>=0.12`
- **实际**: `src/data/sources/akshare_news.py` 仍在使用 snownlp，所以依赖正确。但 DOCS.md 第 570 行说"已替代 snownlp"——矛盾
- **严重程度**: 🟡 建议修（关联 3.11）

---

## 七、已提交的开发过程文档评估

| 文件 | 行数 | 建议 | 理由 |
|------|------|------|------|
| BUILD_LOG.md | 159 | 🟡 考虑移除 | 构建日志，已完成项目初始化，仅历史参考价值 |
| DIAGNOSTIC_REPORT.md | 238 | 🔴 建议移除 | 代码诊断报告，标记了多个"待修"Bug，暴露项目质量问题，对用户无价值 |
| alpha-miner-steps-wsl2.md | 474 | 🔴 建议移除 | Claude Code 执行指令，纯内部开发过程，与 GitHub 开源用户无关 |
| code-quality-hardening.md | 1001 | 🟡 考虑移除 | 代码质量加固规划，内部工作文档 |
| evolution-engine-v2-upgrade.md | 432 | 🟡 考虑移除 | 升级规划文档，已完成，仅历史参考 |
| factor-mining-v2.md | 674 | 🟡 考虑移除 | 因子挖掘 v2 规划，已完成 |
| narrative-strategy-upgrade.md | 825 | 🟡 考虑移除 | 叙事策略升级规划，已完成 |
| strategy-backtest-upgrade.md | 973 | 🟡 考虑移除 | 策略回测升级规划，已完成 |

**建议**: 创建 `docs/archive/` 目录，将上述文件移入。或者直接从 git 移除（可从 git history 恢复）。
保留的文档应只有：`README.md`, `CLAUDE.md`, `DOCS.md`。

---

## 八、其他发现

### 8.1 docs/integration_report_a_share_sentiment.md 已提交但未在 README/DOCS 中提及
- **严重程度**: 🟢 可选
- **修复**: 在 DOCS.md 项目文件表中添加此文件，或移入 docs/archive/

### 8.2 CLAUDE.md 中 Harness Plugin 占比过大
- **文件**: CLAUDE.md 第 83-112 行（30 行/112 行 = 27%）
- **严重程度**: 🟢 可选

### 8.3 README 中策略子系统缺少完整功能描述
- **文件**: README.md 第 108-125 行
- **当前**: 只提到 list/backtest/evolve/scan
- **实际**: 策略子系统还包含信号扫描（signal）、个股推荐（recommend）、风控（chase_protection）、自适应权重（adaptive_weights）、反馈环（feedback）等
- **严重程度**: 🟢 可选

---

## 总结

### 必须修（🔴）: 9 个

| # | 文件 | 问题 |
|---|------|------|
| 1 | README.md:41 | 因子名 `zt_ratio` → `zt_dt_ratio` |
| 2 | README.md:31,143 | 测试数量 288 → 349 |
| 3 | CLAUDE.md:34-49 | 数据库表缺少 5 张 |
| 4 | DOCS.md:35,104 | 因子名 `zt_ratio` → `zt_dt_ratio` |
| 5 | DOCS.md:98,511 | 测试数量 288 → 349 |
| 6 | DOCS.md:577-603 | settings.yaml 配置示例过时（anthropic → deepseek） |
| 7 | .gitignore | 缺少 `logs/` 排除（已提交日志文件） |
| 8 | .gitignore | 缺少 `signals/` 排除（已提交信号文件） |
| 9 | DIAGNOSTIC_REPORT.md | 含"待修"Bug 列表，建议从 GitHub 移除 |

### 建议修（🟡）: 15 个

| # | 文件 | 问题 |
|---|------|------|
| 1 | README.md:20 | 数据源数量 6 → 7 |
| 2 | README.md:11-18 | CLI 目录树缺 signal/query/recommend |
| 3 | README.md | 未覆盖 signal/recommend/query 命令 |
| 4 | README.md:29 | config/ 目录树不完整 |
| 5 | CLAUDE.md:8 | Python 版本 3.12+ → >=3.11 |
| 6 | CLAUDE.md:12-27 | 项目结构缺 narrative/strategy/pipeline |
| 7 | CLAUDE.md:51-74 | 常用命令缺 signal/recommend/query |
| 8 | DOCS.md:532-556 | 测试文件列表不完整 |
| 9 | DOCS.md:73-78 | strategy/ 目录树缺 12 个文件 |
| 10 | DOCS.md:570 | snownlp 替代描述与代码矛盾 |
| 11 | DOCS.md:91-93 | config/ 目录树缺 recommend.yaml |
| 12 | .gitignore:8 | uv.lock 不应忽略 |
| 13 | .gitignore:9-11 | reports/ 规则冗余 |
| 14 | config/ | factor_aliases.yaml 未加入 git |
| 15 | pyproject.toml | tushare 依赖无代码引用 |

### 可选修（🟢）: 6 个

| # | 问题 |
|---|------|
| 1 | README.md:32 Python 版本 `>= 3.11` vs `pyproject.toml` 一致性 |
| 2 | CLAUDE.md Harness Plugin 篇幅过大 |
| 3 | CLAUDE.md 开发原则可补充更多约束 |
| 4 | docs/ 下的归档文档建议移入 docs/archive/ |
| 5 | scripts/ 临时调试脚本建议清理 |
| 6 | README.md 策略子系统描述可更全面 |

### 建议删除的文件

1. **DIAGNOSTIC_REPORT.md** — 含暴露性 Bug 列表
2. **alpha-miner-steps-wsl2.md** — 纯内部 Claude Code 执行指令
3. 考虑将 BUILD_LOG.md, code-quality-hardening.md, evolution-engine-v2-upgrade.md, factor-mining-v2.md, narrative-strategy-upgrade.md, strategy-backtest-upgrade.md 移入 `docs/archive/`

### 建议新增到 git 的文件

1. **config/factor_aliases.yaml** — 因子中文别名，不含敏感信息
2. **uv.lock** — 依赖锁定文件（先从 .gitignore 移除）

### 需要提交的已修改文件

1. **pyproject.toml** — 添加了 tushare 依赖（需评估是否需要）

### 已提交但应从 git 移除的运行时产物

1. `logs/hourly/mine_*.log` (5 个文件)
2. `logs/mining_cron.log`
3. `reports/2026-04-20_brief.txt`
4. `signals/2026-04-24.txt`, `signals/2026-04-26.txt`

执行命令：
```bash
git rm --cached logs/ reports/2026-04-20_brief.txt signals/
echo -e "\nlogs/\nsignals/" >> .gitignore
# 然后修复 .gitignore 中的 reports/ 冗余行
```
