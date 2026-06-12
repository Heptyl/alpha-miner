# Alpha Miner 收盘后自动流水线分析

## 一、所有可用脚本/命令总览

### A. CLI 入口命令 (via `python -m cli <command>`)

---

#### 1. `cli daily` — 一条龙流水线
- **调用方式**: `uv run python -m cli daily [--date YYYY-MM-DD] [--no-push] [--no-collect] [--top N] [--capital N]`
- **功能**: 依次执行：数据采集 → 因子计算 → 盘后推荐(+复盘) → 交易计划 → 微信推送
- **大致耗时**: 5~15 分钟（取决于网络和股票数量）
- **依赖**: 需要网络（采集），需要 akshare API 可用
- **注意**: 这是一个"快速版"流水线，比 `daily_run.sh` 少了 IC/漂移/Regime/进化/剧本等步骤

#### 2. `cli collect` — 数据采集
- **调用方式**: `uv run python -m cli collect --today` 或 `--date YYYY-MM-DD` 或 `--backfill N`
- **功能**: 采集指定日期的全市场数据（涨停池、炸板池、强势股、龙虎榜、日K线、资金流向、新闻、概念映射）→ 聚合 market_emotion + concept_daily
- **大致耗时**: 2~5 分钟（今日模式只拉重点股票约100~200只），回填模式更长
- **依赖**: **必须网络**（akshare API），需东财/同花顺接口可用

#### 3. `cli backtest --compute-today` — 因子计算
- **调用方式**: `uv run python -m cli backtest --compute-today`
- **功能**: 计算当日所有已注册因子的值，写入 factor_values 表
- **大致耗时**: 1~3 分钟
- **依赖**: **不需要网络**，依赖 factor_values 表已有数据（需先 `cli collect`）

#### 4. `cli drift` — 漂移检测
- **调用方式**: `uv run python -m cli drift --date YYYY-MM-DD [--ic-window 20]`
- **功能**: 生成漂移报告（IC衰减、CUSUM变点检测、因子质量评估）
- **大致耗时**: 30秒~2分钟
- **依赖**: **不需要网络**，依赖 ic_series 表已有数据（需先有因子值+IC计算）

#### 5. `cli mine evolve` — 因子进化
- **调用方式**: `uv run python -m cli mine evolve --generations N --population N`
- **功能**: 用遗传算法优化因子权重组合
- **大致耗时**: 3~10 分钟（取决于参数）
- **依赖**: **不需要网络**，依赖 factor_values + daily_price

#### 6. `cli recommend` — 盘后推荐
- **调用方式**: `uv run python -m cli recommend --date YYYY-MM-DD --top N`
- **功能**: 生成 TOP N 推荐股票（含买入点位、止损止盈）
- **大致耗时**: 1~3 分钟
- **依赖**: **不需要网络**，依赖 factor_values + daily_price（需先完成采集+因子计算）

#### 7. `cli tradeplan` — 交易计划
- **调用方式**: `uv run python -m cli tradeplan --date YYYY-MM-DD [--capital N]`
- **功能**: 生成次日交易计划（选股+仓位+止损止盈+具体操作建议）
- **大致耗时**: 30秒~1分钟
- **依赖**: **不需要网络**，依赖推荐结果和 factor_values

#### 8. `cli report` — 日报/盘后简报
- **调用方式**: `uv run python -m cli report --date YYYY-MM-DD [--brief]`
- **功能**: 生成市场日报或盘后简报
- **大致耗时**: 30秒~1分钟
- **依赖**: **不需要网络**，依赖 market_emotion + zt_pool + daily_price

#### 9. `cli script` — 市场剧本
- **调用方式**: `uv run python -m cli script --date YYYY-MM-DD --save`
- **功能**: 生成当日市场剧本（主线+支线+情绪推演）
- **大致耗时**: 1~2分钟
- **依赖**: **不需要网络**，依赖采集数据

#### 10. `cli signal` — 次日选股信号
- **调用方式**: `uv run python -m cli signal --date YYYY-MM-DD`
- **功能**: 生成次日选股信号
- **大致耗时**: 1~2分钟
- **依赖**: **不需要网络**，依赖因子数据

#### 11. `cli query` — 数据查询
- **调用方式**: `uv run python -m cli query <subcommand>`
- **功能**: 查询股票数据、市场概览
- **大致耗时**: <10秒
- **依赖**: 不需要网络

#### 12. `cli replay` — 复盘
- **调用方式**: `uv run python -m cli replay --date YYYY-MM-DD`
- **功能**: 复盘昨日剧本
- **大致耗时**: 30秒
- **依赖**: 不需要网络，依赖剧本数据

#### 13. `cli strategy` — 策略管理
- **调用方式**: `uv run python -m cli strategy <list|backtest|evolve|scan>`
- **功能**: 策略的增删改查、回测、进化、扫描
- **大致耗时**: 视具体操作
- **依赖**: 不需要网络

---

### B. Shell 脚本

---

#### 14. `scripts/daily_run.sh` — 完整每日流水线 (v2)
- **调用方式**: `bash scripts/daily_run.sh [--skip-collect] [--skip-push]`
- **功能**: 最完整的收盘后流水线：
  1. Pre-flight 数据校验
  2. 数据去重
  3. 采集数据 (`cli collect --today`)
  4. 计算因子 (`cli backtest --compute-today`)
  5. IC 管线 (`run_ic_pipeline`)
  6. Regime 识别 (`run_regime_pipeline`)
  7. 漂移检测 (`cli drift`)
  8. 生成盘后简报
  9. 因子进化 (`cli mine evolve`)
  10. 市场剧本 (`cli script`)
  11. 微信推送 (`send_wechat.py`)
- **大致耗时**: 10~25 分钟
- **依赖**: 需要网络（步骤3），后续步骤不需要
- **推荐时间**: 交易日 15:40 后运行

#### 15. `scripts/hourly_mine.sh` — 每小时因子挖掘
- **调用方式**: `bash scripts/hourly_mine.sh`
- **功能**: 轻量版流水线：采集→因子计算→漂移检测→进化
- **大致耗时**: 5~15 分钟
- **依赖**: 需要网络

#### 16. `scripts/setup_cron.sh` — 定时任务安装
- **调用方式**: `bash scripts/setup_cron.sh` / `bash scripts/setup_cron.sh --remove`
- **功能**: 安装 crontab：23:00 晚间推荐，08:30 早间复盘
- **依赖**: 无

#### 17. `scripts/ci_local.sh` — 本地 CI 质量门禁
- **调用方式**: `bash scripts/ci_local.sh`
- **功能**: ruff lint + pytest + coverage
- **大致耗时**: 1~3 分钟
- **依赖**: 不需要网络

---

### C. Python 脚本 (scripts/)

---

#### 18. `scripts/evening_recommend.py` — 晚间推荐 (cron 23:00)
- **调用方式**: `uv run python scripts/evening_recommend.py [--dry-run]`
- **功能**: 确认交易日 → 采集数据 → 计算因子 → 生成 TOP 5 → 推送微信
- **大致耗时**: 5~10 分钟
- **依赖**: 需要网络，依赖 LLM（推送格式化）

#### 19. `scripts/evening_review.py` — 盘后复盘 (cron 15:30)
- **调用方式**: `uv run python scripts/evening_review.py [--dry-run] [--date YYYY-MM-DD]`
- **功能**: 确认交易日 → 采集数据 → 对比昨日推荐 vs 今日实际 → 生成复盘 → 推送
- **大致耗时**: 3~5 分钟
- **依赖**: 需要网络（采集），依赖昨日推荐结果

#### 20. `scripts/morning_reconfirm.py` — 早间复盘再确认 (cron 08:30)
- **调用方式**: `uv run python scripts/morning_reconfirm.py [--dry-run] [--date YYYY-MM-DD]`
- **功能**: 加载昨晚推荐 → 检查停牌/ST/利空 → 生成确认/调整/剔除报告 → 推送
- **大致耗时**: 2~5 分钟
- **依赖**: 需要网络（新闻检查），依赖昨晚推荐结果

#### 21. `scripts/deep_review.py` — LLM 深度复盘
- **调用方式**: `uv run python scripts/deep_review.py [--date YYYY-MM-DD]`
- **功能**: 对比昨日推荐 vs 今日实际 + LLM 深度推理每只股的操作逻辑
- **大致耗时**: 5~15 分钟（取决于 LLM API 响应速度）
- **依赖**: **需要 LLM API**（网络），依赖昨日推荐 + 今日行情数据

#### 22. `scripts/deep_pick.py` — LLM 深度选股分析
- **调用方式**: `uv run python scripts/deep_pick.py [--date YYYY-MM-DD]`
- **功能**: 对全部 5 只推荐股进行 LLM 深度推理，给出操作建议
- **大致耗时**: 5~15 分钟
- **依赖**: **需要 LLM API**（网络），依赖推荐结果 + 行情数据

#### 23. `scripts/compute_factors.py` — 批量因子计算
- **调用方式**: `uv run python scripts/compute_factors.py --date YYYY-MM-DD` 或 `--all`
- **功能**: 批量计算所有因子并写入 factor_values（独立于 CLI，直接调 registry）
- **大致耗时**: 1~3 分钟/天，全量更久
- **依赖**: 不需要网络，依赖 zt_pool + daily_price + lhb_detail 等数据
- **注意**: 有一个 bug：使用了 `sqlite3` 但未 `import sqlite3`

#### 24. `scripts/compute_ic.py` — IC 计算并持久化
- **调用方式**: `uv run python scripts/compute_ic.py`
- **功能**: 计算所有因子的 IC 时序，写入 ic_series 表
- **大致耗时**: 1~3 分钟
- **依赖**: 不需要网络，依赖 factor_values + daily_price

#### 25. `scripts/check_ic.py` — 手动 IC 检查
- **调用方式**: `uv run python scripts/check_ic.py`
- **功能**: 计算并打印各因子的 IC 均值/ICIR/胜率
- **大致耗时**: 10~30秒
- **依赖**: 不需要网络，依赖 factor_values + daily_price

#### 26. `scripts/check_ic_all.py` — 全因子 IC 验证
- **调用方式**: `uv run python scripts/check_ic_all.py`
- **功能**: 全因子 IC 验证（扩展版）
- **大致耗时**: 30秒~1分钟
- **依赖**: 不需要网络

#### 27. `scripts/check_ic_daily.py` — 逐日 IC 检查
- **调用方式**: `uv run python scripts/check_ic_daily.py`
- **功能**: 逐日检查 theme_crowding 因子的 IC
- **大致耗时**: <10秒
- **依赖**: 不需要网络

#### 28. `scripts/check_ic_full.py` — 全量 IC 检查
- **调用方式**: `uv run python scripts/check_ic_full.py`
- **功能**: 全因子 IC 验证（含更多因子）
- **大致耗时**: 30秒~1分钟
- **依赖**: 不需要网络

#### 29. `scripts/send_wechat.py` — 微信推送
- **调用方式**: `uv run python scripts/send_wechat.py [--brief] [--date YYYY-MM-DD]` 或 `echo "msg" | uv run python scripts/send_wechat.py`
- **功能**: 通过 Hermes WeChat gateway 推送消息
- **大致耗时**: 5~15秒
- **依赖**: 需要 Hermes Agent 运行中 + 网络

#### 30. `scripts/validate_data.py` — 数据质量校验
- **调用方式**: `uv run python scripts/validate_data.py`
- **功能**: 检查 daily_price/zt_pool/fund_flow/factor_values 数据质量，自动清理脏数据
- **大致耗时**: 30秒~2分钟
- **依赖**: 不需要网络

#### 31. `scripts/analyze_today.py` — 今日行情分析
- **调用方式**: `uv run python scripts/analyze_today.py`
- **功能**: 打印今日市场情绪总览、涨停板块分布、连板梯队
- **大致耗时**: <10秒
- **依赖**: 不需要网络
- **注意**: 硬编码了日期 "2026-04-24"，需修改后使用

#### 32. `scripts/cleanup_duplicates.py` — 清理重复数据
- **调用方式**: `uv run python scripts/cleanup_duplicates.py [--dry-run] [--tables t1 t2 ...]`
- **功能**: 清理数据库中的重复记录（保留最新 snapshot_time）
- **大致耗时**: 10~60秒
- **依赖**: 不需要网络

#### 33. `scripts/check_data.py` — 快速数据检查
- **调用方式**: `uv run python scripts/check_data.py`
- **功能**: 查看 daily_price 最新日期和指定日期的行数
- **大致耗时**: <5秒
- **依赖**: 不需要网络

#### 34. `scripts/sim_trade.py` — 模拟交易回测
- **调用方式**: `uv run python scripts/sim_trade.py`
- **功能**: 模拟交易验证推荐策略
- **大致耗时**: <10秒
- **依赖**: 不需要网络
- **注意**: 硬编码了日期 "2026-04-24"/"2026-04-28"

#### 35. `scripts/test_akshare.py` — 测试 akshare 连接
- **调用方式**: `uv run python scripts/test_akshare.py`
- **功能**: 快速验证 akshare 能否正常拉取数据
- **大致耗时**: 5~15秒
- **依赖**: 需要网络

#### 36. `scripts/intraday_alert.py` — 盘中买点分析
- **调用方式**: `uv run python scripts/intraday_alert.py [--date YYYY-MM-DD]`
- **功能**: 基于昨日推荐，结合实时行情给出具体买点建议（9:25 集合竞价后）
- **大致耗时**: 30秒~1分钟
- **依赖**: **需要网络**（实时行情），依赖昨日推荐结果
- **场景**: 盘中使用，不属于收盘后流水线

#### 37. `scripts/intraday_analysis.py` — 盘中详细买点分析
- **调用方式**: `uv run python scripts/intraday_analysis.py [--date YYYY-MM-DD]`
- **功能**: 基于实时行情的详细买点分析（09:25/10:00/10:30 三次推送）
- **大致耗时**: 30秒~1分钟
- **依赖**: **需要网络**，依赖昨日推荐
- **场景**: 盘中使用，不属于收盘后流水线

---

### D. 回填类脚本 (scripts/backfill_*.py)

这些是**一次性/维护类**脚本，不属于日常收盘流水线：

| # | 脚本 | 调用方式 | 功能 | 耗时 | 网络 |
|---|------|----------|------|------|------|
| 38 | `backfill_all.py` | `python scripts/backfill_all.py [--days 30]` | 综合回填所有数据源 | 30~60分钟 | 是 |
| 39 | `backfill_smart.py` | `python scripts/backfill_smart.py --days 30 [--phase N]` | 智能回填(分阶段+超时) | 10~30分钟 | 是 |
| 40 | `backfill_daily_price.py` | `python scripts/backfill_daily_price.py` | 补齐指定日期K线 | 10~30分钟 | 是 |
| 41 | `backfill_lhb.py` | `python scripts/backfill_lhb.py [--days 60]` | 回填龙虎榜 | 5~15分钟 | 是 |
| 42 | `backfill_pools.py` | `python scripts/backfill_pools.py [--days 60]` | 回填涨停池/强势池 | 5~15分钟 | 是 |
| 43 | `backfill_concepts.py` | `python scripts/backfill_concepts.py` | 扩充概念映射 | 5~10分钟 | 是 |
| 44 | `backfill_factor_stocks.py` | `python scripts/backfill_factor_stocks.py` | 为因子股票补K线(腾讯源) | 10~30分钟 | 是 |
| 45 | `backfill_tencent.py` | `python scripts/backfill_tencent.py [--days 30]` | 腾讯源回填K线 | 10~30分钟 | 是 |
| 46 | `backfill_daily_tencent.py` | `python scripts/backfill_daily_tencent.py` | 腾讯源批量补K线 | 10~30分钟 | 是 |
| 47 | `backfill_sina.py` | `python scripts/backfill_sina.py` | 新浪源回填K线 | 10~30分钟 | 是 |
| 48 | `backfill_windows_curl.py` | `python scripts/backfill_windows_curl.py --days N` | Windows curl绕过WSL网络回填 | 10~30分钟 | 是 |
| 49 | `fill_daily_price.py` | `python scripts/fill_daily_price.py --start --end` | 批量日K线补全 | 10~30分钟 | 是 |
| 50 | `fill_daily_price_v2.py` | `python scripts/fill_daily_price_v2.py` | 新浪API补K线 | 10~30分钟 | 是 |
| 51 | `quick_backfill.py` | `python scripts/quick_backfill.py` | 快速回填(线程池+腾讯) | 5~15分钟 | 是 |

---

### E. 调试/临时脚本（不建议纳入流水线）

| # | 脚本 | 说明 |
|---|------|------|
| 52 | `_quick_recommend.py` | 快速推荐（硬编码日期） |
| 53 | `_run_recommend_fast.py` | 快速推荐（硬编码 2026-05-06） |
| 54 | `_run_recommend_now.py` | 手动跑推荐（硬编码 2026-05-06） |
| 55 | `run_recommend_only.py` | 仅推荐（硬编码 2026-05-06） |
| 56 | `evening_recommend_lite.py` | 精简推荐（硬编码 2026-04-30） |
| 57 | `_check_db.py` | 快速查 DB 状态 |

---

### F. 数据源采集器 (src/data/sources/)

| 模块 | 采集内容 | API | 备注 |
|------|----------|-----|------|
| `akshare_zt_pool.py` | 涨停池、炸板池、强势股池 | 东财 akshare | 轻量，先采集 |
| `akshare_lhb.py` | 龙虎榜详情 | 东财 akshare | 轻量 |
| `akshare_price.py` | 日K线(今日重点/历史全量) | 东财 akshare | 较重，后采集 |
| `akshare_fund_flow.py` | 资金流向 | 东财 akshare | 中等 |
| `akshare_news.py` | 个股新闻 | 东财 akshare | 限流 0.5s/只 |
| `akshare_concept.py` | 概念映射 | 同花顺 akshare | 不稳定 |
| `fundamentals.py` | 基本面数据 | - | 辅助 |
| `news_miner.py` | 新闻挖掘 | - | 辅助 |

---

### G. 因子计算模块 (src/factors/)

因子注册在 `config/factors.yaml`，通过 `FactorRegistry` 加载：

**公式因子 (formula/)**:
- `turnover_rank` — 换手率排名
- `lhb_institution` — 龙虎榜机构因子
- `consecutive_board` — 连板因子
- `main_flow_intensity` — 主力资金强度
- `zt_ratio` — 涨跌比

**叙事因子 (narrative/)**:
- `theme_crowding` — 主题拥挤度
- `leader_clarity` — 龙头清晰度
- `narrative_velocity` — 叙事速度
- `theme_lifecycle` — 主题生命周期

---

### H. 漂移检测模块 (src/drift/)

| 模块 | 功能 |
|------|------|
| `ic_tracker.py` | IC 时序计算与持久化 |
| `cusum.py` | CUSUM 变点检测 |
| `regime.py` | 市场状态识别（牛市/熊市/震荡） |
| `report.py` | 漂移报告生成 |
| `daily_report.py` | 日报生成 |
| `daily_brief.py` | 盘后简报生成 |
| `push.py` | 微信推送（通过 Hermes gateway） |

---

## 二、推荐的任务编排顺序

### 收盘后主流水线（15:30~16:00 启动）

```
时间线:  T日 15:30 收盘后开始
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

阶段1: 数据准备 [需网络, ~3-5分钟]
  ├── Step 1.1  数据去重
  │   └── `uv run python scripts/cleanup_duplicates.py`
  │       耗时: ~10秒
  │
  ├── Step 1.2  数据采集 (核心!)
  │   └── `uv run python -m cli collect --today`
  │       调用: zt_pool → zb_pool → strong_pool → lhb_detail
  │             → daily_price → fund_flow → news → concept_mapping
  │             → 聚合 market_emotion + concept_daily
  │       耗时: ~3-5分钟 (网络依赖)
  │
  └── Step 1.3  数据质量校验
      └── `uv run python scripts/validate_data.py`
          耗时: ~30秒

阶段2: 因子计算 [纯本地, ~2-3分钟]
  ├── Step 2.1  计算今日因子值
  │   └── `uv run python -m cli backtest --compute-today`
  │       耗时: ~1-3分钟
  │       依赖: Step 1.2 完成
  │
  └── Step 2.2  IC 管线
      └── 内联 Python:
          from src.pipeline.runner import run_ic_pipeline
          run_ic_pipeline(db)
      耗时: ~1-2分钟
      依赖: Step 2.1 完成

阶段3: 市场分析 [纯本地, ~1-2分钟]
  ├── Step 3.1  Regime 识别
  │   └── 内联 Python:
  │       from src.pipeline.runner import run_regime_pipeline
  │       run_regime_pipeline(db)
  │   耗时: ~10秒
  │   依赖: Step 2.2 (需要 ic_series)
  │
  ├── Step 3.2  漂移检测
  │   └── `uv run python -m cli drift --date $DATE`
  │   耗时: ~30秒-1分钟
  │   依赖: Step 2.2 (需要 ic_series)
  │
  └── Step 3.3  生成盘后简报
      └── 内联 Python:
          from src.drift.daily_report import DailyReport
          DailyReport(db).generate(date)
      耗时: ~10-30秒
      依赖: Step 1+2 完成

阶段4: 推荐与交易计划 [纯本地, ~2-3分钟]
  ├── Step 4.1  盘后复盘 (对比昨日推荐 vs 今日实际)
  │   └── `uv run python scripts/evening_review.py --date $DATE`
  │   耗时: ~1-2分钟
  │   依赖: 有昨日推荐 + 今日数据
  │
  ├── Step 4.2  盘后推荐 (今日 TOP N)
  │   └── `uv run python -m cli recommend --date $DATE --top 5`
  │   耗时: ~1-2分钟
  │   依赖: Step 2.1 (因子值)
  │
  └── Step 4.3  交易计划
      └── `uv run python -m cli tradeplan --date $DATE`
      耗时: ~30秒
      依赖: Step 4.2 (推荐结果)

阶段5: 高级分析 [可选, 纯本地, ~5-15分钟]
  ├── Step 5.1  因子进化 (轻量)
  │   └── `uv run python -m cli mine evolve --generations 3 --population 5`
  │   耗时: ~3-5分钟
  │   依赖: Step 2
  │
  ├── Step 5.2  市场剧本
  │   └── `uv run python -m cli script --date $DATE --save`
  │   耗时: ~30秒-1分钟
  │   依赖: Step 1+2
  │
  └── Step 5.3  LLM 深度复盘 (可选, 需 LLM API)
      └── `uv run python scripts/deep_review.py --date $DATE`
      耗时: ~5-15分钟
      依赖: Step 4.1 + LLM API

阶段6: 推送通知 [需网络/Hermes, ~10秒]
  └── Step 6.1  微信推送
      └── `uv run python scripts/send_wechat.py --brief --date $DATE`
      耗时: ~5-15秒
      依赖: Hermes Agent 运行中

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
总计: ~15-25分钟 (不含LLM深度复盘)
     ~25-40分钟 (含LLM深度复盘)
```

### 次日早盘流水线（08:30 启动）

```
Step 1: 早间复盘再确认
  └── `uv run python scripts/morning_reconfirm.py`
  功能: 检查推荐股有无停牌/ST/重大利空
  耗时: ~2-5分钟 (需网络查新闻)
  依赖: 昨晚推荐结果

Step 2: 盘中买点 (09:25 集合竞价后)
  └── `uv run python scripts/intraday_alert.py`
  功能: 根据竞价结果给出买点建议
  耗时: ~30秒
  依赖: 需要实时行情网络
```

---

## 三、推荐的 Cron 编排

```cron
# ── 收盘后主流程 ──
# 15:40 盘后复盘（对比昨日推荐 vs 今日实际）
40 15 * * 1-5 cd /home/ccy/alpha-miner && uv run python scripts/evening_review.py >> logs/evening_review_$(date +\%Y\%m\%d).log 2>&1

# 15:50 完整流水线（采集→因子→IC→漂移→推荐→推送）
50 15 * * 1-5 cd /home/ccy/alpha-miner && bash scripts/daily_run.sh >> logs/daily_run_$(date +\%Y\%m\%d).log 2>&1

# 22:00 LLM 深度复盘（可选，流水线结束后跑）
0 22 * * 1-5 cd /home/ccy/alpha-miner && uv run python scripts/deep_review.py >> logs/deep_review_$(date +\%Y\%m\%d).log 2>&1

# ── 次日早盘 ──
# 08:30 早间复盘再确认
30 8 * * 1-5 cd /home/ccy/alpha-miner && uv run python scripts/morning_reconfirm.py >> logs/morning_$(date +\%Y\%m\%d).log 2>&1

# 09:25 盘中买点推送
25 9 * * 1-5 cd /home/ccy/alpha-miner && uv run python scripts/intraday_alert.py >> logs/intraday_$(date +\%Y\%m\%d).log 2>&1
```

---

## 四、现有流水线对比

| 维度 | `cli daily` | `scripts/daily_run.sh` | `scripts/evening_recommend.py` |
|------|-------------|----------------------|-------------------------------|
| 采集 | ✅ | ✅ | ✅ |
| 去重 | ❌ | ✅ | ❌ |
| 因子计算 | ✅ | ✅ | ✅ |
| IC 管线 | ❌ | ✅ | ❌ |
| Regime | ❌ | ✅ | ❌ |
| 漂移检测 | ❌ | ✅ | ❌ |
| 盘后简报 | ❌ | ✅ | ❌ |
| 因子进化 | ❌ | ✅ | ❌ |
| 市场剧本 | ❌ | ✅ | ❌ |
| 推荐 | ✅ | ❌ (需额外加) | ✅ |
| 交易计划 | ✅ | ❌ | ❌ |
| 复盘 | ✅ (内含) | ❌ | ❌ |
| 微信推送 | ✅ | ✅ | ✅ |
| **完整度** | 中等 | 最完整 | 精简 |

**结论**: `scripts/daily_run.sh` 是目前最完整的收盘后流水线，但缺少推荐和交易计划步骤。建议以 `daily_run.sh` 为基础，在漂移检测之后插入推荐和交易计划步骤。

---

## 五、已知问题

1. **`compute_factors.py`**: 使用 `sqlite3` 但未 import，运行会报错
2. **`evening_recommend_lite.py`**: 硬编码 `trade_date = "2026-04-30"`
3. **`run_recommend_only.py`**: 硬编码 `trade_date = "2026-05-06"`
4. **`_run_recommend_fast.py`**: 硬编码 `trade_date = '2026-05-06'`
5. **`_run_recommend_now.py`**: 硬编码 `trade_date = '2026-05-06'`
6. **`_quick_recommend.py`**: 自动从 DB 获取最新日期（OK）
7. **`analyze_today.py`**: 硬编码日期 "2026-04-24"
8. **`sim_trade.py`**: 硬编码日期 "2026-04-24"/"2026-04-28"
9. **`quick_backfill.py`**: 硬编码日期 "20260427"/"20260428"
10. **`backfill_sina.py`**: 硬编码 "2026-04-28"
11. **`daily_run.sh` 缺少推荐步骤**: 建议在 Step 8(市场剧本) 之前加入推荐和交易计划
