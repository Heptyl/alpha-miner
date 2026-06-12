# Alpha Miner 稳定测试版运行手册

基线日期：2026-06-10

## 当前运行边界

- `RISK_MODE=paper`，禁止连接真实资金。
- 策略 A 为 `paper`；B、C、C1、C2 为 `shadow`。
- B 当前版本为 `B_crash_v2_shadow_20260610`，负期望证据消失前不得恢复 paper。
- daemon 默认保留维护暂停标记，发布检查完成前不得移除。

## 发布验收

代码级验收要求 daemon 已暂停：

```bash
uv run python scripts/release_gate.py --level code --tests full
```

模拟盘启动验收要求：

```bash
uv run python scripts/release_gate.py --level paper --tests quick
```

`paper` 门禁必须同时满足：

- 数据库 `quick_check` 通过，核心表完整。
- 无存活 daemon、无 pending/executing 信号。
- 策略运行模式固定为 A paper，其余 shadow。
- 最新交易日日线不少于 3500 只，OHLC 和 pre_close 有效率不低于 95%。
- 策略 A 使用的涨停池必须与最新日线同日，且至少有 1 只二连板。
- ML 当前不参与 paper 策略决策；同分或过期会显示 WARN，但不会阻断 A。

门禁报告保存在 `output/release/`。任何 FAIL 都不得启动 daemon。

## 每日数据刷新

收盘后执行：

```bash
uv run python -m cli.collect --today
uv run python scripts/release_gate.py --level paper --tests quick
```

若完整采集被外围接口阻塞，可只补全核心日线：

```bash
uv run python - <<'PY'
from datetime import date
from src.data.storage import Storage
from src.data.sources.akshare_price import fetch_baostock_full, save

trade_date = date.today().isoformat()
db = Storage()
db.init_db()
save(fetch_baostock_full(trade_date), db, dedup=True)
PY
```

## 启停流程

启动前先通过 `paper` 门禁，然后解除维护锁并使用统一启动脚本：

```bash
rm output/trader/daemon_logs/daemon.pause
bash scripts/cron_trading_daemon.sh
uv run python -m src.trader.trading_daemon status
```

`cron_trading_daemon.sh` 会再次执行 paper 门禁；失败时自动拒绝启动。

停止时先写维护锁，避免 watchdog 自动拉起，再终止 PID：

```bash
touch output/trader/daemon_logs/daemon.pause
PID=$(cat output/trader/daemon_logs/daemon.pid 2>/dev/null) && kill "$PID"
```

禁止删除或手工改写 pending 信号。若存在 pending/executing 信号，应先审计其来源和状态。

## 日终复核

```bash
uv run python scripts/health_check.py
uv run python scripts/daily_strategy_review.py --date "$(date +%F)"
```

重点查看：

- daemon 是否只有一个 run，是否正常关闭。
- paper 成交是否都有 `strategy_version`、`run_id`、`config_hash`。
- shadow 信号是否按“策略版本 + 股票 + 日期”去重。
- T+3 后验是否回填，是否出现异常亏损或数据缺失。
- A 的模拟成交与 B/C 系列 shadow 信号是否严格隔离。

## 策略晋级规则

- 少于 30 笔独立样本：继续采样，不判断有效。
- PF 小于 1 或期望收益不为正：保持 shadow 或暂停研究。
- 晋级评审最低要求：PF 不低于 1.25、期望收益为正、单笔最大亏损不低于 -8%。
- 实盘前仍需至少 100-200 笔不改参数模拟交易，并覆盖上涨、震荡、下跌市场。

满足工程门禁只代表系统可稳定测试，不代表策略能够盈利。
