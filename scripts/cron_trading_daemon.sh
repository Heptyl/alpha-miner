#!/bin/bash
# 盘中交易守护进程启动脚本 v2
# 用法: bash scripts/cron_trading_daemon.sh
# cron会在每天9:25启动, 15:10后cron关闭守护进程
#
# v2改进:
#   1. 启动前清理__pycache__防止旧代码
#   2. 启动前验证ML候选和策略B候选可用
#   3. 验证失败仍启动(但记录告警)

cd "$(dirname "$0")/.."

LOG_DIR="output/trader/daemon_logs"
mkdir -p "$LOG_DIR"
PAUSE_FILE="$LOG_DIR/daemon.pause"

TODAY=$(date +%Y-%m-%d)
LOG_FILE="$LOG_DIR/daemon_${TODAY}.log"
NOW=$(date '+%H:%M:%S')

log() { echo "[$(date '+%H:%M:%S')] $1" >> "$LOG_FILE"; }

log "========== 守护进程启动流程 =========="

if [ -f "$PAUSE_FILE" ]; then
    log "检测到维护暂停标记, 拒绝启动"
    exit 0
fi

# 1. 先检查是否已在运行，避免cron触碰正在运行进程的文件。
PID_FILE="$LOG_DIR/daemon.pid"
if [ -f "$PID_FILE" ]; then
    OLD_PID=$(cat "$PID_FILE")
    if kill -0 "$OLD_PID" 2>/dev/null; then
        log "守护进程已在运行 (PID=$OLD_PID), 跳过"
        exit 0
    fi
    rm -f "$PID_FILE"
fi

# daemon自身使用文件锁阻止并发启动，不再通过易失真的命令行模式猜测PID。
PENDING_FILE="output/trader/signals/pending_signals.json"
if [ -f "$PENDING_FILE" ]; then
    PENDING_COUNT=$(uv run python3 - <<'PY'
import json
from pathlib import Path
p = Path("output/trader/signals/pending_signals.json")
try:
    data = json.loads(p.read_text(encoding="utf-8"))
except Exception:
    data = []
print(sum(1 for s in data if s.get("status") in ("pending", "executing")))
PY
)
    if [ "$PENDING_COUNT" != "0" ]; then
        log "存在${PENDING_COUNT}条待执行预告, 拒绝启动以避免丢失信号"
        exit 1
    fi
fi

# 2. 模拟盘发布门禁。交易依赖、数据库或策略模式异常时拒绝启动。
log "--- 执行 paper release gate ---"
if ! uv run python scripts/release_gate.py --level paper --tests none \
    --json-out output/release/startup_gate.json >> "$LOG_FILE" 2>&1; then
    log "paper release gate 失败, 拒绝启动"
    exit 1
fi
log "OK: paper release gate 通过"

# 3. 清理Python缓存(防止旧代码生效)
find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null
log "OK: __pycache__已清理"

# 4. 启动前验证候选数据
log "--- 验证候选数据 ---"
VERIFY=$(uv run python3 -c "
import json, sys

errors = []

# 3a. ML预测文件
try:
    pred = json.load(open('output/ml/latest_prediction.json'))
    cands = pred.get('all_top') or pred.get('predictions') or []
    if len(cands) < 5:
        errors.append(f'ML候选仅{len(cands)}只(建议>=5)')
    else:
        print(f'  ML预测: {len(cands)}只候选 OK')
except Exception as e:
    errors.append(f'ML预测文件异常: {e}')

# 3b. 涨停池(策略B数据源)
import sqlite3
db = sqlite3.connect('data/alpha_miner.db')
zt_date = db.execute('SELECT MAX(trade_date) FROM zt_pool').fetchone()[0]
zt_cnt = db.execute(f\"SELECT COUNT(*) FROM zt_pool WHERE trade_date='{zt_date}'\").fetchone()[0]
if zt_cnt < 10:
    errors.append(f'涨停池数据不足: {zt_date}仅{zt_cnt}条')
else:
    print(f'  涨停池: {zt_date} {zt_cnt}条 OK')

# 3c. 日K线完整性
dp_cnt = db.execute(f\"SELECT COUNT(*) FROM daily_price WHERE trade_date='{zt_date}'\").fetchone()[0]
if dp_cnt < 1000:
    errors.append(f'日K线数据不足: {zt_date}仅{dp_cnt}只')
else:
    print(f'  日K线: {zt_date} {dp_cnt}只 OK')

db.close()

if errors:
    print('WARNING:')
    for e in errors:
        print(f'  ⚠️ {e}')
    sys.exit(1)
else:
    print('ALL CHECKS PASSED')
" 2>&1)
echo "$VERIFY" >> "$LOG_FILE"

if echo "$VERIFY" | grep -q "WARNING"; then
    log "⚠️ 候选验证有警告(仍继续启动)"
else
    log "OK: 候选验证通过"
fi

# 5. 启动守护进程
# 使用setsid创建新session, 防止cron job结束时SIGHUP杀掉守护进程
# (2026-05-14教训: nohup在cron中不可靠, 进程随cron session结束被杀)
log "启动守护进程..."
setsid uv run python -m src.trader.trading_daemon start >> "$LOG_FILE" 2>&1 &
DAEMON_PID=$!
sleep 1

# 6. 等待3秒确认启动成功
sleep 3
REAL_PID=""
if [ -f "$PID_FILE" ]; then
    REAL_PID=$(cat "$PID_FILE")
fi
if [ -n "$REAL_PID" ] && kill -0 "$REAL_PID" 2>/dev/null; then
    STARTUP_OK=$(uv run python3 - <<'PY'
import json
import sqlite3
import time
from pathlib import Path

heartbeat_path = Path("output/trader/daemon_logs/daemon.heartbeat.json")
deadline = time.time() + 15
last_error = "heartbeat missing"

while time.time() < deadline:
    try:
        payload = json.loads(heartbeat_path.read_text(encoding="utf-8"))
        age = time.time() - float(payload["timestamp"])
        if age <= 30 and payload.get("state") in {
            "starting", "idle", "scanning", "scan_complete"
        }:
            conn = sqlite3.connect("data/alpha_miner.db")
            running = conn.execute(
                "SELECT COUNT(*) FROM daemon_runs WHERE status='running'"
            ).fetchone()[0]
            conn.close()
            if running == 1:
                print("ok")
                raise SystemExit(0)
            last_error = f"running daemon rows={running}"
        else:
            last_error = f"stale/invalid heartbeat: {payload}"
    except Exception as exc:
        last_error = str(exc)
    time.sleep(1)

print(last_error)
raise SystemExit(1)
PY
)
    if [ "$STARTUP_OK" = "ok" ]; then
        log "OK: 守护进程启动成功 PID=$REAL_PID, 心跳和运行账本正常"
    else
        log "守护进程启动后验收失败: $STARTUP_OK"
        kill "$REAL_PID" 2>/dev/null || true
        exit 1
    fi
elif kill -0 "$DAEMON_PID" 2>/dev/null; then
    log "守护进程仅检测到wrapper PID=$DAEMON_PID, 未生成正式PID"
    kill "$DAEMON_PID" 2>/dev/null || true
    exit 1
else
    log "❌ 守护进程启动失败! 查看日志: $LOG_FILE"
    exit 1
fi
