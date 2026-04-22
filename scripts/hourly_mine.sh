#!/bin/bash
# Alpha Miner 每小时因子挖掘
# 用法: bash scripts/hourly_mine.sh
# 功能: 采集数据 → 计算因子 → 漂移检测 → 进化 → 记录日志
set -e

cd "$(dirname "$0")/.."
DATE=$(date +%Y-%m-%d)
HOUR=$(date +%H:%M:%S)
LOG_DIR="logs/hourly"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/mine_${DATE}_$(date +%H%M).log"

log() { echo "[$(date +%H:%M:%S)] $1" | tee -a "$LOG_FILE"; }

log "===== Alpha Miner Hourly: $DATE $HOUR ====="

# 1. 采集数据
log "[1/4] 采集数据..."
uv run python -m cli.collect --today >> "$LOG_FILE" 2>&1 || log "[WARN] 采集部分失败（可能非交易日）"

# 2. 检查是否有当日行情数据，无数据则跳过后续重量级步骤
HAS_DATA=$(uv run python -c "
import sqlite3
conn = sqlite3.connect('data/alpha_miner.db')
c = conn.cursor()
c.execute(\"SELECT COUNT(*) FROM daily_price WHERE trade_date = '$DATE'\")
n = c.fetchone()[0]
conn.close()
print('yes' if n > 0 else 'no')
" 2>/dev/null || echo "no")

if [ "$HAS_DATA" != "yes" ]; then
    log "[INFO] $DATE 无当日行情数据（非交易日或采集失败），跳过因子计算和进化"
    log "仅执行漂移检测（基于历史数据）..."
    uv run python -m cli.drift --date $DATE >> "$LOG_FILE" 2>&1 || log "[WARN] 漂移检测异常"
    log "===== Done (轻量模式) ====="
    echo "---SUMMARY---"
    echo "非交易日/无数据，轻量运行完成"
    tail -15 "$LOG_FILE" | grep -iE "(IC|ICIR|factor|drift|regime)" || echo "无关键指标"
    echo "---END---"
    exit 0
fi

# 3. 计算因子
log "[2/4] 计算因子..."
uv run python -m cli.backtest --compute-today >> "$LOG_FILE" 2>&1 || log "[WARN] 因子计算异常"

# 4. 漂移检测
log "[3/4] 漂移检测..."
uv run python -m cli.drift --date $DATE >> "$LOG_FILE" 2>&1 || log "[WARN] 漂移检测异常"

# 5. 因子进化（轻量：1代3种群）
log "[4/4] 因子进化..."
uv run python -m cli.mine evolve --generations 1 --population 3 >> "$LOG_FILE" 2>&1 || log "[WARN] 进化异常"

log "===== Done ====="

# 输出摘要
echo "---SUMMARY---"
tail -30 "$LOG_FILE" | grep -iE "(IC|ICIR|胜率|盈亏比|drift|evolve|factor|generation|WARN|Error)" || echo "无关键指标输出"
echo "---END---"
