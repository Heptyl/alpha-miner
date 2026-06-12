#!/bin/bash
# watchdog_daemon.sh — daemon崩溃自动恢复
# 每3分钟检查daemon是否存活, 不存活则重启
# crontab: */3 9:30-15:00 * * 1-5  /home/ccy/alpha-miner/scripts/watchdog_daemon.sh

cd "$(dirname "$0")/.."

PID_FILE="output/trader/daemon_logs/daemon.pid"
PAUSE_FILE="output/trader/daemon_logs/daemon.pause"
HEARTBEAT_FILE="output/trader/daemon_logs/daemon.heartbeat.json"
STALE_STATE_FILE="output/trader/daemon_logs/watchdog_stale.state"
WATCHDOG_LOG="output/trader/daemon_logs/watchdog.log"
HEARTBEAT_MAX_AGE=900
STALE_CONFIRMATIONS=2

log_w() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" >> "$WATCHDOG_LOG"; }

# Crontab invokes this script for whole clock-hours. Only manage the daemon
# inside its intended session so it cannot start before 09:25 or revive after
# the scheduled 15:10 shutdown.
HHMM=${WATCHDOG_HHMM:-$(date +%H%M)}
if ! {
    [ "$HHMM" -ge 0925 ] && [ "$HHMM" -le 1130 ] ||
    [ "$HHMM" -ge 1300 ] && [ "$HHMM" -le 1509 ]
}; then
    exit 0
fi

# 显式维护暂停时不自动拉起daemon。
if [ -f "$PAUSE_FILE" ]; then
    log_w "检测到维护暂停标记, 跳过自动启动"
    exit 0
fi

# 条件1: PID文件不存在 → 启动
if [ ! -f "$PID_FILE" ]; then
    log_w "PID文件不存在, 触发启动"
    bash scripts/cron_trading_daemon.sh
    exit 0
fi

PID=$(cat "$PID_FILE")

# 条件2: 进程已死 → 重启
if ! kill -0 "$PID" 2>/dev/null; then
    log_w "进程$PID已死, 触发重启"
    rm -f "$PID_FILE"
    rm -f "$STALE_STATE_FILE"
    bash scripts/cron_trading_daemon.sh
    exit 0
fi

# 条件3: 独立心跳连续两次超过15分钟才判定主循环卡死。
# 单次行情/API扫描较慢时不应被watchdog误杀。
if [ ! -f "$HEARTBEAT_FILE" ]; then
    log_w "进程$PID存活但心跳文件不存在, 本轮不重启"
    exit 0
fi

LAST_MOD=$(stat -c %Y "$HEARTBEAT_FILE" 2>/dev/null || echo 0)
NOW=$(date +%s)
AGE=$((NOW - LAST_MOD))

if [ "$AGE" -le "$HEARTBEAT_MAX_AGE" ]; then
    rm -f "$STALE_STATE_FILE"
    exit 0
fi

PREV_PID=""
PREV_COUNT=0
if [ -f "$STALE_STATE_FILE" ]; then
    read -r PREV_PID PREV_COUNT < "$STALE_STATE_FILE" || true
fi
if [ "$PREV_PID" = "$PID" ]; then
    STALE_COUNT=$((PREV_COUNT + 1))
else
    STALE_COUNT=1
fi
printf '%s %s\n' "$PID" "$STALE_COUNT" > "$STALE_STATE_FILE"

if [ "$STALE_COUNT" -lt "$STALE_CONFIRMATIONS" ]; then
    log_w "心跳${AGE}秒无更新(进程$PID), 第${STALE_COUNT}次确认, 暂不重启"
    exit 0
fi

log_w "心跳${AGE}秒无更新且连续${STALE_COUNT}次确认, 杀掉进程$PID后重启"
kill "$PID" 2>/dev/null
sleep 2
rm -f "$PID_FILE" "$STALE_STATE_FILE"
bash scripts/cron_trading_daemon.sh
