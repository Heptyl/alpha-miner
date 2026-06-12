#!/bin/bash
# Alpha Miner 早间预热 (v2)
# 由 crontab 在交易日 08:25 触发
# 核心任务: ML预测(确保候选是最新的) + 状态预热
#
# 用法: bash scripts/cron_morning.sh

cd "$(dirname "$0")/.."
DATE=$(date +%Y-%m-%d)
DOW=$(date +%u)

# 周末跳过
[ "$DOW" -ge 6 ] && exit 0

LOG_DIR="output/logs"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/morning_${DATE}.log"

log() { echo "[$(date '+%H:%M:%S')] $1" | tee -a "$LOG_FILE"; }

log "早间预热启动 - $DATE"

# 1. 确认昨日收盘流水线完成
YESTERDAY=$(date -d "yesterday" +%Y-%m-%d 2>/dev/null || date -v-1d +%Y-%m-%d)
LAST_RUN="output/logs/last_run.json"
if [ -f "$LAST_RUN" ]; then
    LAST_DATE=$(python3 -c "import json; print(json.load(open('$LAST_RUN'))['date'])" 2>/dev/null || echo "unknown")
    log "上次流水线: $LAST_DATE"
    if [ "$LAST_DATE" != "$YESTERDAY" ]; then
        log "WARNING: 昨日流水线可能未完成 ($LAST_DATE != $YESTERDAY)"
    fi
fi

# 2. ML预测 — 已废弃(回测证实ML预测力≈0), 策略A改用IC因子超跌反弹
# log "=== ML预测 (更新latest_prediction.json) ==="
# if timeout 120 uv run python -m cli.ml_model predict 2>&1 | tee -a "$LOG_FILE"; then
#     log "OK: ML预测完成"
# else
#     log "WARNING: ML预测失败, 将使用上次候选"
# fi

# 3. 确认交易计划文件
PLAN_DIR="output/recommendations"
if [ -d "$PLAN_DIR" ]; then
    PLANS=$(ls "$PLAN_DIR"/*_tradeplan.json 2>/dev/null | wc -l)
    log "交易计划: ${PLANS} 个文件"
else
    log "无交易计划目录"
fi

# 4. 策略B候选预计算(用昨日涨停池数据)
log "=== 策略B候选预计算 ==="
uv run python -c "
from src.strategy.strategy_b import get_market_emotion, get_hot_sectors, get_strategy_b_candidates
from datetime import datetime, timedelta
import json, os

yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
emotion = get_market_emotion(yesterday)
sectors = get_hot_sectors(yesterday)
cands = get_strategy_b_candidates(yesterday)

os.makedirs('output/trader', exist_ok=True)
data = {
    'date': yesterday,
    'emotion': emotion,
    'hot_sectors': sectors[:5],
    'watchlist': [
        {'code': c['code'], 'name': c.get('name',''), 'source': c.get('source',''),
         'score': c.get('score',0), 'reason': c.get('reason','')}
        for c in cands[:10]
    ],
}
with open('output/trader/premarket_watchlist.json', 'w') as f:
    json.dump(data, f, ensure_ascii=False, indent=2)

print(f'  市场情绪: {emotion[\"phase\"]} ({emotion[\"zt_count\"]}涨停)')
print(f'  热门板块: {[s[\"industry\"] for s in sectors[:3]]}')
print(f'  明日关注: {len(cands)}只')
buyable = [c for c in cands if abs(c.get('realtime_chg',0)) < 9.5]
print(f'  可买入: {len(buyable)}只')
for c in buyable[:5]:
    print(f'    {c[\"code\"]} {c.get(\"name\",\"\")} {c.get(\"source\",\"\")} {c.get(\"reason\",\"\")}')
" 2>&1 | tee -a "$LOG_FILE"

# 5. 检查数据最新日期
DB="data/alpha_miner.db"
if [ -f "$DB" ]; then
    LATEST_DATA=$(uv run python -c "
import sqlite3
conn = sqlite3.connect('$DB')
r = conn.execute('SELECT MAX(trade_date) FROM daily_price').fetchone()[0]
print(r)
conn.close()
" 2>/dev/null)
    log "日K线最新: $LATEST_DATA"
fi

# 6. 清理Python缓存(防止旧代码)
find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null
log "OK: __pycache__已清理"

# 7. 写早间状态
mkdir -p output/logs
echo "{\"date\":\"$DATE\",\"time\":\"$(date '+%H:%M:%S')\",\"type\":\"morning\"}" > output/logs/last_morning.json

log "早间预热完成"
