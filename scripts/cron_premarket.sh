#!/bin/bash
# Alpha Miner 开盘前准备 (v1)
# 由 crontab 在每个交易日 08:30 自动触发
# 1. ML预测(用昨天训练的新模型+最新数据)
# 2. 策略B候选预计算(涨停池/板块热度)

set -uo pipefail

cd "$(dirname "$0")/.."
DATE=$(date +%Y-%m-%d)
LOG_DIR="output/logs"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/premarket_${DATE}.log"

log() { echo "[$(date '+%H:%M:%S')] $1" | tee -a "$LOG_FILE"; }

DOW=$(date +%u)
if [ "$DOW" -ge 6 ]; then
    log "周末跳过"
    exit 0
fi

log "开盘前准备启动 - $DATE"

# 1. ML预测 — 已废弃(回测证实ML预测力≈0)
# log "===== ML预测 ====="
# if timeout 120 uv run python -m cli.ml_model predict 2>&1 | tee -a "$LOG_FILE"; then
#     log "OK: ML预测完成"
# else
#     log "FAIL: ML预测失败"
# fi

# 2. 策略B候选预计算
log "===== 策略B候选预计算 ====="
uv run python -c "
from src.strategy.strategy_b import get_market_emotion, get_hot_sectors, get_strategy_b_candidates
from datetime import datetime, timedelta
import json, os

yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
emotion = get_market_emotion(yesterday)
sectors = get_hot_sectors(yesterday)
cands = get_strategy_b_candidates(yesterday)

# 保存
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

log "开盘前准备完成"
echo "{\"date\":\"$DATE\",\"time\":\"$(date '+%H:%M:%S')\"}" > output/logs/premarket_last_run.json
