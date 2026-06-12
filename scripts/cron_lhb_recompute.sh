#!/bin/bash
# 龙虎榜补采后重算因子+IC
# 东财龙虎榜16:30~18:00才发布, 15:35主流水线采不到当天lhb
# 本脚本在17:30补采lhb后, 重算因子值和IC管线

cd /home/ccy/alpha-miner
LOG="output/logs/lhb_post_$(date +%Y-%m-%d).log"

echo "[$(date +%H:%M:%S)] === 龙虎榜补采+因子重算 ===" >> "$LOG"

# 1. 采集当天数据(重点是lhb_detail)
echo "[$(date +%H:%M:%S)] Step 1: 数据采集" >> "$LOG"
uv run python -m cli.collect --today >> "$LOG" 2>&1

# 2. 重算因子值(lhb_institution依赖lhb_detail)
echo "[$(date +%H:%M:%S)] Step 2: 因子重算" >> "$LOG"
uv run python scripts/recompute_factors.py "$(date +%Y-%m-%d)" >> "$LOG" 2>&1

# 3. IC管线(用新因子值重算IC)
echo "[$(date +%H:%M:%S)] Step 3: IC管线" >> "$LOG"
uv run python scripts/run_ic.py >> "$LOG" 2>&1

echo "[$(date +%H:%M:%S)] === 完成 ===" >> "$LOG"
