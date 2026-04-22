#!/bin/bash
# Alpha Miner 每日流程
# 用法: bash scripts/daily_run.sh
# 推荐每个交易日 15:40 后运行

set -e

cd "$(dirname "$0")/.."
DATE=$(date +%Y-%m-%d)

echo "===== Alpha Miner Daily Run: $DATE ====="
echo ""

echo "[1/7] 采集数据..."
uv run python -m cli.collect --today
echo ""

echo "[2/7] 计算因子值..."
uv run python -m cli.backtest --compute-today
echo ""

echo "[3/7] 漂移检测..."
uv run python -m cli.drift --date $DATE
echo ""

echo "[4/7] 因子进化..."
uv run python -m cli.mine evolve --generations 3 --population 5
echo ""

echo "[5/7] 生成日报..."
uv run python -m cli.report --date $DATE
echo ""

echo "[6/7] 生成市场剧本..."
uv run python -m cli script --date $DATE --save
echo ""

echo "[7/7] 复盘昨日剧本..."
uv run python -m cli replay --date $DATE --save
echo ""

echo "===== Done: $DATE ====="
