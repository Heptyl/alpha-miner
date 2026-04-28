#!/bin/bash
# Alpha Miner 定时任务安装脚本
#
# 功能：
#   23:00 — 晚间推荐（采集数据 + TOP5 推荐）
#   08:30 — 早间复盘再确认
#
# 用法:
#   bash scripts/setup_cron.sh          # 安装定时任务
#   bash scripts/setup_cron.sh --remove # 移除定时任务

set -e

PROJECT_DIR="/home/ccy/alpha-miner"
LOG_DIR="$PROJECT_DIR/logs"
mkdir -p "$LOG_DIR"

# 判断是否要移除
if [ "$1" = "--remove" ]; then
    echo "移除 Alpha Miner 定时任务..."
    (crontab -l 2>/dev/null | grep -v "alpha-miner") | crontab -
    echo "✅ 已移除"
    exit 0
fi

echo "安装 Alpha Miner 定时任务..."
echo "项目目录: $PROJECT_DIR"
echo "日志目录: $LOG_DIR"

# 生成 crontab 条目
CRONTAB_ENTRY="
# Alpha Miner 晚间推荐 — 每日23:00
0 23 * * * cd $PROJECT_DIR && uv run python scripts/evening_recommend.py >> $LOG_DIR/evening_\$(date +\%Y\%m\%d).log 2>&1

# Alpha Miner 早间复盘 — 每日08:30
30 8 * * * cd $PROJECT_DIR && uv run python scripts/morning_reconfirm.py >> $LOG_DIR/morning_\$(date +\%Y\%m\%d).log 2>&1
"

# 保留已有 crontab，追加新条目
(crontab -l 2>/dev/null | grep -v "alpha-miner"; echo "$CRONTAB_ENTRY") | crontab -

echo ""
echo "✅ 定时任务已安装:"
echo "   23:00 — 晚间推荐 (采集 + TOP5)"
echo "   08:30 — 早间复盘再确认"
echo ""
echo "日志位置: $LOG_DIR/"
echo ""
echo "管理命令:"
echo "   crontab -l                      # 查看定时任务"
echo "   bash $PROJECT_DIR/scripts/setup_cron.sh --remove  # 移除"
