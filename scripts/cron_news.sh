#!/bin/bash
# Alpha Miner 新闻盘中采集
# 由 crontab 在盘中每30分钟触发
# 用法: bash scripts/cron_news.sh

cd "$(dirname "$0")/.."
DATE=$(date +%Y-%m-%d)
DOW=$(date +%u)

# 周末跳过
[ "$DOW" -ge 6 ] && exit 0

# 只在交易时段运行 (9:00-15:00)
HOUR=$(date +%H)
[ "$HOUR" -lt 9 ] || [ "$HOUR" -ge 15 ] && exit 0

LOG_DIR="output/logs"
mkdir -p "$LOG_DIR"

echo "[$(date '+%H:%M:%S')] 新闻采集开始" >> "$LOG_DIR/news_${DATE}.log"

# 新闻采集 (如果news_miner模块可用)
uv run python -c "
try:
    from src.data.sources.news_miner import NewsMiner
    from src.data.storage import Storage
    db = Storage('data/alpha_miner.db'); db.init_db()
    miner = NewsMiner(db)
    count = miner.collect_today()
    print(f'  采集 {count} 条新闻')
except ImportError:
    print('  news_miner 模块不可用, 跳过')
except Exception as e:
    print(f'  新闻采集异常: {e}')
" 2>&1 >> "$LOG_DIR/news_${DATE}.log"

echo "[$(date '+%H:%M:%S')] 新闻采集完成" >> "$LOG_DIR/news_${DATE}.log"
