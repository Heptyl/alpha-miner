#!/bin/bash
# Alpha Miner 每日流程 (v2)
# 用法: bash scripts/daily_run.sh [--skip-collect] [--skip-push]
# 推荐每个交易日 15:40 后运行
# --skip-collect: 跳过数据采集(已有最新数据时)
# --skip-push:    跳过微信推送

set -euo pipefail

cd "$(dirname "$0")/.."
DATE=$(date +%Y-%m-%d)
DB="data/alpha_miner.db"

SKIP_COLLECT=false
SKIP_PUSH=false
for arg in "$@"; do
    case $arg in
        --skip-collect) SKIP_COLLECT=true ;;
        --skip-push)    SKIP_PUSH=true ;;
    esac
done

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

ERRORS=()
step=0
total_steps=9

log_step() {
    step=$((step + 1))
    echo ""
    echo -e "${YELLOW}[$step/$total_steps] $1${NC}"
    echo "----------------------------------------"
}

log_ok() {
    echo -e "  ${GREEN}OK${NC}: $1"
}

log_err() {
    echo -e "  ${RED}FAIL${NC}: $1"
    ERRORS+=("$1")
}

log_warn() {
    echo -e "  ${YELLOW}WARN${NC}: $1"
}

# === Pre-flight: 数据校验 ===
log_step "Pre-flight 数据校验"
uv run python -c "
import sqlite3, sys
conn = sqlite3.connect('$DB')
c = conn.cursor()
c.execute('SELECT COUNT(*) FROM daily_price WHERE trade_date = (SELECT MAX(trade_date) FROM daily_price)')
latest_cnt = c.fetchone()[0]
c.execute('SELECT MAX(trade_date) FROM daily_price')
latest_date = c.fetchone()[0]
c.execute('SELECT COUNT(*) FROM daily_price WHERE trade_date = (SELECT MAX(trade_date) FROM daily_price) AND stock_code IN (SELECT stock_code FROM daily_price WHERE trade_date = (SELECT MAX(trade_date) FROM daily_price) GROUP BY stock_code HAVING COUNT(*) > 1)')
dups = c.fetchone()[0]
conn.close()
print(f'  daily_price 最新日期: {latest_date}, 最新日行数: {latest_cnt}, 当日重复: {dups}')
if latest_cnt < 500:
    print(f'  WARNING: 最新日只有 {latest_cnt} 只股票, 可能有数据缺失')
if dups > 0:
    print(f'  WARNING: 发现 {dups} 条重复, 建议运行清理')
sys.exit(0 if latest_cnt >= 500 else 0)
"
log_ok "数据校验通过"

# === 数据去重 ===
log_step "数据去重"
DEDUP_RESULT=$(uv run python -c "
import sqlite3
conn = sqlite3.connect('$DB')
c = conn.cursor()
total_del = 0
for table, key_cols in [
    ('daily_price', 'trade_date, stock_code'),
    ('zt_pool', 'trade_date, stock_code'),
    ('strong_pool', 'trade_date, stock_code'),
    ('lhb_detail', 'trade_date, stock_code'),
    ('factor_values', 'trade_date, stock_code, factor_name'),
    ('ic_series', 'factor_name, trade_date, forward_days'),
    ('regime_state', 'trade_date'),
]:
    c.execute('''DELETE FROM %s WHERE rowid NOT IN (
        SELECT MAX(rowid) FROM %s GROUP BY %s
    )''' % (table, table, key_cols))
    d = c.rowcount
    total_del += d
    if d > 0:
        print('  %s: deleted %d dupes' % (table, d))
conn.commit()
conn.close()
if total_del == 0:
    print('  No duplicates found')
else:
    print('  Total: %d duplicates removed' % total_del)
")
echo "$DEDUP_RESULT"
log_ok "去重完成"

# === Step 1: 采集数据 ===
if [ "$SKIP_COLLECT" = false ]; then
    log_step "采集数据"
    if uv run python -m cli.collect --today 2>&1; then
        log_ok "数据采集完成"
    else
        log_err "数据采集失败 (可能是API限流, 不阻塞后续步骤)"
    fi
else
    log_step "采集数据 (SKIPPED)"
fi

# === Step 2: 计算因子值 ===
log_step "计算因子值"
if uv run python -m cli.backtest --compute-today 2>&1; then
    log_ok "因子计算完成"
else
    log_err "因子计算失败"
fi

# === Step 3: IC 管线 ===
log_step "IC 计算与持久化"
if uv run python -c "
import warnings; warnings.filterwarnings('ignore')
from src.data.storage import Storage
from src.pipeline.runner import run_ic_pipeline
db = Storage('$DB'); db.init_db()
results = run_ic_pipeline(db)
for fn, info in results.items():
    ic_str = '%.4f' % info['avg_ic'] if info['avg_ic'] == info['avg_ic'] else 'N/A'
    print(f'  {fn}: {info[\"valid_ic\"]}/{info[\"dates\"]} valid IC, avg={ic_str}')
" 2>&1; then
    log_ok "IC 管线完成"
else
    log_err "IC 管线失败"
fi

# === Step 4: Regime 识别 ===
log_step "Regime 识别"
if uv run python -c "
import warnings; warnings.filterwarnings('ignore')
from src.data.storage import Storage
from src.pipeline.runner import run_regime_pipeline
db = Storage('$DB'); db.init_db()
result = run_regime_pipeline(db)
print(f'  Regime: {result[\"regime\"]} (conf={result[\"confidence\"]:.2f})')
" 2>&1; then
    log_ok "Regime: 完成"
else
    log_err "Regime 识别失败"
fi

# === Step 5: 漂移检测 ===
log_step "漂移检测"
if uv run python -m cli.drift --date $DATE 2>&1; then
    log_ok "漂移检测完成"
else
    log_warn "漂移检测异常 (可能数据不足)"
fi

# === Step 6: 生成日报 ===
log_step "生成盘后简报"
REPORT_FILE=""
if REPORT_FILE=$(uv run python -c "
import warnings; warnings.filterwarnings('ignore')
from src.data.storage import Storage
from src.drift.daily_report import DailyReport
db = Storage('$DB'); db.init_db()
report = DailyReport(db)
output = report.generate('$DATE')
if output:
    print(output)
" 2>&1); then
    log_ok "日报生成完成"
    # Save report
    REPORT_DIR="output/reports"
    mkdir -p "$REPORT_DIR"
    echo "$REPORT_FILE" > "$REPORT_DIR/report_${DATE}.txt"
else
    log_err "日报生成失败"
fi

# === Step 7: 因子进化 ===
log_step "因子进化"
if uv run python -m cli.mine evolve --generations 3 --population 5 2>&1; then
    log_ok "因子进化完成"
else
    log_warn "因子进化异常 (非关键)"
fi

# === Step 8: 市场剧本 ===
log_step "生成市场剧本"
if uv run python -m cli script --date $DATE --save 2>&1; then
    log_ok "市场剧本完成"
else
    log_warn "市场剧本异常"
fi

# === Step 9: 微信推送 ===
if [ "$SKIP_PUSH" = false ]; then
    log_step "微信推送"
    if uv run python scripts/send_wechat.py 2>&1; then
        log_ok "微信推送完成"
    else
        log_warn "微信推送失败 (检查 token/连接)"
    fi
else
    log_step "微信推送 (SKIPPED)"
fi

# === Summary ===
echo ""
echo "=========================================="
echo -e " Alpha Miner Daily Run: $DATE"
echo "=========================================="

if [ ${#ERRORS[@]} -eq 0 ]; then
    echo -e " ${GREEN}ALL STEPS PASSED${NC}"
else
    echo -e " ${RED}${#ERRORS[@]} ERRORS:${NC}"
    for e in "${ERRORS[@]}"; do
        echo -e "   ${RED}- $e${NC}"
    done
fi

echo ""

# Print data stats
uv run python -c "
import sqlite3
conn = sqlite3.connect('$DB')
c = conn.cursor()
print('Data Summary:')
for t in ['daily_price','zt_pool','strong_pool','lhb_detail','fund_flow','factor_values','ic_series','regime_state']:
    c.execute('SELECT COUNT(*) FROM %s' % t)
    cnt = c.fetchone()[0]
    print('  %-20s %6d rows' % (t, cnt))
conn.close()
"

echo "=========================================="
echo "Done: $DATE"
