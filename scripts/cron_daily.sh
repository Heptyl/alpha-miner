#!/bin/bash
# Alpha Miner 收盘后自动流水线 (v3)
# 由系统 crontab 在每个交易日 15:35 自动触发
# 也可手动运行: bash scripts/cron_daily.sh
#
# 流水线:
#   1. 去重 + 质量校验
#   2. 数据采集 (daily_price/zt_pool/strong_pool/fund_flow/lhb...)
#   2b. K线补全 (腾讯接口兜底缺失的异动股)
#   3. 因子计算 (factor_values)
#   4. IC 管线 (ic_series)
#   5. Regime + 漂移检测
#   6. 盘后推荐 + 交易计划
#   7. 盘后简报
#   8. 因子进化 (非关键)
#   9. 推送通知
#
# 日志输出到 output/logs/daily_YYYY-MM-DD.log

set -uo pipefail

cd "$(dirname "$0")/.."
DATE=$(date +%Y-%m-%d)
DB="data/alpha_miner.db"
LOG_DIR="output/logs"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/daily_${DATE}.log"

# 日志函数
log() { echo "[$(date '+%H:%M:%S')] $1" | tee -a "$LOG_FILE"; }
log_step() { echo "" | tee -a "$LOG_FILE"; log "===== $1 ====="; }
log_ok() { log "  OK: $1"; }
log_err() { log "  FAIL: $1"; ERRORS+=("$1"); }
log_warn() { log "  WARN: $1"; }

ERRORS=()
step=0
total_steps=11

# 检查是否交易日 (简单判断: 周末跳过)
DOW=$(date +%u)
if [ "$DOW" -ge 6 ]; then
    log "今天是周末, 跳过"
    exit 0
fi

log "Alpha Miner 收盘流水线启动 - $DATE"
log "日志文件: $LOG_FILE"

# === Pre-flight: 去重 ===
step=$((step + 1))
log_step "[$step/$total_steps] 数据去重"
uv run python -c "
import sqlite3
conn = sqlite3.connect('$DB')
total_del = 0
for table, key_cols in [
    ('daily_price', 'trade_date, stock_code'),
    ('zt_pool', 'trade_date, stock_code'),
    ('strong_pool', 'trade_date, stock_code'),
    ('lhb_detail', 'trade_date, stock_code'),
    ('fund_flow', 'trade_date, stock_code'),
    ('factor_values', 'trade_date, stock_code, factor_name'),
    ('ic_series', 'factor_name, trade_date, forward_days'),
    ('regime_state', 'trade_date'),
]:
    c = conn.cursor()
    try:
        c.execute('SELECT COUNT(*) FROM %s' % table)
        cnt = c.fetchone()[0]
        if cnt == 0:
            print(f'  {table}: empty, skip')
            continue
        c.execute('''DELETE FROM %s WHERE rowid NOT IN (
            SELECT MAX(rowid) FROM %s GROUP BY %s
        )''' % (table, table, key_cols))
        d = c.rowcount
        total_del += d
        if d > 0:
            print(f'  {table}: deleted {d} dupes')
    except Exception as e:
        print(f'  {table}: skip ({e})')
conn.commit()
conn.close()
print(f'  Total: {total_del} duplicates removed')
" 2>&1 | tee -a "$LOG_FILE"
log_ok "去重完成"

# === Step 1: 数据采集 ===
step=$((step + 1))
log_step "[$step/$total_steps] 数据采集"
# 超时5分钟，防止API卡死
if timeout 300 uv run python -m cli.collect --today 2>&1 | tee -a "$LOG_FILE"; then
    log_ok "数据采集完成"
else
    log_err "数据采集部分失败 (API限流或超时, 继续后续步骤)"
fi

# === Step 1b: 数据完整性校验 & 兜底补全 ===
log_step "[$step/$total_steps] K线补全 (腾讯兜底)"
uv run python -c "
import sqlite3, subprocess, time, sys
DB = '$DB'
CURL = '/mnt/c/Windows/System32/curl.exe'
DATE = '$DATE'
conn = sqlite3.connect(DB)

# 找出异动池(涨停+强势)中缺失K线的
missing = conn.execute('''
    SELECT DISTINCT z.stock_code
    FROM (
        SELECT stock_code FROM zt_pool WHERE trade_date = ?
        UNION
        SELECT stock_code FROM strong_pool WHERE trade_date = ?
    ) z
    LEFT JOIN daily_price d ON z.stock_code = d.stock_code AND d.trade_date = ?
    WHERE d.stock_code IS NULL
    AND NOT (z.stock_code LIKE '688%' OR z.stock_code LIKE '689%')
    AND NOT (LENGTH(z.stock_code)=6 AND SUBSTR(z.stock_code,1,1) IN ('8','9'))
''', (DATE, DATE, DATE)).fetchall()

if not missing:
    print('  K线完整，无需补全')
    conn.close()
    sys.exit(0)

print(f'  缺失K线: {len(missing)}只, 用腾讯接口补全...')
inserted = 0
for (code,) in missing:
    tc = ('sh' if code.startswith(('6','9')) else 'sz') + code
    try:
        r = subprocess.run([CURL, '-s', '--connect-timeout', '10', '--max-time', '15',
            f'http://qt.gtimg.cn/q={tc}'], capture_output=True, timeout=20)
        raw = r.stdout.decode('gbk', errors='replace')
        if '~' not in raw or '=\"\"' in raw:
            continue
        parts = raw.split('~')
        if len(parts) < 48:
            continue
        close = float(parts[3])
        pre_close = float(parts[4])
        open_ = float(parts[5])
        volume = int(parts[6]) if parts[6] else 0
        high = float(parts[33]) if parts[33] else close
        low = float(parts[34]) if parts[34] else close
        amount = float(parts[37]) * 10000 if parts[37] else 0  # 腾讯API返回万元，转元
        turnover = float(parts[38]) if parts[38] and parts[38] != '0.00' else 0.0
        if volume == 0 and amount == 0:
            continue
        conn.execute('''INSERT OR REPLACE INTO daily_price
            (stock_code, trade_date, open, high, low, close, pre_close, volume, amount, turnover_rate)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
            (code, DATE, open_, high, low, close, pre_close, volume, amount, turnover))
        inserted += 1
    except Exception:
        pass
    time.sleep(0.3)
conn.commit()
total = conn.execute('SELECT COUNT(DISTINCT stock_code) FROM daily_price WHERE trade_date = ?', (DATE,)).fetchone()[0]
print(f'  补全: {inserted}只, 5/8 total: {total}只')
conn.close()
" 2>&1 | tee -a "$LOG_FILE"
log_ok "K线补全完成"

# === Step 1c: 基本面采集 (解禁全量 + 异动股减持/财务) ===
step=$((step + 1))
log_step "[$step/$total_steps] 基本面采集 (解禁/减持/财务)"
uv run python -c "
import sqlite3, sys, time
sys.path.insert(0, '.')
from src.data.sources.fundamental import collect_all, init_tables

DB = 'data/alpha_miner.db'
conn = sqlite3.connect(DB)
init_tables(conn)
conn.close()

# 解禁全量(30天内)
from src.data.sources.fundamental import fetch_restricted_release
try:
    fetch_restricted_release(days_ahead=30)
    print('  解禁预告: OK')
except Exception as e:
    print(f'  解禁预告: FAIL - {e}')

# 从涨停池+强势股+龙虎榜取今日异动股, 批量采集减持+财务
conn = sqlite3.connect(DB)
DATE = '$DATE'
codes = conn.execute('''
    SELECT DISTINCT stock_code FROM (
        SELECT stock_code FROM zt_pool WHERE trade_date = ?
        UNION
        SELECT stock_code FROM strong_pool WHERE trade_date = ?
        UNION
        SELECT stock_code FROM lhb_detail WHERE trade_date = ?
    )
''', (DATE, DATE, DATE)).fetchall()
conn.close()
codes = [c[0] for c in codes]
print(f'  异动股: {len(codes)}只, 采集减持+财务...')

from src.data.sources.fundamental import fetch_holder_changes, fetch_financial_summary
ok_h, ok_f = 0, 0
for i, code in enumerate(codes):
    try:
        fetch_holder_changes(code)
        ok_h += 1
    except: pass
    try:
        fetch_financial_summary(code)
        ok_f += 1
    except: pass
    if (i+1) % 20 == 0:
        print(f'    进度: {i+1}/{len(codes)}')
    time.sleep(0.3)

# 验证
conn = sqlite3.connect(DB)
for t in ['restricted_release', 'holder_change', 'financial_summary']:
    cnt = conn.execute(f'SELECT COUNT(*) FROM {t}').fetchone()[0]
    print(f'  {t}: {cnt} rows')
conn.close()
print('  基本面采集完成')
" 2>&1 | tee -a "$LOG_FILE"
log_ok "基本面采集完成"

# === Step 2: 因子计算 ===
step=$((step + 1))
log_step "[$step/$total_steps] 因子计算"
if uv run python -m cli.backtest --compute-today 2>&1 | tee -a "$LOG_FILE"; then
    log_ok "因子计算完成"
else
    log_err "因子计算失败"
fi

# === Step 3: IC 管线 ===
step=$((step + 1))
log_step "[$step/$total_steps] IC 计算"
uv run python -c "
import warnings; warnings.filterwarnings('ignore')
from src.data.storage import Storage
from src.pipeline.runner import run_ic_pipeline
db = Storage('$DB'); db.init_db()
results = run_ic_pipeline(db)
for fn, info in results.items():
    ic_str = '%.4f' % info['avg_ic'] if info['avg_ic'] == info['avg_ic'] else 'N/A'
    print(f'  {fn}: {info[\"valid_ic\"]}/{info[\"dates\"]} valid IC, avg={ic_str}')
" 2>&1 | tee -a "$LOG_FILE"
log_ok "IC 管线完成"

# === Step 4: Regime + 漂移 ===
step=$((step + 1))
log_step "[$step/$total_steps] Regime + 漂移检测"
uv run python -c "
import warnings; warnings.filterwarnings('ignore')
from src.data.storage import Storage
from src.pipeline.runner import run_regime_pipeline
db = Storage('$DB'); db.init_db()
result = run_regime_pipeline(db)
print(f'  Regime: {result[\"regime\"]} (conf={result[\"confidence\"]:.2f})')
" 2>&1 | tee -a "$LOG_FILE"

if uv run python -m cli.drift --date "$DATE" 2>&1 | tee -a "$LOG_FILE"; then
    log_ok "漂移检测完成"
else
    log_warn "漂移检测异常 (可能数据不足)"
fi

# === Step 4b: ML模型重新训练 (已废弃: 回测证实ML预测力≈0) ===
# step=$((step + 1))
# log_step "[$step/$total_steps] ML模型训练"
# if timeout 300 uv run python -m cli.ml_model train 2>&1 | tee -a "$LOG_FILE"; then
#     log_ok "ML模型训练完成"
# else
#     log_err "ML模型训练失败"
# fi

# === Step 5: ML预测 (已废弃: 同上) ===
# step=$((step + 1))
# log_step "[$step/$total_steps] ML预测 (更新latest_prediction.json)"
# if timeout 120 uv run python -m cli.ml_model predict 2>&1 | tee -a "$LOG_FILE"; then
#     log_ok "ML预测完成"
# else
#     log_err "ML预测失败 (守护进程将用过期数据!)"
# fi

# === Step 6: 推荐 + 交易计划 ===
step=$((step + 1))
log_step "[$step/$total_steps] 盘后推荐 + 交易计划"
if uv run python -m cli.recommend --date "$DATE" --save output/recommendations --json 2>&1 | tee -a "$LOG_FILE"; then
    log_ok "推荐生成完成"
else
    log_err "推荐生成失败"
fi

if uv run python -m cli.tradeplan --date "$DATE" 2>&1 | tee -a "$LOG_FILE"; then
    log_ok "交易计划生成完成"
else
    log_err "交易计划生成失败"
fi

# === Step 6: 9维选股 ===
step=$((step + 1))
log_step "[$step/$total_steps] 9维选股"
if uv run python -m cli.screen run --date "$DATE" --top 20 --save output/recommendations 2>&1 | tee -a "$LOG_FILE"; then
    log_ok "9维选股完成"
else
    log_warn "9维选股异常 (非关键)"
fi

# === Step 7: 盘后简报 ===
step=$((step + 1))
log_step "[$step/$total_steps] 盘后简报"
uv run python -c "
import warnings; warnings.filterwarnings('ignore')
from src.data.storage import Storage
from src.drift.daily_report import DailyReport
db = Storage('$DB'); db.init_db()
report = DailyReport(db)
from datetime import datetime
as_of = datetime.strptime('$DATE', '%Y-%m-%d')
try:
    output = report.generate(as_of)
except Exception:
    output = report.generate(as_of, report_date='$DATE')
if output:
    import os
    os.makedirs('output/reports', exist_ok=True)
    with open(f'output/reports/report_${DATE}.txt', 'w') as f:
        f.write(output)
    print('  Report saved to output/reports/report_${DATE}.txt')
" 2>&1 | tee -a "$LOG_FILE"
log_ok "简报完成"

# === Step 8: 因子进化 ===
step=$((step + 1))
log_step "[$step/$total_steps] 因子进化 (非关键)"
if timeout 180 uv run python -m cli.mine evolve --generations 3 --population 5 2>&1 | tee -a "$LOG_FILE"; then
    log_ok "因子进化完成"
else
    log_warn "因子进化异常"
fi

# === Step 9: 推送 ===
step=$((step + 1))
log_step "[$step/$total_steps] 推送通知"
if [ -f scripts/send_wechat.py ]; then
    if timeout 30 uv run python scripts/send_wechat.py 2>&1 | tee -a "$LOG_FILE"; then
        log_ok "推送完成"
    else
        log_warn "推送失败 (检查token)"
    fi
else
    log_warn "send_wechat.py 不存在, 跳过"
fi

# === Summary ===
echo "" | tee -a "$LOG_FILE"
log "=========================================="
if [ ${#ERRORS[@]} -eq 0 ]; then
    log "ALL STEPS PASSED"
else
    log "${#ERRORS[@]} ERRORS:"
    for e in "${ERRORS[@]}"; do
        log "  - $e"
    done
fi

uv run python -c "
import sqlite3
conn = sqlite3.connect('$DB')
c = conn.cursor()
print('Data Summary:')
for t in ['daily_price','zt_pool','strong_pool','lhb_detail','fund_flow','factor_values','ic_series']:
    c.execute('SELECT COUNT(*), MAX(trade_date) FROM %s' % t)
    cnt, latest = c.fetchone()
    print(f'  {t:20s} {cnt:>7d} rows  latest={latest}')
conn.close()
" 2>&1 | tee -a "$LOG_FILE"

log "=========================================="

# === Step 10: 复盘报告 ===
step=$((step + 1))
log_step "[$step/$total_steps] 复盘报告"
if uv run python scripts/generate_daily_review.py --date "$DATE" 2>&1 | tee -a "$LOG_FILE"; then
    log_ok "复盘报告生成完成"
else
    log_warn "复盘报告生成异常"
fi

log "=========================================="
log "Done: $DATE"

# 写完成标记 (供页面状态栏读取)
echo "{\"date\":\"$DATE\",\"time\":\"$(date '+%H:%M:%S')\",\"errors\":${#ERRORS[@]}}" > output/logs/last_run.json
