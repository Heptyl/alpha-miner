"""补全 financial_summary — 直接用 requests+BS4 解析新浪财务指标页面

断点续采: data/collect_fs_progress.json
目标: 从当前覆盖量补到 3800+ 只
"""
import json
import logging
import sqlite3
import time
from pathlib import Path

import requests
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DB_PATH = "data/alpha_miner.db"
PROGRESS_FILE = Path("data/collect_fs_progress.json")

session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
})

# 要提取的指标（新浪行名 → financial_summary DB列名）
# DB列: stock_code, report_date, revenue, net_profit, revenue_yoy, profit_yoy,
#        gross_margin, net_margin, roe, debt_ratio, create_time
INDICATOR_MAP = {
    "销售净利率(%)": "net_margin",
    "净资产收益率(%)": "roe",
    "净利润增长率(%)": "profit_yoy",
    "主营业务收入增长率(%)": "revenue_yoy",
    "资产负债率(%)": "debt_ratio",
    "销售毛利率(%)": "gross_margin",
}


def get_stock_list():
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute(
        "SELECT DISTINCT stock_code FROM daily_price "
        "WHERE stock_code NOT LIKE '688%' "
        "AND stock_code NOT LIKE '8%' "
        "AND stock_code NOT LIKE '9%' "
        "ORDER BY stock_code"
    ).fetchall()
    conn.close()
    return [r[0] for r in rows]


def get_existing_codes():
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute("SELECT DISTINCT stock_code FROM financial_summary").fetchall()
    conn.close()
    return set(r[0] for r in rows)


def load_progress():
    if PROGRESS_FILE.exists():
        d = json.loads(PROGRESS_FILE.read_text())
        # 确保所有字段存在（兼容旧格式）
        d.setdefault("done", [])
        d.setdefault("failed", [])
        return d
    return {"done": [], "failed": []}


def save_progress(progress):
    PROGRESS_FILE.write_text(json.dumps(progress, ensure_ascii=False))


def _safe_float(val):
    if val is None:
        return None
    val = str(val).strip().replace(",", "")
    if val in ("", "--", "-", "None"):
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def collect_one(code):
    """采集一只股票的财务指标，写入 financial_summary"""
    # 新浪URL格式
    url = f"https://money.finance.sina.com.cn/corp/go.php/vFD_FinancialGuideLine/stockid/{code}/ctrl/2024/displaytype/4.phtml"
    
    try:
        r = session.get(url, timeout=15)
        if r.status_code != 200 or len(r.text) < 1000:
            return False, f"status={r.status_code} len={len(r.text)}"
    except Exception as e:
        return False, str(e)[:60]
    
    soup = BeautifulSoup(r.text, "html.parser")
    tables = soup.find_all("table")
    
    # 找到大表（行数>50的财务指标表）
    data_table = None
    for t in tables:
        if len(t.find_all("tr")) > 50:
            data_table = t
            break
    
    if not data_table:
        return False, "无财务指标表"
    
    rows = data_table.find_all("tr")
    if len(rows) < 3:
        return False, "表行数不足"
    
    # 第2行是报告日期
    date_row = rows[1]
    date_cells = date_row.find_all(["td", "th"])
    date_cols = []
    for cell in date_cells[1:]:  # 跳过"报告日期"标签
        date_str = cell.text.strip().replace("-", "")
        if len(date_str) == 8 and date_str.isdigit():
            date_cols.append(date_str)
    
    if not date_cols:
        return False, "无报告日期"
    
    # 解析各指标行
    indicator_values = {}  # {db_col: {date: value}}
    for row in rows[2:]:
        cells = row.find_all(["td", "th"])
        if not cells:
            continue
        name = cells[0].text.strip()
        
        # 精确匹配指标名
        matched_col = None
        for indicator_name, db_col in INDICATOR_MAP.items():
            if indicator_name == name:
                matched_col = db_col
                break
        
        # 特殊处理：roe 要排除"加权净资产收益率"
        if matched_col == "roe" and "加权" in name:
            matched_col = None
        
        if not matched_col:
            continue
        
        vals = {}
        for i, date in enumerate(date_cols):
            if i + 1 < len(cells):
                vals[date] = _safe_float(cells[i + 1].text)
        
        indicator_values[matched_col] = vals
    
    if not indicator_values:
        return False, "未匹配到关键指标"
    
    # 写入DB
    conn = sqlite3.connect(DB_PATH, timeout=30)
    try:
        inserted = 0
        for date in date_cols:
            # 检查已存在
            exists = conn.execute(
                "SELECT 1 FROM financial_summary WHERE stock_code=? AND report_date=?",
                (code, date)
            ).fetchone()
            if exists:
                continue
            
            roe = indicator_values.get("roe", {}).get(date)
            net_margin = indicator_values.get("net_margin", {}).get(date)
            profit_yoy = indicator_values.get("profit_yoy", {}).get(date)
            revenue_yoy = indicator_values.get("revenue_yoy", {}).get(date)
            debt_ratio = indicator_values.get("debt_ratio", {}).get(date)
            
            from datetime import datetime
            conn.execute("""
                INSERT OR IGNORE INTO financial_summary 
                (stock_code, report_date, roe, net_margin, 
                 profit_yoy, revenue_yoy, debt_ratio, create_time)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (code, date, roe, net_margin,
                  profit_yoy, revenue_yoy, debt_ratio, 
                  datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
            inserted += 1
        
        conn.commit()
        return True, f"+{inserted}期"
    finally:
        conn.close()


def main():
    stock_list = get_stock_list()
    existing = get_existing_codes()
    progress = load_progress()
    
    done_set = set(progress["done"])
    remaining = [c for c in stock_list if c not in existing and c not in done_set]
    
    logger.info(f"总数: {len(stock_list)}, 已有: {len(existing)}, 已处理: {len(done_set)}, 待采: {len(remaining)}")
    
    if not remaining:
        logger.info("无待采股票，完成!")
        return
    
    success = 0
    fail = 0
    
    for i, code in enumerate(remaining):
        ok, msg = collect_one(code)
        
        if ok:
            success += 1
            progress["done"].append(code)
        else:
            fail += 1
            progress["failed"].append(code)
        
        # 每50只保存进度并打印
        if (i + 1) % 50 == 0:
            save_progress(progress)
            logger.info(f"进度 {i+1}/{len(remaining)}: 成功{success} 失败{fail}")
        
        time.sleep(2)
    
    # 最终保存
    save_progress(progress)
    logger.info(f"本轮完成! 成功{success} 失败{fail} 共处理{len(remaining)}")
    
    # 验证
    conn = sqlite3.connect(DB_PATH)
    total = conn.execute("SELECT COUNT(DISTINCT stock_code) FROM financial_summary").fetchone()[0]
    conn.close()
    logger.info(f"financial_summary 当前覆盖: {total} 只")


if __name__ == "__main__":
    main()
