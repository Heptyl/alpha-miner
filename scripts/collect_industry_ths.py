"""
行业分类映射采集 — 同花顺行业分类(通过curl.exe绕过WSL网络限制)

数据源: 同花顺 q.10jqka.com.cn
流程:
1. 获取90个行业板块列表
2. 逐行业翻页获取成分股
3. 写入 stock_industry_mapping 表
"""
import json
import logging
import re
import sqlite3
import subprocess
import time
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DB_PATH = "data/alpha_miner.db"
PROGRESS_FILE = Path("data/collect_industry_progress.json")
BASE_URL = "http://q.10jqka.com.cn/thshy"
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"


def curl_get(url):
    """用curl.exe绕过WSL网络限制"""
    result = subprocess.run(
        ["curl.exe", "-s", url,
         "-H", f"User-Agent: {UA}",
         "-H", f"Referer: {BASE_URL}/"],
        capture_output=True, timeout=30
    )
    if result.returncode != 0:
        return ""
    return result.stdout.decode("gbk", errors="ignore")


def get_industry_list():
    """获取同花顺行业板块列表（翻页获取全部）"""
    all_industries = []
    seen = set()
    
    for page in [1, 2, 3]:  # 最多3页
        url = f"{BASE_URL}/page/{page}/" if page > 1 else f"{BASE_URL}/"
        html = curl_get(url)
        if not html:
            break
        
        matches = re.findall(r'code/(\d+)/[^>]*>([^<]+)', html)
        for code, name in matches:
            name = name.strip()
            if code not in seen and name:
                seen.add(code)
                all_industries.append((code, name))
        
        # 检查是否有下一页
        if f'page="{page+1}"' not in html:
            break
        time.sleep(0.5)
    
    logger.info(f"获取到 {len(all_industries)} 个行业板块")
    return all_industries


def get_industry_stocks(code):
    """获取一个行业的所有成分股（翻页）"""
    stocks = []
    page = 1
    
    while True:
        url = f"{BASE_URL}/detail/field/264648/order/desc/page/{page}/ajax/1/code/{code}"
        html = curl_get(url)
        if not html:
            break
        
        # 提取股票代码
        codes = re.findall(r'>\s*(\d{6})\s*<', html)
        codes = [c for c in codes if len(c) == 6 and c[0] in '036']
        
        if not codes:
            break
        
        stocks.extend(codes)
        
        # 检查是否最后一页：取所有page num，取最大值
        page_nums = re.findall(r'page="(\d+)"', html)
        if page_nums:
            max_page = max(int(p) for p in page_nums)
            if page >= max_page:
                break
        else:
            break
        
        page += 1
        time.sleep(0.5)
    
    return list(set(stocks))


def load_progress():
    if PROGRESS_FILE.exists():
        d = json.loads(PROGRESS_FILE.read_text())
        d.setdefault("done_industries", [])
        return d
    return {"done_industries": [], "total_stocks": 0}


def save_progress(progress):
    PROGRESS_FILE.write_text(json.dumps(progress, ensure_ascii=False))


def main():
    industries = get_industry_list()
    if not industries:
        return
    
    progress = load_progress()
    done = set(progress["done_industries"])
    
    conn = sqlite3.connect(DB_PATH, timeout=30)
    # 确保表存在
    conn.execute("""
        CREATE TABLE IF NOT EXISTS stock_industry_mapping (
            stock_code TEXT PRIMARY KEY,
            industry_code TEXT NOT NULL,
            industry_name TEXT NOT NULL,
            update_date TEXT NOT NULL
        )
    """)
    
    total_new = 0
    
    for i, (code, name) in enumerate(industries):
        if code in done:
            continue
        
        stocks = get_industry_stocks(code)
        
        from datetime import datetime
        today = datetime.now().strftime("%Y-%m-%d")
        
        for stock_code in stocks:
            try:
                conn.execute("""
                    INSERT OR REPLACE INTO stock_industry_mapping 
                    (stock_code, industry_code, industry_name, update_date)
                    VALUES (?, ?, ?, ?)
                """, (stock_code, code, name, today))
            except Exception as e:
                pass
        
        conn.commit()
        total_new += len(stocks)
        progress["done_industries"].append(code)
        progress["total_stocks"] = total_new
        
        if (i + 1) % 10 == 0:
            save_progress(progress)
            logger.info(f"进度 {i+1}/{len(industries)}: 行业[{name}] {len(stocks)}只, 累计{total_new}只")
        
        time.sleep(1)
    
    save_progress(progress)
    
    # 验证
    mapping_count = conn.execute("SELECT COUNT(*) FROM stock_industry_mapping").fetchone()[0]
    industry_count = conn.execute("SELECT COUNT(DISTINCT industry_code) FROM stock_industry_mapping").fetchone()[0]
    conn.close()
    
    logger.info(f"完成! {industry_count}个行业, {mapping_count}只股票映射")


if __name__ == "__main__":
    main()
