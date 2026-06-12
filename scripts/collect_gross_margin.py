"""补采 gross_margin（毛利率）字段

策略: 新浪"销售毛利率"全部为"--"，用 100 - 主营业务成本率 计算
金融股(银行/保险)成本率也是"--"，跳过即可
"""
import sqlite3
import time
import requests
from bs4 import BeautifulSoup

DB_PATH = "data/alpha_miner.db"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

def get_gross_margin(code: str) -> dict:
    """从新浪获取毛利率: 100 - 主营业务成本率
    返回 {report_date: gross_margin_value}
    """
    url = f"https://money.finance.sina.com.cn/corp/go.php/vFD_FinancialGuideLine/stockid/{code}/ctrl/2024/displaytype/4.phtml"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        html = resp.content.decode('gbk', errors='ignore')
        soup = BeautifulSoup(html, 'html.parser')
        tables = soup.find_all('table')
        if len(tables) <= 13:
            return {}
        
        t = tables[13]
        rows = t.find_all('tr')
        
        # 找日期行 (row1)
        date_cells = rows[1].find_all(['td', 'th']) if len(rows) > 1 else []
        dates = [c.text.strip() for c in date_cells[1:] if len(c.text.strip()) == 10]
        
        # 找"主营业务成本率(%)"行
        for r in rows:
            cells = r.find_all(['td', 'th'])
            if len(cells) < 3:
                continue
            name = cells[0].text.strip()
            if '主营业务成本率' in name and '(%)' in name:
                vals = {}
                for i, date in enumerate(dates):
                    if i + 1 < len(cells):
                        v = cells[i + 1].text.strip()
                        try:
                            cost_rate = float(v)
                            vals[date] = round(100 - cost_rate, 4)
                        except (ValueError, TypeError):
                            pass
                return vals
        
        return {}
    except Exception:
        return {}

def main():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    
    # 找gross_margin为NULL的记录
    null_rows = conn.execute("""
        SELECT DISTINCT fs.stock_code
        FROM financial_summary fs
        WHERE fs.gross_margin IS NULL
        AND fs.report_date >= '20240101'
        ORDER BY fs.stock_code
    """).fetchall()
    
    codes = [r[0] for r in null_rows]
    print(f"需补采gross_margin: {len(codes)}只")
    
    success, fail = 0, 0
    for idx, code in enumerate(codes):
        gm_data = get_gross_margin(code)
        if gm_data:
            for date, value in gm_data.items():
                conn.execute("""
                    UPDATE financial_summary 
                    SET gross_margin = ? 
                    WHERE stock_code = ? AND report_date = ? AND gross_margin IS NULL
                """, (value, code, date))
            conn.commit()
            success += 1
        else:
            fail += 1
        
        if (idx + 1) % 100 == 0:
            print(f"  进度 {idx+1}/{len(codes)}: 成功{success}, 失败{fail}")
            time.sleep(2)
        
        time.sleep(0.3)
    
    # 验证
    total = conn.execute("SELECT COUNT(*) FROM financial_summary WHERE report_date='20241231'").fetchone()[0]
    has_gm = conn.execute("SELECT COUNT(*) FROM financial_summary WHERE report_date='20241231' AND gross_margin IS NOT NULL").fetchone()[0]
    print(f"\n完成! 补采成功{success}, 失败{fail}")
    print(f"2024年报 gross_margin覆盖: {has_gm}/{total} ({has_gm/total*100:.0f}%)")
    
    # 验证几个已知值
    tests = [
        ("002049", "紫光国微", 55.77),
        ("300750", "宁德时代", 24.44),
        ("600519", "茅台", 91.93),
    ]
    for code, name, expected in tests:
        r = conn.execute("SELECT gross_margin FROM financial_summary WHERE stock_code=? AND report_date='20241231'", (code,)).fetchone()
        if r:
            print(f"  验证 {name}({code}): {r[0]:.2f}% (预期~{expected:.2f}%)")
        else:
            print(f"  验证 {name}({code}): 无数据")
    
    conn.close()

if __name__ == "__main__":
    main()
