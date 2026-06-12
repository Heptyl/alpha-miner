"""
补采3年历史K线 — baostock按股票拉取(免费无限流)
每只股票一次拉2022-2024完整历史, ~5500只约45分钟
"""
import baostock as bs
import sqlite3
import time
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
logger = logging.getLogger(__name__)

DB_PATH = 'data/alpha_miner.db'
START_DATE = '2022-01-01'
END_DATE = '2024-12-31'

def run():
    conn = sqlite3.connect(DB_PATH)
    
    # 获取现有股票代码
    codes = [r[0] for r in conn.execute(
        "SELECT DISTINCT stock_code FROM daily_price "
        "WHERE stock_code NOT LIKE '4%' AND stock_code NOT LIKE '8%' "
        "AND stock_code NOT LIKE '9%' AND stock_code NOT LIKE '200%' "
        "AND stock_code NOT LIKE '900%'"
    ).fetchall()]
    logger.info(f"需补采 {len(codes)} 只股票")
    
    # 登录baostock
    lg = bs.login()
    if lg.error_code != '0':
        logger.error(f"baostock登录失败: {lg.error_msg}")
        return
    
    total_saved = 0
    failed = 0
    start = time.time()
    
    try:
        for i, code in enumerate(codes):
            # 转baostock格式: 000001 → sz.000001, 600000 → sh.600000
            if code.startswith('6'):
                bs_code = f'sh.{code}'
            else:
                bs_code = f'sz.{code}'
            
            try:
                rs = bs.query_history_k_data_plus(
                    code=bs_code,
                    fields='date,code,open,high,low,close,preclose,volume,amount,turn',
                    start_date=START_DATE,
                    end_date=END_DATE,
                    frequency='d',
                    adjustflag='3',  # 不复权
                )
                
                rows = []
                while rs.error_code == '0' and rs.next():
                    row = rs.get_row_data()
                    # row: [date, code, open, high, low, close, preclose, volume, amount, turn]
                    stock_code = row[1].replace('sh.', '').replace('sz.', '')
                    try:
                        o = float(row[2]) if row[2] else 0
                        h = float(row[3]) if row[3] else 0
                        lo = float(row[4]) if row[4] else 0
                        c = float(row[5]) if row[5] else 0
                        pc = float(row[6]) if row[6] else 0
                        v = float(row[7]) if row[7] else 0
                        a = float(row[8]) if row[8] else 0
                    except (ValueError, TypeError):
                        continue
                    
                    if o <= 0 or c <= 0:
                        continue
                    
                    rows.append((stock_code, row[0], o, c, h, lo, pc, v, a))
                
                if rows:
                    conn.executemany(
                        "INSERT OR IGNORE INTO daily_price "
                        "(stock_code, trade_date, open, close, high, low, pre_close, volume, amount) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        rows
                    )
                    conn.commit()
                    total_saved += len(rows)
                else:
                    failed += 1
                    
            except Exception as e:
                failed += 1
            
            if (i + 1) % 200 == 0:
                elapsed = time.time() - start
                rate = (i + 1) / elapsed
                eta = (len(codes) - i - 1) / rate / 60
                logger.info(f"进度: {i+1}/{len(codes)}, 存入{total_saved}条, "
                           f"失败{failed}, {rate:.1f}只/s, ETA {eta:.0f}min")
    
    finally:
        bs.logout()
        conn.close()
    
    elapsed = time.time() - start
    logger.info(f"完成! 存入{total_saved}条, 失败{failed}只, 耗时{elapsed/60:.1f}min")
    
    # 验证
    conn = sqlite3.connect(DB_PATH)
    r = conn.execute("SELECT MIN(trade_date), MAX(trade_date), COUNT(DISTINCT trade_date), COUNT(*) FROM daily_price").fetchone()
    logger.info(f"验证: daily_price {r[0]}~{r[1]}, {r[2]}天, {r[3]}条")
    conn.close()

if __name__ == '__main__':
    run()
