"""
补采3年历史K线 — 用于延长回测
方案A: akshare stock_zh_a_hist 逐只拉取2022-2024
然后从涨跌幅反推涨停(>=9.5%普通/>=19.5%创业板)
"""
import akshare as ak
import pandas as pd
import sqlite3
import time
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
logger = logging.getLogger(__name__)

DB_PATH = 'data/alpha_miner.db'
START_DATE = '20220101'
END_DATE = '20241231'
BATCH_SIZE = 50  # 每批多少只

def get_stock_list():
    """获取全量A股代码列表"""
    # 从现有DB获取
    conn = sqlite3.connect(DB_PATH)
    codes = conn.execute("""
        SELECT DISTINCT stock_code FROM daily_price 
        WHERE stock_code NOT LIKE '4%' AND stock_code NOT LIKE '8%' 
        AND stock_code NOT LIKE '9%'
    """).fetchall()
    conn.close()
    codes = [c[0] for c in codes]
    logger.info(f"从DB获取 {len(codes)} 只股票代码")
    return codes

def fetch_one_stock(code):
    """拉取单只股票3年历史K线"""
    try:
        df = ak.stock_zh_a_hist(
            symbol=code, period="daily",
            start_date=START_DATE, end_date=END_DATE,
            adjust=""  # 不复权
        )
        if df is None or df.empty:
            return code, None
        
        # 重命名列
        df = df.rename(columns={
            '日期': 'trade_date', '股票代码': 'stock_code',
            '开盘': 'open', '收盘': 'close', '最高': 'high', '最低': 'low',
            '成交量': 'volume', '成交额': 'amount', '振幅': 'amplitude',
            '涨跌幅': 'pct_chg', '涨跌额': 'change', '换手率': 'turnover_rate'
        })
        df['trade_date'] = pd.to_datetime(df['trade_date']).dt.strftime('%Y-%m-%d')
        df['pre_close'] = df['close'] - df['change']
        return code, df[['stock_code', 'trade_date', 'open', 'close', 'high', 'low', 
                         'pre_close', 'volume', 'amount', 'pct_chg', 'turnover_rate']]
    except Exception as e:
        return code, None

def save_to_db(dfs):
    """批量保存到DB"""
    conn = sqlite3.connect(DB_PATH)
    saved = 0
    for df in dfs:
        if df is not None and not df.empty:
            for _, row in df.iterrows():
                try:
                    conn.execute("""
                        INSERT OR IGNORE INTO daily_price 
                        (stock_code, trade_date, open, close, high, low, pre_close, volume, amount)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        str(row['stock_code']), str(row['trade_date']),
                        float(row['open']) if row['open'] else 0,
                        float(row['close']) if row['close'] else 0,
                        float(row['high']) if row['high'] else 0,
                        float(row['low']) if row['low'] else 0,
                        float(row['pre_close']) if row['pre_close'] else 0,
                        float(row['volume']) if row['volume'] else 0,
                        float(row['amount']) if row['amount'] else 0,
                    ))
                    saved += 1
                except:
                    pass
    conn.commit()
    conn.close()
    return saved

if __name__ == '__main__':
    codes = get_stock_list()
    logger.info(f"开始补采 {len(codes)} 只股票的 {START_DATE}~{END_DATE} 日K线")
    
    total_saved = 0
    failed = []
    start = time.time()
    
    # 逐只拉取(akshare限流,不要并发太多)
    for i, code in enumerate(codes):
        code, df = fetch_one_stock(code)
        if df is not None and not df.empty:
            saved = save_to_db([df])
            total_saved += saved
        else:
            failed.append(code)
        
        if (i + 1) % 100 == 0:
            elapsed = time.time() - start
            rate = (i + 1) / elapsed
            eta = (len(codes) - i - 1) / rate / 60
            logger.info(f"进度: {i+1}/{len(codes)}, 已存{total_saved}条, "
                       f"失败{len(failed)}, 速度{rate:.1f}只/s, ETA {eta:.0f}min")
        
        time.sleep(0.1)  # 限流
    
    elapsed = time.time() - start
    logger.info(f"完成! 存入{total_saved}条, 失败{len(failed)}只, 耗时{elapsed/60:.1f}min")
    
    if failed:
        logger.info(f"失败代码(前20): {failed[:20]}")
    
    # 验证
    conn = sqlite3.connect(DB_PATH)
    r = conn.execute("SELECT MIN(trade_date), MAX(trade_date), COUNT(DISTINCT trade_date), COUNT(*) FROM daily_price").fetchone()
    logger.info(f"验证: daily_price {r[0]}~{r[1]}, {r[2]}天, {r[3]}条")
    conn.close()
