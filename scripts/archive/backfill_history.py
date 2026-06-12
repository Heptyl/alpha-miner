#!/usr/bin/env python3
"""
补采集历史数据:
1. 日K线: baostock补 2025-12-01 ~ 2026-02-28 (3个月)
2. 涨停池: akshare补 2026-01-01 ~ 2026-04-02 (3个月)
"""
import sqlite3
import baostock as bs
import time
from datetime import datetime, timedelta

DB_PATH = "data/alpha_miner.db"

def backfill_daily(start_date="2025-12-01", end_date="2026-02-28"):
    """用baostock批量补日K线"""
    db = sqlite3.connect(DB_PATH)
    c = db.cursor()
    
    # 先看哪些日期需要补
    c.execute("""
        SELECT trade_date, COUNT(*) as cnt 
        FROM daily_price 
        WHERE trade_date >= ? AND trade_date <= ?
        GROUP BY trade_date
        ORDER BY trade_date
    """, (start_date, end_date))
    existing = {row[0]: row[1] for row in c.fetchall()}
    
    print(f"=== 补采集日K线: {start_date} ~ {end_date} ===")
    print(f"已有数据: {len(existing)}天")
    for d, cnt in sorted(existing.items()):
        status = "✓" if cnt >= 2000 else f"✗ 只有{cnt}只"
        print(f"  {d}: {cnt}只 {status}")
    
    # baostock获取所有股票列表
    lg = bs.login()
    if lg.error_code != '0':
        print(f"baostock login failed: {lg.error_msg}")
        db.close()
        return
    
    # 获取沪深A股列表
    rs = bs.query_stock_basic()
    stocks = []
    while rs.next():
        row = rs.get_row_data()
        code = row[0]  # sh.600000 / sz.000001
        # 只取沪深A股(不含指数/基金/债券)
        if code.startswith('sh.6') or code.startswith('sz.0') or code.startswith('sz.3'):
            stocks.append(code)
    
    print(f"\nbaostock共 {len(stocks)} 只A股")
    print(f"开始逐只补采集...")
    
    total_inserted = 0
    errors = 0
    batch_size = 100
    
    for i, stock_code in enumerate(stocks):
        # baostock用sh.600000格式, 我们的DB用600000格式
        db_code = stock_code[3:]  # 去掉sh./sz.前缀
        
        try:
            rs = bs.query_history_k_data_plus(
                stock_code,
                "date,code,open,high,low,close,preclose,volume,amount,turn",
                start_date=start_date,
                end_date=end_date,
                frequency="d"
            )
            
            rows = []
            while rs.next():
                rows.append(rs.get_row_data())
            
            for row in rows:
                trade_date = row[0]
                # 检查是否已有(避免重复)
                c.execute("SELECT 1 FROM daily_price WHERE stock_code=? AND trade_date=?", 
                         (db_code, trade_date))
                if c.fetchone():
                    continue
                
                try:
                    c.execute("""
                        INSERT OR IGNORE INTO daily_price 
                        (stock_code, trade_date, open, high, low, close, pre_close, volume, amount, turnover_rate)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        db_code, trade_date,
                        float(row[2]) if row[2] else None,  # open
                        float(row[3]) if row[3] else None,  # high
                        float(row[4]) if row[4] else None,  # low
                        float(row[5]) if row[5] else None,  # close
                        float(row[6]) if row[6] else None,  # pre_close
                        float(row[7]) if row[7] else 0,     # volume
                        float(row[8]) if row[8] else 0,     # amount
                        float(row[9]) if row[9] else None,  # turnover_rate
                    ))
                    total_inserted += 1
                except Exception as e:
                    errors += 1
            
        except Exception as e:
            errors += 1
            if errors <= 5:
                print(f"  错误 {stock_code}: {e}")
        
        # 批量提交
        if (i + 1) % batch_size == 0:
            db.commit()
            print(f"  进度: {i+1}/{len(stocks)} 只, 已插入 {total_inserted} 条, 错误 {errors}")
    
    db.commit()
    bs.logout()
    
    # 验证
    print(f"\n=== 补采集完成 ===")
    print(f"  插入: {total_inserted} 条")
    print(f"  错误: {errors}")
    
    c.execute("""
        SELECT trade_date, COUNT(*) as cnt 
        FROM daily_price 
        WHERE trade_date >= ? AND trade_date <= ?
        GROUP BY trade_date
        ORDER BY trade_date
    """, (start_date, end_date))
    
    print(f"\n  补完后数据量:")
    all_complete = True
    for d, cnt in c.fetchall():
        status = "✓" if cnt >= 2000 else f"✗ 不完整"
        if cnt < 2000:
            all_complete = False
        print(f"    {d}: {cnt}只 {status}")
    
    if all_complete:
        print(f"\n  ✓ 数据完整! 每天都有2000+只")
    else:
        print(f"\n  ✗ 仍有不完整日期")
    
    db.close()


def backfill_zt_pool(start_date="2026-01-01", end_date="2026-04-02"):
    """用akshare补涨停池"""
    import akshare as ak
    
    db = sqlite3.connect(DB_PATH)
    c = db.cursor()
    
    print(f"\n=== 补采集涨停池: {start_date} ~ {end_date} ===")
    
    # 生成交易日列表
    current = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    
    total_inserted = 0
    errors = 0
    
    while current <= end:
        date_str = current.strftime("%Y%m%d")
        
        # 检查是否已有
        c.execute("SELECT COUNT(*) FROM zt_pool WHERE trade_date=?", (date_str,))
        existing = c.fetchone()[0]
        if existing > 0:
            current += timedelta(days=1)
            continue
        
        try:
            df = ak.stock_zt_pool_em(date=date_str)
            if df is None or df.empty:
                current += timedelta(days=1)
                continue
            
            for _, row in df.iterrows():
                code = str(row.get('代码', '')).zfill(6)
                name = str(row.get('名称', ''))
                consecutive_zt = int(row.get('连板数', 1)) if row.get('连板数') else 1
                amount = float(row.get('成交额', 0)) if row.get('成交额') else 0
                industry = str(row.get('所属行业', ''))
                
                c.execute("""
                    INSERT OR IGNORE INTO zt_pool 
                    (stock_code, trade_date, name, consecutive_zt, amount, industry)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (code, date_str, name, consecutive_zt, amount, industry))
                total_inserted += 1
            
            db.commit()
            print(f"  {date_str}: {len(df)}只涨停")
            
        except Exception as e:
            errors += 1
            if errors <= 10:
                print(f"  {date_str}: 失败 - {e}")
        
        current += timedelta(days=1)
        time.sleep(0.5)  # 避免限流
    
    # 验证
    c.execute("""
        SELECT trade_date, COUNT(*) FROM zt_pool 
        WHERE trade_date >= ? AND trade_date <= ?
        GROUP BY trade_date ORDER BY trade_date
    """, (start_date, end_date))
    
    print(f"\n  补完后涨停池:")
    for d, cnt in c.fetchall():
        print(f"    {d}: {cnt}只")
    
    print(f"\n  插入: {total_inserted} 条, 错误: {errors}")
    db.close()


if __name__ == "__main__":
    print("开始补采集历史数据...")
    print()
    
    # Step 1: 补日K线
    backfill_daily()
    
    # Step 2: 补涨停池
    backfill_zt_pool()
    
    print("\n全部完成!")
