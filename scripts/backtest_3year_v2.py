"""
3年期回测 v2 — 优化版，用SQL预过滤涨停股
"""
import sqlite3
import statistics
from collections import defaultdict

DB_PATH = 'data/alpha_miner.db'
COST = 0.005

def run():
    conn = sqlite3.connect(DB_PATH)
    
    # 1. 预计算每天涨停股(涨幅>=9.5%)
    print("预计算涨停日...")
    zt_by_date = defaultdict(list)
    for row in conn.execute("""
        SELECT stock_code, trade_date
        FROM daily_price
        WHERE open > 0 AND close > 0 AND pre_close > 0
          AND (close / pre_close - 1) >= 0.095
          AND stock_code NOT LIKE '688%' AND stock_code NOT LIKE '689%'
          AND stock_code NOT LIKE '200%' AND stock_code NOT LIKE '900%'
          AND stock_code NOT LIKE '8%' AND stock_code NOT LIKE '9%'
    """):
        zt_by_date[row[1]].append(row[0])
    
    print(f"  涨停日: {len(zt_by_date)}天, 总涨停{sum(len(v) for v in zt_by_date.values()):,}条")
    
    # 2. 获取所有交易日
    all_dates = sorted(conn.execute(
        "SELECT DISTINCT trade_date FROM daily_price ORDER BY trade_date"
    ).fetchall())
    all_dates = [d[0] for d in all_dates]
    date_idx = {d: i for i, d in enumerate(all_dates)}
    
    # 3. 构建次日数据索引(只取需要的字段)
    print("构建次日数据索引...")
    next_day = {}
    for row in conn.execute("""
        SELECT stock_code, trade_date, open, close, pre_close
        FROM daily_price
        WHERE open > 0 AND close > 0 AND pre_close > 0
          AND stock_code NOT LIKE '688%' AND stock_code NOT LIKE '689%'
          AND stock_code NOT LIKE '200%' AND stock_code NOT LIKE '900%'
          AND stock_code NOT LIKE '8%' AND stock_code NOT LIKE '9%'
    """):
        code, date, o, c, pc = row
        next_day[(code, date)] = (o, c, pc)
    
    print(f"  索引条数: {len(next_day):,}")
    
    # 4. 不同涨停数阈值回测
    print(f"\n{'='*60}")
    print(f"3年期回测 — 策略B「涨停次日低吸」(含涨停数过滤)")
    print(f"{'='*60}")
    
    for zt_limit in [0, 200, 300, 500]:
        results = []
        yearly = defaultdict(list)
        
        for date in sorted(zt_by_date.keys()):
            zt_stocks = zt_by_date[date]
            
            # 涨停数限制
            if zt_limit > 0 and len(zt_stocks) > zt_limit:
                continue
            
            # 找次日
            di = date_idx.get(date)
            if di is None or di + 1 >= len(all_dates):
                continue
            next_date = all_dates[di + 1]
            
            for code in zt_stocks:
                nd = next_day.get((code, next_date))
                if not nd:
                    continue
                buy_open, sell_close, pre_close = nd
                if pre_close <= 0:
                    continue
                
                open_chg = (buy_open / pre_close - 1) * 100
                if open_chg > -2.0 or open_chg < -8.0:
                    continue
                
                pnl = (sell_close / buy_open - 1) * 100 - COST * 100
                results.append(pnl)
                yearly[date[:4]].append(pnl)
        
        if results:
            w = [p for p in results if p > 0]
            l = [p for p in results if p <= 0]
            wr = len(w)/len(results)*100
            avg = statistics.mean(results)
            pf = sum(w)/abs(sum(l)) if l else 999
            sharpe = avg / statistics.stdev(results) if len(results) > 1 else 0
            
            label = f'ZT上限={zt_limit}' if zt_limit > 0 else '无过滤'
            print(f'\n--- {label} ---')
            print(f'  总计: {len(results):>6}笔  胜率{wr:>5.1f}%  均赚{avg:>+5.2f}%  PF={pf:>5.2f}  夏普={sharpe:>5.2f}')
            
            for yr in sorted(yearly.keys()):
                yt = yearly[yr]
                yw = [p for p in yt if p > 0]
                yl = [p for p in yt if p <= 0]
                ywr = len(yw)/len(yt)*100 if yt else 0
                yavg = statistics.mean(yt) if yt else 0
                ypf = sum(yw)/abs(sum(yl)) if yl else 999
                print(f'  {yr}: {len(yt):>5}笔  胜率{ywr:>5.1f}%  均赚{yavg:>+5.2f}%  PF={ypf:>5.2f}')
    
    conn.close()

if __name__ == '__main__':
    run()
