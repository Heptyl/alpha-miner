"""基本面数据采集器 — 解禁预告 + 大股东增减持 + 财务摘要

数据源: akshare (东方财富)
采集频率: 每日一次
用途: 策略A超跌反弹的基本面过滤
"""
import logging
import sqlite3
import time
from datetime import datetime, timedelta

import akshare as ak
import pandas as pd

logger = logging.getLogger(__name__)

DB_PATH = 'data/alpha_miner.db'

# ============================================================
# 表结构
# ============================================================
SCHEMA = """
CREATE TABLE IF NOT EXISTS restricted_release (
    stock_code TEXT,
    stock_name TEXT,
    release_date TEXT,
    release_type TEXT,
    release_volume REAL,
    release_amount REAL,
    pct_circulate REAL,
    pre_close REAL,
    create_time TEXT,
    PRIMARY KEY (stock_code, release_date, release_type)
);

CREATE TABLE IF NOT EXISTS holder_change (
    stock_code TEXT,
    holder_name TEXT,
    holder_type TEXT,
    change_type TEXT,
    change_volume REAL,
    change_amount REAL,
    change_pct REAL,
    change_date TEXT,
    create_time TEXT,
    PRIMARY KEY (stock_code, holder_name, change_date)
);

CREATE TABLE IF NOT EXISTS financial_summary (
    stock_code TEXT,
    report_date TEXT,
    revenue REAL,
    net_profit REAL,
    revenue_yoy REAL,
    profit_yoy REAL,
    gross_margin REAL,
    net_margin REAL,
    roe REAL,
    debt_ratio REAL,
    create_time TEXT,
    PRIMARY KEY (stock_code, report_date)
);

CREATE TABLE IF NOT EXISTS stock_blacklist (
    stock_code TEXT PRIMARY KEY,
    reason TEXT,
    expire_date TEXT,
    create_time TEXT
);
"""


def init_tables(conn):
    for sql in SCHEMA.split(';'):
        sql = sql.strip()
        if sql:
            conn.execute(sql)
    conn.commit()


# ============================================================
# 解禁预告
# ============================================================
def fetch_restricted_release(days_ahead=30):
    """拉取未来N天的解禁预告"""
    try:
        today = datetime.now()
        end = today + timedelta(days=days_ahead)
        df = ak.stock_restricted_release_detail_em(
            start_date=today.strftime('%Y%m%d'),
            end_date=end.strftime('%Y%m%d')
        )
        if df.empty:
            logger.info("解禁预告: 无数据")
            return 0
        
        conn = sqlite3.connect(DB_PATH)
        count = 0
        for _, row in df.iterrows():
            try:
                conn.execute("""
                    INSERT OR REPLACE INTO restricted_release 
                    (stock_code, stock_name, release_date, release_type, 
                     release_volume, release_amount, pct_circulate, pre_close, create_time)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    str(row.get('股票代码', '')),
                    str(row.get('股票简称', '')),
                    str(row.get('解禁时间', '')),
                    str(row.get('限售股类型', '')),
                    float(row.get('实际解禁数量', 0) or 0),
                    float(row.get('实际解禁市值', 0) or 0),
                    float(row.get('占解禁前流通市值比例', 0) or 0),
                    float(row.get('解禁前一交易日收盘价', 0) or 0),
                    datetime.now().isoformat(),
                ))
                count += 1
            except Exception as e:
                logger.warning("解禁数据写入失败: %s", e)
        
        conn.commit()
        conn.close()
        logger.info("解禁预告: 写入%d条", count)
        return count
    except Exception as e:
        logger.error("解禁预告采集失败: %s", e)
        return 0


# ============================================================
# 大股东增减持 (近30天)
# ============================================================
def fetch_holder_changes(stock_code: str):
    """拉取个股大股东增减持(同花顺接口)"""
    try:
        df = ak.stock_shareholder_change_ths(symbol=stock_code)
        if df.empty:
            return 0
        
        conn = sqlite3.connect(DB_PATH)
        count = 0
        now = datetime.now().isoformat()
        for _, row in df.iterrows():
            try:
                # 解析"减持127.45万"/"增持50.00万"格式
                raw_qty = str(row.get('变动数量', ''))
                change_type = '减持' if '减' in raw_qty else ('增持' if '增' in raw_qty else '其他')
                # 提取数值
                import re
                m = re.search(r'([\d.]+)(万)?', raw_qty)
                qty = float(m.group(1)) if m else 0
                if m and m.group(2) == '万':
                    qty *= 10000
                
                # 变动期间 → 取最新日期
                period = str(row.get('变动期间', ''))
                
                # 公告日期作为change_date
                ann_date = str(row.get('公告日期', ''))
                
                # 交易均价估算金额
                raw_price = row.get('交易均价', 0)
                try:
                    avg_price = float(raw_price)
                except (ValueError, TypeError):
                    avg_price = 0  # "未披露"等非数值
                amount = qty * avg_price
                
                holder_name = str(row.get('变动股东', ''))
                
                conn.execute("""
                    INSERT OR REPLACE INTO holder_change
                    (stock_code, holder_name, holder_type, change_type,
                     change_volume, change_amount, change_pct, change_date, create_time)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    stock_code,
                    holder_name,
                    '股东',
                    change_type,
                    qty,
                    amount,
                    0,  # 同花顺无直接比例
                    ann_date,
                    now,
                ))
                count += 1
            except Exception as e:
                logger.warning("增减持数据写入失败: %s", e)
        
        conn.commit()
        conn.close()
        return count
    except Exception as e:
        logger.error("增减持采集失败 %s: %s", stock_code, e)
        return 0


# ============================================================
# 财务摘要
# ============================================================
def fetch_financial_summary(stock_code: str):
    """拉取个股财务摘要"""
    try:
        df = ak.stock_financial_abstract(symbol=stock_code)
        if df.empty:
            return 0
        
        # 提取关键指标
        conn = sqlite3.connect(DB_PATH)
        
        # 找最新两期报告
        date_cols = [c for c in df.columns if c not in ('选项', '指标') and c.startswith('20')]
        if not date_cols:
            return 0
        
        latest = date_cols[0]  # 最新一期
        prev = date_cols[1] if len(date_cols) > 1 else None  # 上一期
        
        def get_val(indicator: str, col: str):
            row = df[df['指标'] == indicator]
            if row.empty:
                return None
            val = row.iloc[0].get(col)
            try:
                return float(val) if val else None
            except:
                return None
        
        revenue = get_val('营业总收入', latest)
        net_profit = get_val('归母净利润', latest)
        # 同比: 和去年同期比(20260331 vs 20250331)
        yoy_col = str(int(latest[:4])-1) + latest[4:]
        revenue_yoy_col = get_val('营业总收入', yoy_col) if yoy_col in date_cols else None
        profit_yoy_col = get_val('归母净利润', yoy_col) if yoy_col in date_cols else None
        revenue_yoy = (revenue / revenue_yoy_col - 1) * 100 if revenue and revenue_yoy_col and revenue_yoy_col != 0 else None
        profit_yoy = (net_profit / profit_yoy_col - 1) * 100 if net_profit and profit_yoy_col and profit_yoy_col != 0 else None
        gross_margin = get_val('毛利率', latest)
        net_margin = get_val('销售净利率', latest)
        roe = get_val('净资产收益率(ROE)', latest)
        debt_ratio = get_val('资产负债率', latest)
        
        conn.execute("""
            INSERT OR REPLACE INTO financial_summary
            (stock_code, report_date, revenue, net_profit, revenue_yoy,
             profit_yoy, gross_margin, net_margin, roe, debt_ratio, create_time)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            stock_code, latest, revenue, net_profit, revenue_yoy,
            profit_yoy, gross_margin, net_margin, roe, debt_ratio,
            datetime.now().isoformat(),
        ))
        
        conn.commit()
        conn.close()
        return 1
    except Exception as e:
        logger.error("财务摘要采集失败 %s: %s", stock_code, e)
        return 0


# ============================================================
# 基本面过滤器 — 给策略A用
# ============================================================
def check_fundamental_redflag(stock_code: str, conn=None) -> list:
    """检查基本面红旗(解禁/减持/业绩暴雷)
    
    返回红旗列表, 空列表=安全
    """
    if conn is None:
        conn = sqlite3.connect(DB_PATH)
    
    redflags = []
    today = datetime.now().strftime('%Y-%m-%d')
    week_later = (datetime.now() + timedelta(days=7)).strftime('%Y-%m-%d')
    
    # 1. 近7天有解禁
    rows = conn.execute("""
        SELECT release_date, release_amount, pct_circulate, release_type
        FROM restricted_release 
        WHERE stock_code = ? AND release_date BETWEEN ? AND ?
    """, (stock_code, today, week_later)).fetchall()
    
    for r in rows:
        amt_wan = (r[1] or 0) / 10000
        pct = r[2] or 0
        if amt_wan > 1000:  # 解禁市值>1000万
            redflags.append(f"解禁: {r[0]} {r[3]} 解禁{amt_wan:.0f}万 占流通{pct:.1f}%")
    
    # 2. 近30天大股东减持
    month_ago = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    rows = conn.execute("""
        SELECT holder_name, change_volume, change_amount, change_date
        FROM holder_change
        WHERE stock_code = ? AND change_date >= ? AND change_type LIKE '%减%'
    """, (stock_code, month_ago)).fetchall()
    
    for r in rows:
        amt_wan = (r[2] or 0) / 10000
        if abs(amt_wan) > 500:  # 减持金额>500万
            redflags.append(f"减持: {r[0]} {r[3]} 减持{abs(amt_wan):.0f}万")
    
    # 3. 业绩暴雷: 最新一期净利润同比下滑>50% 或 亏损
    row = conn.execute("""
        SELECT profit_yoy, net_profit, report_date
        FROM financial_summary WHERE stock_code = ?
        ORDER BY report_date DESC LIMIT 1
    """, (stock_code,)).fetchone()
    
    if row:
        profit_yoy = row[0]
        net_profit = row[1]
        report_date = row[2]
        if profit_yoy is not None and profit_yoy < -50:
            redflags.append(f"业绩: {report_date}净利润同比{profit_yoy:.1f}%")
        if net_profit is not None and net_profit < 0:
            redflags.append(f"亏损: {report_date}净利润{net_profit/1e8:.2f}亿") if net_profit else None
    
    return redflags


# ============================================================
# 批量采集
# ============================================================
def collect_all(stock_codes: list = None):
    """批量采集基本面数据"""
    conn = sqlite3.connect(DB_PATH)
    init_tables(conn)
    conn.close()
    
    # 1. 解禁预告(全市场)
    logger.info("采集解禁预告...")
    fetch_restricted_release(days_ahead=30)
    
    # 2. 增减持+财务摘要(按需采集)
    if stock_codes:
        for i, code in enumerate(stock_codes):
            if i % 50 == 0:
                logger.info("基本面采集进度: %d/%d", i, len(stock_codes))
            fetch_holder_changes(code)
            fetch_financial_summary(code)
            time.sleep(0.3)  # 控制频率


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    collect_all()
