"""基本面数据采集 — 用新浪财经接口获取 PE/PB/ROE/市值/ST状态。

数据源：
1. 新浪财经个股基本面（免费、稳定、不限频）
2. akshare 作为备用

采集字段：
- pe_ttm: 滚动市盈率
- pb: 市净率
- roe: 净资产收益率(%)
- revenue_yoy: 营收同比增速(%)
- profit_yoy: 净利润同比增速(%)
- net_profit: 净利润(亿)
- is_st: 是否ST (0/1)
- total_mv: 总市值(亿)
- circulating_mv: 流通市值(亿)
"""

from __future__ import annotations

import json
import re
import sqlite3
import time
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

DB_PATH = "data/alpha_miner.db"


def ensure_table(db_path: str = DB_PATH) -> None:
    """确保 stock_fundamentals 表存在。"""
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS stock_fundamentals (
            stock_code   TEXT NOT NULL,
            trade_date   TEXT NOT NULL,
            pe_ttm       REAL,
            pb           REAL,
            roe          REAL,
            revenue_yoy  REAL,
            profit_yoy   REAL,
            net_profit   REAL,
            is_st        INTEGER DEFAULT 0,
            total_mv     REAL,
            circulating_mv REAL,
            PRIMARY KEY (stock_code, trade_date)
        )
    """)
    conn.commit()
    conn.close()


def sina_code(code: str) -> str:
    """转为新浪格式：sh600000 / sz000001。"""
    return f"sh{code}" if code.startswith("6") else f"sz{code}"


def fetch_fundamentals_sina(code: str, session: requests.Session = None) -> dict | None:
    """从新浪财经获取单只股票基本面。

    接口：https://finance.sina.com.cn/realstock/company/{code}/nc.shtml
    另一个更可靠的：
    https://money.finance.sina.com.cn/quotes_service/api/json_v2.sinajs/rn=xxx&list=sh600000
    """
    if session is None:
        session = requests.Session()

    sc = sina_code(code)
    result = {"stock_code": code}

    # 接口1：新浪实时行情（含市值）
    try:
        url = f"https://hq.sinajs.cn/list={sc}"
        headers = {
            "Referer": "https://finance.sina.com.cn",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120",
        }
        r = session.get(url, headers=headers, timeout=10)
        r.encoding = "gbk"
        match = re.search(r'="([^"]*)"', r.text)
        if match:
            parts = match.group(1).split(",")
            if len(parts) >= 32:
                # 新浪行情格式：名称,开盘,昨收,当前价,最高,最低,...,成交量,...,成交额,...,日期,...
                name = parts[0]
                result["name"] = name
                # 判断ST
                result["is_st"] = 1 if "ST" in name or "st" in name else 0
    except Exception:
        pass

    # 接口2：新浪财务摘要（PE/PB/ROE）
    try:
        # 新浪财务数据接口
        url2 = f"https://finance.sina.com.cn/realstock/company/{sc}/nc.shtml"
        headers2 = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120",
            "Referer": "https://finance.sina.com.cn",
        }
        r2 = session.get(url2, headers=headers2, timeout=10)
        text = r2.text

        # 从页面提取 PE
        pe_match = re.search(r'市盈率[：:]\s*([\d.-]+)', text)
        if pe_match:
            result["pe_ttm"] = float(pe_match.group(1))

        # PB
        pb_match = re.search(r'市净率[：:]\s*([\d.-]+)', text)
        if pb_match:
            result["pb"] = float(pb_match.group(1))

        # 总市值
        mv_match = re.search(r'总市值[：:]\s*([\d.]+)\s*亿', text)
        if mv_match:
            result["total_mv"] = float(mv_match.group(1))

        # 流通市值
        cmv_match = re.search(r'流通值[：:]\s*([\d.]+)\s*亿', text)
        if cmv_match:
            result["circulating_mv"] = float(cmv_match.group(1))
    except Exception:
        pass

    # 接口3：用新浪另一个API拿更完整的基本面
    try:
        url3 = f"https://vip.stock.finance.sina.com/corp/go.php/vFD_FinancialGuideLine/stockid/{code}/ctrl/2024/displaytype/4.phtml"
        headers3 = {
            "User-Agent": "Mozilla/5.0",
            "Referer": "https://finance.sina.com.cn",
        }
        r3 = session.get(url3, headers=headers3, timeout=10)
        text3 = r3.text

        # 提取 ROE
        roe_match = re.search(r'净资产收益率[\s\S]*?<td[^>]*>([\d.-]+)</td>', text3)
        if roe_match:
            try:
                result["roe"] = float(roe_match.group(1))
            except ValueError:
                pass

        # 提取净利润同比
        profit_match = re.search(r'净利润同比增长率[\s\S]*?<td[^>]*>([\d.-]+)</td>', text3)
        if profit_match:
            try:
                result["profit_yoy"] = float(profit_match.group(1))
            except ValueError:
                pass

        # 提取营收同比
        rev_match = re.search(r'营业收入同比增长率[\s\S]*?<td[^>]*>([\d.-]+)</td>', text3)
        if rev_match:
            try:
                result["revenue_yoy"] = float(rev_match.group(1))
            except ValueError:
                pass
    except Exception:
        pass

    return result if len(result) > 2 else None


def fetch_fundamentals_batch(
    codes: list[str],
    db_path: str = DB_PATH,
    batch_size: int = 50,
    delay: float = 0.5,
) -> dict[str, dict]:
    """批量采集基本面数据。

    Returns:
        {stock_code: {pe_ttm, pb, roe, ...}}
    """
    ensure_table(db_path)
    conn = sqlite3.connect(db_path)
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120",
        "Referer": "https://finance.sina.com.cn",
    })

    # 已有的跳过
    trade_date = pd.Timestamp.now().strftime("%Y-%m-%d")
    existing = set(r[0] for r in conn.execute(
        "SELECT stock_code FROM stock_fundamentals WHERE trade_date = ?", (trade_date,)
    ).fetchall())

    results = {}
    inserted = 0
    errors = 0

    for i, code in enumerate(codes):
        if code in existing:
            continue

        try:
            data = fetch_fundamentals_sina(code, session)
            if data and len(data) > 2:
                results[code] = data
                conn.execute(
                    """INSERT OR REPLACE INTO stock_fundamentals
                    (stock_code, trade_date, pe_ttm, pb, roe, revenue_yoy, profit_yoy, is_st, total_mv, circulating_mv)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        code, trade_date,
                        data.get("pe_ttm"), data.get("pb"), data.get("roe"),
                        data.get("revenue_yoy"), data.get("profit_yoy"),
                        data.get("is_st", 0),
                        data.get("total_mv"), data.get("circulating_mv"),
                    ),
                )
                inserted += 1
        except Exception:
            errors += 1

        if (i + 1) % batch_size == 0:
            conn.commit()
            print(f"  [{i+1}/{len(codes)}] 插入:{inserted} 错误:{errors}")
            time.sleep(delay)

    conn.commit()
    conn.close()
    print(f"完成: 插入:{inserted} 错误:{errors}")
    return results


def get_fundamentals(
    codes: list[str],
    db_path: str = DB_PATH,
) -> dict[str, dict]:
    """获取指定股票的基本面数据（从数据库）。"""
    conn = sqlite3.connect(db_path)
    result = {}
    for code in codes:
        row = conn.execute(
            """SELECT pe_ttm, pb, roe, revenue_yoy, profit_yoy, net_profit,
                      is_st, total_mv, circulating_mv
               FROM stock_fundamentals
               WHERE stock_code = ?
               ORDER BY trade_date DESC LIMIT 1""",
            (code,),
        ).fetchone()
        if row:
            result[code] = {
                "pe_ttm": row[0], "pb": row[1], "roe": row[2],
                "revenue_yoy": row[3], "profit_yoy": row[4], "net_profit": row[5],
                "is_st": row[6], "total_mv": row[7], "circulating_mv": row[8],
            }
    conn.close()
    return result


def filter_by_fundamentals(
    codes: list[str],
    db_path: str = DB_PATH,
    max_pe: float = 200.0,
    min_roe: float = -50.0,
    exclude_st: bool = True,
    min_profit_yoy: float = -100.0,
) -> tuple[list[str], dict[str, list[str]]]:
    """基本面过滤。

    Returns:
        (通过的股票列表, {被过滤股票: 过滤原因列表})
    """
    fundamentals = get_fundamentals(codes, db_path)
    passed = []
    rejected = {}

    for code in codes:
        fund = fundamentals.get(code)
        if not fund:
            # 无基本面数据 → 放行（不能因为没有数据就拒绝）
            passed.append(code)
            continue

        reasons = []

        # ST过滤
        if exclude_st and fund["is_st"] == 1:
            reasons.append("ST/退市风险股")

        # PE过滤（负PE=亏损，太高=泡沫）
        if fund["pe_ttm"] is not None:
            if fund["pe_ttm"] < 0:
                reasons.append(f"亏损(PE={fund['pe_ttm']:.0f})")
            elif fund["pe_ttm"] > max_pe:
                reasons.append(f"估值过高(PE={fund['pe_ttm']:.0f})")

        # ROE过滤
        if fund["roe"] is not None and fund["roe"] < min_roe:
            reasons.append(f"ROE过低({fund['roe']:.1f}%)")

        # 净利润增速
        if fund["profit_yoy"] is not None and fund["profit_yoy"] < min_profit_yoy:
            reasons.append(f"利润大降({fund['profit_yoy']:.1f}%)")

        if reasons:
            rejected[code] = reasons
        else:
            passed.append(code)

    return passed, rejected
