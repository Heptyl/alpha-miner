"""龙虎榜席位明细采集 — 基于akshare stock_lhb_stock_detail_em

采集每只上榜股票的买入/卖出TOP5营业部明细，分类为机构/游资/散户。
"""

import logging
import sqlite3
import time
from pathlib import Path

import akshare as ak
import pandas as pd

logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).parent.parent.parent.parent / "data" / "alpha_miner.db"

# ── 席位分类规则 ──
# 机构: 券商自营/资管/基金/保险/社保/QFII/北向
INSTITUTION_PATTERNS = [
    "机构专用", "自营", "资管", "基金", "保险", "社保",
    "QFII", "RQFII", "深股通", "沪股通", "北向",
    "中信证券北京总部", "中信证券上海分公司", "中国国际金融上海",
    "中金公司上海", "中金公司北京", "中信建投北京总部",
]
# 游资: 已知活跃游资席位(东财拉萨系列=散户集中营, 其他=知名游资)
HOT_MONEY_PATTERNS = [
    "东方财富拉萨", "东方财富证券股份有限公司拉萨",
    "国泰海通顺德", "中信证券绍兴", "华泰证券深圳益田路",
    "招商证券深圳蛇口", "中泰证券上海建国中路", "海通证券上海建国西路",
    "华鑫证券上海红宝石路", "银河证券绍兴", "财通证券杭州上塘路",
    "华泰证券台州中心大道", "浙商证券杭州杭大路",
]


def _classify_seat(seat_name: str) -> str:
    """席位分类: 机构/游资/散户/普通"""
    if not seat_name:
        return "普通"
    for p in INSTITUTION_PATTERNS:
        if p in seat_name:
            return "机构"
    for p in HOT_MONEY_PATTERNS:
        if p in seat_name:
            return "游资"
    # 东财拉萨系列=散户集中营
    if "拉萨" in seat_name:
        return "散户"
    return "普通"


def fetch_stock_seats(stock_code: str, trade_date: str, retries: int = 2) -> list[dict]:
    """采集单只股票的龙虎榜席位明细

    Args:
        stock_code: 股票代码(6位)
        trade_date: 上榜日期 YYYYMMDD 或 YYYY-MM-DD

    Returns:
        list of {stock_code, trade_date, seat_name, direction, buy_amount, sell_amount,
                 net_amount, seat_type, reason}
    """
    date_str = trade_date.replace("-", "")
    rows = []

    for flag in ("买入", "卖出"):
        for attempt in range(retries):
            try:
                df = ak.stock_lhb_stock_detail_em(
                    symbol=stock_code, date=date_str, flag=flag,
                )
                if df is None or df.empty:
                    break

                for _, r in df.iterrows():
                    seat_name = str(r.get("交易营业部名称", ""))
                    if not seat_name or seat_name == "nan":
                        continue
                    rows.append({
                        "stock_code": stock_code,
                        "trade_date": trade_date if "-" in trade_date else f"{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:]}",
                        "seat_name": seat_name,
                        "direction": flag,
                        "buy_amount": float(r.get("买入金额", 0) or 0),
                        "sell_amount": float(r.get("卖出金额", 0) or 0),
                        "net_amount": float(r.get("净额", 0) or 0),
                        "seat_type": _classify_seat(seat_name),
                        "reason": str(r.get("类型", "")),
                    })
                break
            except Exception as e:
                if attempt < retries - 1:
                    time.sleep(1)
                else:
                    logger.debug(f"[lhb_seats] {stock_code} {flag}明细获取失败: {e}")

    return rows


def fetch_date_seats(trade_date: str) -> list[dict]:
    """采集某日所有上榜股票的席位明细

    流程:
      1. stock_lhb_detail_em 获取当日上榜股票列表
      2. 逐只调用 stock_lhb_stock_detail_em 获取买卖TOP5
      3. 分类保存

    Args:
        trade_date: YYYY-MM-DD
    """
    date_str = trade_date.replace("-", "")

    # Step 1: 获取当日上榜股票列表
    try:
        df = ak.stock_lhb_detail_em(start_date=date_str, end_date=date_str)
        if df is None or df.empty:
            logger.info(f"[lhb_seats] {trade_date} 无龙虎榜数据")
            return []
    except Exception as e:
        logger.warning(f"[lhb_seats] 获取{trade_date}上榜列表失败: {e}")
        return []

    codes = df["代码"].unique().tolist()
    logger.info(f"[lhb_seats] {trade_date} 共{len(codes)}只上榜, 开始采集席位明细...")

    # Step 2: 逐只采集
    all_rows = []
    for i, code in enumerate(codes):
        rows = fetch_stock_seats(code, trade_date)
        all_rows.extend(rows)
        if (i + 1) % 20 == 0:
            logger.info(f"[lhb_seats] 进度 {i+1}/{len(codes)}, 已采集{len(all_rows)}条")
        time.sleep(0.3)  # 限速

    logger.info(f"[lhb_seats] {trade_date} 采集完成: {len(codes)}只, {len(all_rows)}条席位明细")
    return all_rows


def save(rows: list[dict], db_path: str = None) -> int:
    """写入lhb_seats表"""
    if not rows:
        return 0

    path = db_path or str(DB_PATH)
    conn = sqlite3.connect(path)
    try:
        # 确保表存在
        conn.execute("""
            CREATE TABLE IF NOT EXISTS lhb_seats (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                stock_code    TEXT NOT NULL,
                trade_date    TEXT NOT NULL,
                seat_name     TEXT NOT NULL,
                direction     TEXT NOT NULL,
                buy_amount    REAL DEFAULT 0,
                sell_amount   REAL DEFAULT 0,
                net_amount    REAL DEFAULT 0,
                seat_type     TEXT DEFAULT '',
                reason        TEXT DEFAULT '',
                snapshot_time TEXT DEFAULT (datetime('now')),
                UNIQUE(stock_code, trade_date, seat_name, direction)
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_lhb_seats_code ON lhb_seats(stock_code)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_lhb_seats_date ON lhb_seats(trade_date)")

        # 删当天旧数据
        dates = set(r["trade_date"] for r in rows)
        for d in dates:
            conn.execute("DELETE FROM lhb_seats WHERE trade_date = ?", (d,))

        cnt = 0
        for r in rows:
            try:
                conn.execute("""
                    INSERT OR REPLACE INTO lhb_seats
                    (stock_code, trade_date, seat_name, direction,
                     buy_amount, sell_amount, net_amount, seat_type, reason)
                    VALUES (?,?,?,?,?,?,?,?,?)
                """, (
                    r["stock_code"], r["trade_date"], r["seat_name"], r["direction"],
                    r["buy_amount"], r["sell_amount"], r["net_amount"],
                    r["seat_type"], r["reason"],
                ))
                cnt += 1
            except Exception:
                pass
        conn.commit()
        return cnt
    finally:
        conn.close()


def get_seat_summary(stock_code: str, trade_date: str = None, db_path: str = None) -> dict:
    """获取个股席位汇总(供策略评分用)

    Returns:
        {
            "inst_net": float,       # 机构净买入
            "hot_money_net": float,  # 游资净买入
            "retail_net": float,     # 散户净买入
            "inst_buy_count": int,   # 机构买入席位数
            "top_buy_seat": str,     # 买入最大席位名
            "top_buy_amt": float,    # 买入最大金额
        }
    """
    path = db_path or str(DB_PATH)
    try:
        conn = sqlite3.connect(path)
        if trade_date:
            rows = conn.execute(
                "SELECT seat_name, direction, buy_amount, sell_amount, net_amount, seat_type "
                "FROM lhb_seats WHERE stock_code=? AND trade_date=?",
                (stock_code, trade_date),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT seat_name, direction, buy_amount, sell_amount, net_amount, seat_type "
                "FROM lhb_seats WHERE stock_code=? ORDER BY trade_date DESC LIMIT 20",
                (stock_code,),
            ).fetchall()
        conn.close()
    except Exception:
        return {"inst_net": 0, "hot_money_net": 0, "retail_net": 0,
                "inst_buy_count": 0, "top_buy_seat": "", "top_buy_amt": 0}

    # r[0]=seat_name, r[1]=direction, r[2]=buy_amount, r[3]=sell_amount, r[4]=net_amount, r[5]=seat_type
    inst_net = sum(r[4] or 0 for r in rows if r[5] == "机构")
    hot_money_net = sum(r[4] or 0 for r in rows if r[5] == "游资")
    retail_net = sum(r[4] or 0 for r in rows if r[5] == "散户")
    inst_buy_count = sum(1 for r in rows if r[5] == "机构" and r[1] == "买入")

    buy_seats = [(r[0], r[2]) for r in rows if r[1] == "买入" and r[2]]
    top_buy_seat = max(buy_seats, key=lambda x: x[1]) if buy_seats else ("", 0)

    return {
        "inst_net": inst_net,
        "hot_money_net": hot_money_net,
        "retail_net": retail_net,
        "inst_buy_count": inst_buy_count,
        "top_buy_seat": top_buy_seat[0],
        "top_buy_amt": top_buy_seat[1],
    }
