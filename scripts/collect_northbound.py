"""北向资金采集器 — 历史回填+每日更新

数据源:
  1. akshare stock_hsgt_hist_em() — 历史数据(到~2026-04, 之后东财停止更新)
  2. 同花顺 hsgtApi — 盘中实时(收盘后的当日累计净买额)
  3. 东财datacenter RPT_MUTUAL_QUOTA — 备用

参考: astock get_northbound_flow() 的CSV自缓存机制, 我们用SQLite替代

用法:
  uv run python scripts/collect_northbound.py           # 当日采集
  uv run python scripts/collect_northbound.py --backfill # 历史回填
  uv run python scripts/collect_northbound.py --backfill --days 60  # 最近60天
"""

import argparse
import json
import logging
import sqlite3
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

DB_PATH = Path(__file__).resolve().parents[1] / "data" / "alpha_miner.db"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def _ensure_table(conn: sqlite3.Connection):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS northbound_flow (
            trade_date   TEXT PRIMARY KEY,
            hgt_net      REAL,
            sgt_net      REAL,
            total_net    REAL,
            hgt_buy      REAL,
            hgt_sell     REAL,
            sgt_buy      REAL,
            sgt_sell     REAL,
            snapshot_time TEXT DEFAULT (datetime('now'))
        )
    """)


def _upsert_row(conn: sqlite3.Connection, row: dict):
    conn.execute("""
        INSERT OR REPLACE INTO northbound_flow
        (trade_date, hgt_net, sgt_net, total_net, hgt_buy, hgt_sell, sgt_buy, sgt_sell, snapshot_time)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
    """, (
        row["trade_date"],
        row.get("hgt_net"), row.get("sgt_net"), row.get("total_net"),
        row.get("hgt_buy"), row.get("hgt_sell"),
        row.get("sgt_buy"), row.get("sgt_sell"),
    ))


def _fetch_realtime_hexin() -> dict | None:
    """同花顺hsgtApi实时数据(盘中/盘后都可获取当日累计)"""
    import requests

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/117.0.0.0 Safari/537.36",
        "Host": "data.hexin.cn",
        "Referer": "https://data.hexin.cn/",
    }
    try:
        r = requests.get(
            "https://data.hexin.cn/market/hsgtApi/method/dayChart/",
            headers=headers, timeout=10,
        )
        d = r.json()
        times = d.get("time", [])
        hgt = d.get("hgt", [])
        sgt = d.get("sgt", [])
        if not times:
            return None

        hgt_close = float(hgt[-1]) if hgt else 0
        sgt_close = float(sgt[-1]) if sgt else 0
        return {
            "trade_date": datetime.now().strftime("%Y-%m-%d"),
            "hgt_net": round(hgt_close, 2),
            "sgt_net": round(sgt_close, 2),
            "total_net": round(hgt_close + sgt_close, 2),
            "source": "同花顺hsgtApi",
        }
    except Exception as e:
        logger.warning(f"同花顺hsgtApi失败: {e}")
        return None


def _fetch_realtime_eastmoney() -> dict | None:
    """东财datacenter实时(备用)"""
    from src.data.sources.eastmoney import EastMoneyClient

    client = EastMoneyClient()
    try:
        url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
        params = {
            "reportName": "RPT_MUTUAL_QUOTA",
            "columns": "TRADE_DATE,MUTUAL_TYPE,BOARD_TYPE",
            "quoteColumns": "netBuyAmt~07~BOARD_CODE",
            "quoteType": "0",
            "pageNumber": "1",
            "pageSize": "100",
            "sortTypes": "1",
            "sortColumns": "MUTUAL_TYPE",
            "source": "WEB",
            "client": "WEB",
        }
        data = client._request_json(url, params)
        if not data or not data.get("result"):
            return None
        rows = data["result"].get("data", [])
        hgt_net = 0.0
        sgt_net = 0.0
        trade_date = None
        for r in rows:
            if not trade_date and r.get("TRADE_DATE"):
                trade_date = str(r["TRADE_DATE"])[:10]
            # netBuyAmt 单位:万元
            net = r.get("netBuyAmt~07~BOARD_CODE", 0) or 0
            if r.get("MUTUAL_TYPE") == "001":  # 沪股通
                hgt_net = float(net) / 10000  # 万→亿
            elif r.get("MUTUAL_TYPE") == "003":  # 深股通
                sgt_net = float(net) / 10000
        if not trade_date:
            return None
        return {
            "trade_date": trade_date,
            "hgt_net": round(hgt_net, 2),
            "sgt_net": round(sgt_net, 2),
            "total_net": round(hgt_net + sgt_net, 2),
            "source": "东财datacenter",
        }
    except Exception as e:
        logger.warning(f"东财datacenter实时失败: {e}")
        return None


def collect_today(conn: sqlite3.Connection):
    """采集当日北向资金(同花顺优先, 东财备用)"""
    logger.info("采集当日北向资金...")

    row = _fetch_realtime_hexin()
    if not row:
        row = _fetch_realtime_eastmoney()
    if not row:
        logger.warning("所有源获取失败, 跳过")
        return

    _upsert_row(conn, row)
    conn.commit()
    logger.info(f"当日北向: {row['trade_date']} HGT={row.get('hgt_net','?')}亿 "
                f"SGT={row.get('sgt_net','?')}亿 合计={row.get('total_net','?')}亿 "
                f"(来源:{row.get('source','?')})")


def backfill(conn: sqlite3.Connection, days: int = 0):
    """用akshare回填历史数据

    Args:
        days: 回填天数, 0=全量
    """
    import akshare as ak
    import pandas as pd

    logger.info(f"回填北向资金历史 (days={days or '全部'})...")

    # 检查DB中最新日期
    existing = conn.execute(
        "SELECT MAX(trade_date) FROM northbound_flow"
    ).fetchone()[0]

    # 获取沪股通+深股通历史
    dfs = []
    for label in ("沪股通", "深股通"):
        try:
            df = ak.stock_hsgt_hist_em(symbol=label)
            df["_type"] = label
            dfs.append(df)
            logger.info(f"  {label}: {len(df)}条")
        except Exception as e:
            logger.warning(f"  {label}获取失败: {e}")

    if not dfs:
        logger.warning("无数据可回填")
        return

    # 合并沪股通+深股通
    df_h = dfs[0] if dfs[0]["_type"].iloc[0] == "沪股通" else dfs[1]
    df_s = dfs[1] if len(dfs) > 1 and dfs[1]["_type"].iloc[0] == "深股通" else (
        dfs[0] if dfs[0]["_type"].iloc[0] == "深股通" else None
    )

    if df_s is None:
        logger.warning("深股通数据缺失, 无法回填")
        return

    # 统一日期列
    df_h["日期"] = pd.to_datetime(df_h["日期"], errors="coerce").dt.strftime("%Y-%m-%d")
    df_s["日期"] = pd.to_datetime(df_s["日期"], errors="coerce").dt.strftime("%Y-%m-%d")

    # 按日期合并
    merged = df_h[["日期", "当日资金流入", "买入成交额", "卖出成交额"]].rename(columns={
        "当日资金流入": "hgt_net",
        "买入成交额": "hgt_buy",
        "卖出成交额": "hgt_sell",
    }).merge(
        df_s[["日期", "当日资金流入", "买入成交额", "卖出成交额"]].rename(columns={
            "当日资金流入": "sgt_net",
            "买入成交额": "sgt_buy",
            "卖出成交额": "sgt_sell",
        }),
        on="日期", how="outer",
    )
    merged = merged.dropna(subset=["hgt_net", "sgt_net"], how="all")

    # 过滤: 只保留有净流入数据的行
    merged = merged[merged["hgt_net"].notna() | merged["sgt_net"].notna()]

    # 天数过滤
    if days > 0:
        cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        merged = merged[merged["日期"] >= cutoff]

    # 已有数据过滤
    if existing:
        merged = merged[merged["日期"] > existing]

    if merged.empty:
        logger.info("无新增数据需要回填")
        return

    # 写入DB
    count = 0
    for _, r in merged.iterrows():
        hgt = float(r["hgt_net"]) if pd.notna(r.get("hgt_net")) else None
        sgt = float(r["sgt_net"]) if pd.notna(r.get("sgt_net")) else None
        total = 0.0
        if hgt is not None:
            total += hgt
        if sgt is not None:
            total += sgt
        row = {
            "trade_date": r["日期"],
            "hgt_net": hgt,
            "sgt_net": sgt,
            "total_net": round(total, 2) if total else None,
            "hgt_buy": float(r["hgt_buy"]) if pd.notna(r.get("hgt_buy")) else None,
            "hgt_sell": float(r["hgt_sell"]) if pd.notna(r.get("hgt_sell")) else None,
            "sgt_buy": float(r["sgt_buy"]) if pd.notna(r.get("sgt_buy")) else None,
            "sgt_sell": float(r["sgt_sell"]) if pd.notna(r.get("sgt_sell")) else None,
        }
        _upsert_row(conn, row)
        count += 1

    conn.commit()
    logger.info(f"回填完成: {count}条新增")


def show_stats(conn: sqlite3.Connection):
    """显示入库数据统计"""
    total = conn.execute("SELECT COUNT(*) FROM northbound_flow").fetchone()[0]
    latest = conn.execute("SELECT MAX(trade_date) FROM northbound_flow").fetchone()[0]
    earliest = conn.execute("SELECT MIN(trade_date) FROM northbound_flow").fetchone()[0]

    print(f"\n北向资金统计:")
    print(f"  总条数: {total}")
    print(f"  时间范围: {earliest} ~ {latest}")

    if total > 0:
        recent = conn.execute("""
            SELECT trade_date, hgt_net, sgt_net, total_net
            FROM northbound_flow ORDER BY trade_date DESC LIMIT 10
        """).fetchall()
        print(f"\n  最近10天:")
        print(f"  {'日期':<12} {'沪股通':>8} {'深股通':>8} {'合计':>8}")
        print(f"  {'-'*40}")
        for r in recent:
            h = f"{r[1]:+.2f}" if r[1] is not None else "N/A"
            s = f"{r[2]:+.2f}" if r[2] is not None else "N/A"
            t = f"{r[3]:+.2f}" if r[3] is not None else "N/A"
            print(f"  {r[0]:<12} {h:>8} {s:>8} {t:>8}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="北向资金采集器")
    parser.add_argument("--backfill", action="store_true", help="历史回填")
    parser.add_argument("--days", type=int, default=0, help="回填天数(0=全部)")
    args = parser.parse_args()

    conn = sqlite3.connect(str(DB_PATH))
    _ensure_table(conn)

    if args.backfill:
        backfill(conn, days=args.days)

    collect_today(conn)
    show_stats(conn)

    conn.close()
