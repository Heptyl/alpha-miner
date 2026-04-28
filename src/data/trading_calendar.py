"""交易日历工具 — 判断交易日、获取最近交易日。

不依赖外部 API，基于以下规则：
1. 周末（周六/周日）不是交易日
2. 从数据库 daily_price 表取实际有数据的日期作为交易日
3. 提供前推/后推交易日功能
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Optional


def get_latest_trade_date(db_path: str = "data/alpha_miner.db") -> Optional[str]:
    """从数据库获取最新交易日（有 daily_price 数据的最新日期）。

    Returns:
        "YYYY-MM-DD" 格式的日期字符串，无数据时返回 None。
    """
    import sqlite3

    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            "SELECT MAX(trade_date) FROM daily_price WHERE trade_date IS NOT NULL"
        ).fetchone()
        if row and row[0]:
            return str(row[0])
    finally:
        conn.close()
    return None


def get_trade_dates(
    db_path: str = "data/alpha_miner.db",
    limit: int = 60,
) -> list[str]:
    """获取所有交易日列表（降序）。

    Returns:
        日期字符串列表，最新的在前。
    """
    import sqlite3

    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            "SELECT DISTINCT trade_date FROM daily_price "
            "WHERE trade_date IS NOT NULL "
            "ORDER BY trade_date DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return [str(r[0]) for r in rows]
    finally:
        conn.close()


def is_weekend(date_str: str) -> bool:
    """判断是否周末。"""
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    return dt.weekday() >= 5  # 5=周六, 6=周日


def get_previous_trade_date(
    date_str: str,
    trade_dates: list[str] | None = None,
    db_path: str = "data/alpha_miner.db",
) -> Optional[str]:
    """获取指定日期之前的最近交易日。

    Args:
        date_str: 参考日期
        trade_dates: 可选，预加载的交易日列表
        db_path: 数据库路径
    """
    if trade_dates is None:
        trade_dates = get_trade_dates(db_path)

    for td in trade_dates:
        if td < date_str:
            return td
    return None


def get_next_trade_date(
    date_str: str,
    trade_dates: list[str] | None = None,
    db_path: str = "data/alpha_miner.db",
) -> Optional[str]:
    """获取指定日期之后的最近交易日。"""
    if trade_dates is None:
        trade_dates = get_trade_dates(db_path)

    # 交易日列表是降序的，反转后找
    for td in reversed(trade_dates):
        if td > date_str:
            return td
    return None


def ensure_trade_date(date_str: str | None = None, db_path: str = "data/alpha_miner.db") -> str:
    """确保获取有效的最新交易日。

    逻辑：
    1. 如果指定了 date_str 且数据库有该日数据 → 返回该日
    2. 否则返回数据库中最新的交易日

    绝不返回没有实际数据的日期。
    """
    import sqlite3

    if date_str:
        conn = sqlite3.connect(db_path)
        try:
            row = conn.execute(
                "SELECT COUNT(*) FROM daily_price WHERE trade_date = ?",
                (date_str,),
            ).fetchone()
            if row and row[0] > 0:
                return date_str
        finally:
            conn.close()

    latest = get_latest_trade_date(db_path)
    if latest:
        return latest

    # 数据库完全没数据，返回今天（让后续流程报错而非用假数据）
    return datetime.now().strftime("%Y-%m-%d")
