"""涨停池 + 炸板池 + 强势股 — akshare 三个接口合一个文件。"""

import time
from datetime import datetime

import akshare as ak
import pandas as pd

from src.data.storage import Storage


def _safe_series(df: pd.DataFrame, col: str, default=0):
    """安全地从 DataFrame 提取列，处理缺失列和非 Series 类型。"""
    if col not in df.columns:
        return default
    val = df[col]
    if isinstance(val, pd.Series):
        return val
    # 有时 akshare 返回的是标量
    return pd.Series([val] * len(df))


def _safe_numeric(df: pd.DataFrame, col: str, default=0.0):
    """安全地转为数值列。"""
    s = _safe_series(df, col, default)
    if isinstance(s, (int, float)):
        return pd.Series([s] * len(df), dtype=float)
    return pd.to_numeric(s, errors="coerce").fillna(default)


def _safe_str(df: pd.DataFrame, col: str, default=""):
    s = _safe_series(df, col, default)
    if isinstance(s, str):
        return pd.Series([s] * len(df), dtype=str)
    return s.astype(str).fillna(default)


# ── 涨停池 ──

def fetch_zt_pool(trade_date: str, retries: int = 3) -> pd.DataFrame:
    """拉取涨停池。"""
    for attempt in range(retries):
        try:
            df = ak.stock_zt_pool_em(date=trade_date.replace("-", ""))
            if df is None or df.empty:
                return pd.DataFrame()

            result = pd.DataFrame({
                "stock_code": df["代码"].values,
                "name": _safe_str(df, "名称", "").values,
                "trade_date": trade_date,
                "consecutive_zt": _safe_numeric(df, "连板数", 1).astype(int).values,
                "amount": _safe_numeric(df, "成交额", 0).values,
                "industry": _safe_str(df, "所属行业", "").values,
                "circulation_mv": _safe_numeric(df, "流通市值", 0).values,
                "open_count": _safe_numeric(df, "炸板次数", 0).astype(int).values,
                "zt_stats": _safe_str(df, "涨停统计", "").values,
            })
            return result
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(2)
            else:
                print(f"[zt_pool] 拉取失败: {e}")
                return pd.DataFrame()


def save_zt_pool(df: pd.DataFrame, db: Storage) -> int:
    if df.empty:
        return 0
    return db.insert("zt_pool", df)


# ── 炸板池 ──

def fetch_zb_pool(trade_date: str, retries: int = 3) -> pd.DataFrame:
    """拉取炸板池。"""
    for attempt in range(retries):
        try:
            df = ak.stock_zt_pool_zbgc_em(date=trade_date.replace("-", ""))
            if df is None or df.empty:
                return pd.DataFrame()

            result = pd.DataFrame({
                "stock_code": df["代码"].values,
                "trade_date": trade_date,
                "amount": _safe_numeric(df, "成交额", 0).values,
                "open_count": _safe_numeric(df, "炸板次数", 0).astype(int).values,
            })
            return result
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(2)
            else:
                print(f"[zb_pool] 拉取失败: {e}")
                return pd.DataFrame()


def save_zb_pool(df: pd.DataFrame, db: Storage) -> int:
    if df.empty:
        return 0
    return db.insert("zb_pool", df)


# ── 强势股 ──

def fetch_strong_pool(trade_date: str, retries: int = 3) -> pd.DataFrame:
    """拉取强势股池。"""
    for attempt in range(retries):
        try:
            df = ak.stock_zt_pool_strong_em(date=trade_date.replace("-", ""))
            if df is None or df.empty:
                return pd.DataFrame()

            result = pd.DataFrame({
                "stock_code": df["代码"].values,
                "name": _safe_str(df, "名称", "").values,
                "trade_date": trade_date,
                "amount": _safe_numeric(df, "成交额", 0).values,
                "reason": _safe_str(df, "入选理由", "").values,
                "industry": _safe_str(df, "所属行业", "").values,
            })
            return result
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(2)
            else:
                print(f"[strong_pool] 拉取失败: {e}")
                return pd.DataFrame()


def save_strong_pool(df: pd.DataFrame, db: Storage) -> int:
    if df.empty:
        return 0
    return db.insert("strong_pool", df)
