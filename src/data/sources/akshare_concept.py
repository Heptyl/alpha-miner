"""板块概念映射 — akshare stock_board_concept_name_em。

不稳定接口，做好 fallback（异常时读缓存表旧数据）。
"""

import time
from datetime import datetime

import akshare as ak
import pandas as pd

from src.data.storage import Storage


def fetch(trade_date: str, retries: int = 3, db: Storage = None) -> pd.DataFrame:
    """拉取板块概念映射。

    获取所有概念板块及其成分股，构建 stock_code <-> concept_name 映射。

    Args:
        trade_date: 交易日期 YYYY-MM-DD
        retries: 重试次数
        db: 数据库实例，用于 fallback

    Returns:
        DataFrame: stock_code, concept_name
    """
    for attempt in range(retries):
        try:
            # 获取概念板块列表
            concepts_df = ak.stock_board_concept_name_em()
            if concepts_df is None or concepts_df.empty:
                if attempt < retries - 1:
                    time.sleep(3)
                    continue
                return _fallback(db, trade_date)

            all_mappings = []
            concept_names = concepts_df["板块名称"].tolist()

            # 逐个概念拉成分股（这个接口限流严重）
            for i, concept_name in enumerate(concept_names):
                try:
                    members = ak.stock_board_concept_cons_em(symbol=concept_name)
                    if members is not None and not members.empty:
                        codes = members["代码"].tolist()
                        for code in codes:
                            all_mappings.append({
                                "stock_code": code,
                                "concept_name": concept_name,
                            })
                    # 限流：每拉 10 个概念歇一下
                    if (i + 1) % 10 == 0:
                        time.sleep(1)
                except Exception:
                    continue

            if not all_mappings:
                return _fallback(db, trade_date)

            return pd.DataFrame(all_mappings)

        except Exception as e:
            if attempt < retries - 1:
                print(f"[concept] 尝试 {attempt + 1}/{retries} 失败: {e}")
                time.sleep(5)
            else:
                print(f"[concept] 拉取失败，使用 fallback: {e}")
                return _fallback(db, trade_date)

    return _fallback(db, trade_date)


def _fallback(db: Storage, trade_date: str) -> pd.DataFrame:
    """读取缓存表中的旧数据作为 fallback。"""
    if db is None:
        return pd.DataFrame()
    try:
        df = db.query("concept_mapping", datetime(2099, 1, 1))
        if not df.empty:
            print(f"[concept] fallback: 使用缓存中的 {len(df)} 条映射")
        return df.drop(columns=["snapshot_time"], errors="ignore")
    except Exception:
        return pd.DataFrame()


def save(df: pd.DataFrame, db: Storage) -> int:
    """将概念映射写入数据库。"""
    if df.empty:
        return 0
    return db.insert("concept_mapping", df)
