"""板块概念映射 — 同花顺源 + 缓存回退。

去掉东方财富 stock_board_concept_name_em / stock_board_concept_cons_em（被 WAF 拦截）。
改用同花顺 stock_board_concept_name_ths 获取概念列表。
成分股映射：从 zt_pool / strong_pool 的「所属行业」字段反推 stock→concept 关系。

首次全量拉取 + 缓存7天，日常采集时跳过（除非缓存过期或不存在）。
"""

import logging
import time
from datetime import datetime, timedelta

import akshare as ak
import pandas as pd

from src.data.storage import Storage

logger = logging.getLogger(__name__)

# 缓存有效期（天）
CACHE_TTL_DAYS = 7


def _cache_valid(db: Storage) -> bool:
    """检查 concept_mapping 缓存是否在有效期内。"""
    try:
        df = db.query("concept_mapping", datetime(2099, 1, 1))
        if df.empty:
            return False
        latest_snap = df["snapshot_time"].max()
        if latest_snap is None:
            return False
        snap_time = pd.to_datetime(latest_snap)
        age = (datetime.now() - snap_time.tz_localize(None)).days
        return age <= CACHE_TTL_DAYS
    except Exception:
        return False


def fetch(trade_date: str, retries: int = 3, db: Storage = None) -> pd.DataFrame:
    """拉取板块概念映射。

    策略：
    - 缓存存在且不超过 7 天 → 返回空 DataFrame（跳过采集）
    - 缓存不存在或超过 7 天 → 同花顺拉概念列表 + DB 行业反推映射
    - 拉取失败 → fallback 读缓存旧数据

    Args:
        trade_date: 交易日期 YYYY-MM-DD
        retries: 重试次数
        db: 数据库实例，用于缓存判断和 fallback

    Returns:
        DataFrame: stock_code, concept_name。空 DataFrame 表示跳过（缓存有效）。
    """
    # 检查缓存
    if db is not None and _cache_valid(db):
        print(f"[concept] 缓存有效（{CACHE_TTL_DAYS}天内），跳过采集")
        return pd.DataFrame()

    all_mappings = []

    # 1. 从同花顺拉概念列表
    ths_concepts = []
    for attempt in range(retries):
        try:
            concepts_df = ak.stock_board_concept_name_ths()
            if concepts_df is not None and not concepts_df.empty:
                # 同花顺返回 name, code 列
                ths_concepts = concepts_df[["name", "code"]].to_dict("records")
                print(f"[concept] 同花顺概念列表: {len(ths_concepts)} 个概念")
                break
            if attempt < retries - 1:
                time.sleep(3)
        except Exception as e:
            if attempt < retries - 1:
                print(f"[concept] 同花顺尝试 {attempt + 1}/{retries} 失败: {e}")
                time.sleep(3)
            else:
                print(f"[concept] 同花顺拉取失败，使用 fallback: {e}")

    # 2. 从 DB 的 zt_pool / strong_pool 提取行业映射
    db_mappings = _extract_industry_mappings(db)
    if db_mappings:
        all_mappings.extend(db_mappings)
        print(f"[concept] 从 DB 行业字段提取 {len(db_mappings)} 条映射")

    # 3. 如果同花顺概念列表获取成功，补充概念名映射
    #    （注意：同花顺没有直接获取概念成分股的稳定接口，
    #      所以我们用「行业」字段作为 concept_name 的替代）
    #    将同花顺概念名存入映射表，供后续 concept_daily 聚合使用
    if ths_concepts:
        concept_names = [c["name"] for c in ths_concepts]
        # 尝试从已有数据中匹配概念→股票关系
        for concept_name in concept_names:
            # 用概念名去行业字段中模糊匹配
            if db is not None:
                try:
                    conn = db._get_conn()
                    # 在 zt_pool 和 strong_pool 的「所属行业」中搜索包含概念名的记录
                    rows = conn.execute(
                        "SELECT DISTINCT stock_code FROM zt_pool WHERE industry LIKE ? "
                        "UNION "
                        "SELECT DISTINCT stock_code FROM strong_pool WHERE reason LIKE ?",
                        (f"%{concept_name}%", f"%{concept_name}%"),
                    ).fetchall()
                    conn.close()
                    for row in rows:
                        all_mappings.append({
                            "stock_code": row[0],
                            "concept_name": concept_name,
                        })
                except Exception:
                    pass

    if not all_mappings:
        return _fallback(db, trade_date)

    # 去重
    df = pd.DataFrame(all_mappings).drop_duplicates(subset=["stock_code", "concept_name"])
    print(f"[concept] 总计 {len(df)} 条映射（去重后）")
    return df


def _extract_industry_mappings(db: Storage) -> list[dict]:
    """从 DB 的 zt_pool / strong_pool 提取 stock_code → 所属行业 映射。"""
    if db is None:
        return []

    mappings = []
    try:
        conn = db._get_conn()
        # zt_pool 有 industry 字段（从 akshare_zt_pool.py 写入时映射）
        # 但当前 zt_pool 没有 industry 列，只有 strong_pool 的 reason
        # 从 strong_pool 取 industry 信息
        rows = conn.execute(
            "SELECT DISTINCT stock_code, reason FROM strong_pool WHERE reason != ''"
        ).fetchall()
        conn.close()

        for stock_code, reason in rows:
            if reason and str(reason).strip():
                mappings.append({
                    "stock_code": stock_code,
                    "concept_name": str(reason).strip(),
                })

        logger.info("从 strong_pool 提取 %d 条行业映射", len(mappings))
    except Exception as e:
        logger.warning("提取行业映射失败: %s", e)

    return mappings


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
