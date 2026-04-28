"""因子自适应权重优化模块。

根据近期IC表现动态调整因子权重，替代固定权重方案。

核心逻辑：
1. 从 ic_series 表读取每个因子的近期 IC 值（最近 10 天）
2. 计算每个因子的滚动 IC / ICIR
3. 根据规则动态调整权重：
   - IC > 0.05 且 ICIR > 0.5 → 加权（×1.2）
   - IC < 0.02 或 ICIR < 0.3  → 降权（×0.5）
   - IC 为负                  → 权重归零
4. 归一化 + 约束（最小 0.01，最大 0.40）

使用方式::

    from src.strategy.adaptive_weights import get_adaptive_weights
    from src.strategy.recommend import RecommendEngine

    weights = get_adaptive_weights(
        db_path="data/alpha_miner.db",
        base_weights=RecommendEngine.DEFAULT_WEIGHTS,
    )
"""

from __future__ import annotations

import logging
import sqlite3
from typing import Dict

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# ─── 阈值常量 ──────────────────────────────────────────────
LOOKBACK_DAYS = 10          # 回看最近 N 个交易日的 IC
MIN_DATA_DAYS = 5           # 数据不足此天数 → 不调整，直接返回 base_weights

# 权重调整乘数
BOOST_MULTIPLIER = 1.2      # 优质因子加权
REDUCE_MULTIPLIER = 0.5     # 弱因子降权
ZERO_MULTIPLIER = 0.0       # 负 IC 因子归零

# 约束
WEIGHT_MIN = 0.01           # 权重下限（保留参与资格）
WEIGHT_MAX = 0.40           # 权重上限（单一因子不能主导）

# IC / ICIR 判定阈值
IC_BOOST_THRESHOLD = 0.05   # IC 高于此值 + ICIR 条件 → 加权
IC_WEAK_THRESHOLD = 0.02    # IC 低于此值 → 降权
ICIR_BOOST_THRESHOLD = 0.5  # ICIR 高于此值 + IC 条件 → 加权
ICIR_WEAK_THRESHOLD = 0.3   # ICIR 低于此值 → 降权


def _load_recent_ic(db_path: str, lookback: int = LOOKBACK_DAYS) -> pd.DataFrame:
    """从 ic_series 表读取近期 IC 数据。

    Returns:
        DataFrame with columns: [factor_name, trade_date, ic_value]
    """
    sql = """
        SELECT factor_name, trade_date, ic_value
        FROM (
            SELECT factor_name, trade_date, ic_value,
                   ROW_NUMBER() OVER (
                       PARTITION BY factor_name
                       ORDER BY trade_date DESC
                   ) AS rn
            FROM ic_series
            WHERE forward_days = 1
              AND snapshot_time = (
                  SELECT MAX(s2.snapshot_time)
                  FROM ic_series s2
                  WHERE s2.factor_name = ic_series.factor_name
                    AND s2.trade_date = ic_series.trade_date
              )
        )
        WHERE rn <= ?
        ORDER BY factor_name, trade_date
    """
    try:
        with sqlite3.connect(db_path) as conn:
            df = pd.read_sql_query(sql, conn, params=(lookback,))
    except Exception as e:
        logger.warning("读取 ic_series 失败: %s", e)
        return pd.DataFrame(columns=["factor_name", "trade_date", "ic_value"])

    return df


def _compute_factor_metrics(ic_df: pd.DataFrame) -> Dict[str, dict]:
    """计算每个因子的滚动 IC / ICIR。

    Args:
        ic_df: _load_recent_ic 返回的 DataFrame

    Returns:
        {factor_name: {"ic_mean": float, "ic_std": float, "icir": float, "n_days": int}}
    """
    results: Dict[str, dict] = {}

    for factor_name, group in ic_df.groupby("factor_name"):
        ic_values = group["ic_value"].dropna().values
        n = len(ic_values)

        if n == 0:
            results[factor_name] = {
                "ic_mean": np.nan,
                "ic_std": np.nan,
                "icir": np.nan,
                "n_days": 0,
            }
            continue

        ic_mean = float(np.mean(ic_values))
        ic_std = float(np.std(ic_values, ddof=1)) if n > 1 else np.nan
        icir = (ic_mean / ic_std) if (ic_std and not np.isnan(ic_std) and ic_std > 1e-9) else np.nan

        results[factor_name] = {
            "ic_mean": ic_mean,
            "ic_std": ic_std,
            "icir": icir,
            "n_days": n,
        }

    return results


def _adjust_weights(
    base_weights: Dict[str, float],
    factor_metrics: Dict[str, dict],
    min_data_days: int = MIN_DATA_DAYS,
) -> Dict[str, float]:
    """根据 IC/ICIR 指标动态调整权重。

    调整规则：
    - IC > 0.05 且 ICIR > 0.5 → base_weight × 1.2（加权）
    - IC < 0.02 或 ICIR < 0.3 → base_weight × 0.5（降权）
    - IC < 0（负值）          → base_weight × 0.0（归零）
    - 其余情况               → 保持 base_weight 不变

    然后施加约束并归一化。

    Args:
        base_weights: 基础权重字典 {factor_name: weight}
        factor_metrics: _compute_factor_metrics 的输出
        min_data_days: 最少数据天数

    Returns:
        调整后的权重字典，总和 = 1.0
    """
    adjusted: Dict[str, float] = {}

    for factor, base_w in base_weights.items():
        metrics = factor_metrics.get(factor)

        # 因子在 ic_series 中无数据 → 保持基础权重
        if metrics is None or metrics["n_days"] < min_data_days:
            adjusted[factor] = base_w
            continue

        ic_mean = metrics["ic_mean"]
        icir = metrics["icir"]

        # IC 为负 → 归零
        if not np.isnan(ic_mean) and ic_mean < 0:
            adjusted[factor] = base_w * ZERO_MULTIPLIER
            logger.debug("因子 %s IC=%.4f < 0 → 归零", factor, ic_mean)

        # IC > 0.05 且 ICIR > 0.5 → 加权
        elif (
            not np.isnan(ic_mean)
            and ic_mean > IC_BOOST_THRESHOLD
            and not np.isnan(icir)
            and icir > ICIR_BOOST_THRESHOLD
        ):
            adjusted[factor] = base_w * BOOST_MULTIPLIER
            logger.debug(
                "因子 %s IC=%.4f ICIR=%.4f → 加权 ×%.1f",
                factor, ic_mean, icir, BOOST_MULTIPLIER,
            )

        # IC < 0.02 或 ICIR < 0.3 → 降权
        elif (
            (not np.isnan(ic_mean) and ic_mean < IC_WEAK_THRESHOLD)
            or (not np.isnan(icir) and icir < ICIR_WEAK_THRESHOLD)
        ):
            adjusted[factor] = base_w * REDUCE_MULTIPLIER
            logger.debug(
                "因子 %s IC=%.4f ICIR=%.4f → 降权 ×%.1f",
                factor, ic_mean, icir, REDUCE_MULTIPLIER,
            )

        # 其余情况 → 保持原权重
        else:
            adjusted[factor] = base_w
            logger.debug(
                "因子 %s IC=%.4f ICIR=%.4f → 保持原权重",
                factor, ic_mean, icir,
            )

    # ── 约束 + 归一化 ──
    adjusted = _normalize_weights(adjusted)

    return adjusted


def _normalize_weights(weights: Dict[str, float]) -> Dict[str, float]:
    """对权重施加约束并归一化。

    约束：
    - 每个权重 >= WEIGHT_MIN (0.01)
    - 每个权重 <= WEIGHT_MAX (0.40)
    - 所有权重总和 = 1.0
    """
    result = {}

    # 第一步：施加上下界约束
    for factor, w in weights.items():
        w = max(w, WEIGHT_MIN)
        w = min(w, WEIGHT_MAX)
        result[factor] = w

    # 第二步：归一化到总和 = 1.0
    total = sum(result.values())
    if total <= 0:
        # 极端情况：所有权重都为零，均分
        logger.warning("所有调整后权重之和 <= 0，回退到均分")
        n = len(result)
        return {f: round(1.0 / n, 6) for f in result}

    result = {f: round(w / total, 6) for f, w in result.items()}

    # 第三步：归一化后可能超出约束（理论上不会），再检查一次
    # 如果最大值超了 WEIGHT_MAX，做 clamp + 再归一化（最多迭代 3 次）
    for _ in range(3):
        max_w = max(result.values())
        if max_w <= WEIGHT_MAX + 1e-9:
            break
        for f in result:
            result[f] = min(result[f], WEIGHT_MAX)
        total = sum(result.values())
        result = {f: round(w / total, 6) for f, w in result.items()}

    return result


def get_adaptive_weights(
    db_path: str,
    base_weights: Dict[str, float],
    lookback: int = LOOKBACK_DAYS,
    min_data_days: int = MIN_DATA_DAYS,
) -> Dict[str, float]:
    """根据近期 IC 表现动态调整因子权重。

    从 ic_series 表读取每个因子的近期 IC 值，计算滚动 IC/ICIR，
    然后根据规则动态调整 base_weights 中的权重。

    Args:
        db_path: 数据库路径（e.g. "data/alpha_miner.db"）
        base_weights: 基础权重字典，格式为 {factor_name: weight}，
                      当数据不足时直接返回此权重
        lookback: 回看天数，默认 10
        min_data_days: 最少数据天数阈值，默认 5。
                       如果因子在 ic_series 中的数据不足此天数，
                       该因子保持原基础权重不调整。

    Returns:
        调整后的权重字典，保证：
        - 所有权重 >= 0.01
        - 所有权重 <= 0.40
        - 权重总和 = 1.0
    """
    # 1. 加载近期 IC 数据
    ic_df = _load_recent_ic(db_path, lookback=lookback)

    if ic_df.empty:
        logger.info("ic_series 无数据，返回基础权重")
        return dict(base_weights)

    # 2. 检查整体数据量是否充足
    distinct_dates = ic_df["trade_date"].nunique()
    if distinct_dates < min_data_days:
        logger.info(
            "ic_series 仅有 %d 天数据（需 %d 天），返回基础权重",
            distinct_dates, min_data_days,
        )
        return dict(base_weights)

    # 3. 计算因子指标
    factor_metrics = _compute_factor_metrics(ic_df)

    # 4. 调整权重
    adjusted = _adjust_weights(base_weights, factor_metrics, min_data_days=min_data_days)

    # 5. 日志输出调整结果
    for factor in base_weights:
        old_w = base_weights[factor]
        new_w = adjusted.get(factor, old_w)
        change = (new_w - old_w) / old_w * 100 if old_w > 0 else 0
        metrics = factor_metrics.get(factor, {})
        ic_str = f"{metrics.get('ic_mean', float('nan')):.4f}" if metrics else "N/A"
        icir_str = f"{metrics.get('icir', float('nan')):.4f}" if metrics else "N/A"
        logger.info(
            "权重调整: %-20s  %.3f → %.3f (%+.1f%%)  [IC=%s, ICIR=%s, days=%s]",
            factor, old_w, new_w, change,
            ic_str, icir_str,
            metrics.get("n_days", "N/A") if metrics else "N/A",
        )

    return adjusted
