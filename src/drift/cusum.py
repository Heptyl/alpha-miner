"""CUSUM 变点检测 — 检测因子 IC 的结构性变化。

对 IC 序列做 CUSUM 统计量计算，检测均值漂移。
"""

from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass
class CUSUMResult:
    """CUSUM 检测结果。"""
    changepoints: list[int]    # 变点位置索引
    cusum_stats: np.ndarray    # CUSUM 统计量序列
    threshold: float           # 检测阈值
    series_length: int


def detect_changepoints(
    series: pd.Series,
    threshold: float = 1.5,
    min_segment: int = 10,
) -> CUSUMResult:
    """CUSUM 变点检测。

    使用递归 CUSUM 方法检测时间序列均值漂移。
    当累积偏差超过阈值时标记变点。

    Args:
        series: 时序数据（如 IC 序列）
        threshold: 标准化阈值，越大越不敏感
        min_segment: 最小段长度

    Returns:
        CUSUMResult 包含变点位置和统计量
    """
    values = series.dropna().values
    n = len(values)
    if n < min_segment * 2:
        return CUSUMResult(
            changepoints=[],
            cusum_stats=np.zeros(n),
            threshold=threshold,
            series_length=n,
        )

    changepoints = _recursive_cusum(values, threshold, min_segment, 0)
    # 计算全序列 CUSUM 统计量
    cusum_stats = _compute_cusum_stats(values)

    return CUSUMResult(
        changepoints=sorted(changepoints),
        cusum_stats=cusum_stats,
        threshold=threshold,
        series_length=n,
    )


def _compute_cusum_stats(values: np.ndarray) -> np.ndarray:
    """计算 CUSUM 统计量序列。"""
    n = len(values)
    mean = np.mean(values)
    std = np.std(values)
    if std == 0:
        return np.zeros(n)

    normalized = (values - mean) / std
    cusum = np.zeros(n)
    cusum[0] = normalized[0]
    for i in range(1, n):
        cusum[i] = cusum[i - 1] + normalized[i]
    return cusum


def _recursive_cusum(
    values: np.ndarray,
    threshold: float,
    min_segment: int,
    offset: int,
) -> list[int]:
    """递归 CUSUM 变点检测。"""
    n = len(values)
    if n < min_segment * 2:
        return []

    # 在当前段内检测变点
    mean = np.mean(values)
    std = np.std(values)
    if std == 0:
        return []

    # 标准化累积和
    cusum = np.zeros(n)
    cusum[0] = (values[0] - mean) / std
    for i in range(1, n):
        cusum[i] = cusum[i - 1] + (values[i] - mean) / std

    # 找最大偏差点
    max_pos = int(np.argmax(np.abs(cusum)))
    max_val = abs(cusum[max_pos])

    if max_val < threshold:
        return []

    # 检查分割后段长度够不够
    if max_pos < min_segment or (n - max_pos) < min_segment:
        return []

    # 递归检测左右子段
    left_cps = _recursive_cusum(values[:max_pos], threshold, min_segment, offset)
    right_cps = _recursive_cusum(values[max_pos:], threshold, min_segment, offset + max_pos)

    return left_cps + [offset + max_pos] + right_cps
