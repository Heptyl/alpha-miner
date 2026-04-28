"""技术分析模块 — 支撑位/压力位/买入区间计算。

基于日K线数据计算：
- 均线系统（MA5/MA10/MA20）
- 支撑位（近期低点 + MA支撑）
- 压力位（近期高点 + MA压力）
- ATR 波动率
- 量比
- 买入区间
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import numpy as np
import pandas as pd


@dataclass
class TechnicalAnalysis:
    """单只股票的技术分析结果。"""

    stock_code: str
    current_price: float       # 最新收盘价
    ma5: Optional[float]       # 5日均线
    ma10: Optional[float]      # 10日均线
    ma20: Optional[float]      # 20日均线
    support_price: float       # 支撑位
    resistance_price: float    # 压力位
    atr: float                 # ATR 波动率
    volume_ratio: float        # 量比（当日成交量 / 5日均量）
    momentum_score: float      # 动量得分 [0, 1]
    buy_zone_low: float        # 买入区间下限
    buy_zone_high: float       # 买入区间上限
    trend: str                 # 趋势: 上涨/震荡/下跌

    def to_dict(self) -> dict:
        return {
            "stock_code": self.stock_code,
            "current_price": round(self.current_price, 2),
            "ma5": round(self.ma5, 2) if self.ma5 else None,
            "ma10": round(self.ma10, 2) if self.ma10 else None,
            "ma20": round(self.ma20, 2) if self.ma20 else None,
            "support_price": round(self.support_price, 2),
            "resistance_price": round(self.resistance_price, 2),
            "atr": round(self.atr, 2),
            "volume_ratio": round(self.volume_ratio, 2),
            "momentum_score": round(self.momentum_score, 3),
            "buy_zone_low": round(self.buy_zone_low, 2),
            "buy_zone_high": round(self.buy_zone_high, 2),
            "trend": self.trend,
        }


def compute_technical(
    df: pd.DataFrame,
    buy_zone_pct: float = 0.02,
    ma_periods: list[int] | None = None,
    atr_period: int = 14,
    volume_days: int = 5,
) -> Optional[TechnicalAnalysis]:
    """计算单只股票的技术分析。

    Args:
        df: 日K线数据，需包含 close/high/low/volume 列，按日期升序。
        buy_zone_pct: 买入区间宽度百分比。
        ma_periods: 均线周期列表。
        atr_period: ATR 周期。
        volume_days: 量比计算天数。

    Returns:
        TechnicalAnalysis 或 None（数据不足时）。
    """
    if ma_periods is None:
        ma_periods = [5, 10, 20]

    if len(df) < 5:
        return None

    # 确保按日期升序
    df = df.sort_values("trade_date").reset_index(drop=True)

    close = df["close"]
    high = df["high"]
    low = df["low"]
    volume = df["volume"]

    current_price = float(close.iloc[-1])

    # 均线
    def safe_ma(series: pd.Series, period: int) -> Optional[float]:
        if len(series) >= period:
            return float(series.iloc[-period:].mean())
        return None

    ma5 = safe_ma(close, 5)
    ma10 = safe_ma(close, 10)
    ma20 = safe_ma(close, 20)

    # ATR (Average True Range)
    atr = _compute_atr(high, low, close, atr_period)

    # 量比
    if len(volume) >= volume_days + 1:
        avg_vol = volume.iloc[-(volume_days + 1):-1].mean()
        today_vol = volume.iloc[-1]
        volume_ratio = float(today_vol / avg_vol) if avg_vol > 0 else 1.0
    else:
        volume_ratio = 1.0

    # 动量得分（综合近期涨幅和趋势）
    momentum_score = _compute_momentum(close, ma5, ma10, ma20)

    # 支撑位 = max(近期最低价, MA支撑)
    support_price = _compute_support(close, low, ma5, ma10, ma20)

    # 压力位 = min(近期最高价, MA压力)
    resistance_price = _compute_resistance(close, high, ma5, ma10, ma20)

    # 买入区间：围绕支撑位构建
    # 下限 = 支撑位附近（支撑位 * 0.98）
    # 上限 = min(当前价 * 1.01, 支撑位 * 1.03)
    buy_zone_low = max(current_price * (1 - buy_zone_pct), support_price * 0.98)
    buy_zone_high = min(current_price * (1 + buy_zone_pct * 0.5), support_price * 1.05)

    # 确保下限 <= 上限
    if buy_zone_low > buy_zone_high:
        mid = (buy_zone_low + buy_zone_high) / 2
        half_spread = current_price * buy_zone_pct * 0.3
        buy_zone_low = mid - half_spread
        buy_zone_high = mid + half_spread

    # 趋势判断
    trend = _determine_trend(current_price, ma5, ma10, ma20)

    return TechnicalAnalysis(
        stock_code=str(df["stock_code"].iloc[-1]) if "stock_code" in df.columns else "",
        current_price=current_price,
        ma5=ma5,
        ma10=ma10,
        ma20=ma20,
        support_price=round(support_price, 2),
        resistance_price=round(resistance_price, 2),
        atr=round(atr, 4),
        volume_ratio=round(volume_ratio, 2),
        momentum_score=round(momentum_score, 3),
        buy_zone_low=round(buy_zone_low, 2),
        buy_zone_high=round(buy_zone_high, 2),
        trend=trend,
    )


def _compute_atr(
    high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14,
) -> float:
    """计算 ATR。"""
    if len(close) < 2:
        return float(high.iloc[-1] - low.iloc[-1]) if len(close) >= 1 else 0.0

    prev_close = close.shift(1)
    tr1 = high - low
    tr2 = (high - prev_close).abs()
    tr3 = (low - prev_close).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    tr = tr.dropna()

    if len(tr) == 0:
        return 0.0

    if len(tr) >= period:
        return float(tr.iloc[-period:].mean())
    return float(tr.mean())


def _compute_momentum(
    close: pd.Series,
    ma5: Optional[float],
    ma10: Optional[float],
    ma20: Optional[float],
) -> float:
    """计算动量得分 [0, 1]。

    综合考虑：
    - 近5日涨幅
    - 均线多头排列
    - 价格相对位置
    """
    score = 0.0
    current = float(close.iloc[-1])

    # 近5日涨幅贡献（最多 0.3）
    if len(close) >= 5:
        ret_5d = (current / float(close.iloc[-5]) - 1) * 100
        # 涨幅 0~10% 映射到 0~0.3
        score += min(max(ret_5d / 10.0, 0) * 0.3, 0.3)
    elif len(close) >= 2:
        ret = (current / float(close.iloc[0]) - 1) * 100
        score += min(max(ret / 10.0, 0) * 0.3, 0.3)

    # 均线多头排列（最多 0.4）
    if ma5 and ma10 and ma20:
        if current > ma5 > ma10 > ma20:
            score += 0.4  # 完美多头
        elif current > ma5 and ma5 > ma10:
            score += 0.3  # 短期多头
        elif current > ma5:
            score += 0.15  # 站上5日线
    elif ma5 and current > ma5:
        score += 0.15

    # 价格相对近期高位的位置（最多 0.3）
    if len(close) >= 10:
        recent_high = close.iloc[-10:].max()
        position = (current - float(close.iloc[-10:].min())) / (float(recent_high) - float(close.iloc[-10:].min())) if recent_high != close.iloc[-10:].min() else 0.5
        score += position * 0.3

    return min(score, 1.0)


def _compute_support(
    close: pd.Series,
    low: pd.Series,
    ma5: Optional[float],
    ma10: Optional[float],
    ma20: Optional[float],
) -> float:
    """计算支撑位。"""
    current = float(close.iloc[-1])

    # 近期最低价
    lookback = min(20, len(low))
    recent_low = float(low.iloc[-lookback:].min())

    # 取所有在当前价格下方的支撑
    supports = [recent_low]
    if ma5 and ma5 < current:
        supports.append(ma5)
    if ma10 and ma10 < current:
        supports.append(ma10)
    if ma20 and ma20 < current:
        supports.append(ma20)

    # 取离当前价格最近的支撑
    return max(supports)


def _compute_resistance(
    close: pd.Series,
    high: pd.Series,
    ma5: Optional[float],
    ma10: Optional[float],
    ma20: Optional[float],
) -> float:
    """计算压力位。"""
    current = float(close.iloc[-1])

    # 近期最高价
    lookback = min(20, len(high))
    recent_high = float(high.iloc[-lookback:].max())

    # 取所有在当前价格上方的压力
    resistances = [recent_high]
    if ma5 and ma5 > current:
        resistances.append(ma5)
    if ma10 and ma10 > current:
        resistances.append(ma10)
    if ma20 and ma20 > current:
        resistances.append(ma20)

    # 取离当前价格最近的压力
    return min(resistances)


def _determine_trend(
    current: float,
    ma5: Optional[float],
    ma10: Optional[float],
    ma20: Optional[float],
) -> str:
    """判断趋势方向。"""
    if ma5 and ma10 and ma20:
        if current > ma5 > ma10 > ma20:
            return "上涨"
        elif current < ma5 < ma10 < ma20:
            return "下跌"
    elif ma5 and ma10:
        if current > ma5 > ma10:
            return "上涨"
        elif current < ma5 < ma10:
            return "下跌"
    return "震荡"
