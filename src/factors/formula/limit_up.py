"""Action-oriented limit-up factors built from the daily limit-up event pool."""

from datetime import datetime

import numpy as np
import pandas as pd

from src.data.storage import Storage
from src.factors.base import BaseFactor, dedup_latest


def _numeric(frame: pd.DataFrame, column: str, default: float = 0.0) -> pd.Series:
    if column not in frame:
        return pd.Series(default, index=frame.index, dtype=float)
    values = pd.to_numeric(frame[column], errors="coerce")
    return values if pd.isna(default) else values.fillna(default)


def _first_seal_score(value: object) -> float:
    digits = "".join(character for character in str(value) if character.isdigit())[-6:]
    if len(digits) != 6:
        return 0.5
    hour, minute = int(digits[:2]), int(digits[2:4])
    minute_of_day = hour * 60 + minute
    return float(np.clip((15 * 60 - minute_of_day) / (15 * 60 - (9 * 60 + 25)), 0, 1))


def build_limit_up_features(
    universe: list[str],
    as_of: datetime,
    db: Storage,
) -> pd.DataFrame:
    """Return normalized, interpretable features for stocks in the T0 limit-up pool."""
    index = pd.Index(universe, name="stock_code")
    result = pd.DataFrame(index=index)
    date_str = as_of.strftime("%Y-%m-%d")
    zt = db.query(
        "zt_pool",
        as_of,
        where="trade_date = ?",
        params=(date_str,),
    )
    zt = dedup_latest(zt)
    if zt.empty:
        return result
    zt = zt.set_index("stock_code").reindex(index)
    present = zt["trade_date"].notna()

    board_raw = _numeric(zt, "consecutive_zt", 1)
    open_raw = _numeric(zt, "open_count", 0)
    amount = _numeric(zt, "amount", 0)
    circulation = _numeric(zt, "circulation_mv", 0).replace(0, np.nan)
    turnover = _numeric(zt, "turnover_rate", np.nan)
    turnover_fallback = amount.div(circulation).mul(100)
    turnover = turnover.fillna(turnover_fallback).fillna(0)
    seal_amount = _numeric(zt, "seal_amount", 0)

    result["board_height"] = board_raw.div(5).clip(0, 1)
    result["board_count"] = board_raw
    result["open_count"] = open_raw
    result["turnover_rate_raw"] = turnover
    result["seal_stability"] = 1.0 / (1.0 + open_raw.clip(lower=0))
    result["seal_ratio"] = seal_amount.div(circulation).div(0.02).clip(0, 1).fillna(0)
    first_time = zt.get("first_seal_time", pd.Series("", index=zt.index))
    result["first_seal"] = first_time.map(_first_seal_score)
    result["turnover_quality"] = (1 - (turnover - 15).abs().div(15)).clip(0, 1)
    result["liquidity_rank"] = amount.rank(pct=True).fillna(0)
    result["market_heat"] = min(len(zt.dropna(subset=["trade_date"])) / 120.0, 1.0)

    industry = zt.get("industry", pd.Series("", index=zt.index)).fillna("")
    counts = industry[industry != ""].value_counts()
    # Five limit-ups in one industry is considered full breadth.  Do not scale by
    # the daily maximum: on a fragmented day every isolated limit-up would then
    # incorrectly receive a breadth score of 1.
    result["sector_breadth"] = industry.map(counts).fillna(0).div(5).clip(0, 1)

    fund = db.query(
        "fund_flow",
        as_of,
        where="trade_date = ?",
        params=(date_str,),
    )
    fund = dedup_latest(fund)
    if not fund.empty and "main_net" in fund:
        main_net = (
            pd.to_numeric(
                fund.drop_duplicates("stock_code", keep="last").set_index("stock_code")["main_net"],
                errors="coerce",
            )
            .reindex(index)
            .fillna(0)
        )
        result["capital_confirmation"] = np.tanh(
            main_net.div(amount.replace(0, np.nan)) * 10
        ).fillna(0)
    else:
        result["capital_confirmation"] = 0.0

    result["break_risk"] = (
        open_raw.div(5).clip(0, 1) * 0.50
        + (1 - result["seal_ratio"]) * 0.25
        + (1 - result["first_seal"]) * 0.25
    ).clip(0, 1)
    result["seal_strength"] = (
        result["seal_stability"] * 0.40 + result["seal_ratio"] * 0.35 + result["first_seal"] * 0.25
    )
    result["relay_quality"] = (
        result["board_height"] * 0.20
        + result["seal_strength"] * 0.30
        + result["turnover_quality"] * 0.15
        + result["sector_breadth"] * 0.15
        + result["capital_confirmation"] * 0.10
        + result["liquidity_rank"] * 0.10
        - result["break_risk"] * 0.20
    )
    return result.where(present, np.nan)


class _LimitUpFeatureFactor(BaseFactor):
    feature_name = ""
    factor_type = "stock"
    lookback_days = 1

    def compute(self, universe: list[str], as_of: datetime, db: Storage) -> pd.Series:
        values = build_limit_up_features(universe, as_of, db)
        if self.feature_name not in values:
            return pd.Series(np.nan, index=universe, name=self.name)
        return values[self.feature_name].rename(self.name)


class LimitUpSealStrengthFactor(_LimitUpFeatureFactor):
    name = "zt_seal_strength"
    feature_name = "seal_strength"
    description = "封板质量：封单占比、炸板稳定性、首次封板时间"


class LimitUpRelayQualityFactor(_LimitUpFeatureFactor):
    name = "zt_relay_quality"
    feature_name = "relay_quality"
    description = "连板接力质量：高度、封板、换手、板块宽度、资金确认的结构组合"


class LimitUpSectorBreadthFactor(_LimitUpFeatureFactor):
    name = "zt_sector_breadth"
    feature_name = "sector_breadth"
    description = "涨停行业簇宽度：同一行业涨停扩散强度"


class LimitUpCapitalConfirmationFactor(_LimitUpFeatureFactor):
    name = "zt_capital_confirmation"
    feature_name = "capital_confirmation"
    description = "涨停资金确认：主力净流入相对当日成交额"


class LimitUpBreakRiskFactor(_LimitUpFeatureFactor):
    name = "zt_break_risk"
    feature_name = "break_risk"
    description = "开板风险：炸板次数与封单不足的风险分数"
