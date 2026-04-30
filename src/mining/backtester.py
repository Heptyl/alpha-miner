"""因子真实回测器 — 在历史数据上逐日计算因子值并算 Spearman IC。

替代 _sandbox_runner.py 中的假 IC 评估。
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Optional

import numpy as np
import pandas as pd
import warnings
from scipy import stats as scipy_stats

warnings.filterwarnings("ignore", message="An input array is constant")

from src.data.storage import Storage


@dataclass
class BacktestResult:
    """回测结果。"""
    factor_name: str
    ic_mean: float = 0.0
    icir: float = 0.0
    win_rate: float = 0.0
    pnl_ratio: float = 0.0
    sample_per_day: float = 0.0
    total_days: int = 0
    ic_series: list = field(default_factory=list)  # [{date, ic, regime, zt_count}]
    error: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "ic_mean": self.ic_mean,
            "icir": self.icir,
            "win_rate": self.win_rate,
            "pnl_ratio": self.pnl_ratio,
            "sample_per_day": self.sample_per_day,
            "total_days": self.total_days,
            "ic_series": self.ic_series,
            "error": self.error,
        }


class FactorBacktester:
    """在历史数据上逐日回测因子 IC。

    与 ICTracker 的区别：
    - ICTracker 从 factor_values 表读已算好的值
    - FactorBacktester 接收一个 compute 函数，现场算因子值再算 IC
    - FactorBacktester 同时记录每天的 regime 和涨停数，用于后续分段分析
    """

    def __init__(self, db: Storage):
        self.db = db

    def run(
        self,
        compute_fn,  # callable: compute(universe, as_of, db) -> pd.Series
        factor_name: str = "unknown",
        lookback_days: int = 60,
        forward_days: int = 1,
    ) -> BacktestResult:
        """在最近 lookback_days 个交易日上逐日回测。

        流程：
        1. 获取交易日历（从 daily_price 表）
        2. 对每个交易日 T：
           a. 构建 universe（当日有行情且成交额 top 500 的活跃股）
           b. 调用 compute_fn 算因子值
           c. 取 T+forward_days 的收益率
           d. 算 Spearman IC
           e. 记录当天的 regime 和涨停数
        3. 汇总统计
        """
        result = BacktestResult(factor_name=factor_name)

        try:
            trade_dates = self._get_trade_dates(lookback_days + forward_days * 3)
        except Exception as e:
            result.error = f"获取交易日历失败: {e}"
            return result

        if len(trade_dates) < 10:
            result.error = f"交易日数据不足: {len(trade_dates)} 天"
            return result

        # 只在有足够前瞻数据的日期上回测
        test_dates = trade_dates[:len(trade_dates) - forward_days]
        if len(test_dates) > lookback_days:
            test_dates = test_dates[-lookback_days:]

        ic_records = []
        sample_sizes = []

        for i, date_str in enumerate(test_dates):
            as_of = datetime.strptime(date_str, "%Y-%m-%d").replace(hour=15)

            # 构建 universe（按成交额取 top 500 活跃股）
            universe = self._get_universe(as_of, date_str)
            if len(universe) < 20:
                continue

            # 计算因子值
            try:
                factor_values = compute_fn(universe, as_of, self.db)
            except Exception:
                continue

            if factor_values is None or factor_values.empty:
                continue
            factor_values = factor_values.dropna()
            if len(factor_values) < 10:
                continue

            # 计算未来收益
            future_idx = trade_dates.index(date_str) + forward_days
            if future_idx >= len(trade_dates):
                continue
            future_date = trade_dates[future_idx]
            forward_returns = self._get_forward_returns(date_str, future_date)
            if forward_returns.empty:
                continue

            # Spearman IC
            common = factor_values.index.intersection(forward_returns.index)
            if len(common) < 10:
                continue
            fv = factor_values.loc[common].astype(float)
            fr = forward_returns.loc[common].astype(float)
            mask = fv.notna() & fr.notna()
            fv, fr = fv[mask], fr[mask]
            if len(fv) < 10:
                continue

            ic, _ = scipy_stats.spearmanr(fv, fr)
            if np.isnan(ic):
                continue

            regime, zt_count = self._get_day_context(as_of, date_str)
            ic_records.append({
                "date": date_str,
                "ic": round(float(ic), 6),
                "regime": regime,
                "zt_count": zt_count,
                "sample_size": len(fv),
            })
            sample_sizes.append(len(fv))

        if not ic_records:
            result.error = "无有效 IC 样本"
            return result

        ic_values = np.array([r["ic"] for r in ic_records])
        result.ic_mean = float(np.mean(ic_values))
        result.icir = float(np.mean(ic_values) / np.std(ic_values)) if np.std(ic_values) > 0 else 0.0
        result.win_rate = float((ic_values > 0).sum() / len(ic_values))
        pos_mean = float(ic_values[ic_values > 0].mean()) if (ic_values > 0).any() else 0.0
        neg_mean = float(np.abs(ic_values[ic_values < 0].mean())) if (ic_values < 0).any() else 1.0
        result.pnl_ratio = pos_mean / neg_mean if neg_mean > 0 else 0.0
        result.sample_per_day = float(np.mean(sample_sizes))
        result.total_days = len(ic_records)
        result.ic_series = ic_records
        return result

    def _get_trade_dates(self, days: int) -> list[str]:
        """从 daily_price 获取最近的交易日列表。

        使用 bypass_snapshot=True：回测场景下 backfill 数据的
        snapshot_time 是导入时间，不是交易当天。
        """
        end = datetime.now()
        df = self.db.query("daily_price", end, bypass_snapshot=True)
        if df.empty:
            return []
        dates = sorted(df["trade_date"].unique())
        return dates[-days:] if len(dates) > days else dates

    def _get_universe(self, as_of: datetime, date_str: str) -> list[str]:
        """获取当日 universe — 按成交额取 top 500 活跃股。

        使用 bypass_snapshot=True 因为回测场景下数据可能是后来采集的
        （backfill），snapshot_time 是导入时间而非交易当天。
        """
        df = self.db.query("daily_price", as_of,
                           where="trade_date = ?", params=(date_str,),
                           bypass_snapshot=True)
        if df.empty:
            return []
        if "amount" in df.columns:
            df = df.sort_values("amount", ascending=False)
        return df["stock_code"].head(500).tolist()

    def _get_forward_returns(self, current_date: str, future_date: str) -> pd.Series:
        """计算 current_date → future_date 的收益率。

        使用 bypass_snapshot=True 以支持 backfill 数据的回测。
        """
        cur_as_of = datetime.strptime(current_date, "%Y-%m-%d").replace(hour=15)
        fut_as_of = datetime.strptime(future_date, "%Y-%m-%d").replace(hour=15)
        cur_df = self.db.query("daily_price", cur_as_of,
                               where="trade_date = ?", params=(current_date,),
                               bypass_snapshot=True)
        fut_df = self.db.query("daily_price", fut_as_of,
                               where="trade_date = ?", params=(future_date,),
                               bypass_snapshot=True)
        if cur_df.empty or fut_df.empty:
            return pd.Series(dtype=float)
        cur_p = cur_df.drop_duplicates("stock_code").set_index("stock_code")["close"]
        fut_p = fut_df.drop_duplicates("stock_code").set_index("stock_code")["close"]
        common = cur_p.index.intersection(fut_p.index)
        if len(common) == 0:
            return pd.Series(dtype=float)
        return (fut_p.loc[common] - cur_p.loc[common]) / cur_p.loc[common]

    def _get_day_context(self, as_of: datetime, date_str: str) -> tuple[str, int]:
        """获取当天的 regime 和涨停数。

        zt_pool 使用 bypass_snapshot=True 以支持 backfill 数据。
        """
        from src.drift.regime import RegimeDetector
        try:
            regime = RegimeDetector(self.db).detect(as_of).regime
        except Exception:
            regime = "unknown"
        try:
            zt_df = self.db.query("zt_pool", as_of,
                                  where="trade_date = ?", params=(date_str,),
                                  bypass_snapshot=True)
            zt_count = len(zt_df) if not zt_df.empty else 0
        except Exception:
            zt_count = 0
        return regime, zt_count
