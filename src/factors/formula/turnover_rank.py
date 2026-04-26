"""换手率/成交量排名 — 股票级因子。

优先用换手率百分位排名（全市场），衡量资金关注度。
若换手率数据不可用（如腾讯源），则用成交额排名替代。
"""

from datetime import datetime

import pandas as pd

from src.data.storage import Storage
from src.factors.base import BaseFactor, dedup_latest


class TurnoverRankFactor(BaseFactor):
    name = "turnover_rank"
    factor_type = "stock"
    description = "换手率/成交额百分位排名 — 资金关注度"
    lookback_days = 1

    def compute(self, universe: list[str], as_of: datetime, db: Storage) -> pd.Series:
        date_str = as_of.strftime("%Y-%m-%d")

        price_df = db.query(
            "daily_price",
            as_of,
            where="trade_date = ?",
            params=(date_str,),
        )
        price_df = dedup_latest(price_df)
        self.validate_no_future(as_of, price_df, date_col="trade_date")

        if price_df.empty:
            return pd.Series(0.0, index=universe, name=self.name)

        # 优先用换手率，其次用成交额，最后用成交量
        metric = None
        for col in ["turnover_rate", "amount", "volume"]:
            if col in price_df.columns:
                vals = pd.to_numeric(price_df.set_index("stock_code")[col], errors="coerce").fillna(0)
                # 检查是否有区分度（不全是同一个值）
                if vals.nunique() > 1:
                    metric = vals
                    break

        if metric is None:
            return pd.Series(0.5, index=universe, name=self.name)

        ranks = metric.rank(pct=True)

        # 映射到 universe
        return pd.Series(
            [float(ranks.get(code, 0.0)) for code in universe],
            index=universe,
            name=self.name,
        )
