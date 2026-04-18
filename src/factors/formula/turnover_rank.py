"""换手率排名 — 股票级因子。

换手率百分位排名（全市场），衡量资金关注度。
"""

from datetime import datetime

import pandas as pd

from src.data.storage import Storage
from src.factors.base import BaseFactor, dedup_latest


class TurnoverRankFactor(BaseFactor):
    name = "turnover_rank"
    factor_type = "stock"
    description = "换手率百分位排名 — 资金关注度"
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

        if price_df.empty or "turnover_rate" not in price_df.columns:
            return pd.Series(0.0, index=universe, name=self.name)

        # 全市场换手率百分位排名
        all_turnover = price_df.set_index("stock_code")["turnover_rate"]
        all_turnover = pd.to_numeric(all_turnover, errors="coerce").fillna(0)
        ranks = all_turnover.rank(pct=True)

        # 映射到 universe
        return pd.Series(
            [float(ranks.get(code, 0.0)) for code in universe],
            index=universe,
            name=self.name,
        )
