"""连板梯队 — 股票级因子。

每个涨停股的连板数，代表接力情绪。
"""

from datetime import datetime

import pandas as pd

from src.data.storage import Storage
from src.factors.base import BaseFactor, dedup_latest


class ConsecutiveBoardFactor(BaseFactor):
    name = "consecutive_board"
    factor_type = "stock"
    description = "连板数 — 接力情绪指标"
    lookback_days = 1

    def compute(self, universe: list[str], as_of: datetime, db: Storage) -> pd.Series:
        zt_df = db.query(
            "zt_pool",
            as_of,
            where="trade_date = ?",
            params=(as_of.strftime("%Y-%m-%d"),),
        )
        zt_df = dedup_latest(zt_df)
        self.validate_no_future(as_of, zt_df)

        if zt_df.empty:
            return pd.Series(0.0, index=universe, name=self.name)

        # 构建 stock_code -> consecutive_zt 映射
        board_map = {}
        if "consecutive_zt" in zt_df.columns:
            for _, row in zt_df.iterrows():
                board_map[row["stock_code"]] = float(row["consecutive_zt"])

        return pd.Series(
            [float(board_map.get(code, 0)) for code in universe],
            index=universe,
            name=self.name,
        )
