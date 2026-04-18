"""主力净流入强度 — 股票级因子。

主力净流入 / 成交额，衡量大资金参与度。
"""

from datetime import datetime

import pandas as pd

from src.data.storage import Storage
from src.factors.base import BaseFactor, dedup_latest


class MainFlowIntensityFactor(BaseFactor):
    name = "main_flow_intensity"
    factor_type = "stock"
    description = "主力净流入/成交额 — 大资金参与强度"
    lookback_days = 1

    def compute(self, universe: list[str], as_of: datetime, db: Storage) -> pd.Series:
        date_str = as_of.strftime("%Y-%m-%d")

        # 资金流向
        flow_df = db.query(
            "fund_flow",
            as_of,
            where="trade_date = ?",
            params=(date_str,),
        )
        flow_df = dedup_latest(flow_df)
        self.validate_no_future(as_of, flow_df)

        # 成交额
        price_df = db.query(
            "daily_price",
            as_of,
            where="trade_date = ?",
            params=(date_str,),
        )
        price_df = dedup_latest(price_df)
        self.validate_no_future(as_of, price_df, date_col="trade_date")

        # 构建主力净流入映射
        main_net_map = {}
        if not flow_df.empty and "main_net" in flow_df.columns:
            for _, row in flow_df.iterrows():
                main_net_map[row["stock_code"]] = float(row["main_net"])

        # 构建成交额映射
        amount_map = {}
        if not price_df.empty and "amount" in price_df.columns:
            for _, row in price_df.iterrows():
                amount_map[row["stock_code"]] = float(row["amount"])

        # 计算强度
        result = {}
        for code in universe:
            net = main_net_map.get(code, 0.0)
            amt = amount_map.get(code, 1.0)  # 避免 /0
            result[code] = net / amt if amt > 0 else 0.0

        return pd.Series(result, index=universe, name=self.name)
