"""龙虎榜机构净买入 — 股票级因子。

龙虎榜中机构席位的净买入额，代表专业资金的判断。
"""

from datetime import datetime

import pandas as pd

from src.data.storage import Storage
from src.factors.base import BaseFactor, dedup_latest


class LhbInstitutionFactor(BaseFactor):
    name = "lhb_institution"
    factor_type = "stock"
    description = "龙虎榜机构净买入额 — 专业资金信号"
    lookback_days = 1

    # 机构席位关键词
    INSTITUTION_KEYWORDS = ["机构专用", "外资机构", "机构"]

    def compute(self, universe: list[str], as_of: datetime, db: Storage) -> pd.Series:
        date_str = as_of.strftime("%Y-%m-%d")

        lhb_df = db.query(
            "lhb_detail",
            as_of,
            where="trade_date = ?",
            params=(date_str,),
        )
        lhb_df = dedup_latest(lhb_df, key_cols=("stock_code", "trade_date", "buy_depart", "sell_depart"))
        self.validate_no_future(as_of, lhb_df)

        if lhb_df.empty:
            return pd.Series(0.0, index=universe, name=self.name)

        # 计算每个股票的机构净买入
        result = {code: 0.0 for code in universe}

        # 按股票聚合
        for stock_code, group in lhb_df.groupby("stock_code"):
            net = 0.0
            if "buy_depart" in group.columns:
                for _, row in group.iterrows():
                    buy_dept = str(row.get("buy_depart", ""))
                    sell_dept = str(row.get("sell_depart", ""))
                    # 买入方有机构 → 加
                    for kw in self.INSTITUTION_KEYWORDS:
                        if kw in buy_dept:
                            net += float(row.get("buy_amount", 0))
                            break
                    # 卖出方有机构 → 减
                    for kw in self.INSTITUTION_KEYWORDS:
                        if kw in sell_dept:
                            net -= float(row.get("sell_amount", 0))
                            break
            result[stock_code] = net

        return pd.Series(result, index=universe, name=self.name)
