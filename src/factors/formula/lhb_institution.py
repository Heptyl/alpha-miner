"""龙虎榜资金活跃度 — 股票级因子。

龙虎榜上榜股票的净买入额，代表大资金（机构+游资）的合力方向。
优先使用机构席位净买入（需席位信息），若席位信息不可用则使用整体净买入额。
"""

from datetime import datetime

import numpy as np
import pandas as pd

from src.data.storage import Storage
from src.factors.base import BaseFactor, dedup_latest


class LhbInstitutionFactor(BaseFactor):
    name = "lhb_institution"
    factor_type = "stock"
    description = "龙虎榜资金活跃度 — 大资金信号"
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
        lhb_df = dedup_latest(lhb_df, key_cols=("stock_code", "trade_date"))
        self.validate_no_future(as_of, lhb_df)

        if lhb_df.empty:
            return pd.Series(np.nan, index=universe, name=self.name)

        result = {code: np.nan for code in universe}

        # 检查是否有有效的席位信息
        has_depart_info = False
        if "buy_depart" in lhb_df.columns:
            non_empty = lhb_df["buy_depart"].apply(lambda x: bool(str(x).strip()))
            has_depart_info = non_empty.any()

        if has_depart_info:
            # 模式1：有席位信息 → 只计算机构净买入
            for stock_code, group in lhb_df.groupby("stock_code"):
                net = 0.0
                for _, row in group.iterrows():
                    buy_dept = str(row.get("buy_depart", ""))
                    sell_dept = str(row.get("sell_depart", ""))
                    for kw in self.INSTITUTION_KEYWORDS:
                        if kw in buy_dept:
                            net += float(row.get("buy_amount", 0) or 0)
                            break
                    for kw in self.INSTITUTION_KEYWORDS:
                        if kw in sell_dept:
                            net -= float(row.get("sell_amount", 0) or 0)
                            break
                result[stock_code] = net
        else:
            # 模式2：无席位信息 → 用整体净买入额
            # 注意：lhb_detail 每只股票可能有多条（不同上榜原因），但 net_amount 是该股总额
            # 所以取每只股票第一条的 net_amount（已去重），不能 sum
            if "net_amount" in lhb_df.columns:
                # 按 stock_code 去重取第一条（避免重复 sum 导致金额膨胀）
                lhb_unique = lhb_df.drop_duplicates(subset=["stock_code"], keep="first")
                net_by_stock = lhb_unique.set_index("stock_code")["net_amount"]
                for code in universe:
                    if code in net_by_stock.index:
                        result[code] = float(net_by_stock[code])

        return pd.Series(result, index=universe, name=self.name)
