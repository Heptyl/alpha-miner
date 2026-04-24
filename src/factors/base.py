"""因子基类 — 所有因子继承 BaseFactor。

核心设计：
1. BaseFactor — 公式因子基类
2. ConditionalFactor — 多条件组合因子
3. CrossFactor — 两个已有因子的交叉
4. FutureDataError — 使用未来数据的异常
"""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Optional

import numpy as np
import pandas as pd

from src.data.storage import Storage


def dedup_latest(df: pd.DataFrame, key_cols: tuple[str, ...] = ("stock_code", "trade_date"), time_col: str = "snapshot_time") -> pd.DataFrame:
    """对 query 结果去重：按 key_cols 分组，保留最新 snapshot_time 的记录。

    防止多次采集产生的重复数据影响因子计算。
    如果 DataFrame 为空或缺少 snapshot_time 列，原样返回。
    """
    if df.empty:
        return df
    if time_col not in df.columns:
        # 没有 snapshot_time 列，用 drop_duplicates 保底
        return df.drop_duplicates(subset=list(key_cols), keep="last")
    return df.sort_values(time_col).groupby(list(key_cols)).last().reset_index()


class FutureDataError(Exception):
    """因子计算中检测到使用未来数据。"""
    pass


class BaseFactor(ABC):
    """因子抽象基类。

    所有因子必须实现 compute 方法。
    compute 开头应调用 validate_no_future 检查数据时间边界。
    """

    name: str = ""
    factor_type: str = ""  # "market" | "stock"
    description: str = ""
    lookback_days: int = 1

    @abstractmethod
    def compute(
        self,
        universe: list[str],
        as_of: datetime,
        db: Storage,
    ) -> pd.Series:
        """计算因子值。

        Args:
            universe: 股票代码列表
            as_of: 计算时间点（只能用此时间之前的数据）
            db: 数据存储层

        Returns:
            pd.Series, index=stock_code, values=float
            对于市场级因子，可以只返回一条（index=["market"]）
        """
        ...

    def validate_no_future(
        self,
        as_of: datetime,
        data: pd.DataFrame,
        date_col: str = "trade_date",
    ) -> None:
        """验证数据中没有未来数据。

        检查 DataFrame 中所有 date_col 值都 <= as_of 的日期部分。
        如果发现未来数据，抛出 FutureDataError。
        """
        if data.empty:
            return

        as_of_date = as_of.strftime("%Y-%m-%d")
        # 截取日期部分（兼容 "YYYY-MM-DD HH:MM" 格式）
        date_values = data[date_col].astype(str).str[:10]
        max_date = date_values.max()
        if max_date > as_of_date:
            raise FutureDataError(
                f"Factor {self.name}: 发现未来数据！"
                f"as_of={as_of_date}, 但数据中有 {date_col}={max_date}"
            )


class Condition:
    """单个条件，可复用于多个条件因子。

    条件因子是逻辑组合 (A and B and C) -> {0, 1} 或满足条件数/总条件数。
    A股短线的 alpha 往往在条件交叉处。
    """

    def __init__(
        self,
        name: str,
        table: str,
        column: str,
        operator: str,
        value: Any = None,
    ):
        """
        Args:
            name: 条件名称
            table: 数据表名
            column: 列名
            operator: 比较运算符 ">", "<", ">=", "<=", "==", "in", "between"
            value: 比较值；"between" 时传 tuple (low, high)
        """
        self.name = name
        self.table = table
        self.column = column
        self.operator = operator
        self.value = value

    def evaluate(
        self,
        universe: list[str],
        as_of: datetime,
        db: Storage,
    ) -> pd.Series:
        """评估条件，返回 stock_code -> bool。"""
        data = db.query_range(
            self.table,
            as_of,
            lookback_days=1,
            where="stock_code IN ({})".format(
                ",".join(["?"] * len(universe))
            ),
            params=tuple(universe),
        )

        if data.empty:
            return pd.Series(dtype=float, name=self.name)

        # 去重：多次采集可能产生重复记录
        data = dedup_latest(data)

        # 取每个股票最新一天的数据
        data = data.sort_values("trade_date").groupby("stock_code").last()

        if self.operator == ">":
            result = data[self.column] > self.value
        elif self.operator == "<":
            result = data[self.column] < self.value
        elif self.operator == ">=":
            result = data[self.column] >= self.value
        elif self.operator == "<=":
            result = data[self.column] <= self.value
        elif self.operator == "==":
            result = data[self.column] == self.value
        elif self.operator == "in":
            result = data[self.column].isin(self.value)
        elif self.operator == "between":
            low, high = self.value
            result = (data[self.column] >= low) & (data[self.column] <= high)
        else:
            raise ValueError(f"未知运算符: {self.operator}")

        return result.astype(float)


class ConditionalFactor(BaseFactor):
    """条件因子：将多个离散条件组合成一个信号。"""

    conditions: list[Condition] = []
    logic: str = "all"  # "all" | "any" | "count"

    def compute(self, universe: list[str], as_of: datetime, db: Storage) -> pd.Series:
        if not self.conditions:
            return pd.Series(dtype=float, name=self.name)

        results = pd.DataFrame(index=universe)
        for cond in self.conditions:
            cond_result = cond.evaluate(universe, as_of, db)
            results[cond.name] = cond_result

        if self.logic == "all":
            return results.all(axis=1).astype(float)
        elif self.logic == "any":
            return results.any(axis=1).astype(float)
        elif self.logic == "count":
            return results.sum(axis=1) / len(self.conditions)
        else:
            raise ValueError(f"未知逻辑: {self.logic}")


class CrossFactor(BaseFactor):
    """交叉因子：组合两个已有因子。

    因子之间的交互效应可能比单因子更强。
    """

    factor_a_name: str = ""
    factor_b_name: str = ""
    operation: str = "multiply"  # "multiply" | "divide" | "max" | "conditional"

    def compute(self, universe: list[str], as_of: datetime, db: Storage) -> pd.Series:
        # 延迟导入避免循环依赖
        from src.factors.registry import FactorRegistry

        registry = FactorRegistry()
        a = registry.get_factor(self.factor_a_name).compute(universe, as_of, db)
        b = registry.get_factor(self.factor_b_name).compute(universe, as_of, db)

        if self.operation == "multiply":
            return a * b
        elif self.operation == "divide":
            return a / b.replace(0, np.nan)
        elif self.operation == "max":
            return pd.concat([a, b], axis=1).max(axis=1)
        elif self.operation == "conditional":
            # a > 中位数时才用 b，否则为 0
            return b.where(a > a.median(), 0)
        else:
            raise ValueError(f"未知操作: {self.operation}")
