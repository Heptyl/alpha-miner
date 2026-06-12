"""选股器基类 — 所有选股策略的统一接口。"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

import pandas as pd

from src.data.storage import Storage


@dataclass
class ScreenResult:
    """单只股票的选股结果。"""
    stock_code: str
    stock_name: str = ""
    strategy_name: str = ""
    score: float = 0.0            # 0-1, 策略内得分
    signal_strength: str = ""     # strong/medium/weak
    reasons: list[str] = field(default_factory=list)
    risks: list[str] = field(default_factory=list)
    extra: dict = field(default_factory=dict)  # 策略特有数据

    def to_dict(self) -> dict:
        return {
            "stock_code": self.stock_code,
            "stock_name": self.stock_name,
            "strategy_name": self.strategy_name,
            "score": round(self.score, 3),
            "signal_strength": self.signal_strength,
            "reasons": self.reasons,
            "risks": self.risks,
            "extra": {k: round(v, 4) if isinstance(v, float) else v
                      for k, v in self.extra.items()},
        }


class BaseScreener(ABC):
    """选股器基类。"""

    name: str = ""
    description: str = ""

    def __init__(self, db: Storage):
        self.db = db

    @abstractmethod
    def screen(self, as_of: datetime, report_date: str) -> list[ScreenResult]:
        """执行选股，返回候选列表。"""
        ...

    def _load_price(self, code: str, as_of: datetime, days: int = 60) -> pd.DataFrame:
        """加载个股历史K线。"""
        return self.db.query_range(
            "daily_price", as_of, days,
            where="stock_code = ?", params=(code,),
        )

    def _load_universe(self, report_date: str) -> list[str]:
        """获取当日全市场可交易股票代码。"""
        import sqlite3
        conn = sqlite3.connect(self.db.db_path)
        rows = conn.execute(
            "SELECT DISTINCT stock_code FROM daily_price WHERE trade_date = ?",
            (report_date,),
        ).fetchall()
        conn.close()
        # 排除科创板和北交所
        return [r[0] for r in rows
                if not r[0].startswith(("688", "689"))
                and not (len(r[0]) == 6 and r[0][0] in ("8", "9"))]

    @staticmethod
    def _is_tradeable(code: str) -> bool:
        return not code.startswith(("688", "689")) and not (
            len(code) == 6 and code[0] in ("8", "9")
        )
