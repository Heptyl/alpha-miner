"""选股器抽象基类。

借鉴 KHunter 的策略架构:
- quick_filter() 快速过滤，避免全量计算
- screen() 完整选股逻辑
- 统一的 ScreenResult 输出格式
"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd


@dataclass
class ScreenResult:
    """选股结果统一格式。"""
    stock_code: str
    stock_name: str = ""
    strategy_name: str = ""
    score: float = 0.0          # 0~1 归一化得分
    signal_strength: str = "C"  # A=强 / B=中 / C=弱
    reasons: list[str] = field(default_factory=list)
    risks: list[str] = field(default_factory=list)
    extra: dict = field(default_factory=dict)  # 策略特有数据

    @property
    def is_tradeable(self) -> bool:
        """排除科创板(688/689)和北交所(8/9开头)。"""
        code = self.stock_code
        if code.startswith("688") or code.startswith("689"):
            return False
        if len(code) == 6 and code[0] in ("8", "9"):
            return False
        return True


class ScreenerBase:
    """选股器抽象基类。

    子类只需实现 screen(report_date) -> list[ScreenResult].
    基类提供 DB 访问、数据获取、通用过滤等辅助方法。
    """

    # 策略名称（子类覆盖）
    name: str = "base"
    # 策略维度（1-9，对应9维体系）
    dimension: int = 0

    def __init__(self, db_path: str = "data/alpha_miner.db"):
        self.db_path = db_path

    def _get_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        return conn

    def screen(self, report_date: str) -> list[ScreenResult]:
        """执行选股，返回候选列表。子类必须实现。"""
        raise NotImplementedError

    # ---------- 辅助方法 ----------

    def get_all_tradeable_codes(self, report_date: str) -> list[str]:
        """获取 report_date 当天有K线的所有可交易股票代码。
        排除科创板和北交所。"""
        conn = self._get_conn()
        try:
            rows = conn.execute(
                "SELECT DISTINCT stock_code FROM daily_price "
                "WHERE trade_date = ? "
                "AND stock_code NOT LIKE '688%' "
                "AND stock_code NOT LIKE '689%'",
                (report_date,),
            ).fetchall()
            codes = [r["stock_code"] for r in rows]
            # 排除北交所
            return [c for c in codes if not (len(c) == 6 and c[0] in ("8", "9"))]
        finally:
            conn.close()

    def get_prices(self, stock_code: str, report_date: str,
                   lookback: int = 120) -> Optional[pd.DataFrame]:
        """获取个股 lookback 天K线数据（到 report_date 为止）。

        返回按日期升序排列的 DataFrame，包含:
        trade_date, open, high, low, close, pre_close, volume, amount
        """
        conn = self._get_conn()
        try:
            df = pd.read_sql_query(
                "SELECT trade_date, open, high, low, close, pre_close, "
                "volume, amount FROM daily_price "
                "WHERE stock_code = ? AND trade_date <= ? "
                "ORDER BY trade_date ASC LIMIT ?",
                conn, params=(stock_code, report_date, lookback),
            )
            if df.empty or len(df) < 10:
                return None
            # 确保数值类型
            for col in ("open", "high", "low", "close", "volume"):
                df[col] = pd.to_numeric(df[col], errors="coerce")
            df = df.dropna(subset=["close", "volume"])
            return df
        finally:
            conn.close()

    def get_prices_batch(self, report_date: str,
                         lookback: int = 60) -> dict[str, pd.DataFrame]:
        """批量获取所有可交易股票的K线（一次性SQL，避免逐只查询）。

        返回 {stock_code: DataFrame} 字典。
        """
        conn = self._get_conn()
        try:
            # 计算起始日期
            start = (datetime.strptime(report_date, "%Y-%m-%d")
                     - pd.Timedelta(days=lookback * 2)).strftime("%Y-%m-%d")
            df = pd.read_sql_query(
                "SELECT stock_code, trade_date, open, high, low, close, "
                "pre_close, volume, amount FROM daily_price "
                "WHERE trade_date >= ? AND trade_date <= ? "
                "AND stock_code NOT LIKE '688%' "
                "AND stock_code NOT LIKE '689%' "
                "ORDER BY stock_code, trade_date",
                conn, params=(start, report_date),
            )
            if df.empty:
                return {}
            for col in ("open", "high", "low", "close", "volume"):
                df[col] = pd.to_numeric(df[col], errors="coerce")

            result = {}
            for code, group in df.groupby("stock_code"):
                if len(code) == 6 and code[0] in ("8", "9"):
                    continue  # 北交所
                g = group.dropna(subset=["close", "volume"]).tail(lookback)
                if len(g) >= 10:
                    result[code] = g.reset_index(drop=True)
            return result
        finally:
            conn.close()

    def get_stock_name(self, stock_code: str, report_date: str) -> str:
        """从多个表获取股票名称。"""
        conn = self._get_conn()
        try:
            # 1. zt_pool / strong_pool (有name字段)
            for table in ("zt_pool", "strong_pool"):
                row = conn.execute(
                    f"SELECT name FROM {table} "
                    f"WHERE stock_code = ? AND trade_date <= ? "
                    f"ORDER BY trade_date DESC LIMIT 1",
                    (stock_code, report_date),
                ).fetchone()
                if row and row["name"]:
                    return row["name"]
            # 2. fund_flow (有stock_name字段)
            row = conn.execute(
                "SELECT stock_name FROM fund_flow "
                "WHERE stock_code = ? AND trade_date <= ? "
                "ORDER BY trade_date DESC LIMIT 1",
                (stock_code, report_date),
            ).fetchone()
            if row and row["stock_name"]:
                return row["stock_name"]
            return stock_code
        finally:
            conn.close()

    def get_stock_names_batch(self, codes: list[str], report_date: str) -> dict[str, str]:
        """批量获取股票名称。"""
        if not codes:
            return {}
        conn = self._get_conn()
        try:
            names = {}
            placeholders = ",".join("?" * len(codes))
            # 从所有有名称字段的表查
            for table, name_col in [
                ("zt_pool", "name"), ("strong_pool", "name"),
                ("fund_flow", "stock_name"),
            ]:
                rows = conn.execute(
                    f"SELECT stock_code, {name_col} as name FROM {table} "
                    f"WHERE stock_code IN ({placeholders}) "
                    f"GROUP BY stock_code HAVING trade_date = MAX(trade_date)",
                    codes,
                ).fetchall()
                for r in rows:
                    if r["name"] and r["stock_code"] not in names:
                        names[r["stock_code"]] = r["name"]
            # 未找到的返回代码
            for c in codes:
                if c not in names:
                    names[c] = c
            return names
        finally:
            conn.close()

    @staticmethod
    def calc_ma(series: pd.Series, period: int) -> pd.Series:
        """计算移动平均线。"""
        return series.rolling(window=period, min_periods=period).mean()

    @staticmethod
    def calc_volume_ratio(volume: pd.Series, period: int = 5) -> pd.Series:
        """计算量比（当日成交量 / N日均量）。"""
        ma = volume.rolling(window=period, min_periods=period).mean()
        return volume / ma.replace(0, np.nan)

    @staticmethod
    def calc_rsi(close: pd.Series, period: int = 14) -> pd.Series:
        """计算RSI。"""
        delta = close.diff()
        gain = delta.clip(lower=0)
        loss = (-delta).clip(lower=0)
        avg_gain = gain.rolling(window=period, min_periods=period).mean()
        avg_loss = loss.rolling(window=period, min_periods=period).mean()
        rs = avg_gain / avg_loss.replace(0, np.nan)
        return 100 - (100 / (1 + rs))

    @staticmethod
    def calc_macd(close: pd.Series, fast: int = 12, slow: int = 26,
                  signal: int = 9) -> tuple[pd.Series, pd.Series, pd.Series]:
        """计算MACD，返回 (dif, dea, macd_hist)。"""
        ema_fast = close.ewm(span=fast, adjust=False).mean()
        ema_slow = close.ewm(span=slow, adjust=False).mean()
        dif = ema_fast - ema_slow
        dea = dif.ewm(span=signal, adjust=False).mean()
        hist = (dif - dea) * 2
        return dif, dea, hist

    @staticmethod
    def is_st_stock(name: str) -> bool:
        """判断是否ST/退市股。"""
        if not name:
            return False
        upper = name.upper()
        return "ST" in upper or "退" in upper

    def make_result(self, stock_code: str, report_date: str,
                    score: float, reasons: list[str],
                    risks: list[str] = None,
                    extra: dict = None) -> ScreenResult:
        """便捷构造 ScreenResult。"""
        try:
            name = self.get_stock_name(stock_code, report_date)
        except Exception:
            name = stock_code
        sig = "A" if score >= 0.7 else ("B" if score >= 0.4 else "C")
        return ScreenResult(
            stock_code=stock_code,
            stock_name=name,
            strategy_name=self.name,
            score=min(max(score, 0.0), 1.0),
            signal_strength=sig,
            reasons=reasons,
            risks=risks or [],
            extra=extra or {},
        )
