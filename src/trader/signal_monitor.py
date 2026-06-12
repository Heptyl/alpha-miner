"""实时买卖信号监控系统 — 基于技术指标分析持仓与买入候选

核心功能:
  1. analyze_position()  — 分析持仓股卖出信号（止损/减仓/持有）
  2. analyze_buy_candidate() — 分析ML推荐股买入信号
  3. get_all_signals() — 汇总所有持仓和ML推荐的完整信号报告

技术指标 (纯Python, 不依赖ta-lib):
  MACD(12,26,9), KDJ(9,3,3), 布林带(20,2), MA5/10/20/60, RSI(14), 成交量

CLI:
  uv run python -m src.trader.signal_monitor
"""

from __future__ import annotations

import json
import logging
import sqlite3
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 常量 & 项目路径
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DB_PATH = PROJECT_ROOT / "data" / "alpha_miner.db"
PRED_PATH = PROJECT_ROOT / "output" / "ml" / "latest_prediction.json"

# 用户持仓 (与 plan_generator 保持一致)
# 用户持仓 — 统一从 portfolio.json 读取（同源）
from src.config.portfolio import get_legacy_portfolio_dict as _get_portfolio
PORTFOLIO = _get_portfolio()


# ---------------------------------------------------------------------------
# 技术指标计算 (纯 Python / Pandas)
# ---------------------------------------------------------------------------

def _calc_ema(series: pd.Series, span: int) -> pd.Series:
    """计算 EMA (指数移动平均)"""
    return series.ewm(span=span, adjust=False).mean()


def _calc_macd(df: pd.DataFrame, fast: int = 12, slow: int = 26, signal: int = 9
               ) -> tuple[pd.Series, pd.Series, pd.Series]:
    """计算 MACD 指标

    Returns:
        (dif, dea, macd_hist)  — DIF线, DEA线, MACD柱(2*(DIF-DEA))
    """
    close = df["close"]
    ema_fast = _calc_ema(close, fast)
    ema_slow = _calc_ema(close, slow)
    dif = ema_fast - ema_slow
    dea = _calc_ema(dif, signal)
    macd_hist = 2 * (dif - dea)
    return dif, dea, macd_hist


def _calc_kdj(df: pd.DataFrame, n: int = 9, m1: int = 3, m2: int = 3
              ) -> tuple[pd.Series, pd.Series, pd.Series]:
    """计算 KDJ 指标

    Returns:
        (K, D, J)
    """
    low_n = df["low"].rolling(window=n, min_periods=1).min()
    high_n = df["high"].rolling(window=n, min_periods=1).max()

    rsv = (df["close"] - low_n) / (high_n - low_n) * 100
    rsv = rsv.fillna(50)  # 极端情况: 最高=最低

    k = rsv.ewm(com=m1 - 1, adjust=False).mean()
    d = k.ewm(com=m2 - 1, adjust=False).mean()
    j = 3 * k - 2 * d
    return k, d, j


def _calc_bollinger(df: pd.DataFrame, n: int = 20, k: float = 2.0
                    ) -> tuple[pd.Series, pd.Series, pd.Series]:
    """计算布林带

    Returns:
        (upper, middle, lower)  — 上轨, 中轨(MA20), 下轨
    """
    middle = df["close"].rolling(window=n, min_periods=1).mean()
    std = df["close"].rolling(window=n, min_periods=1).std()
    upper = middle + k * std
    lower = middle - k * std
    return upper, middle, lower


def _calc_ma(df: pd.DataFrame, periods: list[int] | None = None) -> dict[int, pd.Series]:
    """计算 MA 均线"""
    if periods is None:
        periods = [5, 10, 20, 60]
    result: dict[int, pd.Series] = {}
    for p in periods:
        result[p] = df["close"].rolling(window=p, min_periods=1).mean()
    return result


def _calc_rsi(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """计算 RSI (相对强弱指标)"""
    close = df["close"]
    delta = close.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = (-delta).where(delta < 0, 0.0)

    avg_gain = gain.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()

    rs = avg_gain / avg_loss.replace(0, 1e-10)
    rsi = 100 - 100 / (1 + rs)
    return rsi


# ---------------------------------------------------------------------------
# 数据获取
# ---------------------------------------------------------------------------

def _get_daily_data(code: str, days: int = 120) -> pd.DataFrame | None:
    """从 daily_price 获取最近 days 天日K数据, 按日期升序排列"""
    if not DB_PATH.exists():
        logger.warning("数据库不存在: %s", DB_PATH)
        return None
    conn = sqlite3.connect(str(DB_PATH))
    try:
        sql = """
            SELECT trade_date, open, high, low, close, pre_close, volume, amount
            FROM daily_price
            WHERE stock_code = ?
            ORDER BY trade_date DESC
            LIMIT ?
        """
        df = pd.read_sql_query(sql, conn, params=(code, days))
        if df.empty:
            return None
        # 按日期升序排列 (最老在前)
        df = df.sort_values("trade_date").reset_index(drop=True)
        # 确保数值类型
        for col in ["open", "high", "low", "close", "pre_close", "volume", "amount"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        return df
    finally:
        conn.close()


def _get_name_map(codes: list[str]) -> dict[str, str]:
    """获取股票名称，从多个可能的数据源中查找"""
    if not codes:
        return {}
    if not DB_PATH.exists():
        return {}
    conn = sqlite3.connect(str(DB_PATH))
    try:
        names: dict[str, str] = {}
        placeholders = ",".join("?" * len(codes))
        for table, code_col, name_col in [
            ("zt_pool", "code", "name"),
            ("strong_pool", "code", "name"),
            ("fund_flow", "code", "name"),
        ]:
            try:
                rows = conn.execute(
                    f"SELECT DISTINCT {code_col}, {name_col} "
                    f"FROM {table} WHERE {code_col} IN ({placeholders})",
                    codes,
                ).fetchall()
                for r in rows:
                    if r[1]:
                        names[r[0]] = r[1]
            except Exception:
                pass
        return names
    finally:
        conn.close()


def _get_current_price(code: str) -> float | None:
    """获取最新收盘价"""
    if not DB_PATH.exists():
        return None
    conn = sqlite3.connect(str(DB_PATH))
    try:
        row = conn.execute(
            "SELECT close FROM daily_price "
            "WHERE stock_code = ? AND trade_date = (SELECT MAX(trade_date) FROM daily_price)",
            (code,),
        ).fetchone()
        return float(row[0]) if row and row[0] else None
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 技术信号分析
# ---------------------------------------------------------------------------

def _compute_technical_signals(df: pd.DataFrame) -> dict[str, Any]:
    """计算全部技术指标信号, 返回最新一天的信号字典"""
    # --- 计算各指标 ---
    dif, dea, macd_hist = _calc_macd(df)
    k, d, j = _calc_kdj(df)
    upper, middle, lower = _calc_bollinger(df)
    ma_map = _calc_ma(df)
    rsi = _calc_rsi(df)

    # 取最新值 (最后一条)
    idx = len(df) - 1
    prev_idx = idx - 1 if idx > 0 else idx

    cur_close = df["close"].iloc[idx]
    cur_close = float(cur_close) if pd.notna(cur_close) else 0.0

    # --- MACD 信号 ---
    cur_dif = dif.iloc[idx]
    cur_dea = dea.iloc[idx]
    prev_dif = dif.iloc[prev_idx]
    prev_dea = dea.iloc[prev_idx]
    cur_macd_hist = macd_hist.iloc[idx]

    if pd.isna(cur_dif) or pd.isna(cur_dea):
        macd_signal = "数据不足"
    elif prev_dif <= prev_dea and cur_dif > cur_dea:
        macd_signal = "金叉"
    elif prev_dif >= prev_dea and cur_dif < cur_dea:
        macd_signal = "死叉"
    elif cur_dif > cur_dea:
        macd_signal = "多头"
    else:
        macd_signal = "空头"

    # --- KDJ 信号 ---
    cur_j = j.iloc[idx]
    cur_k = k.iloc[idx]
    cur_d_val = d.iloc[idx]
    prev_k = k.iloc[prev_idx]
    prev_d_val = d.iloc[prev_idx]

    if pd.isna(cur_j):
        kdj_signal = "数据不足"
    elif cur_j > 100:
        kdj_signal = "超买"
    elif cur_j < 0:
        kdj_signal = "超卖"
    else:
        kdj_signal = "中性"

    # K上穿D金叉
    kdj_cross = ""
    if not pd.isna(cur_k) and not pd.isna(prev_k):
        if prev_k <= prev_d_val and cur_k > cur_d_val:
            kdj_cross = "金叉"
        elif prev_k >= prev_d_val and cur_k < cur_d_val:
            kdj_cross = "死叉"

    # --- 布林带 ---
    cur_upper = upper.iloc[idx]
    cur_mid = middle.iloc[idx]
    cur_lower = lower.iloc[idx]

    if pd.isna(cur_upper) or pd.isna(cur_lower):
        bollinger_signal = "数据不足"
    elif cur_close <= cur_lower:
        bollinger_signal = "触及下轨"
    elif cur_close >= cur_upper:
        bollinger_signal = "触及上轨"
    elif cur_close >= cur_mid:
        bollinger_signal = "中轨上方"
    else:
        bollinger_signal = "中轨下方"

    # --- MA 均线 ---
    ma5 = ma_map[5].iloc[idx]
    ma20 = ma_map[20].iloc[idx]
    prev_ma5 = ma_map[5].iloc[prev_idx]
    prev_ma20 = ma_map[20].iloc[prev_idx]

    if pd.isna(ma5) or pd.isna(ma20):
        ma_signal = "数据不足"
    elif prev_ma5 <= prev_ma20 and ma5 > ma20:
        ma_signal = "金叉"
    elif prev_ma5 >= prev_ma20 and ma5 < ma20:
        ma_signal = "死叉"
    elif ma5 > ma20:
        ma_signal = "多头"
    else:
        ma_signal = "空头"

    # --- 成交量 ---
    cur_vol = df["volume"].iloc[idx]
    vol_series = df["volume"].iloc[-6:-1]  # 前5天
    avg_vol_5 = vol_series.mean() if len(vol_series) > 0 else cur_vol

    vol_ratio = cur_vol / avg_vol_5 if avg_vol_5 > 0 else 1.0

    prev_close = df["close"].iloc[prev_idx] if prev_idx >= 0 else cur_close
    price_up = cur_close > prev_close if pd.notna(prev_close) else True

    if vol_ratio > 1.5:
        if price_up:
            volume_signal = "放量上涨"
        else:
            volume_signal = "放量下跌"
    elif vol_ratio < 0.5:
        volume_signal = "缩量"
    else:
        volume_signal = "正常"

    # --- RSI ---
    rsi_val = rsi.iloc[idx]
    rsi_val = float(rsi_val) if pd.notna(rsi_val) else 50.0

    # --- 涨跌幅(今天) ---
    pre_close = df["pre_close"].iloc[idx] if "pre_close" in df.columns else prev_close
    chg_pct = 0.0
    if pd.notna(pre_close) and pre_close > 0:
        chg_pct = (cur_close / pre_close - 1) * 100

    return {
        "macd": macd_signal,
        "macd_dif": round(float(cur_dif), 4) if pd.notna(cur_dif) else None,
        "macd_dea": round(float(cur_dea), 4) if pd.notna(cur_dea) else None,
        "macd_hist": round(float(cur_macd_hist), 4) if pd.notna(cur_macd_hist) else None,
        "kdj": kdj_signal,
        "kdj_k": round(float(cur_k), 2) if pd.notna(cur_k) else None,
        "kdj_d": round(float(cur_d_val), 2) if pd.notna(cur_d_val) else None,
        "kdj_j": round(float(cur_j), 2) if pd.notna(cur_j) else None,
        "kdj_cross": kdj_cross,
        "bollinger": bollinger_signal,
        "boll_upper": round(float(cur_upper), 2) if pd.notna(cur_upper) else None,
        "boll_mid": round(float(cur_mid), 2) if pd.notna(cur_mid) else None,
        "boll_lower": round(float(cur_lower), 2) if pd.notna(cur_lower) else None,
        "ma5_ma20": ma_signal,
        "ma5": round(float(ma5), 2) if pd.notna(ma5) else None,
        "ma20": round(float(ma20), 2) if pd.notna(ma20) else None,
        "volume": volume_signal,
        "vol_ratio": round(float(vol_ratio), 2),
        "rsi_14": round(rsi_val, 1),
        "chg_pct": round(chg_pct, 2),
    }


def _compute_support_resistance(df: pd.DataFrame, window: int = 20
                                ) -> tuple[float, float]:
    """计算支撑位和压力位

    支撑: 近 N 天最低价
    压力: 近 N 天最高价
    """
    tail = df.tail(window)
    support = float(tail["low"].min())
    resistance = float(tail["high"].max())
    return support, resistance


# ---------------------------------------------------------------------------
# 综合判断
# ---------------------------------------------------------------------------

def _judge_sell_action(
    signals: dict[str, Any],
    pnl_pct: float,
    stop_loss_dist: float,
) -> tuple[str, str, str]:
    """判断卖出操作

    Returns:
        (action, action_reason, urgency)
        action: "清仓" | "减仓" | "持有" | "观望"
        urgency: "高" | "中" | "低"
    """
    # 2. 已破止损线 → 立即清仓
    if stop_loss_dist <= 0:
        return "清仓", "已破止损线", "高"

    # 1. 接近止损线 (<2%) → 清仓
    if stop_loss_dist < 2:
        return "清仓", "接近止损线", "高"

    # 3. 技术面全面看空 → 减仓
    bear_count = sum([
        signals.get("macd") == "死叉",
        signals.get("kdj") == "超卖",
        signals.get("ma5_ma20") == "空头",
        signals.get("volume") == "放量下跌",
    ])
    if bear_count >= 3:
        return "减仓", f"技术面全面看空({bear_count}项看空指标)", "高"

    # 4. 深度套牢 + 无反弹迹象 → 止损
    if pnl_pct < -20 and signals.get("ma5_ma20") == "空头":
        return "清仓", "深度套牢+无反弹迹象", "中"

    # 5. 多项看空 + 浮亏 → 减仓
    if bear_count >= 2 and pnl_pct < -10:
        return "减仓", f"技术面偏空({bear_count}项看空)+浮亏{pnl_pct:.1f}%", "中"

    # 6. 死叉 + 浮亏 → 观望
    if signals.get("macd") == "死叉" and pnl_pct < -5:
        return "观望", "MACD死叉+浮亏", "中"

    # 7. 技术面中性 → 持有
    return "持有", "等待信号", "低"


def _estimate_sell_price_target(
    current_price: float,
    signals: dict[str, Any],
    resistance: float,
    action: str,
) -> float:
    """估算建议卖出价位"""
    if action == "清仓":
        # 清仓时用当前价即可, 不等更高
        return round(current_price, 2)
    if action == "减仓":
        # 减仓可等到压力位附近
        return round(min(resistance, current_price * 1.03), 2)
    # 持有: 等到压力位
    return round(resistance, 2)


def _estimate_sell_date(signals: dict[str, Any], urgency: str) -> str:
    """估算建议卖出时间 (MM-DD 格式)"""
    today = date.today()
    if urgency == "高":
        # 尽快: 今天或明天
        target = today + timedelta(days=1)
    elif urgency == "中":
        target = today + timedelta(days=3)
    else:
        target = today + timedelta(days=7)
    return target.strftime("%m-%d")


def _judge_buy_action(
    signals: dict[str, Any],
    current_price: float,
    support: float,
    resistance: float,
) -> tuple[str, str, float, str]:
    """判断买入操作

    Returns:
        (action, action_reason, suggested_entry, entry_type)
    """
    chg_pct = signals.get("chg_pct", 0.0)
    rsi = signals.get("rsi_14", 50.0)
    ma_signal = signals.get("ma5_ma20", "")
    macd_signal = signals.get("macd", "")
    vol_signal = signals.get("volume", "")
    bollinger = signals.get("bollinger", "")

    # 1. 涨停/接近涨停 → 观望
    if chg_pct >= 9.5:
        return "观望", "涨停或接近涨停,明天可能回调", current_price, "观望"

    # 2. RSI > 70 超买 → 等待回调
    if rsi > 70:
        entry = round(support * 1.02, 2)  # 支撑位附近
        return "等待回调", f"RSI={rsi:.1f}超买,等回调至{entry}", entry, "回调买入"

    # 3. MA5 < MA20 空头 → 等待均线走平
    if ma_signal in ("空头", "死叉"):
        entry = round(support * 1.01, 2)
        return "等待回调", f"均线{ma_signal},等待均线走平", entry, "回调买入"

    # 4. MACD金叉 + 放量 + RSI < 70 → 可以买入
    bull_signals = 0
    reasons = []
    if macd_signal in ("金叉", "多头"):
        bull_signals += 1
        reasons.append(f"MACD{macd_signal}")
    if "放量" in vol_signal:
        bull_signals += 1
        reasons.append(vol_signal)
    if rsi < 70:
        bull_signals += 1

    if macd_signal == "金叉" and "放量" in vol_signal and rsi < 70:
        return "买入", "MACD金叉+放量+RSI正常", current_price, "直接买入"

    # 5. 价格在支撑位附近 → 回调买入
    if current_price <= support * 1.03:
        return "买入", f"价格在支撑位{support:.2f}附近", current_price, "回调买入"

    # 6. 布林带下轨 → 回调买入机会
    if bollinger == "触及下轨":
        return "买入", "触及布林带下轨,超卖反弹", current_price, "回调买入"

    # 7. 多头信号综合
    if bull_signals >= 2:
        return "买入", "+".join(reasons), current_price, "直接买入"

    # 8. 默认观望
    entry = round(support * 1.02, 2)
    return "观望", "信号不明确,建议观望", entry, "回调买入"


# ---------------------------------------------------------------------------
# 核心类: SignalMonitor
# ---------------------------------------------------------------------------

class SignalMonitor:
    """实时买卖信号监控系统

    基于用户真实持仓, 用技术指标计算具体的买卖点信号。

    Usage:
        monitor = SignalMonitor()
        report = monitor.analyze_position("300059", 22.737, 3400)
        buy_report = monitor.analyze_buy_candidate("600234", 0.1381)
        full_report = monitor.get_all_signals()
    """

    def __init__(self, db_path: Path | str | None = None) -> None:
        self._db_path = Path(db_path) if db_path else DB_PATH

    # -------------------------------------------------------------------
    # 内部工具
    # -------------------------------------------------------------------

    def _get_name(self, code: str) -> str:
        """获取股票名称"""
        # 先从预定义持仓中找
        if code in PORTFOLIO:
            return PORTFOLIO[code]["name"]
        # 再从数据库找
        names = _get_name_map([code])
        return names.get(code, code)

    def _analyze_technicals(self, code: str) -> dict[str, Any] | None:
        """获取技术分析原始数据 (df + signals + support/resistance)"""
        df = _get_daily_data(code, days=120)
        if df is None or len(df) < 30:
            logger.warning("%s 数据不足 (需要至少30天)", code)
            return None

        signals = _compute_technical_signals(df)
        support, resistance = _compute_support_resistance(df)
        current_price = float(df["close"].iloc[-1])

        return {
            "df": df,
            "signals": signals,
            "support": support,
            "resistance": resistance,
            "current_price": current_price,
        }

    # -------------------------------------------------------------------
    # 方法1: 分析持仓卖出信号
    # -------------------------------------------------------------------

    def analyze_position(self, code: str, cost_price: float, shares: int,
                         stop_loss: float | None = None) -> dict[str, Any]:
        """分析一只持仓股的卖出信号

        Args:
            code: 股票代码
            cost_price: 成本价
            shares: 持仓数量
            stop_loss: 止损价 (None则从PORTFOLIO读取或默认-8%)

        Returns:
            完整的卖出信号分析字典
        """
        name = self._get_name(code)

        # 获取止损价
        if stop_loss is None:
            if code in PORTFOLIO:
                stop_loss = PORTFOLIO[code].get("stop_loss", cost_price * 0.92)
            else:
                stop_loss = cost_price * 0.92  # 默认-8%

        # 技术分析
        tech = self._analyze_technicals(code)

        if tech is None:
            # 数据不足, 返回基础信息
            return {
                "code": code,
                "name": name,
                "current_price": None,
                "cost_price": cost_price,
                "pnl_pct": None,
                "stop_loss": stop_loss,
                "stop_loss_dist": None,
                "signals": {},
                "action": "观望",
                "action_reason": "数据不足,无法分析",
                "urgency": "低",
                "target_sell_price": None,
                "target_sell_date": None,
                "support": None,
                "resistance": None,
                "error": "数据不足(需要至少30天日K)",
            }

        current_price = tech["current_price"]
        signals = tech["signals"]
        support = tech["support"]
        resistance = tech["resistance"]

        # 计算盈亏
        pnl_pct = round((current_price / cost_price - 1) * 100, 1) if cost_price > 0 else 0.0
        stop_loss_dist = round((current_price / stop_loss - 1) * 100, 1) if stop_loss > 0 else 999.0

        # 综合判断
        action, action_reason, urgency = _judge_sell_action(signals, pnl_pct, stop_loss_dist)

        # 建议卖出价位
        target_sell_price = _estimate_sell_price_target(current_price, signals, resistance, action)
        target_sell_date = _estimate_sell_date(signals, urgency)

        return {
            "code": code,
            "name": name,
            "current_price": round(current_price, 2),
            "cost_price": cost_price,
            "pnl_pct": pnl_pct,
            "stop_loss": stop_loss,
            "stop_loss_dist": stop_loss_dist,

            # 技术信号
            "signals": {
                "macd": signals["macd"],
                "kdj": signals["kdj"],
                "bollinger": signals["bollinger"],
                "ma5_ma20": signals["ma5_ma20"],
                "volume": signals["volume"],
                "rsi_14": signals["rsi_14"],
            },

            # 详细指标数值
            "signal_details": {
                "macd_dif": signals["macd_dif"],
                "macd_dea": signals["macd_dea"],
                "macd_hist": signals["macd_hist"],
                "kdj_k": signals["kdj_k"],
                "kdj_d": signals["kdj_d"],
                "kdj_j": signals["kdj_j"],
                "kdj_cross": signals["kdj_cross"],
                "boll_upper": signals["boll_upper"],
                "boll_mid": signals["boll_mid"],
                "boll_lower": signals["boll_lower"],
                "ma5": signals["ma5"],
                "ma20": signals["ma20"],
                "vol_ratio": signals["vol_ratio"],
                "chg_pct": signals["chg_pct"],
            },

            # 综合判断
            "action": action,
            "action_reason": action_reason,
            "urgency": urgency,
            "target_sell_price": target_sell_price,
            "target_sell_date": target_sell_date,

            # 支撑压力位
            "support": round(support, 2),
            "resistance": round(resistance, 2),
        }

    # -------------------------------------------------------------------
    # 方法2: 分析买入候选
    # -------------------------------------------------------------------

    def analyze_buy_candidate(self, code: str, score: float = 0.0) -> dict[str, Any]:
        """分析一只ML推荐股的买入信号

        Args:
            code: 股票代码
            score: ML模型预测得分

        Returns:
            完整的买入信号分析字典
        """
        name = self._get_name(code)

        # 技术分析
        tech = self._analyze_technicals(code)

        if tech is None:
            return {
                "code": code,
                "name": name,
                "ml_score": score,
                "current_price": None,
                "signals": {},
                "action": "观望",
                "action_reason": "数据不足,无法分析",
                "suggested_entry": None,
                "entry_type": "观望",
                "support": None,
                "resistance": None,
                "risk_reward": None,
                "error": "数据不足(需要至少30天日K)",
            }

        current_price = tech["current_price"]
        signals = tech["signals"]
        support = tech["support"]
        resistance = tech["resistance"]

        # 买入判断
        action, action_reason, suggested_entry, entry_type = _judge_buy_action(
            signals, current_price, support, resistance,
        )

        # 风险收益比: (压力位 - 当前价) / (当前价 - 支撑位)
        risk = current_price - support
        reward = resistance - current_price
        if risk > 0:
            risk_reward = round(reward / risk, 1)
        else:
            risk_reward = 99.9  # 支撑就在脚下, 风险极低

        return {
            "code": code,
            "name": name,
            "ml_score": round(score, 4),
            "current_price": round(current_price, 2),

            # 技术信号
            "signals": {
                "macd": signals["macd"],
                "kdj": signals["kdj"],
                "bollinger": signals["bollinger"],
                "ma5_ma20": signals["ma5_ma20"],
                "volume": signals["volume"],
                "rsi_14": signals["rsi_14"],
            },

            # 详细指标数值
            "signal_details": {
                "macd_dif": signals["macd_dif"],
                "macd_dea": signals["macd_dea"],
                "macd_hist": signals["macd_hist"],
                "kdj_k": signals["kdj_k"],
                "kdj_d": signals["kdj_d"],
                "kdj_j": signals["kdj_j"],
                "kdj_cross": signals["kdj_cross"],
                "boll_upper": signals["boll_upper"],
                "boll_mid": signals["boll_mid"],
                "boll_lower": signals["boll_lower"],
                "ma5": signals["ma5"],
                "ma20": signals["ma20"],
                "vol_ratio": signals["vol_ratio"],
                "chg_pct": signals["chg_pct"],
            },

            # 综合判断
            "action": action,
            "action_reason": action_reason,
            "suggested_entry": suggested_entry,
            "entry_type": entry_type,

            # 支撑压力位
            "support": round(support, 2),
            "resistance": round(resistance, 2),
            "risk_reward": risk_reward,
        }

    # -------------------------------------------------------------------
    # 方法3: 完整信号报告
    # -------------------------------------------------------------------

    def get_all_signals(self) -> dict[str, Any]:
        """返回所有持仓和ML推荐的完整信号报告

        从数据库读取用户持仓配置和ML预测结果。

        Returns:
            {
                "date": "2026-05-09",
                "positions": [...],   # 持仓分析列表
                "buy_candidates": [...],  # 买入候选列表
                "summary": {...},     # 汇总
            }
        """
        today = date.today().isoformat()

        # ===== 持仓分析 =====
        position_reports: list[dict] = []
        for code, info in PORTFOLIO.items():
            report = self.analyze_position(
                code=code,
                cost_price=info["cost"],
                shares=info["shares"],
                stop_loss=info.get("stop_loss"),
            )
            position_reports.append(report)

        # 按 urgency 排序: 高 > 中 > 低
        urgency_order = {"高": 0, "中": 1, "低": 2, "": 3}
        position_reports.sort(
            key=lambda r: urgency_order.get(r.get("urgency", ""), 3)
        )

        # ===== 买入候选 =====
        buy_reports: list[dict] = []
        prediction = self._load_prediction()
        if prediction:
            for item in prediction.get("top7", []):
                code = item.get("code", "")
                score = item.get("score", 0.0)
                # 跳过已持仓
                if code in PORTFOLIO:
                    continue
                report = self.analyze_buy_candidate(code, score)
                buy_reports.append(report)

            # 如果 top7 不够, 补充到 top20
            top7_codes = {item.get("code") for item in prediction.get("top7", [])}
            for item in prediction.get("all_top", [])[:20]:
                code = item.get("code", "")
                if code in top7_codes or code in PORTFOLIO:
                    continue
                score = item.get("score", 0.0)
                if score < 0.05:
                    continue
                report = self.analyze_buy_candidate(code, score)
                buy_reports.append(report)

        # 按 action 优先级排序: 买入 > 等待回调 > 观望
        action_order = {"买入": 0, "等待回调": 1, "观望": 2, "不买": 3, "": 4}
        buy_reports.sort(
            key=lambda r: action_order.get(r.get("action", ""), 4)
        )

        # ===== 汇总 =====
        high_urgency = [r for r in position_reports if r.get("urgency") == "高"]
        need_sell = [r for r in position_reports if r.get("action") in ("清仓", "减仓")]
        can_buy = [r for r in buy_reports if r.get("action") == "买入"]
        wait_buy = [r for r in buy_reports if r.get("action") == "等待回调"]

        summary = {
            "total_positions": len(position_reports),
            "high_urgency_count": len(high_urgency),
            "need_sell_count": len(need_sell),
            "need_sell_codes": [r["code"] for r in need_sell],
            "can_buy_count": len(can_buy),
            "can_buy_codes": [r["code"] for r in can_buy],
            "wait_buy_count": len(wait_buy),
            "alert": "",
        }

        # 生成告警
        alerts: list[str] = []
        if high_urgency:
            alerts.append(
                f"⚠ {len(high_urgency)}只持仓紧急: "
                + ", ".join(f"{r['name']}({r['action']})" for r in high_urgency)
            )
        if can_buy:
            alerts.append(
                f"✓ {len(can_buy)}只可买入: "
                + ", ".join(f"{r['name']}" for r in can_buy)
            )
        summary["alert"] = " | ".join(alerts) if alerts else "无紧急信号"

        return {
            "date": today,
            "positions": position_reports,
            "buy_candidates": buy_reports,
            "summary": summary,
        }

    # -------------------------------------------------------------------
    # 辅助
    # -------------------------------------------------------------------

    def _load_prediction(self) -> dict | None:
        """加载ML预测结果"""
        if not PRED_PATH.exists():
            return None
        try:
            return json.loads(PRED_PATH.read_text(encoding="utf-8"))
        except Exception as e:
            logger.warning("加载预测结果失败: %s", e)
            return None

    # -------------------------------------------------------------------
    # 格式化输出
    # -------------------------------------------------------------------

    @staticmethod
    def format_position_report(report: dict) -> str:
        """格式化单只持仓报告为可读文本"""
        lines = []
        lines.append(f"{'='*60}")
        lines.append(f"  {report['name']} ({report['code']})")
        lines.append(f"{'='*60}")
        lines.append(f"  当前价: {report['current_price']}  成本价: {report['cost_price']}")
        lines.append(f"  盈亏: {report['pnl_pct']:+.1f}%")
        lines.append(f"  止损线: {report['stop_loss']}  距止损: {report['stop_loss_dist']:+.1f}%")
        lines.append(f"")
        lines.append(f"  技术信号:")
        sig = report.get("signals", {})
        lines.append(f"    MACD: {sig.get('macd', '-')}")
        lines.append(f"    KDJ:  {sig.get('kdj', '-')}")
        lines.append(f"    布林: {sig.get('bollinger', '-')}")
        lines.append(f"    MA:   {sig.get('ma5_ma20', '-')}")
        lines.append(f"    量:   {sig.get('volume', '-')}")
        lines.append(f"    RSI:  {sig.get('rsi_14', '-')}")
        lines.append(f"")
        lines.append(f"  支撑: {report.get('support', '-')}  压力: {report.get('resistance', '-')}")
        lines.append(f"")
        lines.append(f"  >>> 操作: {report['action']} ({report['urgency']}紧迫)")
        lines.append(f"  >>> 原因: {report['action_reason']}")
        lines.append(f"  >>> 目标价: {report.get('target_sell_price', '-')}  时间: {report.get('target_sell_date', '-')}")
        return "\n".join(lines)

    @staticmethod
    def format_buy_report(report: dict) -> str:
        """格式化单只买入候选报告"""
        lines = []
        lines.append(f"{'='*60}")
        lines.append(f"  {report['name']} ({report['code']})")
        lines.append(f"{'='*60}")
        lines.append(f"  当前价: {report['current_price']}  ML得分: {report['ml_score']}")
        lines.append(f"")
        lines.append(f"  技术信号:")
        sig = report.get("signals", {})
        lines.append(f"    MACD: {sig.get('macd', '-')}")
        lines.append(f"    KDJ:  {sig.get('kdj', '-')}")
        lines.append(f"    布林: {sig.get('bollinger', '-')}")
        lines.append(f"    MA:   {sig.get('ma5_ma20', '-')}")
        lines.append(f"    量:   {sig.get('volume', '-')}")
        lines.append(f"    RSI:  {sig.get('rsi_14', '-')}")
        lines.append(f"")
        lines.append(f"  支撑: {report.get('support', '-')}  压力: {report.get('resistance', '-')}")
        lines.append(f"  风险收益比: {report.get('risk_reward', '-')}")
        lines.append(f"")
        lines.append(f"  >>> 操作: {report['action']}")
        lines.append(f"  >>> 原因: {report['action_reason']}")
        lines.append(f"  >>> 建议价: {report.get('suggested_entry', '-')}  方式: {report.get('entry_type', '-')}")
        return "\n".join(lines)

    def format_full_report(self, report: dict | None = None) -> str:
        """格式化完整报告"""
        if report is None:
            report = self.get_all_signals()

        lines = []
        lines.append(f"{'#'*65}")
        lines.append(f"  信号监控报告 — {report['date']}")
        lines.append(f"{'#'*65}")
        lines.append("")

        # 汇总
        summary = report.get("summary", {})
        lines.append(f"  {summary.get('alert', '')}")
        lines.append("")

        # 持仓
        lines.append(f"  ── 持仓分析 ({summary.get('total_positions', 0)}只) ──")
        for pos in report.get("positions", []):
            lines.append(self.format_position_report(pos))
            lines.append("")

        # 买入候选
        buy_list = report.get("buy_candidates", [])
        if buy_list:
            lines.append(f"  ── 买入候选 ({len(buy_list)}只) ──")
            for buy in buy_list:
                lines.append(self.format_buy_report(buy))
                lines.append("")

        return "\n".join(lines)


# ---------------------------------------------------------------------------
# CLI 入口
# ---------------------------------------------------------------------------

def main() -> None:
    """CLI 入口: 打印完整信号报告"""
    monitor = SignalMonitor()
    report = monitor.get_all_signals()
    print(monitor.format_full_report(report))

    # 保存 JSON 报告
    output_dir = PROJECT_ROOT / "output" / "trader"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "signal_monitor.json"
    output_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"\n报告已保存: {output_path}")


if __name__ == "__main__":
    main()


# ---------------------------------------------------------------------------
# 公共接口 — 供trader层调用的数据/信号工具函数
# ---------------------------------------------------------------------------
# 内部实现用下划线前缀(模块内私有)，这里提供公共别名供外部使用

get_daily_data = _get_daily_data
"""获取日K线数据(日期升序)，供trader层买入检查使用"""

compute_technical_signals = _compute_technical_signals
"""计算技术指标信号(MACD/KDJ/BOLL/RSI等)，供trader层使用"""

compute_support_resistance = _compute_support_resistance
"""计算支撑阻力位，供trader层使用"""
