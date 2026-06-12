"""
技术指标评估模块 — 卖出决策辅助

524笔5分钟K线回测结论:
- 纯日K线评分: 6只对2只(比抛硬币差), 日K线滞后
- "当前价vs开盘价"单点: 6只对3只, 单点没说服力
- 5分钟走势方向: 大样本区分度1.28x(不够)
- 最有效: 开盘30分+斜率>0.3%/5min(区分度1.33x, 综合改善0.22%/笔)
- 下午触发: 0%反转率, 必须止损

规则(数据驱动):
  开盘30分钟内触发 + 5分钟斜率>0.3% → 跳过止损
  其他时段/条件不足 → 一律止损
"""

import logging
import sqlite3
from pathlib import Path

logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).resolve().parent.parent.parent / "data" / "alpha_miner.db"


def should_skip_stop_loss(code: str, chg_from_buy: float, strategy: str,
                          quote: dict = None) -> tuple[bool, str]:
    """判断是否应该跳过止损
    
    524笔回测: 只有"开盘30分+斜率>0.3%/5min"有效
    """
    from src.trader.intraday_cache import get_cache
    cache = get_cache()
    analysis = cache.analyze(code)
    
    if not analysis["ready"]:
        return False, f"分时不足({analysis['detail']}) → 止损"
    
    if analysis["is_strong"]:
        return True, f"分时强势({analysis['detail']})"
    
    return False, f"分时弱势({analysis['detail']})"


def get_atr(code: str, period: int = 14) -> float | None:
    """计算ATR(绝对值, 单位:元). 返回None表示数据不足."""
    try:
        conn = sqlite3.connect(str(DB_PATH))
        rows = conn.execute("""
            SELECT high, low, close
            FROM daily_price
            WHERE stock_code = ? AND COALESCE(close, 0) > 0
            ORDER BY trade_date DESC LIMIT ?
        """, (code, period + 5)).fetchall()
        conn.close()

        if len(rows) < period + 1:
            return None

        import pandas as pd
        from src.strategy.technical import _compute_atr

        high = pd.Series([r[0] for r in reversed(rows)])
        low = pd.Series([r[1] for r in reversed(rows)])
        close = pd.Series([r[2] for r in reversed(rows)])
        return _compute_atr(high, low, close, period)
    except Exception as e:
        logger.debug(f"ATR计算 {code} 失败: {e}")
        return None


_atr_cache: dict[str, tuple[float | None, str]] = {}


def get_atr_cached(code: str, period: int = 14) -> float | None:
    """带日期缓存的ATR — 同一天内同一股票只查一次DB."""
    from datetime import date
    today = date.today().isoformat()
    cached = _atr_cache.get(code)
    if cached and cached[1] == today:
        return cached[0]
    atr = get_atr(code, period)
    _atr_cache[code] = (atr, today)
    return atr


def get_tech_indicators(code: str, lookback: int = 20) -> dict | None:
    """获取日K线技术指标(仅供display用, 不参与止损决策)"""
    try:
        conn = sqlite3.connect(str(DB_PATH))
        rows = conn.execute("""
            SELECT close, volume, high, low 
            FROM daily_price 
            WHERE stock_code = ? AND COALESCE(close,0) > 0 
            ORDER BY trade_date DESC LIMIT ?
        """, (code, lookback + 5)).fetchall()
        conn.close()
        
        if len(rows) < lookback:
            return None
        
        closes = [r[0] for r in reversed(rows)]
        n = len(closes)
        ma5 = sum(closes[-5:]) / 5
        ma10 = sum(closes[-10:]) / 10 if n >= 10 else None
        ma20 = sum(closes[-20:]) / 20 if n >= 20 else None
        
        return {
            "ma5": round(ma5, 2),
            "ma10": round(ma10, 2) if ma10 else None,
            "ma20": round(ma20, 2) if ma20 else None,
            "last_close": closes[-1],
        }
    except Exception as e:
        logger.error(f"[技术指标] {code} 计算失败: {e}")
        return None
