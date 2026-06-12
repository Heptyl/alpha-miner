"""日内实时信号引擎 — 基于分时数据计算买卖信号

功能:
- 基于实时行情计算日内技术指标
- 检测量价异动(急涨急跌/放量)
- 生成精确的买入/卖出时机信号
- 适配Streamlit实时面板
"""

import json
import time
import sqlite3
from pathlib import Path
from typing import Optional
from datetime import datetime

from src.trader.realtime_quote import get_realtime, USER_POSITIONS

OUTPUT_PATH = Path("output/trader/intraday_signals.json")

# 用户持仓信息
# 用户持仓 — 统一从 portfolio.json 读取（同源）
from src.config.portfolio import get_legacy_portfolio_dict as _get_portfolio
USER_PORTFOLIO = _get_portfolio()


def get_minute_data(code: str, period: str = "5") -> list[dict]:
    """获取分钟K线数据 (从akshare或数据库)"""
    import warnings
    warnings.filterwarnings("ignore")
    
    try:
        import akshare as ak
        df = ak.stock_zh_a_hist_min_em(symbol=code, period=period, adjust="")
        if df is None or len(df) == 0:
            return []
        
        cols = list(df.columns)
        result = []
        for _, row in df.tail(60).iterrows():  # 最近60根
            result.append({
                "time": str(row[cols[0]]),
                "open": float(row[cols[1]]),
                "close": float(row[cols[2]]),
                "high": float(row[cols[3]]),
                "low": float(row[cols[4]]),
                "volume": int(row[cols[5]]),
                "amount": float(row[cols[6]]) if len(cols) > 6 else 0,
            })
        return result
    except Exception:
        # Fallback to DB daily data
        return _get_daily_from_db(code)


def _get_daily_from_db(code: str, n: int = 20) -> list[dict]:
    """从数据库获取日K线作为后备"""
    conn = sqlite3.connect("data/alpha_miner.db")
    rows = conn.execute(
        "SELECT trade_date, open, close, high, low, volume, amount "
        "FROM daily_price WHERE stock_code=? ORDER BY trade_date DESC LIMIT ?",
        (code, n)
    ).fetchall()
    conn.close()
    
    return [{
        "time": r[0], "open": r[1], "close": r[2],
        "high": r[3], "low": r[4], "volume": r[5], "amount": r[6] or 0
    } for r in reversed(rows)]


def calc_intraday_indicators(bars: list[dict]) -> dict:
    """计算日内技术指标 (基于分钟K线)"""
    if len(bars) < 20:
        return {"error": "数据不足"}
    
    closes = [b["close"] for b in bars]
    volumes = [b["volume"] for b in bars]
    highs = [b["high"] for b in bars]
    lows = [b["low"] for b in bars]
    
    n = len(closes)
    
    # MA5/MA10/MA20
    ma5 = sum(closes[-5:]) / 5 if n >= 5 else 0
    ma10 = sum(closes[-10:]) / 10 if n >= 10 else 0
    ma20 = sum(closes[-20:]) / 20 if n >= 20 else 0
    
    # RSI(14)
    if n >= 15:
        gains, losses = [], []
        for i in range(-14, 0):
            diff = closes[i] - closes[i-1]
            gains.append(max(diff, 0))
            losses.append(max(-diff, 0))
        avg_gain = sum(gains) / 14
        avg_loss = sum(losses) / 14
        rs = avg_gain / avg_loss if avg_loss > 0 else 100
        rsi = 100 - 100 / (1 + rs)
    else:
        rsi = 50
    
    # 布林带
    if n >= 20:
        ma20_val = sum(closes[-20:]) / 20
        std = (sum((c - ma20_val)**2 for c in closes[-20:]) / 20) ** 0.5
        boll_upper = ma20_val + 2 * std
        boll_mid = ma20_val
        boll_lower = ma20_val - 2 * std
    else:
        boll_upper = boll_mid = boll_lower = closes[-1]
    
    # MACD(12,26,9) - 简化版
    macd_signal = "中性"
    dif = 0
    if n >= 26:
        ema12 = _calc_ema(closes, 12)
        ema26 = _calc_ema(closes, 26)
        dif = ema12 - ema26
        # 近似DEA
        if n >= 35:
            difs = []
            for i in range(26, n):
                e12 = _calc_ema(closes[:i+1], 12)
                e26 = _calc_ema(closes[:i+1], 26)
                difs.append(e12 - e26)
            dea = sum(difs[-9:]) / 9 if len(difs) >= 9 else 0
            prev_dif = difs[-2] if len(difs) >= 2 else 0
            prev_dea = sum(difs[-10:-1]) / 9 if len(difs) >= 10 else 0
            
            if prev_dif <= prev_dea and dif > dea:
                macd_signal = "金叉"  # 看涨
            elif prev_dif >= prev_dea and dif < dea:
                macd_signal = "死叉"  # 看跌
            elif dif > dea:
                macd_signal = "多头"
            else:
                macd_signal = "空头"
    
    # KDJ(9,3,3)
    kdj_k = kdj_d = kdj_j = 50
    if n >= 9:
        rsv = (closes[-1] - min(lows[-9:])) / (max(highs[-9:]) - min(lows[-9:])) * 100             if max(highs[-9:]) != min(lows[-9:]) else 50
        kdj_k = rsv  # 简化
        kdj_d = kdj_k
        kdj_j = 3 * kdj_k - 2 * kdj_d
    
    # 量价分析
    avg_vol = sum(volumes[-10:]) / 10 if n >= 10 else volumes[-1]
    vol_ratio = volumes[-1] / avg_vol if avg_vol > 0 else 1
    price_chg = (closes[-1] - closes[-2]) / closes[-2] * 100 if n >= 2 else 0
    
    if vol_ratio > 2 and price_chg > 1:
        vol_signal = "放量上涨"
    elif vol_ratio > 2 and price_chg < -1:
        vol_signal = "放量下跌"
    elif vol_ratio < 0.5:
        vol_signal = "极度缩量"
    elif price_chg > 1:
        vol_signal = "上涨"
    elif price_chg < -1:
        vol_signal = "下跌"
    else:
        vol_signal = "横盘"
    
    return {
        "ma5": round(ma5, 2), "ma10": round(ma10, 2), "ma20": round(ma20, 2),
        "rsi": round(rsi, 1),
        "boll_upper": round(boll_upper, 2), "boll_mid": round(boll_mid, 2), "boll_lower": round(boll_lower, 2),
        "macd": macd_signal, "dif": round(dif, 4),
        "kdj_k": round(kdj_k, 1), "kdj_d": round(kdj_d, 1), "kdj_j": round(kdj_j, 1),
        "vol_ratio": round(vol_ratio, 2), "vol_signal": vol_signal,
        "price_change_5min": round(price_chg, 2),
    }


def _calc_ema(data: list, period: int) -> float:
    """计算EMA"""
    if len(data) < period:
        return sum(data) / len(data) if data else 0
    k = 2 / (period + 1)
    ema = sum(data[:period]) / period
    for val in data[period:]:
        ema = val * k + ema * (1 - k)
    return ema


def detect_abnormal_move(quote: dict, indicators: dict) -> list[str]:
    """检测量价异动"""
    alerts = []
    
    # 急涨急跌
    chg_5min = indicators.get("price_change_5min", 0)
    if chg_5min > 2:
        alerts.append(f"⚡ 急涨 +{chg_5min:.1f}%")
    elif chg_5min < -2:
        alerts.append(f"⚠️ 急跌 {chg_5min:.1f}%")
    
    # 放量异动
    vol_ratio = indicators.get("vol_ratio", 1)
    if vol_ratio > 3:
        alerts.append(f"📊 异常放量 {vol_ratio:.1f}倍")
    elif vol_ratio > 2:
        alerts.append(f"📊 明显放量 {vol_ratio:.1f}倍")
    
    # RSI极端
    rsi = indicators.get("rsi", 50)
    if rsi > 80:
        alerts.append(f"🔴 RSI超买 {rsi:.0f}")
    elif rsi < 20:
        alerts.append(f"🟢 RSI超卖 {rsi:.0f}")
    
    # 布林带突破
    price = quote.get("price", 0)
    boll_upper = indicators.get("boll_upper", 0)
    boll_lower = indicators.get("boll_lower", 0)
    if price >= boll_upper and boll_upper > 0:
        alerts.append("📈 触及布林上轨")
    elif price <= boll_lower and boll_lower > 0:
        alerts.append("📉 触及布林下轨")
    
    return alerts


def generate_intraday_signal(code: str, quote: dict, indicators: dict, 
                              is_position: bool = True, cost: float = 0,
                              stop_loss: float = 0) -> dict:
    """生成日内交易信号"""
    
    price = quote.get("price", 0)
    alerts = detect_abnormal_move(quote, indicators)
    
    signal = "观望"
    reason = ""
    
    if is_position:
        # === 持仓股信号 ===
        pnl_pct = (price / cost - 1) * 100 if cost > 0 else 0
        stop_dist = (price / stop_loss - 1) * 100 if stop_loss > 0 else 0
        
        # 1. 止损预警
        if stop_dist < 2 and stop_dist > 0:
            signal = "减仓"
            reason = f"距止损仅{stop_dist:.1f}%，建议减仓"
        elif stop_dist <= 0:
            signal = "清仓"
            reason = f"已破止损线{stop_loss}！"
        
        # 2. MACD/KDJ信号
        elif indicators.get("macd") == "死叉" and indicators.get("rsi", 50) < 30:
            signal = "减仓"
            reason = "MACD死叉+RSI超卖，可能有反弹但趋势偏弱"
        
        elif indicators.get("macd") == "金叉" and indicators.get("rsi", 50) < 70:
            signal = "持有"
            reason = "MACD金叉，短期可能反弹"
            if pnl_pct > -5:
                signal = "持有等反弹"
                reason = f"MACD金叉，浮亏{pnl_pct:+.1f}%等反弹"
        
        # 3. 急跌信号
        elif indicators.get("price_change_5min", 0) < -2:
            signal = "关注"
            reason = "急跌，观察是否企稳"
        
        # 4. 急涨信号(浮亏股)
        elif indicators.get("price_change_5min", 0) > 2 and pnl_pct < 0:
            signal = "关注减仓机会"
            reason = f"反弹中，浮亏{pnl_pct:+.1f}%，关注减仓机会"
        
        else:
            reason = f"持有，浮亏{pnl_pct:+.1f}%"
    else:
        # === 买入候选信号 ===
        rsi = indicators.get("rsi", 50)
        macd = indicators.get("macd", "中性")
        
        if rsi < 30 and macd in ("金叉", "多头"):
            signal = "关注买入"
            reason = f"RSI={rsi:.0f}超卖+MACD{macd}，可能见底"
        elif rsi > 70:
            signal = "等回调"
            reason = f"RSI={rsi:.0f}超买，等回调"
        elif indicators.get("vol_signal") == "放量上涨":
            signal = "关注突破"
            reason = "放量上涨，关注是否有效突破"
        else:
            reason = "信号不明确，继续观察"
    
    return {
        "code": code,
        "name": quote.get("name", ""),
        "price": price,
        "signal": signal,
        "reason": reason,
        "alerts": alerts,
        "indicators": indicators,
        "timestamp": datetime.now().strftime("%H:%M:%S"),
    }


def scan_all() -> dict:
    """扫描所有持仓+ML候选，生成日内信号"""
    # 1. 获取实时行情
    all_codes = list(USER_POSITIONS)
    
    # 加ML候选
    ml_path = Path("output/ml/latest_prediction.json")
    ml_codes = []
    if ml_path.exists():
        ml_data = json.loads(ml_path.read_text())
        ml_codes = [item["code"] for item in ml_data.get("predictions", [])[:7]]
        all_codes.extend(ml_codes)
    
    quotes = get_realtime(all_codes)
    
    # 2. 生成信号
    position_signals = []
    buy_signals = []
    
    for code in all_codes:
        q = quotes.get(code)
        if not q or "error" in q:
            continue
        
        # 获取分钟K线
        bars = get_minute_data(code, period="5")
        indicators = calc_intraday_indicators(bars)
        
        if code in USER_POSITIONS:
            info = USER_PORTFOLIO[code]
            sig = generate_intraday_signal(
                code, q, indicators,
                is_position=True, cost=info["cost"], stop_loss=info["stop_loss"]
            )
            sig["cost"] = info["cost"]
            sig["stop_loss"] = info["stop_loss"]
            sig["pnl_pct"] = round((q["price"] / info["cost"] - 1) * 100, 1)
            sig["shares"] = info["shares"]
            sig["market_value"] = round(q["price"] * info["shares"])
            position_signals.append(sig)
        elif code in ml_codes:
            # 找ML得分
            score = 0
            if ml_path.exists():
                for item in ml_data.get("predictions", []):
                    if item["code"] == code:
                        score = item.get("score", 0)
                        break
            
            sig = generate_intraday_signal(
                code, q, indicators, is_position=False
            )
            sig["ml_score"] = score
            buy_signals.append(sig)
    
    result = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "position_signals": position_signals,
        "buy_signals": buy_signals,
        "total_alerts": sum(len(s.get("alerts", [])) for s in position_signals + buy_signals),
    }
    
    # 保存
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(json.dumps(result, ensure_ascii=False, indent=2))
    
    return result


if __name__ == "__main__":
    result = scan_all()
    print(f"扫描完成 @ {result['timestamp']}")
    print(f"总异动: {result['total_alerts']}")
    
    print("\n=== 持仓信号 ===")
    for s in result["position_signals"]:
        alerts_str = " | ".join(s["alerts"]) if s["alerts"] else "无"
        print(f"  {s['code']} {s['name']} {s['price']:.2f} → {s['signal']} ({s['reason']})")
        print(f"    异动: {alerts_str}")
    
    print("\n=== ML买入信号 ===")
    for s in result["buy_signals"]:
        print(f"  {s['code']} {s['name']} {s['price']:.2f} ML:{s.get('ml_score',0):.4f} → {s['signal']} ({s['reason']})")
