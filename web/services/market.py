"""市场数据: 涨停梯队/板块/情绪/炸板 — 盘中实时"""
import sqlite3
from pathlib import Path
from datetime import datetime

import pandas as pd

DB = Path(__file__).parent.parent.parent / "data" / "alpha_miner.db"


def _conn():
    return sqlite3.connect(str(DB))


def get_zt_ladder() -> dict:
    """涨停梯队: {连板数: [股票列表]}"""
    conn = _conn()
    latest = conn.execute("SELECT MAX(trade_date) FROM zt_pool").fetchone()[0]
    df = pd.read_sql(
        "SELECT stock_code,name,consecutive_zt,industry,open_count,amount "
        "FROM zt_pool WHERE trade_date=? ORDER BY consecutive_zt DESC, amount DESC",
        conn, params=(latest,),
    )
    conn.close()

    ladder = {}
    for _, r in df.iterrows():
        n = int(r["consecutive_zt"])
        ladder.setdefault(n, []).append({
            "code": r["stock_code"], "name": r["name"],
            "industry": r["industry"], "open": int(r["open_count"]),
            "amount": r["amount"],
        })
    return ladder, latest


def get_hot_sectors() -> pd.DataFrame:
    """热门板块(按涨停家数)"""
    conn = _conn()
    latest = conn.execute("SELECT MAX(trade_date) FROM zt_pool").fetchone()[0]
    df = pd.read_sql(
        "SELECT industry, COUNT(*) as zt_count, GROUP_CONCAT(name) as names "
        "FROM zt_pool WHERE trade_date=? GROUP BY industry ORDER BY zt_count DESC LIMIT 15",
        conn, params=(latest,),
    )
    conn.close()
    return df


def get_market_breadth() -> dict:
    """市场情绪指标 — 盘中用实时数据，收盘后用DB

    复用 strategy_b.get_market_emotion 的实时数据获取逻辑。
    """
    today = datetime.now().strftime("%Y-%m-%d")
    now_hm = datetime.now().strftime("%H%M")
    is_trading = "0925" <= now_hm <= "1505"

    # ===== 盘中: 用实时数据 =====
    if is_trading:
        try:
            from src.strategy.strategy_b import get_market_emotion
            emo = get_market_emotion(today)
            zt_count = emo.get("zt_count", 0)
            zb_rate = emo.get("zb_rate", 0)
            max_lb = emo.get("max_consecutive", 0)
            phase = emo.get("phase", "未知")
            can_buy = emo.get("can_buy", True)

            # 炸板数从zb_rate反算
            zhab_count = int(zt_count * zb_rate / max(1 - zb_rate, 0.01)) if zb_rate > 0 else 0

            # 涨跌比: 从akshare实时获取
            up_count = emo.get("up_count", 0)
            down_count = emo.get("down_count", 0)

            return {
                "date": today,
                "up": up_count, "down": down_count,
                "total": up_count + down_count,
                "zt": zt_count, "dt": 0,  # 跌停实时暂无
                "zhab": zhab_count,
                "sealed": max(zt_count - zhab_count, 0),
                "up_ratio": round(up_count / max(up_count + down_count, 1) * 100, 1),
                "phase": phase,
                "max_lb": max_lb,
                "can_buy": can_buy,
                "is_realtime": True,
            }
        except Exception as e:
            pass  # fallback到DB

    # ===== 收盘后/历史: 用DB =====
    conn = _conn()

    # 涨停数据
    zt_latest = conn.execute("SELECT MAX(trade_date) FROM zt_pool").fetchone()[0]
    zt_count = conn.execute(
        "SELECT COUNT(*) FROM zt_pool WHERE trade_date=?", (zt_latest,)
    ).fetchone()[0]
    zhab_count = conn.execute(
        "SELECT COUNT(*) FROM zt_pool WHERE trade_date=? AND open_count > 0",
        (zt_latest,),
    ).fetchone()[0]
    sealed_count = zt_count - zhab_count

    # 涨跌比
    dp_latest = conn.execute("SELECT MAX(trade_date) FROM daily_price").fetchone()[0]
    df = pd.read_sql(
        "SELECT close, pre_close FROM daily_price "
        "WHERE trade_date=? AND close IS NOT NULL AND pre_close IS NOT NULL AND pre_close > 0",
        conn, params=(dp_latest,),
    )
    conn.close()

    up = int((df["close"] > df["pre_close"]).sum()) if not df.empty else 0
    down = int((df["close"] < df["pre_close"]).sum()) if not df.empty else 0
    total = len(df) if not df.empty else 1

    # 跌停
    chg_pct = (df["close"] - df["pre_close"]) / df["pre_close"] * 100 if not df.empty else pd.Series()
    dt_count = int((chg_pct <= -9.9).sum()) if not df.empty else 0

    # 最高连板
    conn = _conn()
    max_lb = conn.execute(
        "SELECT MAX(consecutive_zt) FROM zt_pool WHERE trade_date=?", (zt_latest,)
    ).fetchone()[0] or 0
    conn.close()

    use_date = max(dp_latest, zt_latest)

    return {
        "date": use_date,
        "up": up, "down": down, "total": total,
        "zt": zt_count, "dt": dt_count,
        "zhab": zhab_count, "sealed": sealed_count,
        "up_ratio": round(up / total * 100, 1),
        "phase": "",
        "max_lb": max_lb,
        "can_buy": True,
        "is_realtime": False,
    }


def get_strong_count_history(days=15) -> pd.DataFrame:
    """强势股数量趋势"""
    conn = _conn()
    df = pd.read_sql(
        "SELECT trade_date, COUNT(*) as cnt FROM strong_pool "
        "GROUP BY trade_date ORDER BY trade_date DESC LIMIT ?",
        conn, params=(days,),
    )
    conn.close()
    return df.sort_values("trade_date") if not df.empty else df


def get_zt_count_history(days=15) -> pd.DataFrame:
    """涨停数量趋势"""
    conn = _conn()
    df = pd.read_sql(
        "SELECT trade_date, COUNT(*) as cnt FROM zt_pool "
        "GROUP BY trade_date ORDER BY trade_date DESC LIMIT ?",
        conn, params=(days,),
    )
    conn.close()
    return df.sort_values("trade_date") if not df.empty else df


def get_zhab_count_history(days=15) -> pd.DataFrame:
    """炸板数量趋势"""
    conn = _conn()
    df = pd.read_sql(
        "SELECT trade_date, COUNT(*) as cnt FROM zt_pool "
        "WHERE open_count > 0 "
        "GROUP BY trade_date ORDER BY trade_date DESC LIMIT ?",
        conn, params=(days,),
    )
    conn.close()
    return df.sort_values("trade_date") if not df.empty else df


def get_zt_by_board() -> dict:
    """按板块分组的涨停数据"""
    conn = _conn()
    latest = conn.execute("SELECT MAX(trade_date) FROM zt_pool").fetchone()[0]
    df = pd.read_sql(
        "SELECT stock_code, name, consecutive_zt, industry, open_count, amount "
        "FROM zt_pool WHERE trade_date=? ORDER BY consecutive_zt DESC, amount DESC",
        conn, params=(latest,),
    )
    conn.close()

    boards = {"主板": [], "科创板": [], "北交所": []}
    for _, r in df.iterrows():
        code = r["stock_code"]
        item = {
            "code": code, "name": r["name"], "industry": r["industry"],
            "board": int(r["consecutive_zt"]), "open": int(r["open_count"]),
            "amount": r["amount"],
        }
        if code.startswith(("688", "689")):
            boards["科创板"].append(item)
        elif len(code) == 6 and (code[0] in ("8", "9")):
            boards["北交所"].append(item)
        else:
            boards["主板"].append(item)

    return boards, latest
