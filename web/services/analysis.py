"""因子分析 + 交易计划 + K线"""
import sqlite3
from pathlib import Path

import pandas as pd

DB = Path(__file__).parent.parent.parent / "data" / "alpha_miner.db"

# 因子权重 (IC加权)
WEIGHTS = {
    "theme_crowding":    {"w": 0.40, "dir": 1},
    "leader_clarity":    {"w": 0.25, "dir": 1},
    "consecutive_board": {"w": 0.15, "dir": 1},
    "lhb_institution":   {"w": 0.10, "dir": 1},
    "turnover_rank":     {"w": 0.10, "dir": -1},
}
EXCLUDE = ("688", "689", "8", "9")

# 板块分类前缀
BOARD_MAIN = ("000", "001", "002", "003", "300", "301", "600", "601", "603", "605")
BOARD_KCB = ("688", "689")
BOARD_BJS = ("8", "9")  # 6位且8/9开头


def _conn():
    return sqlite3.connect(str(DB))


def score_row(row) -> float:
    s = 0.0
    for f, c in WEIGHTS.items():
        v = row.get(f)
        if v is not None and pd.notna(v):
            s += v * c["w"] * c["dir"]
    return round(s, 4)


def get_strong_with_factors(top_n=80) -> pd.DataFrame:
    """强势股 + 因子 + 涨停信息"""
    conn = _conn()
    latest = conn.execute("SELECT MAX(trade_date) FROM strong_pool").fetchone()[0]

    sp = pd.read_sql(
        "SELECT stock_code,name,industry,amount,reason FROM strong_pool WHERE trade_date=?",
        conn, params=(latest,),
    )
    if sp.empty:
        conn.close()
        return sp

    codes = sp["stock_code"].tolist()
    ph = ",".join(["?"] * len(codes))

    fv = pd.read_sql(
        f"SELECT stock_code,factor_name,factor_value FROM factor_values "
        f"WHERE trade_date=? AND stock_code IN ({ph})",
        conn, params=[latest] + codes,
    )
    if not fv.empty:
        piv = fv.pivot(index="stock_code", columns="factor_name", values="factor_value").reset_index()
        sp = sp.merge(piv, on="stock_code", how="left")

    zt = pd.read_sql(
        "SELECT stock_code,consecutive_zt,open_count FROM zt_pool WHERE trade_date=?",
        conn, params=(latest,),
    )
    if not zt.empty:
        sp = sp.merge(zt, on="stock_code", how="left")

    conn.close()
    sp["trade_date"] = latest

    # 排除科创板/北交所
    sp = sp[~sp["stock_code"].apply(lambda c: any(c.startswith(p) for p in EXCLUDE))].copy()
    sp["score"] = sp.apply(score_row, axis=1)
    sp = sp[sp["score"] > 0].sort_values("score", ascending=False).reset_index(drop=True)
    return sp.head(top_n)


def get_strong_with_board(board: str = "all", top_n=80) -> pd.DataFrame:
    """强势股 + 因子，按板块筛选。board: 'main'/'kcb'/'bjs'/'all'"""
    conn = _conn()
    latest = conn.execute("SELECT MAX(trade_date) FROM strong_pool").fetchone()[0]

    sp = pd.read_sql(
        "SELECT stock_code,name,industry,amount,reason FROM strong_pool WHERE trade_date=?",
        conn, params=(latest,),
    )
    if sp.empty:
        conn.close()
        return sp

    codes = sp["stock_code"].tolist()
    ph = ",".join(["?"] * len(codes))

    fv = pd.read_sql(
        f"SELECT stock_code,factor_name,factor_value FROM factor_values "
        f"WHERE trade_date=? AND stock_code IN ({ph})",
        conn, params=[latest] + codes,
    )
    if not fv.empty:
        piv = fv.pivot(index="stock_code", columns="factor_name", values="factor_value").reset_index()
        sp = sp.merge(piv, on="stock_code", how="left")

    zt = pd.read_sql(
        "SELECT stock_code,consecutive_zt,open_count FROM zt_pool WHERE trade_date=?",
        conn, params=(latest,),
    )
    if not zt.empty:
        sp = sp.merge(zt, on="stock_code", how="left")

    conn.close()
    sp["trade_date"] = latest

    # 按板块筛选
    def _classify(code):
        if code.startswith(BOARD_KCB):
            return "kcb"
        elif len(code) == 6 and code[0] in BOARD_BJS:
            return "bjs"
        else:
            return "main"

    sp["_board"] = sp["stock_code"].apply(_classify)
    if board != "all":
        sp = sp[sp["_board"] == board].copy()
    sp = sp.drop(columns=["_board"])

    sp["score"] = sp.apply(score_row, axis=1)
    sp = sp[sp["score"] > 0].sort_values("score", ascending=False).reset_index(drop=True)
    return sp.head(top_n)


def get_kline(code: str, days: int = 20) -> pd.DataFrame:
    conn = _conn()
    df = pd.read_sql(
        "SELECT trade_date,open,high,low,close,amount,pre_close "
        "FROM daily_price WHERE stock_code=? ORDER BY trade_date DESC LIMIT ?",
        conn, params=(code, days),
    )
    conn.close()
    if df.empty:
        return df
    df = df.sort_values("trade_date").reset_index(drop=True)
    df["pct"] = ((df["close"] - df["pre_close"]) / df["pre_close"] * 100).round(2)
    return df


def get_factor_history(code: str, days: int = 30) -> pd.DataFrame:
    conn = _conn()
    df = pd.read_sql(
        "SELECT trade_date,factor_name,factor_value FROM factor_values "
        "WHERE stock_code=? ORDER BY trade_date DESC LIMIT ?",
        conn, params=(code, days * 10),
    )
    conn.close()
    return df.sort_values("trade_date").reset_index(drop=True) if not df.empty else df


def get_ic_series() -> pd.DataFrame:
    conn = _conn()
    df = pd.read_sql(
        "SELECT factor_name,trade_date,ic_value FROM ic_series ORDER BY trade_date",
        conn,
    )
    conn.close()
    return df


def review_plan(plan: dict) -> list[dict]:
    """对 tradeplan 做盘中/盘后复盘，返回每只票的实绩"""
    conn = _conn()
    results = []
    target_date = plan.get("target_date", "")

    all_stocks = plan.get("top", []) + plan.get("backup", [])
    labels = ["主选"] * len(plan.get("top", [])) + ["备选"] * len(plan.get("backup", []))

    for label, s in zip(labels, all_stocks):
        code = s["code"]
        entry = s.get("entry_target", 0)
        stop = s.get("stop_price", 0)
        target = s.get("target_price", 0)
        entry_max = s.get("entry_max", 0)

        r = conn.execute(
            "SELECT open, high, low, close, pre_close, amount FROM daily_price "
            "WHERE stock_code=? AND trade_date=?",
            (code, target_date),
        ).fetchone()

        if not r:
            # DB无当日数据，尝试最近一天
            r = conn.execute(
                "SELECT open, high, low, close, pre_close, amount FROM daily_price "
                "WHERE stock_code=? ORDER BY trade_date DESC LIMIT 1",
                (code,),
            ).fetchone()

        res = {
            "label": label,
            "code": code,
            "name": s.get("name", ""),
            "industry": s.get("industry", ""),
            "entry": entry,
            "stop": stop,
            "target": target,
            "entry_max": entry_max,
            "shares": s.get("shares", 0),
            "cost": s.get("cost", 0),
            "risk_amt": s.get("risk_amt", 0),
            "profit_amt": s.get("profit_amt", 0),
        }

        if r:
            o, h, l, c, pre, amt = r
            pct = (c - pre) / pre * 100 if pre else 0
            touched = l <= entry * 1.003 if entry > 0 else False
            hit_target = c >= target if target > 0 else False
            hit_stop = l <= stop if stop > 0 else False
            pnl = (c - entry) / entry * 100 if entry > 0 else 0
            risk = entry - stop
            reward = target - entry
            rr = reward / risk if risk > 0 else 0

            res.update({
                "has_data": True,
                "open": o, "high": h, "low": l, "close": c,
                "pre_close": pre, "pct": round(pct, 2),
                "amount": amt or 0,
                "touched_entry": touched,
                "hit_target": hit_target,
                "hit_stop": hit_stop,
                "pnl_pct": round(pnl, 2) if touched else None,
                "pnl_pct_open": round((c - o) / o * 100, 2) if o > 0 else 0,
                "rr": round(rr, 1),
            })
        else:
            res["has_data"] = False

        results.append(res)

    conn.close()
    return results


def make_plan(row, quote, capital: float, stop_pct: float, target_pct: float) -> dict:
    """生成单只交易计划"""
    entry = quote.price
    stop = round(entry * (1 - stop_pct / 100), 2)
    target = round(entry * (1 + target_pct / 100), 2)
    shares = max(int(capital / 2 / entry / 100) * 100, 100)
    cost = shares * entry
    risk = shares * (entry - stop)
    profit = shares * (target - entry)
    return {
        "code": row["stock_code"],
        "name": quote.name or row["name"],
        "industry": row.get("industry", ""),
        "price": entry, "pct": round(quote.pct, 2),
        "score": row["score"],
        "entry": entry, "stop": stop, "target": target,
        "shares": shares, "cost": round(cost),
        "risk": round(risk), "profit": round(profit),
        "reason": row.get("reason", ""),
        **{k: row.get(k) for k in WEIGHTS},
        "board": row.get("consecutive_zt", 0),
    }
