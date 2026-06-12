"""Shadow-only C strategy hypotheses.

C1: abnormal-volume attention momentum.
C2: panic-volume reversal.

Both strategies only generate candidates. The daemon records them as shadow
signals and does not place paper trades unless their run_mode is promoted.
"""

from __future__ import annotations

import logging
import sqlite3
from collections import defaultdict
from pathlib import Path


logger = logging.getLogger(__name__)
DB_PATH = Path(__file__).resolve().parents[2] / "data" / "alpha_miner.db"


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro&immutable=1", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _latest_dates(conn: sqlite3.Connection, n: int = 80) -> list[str]:
    rows = conn.execute(
        """
        SELECT trade_date
        FROM daily_price
        GROUP BY trade_date
        HAVING COUNT(DISTINCT stock_code) >= 1000
        ORDER BY trade_date DESC
        LIMIT ?
        """,
        (n,),
    ).fetchall()
    return [r["trade_date"] for r in reversed(rows)]


def _name_map(conn: sqlite3.Connection) -> dict[str, str]:
    result: dict[str, str] = {}
    for row in conn.execute(
        """
        SELECT stock_code, MAX(name) AS name
        FROM zt_pool
        WHERE name IS NOT NULL AND name!=''
        GROUP BY stock_code
        """
    ).fetchall():
        result[row["stock_code"]] = row["name"]
    return result


def _industry_map(conn: sqlite3.Connection) -> dict[str, str]:
    rows = conn.execute(
        """
        SELECT stock_code, MAX(industry_name) AS industry
        FROM stock_industry_mapping
        GROUP BY stock_code
        """
    ).fetchall()
    return {r["stock_code"]: r["industry"] or "" for r in rows}


def _load_recent_data(conn: sqlite3.Connection, dates: list[str]) -> dict[str, list[sqlite3.Row]]:
    if not dates:
        return {}
    placeholders = ",".join("?" for _ in dates)
    rows = conn.execute(
        f"""
        SELECT stock_code, trade_date, open, high, low, close, pre_close,
               volume, amount, turnover_rate
        FROM daily_price
        WHERE trade_date IN ({placeholders})
          AND close IS NOT NULL AND close > 0
          AND volume IS NOT NULL AND volume > 0
        ORDER BY stock_code, trade_date
        """,
        dates,
    ).fetchall()
    grouped: dict[str, list[sqlite3.Row]] = defaultdict(list)
    for row in rows:
        grouped[row["stock_code"]].append(row)
    return grouped


def _avg(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _skip_code(code: str, name: str) -> bool:
    if code.startswith(("688", "689", "8", "4")):
        return True
    return "ST" in name or "退" in name


def _features(rows: list[sqlite3.Row]) -> dict | None:
    if len(rows) < 61:
        return None
    today = rows[-1]
    close = float(today["close"] or 0)
    pre_close = float(today["pre_close"] or 0)
    if close <= 0 or pre_close <= 0:
        return None

    closes = [float(r["close"]) for r in rows]
    volumes = [float(r["volume"] or 0) for r in rows]
    amounts = [float(r["amount"] or 0) for r in rows]
    if len(closes) < 61 or any(v <= 0 for v in volumes[-21:]):
        return None

    ma20 = _avg(closes[-20:])
    ma60 = _avg(closes[-60:])
    avg_vol20 = _avg(volumes[-21:-1])
    avg_amt20 = _avg(amounts[-21:-1])
    if ma20 <= 0 or ma60 <= 0 or avg_vol20 <= 0:
        return None

    day_ret = (close / pre_close - 1) * 100
    ret20 = (close / closes[-21] - 1) * 100 if closes[-21] > 0 else 0
    ret60 = (close / closes[-61] - 1) * 100 if closes[-61] > 0 else 0
    ma20_dist = (close / ma20 - 1) * 100
    ma60_dist = (close / ma60 - 1) * 100
    amount_ratio20 = float(today["amount"] or 0) / avg_amt20 if avg_amt20 > 0 else 0
    volume_ratio20 = float(today["volume"] or 0) / avg_vol20
    last3_big_up = sum(
        1 for i in range(1, 4)
        if closes[-i - 1] > 0 and (closes[-i] / closes[-i - 1] - 1) * 100 > 3
    )

    return {
        "trade_date": today["trade_date"],
        "close": close,
        "amount": float(today["amount"] or 0),
        "turnover": float(today["turnover_rate"] or 0),
        "day_ret": day_ret,
        "ret20": ret20,
        "ret60": ret60,
        "ma20_dist": ma20_dist,
        "ma60_dist": ma60_dist,
        "amount_ratio20": amount_ratio20,
        "volume_ratio20": volume_ratio20,
        "last3_big_up": last3_big_up,
    }


def get_c1_attention_momentum_candidates(top_n: int = 8) -> list[dict]:
    """C1: abnormal volume + moderate strength, shadow-only."""
    conn = _connect()
    try:
        dates = _latest_dates(conn, 80)
        names = _name_map(conn)
        industries = _industry_map(conn)
        data = _load_recent_data(conn, dates)
    finally:
        conn.close()

    candidates: list[dict] = []
    for code, rows in data.items():
        name = names.get(code, code)
        if _skip_code(code, name):
            continue
        f = _features(rows)
        if not f:
            continue
        if f["trade_date"] != dates[-1]:
            continue
        if not (2.0 <= f["day_ret"] <= 7.0):
            continue
        if f["amount_ratio20"] < 2.5:
            continue
        if f["amount"] < 80_000_000:
            continue
        if not (-5.0 <= f["ma20_dist"] <= 10.0):
            continue
        if not (-10.0 <= f["ret20"] <= 25.0):
            continue
        if not (3.0 <= f["turnover"] <= 20.0):
            continue
        if f["last3_big_up"] >= 3:
            continue

        score = (
            min(f["amount_ratio20"], 8) * 12
            + max(f["day_ret"], 0) * 4
            + max(0, 10 - abs(f["ma20_dist"])) * 2
        )
        candidates.append({
            "code": code,
            "name": name,
            "industry": industries.get(code, ""),
            "score": round(score, 2),
            "signal_type": "关注度动量(C1)",
            "reason": (
                f"放量{f['amount_ratio20']:.1f}x 涨{f['day_ret']:+.1f}% "
                f"MA20距{f['ma20_dist']:+.1f}% 20日{f['ret20']:+.1f}%"
            ),
            "_strategy": "C1",
            "_trade_date": f["trade_date"],
            "_close": f["close"],
            "_day_ret": round(f["day_ret"], 2),
            "_amount_ratio20": round(f["amount_ratio20"], 2),
            "_ma20_dist": round(f["ma20_dist"], 2),
            "_ret20": round(f["ret20"], 2),
            "_turnover": round(f["turnover"], 2),
        })

    candidates.sort(key=lambda x: x["score"], reverse=True)
    logger.info(f"[C1 shadow] 候选{len(candidates)}只")
    return candidates[:top_n]


def get_c2_panic_reversal_candidates(top_n: int = 8) -> list[dict]:
    """C2: oversold + volume after panic, shadow-only."""
    conn = _connect()
    try:
        dates = _latest_dates(conn, 80)
        names = _name_map(conn)
        industries = _industry_map(conn)
        data = _load_recent_data(conn, dates)
    finally:
        conn.close()

    candidates: list[dict] = []
    for code, rows in data.items():
        name = names.get(code, code)
        if _skip_code(code, name):
            continue
        f = _features(rows)
        if not f:
            continue
        if f["trade_date"] != dates[-1]:
            continue
        if f["close"] < 3 or f["close"] > 90:
            continue
        if f["amount"] < 80_000_000:
            continue
        if f["ret20"] > -15:
            continue
        if f["ma20_dist"] > -10:
            continue
        if f["amount_ratio20"] < 1.5:
            continue
        if f["day_ret"] > -2:
            continue

        score = (
            abs(f["ret20"]) * 2
            + abs(f["ma20_dist"]) * 1.5
            + min(f["amount_ratio20"], 6) * 10
        )
        candidates.append({
            "code": code,
            "name": name,
            "industry": industries.get(code, ""),
            "score": round(score, 2),
            "signal_type": "恐慌反转(C2)",
            "reason": (
                f"20日{f['ret20']:+.1f}% MA20距{f['ma20_dist']:+.1f}% "
                f"放量{f['amount_ratio20']:.1f}x 当日{f['day_ret']:+.1f}%"
            ),
            "_strategy": "C2",
            "_trade_date": f["trade_date"],
            "_close": f["close"],
            "_day_ret": round(f["day_ret"], 2),
            "_amount_ratio20": round(f["amount_ratio20"], 2),
            "_ma20_dist": round(f["ma20_dist"], 2),
            "_ret20": round(f["ret20"], 2),
            "_turnover": round(f["turnover"], 2),
        })

    candidates.sort(key=lambda x: x["score"], reverse=True)
    logger.info(f"[C2 shadow] 候选{len(candidates)}只")
    return candidates[:top_n]
