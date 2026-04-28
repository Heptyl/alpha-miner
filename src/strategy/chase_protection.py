"""追高保护模块 — 检测短期涨幅过大，降低推荐权重或排除。

核心逻辑：
1. 计算 N 日累计涨幅
2. 连板数越高，追高风险越大
3. 涨幅超过阈值 → 降权或排除
4. 生成追高风险评级
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Optional

DB_PATH = "data/alpha_miner.db"


@dataclass
class ChaseRisk:
    """追高风险评估。"""

    stock_code: str
    days: int                      # 计算天数
    total_change_pct: float        # 累计涨幅%
    max_change_pct: float          # 单日最大涨幅%
    consecutive_up: int            # 连涨天数
    is_limit_up: bool              # 最近一天是否涨停
    risk_level: str                # low/medium/high/extreme
    score_penalty: float           # 推荐分惩罚 (0~1, 0=不惩罚, 1=完全排除)
    reasons: list[str]

    def to_dict(self) -> dict:
        return {
            "stock_code": self.stock_code,
            "total_change_pct": round(self.total_change_pct, 1),
            "consecutive_up": self.consecutive_up,
            "risk_level": self.risk_level,
            "score_penalty": round(self.score_penalty, 2),
            "reasons": self.reasons,
        }


def compute_chase_risk(
    stock_code: str,
    trade_date: str,
    lookback_days: int = 5,
    db_path: str = DB_PATH,
) -> Optional[ChaseRisk]:
    """计算追高风险。

    Args:
        stock_code: 股票代码
        trade_date: 基准日期 (YYYY-MM-DD)
        lookback_days: 回看天数
        db_path: 数据库路径

    Returns:
        ChaseRisk 或 None（数据不足时）
    """
    conn = sqlite3.connect(db_path)

    # 取最近N天的K线
    rows = conn.execute(
        """SELECT trade_date, open, close, high, low, pre_close
           FROM daily_price
           WHERE stock_code = ? AND trade_date <= ?
           ORDER BY trade_date DESC
           LIMIT ?""",
        (stock_code, trade_date, lookback_days),
    ).fetchall()
    conn.close()

    if len(rows) < 2:
        return None

    # rows 是降序的，反转为升序
    rows = list(reversed(rows))

    # 计算累计涨幅
    first_close = rows[0][2]  # 最早一天的收盘价
    last_close = rows[-1][2]  # 最近一天的收盘价
    total_change = (last_close / first_close - 1) * 100 if first_close > 0 else 0

    # 单日最大涨幅（用 close/pre_close 计算）
    max_change = 0.0
    for r in rows:
        close_val = r[2]
        pre_close_val = r[5]
        if pre_close_val and pre_close_val > 0:
            chg = (close_val / pre_close_val - 1) * 100
        else:
            chg = 0
        max_change = max(max_change, abs(chg))

    # 连涨天数（从最近往回数）
    consecutive_up = 0
    for r in reversed(rows):
        chg = r[5] if r[5] is not None else 0
        if chg > 0:
            consecutive_up += 1
        else:
            break

    # 是否涨停（涨幅 >= 9.9%）
    last_chg = rows[-1][5] if rows[-1][5] is not None else 0
    is_limit_up = last_chg >= 9.9

    # 风险评级
    risk_level, penalty, reasons = _assess_risk(
        total_change, consecutive_up, is_limit_up, lookback_days
    )

    return ChaseRisk(
        stock_code=stock_code,
        days=len(rows),
        total_change_pct=total_change,
        max_change_pct=max_change,
        consecutive_up=consecutive_up,
        is_limit_up=is_limit_up,
        risk_level=risk_level,
        score_penalty=penalty,
        reasons=reasons,
    )


def _assess_risk(
    total_change: float,
    consecutive_up: int,
    is_limit_up: bool,
    lookback: int,
) -> tuple[str, float, list[str]]:
    """评估追高风险。

    Returns:
        (risk_level, score_penalty, reasons)

    阈值设计：
    - 5日涨 0~15%  → low (不惩罚)
    - 5日涨 15~25% → medium (惩罚30%)
    - 5日涨 25~40% → high (惩罚60%)
    - 5日涨 >40%   → extreme (惩罚90%, 几乎排除)
    - 连续3天以上涨停 → 额外惩罚
    """
    reasons = []
    penalty = 0.0

    # 基于累计涨幅的惩罚
    if total_change >= 40:
        risk = "extreme"
        penalty = 0.90
        reasons.append(f"{lookback}日暴涨{total_change:.0f}%，追高风险极大")
    elif total_change >= 25:
        risk = "high"
        penalty = 0.60
        reasons.append(f"{lookback}日涨{total_change:.0f}%，短期涨幅过大")
    elif total_change >= 15:
        risk = "medium"
        penalty = 0.30
        reasons.append(f"{lookback}日涨{total_change:.0f}%，需注意回调风险")
    else:
        risk = "low"
        penalty = 0.0

    # 连涨天数叠加惩罚
    if consecutive_up >= 4:
        penalty = min(penalty + 0.30, 0.95)
        reasons.append(f"连涨{consecutive_up}天，获利盘压力大")
    elif consecutive_up >= 3:
        penalty = min(penalty + 0.15, 0.90)
        reasons.append(f"连涨{consecutive_up}天")

    # 涨停叠加
    if is_limit_up and total_change >= 20:
        penalty = min(penalty + 0.10, 0.95)
        reasons.append("涨停收盘，次日高开低走风险")

    # 如果没触发任何风险
    if not reasons:
        reasons.append("短期涨幅温和，追高风险低")

    return risk, round(penalty, 2), reasons


def batch_chase_risk(
    codes: list[str],
    trade_date: str,
    lookback_days: int = 5,
    db_path: str = DB_PATH,
) -> dict[str, ChaseRisk]:
    """批量计算追高风险。"""
    results = {}
    for code in codes:
        risk = compute_chase_risk(code, trade_date, lookback_days, db_path)
        if risk:
            results[code] = risk
    return results
