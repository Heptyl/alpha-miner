"""预演三问过滤模块 — 买入前的最后一道关

三问:
  1. 主线: 这只票属于当日市场主线板块吗? (涨停最多的前5个板块)
  2. 节点: 处于上升趋势的什么位置? (均线多头/突破/加速/高位)
  3. 辨识度: 在板块内是不是核心标的? (涨停龙头/资金关注/连板股)

评分:
  每问0-3分, 总分0-9分
  >= 7分: 优先买入
  5-6分: 可以买入
  3-4分: 观望
  <= 2分: 不买

数据来源: zt_pool / concept_daily / concept_mapping / daily_price / strong_pool
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Optional

import numpy as np

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
import sys
sys.path.insert(0, str(PROJECT_ROOT))

DB_PATH = PROJECT_ROOT / "data" / "alpha_miner.db"


@dataclass
class ThreeQuestionsResult:
    """三问过滤结果"""
    code: str
    name: str
    # 各项得分(0-3)
    main_line_score: int      # 主线得分
    node_score: int           # 节点得分
    identity_score: int       # 辨识度得分
    # 总分
    total_score: int          # 总分 0-9
    # 详细
    main_line_concepts: list  # 所属板块
    hot_concepts: list        # 当日热门板块
    is_leader: bool           # 是否板块龙头
    ma_status: str            # 均线状态
    # 建议
    passed: bool              # 是否通过(>=5分)
    advice: str               # 建议


def _get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


def get_hot_concepts(target_date: str, top_n: int = 5) -> list[dict]:
    """获取当日最热门板块(按涨停数排序)

    Returns:
        [{"concept_name": str, "zt_count": int, "leader_code": str}, ...]
    """
    conn = _get_conn()
    try:
        rows = conn.execute("""
            SELECT concept_name, zt_count, leader_code, leader_consecutive
            FROM concept_daily
            WHERE trade_date = ?
            ORDER BY zt_count DESC LIMIT ?
        """, (target_date, top_n)).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def get_stock_concepts(code: str) -> list[str]:
    """获取个股所属板块"""
    conn = _get_conn()
    try:
        rows = conn.execute(
            "SELECT DISTINCT concept_name FROM concept_mapping WHERE stock_code = ?",
            (code,)
        ).fetchall()
        return [r["concept_name"] for r in rows]
    finally:
        conn.close()


def _score_main_line(code: str, target_date: str) -> tuple[int, list, list]:
    """第一问: 主线判断

    得分规则:
      3分: 属于当日涨停最多的第1-2名板块
      2分: 属于当日涨停最多的第3-5名板块
      1分: 有板块归属, 但不是热门
      0分: 无板块数据
    """
    hot_concepts = get_hot_concepts(target_date, top_n=5)
    stock_concepts = get_stock_concepts(code)

    if not hot_concepts or not stock_concepts:
        return 0, stock_concepts, hot_concepts

    hot_names = [c["concept_name"] for c in hot_concepts]
    overlap = set(stock_concepts) & set(hot_names)

    if overlap:
        # 找到重叠中排名最高的
        for i, hc in enumerate(hot_names):
            if hc in overlap:
                if i < 2:
                    return 3, stock_concepts, hot_concepts
                else:
                    return 2, stock_concepts, hot_concepts

    # 有板块但不在热门中
    if stock_concepts:
        return 1, stock_concepts, hot_concepts

    return 0, stock_concepts, hot_concepts


def _score_node(code: str, target_date: str) -> tuple[int, str]:
    """第二问: 节点判断(基于均线和趋势)

    得分规则:
      3分: 均线多头排列 + 突破(收盘>前高)或放量
      2分: 站上5/10日均线, 趋势向上
      1分: 站上某条均线, 方向不明
      0分: 均线下方或数据不足
    """
    conn = _get_conn()
    try:
        # 取最近20天行情
        rows = conn.execute("""
            SELECT trade_date, open, high, low, close, volume, amount
            FROM daily_price
            WHERE stock_code = ? AND trade_date <= ?
            ORDER BY trade_date DESC LIMIT 20
        """, (code, target_date)).fetchall()

        if len(rows) < 10:
            return 0, "数据不足"

        # 反转为时间正序
        closes = np.array([float(r["close"]) for r in reversed(rows)])
        volumes = np.array([float(r["volume"]) for r in reversed(rows)])

        # 计算均线
        ma5 = np.mean(closes[-5:])
        ma10 = np.mean(closes[-10:]) if len(closes) >= 10 else closes[-1]
        ma20 = np.mean(closes[-min(20, len(closes)):])
        cur = closes[-1]

        # 均线多头: MA5 > MA10 > MA20
        bullish = ma5 > ma10 > ma20
        above_ma5 = cur > ma5
        above_ma10 = cur > ma10

        # 放量判断(今日成交量 > 5日均量1.5倍)
        avg_vol = np.mean(volumes[-5:])
        vol_surge = volumes[-1] > avg_vol * 1.5

        # 突破判断(收盘价创10日新高)
        high_10 = np.max(closes[-10:-1]) if len(closes) >= 10 else closes[-1]
        breakout = cur > high_10

        if bullish and (breakout or vol_surge):
            return 3, "多头+突破" if breakout else "多头+放量"
        elif above_ma5 and above_ma10 and ma5 > ma10:
            return 2, "站上均线,趋势向上"
        elif above_ma5 or above_ma10:
            return 1, "站上部分均线"
        else:
            return 0, "均线下方"

    except Exception:
        return 0, "计算异常"
    finally:
        conn.close()


def _score_identity(code: str, target_date: str) -> tuple[int, bool]:
    """第三问: 辨识度(是否板块龙头/核心标的)

    得分规则:
      3分: 板块龙头(concept_daily的leader_code) 或 涨停池连板>=3
      2分: 涨停(首板) 或 强势股池上榜
      1分: 有资金流入(fund_flow有记录)
      0分: 无特征
    """
    conn = _get_conn()
    try:
        is_leader = False

        # 检查是否板块龙头
        leader_rows = conn.execute("""
            SELECT concept_name FROM concept_daily
            WHERE trade_date = ? AND leader_code = ?
        """, (target_date, code)).fetchall()
        if leader_rows:
            is_leader = True
            return 3, is_leader

        # 检查涨停池(连板高度)
        zt_row = conn.execute("""
            SELECT consecutive_zt FROM zt_pool
            WHERE stock_code = ? AND trade_date = ?
        """, (code, target_date)).fetchone()
        if zt_row and zt_row["consecutive_zt"] >= 3:
            return 3, False
        if zt_row and zt_row["consecutive_zt"] >= 1:
            return 2, False

        # 检查强势股池
        strong_row = conn.execute("""
            SELECT 1 FROM strong_pool
            WHERE stock_code = ? AND trade_date = ?
        """, (code, target_date)).fetchone()
        if strong_row:
            return 2, is_leader

        # 检查资金流入
        fund_row = conn.execute("""
            SELECT 1 FROM fund_flow
            WHERE stock_code = ? AND trade_date = ?
            LIMIT 1
        """, (code, target_date)).fetchone()
        if fund_row:
            return 1, False

        return 0, False

    except Exception:
        return 0, False
    finally:
        conn.close()


def three_questions_filter(
    code: str,
    target_date: str,
    name: str = "",
    min_score: int = 5,
) -> ThreeQuestionsResult:
    """执行预演三问过滤

    Args:
        code: 股票代码
        target_date: 日期
        name: 股票名称
        min_score: 最低通过分数(默认5分)

    Returns:
        ThreeQuestionsResult
    """
    # 第一问: 主线
    main_score, concepts, hot = _score_main_line(code, target_date)

    # 第二问: 节点
    node_score, ma_status = _score_node(code, target_date)

    # 第三问: 辨识度
    identity_score, is_leader = _score_identity(code, target_date)

    total = main_score + node_score + identity_score
    passed = total >= min_score

    # 生成建议
    if total >= 7:
        advice = "优质标的, 优先买入"
    elif total >= 5:
        advice = "可以买入"
    elif total >= 3:
        advice = "偏弱, 观望"
    else:
        advice = "不建议"

    return ThreeQuestionsResult(
        code=code,
        name=name,
        main_line_score=main_score,
        node_score=node_score,
        identity_score=identity_score,
        total_score=total,
        main_line_concepts=concepts,
        hot_concepts=hot,
        is_leader=is_leader,
        ma_status=ma_status,
        passed=passed,
        advice=advice,
    )


def filter_stock_list(
    stocks: list[dict],
    target_date: str,
    min_score: int = 5,
) -> list[dict]:
    """批量过滤股票列表, 返回通过三问的股票

    Args:
        stocks: [{"code": "000001", "name": "平安银行", "score": 0.85}, ...]
        target_date: 日期
        min_score: 最低分

    Returns:
        通过三问的股票列表, 按总分+ML得分排序
    """
    results = []
    for s in stocks:
        code = s.get("code", "")
        name = s.get("name", "")
        r = three_questions_filter(code, target_date, name, min_score)
        if r.passed:
            s_copy = dict(s)
            s_copy["three_q_score"] = r.total_score
            s_copy["three_q_detail"] = {
                "main": r.main_line_score,
                "node": r.node_score,
                "identity": r.identity_score,
                "ma_status": r.ma_status,
                "is_leader": r.is_leader,
            }
            results.append(s_copy)

    # 按三问总分(降序) + ML score(降序) 排序
    results.sort(key=lambda x: (x["three_q_score"], x.get("score", 0)), reverse=True)
    return results


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("用法: python -m src.trader.three_questions <stock_code> [date]")
        sys.exit(1)

    code = sys.argv[1]
    dt = sys.argv[2] if len(sys.argv) > 2 else None

    if not dt:
        conn = _get_conn()
        row = conn.execute("SELECT MAX(trade_date) FROM daily_price").fetchone()
        dt = row[0]
        conn.close()

    r = three_questions_filter(code, dt)
    print(f"=== 预演三问 [{r.code}] {dt} ===")
    print(f"主线: {r.main_line_score}/3  板块={r.main_line_concepts}")
    print(f"节点: {r.node_score}/3  {r.ma_status}")
    print(f"辨识: {r.identity_score}/3  龙头={'是' if r.is_leader else '否'}")
    print(f"总分: {r.total_score}/9  {'通过' if r.passed else '不通过'}")
    print(f"热门板块: {[c['concept_name'] for c in r.hot_concepts[:5]]}")
    print(f"建议: {r.advice}")
