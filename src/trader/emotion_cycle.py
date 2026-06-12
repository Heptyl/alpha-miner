"""市场情绪周期判断模块 — 基于92科比四阶段法

四阶段:
  冰期期 (ICE)     — 涨停<30, 连板高度<=2, 炸板率>30%, 跌多涨少
  复苏期 (RECOVER) — 涨停30-60, 连板高度3-4, 炸板率15-25%, 赚钱效应初现
  高潮期 (CLIMAX)  — 涨停>80, 连板高度>=5, 炸板率<15%, 全面赚钱
  退潮期 (EBB)     — 涨停从高位回落, 连板高度下降, 炸板率上升, 亏钱效应

策略适配:
  ICE    → 不开新仓(或极小仓位试错)
  RECOVER→ 可以开仓, 选龙头
  CLIMAX → 持仓为主, 注意移动止盈
  EBB    → 减仓/清仓, 严守止损

数据来源: zt_pool / zb_pool / market_emotion 表
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import date, timedelta
from enum import Enum
from pathlib import Path
from typing import Optional

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DB_PATH = PROJECT_ROOT / "data" / "alpha_miner.db"


class EmotionPhase(str, Enum):
    ICE = "冰点"
    RECOVER = "复苏"
    CLIMAX = "高潮"
    EBB = "退潮"


@dataclass
class EmotionState:
    """某一天的市场情绪状态"""
    trade_date: str
    phase: EmotionPhase
    zt_count: int           # 涨停数
    zb_count: int           # 炸板数
    zb_rate: float          # 炸板率
    highest_board: int      # 最高连板
    multi_board_count: int  # 连板股数(>=2板)
    up_count: int = 0       # 上涨家数
    down_count: int = 0     # 下跌家数
    score: float = 0.0      # 综合情绪分 0-100
    advice: str = ""        # 操作建议

    @property
    def can_open(self) -> bool:
        """当前阶段是否适合开新仓"""
        return self.phase in (EmotionPhase.RECOVER, EmotionPhase.CLIMAX)

    @property
    def position_ratio(self) -> float:
        """建议仓位比例"""
        ratios = {
            EmotionPhase.ICE: 0.0,
            EmotionPhase.RECOVER: 0.5,
            EmotionPhase.CLIMAX: 1.0,
            EmotionPhase.EBB: 0.3,
        }
        return ratios.get(self.phase, 0.5)


def _get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


def get_emotion_state(target_date: Optional[str] = None) -> EmotionState:
    """获取指定日期的市场情绪状态

    Args:
        target_date: 目标日期, 默认取数据库最新交易日

    Returns:
        EmotionState 对象
    """
    conn = _get_conn()
    try:
        if target_date is None:
            row = conn.execute(
                "SELECT MAX(trade_date) FROM zt_pool"
            ).fetchone()
            target_date = row[0] if row else None

        if not target_date:
            return _default_state(str(date.today()))

        # --- 涨停数据 ---
        zt_row = conn.execute("""
            SELECT COUNT(*) as zt_count,
                   SUM(CASE WHEN consecutive_zt >= 2 THEN 1 ELSE 0 END) as multi,
                   MAX(consecutive_zt) as highest_board
            FROM zt_pool WHERE trade_date = ?
        """, (target_date,)).fetchone()

        zt_count = zt_row["zt_count"] or 0
        multi_board_count = zt_row["multi"] or 0
        highest_board = zt_row["highest_board"] or 0

        # --- 炸板数据 ---
        zb_row = conn.execute("""
            SELECT COUNT(*) as zb_count FROM zb_pool WHERE trade_date = ?
        """, (target_date,)).fetchone()
        zb_count = zb_row["zb_count"] or 0

        # 炸板率 = 炸板 / (涨停+炸板)
        total_attacks = zt_count + zb_count
        zb_rate = zb_count / total_attacks if total_attacks > 0 else 0

        # --- 涨跌家数(从market_emotion取) ---
        up_count = 0
        down_count = 0
        me_row = conn.execute(
            "SELECT up_count, down_count FROM market_emotion WHERE trade_date = ?",
            (target_date,)
        ).fetchone()
        if me_row:
            up_count = me_row["up_count"] or 0
            down_count = me_row["down_count"] or 0

        # --- 趋势判断(需要前3天数据对比) ---
        phase = _classify_phase(
            target_date, conn, zt_count, zb_rate, highest_board, up_count, down_count
        )

        # --- 综合评分(0-100) ---
        score = _calc_score(zt_count, zb_rate, highest_board, up_count, down_count)

        # --- 操作建议 ---
        advice = _get_advice(phase, score)

        return EmotionState(
            trade_date=target_date,
            phase=phase,
            zt_count=zt_count,
            zb_count=zb_count,
            zb_rate=round(zb_rate, 3),
            highest_board=highest_board,
            multi_board_count=multi_board_count,
            up_count=up_count,
            down_count=down_count,
            score=round(score, 1),
            advice=advice,
        )
    finally:
        conn.close()


def _classify_phase(
    target_date: str,
    conn: sqlite3.Connection,
    zt_count: int,
    zb_rate: float,
    highest_board: int,
    up_count: int,
    down_count: int,
) -> EmotionPhase:
    """基于多维指标判断情绪阶段

    核心逻辑:
      - 冰点: 涨停<30, 炸板率>30%, 最高板<=2
      - 复苏: 涨停30-60且趋势向上
      - 高潮: 涨停>80, 炸板率<15%, 最高板>=5
      - 退潮: 从高潮回落(涨停从>80降到60以下)
    """
    # 先获取前3天涨停趋势
    prev_rows = conn.execute("""
        SELECT trade_date, COUNT(*) as cnt
        FROM zt_pool
        WHERE trade_date < ?
        GROUP BY trade_date
        ORDER BY trade_date DESC LIMIT 3
    """, (target_date,)).fetchall()

    prev_zt = [r["cnt"] for r in prev_rows]

    # === 冰点判断 ===
    if zt_count < 30 and (zb_rate > 0.30 or highest_board <= 2):
        return EmotionPhase.ICE

    # === 高潮判断(宽松版: 涨停>80即可, 或涨停>60+最高板>=5) ===
    if zt_count > 80:
        return EmotionPhase.CLIMAX
    if zt_count > 60 and highest_board >= 5 and zb_rate < 0.20:
        return EmotionPhase.CLIMAX

    # === 退潮判断(从高位回落) ===
    if len(prev_zt) >= 2:
        avg_prev = sum(prev_zt[:2]) / 2
        # 前几天涨停>60(高位), 今天明显下降
        if avg_prev > 60 and zt_count < avg_prev * 0.75:
            return EmotionPhase.EBB
        # 炸板率从低位突然升高
        if zb_rate > 0.25 and zt_count < 70:
            return EmotionPhase.EBB

    # === 趋势方向判断(复苏 vs 退潮) ===
    if len(prev_zt) >= 2:
        trending_up = zt_count > prev_zt[0]  # 今天比昨天多
        if zt_count >= 30 and zt_count <= 80:
            return EmotionPhase.RECOVER if trending_up else EmotionPhase.EBB

    # === 默认: 涨跌家数辅助 ===
    if up_count > 0 and down_count > 0:
        if up_count > down_count * 1.5:
            return EmotionPhase.RECOVER
        elif down_count > up_count * 1.5:
            return EmotionPhase.EBB

    # 兜底
    if zt_count >= 60:
        return EmotionPhase.RECOVER
    return EmotionPhase.ICE


def _calc_score(
    zt_count: int, zb_rate: float, highest_board: int,
    up_count: int, down_count: int
) -> float:
    """综合情绪评分 0-100

    权重: 涨停活跃度40% + 炸板质量20% + 连板高度20% + 涨跌比20%
    """
    # 涨停活跃度 (0-40分, 涨停100只=满分)
    zt_score = min(zt_count / 100 * 40, 40)

    # 炸板质量 (0-20分, 炸板率越低越好)
    zb_score = max((1 - zb_rate) * 20, 0)

    # 连板高度 (0-20分, 5板以上满分)
    board_score = min(highest_board / 5 * 20, 20)

    # 涨跌比 (0-20分)
    if up_count + down_count > 0:
        ratio = up_count / (up_count + down_count)
        ud_score = ratio * 20
    else:
        ud_score = 10  # 无数据给中间分

    return zt_score + zb_score + board_score + ud_score


def _get_advice(phase: EmotionPhase, score: float) -> str:
    """根据阶段给出操作建议"""
    advice_map = {
        EmotionPhase.ICE: "冰点期: 不开新仓! 空仓等待, 关注跌停减少/涨停增加的拐点信号",
        EmotionPhase.RECOVER: "复苏期: 可开仓, 选主流题材龙头, 仓位控制在50%以内",
        EmotionPhase.CLIMAX: "高潮期: 持仓为主, 严格执行移动止盈, 不追高开新仓",
        EmotionPhase.EBB: "退潮期: 减仓! 严守止损, 不加仓, 等待情绪企稳",
    }
    return advice_map.get(phase, "观望")


def _default_state(dt: str) -> EmotionState:
    """无数据时返回默认状态"""
    return EmotionState(
        trade_date=dt,
        phase=EmotionPhase.ICE,
        zt_count=0, zb_count=0, zb_rate=0,
        highest_board=0, multi_board_count=0,
        score=0, advice="无数据, 建议观望",
    )


# ---------------------------------------------------------------------------
# 便捷接口
# ---------------------------------------------------------------------------
def should_trade(target_date: Optional[str] = None) -> tuple[bool, float, str]:
    """快速判断今天是否该交易

    Returns:
        (can_open, position_ratio, phase_name)
    """
    state = get_emotion_state(target_date)
    return state.can_open, state.position_ratio, state.phase.value


def emotion_brief(target_date: Optional[str] = None) -> str:
    """生成一句话情绪摘要(适合推送到交易日志)"""
    state = get_emotion_state(target_date)
    return (
        f"[{state.trade_date}] 情绪:{state.phase.value}({state.score}分) "
        f"涨停{state.zt_count}/炸板{state.zb_count}({state.zb_rate:.0%}) "
        f"最高{state.highest_board}板 "
        f"涨跌{state.up_count}:{state.down_count} "
        f"| {state.advice}"
    )


# ---------------------------------------------------------------------------
# CLI入口
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import sys
    dt = sys.argv[1] if len(sys.argv) > 1 else None
    state = get_emotion_state(dt)
    print(f"=== 市场情绪 [{state.trade_date}] ===")
    print(f"阶段: {state.phase.value} (评分{state.score})")
    print(f"涨停: {state.zt_count}只, 炸板: {state.zb_count}只 ({state.zb_rate:.0%})")
    print(f"连板: 最高{state.highest_board}板, {state.multi_board_count}只连板股")
    print(f"涨跌: {state.up_count}涨 / {state.down_count}跌")
    print(f"开仓: {'可以' if state.can_open else '不建议'}")
    print(f"建议仓位: {state.position_ratio:.0%}")
    print(f"操作建议: {state.advice}")
