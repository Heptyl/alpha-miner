"""
龙头四维评分系统 — 基于dragon-quant算法, 用日K线近似实现

四维评分(借鉴dragon-quant, 按数据可用性调整权重):
  1. 带动性 35% — 这只票涨停时, 板块里其他票跟不跟?
  2. 领涨性 25% — 平时在板块里是不是领跑?
  3. 成交强度 25% — 替代"资金承接"(需要5分K), 用成交额变化率+换手率
  4. 抗跌性 15% — 大盘跳水时, 这只票硬不硬?

数据来源: daily_price + zt_pool (全部日K线级别)
dragon-quant参考: https://github.com/gitBingxu/dragon-quant
"""

import sqlite3
import logging
from typing import Optional
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

DB_PATH = "data/alpha_miner.db"


def _get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


# ---------------------------------------------------------------------------
# 带动性评分 (权重 35%)
# 核心问题: 这只票封板时, 同板块其他票跟不跟?
# 近似方案: 用板块涨停家数占比 + 一字板判定 + 跟风力度
# ---------------------------------------------------------------------------
def score_drive(code: str, trade_date: str) -> dict:
    """
    Returns: {"score": 0-100, "weight": 0.35, "details": {...}}
    """
    conn = _get_conn()
    try:
        # 1. 获取个股涨停信息
        zt = conn.execute(
            "SELECT * FROM zt_pool WHERE stock_code=? AND trade_date=?",
            (code, trade_date),
        ).fetchone()
        if not zt:
            return {"score": 30.0, "weight": 0.35, "details": {"reason": "非涨停日"}}

        industry = zt["industry"] or ""
        open_count = zt["open_count"] or 0  # 开板次数
        amount = zt["amount"] or 0
        cons_zt = zt["consecutive_zt"] or 1

        if not industry:
            return {"score": 30.0, "weight": 0.35, "details": {"reason": "无行业信息"}}

        # 2. 板块共鸣: 同行业涨停家数占比
        sector_zt = conn.execute(
            "SELECT COUNT(*) as cnt FROM zt_pool WHERE trade_date=? AND industry=?",
            (trade_date, industry),
        ).fetchone()["cnt"]
        # 板块共鸣简化: 同行业涨停家数>=3只就算有共鸣
        # 不再估算板块总成分股数(太复杂且不准确)
        voice_ratio = min(sector_zt / 5.0, 1.0)  # 5只涨停即满分
        voice_score = voice_ratio * 100

        # 3. 跟风力度: 同行业涨>3%但未涨停的家数
        # 从daily_price取同行业涨幅
        sector_stocks = conn.execute(
            "SELECT dp.stock_code, (dp.close/dp.pre_close - 1) * 100 as pct "
            "FROM daily_price dp "
            "JOIN zt_pool zt ON dp.stock_code = zt.stock_code AND zt.industry=? "
            "WHERE dp.trade_date=? AND dp.volume > 0 AND dp.pre_close > 0",
            (industry, trade_date),
        ).fetchall()
        # 这个JOIN可能不全, 因为zt_pool只有涨停票
        # 改用更简单的方法: 看同行业有多少只也涨停了
        follow_score = min(sector_zt / max(3, 1), 1.0) * 100  # 3只涨停即满分

        # 4. 封板质量: 开板次数越少越好, 一字板反而是劣势
        if open_count == 0:
            # 一字板: 用成交额判断(缩量一字=弱势)
            board_score = 50.0  # 一字板不确定
        elif open_count <= 2:
            board_score = 90.0  # 开板1-2次反而好(有换手)
        elif open_count <= 4:
            board_score = 70.0
        else:
            board_score = 40.0  # 频繁开板

        # 5. 连板加分
        cons_bonus = min((cons_zt - 1) * 10, 30)

        # 加权: 板块共鸣30% + 跟风30% + 封板质量40%
        drive_score = voice_score * 0.3 + follow_score * 0.3 + board_score * 0.4 + cons_bonus
        drive_score = min(drive_score, 100)

        return {
            "score": round(drive_score, 2),
            "weight": 0.35,
            "details": {
                "industry": industry,
                "sector_zt_count": sector_zt,
                "open_count": open_count,
                "consecutive_zt": cons_zt,
                "voice_score": round(voice_score, 2),
                "follow_score": round(follow_score, 2),
                "board_score": round(board_score, 2),
                "cons_bonus": cons_bonus,
            },
        }
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 领涨性评分 (权重 25%)
# 核心问题: 这只票在板块里是不是领跑?
# 近似方案: 涨停日板块内涨幅排名 + 历史5日板块内排名
# ---------------------------------------------------------------------------
def score_leadership(code: str, trade_date: str) -> dict:
    """
    Returns: {"score": 0-100, "weight": 0.25, "details": {...}}
    """
    conn = _get_conn()
    try:
        # 获取个股industry
        zt = conn.execute(
            "SELECT industry FROM zt_pool WHERE stock_code=? AND trade_date=?",
            (code, trade_date),
        ).fetchone()
        if not zt or not zt["industry"]:
            return {"score": 50.0, "weight": 0.25, "details": {"reason": "无行业"}}

        industry = zt["industry"]

        # 1. 当日板块内涨幅排名
        # 找同行业所有票的涨幅
        # 简化: 只看同行业涨停票的排名(这些是我们关心的票)
        sector_zt_all = conn.execute(
            "SELECT stock_code, name, amount, open_count FROM zt_pool "
            "WHERE trade_date=? AND industry=? ORDER BY amount DESC",
            (trade_date, industry),
        ).fetchall()

        if len(sector_zt_all) <= 1:
            # 独苗, 无法排名
            intraday_score = 70.0  # 独立涨停也不错
        else:
            # 按成交额排名(大成交额=更受关注=更可能领涨)
            for i, s in enumerate(sector_zt_all):
                if s["stock_code"] == code:
                    rank = i + 1
                    total = len(sector_zt_all)
                    intraday_score = (1 - (rank - 1) / total) * 100
                    break
            else:
                intraday_score = 50.0

        # 2. 历史5日非涨停日排名
        # 找个股近5天涨幅, vs 大盘涨跌幅
        stock_klines = conn.execute(
            "SELECT trade_date, (close/pre_close-1)*100 as pct FROM daily_price "
            "WHERE stock_code=? AND trade_date<? AND volume>0 AND pre_close>0 "
            "ORDER BY trade_date DESC LIMIT 10",
            (code, trade_date),
        ).fetchall()

        # 大盘(上证)涨跌幅
        market_klines = conn.execute(
            "SELECT trade_date, (close/pre_close-1)*100 as pct FROM daily_price "
            "WHERE stock_code='000001' AND trade_date<? AND volume>0 AND pre_close>0 "
            "ORDER BY trade_date DESC LIMIT 10",
            (trade_date,),
        ).fetchall()

        market_map = {r["trade_date"]: r["pct"] for r in market_klines}

        # 非涨停日的超额收益
        excess_returns = []
        for sk in stock_klines:
            if sk["pct"] < 9.5:  # 非涨停日
                mkt = market_map.get(sk["trade_date"], 0)
                excess_returns.append(sk["pct"] - mkt)
            if len(excess_returns) >= 5:
                break

        if excess_returns:
            avg_excess = sum(excess_returns) / len(excess_returns)
            # 超额收益>2%=100分, 0%=50分, <-2%=0分
            hist_score = max(0, min(100, 50 + avg_excess * 25))
        else:
            hist_score = 50.0

        # 加权: 当日60% + 历史40%
        final_score = intraday_score * 0.6 + hist_score * 0.4
        final_score = max(0, min(100, final_score))

        return {
            "score": round(final_score, 2),
            "weight": 0.25,
            "details": {
                "industry": industry,
                "sector_zt_count": len(sector_zt_all),
                "intraday_score": round(intraday_score, 2),
                "hist_excess_avg": round(sum(excess_returns) / len(excess_returns), 2) if excess_returns else 0,
                "hist_score": round(hist_score, 2),
            },
        }
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 成交强度评分 (权重 25%) — 替代"资金承接"
# 核心问题: 资金参与度高不高? 是不是越来越活跃?
# 近似方案: 成交额变化率 + 换手率 + 量比
# ---------------------------------------------------------------------------
def score_volume_strength(code: str, trade_date: str) -> dict:
    """
    Returns: {"score": 0-100, "weight": 0.25, "details": {...}}
    """
    conn = _get_conn()
    try:
        # 1. 涨停日成交额 vs 前5日平均成交额 = 量比
        today_amount = conn.execute(
            "SELECT amount FROM zt_pool WHERE stock_code=? AND trade_date=?",
            (code, trade_date),
        ).fetchone()

        if not today_amount or not today_amount["amount"]:
            return {"score": 50.0, "weight": 0.25, "details": {"reason": "无成交额"}}

        today_amt = today_amount["amount"]

        # 前5日成交额(从daily_price取)
        prev_amounts = conn.execute(
            "SELECT amount FROM daily_price "
            "WHERE stock_code=? AND trade_date<? AND volume>0 "
            "ORDER BY trade_date DESC LIMIT 5",
            (code, trade_date),
        ).fetchall()
        prev_amts = [r["amount"] for r in prev_amounts if r["amount"] and r["amount"] > 0]

        if prev_amts:
            avg_prev = sum(prev_amts) / len(prev_amts)
            volume_ratio = today_amt / avg_prev if avg_prev > 0 else 1.0
        else:
            volume_ratio = 1.0

        # 量比评分: 3-5倍=好(有资金关注), >10倍=过度(可能见顶)
        if 3 <= volume_ratio <= 5:
            ratio_score = 100.0
        elif 2 <= volume_ratio < 3:
            ratio_score = 80.0
        elif 5 < volume_ratio <= 8:
            ratio_score = 70.0
        elif volume_ratio > 8:
            ratio_score = 40.0  # 过度放量
        else:
            ratio_score = max(0, volume_ratio / 2 * 100)  # 量不够

        # 2. 换手率
        turnover = conn.execute(
            "SELECT turnover_rate FROM daily_price "
            "WHERE stock_code=? AND trade_date=? AND volume>0",
            (code, trade_date),
        ).fetchone()
        tr = turnover["turnover_rate"] if turnover and turnover["turnover_rate"] else 0

        # 换手率评分: 5-15%=好(活跃但不过度)
        if 5 <= tr <= 15:
            turnover_score = 100.0
        elif 3 <= tr < 5:
            turnover_score = 70.0
        elif 15 < tr <= 25:
            turnover_score = 60.0
        elif tr > 25:
            turnover_score = 30.0  # 过度换手=出货
        else:
            turnover_score = max(0, tr / 3 * 100)

        # 3. 成交额趋势: 近3日递增=好
        recent_3 = conn.execute(
            "SELECT amount FROM daily_price "
            "WHERE stock_code=? AND trade_date<=? AND volume>0 "
            "ORDER BY trade_date DESC LIMIT 4",
            (code, trade_date),
        ).fetchall()
        amts_3 = [r["amount"] for r in recent_3 if r["amount"] and r["amount"] > 0]

        if len(amts_3) >= 3:
            # 是否递增
            increasing = sum(1 for i in range(len(amts_3) - 1) if amts_3[i] > amts_3[i + 1])
            trend_score = increasing / (len(amts_3) - 1) * 100
        else:
            trend_score = 50.0

        # 加权: 量比40% + 换手率40% + 趋势20%
        final_score = ratio_score * 0.4 + turnover_score * 0.4 + trend_score * 0.2
        final_score = max(0, min(100, final_score))

        return {
            "score": round(final_score, 2),
            "weight": 0.25,
            "details": {
                "volume_ratio": round(volume_ratio, 2),
                "turnover_rate": round(tr, 2),
                "ratio_score": round(ratio_score, 2),
                "turnover_score": round(turnover_score, 2),
                "trend_score": round(trend_score, 2),
            },
        }
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 抗跌性评分 (权重 15%)
# 核心问题: 大盘跳水时, 这只票硬不硬?
# 近似方案: 大盘跳水日个股超额收益 + 日内承接 + 次日反弹
# ---------------------------------------------------------------------------
def score_anti_drop(code: str, trade_date: str) -> dict:
    """
    Returns: {"score": 0-100, "weight": 0.15, "details": {...}}
    """
    conn = _get_conn()
    try:
        # 找近30天大盘跳水日(上证跌幅>0.7%)
        td = datetime.strptime(trade_date, "%Y-%m-%d")
        start_date = (td - timedelta(days=45)).strftime("%Y-%m-%d")

        plunge_days = conn.execute(
            "SELECT trade_date, (close/pre_close-1)*100 as pct FROM daily_price "
            "WHERE stock_code='000001' AND trade_date>=? AND trade_date<=? "
            "AND volume>0 AND pre_close>0 AND (close/pre_close-1)*100 < -0.7 "
            "ORDER BY trade_date DESC LIMIT 5",
            (start_date, trade_date),
        ).fetchall()

        if not plunge_days:
            return {"score": 60.0, "weight": 0.15, "details": {"reason": "近期无跳水日"}}

        day_scores = []
        for pd_row in plunge_days:
            pd_date = pd_row["trade_date"]
            mkt_pct = pd_row["pct"]

            # 个股当天涨跌幅
            stock_row = conn.execute(
                "SELECT open, high, low, close, pre_close, "
                "(close/pre_close-1)*100 as pct FROM daily_price "
                "WHERE stock_code=? AND trade_date=? AND volume>0 AND pre_close>0",
                (code, pd_date),
            ).fetchone()

            if not stock_row:
                continue

            stock_pct = stock_row["pct"]
            o, h, l, c = stock_row["open"], stock_row["high"], stock_row["low"], stock_row["close"]
            pc = stock_row["pre_close"]

            # (a) 相对回撤强度 40%
            excess = stock_pct - mkt_pct
            if stock_pct > 0:
                rel_score = 100.0  # 大盘跌它涨
            elif excess > 0:
                rel_score = 60.0 + excess / abs(mkt_pct) * 40.0
            elif stock_pct > -2.0:
                rel_score = 30.0
            else:
                rel_score = 0.0

            # (b) 日内承接强度 30%: 下影线比例
            if h != l and pc > 0:
                entity_low = min(o, c)
                lower_shadow = (entity_low - l) / (h - l)
                close_pos = (c - o) / (h - l) if h != o else 0.5
                support = lower_shadow * 0.6 + close_pos * 0.4
                intraday_score = support * 100
            else:
                intraday_score = 50.0

            # (c) 反弹弹性 30%: 跳水日次日超额收益
            next_row = conn.execute(
                "SELECT trade_date FROM daily_price WHERE stock_code=? "
                "AND trade_date>? AND volume>0 ORDER BY trade_date LIMIT 1",
                (code, pd_date),
            ).fetchone()

            if next_row:
                next_pct_s = conn.execute(
                    "SELECT (close/pre_close-1)*100 as pct FROM daily_price "
                    "WHERE stock_code=? AND trade_date=? AND pre_close>0",
                    (code, next_row["trade_date"]),
                ).fetchone()
                next_pct_m = conn.execute(
                    "SELECT (close/pre_close-1)*100 as pct FROM daily_price "
                    "WHERE stock_code='000001' AND trade_date=? AND pre_close>0",
                    (next_row["trade_date"],),
                ).fetchone()

                if next_pct_s and next_pct_m:
                    next_alpha = next_pct_s["pct"] - next_pct_m["pct"]
                    if next_pct_s["pct"] > 0 and next_alpha > 0:
                        rebound_score = min(next_alpha / 3 * 100, 100)
                    elif next_pct_s["pct"] > 0:
                        rebound_score = 60.0
                    else:
                        rebound_score = max(0, 30 - abs(next_alpha) * 10)
                else:
                    rebound_score = 50.0
            else:
                rebound_score = 50.0

            day_score = rel_score * 0.4 + intraday_score * 0.3 + rebound_score * 0.3
            day_scores.append(day_score)

        if not day_scores:
            return {"score": 50.0, "weight": 0.15, "details": {"reason": "无个股跳水日数据"}}

        final_score = sum(day_scores) / len(day_scores)
        final_score = max(0, min(100, final_score))

        return {
            "score": round(final_score, 2),
            "weight": 0.15,
            "details": {
                "plunge_days_count": len(day_scores),
                "avg_score": round(final_score, 2),
            },
        }
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 综合评分
# ---------------------------------------------------------------------------
def dragon_score(code: str, trade_date: str) -> dict:
    """
    综合四维评分

    Returns:
        {
            "code": "000417",
            "trade_date": "2026-05-19",
            "total_score": 78.5,
            "dimensions": {
                "drive": {"score": 85.0, "weight": 0.35},
                "leadership": {"score": 72.0, "weight": 0.25},
                "volume_strength": {"score": 80.0, "weight": 0.25},
                "anti_drop": {"score": 65.0, "weight": 0.15},
            },
            "grade": "A",  # S/A/B/C/D
        }
    """
    drive = score_drive(code, trade_date)
    leadership = score_leadership(code, trade_date)
    volume = score_volume_strength(code, trade_date)
    anti = score_anti_drop(code, trade_date)

    total = (
        drive["score"] * drive["weight"]
        + leadership["score"] * leadership["weight"]
        + volume["score"] * volume["weight"]
        + anti["score"] * anti["weight"]
    )

    # 分级
    if total >= 85:
        grade = "S"
    elif total >= 70:
        grade = "A"
    elif total >= 55:
        grade = "B"
    elif total >= 40:
        grade = "C"
    else:
        grade = "D"

    return {
        "code": code,
        "trade_date": trade_date,
        "total_score": round(total, 2),
        "grade": grade,
        "dimensions": {
            "drive": drive,
            "leadership": leadership,
            "volume_strength": volume,
            "anti_drop": anti,
        },
    }


def batch_dragon_score(codes: list[str], trade_date: str) -> list[dict]:
    """批量评分, 按总分降序"""
    results = []
    for code in codes:
        try:
            r = dragon_score(code, trade_date)
            results.append(r)
        except Exception as e:
            logger.warning(f"dragon_score({code})失败: {e}")
            results.append({"code": code, "total_score": 0, "grade": "D", "error": str(e)})
    results.sort(key=lambda x: x.get("total_score", 0), reverse=True)
    return results
