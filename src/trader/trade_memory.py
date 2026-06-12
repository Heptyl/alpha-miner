"""交易记忆引擎 — 从历史交易中学习

功能:
  1. 盘后复盘: 对当天已平仓交易生成归因分析
  2. 相似交易检索: 查询历史相似策略+行业+信号的胜率
  3. 买入参考: daemon买入时可查询相似交易历史表现

表: trade_memory (关联daemon_trades)

用法:
  from src.trader.trade_memory import (
      run_daily_review,
      query_similar_trades,
      get_strategy_stats,
  )
"""

from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime, date
from pathlib import Path
from typing import Optional

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

DB_PATH = Path(__file__).resolve().parents[2] / "data" / "alpha_miner.db"

logger = logging.getLogger("trading_daemon")


def _get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def _ensure_table(conn: sqlite3.Connection):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS trade_memory (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_id        INTEGER NOT NULL,
            code            TEXT NOT NULL,
            name            TEXT DEFAULT '',
            strategy        TEXT NOT NULL,
            action          TEXT NOT NULL,
            pnl_pct         REAL DEFAULT 0,
            hold_days       INTEGER DEFAULT 0,
            market_phase    TEXT DEFAULT '',
            industry        TEXT DEFAULT '',
            entry_signal    TEXT DEFAULT '',
            exit_reason     TEXT DEFAULT '',
            attribution     TEXT DEFAULT '',
            lessons         TEXT DEFAULT '',
            similar_win_rate REAL DEFAULT 0,
            created_at      TEXT DEFAULT (datetime('now'))
        )
    """)


def _get_industry(conn: sqlite3.Connection, code: str) -> str:
    """查股票行业"""
    row = conn.execute(
        "SELECT industry_name FROM stock_industry_mapping WHERE stock_code = ?",
        (code,),
    ).fetchone()
    if row:
        return row[0]
    row = conn.execute(
        "SELECT concept_name FROM concept_mapping WHERE stock_code = ? LIMIT 1",
        (code,),
    ).fetchone()
    return row[0] if row else "未知"


def _extract_strategy(signal_type: str) -> str:
    """从signal_type提取策略标识"""
    if "首阴" in signal_type or "策略A" in signal_type or "涨停次日" in signal_type:
        return "A"
    if "暴跌" in signal_type or "策略B" in signal_type or "低开" in signal_type or "回踩低吸" in signal_type:
        return "B"
    if "趋势" in signal_type or "策略C" in signal_type or "基本面" in signal_type:
        return "C"
    return "未知"


def _calc_hold_days(conn: sqlite3.Connection, code: str, buy_date: str, sell_date: str) -> int:
    """计算持仓天数(交易日)"""
    from src.trader.daemon_db import _count_trading_days
    try:
        return _count_trading_days(buy_date[:10], sell_date[:10] if sell_date else buy_date[:10])
    except Exception:
        return 0


def _rget(row, key, default=""):
    """安全取值 — 兼容 dict 和 sqlite3.Row"""
    try:
        val = row[key] if key in row.keys() else default
    except (KeyError, IndexError):
        val = default
    return default if val is None else val


def _generate_attribution(trade, industry: str, hold_days: int,
                          similar_stats: dict) -> tuple[str, str]:
    """生成归因分析和经验教训(纯规则, 不依赖LLM)

    trade: dict 或 sqlite3.Row
    Returns: (attribution, lessons)
    """
    strategy = _extract_strategy(_rget(trade, "signal_type"))
    pnl_pct = _rget(trade, "pnl_pct", 0)
    action = _rget(trade, "action")
    exit_reason = _rget(trade, "reason")

    parts = []
    lessons_parts = []

    if action == "sell":
        # 盈亏归因
        if pnl_pct > 0:
            parts.append(f"盈利{pnl_pct:+.1f}%")
            if hold_days <= 2:
                parts.append("短线快进快出")
            elif "止盈" in exit_reason or "trailing" in exit_reason:
                parts.append("trailing止盈锁定利润")
            elif "目标" in exit_reason:
                parts.append("目标收益达成")
        else:
            parts.append(f"亏损{pnl_pct:+.1f}%")
            if "止损" in exit_reason:
                parts.append("触发止损")
                if hold_days <= 1:
                    lessons_parts.append("买入当天/次日即止损, 检查买入时机是否合理")
            elif "清仓" in exit_reason or "到期" in exit_reason:
                parts.append("到期清仓")
                if pnl_pct < -3:
                    lessons_parts.append("到期时大亏, 应提前止损而非等到期")

        # 持仓时间归因
        if hold_days > 5:
            parts.append(f"持{hold_days}天")
        else:
            parts.append(f"持{hold_days}天")

        # 市场环境
        if similar_stats.get("total", 0) >= 5:
            wr = similar_stats.get("win_rate", 0)
            avg = similar_stats.get("avg_pnl", 0)
            parts.append(f"历史同策略胜率{wr:.0%}均收{avg:+.1f}%")
            if wr < 0.4:
                lessons_parts.append("历史胜率<40%, 该策略信号需优化")
            elif wr > 0.6:
                lessons_parts.append("历史胜率>60%, 单笔亏损属正常波动")

        # 行业归因
        if pnl_pct < -5:
            lessons_parts.append(f"{industry}板块大亏, 注意行业集中度")

    elif action == "buy":
        parts.append(f"策略{strategy}买入")
        if similar_stats.get("total", 0) >= 5:
            wr = similar_stats.get("win_rate", 0)
            parts.append(f"历史胜率{wr:.0%}")

    attribution = "; ".join(parts) if parts else "无归因"
    lessons = "; ".join(lessons_parts) if lessons_parts else ""
    return attribution, lessons


def query_similar_trades(strategy: str, industry: str = "",
                        entry_signal: str = "") -> dict:
    """查询历史相似交易的胜率和均收益

    Args:
        strategy: 策略A/B/C
        industry: 行业(可选)
        entry_signal: 买入信号类型(可选)

    Returns:
        {"total", "wins", "win_rate", "avg_pnl", "avg_hold_days"}
    """
    conn = _get_conn()
    try:
        conditions = ["action='sell'", "pnl_pct IS NOT NULL"]
        params = []

        if strategy and strategy != "未知":
            conditions.append("strategy=?")
            params.append(strategy)
        if industry and industry != "未知":
            conditions.append("industry=?")
            params.append(industry)
        if entry_signal:
            conditions.append("entry_signal LIKE ?")
            params.append(f"%{entry_signal}%")

        where = " AND ".join(conditions)
        rows = conn.execute(
            f"SELECT pnl_pct, hold_days FROM trade_memory WHERE {where}",
            params,
        ).fetchall()

        if not rows:
            return {"total": 0, "wins": 0, "win_rate": 0, "avg_pnl": 0, "avg_hold_days": 0}

        pnls = [r["pnl_pct"] for r in rows if r["pnl_pct"] is not None]
        holds = [r["hold_days"] for r in rows if r["hold_days"] is not None]
        wins = sum(1 for p in pnls if p > 0)

        return {
            "total": len(pnls),
            "wins": wins,
            "win_rate": wins / len(pnls) if pnls else 0,
            "avg_pnl": sum(pnls) / len(pnls) if pnls else 0,
            "avg_hold_days": sum(holds) / len(holds) if holds else 0,
        }
    finally:
        conn.close()


def get_strategy_stats() -> dict:
    """获取各策略的历史统计(用于展示和决策参考)"""
    conn = _get_conn()
    try:
        stats = {}
        for strat in ("A", "B", "C"):
            rows = conn.execute(
                "SELECT pnl_pct, hold_days FROM trade_memory "
                "WHERE strategy=? AND action='sell' AND pnl_pct IS NOT NULL",
                (strat,),
            ).fetchall()
            if not rows:
                stats[strat] = {"total": 0, "win_rate": 0, "avg_pnl": 0}
                continue
            pnls = [r["pnl_pct"] for r in rows]
            wins = sum(1 for p in pnls if p > 0)
            stats[strat] = {
                "total": len(pnls),
                "win_rate": round(wins / len(pnls), 2),
                "avg_pnl": round(sum(pnls) / len(pnls), 2),
            }
        return stats
    finally:
        conn.close()


def run_daily_review(trade_date: str = None):
    """盘后复盘 — 对当天已平仓交易生成归因分析并存入记忆

    对每笔sell交易:
      1. 查找对应的buy交易(配对)
      2. 计算持仓天数、行业、策略
      3. 查询历史相似交易胜率
      4. 生成归因分析和经验教训
      5. 存入trade_memory表
    """
    if not trade_date:
        trade_date = date.today().isoformat()

    conn = _get_conn()
    try:
        _ensure_table(conn)

        # 查当天sell交易
        sells = conn.execute("""
            SELECT t.id, t.code, t.name, t.action, t.trade_date, t.price,
                   t.pnl, t.pnl_pct, t.signal_type, t.reason, t.ml_score
            FROM daemon_trades t
            WHERE t.action='sell' AND t.trade_date = ?
              AND t.id NOT IN (SELECT trade_id FROM trade_memory WHERE action='sell')
            ORDER BY t.id
        """, (trade_date,)).fetchall()

        if not sells:
            logger.info(f"[复盘] {trade_date} 无新增已平仓交易")
            return

        logger.info(f"[复盘] {trade_date} 处理{len(sells)}笔已平仓交易")

        for sell in sells:
            code = sell["code"]
            industry = _get_industry(conn, code)

            # 查对应的buy交易
            buy = conn.execute("""
                SELECT trade_date, signal_type, reason, price
                FROM daemon_trades
                WHERE code=? AND action='buy'
                  AND trade_date <= ? AND id < ?
                ORDER BY id DESC LIMIT 1
            """, (code, trade_date, sell["id"])).fetchone()

            hold_days = 0
            entry_signal = ""
            buy_price = 0
            if buy:
                hold_days = _calc_hold_days(conn, code, buy["trade_date"], trade_date)
                entry_signal = _rget(buy, "signal_type")
                buy_price = _rget(buy, "price", 0)

            # 策略从买入信号提取(sell的signal_type是卖出原因)
            strategy = _extract_strategy(entry_signal)

            # 计算实际盈亏百分比
            pnl_pct = _rget(sell, "pnl_pct", 0)
            if pnl_pct == 0 and buy_price > 0:
                pnl_pct = (_rget(sell, "price", 0) - buy_price) / buy_price * 100

            # 查历史相似交易
            similar = query_similar_trades(strategy, industry)

            # 用修正后的pnl_pct做归因
            sell_data = dict(sell)
            sell_data["pnl_pct"] = pnl_pct
            attribution, lessons = _generate_attribution(sell_data, industry, hold_days, similar)

            # 存入trade_memory
            conn.execute("""
                INSERT INTO trade_memory
                (trade_id, code, name, strategy, action, pnl_pct, hold_days,
                 industry, entry_signal, exit_reason, attribution, lessons,
                 similar_win_rate)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                sell["id"], code, sell["name"], strategy, "sell",
                pnl_pct, hold_days,
                industry, entry_signal, _rget(sell, "reason"),
                attribution, lessons,
                similar.get("win_rate", 0),
            ))

            logger.info(
                f"[复盘] {code} {sell['name']} 策略{strategy} "
                f"{pnl_pct:+.1f}% 持{hold_days}天 "
                f"{industry} | {attribution}"
            )

        conn.commit()

        # 同时处理当天buy交易(记录买入信号)
        buys = conn.execute("""
            SELECT t.id, t.code, t.name, t.signal_type, t.reason, t.ml_score
            FROM daemon_trades t
            WHERE t.action='buy' AND t.trade_date = ?
              AND t.id NOT IN (SELECT trade_id FROM trade_memory WHERE action='buy')
        """, (trade_date,)).fetchall()

        for buy in buys:
            code = buy["code"]
            strategy = _extract_strategy(_rget(buy, "signal_type"))
            industry = _get_industry(conn, code)
            similar = query_similar_trades(strategy, industry)
            attribution, _ = _generate_attribution(buy, industry, 0, similar)

            conn.execute("""
                INSERT INTO trade_memory
                (trade_id, code, name, strategy, action, pnl_pct,
                 industry, entry_signal, attribution, similar_win_rate)
                VALUES (?, ?, ?, ?, ?, 0, ?, ?, ?, ?)
            """, (
                buy["id"], code, buy["name"], strategy, "buy",
                industry, _rget(buy, "signal_type"),
                attribution, similar.get("win_rate", 0),
            ))

        if buys:
            conn.commit()
            logger.info(f"[复盘] {trade_date} 记录{len(buys)}笔买入")

        # 汇总
        total_sells = len(sells)
        wins = sum(1 for s in sells if (_rget(s, "pnl_pct", 0) or 0) > 0)
        total_pnl = sum(_rget(s, "pnl", 0) or 0 for s in sells)
        logger.info(
            f"[复盘汇总] {trade_date}: {total_sells}笔平仓, "
            f"胜{wins}负{total_sells-wins}, "
            f"总盈亏¥{total_pnl:+,.0f}"
        )

    finally:
        conn.close()


def backfill_memory(days: int = 90):
    """回填历史交易记忆

    对daemon_trades中已有的历史sell交易生成归因分析
    """
    conn = _get_conn()
    try:
        _ensure_table(conn)

        already = conn.execute("SELECT COUNT(DISTINCT trade_id) FROM trade_memory").fetchone()[0]
        total_trades = conn.execute("SELECT COUNT(*) FROM daemon_trades").fetchone()[0]
        logger.info(f"[回填] 已有{already}条记忆, 共{total_trades}笔交易")

        # 按日期回填
        sells = conn.execute("""
            SELECT t.id, t.code, t.name, t.action, t.trade_date, t.price,
                   t.pnl, t.pnl_pct, t.signal_type, t.reason
            FROM daemon_trades t
            WHERE t.action='sell'
              AND t.id NOT IN (SELECT trade_id FROM trade_memory WHERE action='sell')
            ORDER BY t.trade_date, t.id
        """).fetchall()

        if not sells:
            logger.info("[回填] 无新增sell交易需要处理")
            return

        for sell in sells:
            code = sell["code"]
            industry = _get_industry(conn, code)
            trade_date = sell["trade_date"]

            buy = conn.execute("""
                SELECT trade_date, signal_type, price
                FROM daemon_trades
                WHERE code=? AND action='buy'
                  AND trade_date <= ? AND id < ?
                ORDER BY id DESC LIMIT 1
            """, (code, trade_date, sell["id"])).fetchone()

            hold_days = 0
            entry_signal = ""
            buy_price = 0
            if buy:
                hold_days = _calc_hold_days(conn, code, buy["trade_date"], trade_date)
                entry_signal = _rget(buy, "signal_type")
                buy_price = _rget(buy, "price", 0)

            strategy = _extract_strategy(entry_signal)

            pnl_pct = _rget(sell, "pnl_pct", 0)
            if pnl_pct == 0 and buy_price > 0:
                pnl_pct = (_rget(sell, "price", 0) - buy_price) / buy_price * 100

            similar = query_similar_trades(strategy, industry)
            sell_data = dict(sell)
            sell_data["pnl_pct"] = pnl_pct
            attribution, lessons = _generate_attribution(sell_data, industry, hold_days, similar)

            conn.execute("""
                INSERT INTO trade_memory
                (trade_id, code, name, strategy, action, pnl_pct, hold_days,
                 industry, entry_signal, exit_reason, attribution, lessons,
                 similar_win_rate)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                sell["id"], code, sell["name"], strategy, "sell",
                pnl_pct, hold_days,
                industry, entry_signal, _rget(sell, "reason"),
                attribution, lessons,
                similar.get("win_rate", 0),
            ))

        conn.commit()
        logger.info(f"[回填] 完成, 处理{len(sells)}笔sell交易")

    finally:
        conn.close()


def show_memory_stats():
    """显示记忆统计"""
    conn = _get_conn()
    try:
        _ensure_table(conn)
        total = conn.execute("SELECT COUNT(*) FROM trade_memory").fetchone()[0]
        sells = conn.execute(
            "SELECT COUNT(*) FROM trade_memory WHERE action='sell'"
        ).fetchone()[0]

        print(f"\n交易记忆统计:")
        print(f"  总条数: {total} (买入{sells}笔, 卖出{sells}笔)")

        if sells > 0:
            for strat in ("A", "B", "C"):
                rows = conn.execute(
                    "SELECT pnl_pct FROM trade_memory "
                    "WHERE strategy=? AND action='sell' AND pnl_pct IS NOT NULL",
                    (strat,),
                ).fetchall()
                if not rows:
                    continue
                pnls = [r["pnl_pct"] for r in rows]
                wins = sum(1 for p in pnls if p > 0)
                wr = wins / len(pnls)
                avg = sum(pnls) / len(pnls)
                print(f"  策略{strat}: {len(pnls)}笔, 胜率{wr:.0%}, 均收{avg:+.1f}%")

            # 最近5笔
            recent = conn.execute("""
                SELECT code, name, strategy, pnl_pct, hold_days, industry, attribution
                FROM trade_memory WHERE action='sell'
                ORDER BY created_at DESC LIMIT 5
            """).fetchall()
            if recent:
                print(f"\n  最近5笔:")
                for r in recent:
                    print(f"    {r['code']} {r['name']} 策略{r['strategy']} "
                          f"{r['pnl_pct']:+.1f}% 持{r['hold_days']}天 "
                          f"{r['industry']} | {r['attribution'][:60]}")
    finally:
        conn.close()


if __name__ == "__main__":
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="交易记忆引擎")
    parser.add_argument("--review", action="store_true", help="盘后复盘(当天)")
    parser.add_argument("--backfill", action="store_true", help="回填历史记忆")
    parser.add_argument("--stats", action="store_true", help="显示统计")
    parser.add_argument("--date", type=str, help="指定日期(YYYY-MM-DD)")
    args = parser.parse_args()

    if args.backfill:
        backfill_memory()
    if args.review:
        run_daily_review(args.date)
    if args.stats or not (args.review or args.backfill):
        show_memory_stats()
