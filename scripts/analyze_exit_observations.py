"""analyze_exit_observations.py — 卖出后观察分析: 卖早了吗?

从 daemon_exit_observations 读取卖出记录, 用 daily_price 回填 T+1/T+2/T+3 的
future_max_ret 和 future_close_ret, 并判断"卖早率"。

用法:
    python scripts/analyze_exit_observations.py
    python scripts/analyze_exit_observations.py --start-date 2026-05-01 --end-date 2026-06-01
    python scripts/analyze_exit_observations.py --code 000063
    python scripts/analyze_exit_observations.py --horizon 3 --start-date 2026-05-15

逻辑:
    sell_price 为基准价。
    对于每个 future day (T+1, T+2, T+3):
      max_ret  = (future_high  - sell_price) / sell_price * 100
      close_ret = (future_close - sell_price) / sell_price * 100
    如果 future_max_ret_3d > 5%  → "可能卖早"
    如果 future_max_ret_3d <= 0% → "卖出有效"
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path
from datetime import datetime

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_ROOT / "data" / "alpha_miner.db"


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


def backfill_future_returns(conn: sqlite3.Connection, horizon: int = 3) -> int:
    """回填 future_max_ret_Nd / future_close_ret_Nd 字段

    只处理 future_checked_until 为 NULL 或不够 horizon 天的记录。
    返回更新的行数。
    """
    rows = conn.execute(
        "SELECT * FROM daemon_exit_observations ORDER BY sell_date, code"
    ).fetchall()

    updated = 0
    for row in rows:
        obs_id = row["id"]
        sell_date = row["sell_date"]
        code = row["code"]
        sell_price = row["sell_price"]

        if sell_price <= 0:
            continue

        # 获取卖出日之后的 daily_price 记录
        prices = conn.execute(
            """SELECT trade_date, high, close
               FROM daily_price
               WHERE stock_code = ? AND trade_date > ?
               ORDER BY trade_date ASC
               LIMIT ?""",
            (code, sell_date, horizon),
        ).fetchall()

        if not prices:
            # 无后续数据, 标记 checked_until
            conn.execute(
                "UPDATE daemon_exit_observations SET future_checked_until = ? WHERE id = ?",
                (sell_date, obs_id),
            )
            continue

        updates = {}
        for i, p in enumerate(prices):
            day_idx = i + 1  # T+1, T+2, ...
            if day_idx > horizon:
                break
            max_ret = (p["high"] - sell_price) / sell_price * 100
            close_ret = (p["close"] - sell_price) / sell_price * 100
            updates[f"future_max_ret_{day_idx}d"] = round(max_ret, 2)
            updates[f"future_close_ret_{day_idx}d"] = round(close_ret, 2)

        last_date = prices[-1]["trade_date"]
        # 不足 horizon 天的, 清空缺的天数为 NULL
        for day_idx in range(1, horizon + 1):
            if f"future_max_ret_{day_idx}d" not in updates:
                updates[f"future_max_ret_{day_idx}d"] = None
                updates[f"future_close_ret_{day_idx}d"] = None

        set_clause = ", ".join(f"{k} = ?" for k in updates)
        values = list(updates.values()) + [last_date, obs_id]
        conn.execute(
            f"UPDATE daemon_exit_observations SET {set_clause}, future_checked_until = ? WHERE id = ?",
            values,
        )
        updated += 1

    conn.commit()
    return updated


def analyze(conn: sqlite3.Connection, start_date: str | None, end_date: str | None,
            code: str | None, horizon: int) -> None:
    """输出卖早分析报告"""
    where_parts = []
    params: list = []

    if start_date:
        where_parts.append("sell_date >= ?")
        params.append(start_date)
    if end_date:
        where_parts.append("sell_date <= ?")
        params.append(end_date)
    if code:
        where_parts.append("code = ?")
        params.append(code)

    where = " AND ".join(where_parts) if where_parts else "1=1"

    rows = conn.execute(
        f"SELECT * FROM daemon_exit_observations WHERE {where} ORDER BY sell_date, code",
        params,
    ).fetchall()

    if not rows:
        print("暂无退出观察数据。")
        return

    print(f"\n{'='*80}")
    print(f"  卖出后观察分析 (T+{horizon}d)")
    print(f"{'='*80}")
    print(f"  总记录数: {len(rows)}")
    print()

    sold_early = 0   # 可能卖早
    sold_ok = 0       # 卖出有效
    sold_uncertain = 0

    print(f"{'日期':<12} {'代码':<8} {'名称':<10} {'策略':<4} {'卖出原因':<25} "
          f"{'卖价':>8} {'盈亏%':>7} {'T+1max':>8} {'T+1cls':>8} "
          f"{'T+2max':>8} {'T+2cls':>8} {'T+3max':>8} {'T+3cls':>8} {'判断':<10}")
    print("-" * 160)

    for r in rows:
        # 使用指定 horizon 的 max_ret 判断
        max_ret_col = f"future_max_ret_{horizon}d"
        max_ret = r[max_ret_col]

        if max_ret is not None:
            if max_ret > 5.0:
                verdict = "可能卖早"
                sold_early += 1
            elif max_ret <= 0:
                verdict = "卖出有效"
                sold_ok += 1
            else:
                verdict = "不确定"
                sold_uncertain += 1
        else:
            verdict = "数据不足"
            sold_uncertain += 1

        pnl_str = f"{r['pnl_pct_at_sell']:+.1f}" if r['pnl_pct_at_sell'] else "N/A"
        sell_reason_short = (r['sell_reason'] or '')[:25]

        t1m = f"{r['future_max_ret_1d']:+.1f}" if r['future_max_ret_1d'] is not None else "N/A"
        t1c = f"{r['future_close_ret_1d']:+.1f}" if r['future_close_ret_1d'] is not None else "N/A"
        t2m = f"{r['future_max_ret_2d']:+.1f}" if r['future_max_ret_2d'] is not None else "N/A"
        t2c = f"{r['future_close_ret_2d']:+.1f}" if r['future_close_ret_2d'] is not None else "N/A"
        t3m = f"{r['future_max_ret_3d']:+.1f}" if r['future_max_ret_3d'] is not None else "N/A"
        t3c = f"{r['future_close_ret_3d']:+.1f}" if r['future_close_ret_3d'] is not None else "N/A"

        print(f"{r['sell_date']:<12} {r['code']:<8} {r['name']:<10} {r['strategy']:<4} "
              f"{sell_reason_short:<25} {r['sell_price']:>8.2f} {pnl_str:>7} "
              f"{t1m:>8} {t1c:>8} {t2m:>8} {t2c:>8} {t3m:>8} {t3c:>8} {verdict:<10}")

    total_judged = sold_early + sold_ok + sold_uncertain
    print()
    print(f"--- 汇总 ---")
    print(f"  可能卖早: {sold_early} ({sold_early/total_judged*100:.0f}%)" if total_judged else "  可能卖早: 0")
    print(f"  卖出有效: {sold_ok} ({sold_ok/total_judged*100:.0f}%)" if total_judged else "  卖出有效: 0")
    print(f"  不确定:   {sold_uncertain} ({sold_uncertain/total_judged*100:.0f}%)" if total_judged else "  不确定: 0")
    early_rate = sold_early / total_judged * 100 if total_judged else 0
    print(f"  卖早率:   {early_rate:.1f}%")

    # 按策略分组
    print()
    print(f"--- 按策略分组 ---")
    strategies = {}
    for r in rows:
        s = r["strategy"] or "未知"
        strategies.setdefault(s, []).append(r)

    for strat, strat_rows in sorted(strategies.items()):
        se = sum(1 for r in strat_rows if r[f"future_max_ret_{horizon}d"] is not None and r[f"future_max_ret_{horizon}d"] > 5.0)
        so = sum(1 for r in strat_rows if r[f"future_max_ret_{horizon}d"] is not None and r[f"future_max_ret_{horizon}d"] <= 0)
        su = len(strat_rows) - se - so
        total = len(strat_rows)
        print(f"  策略{strat}: 共{total}笔, 可能卖早{se}笔, 卖出有效{so}笔, 不确定{su}笔")
        if total > 0:
            avg_t3m = [r[f"future_max_ret_{horizon}d"] for r in strat_rows if r[f"future_max_ret_{horizon}d"] is not None]
            if avg_t3m:
                print(f"    平均T+{horizon}最大涨幅: {sum(avg_t3m)/len(avg_t3m):+.2f}%")


def main():
    parser = argparse.ArgumentParser(description="卖出后观察分析: 卖早了吗?")
    parser.add_argument("--start-date", type=str, default=None, help="起始卖出日期 (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, default=None, help="结束卖出日期 (YYYY-MM-DD)")
    parser.add_argument("--code", type=str, default=None, help="指定股票代码 (如 000063)")
    parser.add_argument("--horizon", type=int, default=3, help="观察天数 (默认3)")
    parser.add_argument("--no-backfill", action="store_true", help="跳过回填, 只输出已有数据")
    args = parser.parse_args()

    conn = get_conn()
    try:
        # 确保 daemon_exit_observations 表存在
        conn.execute("""
            CREATE TABLE IF NOT EXISTS daemon_exit_observations (
                id                      INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at              TEXT NOT NULL,
                sell_date               TEXT NOT NULL,
                code                    TEXT NOT NULL,
                name                    TEXT DEFAULT '',
                strategy                TEXT DEFAULT '',
                sell_reason             TEXT DEFAULT '',
                sell_price              REAL DEFAULT 0,
                buy_price               REAL DEFAULT 0,
                shares                  INTEGER DEFAULT 0,
                pnl_pct_at_sell         REAL DEFAULT 0,
                highest_price_before_sell REAL DEFAULT 0,
                market_phase            TEXT DEFAULT '',
                raw_json                TEXT DEFAULT '',
                future_checked_until    TEXT,
                future_max_ret_1d       REAL,
                future_close_ret_1d     REAL,
                future_max_ret_2d       REAL,
                future_close_ret_2d     REAL,
                future_max_ret_3d       REAL,
                future_close_ret_3d     REAL
            );
        """)
        conn.commit()

        # 回填 future 数据
        if not args.no_backfill:
            n = backfill_future_returns(conn, horizon=args.horizon)
            print(f"[回填] 更新了 {n} 条记录的 future 数据")

        # 输出分析
        analyze(conn, args.start_date, args.end_date, args.code, args.horizon)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
