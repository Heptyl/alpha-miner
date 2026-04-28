"""股票数据查询 CLI — python -m cli query

用法:
  python -m cli query 603318              # 查某只股票的完整数据
  python -m cli query 603318 --factor     # 只看因子值
  python -m cli query 603318 --kline 10   # 看最近10天K线
  python -m cli query --date 2026-04-24   # 看某天的市场概览
  python -m cli query --overview          # 数据库总览
"""

import argparse
import sqlite3
import sys


def get_conn(db_path: str) -> sqlite3.Connection:
    return sqlite3.connect(db_path)


def cmd_overview(conn):
    """数据库总览."""
    tables = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()

    print("\n" + "=" * 60)
    print("  Alpha Miner 数据库总览")
    print("=" * 60)

    for (table,) in tables:
        cnt = conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
        cols = [c[1] for c in conn.execute(f'PRAGMA table_info("{table}")').fetchall()]
        print(f"\n  {table}: {cnt} 行")
        print(f"    列: {', '.join(cols)}")
        if "trade_date" in cols:
            dr = conn.execute(
                f'SELECT MIN(trade_date), MAX(trade_date), COUNT(DISTINCT trade_date) FROM "{table}"'
            ).fetchone()
            if dr and dr[0]:
                print(f"    日期: {dr[0]} ~ {dr[1]} ({dr[2]} 天)")


def cmd_stock(conn, code: str, kline: int = 10, factor: bool = False):
    """查询单只股票."""
    # 股票名
    name_row = conn.execute(
        "SELECT name FROM zt_pool WHERE stock_code=? LIMIT 1", (code,)
    ).fetchone()
    name = name_row[0] if name_row else ""
    print(f"\n{'=' * 60}")
    print(f"  {code} {name}")
    print(f"{'=' * 60}")

    # 概念
    concepts = conn.execute(
        "SELECT concept_name FROM concept_mapping WHERE stock_code=?", (code,)
    ).fetchall()
    if concepts:
        print(f"\n  所属概念: {', '.join([c[0] for c in concepts])}")

    # K线
    if not factor:
        rows = conn.execute("""
            SELECT trade_date, open, close, high, low, volume, amount, turnover_rate
            FROM daily_price WHERE stock_code=? ORDER BY trade_date DESC LIMIT ?
        """, (code, kline)).fetchall()
        if rows:
            print(f"\n  --- K线 (近{len(rows)}天) ---")
            print(f"  {'日期':<12} {'开盘':>8} {'收盘':>8} {'最高':>8} {'最低':>8} {'涨跌幅':>8} {'成交额(万)':>12}")
            for r in rows:
                chg = (r[2] - r[1]) / r[1] * 100 if r[1] > 0 else 0
                amt_wan = r[6] / 1e4 if r[6] else 0
                print(f"  {r[0]:<12} {r[1]:>8.2f} {r[2]:>8.2f} {r[3]:>8.2f} {r[4]:>8.2f} {chg:>+7.2f}% {amt_wan:>11.0f}")

    # 涨停池
    zt_rows = conn.execute("""
        SELECT trade_date, consecutive_zt, amount, industry, open_count, zt_stats
        FROM zt_pool WHERE stock_code=? ORDER BY trade_date DESC
    """, (code,)).fetchall()
    if zt_rows:
        print(f"\n  --- 涨停记录 ({len(zt_rows)}次) ---")
        for r in zt_rows:
            print(f"  {r[0]}: {r[5]} | {r[3]} | 成交{r[2]/1e8:.2f}亿 | 炸板{r[4]}次")

    # 龙虎榜
    lhb_rows = conn.execute("""
        SELECT trade_date, buy_amount, sell_amount, net_amount, reason
        FROM lhb_detail WHERE stock_code=? ORDER BY trade_date DESC
    """, (code,)).fetchall()
    if lhb_rows:
        print(f"\n  --- 龙虎榜 ({len(lhb_rows)}条) ---")
        for r in lhb_rows:
            print(f"  {r[0]}: 净{r[3]/1e8:+.2f}亿 (买{r[1]/1e8:.2f} 卖{r[2]/1e8:.2f}) [{r[4]}]")

    # 因子值
    if factor or not factor:  # 总是显示因子
        fv_rows = conn.execute("""
            SELECT trade_date, factor_name, factor_value
            FROM factor_values WHERE stock_code=? ORDER BY trade_date DESC, factor_name
        """, (code,)).fetchall()
        if fv_rows:
            print(f"\n  --- 因子值 ---")
            current_date = ""
            for r in fv_rows:
                if r[0] != current_date:
                    current_date = r[0]
                    print(f"\n  {current_date}:")
                val = r[2]
                if abs(val) >= 1e6:
                    val_str = f"{val/1e8:.2f}亿"
                else:
                    val_str = f"{val:.4f}"
                print(f"    {r[1]:<22} {val_str}")


def cmd_date(conn, date: str):
    """查看某天的市场概览."""
    print(f"\n{'=' * 60}")
    print(f"  {date} 市场概览")
    print(f"{'=' * 60}")

    # 涨停池
    zt = conn.execute("""
        SELECT stock_code, name, consecutive_zt, industry, open_count, amount, zt_stats
        FROM zt_pool WHERE trade_date=? ORDER BY consecutive_zt DESC, amount DESC
    """, (date,)).fetchall()
    print(f"\n  涨停池 ({len(zt)} 只):")
    print(f"  {'代码':<8} {'名称':<8} {'连板':>4} {'板块':<10} {'炸板':>4} {'成交(亿)':>10} {'统计':<8}")
    for r in zt:
        print(f"  {r[0]:<8} {r[1]:<8} {r[2]:>4} {r[3]:<10} {r[4]:>4} {r[5]/1e8:>10.2f} {r[6]:<8}")

    # 龙虎榜
    lhb = conn.execute("""
        SELECT stock_code, buy_amount, sell_amount, net_amount, reason
        FROM lhb_detail WHERE trade_date=? ORDER BY net_amount DESC
    """, (date,)).fetchall()
    if lhb:
        print(f"\n  龙虎榜 ({len(lhb)} 条):")
        print(f"  {'代码':<8} {'净额(亿)':>10} {'买入(亿)':>10} {'卖出(亿)':>10} 原因")
        for r in lhb:
            print(f"  {r[0]:<8} {r[3]/1e8:>+10.2f} {r[1]/1e8:>10.2f} {r[2]/1e8:>10.2f} {r[4]}")

    # 资金流向 TOP10
    ff = conn.execute("""
        SELECT stock_code, main_net FROM fund_flow WHERE trade_date=?
        ORDER BY main_net DESC LIMIT 10
    """, (date,)).fetchall()
    if ff:
        print(f"\n  资金净流入 TOP10:")
        for r in ff:
            print(f"  {r[0]:<8} {r[1]/1e8:>+.2f}亿")


def main():
    parser = argparse.ArgumentParser(description="Alpha Miner 数据查询")
    parser.add_argument("code", nargs="?", default=None, help="股票代码 (如 603318)")
    parser.add_argument("--date", type=str, default=None, help="查看某天市场概览")
    parser.add_argument("--overview", action="store_true", help="数据库总览")
    parser.add_argument("--factor", action="store_true", help="只看因子值")
    parser.add_argument("--kline", type=int, default=10, help="K线天数 (默认10)")
    parser.add_argument("--db", type=str, default="data/alpha_miner.db", help="数据库路径")
    args = parser.parse_args()

    conn = get_conn(args.db)

    try:
        if args.overview:
            cmd_overview(conn)
        elif args.date:
            cmd_date(conn, args.date)
        elif args.code:
            cmd_stock(conn, args.code, kline=args.kline, factor=args.factor)
        else:
            parser.print_help()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
