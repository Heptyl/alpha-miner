"""eod_summary.py — 收盘总结（一屏看完）

用法:
  uv run python scripts/eod_summary.py              # 今天
  uv run python scripts/eod_summary.py --date 2026-06-01

数据源: daemon_log文件 + alpha_miner.db
"""
from __future__ import annotations

import argparse
import re
import sqlite3
import sys
from collections import Counter
from datetime import date, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "data" / "alpha_miner.db"
LOG_DIR = ROOT / "output" / "trader" / "daemon_logs"


def _conn():
    return sqlite3.connect(str(DB_PATH))


def _load_log(target_date: str) -> list[str]:
    log_file = LOG_DIR / f"daemon_{target_date}.log"
    if not log_file.exists():
        return []
    return log_file.read_text(encoding="utf-8", errors="ignore").splitlines()


# ═══════════════════════════════════════════════════════════
# 各模块
# ═══════════════════════════════════════════════════════════


def print_account(conn: sqlite3.Connection, target_date: str):
    """账户概览"""
    row = conn.execute(
        "SELECT * FROM daemon_account WHERE period=3 AND date=?",
        (target_date,),
    ).fetchone()
    if not row:
        # 取最近一条
        row = conn.execute(
            "SELECT * FROM daemon_account WHERE period=3 ORDER BY date DESC LIMIT 1"
        ).fetchone()
    if not row:
        print("  无账户数据")
        return
    d = dict(zip([c[0] for c in conn.execute("SELECT * FROM daemon_account LIMIT 1").description], row))
    pnl_color = "+" if d.get("daily_pnl", 0) >= 0 else ""
    cum_color = "+" if d.get("cumulative_pnl", 0) >= 0 else ""
    print(f"  日期: {d['date']}  现金: {d['cash']:.0f}  市值: {d.get('market_value',0):.0f}"
          f"  总资产: {d['total_assets']:.0f}")
    print(f"  日PnL: {pnl_color}{d.get('daily_pnl',0):.0f}"
          f"  累计PnL: {cum_color}{d.get('cumulative_pnl',0):.0f}"
          f"  交易: {d.get('total_trades',0)}笔  胜: {d.get('win_trades',0)}")


def print_trades(conn: sqlite3.Connection, target_date: str):
    """今日买卖"""
    rows = conn.execute("""
        SELECT action, code, name, trade_time, price, shares, amount, reason, signal_type
        FROM daemon_trades WHERE trade_date=? ORDER BY trade_time
    """, (target_date,)).fetchall()
    if not rows:
        print("  无交易")
        return
    for r in rows:
        action = "买入" if r[0] == "buy" else "卖出"
        reason = (r[7] or "")[:40]
        print(f"  {r[3]} {action} {r[1]} {r[2]} @{r[4]:.2f} {r[5]}股 ¥{r[6]:.0f}")
        print(f"         {reason}")
    buys = sum(1 for r in rows if r[0] == "buy")
    sells = sum(1 for r in rows if r[0] == "sell")
    print(f"  合计: {buys}买 {sells}卖")


def print_positions(conn: sqlite3.Connection):
    """当前持仓"""
    rows = conn.execute("""
        SELECT code, name, buy_price, shares, buy_time, signal_type, highest_price
        FROM daemon_positions WHERE status='held' ORDER BY buy_time
    """).fetchall()
    if not rows:
        print("  空仓")
        return
    for r in rows:
        print(f"  {r[0]} {r[1]} 买{r[2]:.2f} {r[3]}股 {r[4]} [{r[5]}] 高{r[6] or r[2]:.2f}")


def print_closed_today(conn: sqlite3.Connection, target_date: str):
    """今日平仓"""
    rows = conn.execute("""
        SELECT code, name, buy_price, sell_price, shares, pnl, pnl_pct, sell_time, sell_reason
        FROM daemon_positions
        WHERE status='closed' AND sell_date=?
        ORDER BY sell_time
    """, (target_date,)).fetchall()
    if not rows:
        return
    total_pnl = 0
    for r in rows:
        total_pnl += r[5]
        pct = r[6] if r[6] and abs(r[6]) < 1 else (r[6] / 100 if r[6] else 0)
        sign = "+" if pct >= 0 else ""
        reason = (r[8] or "")[:45]
        print(f"  {r[7]} {r[0]} {r[1]} {r[2]:.2f}→{r[3]:.2f} {sign}{pct*100:.1f}% ¥{r[5]:.0f} ({reason})")
    print(f"  平仓PnL合计: ¥{total_pnl:.0f}")


def print_market_timeline(log_lines: list[str]):
    """涨跌比走势 (开盘/10:00/10:30/11:00/11:30/13:00/13:30/14:00/14:30/收盘)"""
    print("  涨跌比走势:")
    checkpoints = {"09:30": "开盘", "10:00": "10:00", "10:30": "10:30",
                   "11:00": "11:00", "11:30": "11:30", "13:00": "13:00",
                   "13:30": "13:30", "14:00": "14:00", "14:30": "14:30", "15:00": "收盘"}
    phase_timeline = []
    for line in log_lines:
        m = re.search(r"\[扫描\] 情绪: (\S+)\([^)]*\) 涨跌(\d+)/(\d+)\((\d+)%\)", line)
        if not m:
            continue
        ts_match = re.match(r"(\d{4}-\d{2}-\d{2} (\d{2}):(\d{2}))", line)
        if not ts_match:
            continue
        ts = ts_match.group(1)
        phase = m.group(1)
        ratio = int(m.group(4))
        mins = int(ts_match.group(2)) * 60 + int(ts_match.group(3))
        phase_timeline.append((ts, phase, ratio, mins))

    # 取各时间点最近的值
    for target_time, label in checkpoints.items():
        th, tm = target_time.split(":")
        target_mins = int(th) * 60 + int(tm)
        best = min(phase_timeline, key=lambda x: abs(x[3] - target_mins)) if phase_timeline else None
        if best and abs(best[3] - target_mins) < 20:
            print(f"    {label:>4}: {best[2]:>3}% ({best[1]})")

    # 情绪阶段变化
    phases_seen = []
    for _, phase, _, _ in phase_timeline:
        if not phases_seen or phases_seen[-1] != phase:
            phases_seen.append(phase)
    if phases_seen:
        print(f"  情绪演变: {' → '.join(phases_seen)}")


def print_reversal_summary(log_lines: list[str]):
    """反转检测记录"""
    signals = Counter()
    last_signal = None
    for line in log_lines:
        m = re.search(r"\[反转检测\] (\S+): (.+)", line)
        if m:
            sig = m.group(1)
            signals[sig] += 1
            last_signal = (sig, m.group(2)[:60])
    if not signals:
        print("  无反转检测记录")
        return
    for sig, cnt in signals.most_common():
        print(f"    {sig}: {cnt}次")
    if last_signal:
        print(f"    最后: {last_signal[0]} — {last_signal[1]}")


def print_sector_rotation(log_lines: list[str]):
    """板块轮动 (从[盘面]日志提取)"""
    first_sectors = None
    last_sectors = None
    for line in log_lines:
        m = re.search(r"\[盘面\] 风格=(\S+) 涨跌比=(\S+)% 权重翻红=(\S+)%(.*)", line)
        if m:
            info = {"style": m.group(1), "ratio": m.group(2), "weight": m.group(3), "sectors": m.group(4).strip()}
            if not first_sectors:
                first_sectors = info
            last_sectors = info
    if not first_sectors:
        print("  无盘面数据")
        return
    print(f"  风格: {first_sectors['style']} → {last_sectors['style']}")
    print(f"  权重翻红: {first_sectors['weight']}% → {last_sectors['weight']}%")
    if first_sectors.get("sectors"):
        print(f"  早盘板块: {first_sectors['sectors'][:80]}")
    if last_sectors.get("sectors"):
        print(f"  尾盘板块: {last_sectors['sectors'][:80]}")


def print_sector_ranking_db(conn: sqlite3.Connection):
    """DB板块涨跌排名 (最新交易日)"""
    latest = conn.execute("SELECT MAX(trade_date) FROM daily_price").fetchone()[0]
    if not latest:
        return
    rows = conn.execute("""
        SELECT sim.industry_name, COUNT(*) as cnt,
               ROUND(AVG(CASE WHEN dp.pre_close > 0 AND ABS((dp.close-dp.pre_close)/dp.pre_close*100) < 25
                          THEN (dp.close-dp.pre_close)/dp.pre_close*100 END), 2) as avg_chg
        FROM daily_price dp
        JOIN stock_industry_mapping sim ON dp.stock_code = sim.stock_code
        WHERE dp.trade_date = ?
        GROUP BY sim.industry_name HAVING cnt >= 5
        ORDER BY avg_chg DESC LIMIT 5
    """, (latest,)).fetchall()
    if not rows:
        return
    print(f"  板块涨幅TOP5 ({latest}):")
    for r in rows:
        print(f"    {r[0]}: {r[2]:+.2f}% ({r[1]}只)")
    rows_down = conn.execute("""
        SELECT sim.industry_name, COUNT(*) as cnt,
               ROUND(AVG(CASE WHEN dp.pre_close > 0 AND ABS((dp.close-dp.pre_close)/dp.pre_close*100) < 25
                          THEN (dp.close-dp.pre_close)/dp.pre_close*100 END), 2) as avg_chg
        FROM daily_price dp
        JOIN stock_industry_mapping sim ON dp.stock_code = sim.stock_code
        WHERE dp.trade_date = ?
        GROUP BY sim.industry_name HAVING cnt >= 5
        ORDER BY avg_chg LIMIT 5
    """, (latest,)).fetchall()
    if rows_down:
        print(f"  板块跌幅TOP5:")
        for r in rows_down:
            print(f"    {r[0]}: {r[2]:+.2f}% ({r[1]}只)")


def save_perception_snapshot(conn: sqlite3.Connection, target_date: str):
    """保存感知快照到emotion_snapshot表"""
    # 补充缺失列
    existing = {d[1] for d in conn.execute("PRAGMA table_info(emotion_snapshot)").fetchall()}
    for col, ctype in [
        ("ratio_now", "REAL"), ("weight_red_pct", "REAL"),
        ("top5_up", "TEXT"), ("top5_down", "TEXT"), ("style", "TEXT"),
        ("reversal_signal", "TEXT"), ("reversal_reason", "TEXT"),
        ("ratio_min", "REAL"),
    ]:
        if col not in existing:
            conn.execute(f"ALTER TABLE emotion_snapshot ADD COLUMN {col} {ctype}")
    conn.commit()

    from src.agent.market_perception import perceive_market, detect_reversal
    mp = perceive_market()
    dr = detect_reversal(market_data=mp)
    import json
    snapshot_time = target_date + " 15:00"
    # 检查是否已有该时间点的记录
    row = conn.execute(
        "SELECT id FROM emotion_snapshot WHERE snapshot_time=?", (snapshot_time,)
    ).fetchone()
    if row:
        conn.execute("""
            UPDATE emotion_snapshot SET ratio_now=?, weight_red_pct=?,
                top5_up=?, top5_down=?, style=?,
                reversal_signal=?, reversal_reason=?,
                dt_count=?, ratio_min=?, ratio=?
            WHERE snapshot_time=?
        """, (
            mp.get("ratio_now", -1), mp.get("weight_red_pct", -1),
            json.dumps(mp.get("top5_up_sectors", [])[:5], ensure_ascii=False),
            json.dumps(mp.get("top5_down_sectors", [])[:5], ensure_ascii=False),
            mp.get("style", ""),
            dr.get("signal", ""), dr.get("reason", ""),
            dr.get("dt_count", -1), dr.get("ratio_min", -1),
            mp.get("ratio_now", -1),
            snapshot_time,
        ))
    else:
        conn.execute("""
            INSERT INTO emotion_snapshot
            (snapshot_time, trade_date, ratio, ratio_now, weight_red_pct,
             top5_up, top5_down, style, reversal_signal, reversal_reason,
             dt_count, ratio_min, phase)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            snapshot_time, target_date, mp.get("ratio_now", -1),
            mp.get("ratio_now", -1), mp.get("weight_red_pct", -1),
            json.dumps(mp.get("top5_up_sectors", [])[:5], ensure_ascii=False),
            json.dumps(mp.get("top5_down_sectors", [])[:5], ensure_ascii=False),
            mp.get("style", ""),
            dr.get("signal", ""), dr.get("reason", ""),
            dr.get("dt_count", -1), dr.get("ratio_min", -1),
            dr.get("signal", ""),
        ))
    conn.commit()
    print(f"  已保存感知快照 → emotion_snapshot ({snapshot_time})"
          f" ratio={mp.get('ratio_now',-1)}% weight={mp.get('weight_red_pct',-1)}%"
          f" 反转={dr.get('signal','')}")


# ═══════════════════════════════════════════════════════════


def main():
    parser = argparse.ArgumentParser(description="收盘总结")
    parser.add_argument("--date", default=date.today().isoformat())
    args = parser.parse_args()
    target = args.date

    print(f"{'='*60}")
    print(f"  收盘总结  {target}")
    print(f"{'='*60}")

    conn = _conn()
    log_lines = _load_log(target)

    print("\n── 账户 ──")
    print_account(conn, target)

    print("\n── 今日交易 ──")
    print_trades(conn, target)

    print("\n── 今日平仓 ──")
    print_closed_today(conn, target)

    print("\n── 当前持仓 ──")
    print_positions(conn)

    print("\n── 市场走势 ──")
    print_market_timeline(log_lines)

    print("\n── 反转检测 ──")
    print_reversal_summary(log_lines)

    print("\n── 盘面感知 ──")
    print_sector_rotation(log_lines)

    print("\n── 板块排名(DB) ──")
    print_sector_ranking_db(conn)

    print("\n── 感知快照 ──")
    try:
        save_perception_snapshot(conn, target)
    except Exception as e:
        print(f"  保存失败: {e}")

    conn.close()
    print(f"\n{'='*60}")


if __name__ == "__main__":
    main()
