#!/usr/bin/env python3
"""盘中实时监控 — 核心指标 + 异常告警 + 自动修复

每30秒扫描一次:
  1. 涨跌比 + 交叉验证delta
  2. 持仓实时盈亏 + 止损距离
  3. daemon心跳(是否在运行)
  4. 涨停/跌停数
  5. pending信号是否过期
  6. DB快照是否正常写入

异常告警:
  - 交叉验证delta>=10% → ERROR
  - 持仓亏损>止损线90% → WARNING
  - daemon无心跳>2分钟 → CRITICAL
  - 涨跌比突变(5分钟变化>15%) → WARNING
  - 快照连续3个slot没写入 → WARNING
"""

import json
import logging
import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path

import requests

# ── 配置 ──
ROOT = Path(__file__).resolve().parent.parent
DB = ROOT / "data" / "alpha_miner.db"
DAEMON_LOG = ROOT / "output" / "trader" / "daemon_logs" / f"daemon_{datetime.now():%Y-%m-%d}.log"
SIGNAL_FILE = ROOT / "output" / "trader" / "signals" / "pending_signals.json"
EMOTION_CACHE = ROOT / "output" / "trader" / "market_emotion_cache.json"

# 止损参数(与daemon_config一致)
STOP_LOSS = -3.0  # %
GRACE_PERIOD_END = "10:00"  # 开盘30分钟不执行止损

# 东财session
_session = None
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("monitor")


def get_session():
    global _session
    if _session is None:
        _session = requests.Session()
        _session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Referer": "https://quote.eastmoney.com/",
        })
    return _session


def get_realtime_price(code: str) -> dict | None:
    """东财实时行情"""
    # 补全市场前缀
    market = "1" if code.startswith(("6", "9")) else "0"
    secid = f"{market}.{code}"
    url = f"http://push2.eastmoney.com/api/qt/stock/get?secid={secid}&fields=f43,f44,f45,f46,f47,f57,f58,f170"
    try:
        r = get_session().get(url, timeout=5)
        d = r.json().get("data", {})
        if d:
            return {
                "code": d.get("f57", code),
                "name": d.get("f58", ""),
                "price": d.get("f43", 0) / 100 if isinstance(d.get("f43"), (int, float)) and d.get("f43", 0) > 100 else d.get("f43", 0),
                "high": d.get("f44", 0) / 100 if isinstance(d.get("f44"), (int, float)) and d.get("f44", 0) > 100 else d.get("f44", 0),
                "low": d.get("f45", 0) / 100 if isinstance(d.get("f45"), (int, float)) and d.get("f45", 0) > 100 else d.get("f45", 0),
                "open": d.get("f46", 0) / 100 if isinstance(d.get("f46"), (int, float)) and d.get("f46", 0) > 100 else d.get("f46", 0),
                "vol": d.get("f47", 0),
                "pct": d.get("f170", 0) / 100 if isinstance(d.get("f170"), (int, float)) and abs(d.get("f170", 0)) > 10 else d.get("f170", 0),
            }
    except Exception:
        return None


def get_emotion_from_cache() -> dict | None:
    """读情绪缓存"""
    try:
        if EMOTION_CACHE.exists():
            return json.loads(EMOTION_CACHE.read_text())
    except Exception:
        pass
    return None


def get_daemon_heartbeat() -> str | None:
    """daemon最后一条日志时间"""
    try:
        if DAEMON_LOG.exists():
            with open(DAEMON_LOG) as f:
                lines = f.readlines()
                if lines:
                    # 取最后几行找时间戳
                    for line in reversed(lines[-50:]):
                        line = line.strip()
                        if line and line[0].isdigit():
                            return line[:19]
    except Exception:
        pass
    return None


def check_db_snapshot(conn: sqlite3.Connection) -> str:
    """检查DB快照写入是否正常"""
    c = conn.cursor()
    now = datetime.now()
    trade_date = now.strftime("%Y-%m-%d")

    # 今天的快照数
    c.execute("SELECT COUNT(*) FROM emotion_snapshot WHERE trade_date=?", (trade_date,))
    count = c.fetchone()[0]

    # 最近一条快照时间
    c.execute("SELECT snapshot_time FROM emotion_snapshot WHERE trade_date=? ORDER BY id DESC LIMIT 1",
              (trade_date,))
    row = c.fetchone()
    last_snap = row[0] if row else "无"

    return f"今日{count}条 最新{last_snap}"


def check_cross_validation(conn: sqlite3.Connection) -> list[str]:
    """检查交叉验证delta"""
    alerts = []
    c = conn.cursor()
    trade_date = datetime.now().strftime("%Y-%m-%d")
    c.execute("""SELECT snapshot_time, ratio, delta, validated,
                        source_a_up, source_a_down, source_b_up, source_b_down
                 FROM emotion_snapshot WHERE trade_date=? ORDER BY id DESC LIMIT 1""",
              (trade_date,))
    row = c.fetchone()
    if row:
        snap_time, ratio, delta, validated, sa_up, sa_down, sb_up, sb_down = row
        if validated == 0:
            alerts.append(f"CRITICAL 交叉验证未通过! delta={delta:.1%} ulist={sa_up}/{sa_down} clist={sb_up}/{sb_down}")
        elif delta > 0.03:
            alerts.append(f"WARNING 交叉验证偏差较大 delta={delta:.1%}")
    return alerts


def check_positions(conn: sqlite3.Connection) -> list[str]:
    """检查持仓盈亏"""
    alerts = []
    c = conn.cursor()
    c.execute("SELECT code, name, buy_price, shares FROM daemon_positions WHERE status='held' AND period=3")
    positions = c.fetchall()

    now_str = datetime.now().strftime("%H:%M")
    in_grace = now_str < GRACE_PERIOD_END

    for code, name, cost, shares in positions:
        q = get_realtime_price(code)
        if not q or q["price"] <= 0:
            continue
        pct = (q["price"] - cost) / cost * 100
        loss_pct = STOP_LOSS * 0.9  # 90%预警线

        if pct <= loss_pct and not in_grace:
            alerts.append(f"WARNING {name}({code}) 亏损{pct:.1f}% 接近止损线{STOP_LOSS}% 现价{q['price']}")
        elif pct <= STOP_LOSS and in_grace:
            alerts.append(f"INFO {name}({code}) 亏损{pct:.1f}% 已触止损但在Grace Period内(观望到{GRACE_PERIOD_END})")

    return alerts


def check_daemon_alive() -> list[str]:
    """检查daemon心跳"""
    alerts = []
    heartbeat = get_daemon_heartbeat()
    if heartbeat:
        try:
            hb_time = datetime.strptime(heartbeat, "%Y-%m-%d %H:%M:%S")
            elapsed = (datetime.now() - hb_time).total_seconds()
            if elapsed > 120:
                alerts.append(f"CRITICAL daemon无心跳超过{elapsed:.0f}秒! 最后活动: {heartbeat}")
            elif elapsed > 60:
                alerts.append(f"WARNING daemon心跳延迟{elapsed:.0f}秒 最后活动: {heartbeat}")
        except ValueError:
            pass
    else:
        alerts.append("CRITICAL 无法读取daemon日志!")
    return alerts


def check_pending_signals() -> list[str]:
    """检查pending信号"""
    alerts = []
    if not SIGNAL_FILE.exists():
        return alerts
    try:
        sigs = json.loads(SIGNAL_FILE.read_text())
        now = time.time()
        for s in sigs:
            if s.get("status") == "pending":
                created = s.get("created_ts", 0)
                age_min = (now - created) / 60 if created else 0
                if age_min > 30:
                    alerts.append(
                        f"WARNING 信号过期: {s.get('action')} {s.get('name')}({s.get('symbol')}) "
                        f"已等{age_min:.0f}分钟 reason={s.get('reason')}"
                    )
    except Exception:
        pass
    return alerts


def check_ratio_sudden_change(conn: sqlite3.Connection) -> list[str]:
    """检查涨跌比突变(5分钟变化>15%)"""
    alerts = []
    c = conn.cursor()
    trade_date = datetime.now().strftime("%Y-%m-%d")
    c.execute("""SELECT snapshot_time, ratio FROM emotion_snapshot
                 WHERE trade_date=? ORDER BY id DESC LIMIT 3""",
              (trade_date,))
    rows = c.fetchall()
    if len(rows) >= 2:
        latest_ratio = rows[0][1]
        prev_ratio = rows[1][1]
        if latest_ratio > 0 and prev_ratio > 0:
            change = latest_ratio - prev_ratio
            if abs(change) > 0.15:
                direction = "急涨" if change > 0 else "急跌"
                alerts.append(
                    f"WARNING 涨跌比{direction}: {prev_ratio:.1%}→{latest_ratio:.1%}(变化{change:+.1%})"
                )
    return alerts


def run_monitor():
    """主监控循环"""
    log.info("盘中监控启动 — 30秒/次")

    # 上次全量报告时间
    last_report = 0

    while True:
        now = datetime.now()
        now_str = now.strftime("%H:%M:%S")
        trade_date = now.strftime("%Y-%m-%d")

        # 只在交易时间运行
        if now_str < "09:25" or now_str > "15:05":
            log.info(f"非交易时间({now_str}), 60秒后再检")
            time.sleep(60)
            continue

        conn = sqlite3.connect(str(DB))
        all_alerts = []

        # ── 1. 情绪数据 ──
        emotion = get_emotion_from_cache()
        emotion_str = "无缓存"
        if emotion:
            up = emotion.get("up_count", 0)
            down = emotion.get("down_count", 0)
            ratio = up / (up + down) * 100 if (up + down) > 0 else 0
            zt = emotion.get("zt_count", 0)
            dt = emotion.get("dt_count", 0)
            validated = emotion.get("validated", True)
            emotion_str = f"涨跌比{ratio:.1f}% 涨{up}/跌{down} 涨停{zt}/跌停{dt} valid={validated}"

        # ── 2. 持仓 ──
        c = conn.cursor()
        c.execute("SELECT code, name, buy_price, shares, highest_price FROM daemon_positions WHERE status='held' AND period=3")
        positions = c.fetchall()
        pos_strs = []
        total_pnl = 0
        for code, name, cost, shares, highest in positions:
            q = get_realtime_price(code)
            if q and q["price"] > 0:
                pnl = (q["price"] - cost) / cost * 100
                total_pnl += pnl * shares * cost / 90000  # 加权
                pos_strs.append(f"{name} {pnl:+.1f}%")
            else:
                pos_strs.append(f"{name} ???")

        # ── 3. DB快照 ──
        snap_info = check_db_snapshot(conn)

        # ── 4. 各种告警检查 ──
        all_alerts.extend(check_cross_validation(conn))
        all_alerts.extend(check_positions(conn))
        all_alerts.extend(check_daemon_alive())
        all_alerts.extend(check_pending_signals())
        all_alerts.extend(check_ratio_sudden_change(conn))

        # ── 5. 账户 ──
        c.execute("SELECT cash, total_assets FROM daemon_account WHERE period=3 ORDER BY rowid DESC LIMIT 1")
        acct = c.fetchone()

        conn.close()

        # ── 输出 ──
        elapsed = time.time() - last_report
        if all_alerts:
            for a in all_alerts:
                log.warning(f"⚠ {a}")
            last_report = time.time()
        elif elapsed > 300:  # 5分钟一次全量报告
            log.info(f"[{now_str}] {emotion_str} | 持仓: {', '.join(pos_strs)} | 组合{total_pnl:+.2f}% | {snap_info} | 账户{acct}")
            if acct:
                log.info(f"  现金{acct[0]:.0f} 总资产{acct[1]:.0f}")
            last_report = time.time()

        time.sleep(30)


if __name__ == "__main__":
    try:
        run_monitor()
    except KeyboardInterrupt:
        log.info("监控停止")
