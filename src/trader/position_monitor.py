"""实盘持仓监控 — 只盯用户真实持仓, 触发止损/止盈告警

核心功能:
1. 每60秒轮询用户5只持仓的实时行情(腾讯API)
2. 计算浮盈浮亏、距止损线距离、日内高点追踪
3. 触发止损告警 / 接近止损预警 / 移动止盈告警
4. 写入监控日志到 monitor_log 表
5. 不做任何买入卖出操作, 纯监控+告警

与 trading_daemon 的区别:
- daemon 是模拟盘(5万虚拟资金), 自动买卖
- monitor 是实盘(用户12万真实持仓), 只看不动

用法:
  uv run python -m src.trader.position_monitor status     # 查看当前状态
  uv run python -m src.trader.position_monitor start      # 启动监控(主循环)
  uv run python -m src.trader.position_monitor start --once  # 单次扫描
"""

import json
import time
import sqlite3
import logging
from pathlib import Path
from datetime import datetime, date
from typing import Optional

from src.trader.realtime_quote import get_realtime

# ============================================================
# 用户持仓 — 统一从 portfolio.json 读取（同源）
# ============================================================
from src.config.portfolio import get_legacy_portfolio_dict as _get_dict
USER_POSITIONS = _get_dict()

# 止盈参数(与稳健策略一致)
TRAILING_STOP_PCT = 0.05   # 移动止盈: 从高点回落5%告警
PROFIT_TARGET_PCT = 0.10   # 止盈目标: +10%

# 预警阈值
STOP_LOSS_WARN_PCT = 3.0   # 距止损线3%时预警

# 数据库
DB_PATH = Path("data/alpha_miner.db")
LOG_DIR = Path("output/monitor")

# 监控间隔
POLL_INTERVAL = 60  # 秒

# 日志
logger = logging.getLogger("position_monitor")


# ============================================================
# 数据库
# ============================================================
def _get_conn():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def init_tables():
    """初始化监控日志表"""
    conn = _get_conn()
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS monitor_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                code TEXT NOT NULL,
                name TEXT,
                price REAL,
                change_pct REAL,
                pnl_pct REAL,
                pnl_amount REAL,
                stop_dist_pct REAL,
                alert_type TEXT,
                alert_msg TEXT,
                day_high REAL,
                day_low REAL,
                high_water_mark REAL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS monitor_high_water (
                code TEXT PRIMARY KEY,
                name TEXT,
                high_price REAL NOT NULL,
                high_date TEXT NOT NULL,
                updated_at TEXT
            )
        """)
        conn.commit()
    finally:
        conn.close()


def _get_high_water(code: str) -> Optional[float]:
    """获取持仓股的最高水位(用于移动止盈)"""
    conn = _get_conn()
    try:
        row = conn.execute(
            "SELECT high_price FROM monitor_high_water WHERE code=?", (code,)
        ).fetchone()
        return row["high_price"] if row else None
    finally:
        conn.close()


def _update_high_water(code: str, name: str, price: float, dt: str):
    """更新最高水位"""
    conn = _get_conn()
    try:
        existing = conn.execute(
            "SELECT high_price, high_date FROM monitor_high_water WHERE code=?",
            (code,)
        ).fetchone()

        if existing is None:
            conn.execute(
                "INSERT INTO monitor_high_water VALUES (?,?,?,?,?)",
                (code, name, price, dt, dt)
            )
        elif price > existing["high_price"]:
            conn.execute(
                "UPDATE monitor_high_water SET high_price=?, high_date=?, updated_at=? WHERE code=?",
                (price, dt, dt, code)
            )
        conn.commit()
    finally:
        conn.close()


def _log_alert(code: str, name: str, price: float, change_pct: float,
               pnl_pct: float, pnl_amount: float, stop_dist_pct: float,
               alert_type: str, alert_msg: str, day_high: float = 0,
               day_low: float = 0, hwm: float = 0):
    """记录告警到数据库"""
    conn = _get_conn()
    try:
        conn.execute(
            "INSERT INTO monitor_log "
            "(timestamp, code, name, price, change_pct, pnl_pct, pnl_amount, "
            "stop_dist_pct, alert_type, alert_msg, day_high, day_low, high_water_mark) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (datetime.now().isoformat(), code, name, price, change_pct,
             pnl_pct, pnl_amount, stop_dist_pct, alert_type, alert_msg,
             day_high, day_low, hwm)
        )
        conn.commit()
    finally:
        conn.close()


# ============================================================
# 核心扫描
# ============================================================
def scan_once() -> dict:
    """扫描全部持仓, 返回状态+告警"""
    init_tables()  # 确保表存在
    codes = list(USER_POSITIONS.keys())
    quotes = get_realtime(codes)

    result = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "positions": [],
        "alerts": [],
        "summary": {
            "total_value": 0,
            "total_cost": 0,
            "total_pnl": 0,
            "total_pnl_pct": 0,
        },
    }

    for code, info in USER_POSITIONS.items():
        q = quotes.get(code)
        if not q or "error" in q:
            result["positions"].append({
                "code": code, "name": info["name"], "error": "行情获取失败",
            })
            continue

        price = q.get("price", 0)
        if price <= 0:
            continue

        cost = info["cost"]
        shares = info["shares"]
        stop_loss = info["stop_loss"]
        name = q.get("name", info["name"])

        # === 计算核心指标 ===
        change_pct = q.get("change_pct_calc", 0)
        pnl_pct = (price / cost - 1) * 100
        market_value = price * shares
        cost_value = cost * shares
        pnl_amount = market_value - cost_value

        # 距止损线
        stop_dist_pct = ((price - stop_loss) / stop_loss) * 100 if stop_loss > 0 else 999

        # 日内高低
        day_high = q.get("high", price)
        day_low = q.get("low", price)

        # === 移动止盈: 追踪最高水位 ===
        today = date.today().isoformat()
        _update_high_water(code, name, price, today)
        hwm = _get_high_water(code) or price

        # 从高点回落幅度
        drawdown_from_high = ((price - hwm) / hwm) * 100 if hwm > 0 else 0

        # === 告警判断 ===
        alerts = []

        # 1. 止损告警
        if price <= stop_loss:
            alert_msg = f"[止损触发] {name} 现价{price:.2f} <= 止损线{stop_loss:.2f}! 请立即卖出!"
            alerts.append({"level": "CRITICAL", "type": "stop_loss", "msg": alert_msg})
            _log_alert(code, name, price, change_pct, pnl_pct, pnl_amount,
                       stop_dist_pct, "stop_loss", alert_msg, day_high, day_low, hwm)

        # 2. 接近止损预警(3%以内)
        elif stop_dist_pct <= STOP_LOSS_WARN_PCT:
            alert_msg = f"[止损预警] {name} 现价{price:.2f}, 距止损线{stop_loss:.2f}仅{stop_dist_pct:.1f}%"
            alerts.append({"level": "WARNING", "type": "near_stop", "msg": alert_msg})
            _log_alert(code, name, price, change_pct, pnl_pct, pnl_amount,
                       stop_dist_pct, "near_stop", alert_msg, day_high, day_low, hwm)

        # 3. 移动止盈告警(从高点回落5%)
        if hwm > cost and drawdown_from_high <= -TRAILING_STOP_PCT * 100:
            alert_msg = (f"[移动止盈] {name} 从高点{hwm:.2f}回落{abs(drawdown_from_high):.1f}%"
                         f" (当前{price:.2f}), 考虑减仓锁利")
            alerts.append({"level": "WARNING", "type": "trailing_stop", "msg": alert_msg})
            _log_alert(code, name, price, change_pct, pnl_pct, pnl_amount,
                       stop_dist_pct, "trailing_stop", alert_msg, day_high, day_low, hwm)

        # 4. 达到止盈目标(+10%)
        if pnl_pct >= PROFIT_TARGET_PCT * 100:
            alert_msg = f"[止盈提醒] {name} 浮盈{pnl_pct:+.1f}%(目标+10%), 可考虑止盈"
            alerts.append({"level": "INFO", "type": "profit_target", "msg": alert_msg})
            _log_alert(code, name, price, change_pct, pnl_pct, pnl_amount,
                       stop_dist_pct, "profit_target", alert_msg, day_high, day_low, hwm)

        # 5. 急涨急跌
        if change_pct >= 5:
            alert_msg = f"[急涨] {name} 日内涨幅{change_pct:+.1f}%"
            alerts.append({"level": "INFO", "type": "surge", "msg": alert_msg})
        elif change_pct <= -5:
            alert_msg = f"[急跌] {name} 日内跌幅{change_pct:+.1f}%"
            alerts.append({"level": "WARNING", "type": "plunge", "msg": alert_msg})

        pos_data = {
            "code": code,
            "name": name,
            "price": price,
            "change_pct": round(change_pct, 2),
            "cost": cost,
            "shares": shares,
            "market_value": round(market_value),
            "cost_value": round(cost_value),
            "pnl_pct": round(pnl_pct, 2),
            "pnl_amount": round(pnl_amount),
            "stop_loss": stop_loss,
            "stop_dist_pct": round(stop_dist_pct, 2),
            "day_high": day_high,
            "day_low": day_low,
            "high_water_mark": round(hwm, 2),
            "drawdown_from_high": round(drawdown_from_high, 2),
            "alerts": alerts,
        }
        result["positions"].append(pos_data)
        result["alerts"].extend(alerts)

        # 累计
        result["summary"]["total_value"] += market_value
        result["summary"]["total_cost"] += cost_value
        result["summary"]["total_pnl"] += pnl_amount

    # 汇总
    total_cost = result["summary"]["total_cost"]
    total_pnl = result["summary"]["total_pnl"]
    result["summary"]["total_pnl_pct"] = round(
        (total_pnl / total_cost * 100) if total_cost > 0 else 0, 2
    )
    result["summary"]["total_value"] = round(result["summary"]["total_value"])
    result["summary"]["total_cost"] = round(result["summary"]["total_cost"])
    result["summary"]["total_pnl"] = round(total_pnl)

    return result


# ============================================================
# 状态输出
# ============================================================
def print_status():
    """打印当前持仓状态"""
    result = scan_once()
    s = result["summary"]

    print(f"\n{'='*70}")
    print(f"实盘持仓监控 — {result['timestamp']}")
    print(f"{'='*70}")

    # 持仓概览
    print(f"\n  总市值: ¥{s['total_value']:>10,}  总成本: ¥{s['total_cost']:>10,}  浮盈亏: ¥{s['total_pnl']:>+10,} ({s['total_pnl_pct']:+.2f}%)")

    # 逐只
    print(f"\n  {'代码':<8} {'名称':<8} {'现价':>8} {'涨跌%':>7} {'成本':>8} {'浮盈亏%':>8} {'浮盈亏':>10} {'距止损':>7} {'高点':>8} {'回落':>6}")
    print(f"  {'-'*80}")

    for p in result["positions"]:
        if "error" in p:
            print(f"  {p['code']:<8} {p.get('name',''):<8} {p['error']}")
            continue
        print(
            f"  {p['code']:<8} {p['name']:<8} {p['price']:>8.2f} {p['change_pct']:>+6.2f}% "
            f"{p['cost']:>8.2f} {p['pnl_pct']:>+7.2f}% {p['pnl_amount']:>+10,} "
            f"{p['stop_dist_pct']:>+6.1f}% {p['high_water_mark']:>8.2f} {p['drawdown_from_high']:>+5.1f}%"
        )

    # 告警
    if result["alerts"]:
        print(f"\n  ⚠️ 告警 ({len(result['alerts'])}条):")
        for a in result["alerts"]:
            icon = "🔴" if a["level"] == "CRITICAL" else "🟡" if a["level"] == "WARNING" else "ℹ️"
            print(f"    {icon} {a['msg']}")
    else:
        print(f"\n  ✅ 无告警")

    print(f"{'='*70}\n")


# ============================================================
# 主循环
# ============================================================
def _is_trading_time() -> bool:
    now = datetime.now()
    h, m = now.hour, now.minute
    weekday = now.weekday()

    if weekday >= 5:  # 周末
        return False

    if (h == 9 and m >= 30) or (h == 10) or (h == 11 and m <= 30):
        return True
    if h in (13, 14):
        return True
    return False


def run_monitor():
    """启动监控主循环"""
    init_tables()
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    # 日志
    fh = logging.FileHandler(
        LOG_DIR / f"monitor_{date.today().isoformat()}.log",
        encoding="utf-8",
    )
    fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(fh)
    logger.setLevel(logging.INFO)

    logger.info("=" * 50)
    logger.info("实盘持仓监控启动")
    logger.info(f"监控持仓: {len(USER_POSITIONS)}只")
    logger.info("=" * 50)

    # 初始化最高水位
    for code, info in USER_POSITIONS.items():
        hwm = _get_high_water(code)
        if hwm is None:
            # 首次启动, 用成本价作为初始水位
            _update_high_water(code, info["name"], info["cost"], date.today().isoformat())
            logger.info(f"初始化水位: {info['name']} = {info['cost']:.2f}(成本价)")

    while True:
        try:
            if not _is_trading_time():
                time.sleep(300)
                continue

            result = scan_once()

            # 有告警时记录
            if result["alerts"]:
                for a in result["alerts"]:
                    level = logging.CRITICAL if a["level"] == "CRITICAL" else \
                            logging.WARNING if a["level"] == "WARNING" else \
                            logging.INFO
                    logger.log(level, a["msg"])

        except KeyboardInterrupt:
            logger.info("监控被用户中断")
            break
        except Exception as e:
            logger.error(f"扫描异常: {e}", exc_info=True)

        time.sleep(POLL_INTERVAL)


# ============================================================
# CLI
# ============================================================
if __name__ == "__main__":
    import click

    @click.group()
    def cli():
        pass

    @cli.command()
    @click.option("--once", is_flag=True, help="单次扫描")
    def start(once):
        """启动实盘持仓监控"""
        if once:
            print_status()
        else:
            run_monitor()

    @cli.command()
    def status():
        """查看当前持仓状态"""
        print_status()

    @cli.command()
    @click.option("--days", default=7, help="查看最近N天的告警")
    def alerts(days):
        """查看历史告警"""
        conn = _get_conn()
        try:
            rows = conn.execute(
                "SELECT * FROM monitor_log WHERE alert_type IS NOT NULL "
                "AND timestamp >= date('now', ?) ORDER BY timestamp DESC LIMIT 100",
                (f"-{days} days",)
            ).fetchall()
            if not rows:
                print(f"最近{days}天无告警记录")
                return
            print(f"\n最近{days}天告警 ({len(rows)}条):")
            for r in rows:
                icon = "🔴" if r["alert_type"] == "stop_loss" else \
                       "🟡" if r["alert_type"] in ("near_stop", "trailing_stop") else "ℹ️"
                print(f"  {icon} {r['timestamp'][:16]} {r['name']} {r['alert_msg']}")
        finally:
            conn.close()

    @cli.command()
    def highwater():
        """查看各持仓最高水位"""
        conn = _get_conn()
        try:
            rows = conn.execute(
                "SELECT * FROM monitor_high_water ORDER BY code"
            ).fetchall()
            if not rows:
                print("无最高水位记录")
                return
            print("\n持仓最高水位:")
            for r in rows:
                info = USER_POSITIONS.get(r["code"], {})
                cost = info.get("cost", 0)
                pct = ((r["high_price"] / cost - 1) * 100) if cost > 0 else 0
                print(f"  {r['code']} {r['name']} 最高{r['high_price']:.2f} ({r['high_date']}) 成本{cost:.2f} 高点收益{pct:+.1f}%")
        finally:
            conn.close()

    cli()
