"""health_check.py — 系统健康检查

检查所有子系统状态, 输出汇总报告。

用法:
  uv run python scripts/health_check.py
"""
from __future__ import annotations

import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

DB_PATH = Path(__file__).resolve().parents[1] / "data" / "alpha_miner.db"


def check(name: str, fn) -> dict:
    """执行单个检查"""
    try:
        result = fn()
        status = result.get("status", "ok")
        msg = result.get("msg", "")
        return {"name": name, "status": status, "msg": msg}
    except Exception as e:
        return {"name": name, "status": "error", "msg": str(e)[:80]}


def _check_db() -> dict:
    conn = sqlite3.connect(str(DB_PATH))
    size_mb = DB_PATH.stat().st_size / 1024 / 1024
    tables = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()
    conn.close()
    return {"status": "ok", "msg": f"{size_mb:.1f}MB, {len(tables)}表"}


def _check_price_data() -> dict:
    conn = sqlite3.connect(str(DB_PATH))
    r = conn.execute(
        "SELECT COUNT(DISTINCT trade_date), MAX(trade_date) FROM daily_price"
    ).fetchone()
    conn.close()
    days, latest = r[0], r[1]
    if not latest:
        return {"status": "error", "msg": "无数据"}
    # 检查最新数据是否在最近3天内(考虑周末/假日)
    latest_dt = datetime.strptime(latest, "%Y-%m-%d")
    diff = (datetime.now() - latest_dt).days
    if diff <= 3:
        return {"status": "ok", "msg": f"{days}天, 最新{latest}"}
    return {"status": "warn", "msg": f"{days}天, 最新{latest}(已过期{diff}天)"}


def _check_zt_pool() -> dict:
    conn = sqlite3.connect(str(DB_PATH))
    r = conn.execute("SELECT COUNT(*), MAX(trade_date) FROM zt_pool").fetchone()
    conn.close()
    return {"status": "ok" if r[0] > 0 else "warn", "msg": f"{r[0]}条, 最新{r[1] or '无'}"}


def _check_fund_flow() -> dict:
    conn = sqlite3.connect(str(DB_PATH))
    r = conn.execute("SELECT COUNT(*), MAX(trade_date) FROM fund_flow").fetchone()
    conn.close()
    return {"status": "ok" if r[0] > 0 else "warn", "msg": f"{r[0]}条, 最新{r[1] or '无'}"}


def _check_lhb() -> dict:
    conn = sqlite3.connect(str(DB_PATH))
    r1 = conn.execute("SELECT COUNT(*), MAX(trade_date) FROM lhb_detail").fetchone()
    r2 = conn.execute("SELECT COUNT(*), MAX(trade_date) FROM lhb_seats").fetchone()
    conn.close()
    return {"status": "ok" if r1[0] > 0 else "warn",
            "msg": f"汇总{r1[0]}条(最新{r1[1] or '无'}), 席位{r2[0]}条(最新{r2[1] or '无'})"}


def _check_lockup() -> dict:
    conn = sqlite3.connect(str(DB_PATH))
    r = conn.execute(
        "SELECT COUNT(*) FROM lockup_calendar WHERE free_date >= date('now')"
    ).fetchone()
    conn.close()
    return {"status": "ok", "msg": f"未来解禁{r[0]}条"}


def _check_collect_log() -> dict:
    conn = sqlite3.connect(str(DB_PATH))
    r = conn.execute(
        "SELECT trade_date, total, success, failed, duration_s "
        "FROM collect_log ORDER BY trade_date DESC LIMIT 1"
    ).fetchone()
    conn.close()
    if not r:
        return {"status": "warn", "msg": "无采集记录"}
    status = "ok" if r[3] == 0 else "warn"
    return {"status": status, "msg": f"{r[0]} 成功{r[2]}/{r[1]} 失败{r[3]} ({r[4]:.0f}s)"}


def _check_llm_client() -> dict:
    try:
        from src.agent.llm_client import get_client
        c = get_client()
        if c.has_provider:
            return {"status": "ok", "msg": f"provider可用, 首选{c.primary_model}"}
        return {"status": "error", "msg": "无LLM provider(需配置API Key)"}
    except Exception as e:
        return {"status": "error", "msg": str(e)[:80]}


def _check_llm_usage() -> dict:
    conn = sqlite3.connect(str(DB_PATH))
    today = datetime.now().strftime("%Y-%m-%d")
    r = conn.execute(
        "SELECT COUNT(*), SUM(CASE WHEN success=1 THEN 1 ELSE 0 END), "
        "SUM(cost), AVG(latency_ms) FROM llm_usage WHERE trade_date = ?",
        (today,),
    ).fetchone()
    conn.close()
    calls, ok, cost, avg_ms = r[0], r[1] or 0, r[2] or 0, r[3] or 0
    if calls == 0:
        return {"status": "ok", "msg": "今日无LLM调用"}
    fail_rate = (calls - ok) / calls * 100
    status = "ok" if fail_rate < 20 else "warn"
    return {"status": status, "msg": f"今日{calls}次 成功{ok} 失败率{fail_rate:.0f}% 成本¥{cost:.4f} 均延迟{avg_ms:.0f}ms"}


def _check_daemon_positions() -> dict:
    conn = sqlite3.connect(str(DB_PATH))
    try:
        r = conn.execute(
            "SELECT COUNT(*) FROM daemon_positions WHERE status='held' AND period=3"
        ).fetchone()
        positions = r[0]
        latest = conn.execute(
            "SELECT MAX(trade_date) FROM daemon_trades WHERE period=3"
        ).fetchone()[0]
        conn.close()
        return {"status": "ok", "msg": f"当前持仓{positions}只, 最近交易{latest or '无'}"}
    except Exception:
        conn.close()
        return {"status": "ok", "msg": "daemon未运行过(daemon_trades将自动创建)"}


def _check_webhook() -> dict:
    try:
        from src.trader.daemon_config import DINGTALK_WEBHOOK_URL, FEISHU_WEBHOOK_URL
        parts = []
        if DINGTALK_WEBHOOK_URL:
            parts.append("钉钉✓")
        if FEISHU_WEBHOOK_URL:
            parts.append("飞书✓")
        if not parts:
            return {"status": "ok", "msg": "未配置(微信推送仍可用)"}
        return {"status": "ok", "msg": " + ".join(parts)}
    except Exception as e:
        return {"status": "error", "msg": str(e)[:60]}


def _check_debate() -> dict:
    try:
        from src.trader.daemon_config import DEBATE_ENABLED
        return {"status": "ok", "msg": f"DEBATE_ENABLED={DEBATE_ENABLED}"}
    except Exception:
        return {"status": "ok", "msg": "未配置"}


STATUS_ICONS = {"ok": "✅", "warn": "⚠️", "error": "❌"}


def main():
    print("=" * 55)
    print(f"  Alpha Miner 系统健康检查")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 55)

    checks = [
        ("数据库", _check_db),
        ("日K线", _check_price_data),
        ("涨停池", _check_zt_pool),
        ("资金流向", _check_fund_flow),
        ("龙虎榜", _check_lhb),
        ("解禁日历", _check_lockup),
        ("采集日志", _check_collect_log),
        ("LLM客户端", _check_llm_client),
        ("LLM用量", _check_llm_usage),
        ("Daemon持仓", _check_daemon_positions),
        ("Webhook通知", _check_webhook),
        ("辩论Agent", _check_debate),
    ]

    ok_count = 0
    warn_count = 0
    err_count = 0

    for name, fn in checks:
        r = check(name, fn)
        icon = STATUS_ICONS.get(r["status"], "?")
        print(f"  {icon} {r['name']:<10s} {r['msg']}")
        if r["status"] == "ok":
            ok_count += 1
        elif r["status"] == "warn":
            warn_count += 1
        else:
            err_count += 1

    print("-" * 55)
    summary = f"  {ok_count}正常 / {warn_count}警告 / {err_count}异常"
    if err_count > 0:
        print(summary + " ← 需要处理")
    elif warn_count > 0:
        print(summary + " ← 建议检查")
    else:
        print(summary + " ← 一切正常")
    print("=" * 55)


if __name__ == "__main__":
    main()
