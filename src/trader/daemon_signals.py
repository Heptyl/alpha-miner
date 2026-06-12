"""daemon_signals.py — 预告信号管理

从 trading_daemon.py 拆分出的信号管理层。
"""

from __future__ import annotations

import json
import logging
from src.strategy.constants import get_strategy_by_signal
from datetime import datetime, date, timedelta
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.trader.daemon_config import (
    DB_PATH, CURRENT_PERIOD, SIGNAL_DIR, SIGNAL_PENDING,
    SIGNAL_DELAY_SEC, SIGNAL_URGENT_DELAY_SEC, SIGNAL_NOTIFY_SCRIPT,
    SLIPPAGE, MAX_POSITIONS, MAX_AB_POSITIONS,
    STRATEGY_C_CONFIG, B_MAX_POSITIONS, B_POSITION_RATIO,
    B_INITIAL_CAPITAL, C_INITIAL_CAPITAL, C_MAX_POSITIONS,
    C_POSITION_RATIO, A_INITIAL_CAPITAL, A_MAX_POSITIONS,
    A_POSITION_RATIO, AB_POSITION_RATIO,
)
from src.trader.daemon_db import _get_conn, _log_to_db, get_account, _calc_shares, get_held_positions
from src.trader.daemon_notifier import _send_batch_notifications
from src.trader.realtime_quote import get_realtime

logger = logging.getLogger("trading_daemon")


def _read_pending_signals() -> list[dict]:
    """读取待执行的预告信号"""
    try:
        if SIGNAL_PENDING.exists():
            return json.loads(SIGNAL_PENDING.read_text(encoding="utf-8"))
    except Exception:
        pass
    return []




def _write_pending_signals(signals: list[dict]):
    """写入待执行的预告信号(原子写, 防止并发读写冲突)"""
    import tempfile
    tmp = SIGNAL_PENDING.with_suffix(".tmp")
    tmp.write_text(
        json.dumps(signals, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    tmp.rename(SIGNAL_PENDING)  # atomic on same filesystem




def _add_signal(action: str, code: str, name: str, price: float,
                reason: str, signal_type: str, extra: dict = None,
                urgent: bool = False, delay_sec: int = None):
    """添加一个操作预告(延迟执行)

    Args:
        action: "buy" 或 "sell"
        code: 股票代码
        name: 股票名称
        price: 触发时的价格
        reason: 操作原因
        signal_type: 信号类型(ML低吸/涨停确认/止损/...))
        extra: 额外信息(如ml_score, shares等)
        urgent: 紧急信号(止损/最长持有)→60秒执行, 否则5分钟
        delay_sec: 自定义延迟秒数(优先级最高, 用于实时回踩30秒执行)
    """
    from src.trader.daemon_risk import _is_trading_time
    if not _is_trading_time():
        logger.warning(f"非交易时段，跳过信号: {action} {code} {name}")
        return

    now = datetime.now()
    # 延迟优先级: delay_sec参数 > sell/urgent判断
    if delay_sec is not None:
        delay = delay_sec
    elif action == "sell":
        delay = SIGNAL_URGENT_DELAY_SEC  # 60秒
    else:
        delay = SIGNAL_URGENT_DELAY_SEC if urgent else SIGNAL_DELAY_SEC
    execute_at = (now + timedelta(seconds=delay)).strftime("%Y-%m-%d %H:%M:%S")

    _extra = extra or {}
    from src.trader.daemon_db import get_current_run_id, get_current_config_hash
    _extra.setdefault("run_id", get_current_run_id())
    _extra.setdefault("config_hash", get_current_config_hash())

    signal = {
        "id": f"{action}_{code}_{now.strftime('%H%M%S')}",
        "action": action,
        "code": code,
        "name": name,
        "price": price,
        "reason": reason,
        "signal_type": signal_type,
        "created_at": now.strftime("%Y-%m-%d %H:%M:%S"),
        "execute_at": execute_at,
        "status": "pending",
        "urgent": urgent,
        "extra": _extra,
    }

    pending = _read_pending_signals()

    # 去重: 同一只股票同一操作不重复添加(包括已执行的, 防止多轮扫描重复预告)
    today_str = now.strftime("%Y-%m-%d")
    dup_key = f"{action}_{code}"
    if any(f"{s['action']}_{s['code']}" == dup_key
           and s.get("status") in ("pending", "executing")
           for s in pending):
        return  # 已有相同预告, 跳过
    if any(f"{s['action']}_{s['code']}" == dup_key
           and s.get("created_at", "").startswith(today_str)
           for s in pending):
        return  # 今天已经为该code发过同action预告(含已执行), 跳过

    # P0-3: DB级去重 — 防止同一天重复买入同一只股票
    # 过拟合分析: 南威软件同一天被买入两次(-7.5%+-6.7%), 信号文件去重不足以覆盖DB级重复
    if action == "buy":
        try:
            _db = _get_conn()
            _dup = _db.execute(
                "SELECT 1 FROM daemon_trades WHERE action='buy' AND code=? AND trade_date=? LIMIT 1",
                (code, today_str)
            ).fetchone()
            _db.close()
            if _dup:
                logger.info(f"[去重] {code} 今日DB已有买入记录, 跳过重复买入")
                return
        except Exception:
            pass  # daemon_trades表不存在等异常, 放行
    
    pending.append(signal)
    _write_pending_signals(pending)

    tag = "⚠️紧急" if urgent else "预告"
    action_cn = "买入" if action == "buy" else "卖出"
    logger.info(
        f"[{tag}] {action_cn} {name}({code}) ¥{price:.2f} "
        f"[{signal_type}] {reason} — 将于{execute_at}执行"
    )




def _execute_pending_signals() -> list[dict]:
    """检查并执行到期的预告信号, 返回已执行列表"""
    pending = _read_pending_signals()
    if not pending:
        return []

    now = datetime.now()
    executed = []
    remaining = []
    
    # === 批量执行预告, 合并推送(避免iLink限流) ===
    batch_msgs = []
    for sig in pending:
        if sig.get("status") != "pending":
            remaining.append(sig)
            continue

        # 预告时间窗口检查
        execute_at = sig.get("execute_at", "")
        if execute_at:
            try:
                target = datetime.fromisoformat(execute_at)
                if now < target:
                    remaining.append(sig)
                    continue
            except (ValueError, TypeError):
                pass

            # 检查是否在交易时间
            now_hm = now.hour * 100 + now.minute
            in_morning = 930 <= now_hm <= 1130
            in_afternoon = 1300 <= now_hm <= 1500  # 与_is_trading_time统一
            if not (in_morning or in_afternoon):
                sig["status"] = "expired"
                sig["result"] = {"success": False, "reason": f"不在开盘窗口({now_hm})"}
                logger.info(f"[预告过期] {sig['name']} 不在开盘窗口({now_hm}), 跳过")
                executed.append(sig)
                continue

        # 到期了, 执行
        sig["status"] = "executing"
        result = _do_execute_signal(sig)
        sig["status"] = "done" if result.get("success") else "failed"
        sig["executed_at"] = now.strftime("%Y-%m-%d %H:%M:%S")
        sig["result"] = result
        executed.append(sig)

        # 收集消息, 最后统一推送
        if result.get("success"):
            batch_msgs.append((sig, result))
    
    # 批量合并推送: 多条信号合并成一条消息, 避免iLink限流
    if batch_msgs:
        _send_batch_notifications(batch_msgs)

    _write_pending_signals(remaining)
    return executed




def _do_execute_signal(sig: dict) -> dict:
    """实际执行单个预告信号"""
    from src.trader.trading_daemon import execute_buy, execute_sell
    action = sig["action"]
    code = sig["code"]

    try:
        # 重新获取实时价格(预告时和执行时价格可能不同)
        quotes = get_realtime([code])
        q = quotes.get(code)
        if not q or "error" in q or q.get("price", 0) <= 0:
            return {"success": False, "reason": f"无法获取{code}行情"}

        price = q["price"]

        if action == "sell":
            # 找持仓
            positions = get_held_positions()
            pos = next((p for p in positions if p["code"] == code), None)
            if not pos:
                return {"success": False, "reason": f"{code}已不在持仓中"}
            # T+1铁律: 当天买入不能卖
            buy_date = pos.get("buy_date", "")
            today_str = date.today().isoformat()
            if buy_date == today_str:
                logger.warning(f"[T+1拦截] {sig['name']} 买入日{buy_date}, 今天不能卖")
                return {"success": False, "reason": f"T+1锁仓(买入日{buy_date})"}
            trade = execute_sell(pos, price, sig["reason"])
            if trade:
                logger.info(
                    f"[预告执行] 卖出 {sig['name']} {trade['shares']}股@{price:.2f} "
                    f"盈亏{trade['pnl']:+.0f} [{sig['signal_type']}]"
                )
                return {"success": True, "trade": trade, "price": price}
            return {"success": False, "reason": "execute_sell返回None"}

        elif action == "buy":
            # 检查是否还在持仓
            held = get_held_positions()
            held_codes = {p["code"] for p in held}
            if code in held_codes:
                return {"success": False, "reason": f"{code}已在持仓中"}

            if len(held) >= MAX_POSITIONS:
                return {"success": False, "reason": "持仓已满"}

            # 止损冷却: 今天止损的票不再买
            today_str = datetime.now().strftime("%Y-%m-%d")
            try:
                import sqlite3 as _sq2
                _c2 = _sq2.connect(str(DB_PATH))
                _stopped_all = _c2.execute(
                    "SELECT DISTINCT code FROM daemon_positions WHERE status='closed' AND sell_time LIKE ? AND sell_reason LIKE '%止损%'",
                    (f"{today_str}%",)
                ).fetchall()
                _c2.close()
                stopped_codes = {r[0] for r in _stopped_all}
                if code in stopped_codes:
                    return {"success": False, "reason": f"{code}今日已止损, 冷却中"}
            except Exception:
                pass

            # 策略级仓位检查(防止预告生成→执行之间仓位变化)
            sig_type = sig.get("signal_type", "")
            extra = sig.get("extra", {})
            if "暴跌日狙击" in sig_type:
                from src.trader.daemon_strategies import _validate_b_crash_candidate
                reject_reason = _validate_b_crash_candidate(
                    extra.get("crash_day_ret"),
                    extra.get("roe"),
                )
                if reject_reason:
                    return {"success": False, "reason": f"策略B数据无效: {reject_reason}"}

            if "首阴" in sig_type:
                held_a = [h for h in held if "首阴" in h.get("signal_type", "")]
                if len(held_a) >= A_MAX_POSITIONS:
                    return {"success": False, "reason": f"策略A满仓{len(held_a)}只"}
            elif ("回踩低吸" in sig_type or "低开反弹" in sig_type
                  or "暴跌日狙击" in sig_type or "策略B" in sig_type
                  or sig_type in ("涨停低吸", "板块补涨", "涨停确认")):  # [GUARD-BYPASS]
                held_b = [h for h in held if "回踩低吸" in h.get("signal_type", "")
                          or "低开反弹" in h.get("signal_type", "")
                          or "暴跌日狙击" in h.get("signal_type", "")
                          or "策略B" in h.get("signal_type", "")
                          or h.get("signal_type", "") in ("涨停低吸", "板块补涨", "涨停确认")]
                if len(held_b) >= B_MAX_POSITIONS:
                    return {"success": False, "reason": f"策略B满仓{len(held_b)}只"}
            elif ("趋势牛股" in sig_type or "基本面" in sig_type
                  or "策略C" in sig_type or "缩量反包" in sig_type):
                held_d_check = [h for h in held if h.get("signal_type", "").startswith("趋势牛股")
                                or "基本面" in h.get("signal_type", "")
                                or "策略C" in h.get("signal_type", "")
                                or "缩量反包" in h.get("signal_type", "")]
                if len(held_d_check) >= STRATEGY_C_CONFIG['max_positions']:
                    return {"success": False, "reason": f"策略C满仓{len(held_d_check)}只"}

            acct = get_account()
            if acct["cash"] < 1000:
                return {"success": False, "reason": "现金不足"}

            trade = execute_buy(
                code=code,
                name=sig["name"],
                price=price,
                ml_score=extra.get("ml_score", 0),
                signal_type=sig["signal_type"],
                signal_reason=sig["reason"],
                strategy_code=extra.get("strategy_code", ""),
                strategy_version=extra.get("strategy_version", ""),
                run_mode=extra.get("run_mode", ""),
                entry_rule_id=extra.get("entry_rule_id", ""),
                exit_rule_id=extra.get("exit_rule_id", ""),
                candidate_score=extra.get("candidate_score", extra.get("ml_score", 0)),
                market_phase=extra.get("market_phase", ""),
                run_id=extra.get("run_id", ""),
                config_hash=extra.get("config_hash", ""),
            )
            if trade:
                logger.info(
                    f"[预告执行] 买入 {sig['name']} {trade['shares']}股@{price:.2f} [{sig['signal_type']}]"
                )
                return {"success": True, "trade": trade, "price": price}
            return {"success": False, "reason": "execute_buy返回None"}

    except Exception as e:
        logger.error(f"[预告执行] 异常: {e}")
        return {"success": False, "reason": str(e)}
