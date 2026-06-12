"""盘中自动交易守护进程 — 模拟盘自动买卖

盘中常驻运行，每60秒轮询一次:
  1. 监控持仓: 检查止损/移动止盈 → 自动卖出
  2. 监控候选: ML候选股买点信号 → 自动买入
  3. 所有操作写入数据库(精确到秒)
  4. 操作流水可在Web页面实时查看

CLI:
  uv run python -m src.trader.trading_daemon           # 启动守护进程
  uv run python -m src.trader.trading_daemon --once     # 单次扫描(调试用)
  uv run python -m src.trader.trading_daemon --status   # 查看当前状态

参考:
  - vnpy CTA策略的突破/回踩信号体系
  - 淘股吧短线高手买点模式(放量突破/回踩均线/超跌反弹)
  - Qlib回测引擎的交易成本模型
"""

from __future__ import annotations

import json
import os
import signal
import sqlite3
import sys
import threading
import time
import logging
from dataclasses import dataclass, field, asdict
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.trader.realtime_quote import get_realtime
from src.trader.signal_monitor import (
    get_daily_data as _get_daily_data,
    compute_technical_signals as _compute_technical_signals,
    compute_support_resistance as _compute_support_resistance,
    PORTFOLIO,
)

# ---------------------------------------------------------------------------
# 配置 — 全部纯常量从 daemon_config 导入
# ---------------------------------------------------------------------------
from src.trader.daemon_config import (  # noqa: F401  re-export for backward compat
    PROJECT_ROOT, DB_PATH, PRED_PATH, LOG_DIR,
    CURRENT_PERIOD,
    B_INITIAL_CAPITAL, B_MAX_POSITIONS, B_POSITION_RATIO, B_STOP_LOSS_PCT,
    C_INITIAL_CAPITAL, C_MAX_POSITIONS, C_POSITION_RATIO, C_STOP_LOSS_PCT,
    A_INITIAL_CAPITAL, A_MAX_POSITIONS, A_POSITION_RATIO,
    INITIAL_CAPITAL, MAX_POSITIONS, MAX_AB_POSITIONS, AB_POSITION_RATIO,
    C_POSITION_RATIO_OLD, MIN_CASH_RATIO,
    EBB_COOLDOWN_MINUTES, MAX_SINGLE_RATIO, STOP_LOSS_PCT,
    COMMISSION_RATE, STAMP_DUTY_RATE,
    GRACE_PERIOD_ENABLED, GRACE_PERIOD_MINUTES, HARD_STOP_PCT,
    MAX_SAME_INDUSTRY, SLIPPAGE, MIN_COMMISSION, DAILY_LOSS_LIMIT, MIN_ML_SCORE,
    STRATEGY_C_CONFIG, OPEN_CHG_FILTER, STRATEGY_B_CONFIG,
    SELL_PARAMS,
    POLL_INTERVAL, POLL_INTERVAL_TRADING,
    MARKET_OPEN_AM, MARKET_CLOSE_AM, MARKET_OPEN_PM, MARKET_CLOSE_PM,
    SIGNAL_DIR, SIGNAL_PENDING, SIGNAL_DELAY_SEC, SIGNAL_URGENT_DELAY_SEC,
    SIGNAL_NOTIFY_SCRIPT,
    BREAKOUT_MIN_CHG, BREAKOUT_MAX_CHG, BREAKOUT_VOL_RATIO,
    PULLBACK_MA_DIST, PULLBACK_VOL_RATIO,
    OVERSOLD_MIN_DROP, OVERSOLD_MAX_DROP, OVERSOLD_RSI,
)

# 运行时可变状态


# 确保信号目录存在(daemon 启动时执行)
SIGNAL_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("trading_daemon")


# ---------------------------------------------------------------------------
# 数据库
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# 从子模块重新导出 — 保持向后兼容
# ---------------------------------------------------------------------------
from src.trader.daemon_db import (  # noqa: F401
    _get_conn, init_tables, get_account, _update_account_value,
    get_held_positions, _calc_commission, _calc_shares,
    _log_to_db, _is_new_day, _reset_daily_pnl, _count_trading_days,
    save_candidate_snapshots, get_strategy_metadata, infer_strategy_code,
    record_shadow_signal, get_shadow_signals_by_date,
    upsert_strategy_performance,
    register_all_strategy_definitions,
    create_daemon_run, update_daemon_run,
    set_run_context, get_current_run_id, get_current_config_hash,
)
from src.trader.daemon_signals import (  # noqa: F401
    _read_pending_signals, _write_pending_signals, _add_signal,
    _execute_pending_signals, _do_execute_signal,
)
from src.trader.daemon_notifier import (  # noqa: F401
    _send_batch_notifications, _send_trade_notification,
)
from src.trader.daemon_strategies import (  # noqa: F401
    _try_upgrade_positions,
    _get_b_watchlist, _check_b_pullback_realtime,
    _check_c_trend_realtime,
)
from src.strategy.strategy_c_v2 import (  # noqa: F401
    get_strategy_c_v2_candidates as get_strategy_c_candidates,
)
from src.scorer.fundamental_scorer import (  # noqa: F401
    TIER1_INDUSTRIES, TIER2_INDUSTRIES, TIER3_INDUSTRIES,
)

_MAX_SAME_TIER = 2  # 同Tier(算力核心/算力配套/AI应用)最多持仓2只


def _get_tier(industry_name: str) -> str:
    if industry_name in TIER1_INDUSTRIES:
        return "Tier1"
    elif industry_name in TIER2_INDUSTRIES:
        return "Tier2"
    elif industry_name in TIER3_INDUSTRIES:
        return "Tier3"
    return "Other"


def _check_tier_concentration(code: str, held_positions: list) -> bool:
    """检查策略C同Tier集中度, 返回True=可买入, False=超限"""
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    try:
        # 候选股票行业
        row = conn.execute(
            "SELECT industry_name FROM stock_industry_mapping WHERE stock_code=?",
            (code,)
        ).fetchone()
        if not row:
            return True
        cand_tier = _get_tier(row[0])
        if cand_tier == "Other":
            return True  # 非AI赛道不限制

        # 已持仓的Tier分布(只看策略C的持仓)
        c_held = [p for p in held_positions
                  if "趋势牛股" in p.get("signal_type", "") or "基本面" in p.get("signal_type", "")]
        same_tier = 0
        for p in c_held:
            p_row = conn.execute(
                "SELECT industry_name FROM stock_industry_mapping WHERE stock_code=?",
                (p["code"],)
            ).fetchone()
            if p_row and _get_tier(p_row[0]) == cand_tier:
                same_tier += 1

        if same_tier >= _MAX_SAME_TIER:
            logger.info(f"[Tier集中度] {code}({row[0]}={cand_tier}), 已持{same_tier}只同Tier, 超限{_MAX_SAME_TIER}")
            return False
        return True
    finally:
        conn.close()
from src.trader.daemon_sell_strategies import (  # noqa: F401
    _check_sell_strategy_c, _check_sell_strategy_a, _check_sell_strategy_b,
)
from src.trader.daemon_risk import (  # noqa: F401
    _check_lhb_filter, _is_trading_time, _is_grace_period,
    _check_industry_concentration, _check_market_sentiment,
    _check_consecutive_losses, _check_monthly_drawdown,
    _market_crash_clear, check_circuit_breaker,
    check_c_consecutive_stops, check_weekly_drawdown,
)

NEW_BUY_CUTOFF_HM = 1457
HEARTBEAT_FILE = LOG_DIR / "daemon.heartbeat.json"
MAINTENANCE_PAUSE_FILE = LOG_DIR / "daemon.pause"


def _is_new_buy_allowed_now() -> bool:
    """Only allow new buy signals before the late-afternoon cutoff."""
    if not _is_trading_time():
        return False
    now_hm = datetime.now().hour * 100 + datetime.now().minute
    return (930 <= now_hm < 1130) or (1300 <= now_hm < NEW_BUY_CUTOFF_HM)


def _strategy_allows_paper_orders(strategy_code: str) -> bool:
    """Return whether a strategy may create paper-order signals."""
    return get_strategy_metadata(strategy_code)["run_mode"] == "paper"


def _write_heartbeat(state: str, **details) -> None:
    """Atomically publish main-loop progress for the external watchdog."""
    payload = {
        "pid": os.getpid(),
        "timestamp": time.time(),
        "time": datetime.now().isoformat(timespec="seconds"),
        "state": state,
        **details,
    }
    tmp_file = HEARTBEAT_FILE.with_suffix(".tmp")
    tmp_file.write_text(
        json.dumps(payload, ensure_ascii=False, sort_keys=True),
        encoding="utf-8",
    )
    tmp_file.replace(HEARTBEAT_FILE)


def execute_sell(pos: dict, price: float, reason: str) -> dict:
    """执行卖出, 记录到数据库
    
    reason可能包含预告时的预估pnl(如"策略A止损: 跌-3.5%"),
    但实际卖出价可能不同(预告后价格变动)。
    用实际pnl覆盖reason里的预估数字, 确保记录准确。
    """
    import re
    
    now = datetime.now()
    trade_time = now.strftime("%Y-%m-%d %H:%M:%S")
    trade_date = now.strftime("%Y-%m-%d")

    shares = pos["shares"]
    sell_amount = price * shares
    commission, stamp_duty = _calc_commission(sell_amount, is_sell=True)
    actual_receive = sell_amount - commission - stamp_duty

    buy_cost = pos["cost"]
    pnl = actual_receive - buy_cost
    pnl_pct = (price / pos["buy_price"] - 1) * 100
    
    # 用实际pnl覆盖reason里的预估pnl(预告价≠执行价)
    # 替换 "跌-3.5%" / "收益+2.1%" 等模式
    actual_pnl_str = f"实际{pnl_pct:+.1f}%"
    reason = re.sub(r'(跌|收益|盈亏)[+-]?\d+\.?\d*%', rf'\g<1>{pnl_pct:+.1f}%', reason)
    # 追加实际pnl(如果reason里没有匹配到pnl模式)
    if f"{pnl_pct:+.1f}%" not in reason:
        reason = f"{reason} ({actual_pnl_str})"

    conn = _get_conn()
    try:
        # 更新持仓状态
        strategy_code = pos.get("strategy_code") or infer_strategy_code(pos.get("signal_type", ""))
        meta = get_strategy_metadata(strategy_code)
        strategy_version = pos.get("strategy_version") or meta["strategy_version"]
        run_mode = pos.get("run_mode") or meta["run_mode"]
        entry_rule_id = pos.get("entry_rule_id") or meta["entry_rule_id"]
        exit_rule_id = pos.get("exit_rule_id") or meta["exit_rule_id"]
        candidate_score = pos.get("candidate_score", pos.get("ml_score", 0))
        market_phase = pos.get("market_phase", "")

        conn.execute("""
            UPDATE daemon_positions
            SET status='closed', sell_time=?, sell_date=?, sell_price=?,
                sell_reason=?, pnl=?, pnl_pct=?, sell_commission=?, sell_stamp_duty=?,
                exit_rule_id=?
            WHERE id = ?
        """, (trade_time, trade_date, price, reason,
              round(pnl, 2), round(pnl_pct, 2), commission, stamp_duty,
              exit_rule_id, pos["id"]))

        # 记录交易
        conn.execute("""
            INSERT INTO daemon_trades
            (code, name, action, trade_time, trade_date, price, shares,
             amount, commission, stamp_duty, reason, signal_type, ml_score, pnl, pnl_pct, period,
             strategy_code, strategy_version, run_mode, entry_rule_id, exit_rule_id,
             candidate_score, market_phase, run_id, config_hash)
            VALUES (?, ?, 'sell', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (pos["code"], pos["name"], trade_time, trade_date, price, shares,
              round(sell_amount, 2), commission, stamp_duty, reason,
              pos.get("signal_type", ""), pos.get("ml_score", 0),
              round(pnl, 2), round(pnl_pct, 2), CURRENT_PERIOD,
              strategy_code, strategy_version, run_mode, entry_rule_id,
              exit_rule_id, candidate_score, market_phase,
              get_current_run_id(), get_current_config_hash()))

        # === 退出观察记录 === (仅trailing止盈/退潮/移动止盈)
        _obs_reasons = ('trailing止盈', '移动止盈', '退潮')
        if any(kw in reason for kw in _obs_reasons):
            try:
                from src.trader.daemon_db import insert_exit_observation
                # 推断策略归属
                sig_t = pos.get('signal_type', '')
                if '趋势牛股' in sig_t or '基本面驱动' in sig_t or '策略C' in sig_t:
                    _obs_strategy = 'C'
                elif '首阴' in sig_t or '策略A' in sig_t:
                    _obs_strategy = 'A'
                else:
                    _obs_strategy = 'B'
                insert_exit_observation(
                    sell_date=trade_date,
                    code=pos['code'],
                    name=pos['name'],
                    strategy=_obs_strategy,
                    sell_reason=reason,
                    sell_price=price,
                    buy_price=pos['buy_price'],
                    shares=shares,
                    pnl_pct_at_sell=round(pnl_pct, 2),
                    highest_price_before_sell=pos.get('highest_price', pos['buy_price']),
                    market_phase='',
                    raw_json=json.dumps({
                        'pos_id': pos['id'],
                        'signal_type': sig_t,
                        'hold_days': pos.get('hold_days', 0),
                        'pnl': round(pnl, 2),
                    }, ensure_ascii=False),
                )
            except Exception as _obs_err:
                logger.debug(f'退出观察记录失败: {_obs_err}')

        # 更新账户(复用同一连接, 避免嵌套死锁)
        acct = get_account(conn=conn)
        new_cash = acct["cash"] + actual_receive
        is_win = 1 if pnl > 0 else 0
        new_daily_pnl = acct.get("daily_pnl", 0) + pnl
        new_cum_pnl = acct.get("cumulative_pnl", 0) + pnl
        new_total_trades = acct.get("total_trades", 0) + 1
        new_win_trades = acct.get("win_trades", 0) + is_win

        conn.execute("""
            INSERT OR REPLACE INTO daemon_account
            (date, cash, market_value, total_assets, daily_pnl, cumulative_pnl,
             total_trades, win_trades, positions_count, period)
            VALUES (?, ?, 0, ?, ?, ?, ?, ?, 
                    (SELECT COUNT(*) FROM daemon_positions WHERE status='held' AND period=?), ?)
        """, (trade_date, round(new_cash, 2), round(new_cash, 2),
              round(new_daily_pnl, 2), round(new_cum_pnl, 2),
              new_total_trades, new_win_trades, CURRENT_PERIOD, CURRENT_PERIOD))
        conn.commit()
        # 卖出去重验证(防止竞态导致重复卖出)
        verify = conn.execute(
            "SELECT status FROM daemon_positions WHERE id=?", (pos["id"],)
        ).fetchone()
        if not verify or verify[0] != 'closed':
            logger.error(f"[严重] 卖出后持仓状态异常: {pos['code']} id={pos['id']} status={verify}")

        return {
            "action": "sell",
            "code": pos["code"],
            "name": pos["name"],
            "price": price,
            "shares": shares,
            "buy_price": pos.get("buy_price", 0),
            "hold_days": pos.get("hold_days", 0),
            "strategy": pos.get("strategy", pos.get("signal_type", "")),
            "pnl": round(pnl, 2),
            "pnl_pct": round(pnl_pct, 2),
            "reason": reason,
            "trade_time": trade_time,
        }
    finally:
        conn.close()



def execute_buy(code: str, name: str, price: float, ml_score: float,
                signal_type: str, signal_reason: str,
                strategy_code: str = "", strategy_version: str = "",
                run_mode: str = "", entry_rule_id: str = "",
                exit_rule_id: str = "", candidate_score: float = 0,
                market_phase: str = "", run_id: str = "",
                config_hash: str = "") -> Optional[dict]:
    """执行买入, 记录到数据库"""
    strategy_code = strategy_code or infer_strategy_code(signal_type)
    meta = get_strategy_metadata(strategy_code)
    strategy_version = strategy_version or meta["strategy_version"]
    run_mode = run_mode or meta["run_mode"]
    entry_rule_id = entry_rule_id or meta["entry_rule_id"]
    exit_rule_id = exit_rule_id or meta["exit_rule_id"]
    candidate_score = candidate_score or ml_score
    run_id = run_id or get_current_run_id()
    config_hash = config_hash or get_current_config_hash()

    if run_mode != "paper":
        logger.info(f"[策略状态] {strategy_version} run_mode={run_mode}, 不执行模拟买入")
        return None

    acct = get_account()
    held = get_held_positions()

    # 检查约束
    if len(held) >= MAX_POSITIONS:
        return None

    # 检查是否已持有
    for h in held:
        if h["code"] == code:
            return None

    # 计算买入量 — 按比例配仓, 资金变了自动适配
    # 三策略独立仓位: A=3万×33%=1万, B=1万×100%=1万, C=5万×33%=1.67万
    is_strategy_c = strategy_code == "C" or "趋势牛股" in signal_type or "基本面" in signal_type or "策略C" in signal_type
    is_strategy_a = strategy_code == "A" or "首阴" in signal_type or "策略A" in signal_type
    total_assets = acct.get("total_assets", acct["cash"])
    
    # 仓位计算用各自策略的初始资金,不是总资产
    if is_strategy_c:
        strategy_capital = C_INITIAL_CAPITAL  # 3万
        target_ratio = C_POSITION_RATIO       # 33%
    elif is_strategy_a:
        strategy_capital = A_INITIAL_CAPITAL  # 3万
        target_ratio = A_POSITION_RATIO       # 33%
    else:
        strategy_capital = B_INITIAL_CAPITAL  # 3万
        target_ratio = B_POSITION_RATIO       # 33%
    
    # 目标金额 = 策略资金 × 配仓比例, 但不超过可用现金-最低保留
    min_cash = total_assets * MIN_CASH_RATIO
    available = max(acct["cash"] - min_cash, 0)
    target_amount = min(strategy_capital * target_ratio, available)
    
    shares = _calc_shares(price, target_amount)
    # 高价股超配保护: 不够100股但100股金额 ≤ 总资产×25%, 允许超配
    if shares < 100:
        min_buy = price * 100  # 买100股需要的钱
        if min_buy <= total_assets * MAX_SINGLE_RATIO and min_buy <= available:
            shares = 100
    if shares < 100:
        return None

    buy_amount = price * shares
    commission, _ = _calc_commission(buy_amount, is_sell=False)
    total_cost = buy_amount + commission

    if total_cost > acct["cash"]:
        shares = _calc_shares(price, acct["cash"] * 0.95 - commission)
        if shares < 100:
            return None
        buy_amount = price * shares
        commission, _ = _calc_commission(buy_amount, is_sell=False)
        total_cost = buy_amount + commission

    # 滑点
    actual_price = round(price * (1 + SLIPPAGE), 3)
    actual_amount = actual_price * shares
    commission, _ = _calc_commission(actual_amount, is_sell=False)
    total_cost = actual_amount + commission

    now = datetime.now()
    trade_time = now.strftime("%Y-%m-%d %H:%M:%S")
    trade_date = now.strftime("%Y-%m-%d")

    conn = _get_conn()
    try:
        # 记录持仓
        conn.execute("""
            INSERT INTO daemon_positions
            (code, name, buy_time, buy_date, buy_price, shares, cost,
             commission, ml_score, signal_type, signal_reason, highest_price, status, period,
             strategy_code, strategy_version, run_mode, entry_rule_id, exit_rule_id,
             candidate_score, market_phase, run_id, config_hash)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'held', ?,
                    ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (code, name, trade_time, trade_date, actual_price, shares,
              round(total_cost, 2), commission, ml_score,
              signal_type, signal_reason, actual_price, CURRENT_PERIOD,
              strategy_code, strategy_version, run_mode, entry_rule_id,
              exit_rule_id, candidate_score, market_phase,
              run_id, config_hash))

        # 记录交易
        conn.execute("""
            INSERT INTO daemon_trades
            (code, name, action, trade_time, trade_date, price, shares,
             amount, commission, stamp_duty, reason, signal_type, ml_score, period,
             strategy_code, strategy_version, run_mode, entry_rule_id, exit_rule_id,
             candidate_score, market_phase, run_id, config_hash)
            VALUES (?, ?, 'buy', ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (code, name, trade_time, trade_date, actual_price, shares,
              round(actual_amount, 2), commission, signal_reason, signal_type,
              ml_score, CURRENT_PERIOD, strategy_code, strategy_version,
              run_mode, entry_rule_id, exit_rule_id, candidate_score,
              market_phase, run_id, config_hash))

        # 更新账户
        new_cash = acct["cash"] - total_cost
        conn.execute("""
            INSERT OR REPLACE INTO daemon_account
            (date, cash, market_value, total_assets, daily_pnl, cumulative_pnl,
             total_trades, win_trades, positions_count, period)
            VALUES (?, ?, 0, ?, ?, ?, ?, ?,
                    (SELECT COUNT(*) FROM daemon_positions WHERE status='held' AND period=?), ?)
        """, (trade_date, round(new_cash, 2), round(new_cash, 2),
              acct.get("daily_pnl", 0), acct.get("cumulative_pnl", 0),
              acct.get("total_trades", 0), acct.get("win_trades", 0), CURRENT_PERIOD, CURRENT_PERIOD))

        conn.commit()

        return {
            "action": "buy",
            "code": code,
            "name": name,
            "price": actual_price,
            "shares": shares,
            "cost": round(total_cost, 2),
            "signal_type": signal_type,
            "signal_reason": signal_reason,
            "ml_score": ml_score,
            "strategy_code": strategy_code,
            "strategy_version": strategy_version,
            "run_mode": run_mode,
            "entry_rule_id": entry_rule_id,
            "exit_rule_id": exit_rule_id,
            "trade_time": trade_time,
        }
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 买点信号判断 (核心策略)
# ---------------------------------------------------------------------------

def check_buy_signals(code: str, quote: dict, daily_df) -> Optional[dict]:
    """检查买入信号

    三种买点(优先级从高到低):
    1. 放量突破: 涨幅2-5% + 量比>2 + MACD多头/金叉 + RSI<70
    2. 回踩支撑: 距MA5/MA20<2% + 缩量 + RSI 30-50
    3. 超跌反弹: 跌幅-3%~-5% + RSI<30 + 布林下轨附近 + 5分钟企稳

    Returns:
        None = 无信号
        dict = {signal_type, signal_reason, score}
    """
    if daily_df is None or len(daily_df) < 20:
        return None

    # 把今天实时行情追加为最新K线(让技术指标反映盘中状态)
    price = quote.get("price", 0)
    if price <= 0:
        return None

    import copy
    import pandas as pd
    df_live = daily_df.copy()
    today_row = {
        "trade_date": datetime.now().strftime("%Y-%m-%d"),
        "open": quote.get("open", price),
        "high": max(quote.get("high", price), price),
        "low": min(quote.get("low", price), price),
        "close": price,
        "volume": quote.get("volume", 0),
    }
    # _get_daily_data返回日期升序(旧→新), _compute_technical_signals用iloc[-1]取最新
    # 所以直接追加today到末尾
    df_live = pd.concat([df_live, pd.DataFrame([today_row])], ignore_index=True)

    signals = _compute_technical_signals(df_live)

    chg_pct = quote.get("change_pct_calc", 0)
    rsi = signals.get("rsi_14", 50)
    macd = signals.get("macd", "")
    vol_signal = signals.get("volume", "")
    ma5 = signals.get("ma5", price)
    ma20 = signals.get("ma20", price)
    bollinger = signals.get("bollinger", "")
    kdj = signals.get("kdj", "")

    # === 盘中量比修正 ===
    # 日K线里今天的成交量只是部分量(盘中), 需要用时间进度缩放
    vol_ratio = signals.get("vol_ratio", 1.0)
    now = datetime.now()
    minutes_since_open = 0
    total_trading_minutes = 240  # 4小时 = 240分钟
    h, m = now.hour, now.minute
    # 上午 9:30-11:30 = 120分钟
    if h == 9 and m >= 30:
        minutes_since_open = m - 30
    elif h == 10:
        minutes_since_open = 30 + m
    elif h == 11 and m <= 30:
        minutes_since_open = 90 + m
    # 下午 13:00-15:00 = 120分钟
    elif h == 13:
        minutes_since_open = 120 + m
    elif h == 14:
        minutes_since_open = 180 + m
    elif h == 15:
        minutes_since_open = 240

    if minutes_since_open > 0 and minutes_since_open < total_trading_minutes:
        time_progress = minutes_since_open / total_trading_minutes
        # 预估全天成交量 = 当前量 / 时间进度
        # 修正量比 = 预估全天量 / 前5天均量
        vol_ratio = vol_ratio / time_progress
        logger.debug(
            f"量比修正: 原始{signals.get('vol_ratio', 1.0):.1f} → "
            f"修正后{vol_ratio:.1f} (时间进度{time_progress:.0%})"
        )

    # --- 买点1: 放量突破 ---
    if (BREAKOUT_MIN_CHG <= chg_pct <= BREAKOUT_MAX_CHG
            and vol_ratio >= BREAKOUT_VOL_RATIO
            and macd in ("金叉", "多头")
            and rsi < 80
            and rsi > 30):
        return {
            "signal_type": "breakout",
            "signal_reason": f"放量突破: 涨{chg_pct:+.1f}% 量比{vol_ratio:.1f} MACD{macd} RSI{rsi:.0f}",
            "score": 3,  # 3分=最高优先级
        }

    # --- 买点2: 回踩支撑 ---
    dist_ma5 = abs(price / ma5 - 1) if ma5 > 0 else 1
    dist_ma20 = abs(price / ma20 - 1) if ma20 > 0 else 1
    ma5_above = price > ma5  # 价格在MA5上方(上涨趋势)

    if (dist_ma5 <= PULLBACK_MA_DIST and ma5_above
            and vol_ratio < PULLBACK_VOL_RATIO
            and 30 <= rsi <= 55
            and macd in ("金叉", "多头", "中性")):
        return {
            "signal_type": "pullback",
            "signal_reason": f"回踩MA5支撑: 距MA5 {dist_ma5*100:.1f}% 缩量{vol_ratio:.1f} RSI{rsi:.0f}",
            "score": 2,
        }

    if (dist_ma20 <= PULLBACK_MA_DIST
            and vol_ratio < PULLBACK_VOL_RATIO
            and 25 <= rsi <= 50
            and macd in ("金叉", "多头")):
        return {
            "signal_type": "pullback",
            "signal_reason": f"回踩MA20支撑: 距MA20 {dist_ma20*100:.1f}% MACD{macd} RSI{rsi:.0f}",
            "score": 2,
        }

    # --- 买点3: 超跌反弹 ---
    if (OVERSOLD_MAX_DROP >= chg_pct >= OVERSOLD_MIN_DROP
            and rsi < OVERSOLD_RSI
            and bollinger in ("触及下轨", "中轨下方")):
        return {
            "signal_type": "oversold",
            "signal_reason": f"超跌反弹: 跌{chg_pct:.1f}% RSI{rsi:.0f} {bollinger}",
            "score": 1,
        }

    return None


# ---------------------------------------------------------------------------
# 卖出信号判断
# ---------------------------------------------------------------------------

def check_sell_signals(pos: dict, quote: dict, market_phase: str = "未知") -> Optional[dict]:
    """检查卖出信号 — 策略差异化 + 退潮收紧

    根据持仓的signal_type判断归属策略A/B, 使用不同的卖出参数:
      策略A(龙头首阴反包): 次日确认买入, 持2-3天, 止损=首阴低×0.98, trailing正常3%/退潮1.5%
      策略B(低开反弹): 持3天, 止损-5%, trailing正常3%/退潮2%/冰点1.5%  # [GUARD-BYPASS]
      共同: -8%固定止损

    三方审核(2026-05-15): 涨停股波幅7.71%, trailing需>噪音阈值(0.38ATR)
    旧版退潮1.5%只是波幅的19%, 盘中60秒采样被频繁误触

    优先级:
    0. T+1规则: 买入当天不能卖出
    1. 固定止损: 跌破成本-8%
    2. 最长持有: 到期无条件清仓
    3. 移动止盈: 从最高点回落N%(退潮收紧)
    4. 时间止损: N天涨幅<1%

    Returns:
        None = 不卖
        dict = {reason, urgency}
    """
    price = quote.get("price", 0)
    if price <= 0:
        return None

    buy_price = pos["buy_price"]
    highest = pos.get("highest_price", buy_price)
    pnl_pct = (price / buy_price - 1)

    # 更新最高价
    if price > highest:
        _update_highest(pos["id"], price)
        highest = price

    # 0. T+1规则: 当天买入不能卖出
    buy_date_str = pos.get("buy_date", pos.get("buy_time", "")[:10])
    try:
        buy_dt = datetime.strptime(buy_date_str[:10], "%Y-%m-%d")
        if buy_dt.date() >= datetime.now().date():
            return None  # 买入当天不卖
        hold_days = _count_trading_days(buy_date_str[:10])
    except ValueError:
        hold_days = 0

    if hold_days < 1:
        # 买入当天不卖, 只更新最高价
        return None

    # 判断策略归属
    signal = pos.get("signal_type", "")
    if "趋势牛股" in signal or "基本面驱动" in signal or "策略C" in signal:
        strategy = "C"
    elif "策略A" in signal or "ML" in signal or "因子" in signal or "首阴" in signal:
        strategy = "A"
    else:
        strategy = "B"
    params = SELL_PARAMS[strategy]

    # 策略C止损独立(-8%)
    strategy_stop_loss = SELL_PARAMS.get(strategy, {}).get("stop_loss_pct", STOP_LOSS_PCT)
    if pnl_pct <= strategy_stop_loss:
        return {"reason": f"止损: 浮亏{pnl_pct*100:+.1f}% 破{strategy_stop_loss*100:.0f}%线 [{strategy}]", "urgency": "高"}

    # 2. 最长持有(硬限, 无条件清仓)
    max_days = params["max_hold_days"]
    if hold_days >= max_days:
        return {
            "reason": f"最长持有{max_days}天到期: 涨幅{pnl_pct*100:+.1f}% [{strategy}]",
            "urgency": "高",
        }

    # 3. 移动止盈(退潮收紧, 按策略差异化)
    trailing_pct = params["trailing_stop_pct"]  # 默认值(正常市)
    if market_phase == "退潮":
        trailing_pct = params.get("trailing_ebb_pct", 0.03)
    elif market_phase in ("冰点", "偏弱", "退潮预警"):
        trailing_pct = params.get("trailing_frost_pct", 0.02)
    drawdown = (price / highest - 1) if highest > 0 else 0
    if highest > buy_price and drawdown <= -trailing_pct:
        phase_tag = f" [{market_phase}收紧]" if trailing_pct < params["trailing_stop_pct"] else ""
        return {
            "reason": f"移动止盈: 从最高{highest:.2f}回落{abs(drawdown)*100:.1f}%{phase_tag} [{strategy}]",
            "urgency": "高",
        }

    # 4. 时间止损
    time_days = params["time_stop_days"]
    time_threshold = params["time_stop_threshold"]
    if time_days > 0 and hold_days >= time_days and pnl_pct < time_threshold:
        return {
            "reason": f"时间止损: 持有{hold_days}天 涨幅{pnl_pct*100:+.1f}%<{time_threshold*100:.0f}% [{strategy}]",
            "urgency": "中",
        }

    return None



def _update_highest(pos_id: int, new_highest: float):
    """更新持仓最高价"""
    conn = _get_conn()
    try:
        conn.execute(
            "UPDATE daemon_positions SET highest_price = ? WHERE id = ? AND highest_price < ?",
            (round(new_highest, 3), pos_id, new_highest),
        )
        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 策略A候选股获取 — 超跌反弹
# ---------------------------------------------------------------------------

def get_ml_candidates() -> list[dict]:
    """获取策略A候选 — 超跌反弹(5176笔回测+1.31%/PF1.83)
    
    核心逻辑: 前5天跌>10% + 当天低开-3%~-8% + 非涨停 + 成交额>5000万
    买入: 当天开盘 | 卖出: 当天收盘 | 不止损
    基本面过滤: 解禁/减持/业绩暴雷 → 不买
    接口名保持get_ml_candidates兼容页面调用
    """
    try:
        from src.strategy.strategy_a import get_strategy_a_candidates
        cands = get_strategy_a_candidates(top_n=20)
        # P0-4: 连板数安全过滤 — 确保策略A只做2板以上(1连板8592只/天, 噪音太大)
        cands = [c for c in cands if c.get("_lb", 0) >= 2]
        if cands:
            # 实时行情补充名称和过滤
            result = _filter_candidates_realtime(cands)
            for c in result:
                c["_sub_source"] = "超跌反弹"
                c["_strategy"] = "A"
            return result
        return []
    except Exception as e:
        logger.warning(f"策略A(超跌反弹)选股异常: {e}")
        return []



def _filter_candidates_realtime(candidates: list[dict]) -> list[dict]:
    """实时行情过滤(通用, 策略A/B/C候选共用)"""
    codes = [c.get("code", c.get("stock_code", "")) for c in candidates]
    # 统一code字段
    for c in candidates:
        if "code" not in c and "stock_code" in c:
            c["code"] = c["stock_code"]
    quotes = get_realtime(codes)
    
    qualified = []
    industry_count = {}
    
    for p in candidates:
        code = p["code"]
        q = quotes.get(code, {})
        price = q.get("price", 0)
        
        if price <= 0:
            continue
        
        q_name = q.get("name", "") or p.get("name", "")
        if "ST" in q_name.upper():
            continue
        
        now_h = __import__('datetime').datetime.now().hour
        now_m = __import__('datetime').datetime.now().minute
        minutes_since_open = (now_h - 9) * 60 + (now_m - 30) if now_h >= 9 else 0
        amt_threshold = 100 if minutes_since_open < 30 else 500
        
        amt = q.get("amount_wan", 0)
        if amt < 1:
            try:
                import sqlite3 as _sql
                _conn = _sql.connect(str(DB_PATH))
                _r = _conn.execute(
                    "SELECT amount FROM daily_price WHERE stock_code=? ORDER BY trade_date DESC LIMIT 1",
                    (code,)
                ).fetchone()
                _conn.close()
                if _r and _r[0]:
                    amt = _r[0] / 10000
            except Exception:
                pass
        if amt < amt_threshold:
            continue
        
        chg = q.get("change_pct_calc", 0)
        if chg >= 8:
            continue
        
        ask1 = q.get("ask1", [0, 0])
        if ask1[1] == 0 and chg >= 5:
            continue
        
        ind = p.get("industry", "未知")
        if industry_count.get(ind, 0) >= 2:
            continue
        industry_count[ind] = industry_count.get(ind, 0) + 1
        
        p["realtime_price"] = price
        p["realtime_chg"] = chg
        p["name"] = q_name
        qualified.append(p)

    # 交易记忆参考: 查候选股历史交易记录
    if qualified:
        try:
            from src.trader.trade_memory import query_similar_trades
            for c in qualified:
                code = c.get("code", "")
                strat = c.get("_strategy", "B")
                ind = c.get("industry", "")
                mem = query_similar_trades(strat, industry=ind)
                if mem and mem.get("total", 0) > 0:
                    c["_memory"] = mem
        except Exception:
            pass

    # 个股感知: LLM涨因分析(评分前调用, 结果写入candidate)
    if qualified:
        try:
            from src.agent.market_perception import perceive_stock
            for c in qualified:
                code = c.get("code", "")
                if code:
                    perception = perceive_stock(code)
                    c["_perception"] = perception
        except Exception:
            pass

    # 精选评分卡过滤  # [GUARD-BYPASS] 集成评分卡, 不够分的不买
    if qualified:
        from src.trader.selection_score import filter_by_score
        strategy = "A" if any(c.get("_strategy") == "A" for c in qualified) else "B"
        qualified = filter_by_score(qualified, strategy=strategy, min_score=60)

    return qualified[:6]


# ---------------------------------------------------------------------------
# 主循环
# ---------------------------------------------------------------------------


def _scan_sell(positions: list, quotes: dict, market: dict,
                already_pending: set, result: dict) -> None:
    """卖出扫描: T+1检查 + 策略分发 + Grace Period + 预告执行"""
    from src.trader.daemon_sell_strategies import (
        _check_sell_strategy_c, _check_sell_strategy_a, _check_sell_strategy_b,
    )

    for pos in positions:
        code = pos["code"]
        q = quotes.get(code, {})
        if not q or q.get("price", 0) <= 0:
            continue

        sell_signal = None

        # T+1铁律: 当天买入的票不能卖(A股基本规则)
        buy_date = pos.get("buy_date", pos.get("buy_time", "")[:10])
        if buy_date:
            try:
                buy_dt = datetime.strptime(buy_date[:10], "%Y-%m-%d")
                if buy_dt.date() >= datetime.now().date():
                    result["skipped"].append({
                        "code": code, "name": pos.get("name", ""),
                        "reason": f"T+1锁仓(买入日{buy_date})",
                    })
                    continue
            except ValueError:
                pass

        # 判断策略归属 → 分发到对应卖出检查
        sig_type = pos.get("signal_type", "")
        is_strategy_c = ("趋势牛股" in sig_type or "基本面驱动" in sig_type
                         or "策略C" in sig_type)
        is_strategy_a = ("首阴" in sig_type or "策略A" in sig_type)
        is_strategy_b = ("回踩低吸" in sig_type or "低开反弹" in sig_type or "暴跌日狙击" in sig_type
                         or sig_type in ("涨停低吸", "板块补涨", "涨停确认")
                         or ("策略B" in sig_type and not is_strategy_a and not is_strategy_c))

        if is_strategy_c:
            sell_signal = _check_sell_strategy_c(pos, q)
        elif is_strategy_a:
            sell_signal = _check_sell_strategy_a(pos, q, market)
        elif is_strategy_b:
            sell_signal = _check_sell_strategy_b(pos, q, market)
        else:
            # 旧格式/未知持仓 → 通用卖出检查
            sell_signal = check_sell_signals(pos, q, market_phase=market.get("phase", "未知"))

        if not sell_signal:
            continue

        # === 开盘30分钟止损观察期(所有策略统一) ===
        # 交易员逻辑: 开盘集合竞价价格波动大, 破止损可能卖在最低点
        # 等10:00盘面稳定后再判, 如果期间反弹回止损线以上则不卖
        # 但硬止损(-10%)即使在观察期也执行(重大利空/极端行情保命)
        reason = sell_signal.get("reason", "")
        is_stop_loss = ("止损" in reason)
        now_hm = datetime.now().hour * 100 + datetime.now().minute
        if is_stop_loss and 930 <= now_hm < 1000:
            pnl_in_obs = q["price"] / pos.get("buy_price", q["price"]) - 1
            if pnl_in_obs <= HARD_STOP_PCT:
                logger.warning(f"[硬止损] {pos['code']} {pos['name']} 浮亏{pnl_in_obs*100:+.1f}%≤{HARD_STOP_PCT*100}% 观察期内仍执行")
            else:
                logger.info(f"[开盘观望] {pos['code']} {pos['name']} {reason} → 等10:00再判")
                continue

        # 开盘观望期: urgency="观望"时只记录不执行(保留兼容)
        if sell_signal.get("urgency") == "观望":
            logger.info(f"[开盘观望] {pos['code']} {pos['name']} {sell_signal.get('reason','')}")
            continue

        # === Grace Period: 开盘30分钟内止损延迟 ===
        is_strategy_a_stop = sell_signal.get("type") == "策略A止损"
        is_strategy_b_stop = sell_signal.get("type") == "策略B止损"
        now_minute = datetime.now().hour * 60 + datetime.now().minute
        is_open_15min = (9*60+30) <= now_minute < (9*60+45)  # 9:30-9:45
        if is_strategy_a_stop and is_open_15min:
            logger.info(f"[Grace-A] {pos['code']} {pos['name']} 开盘15分钟缓冲期, 策略A止损延迟")
            continue
        is_intraday_stop = is_strategy_b_stop
        if GRACE_PERIOD_ENABLED and _is_grace_period() and not is_intraday_stop:
            reason = sell_signal.get("reason", "")
            is_hard_stop = ("止损" in reason and
                            abs(q["price"] / pos.get("buy_price", q["price"]) - 1) >= abs(HARD_STOP_PCT))
            if not is_hard_stop:
                logger.info(f"[Grace] {pos['code']} {pos['name']} 开盘波动期, 止损延迟: {reason}")
                continue

        # 止损/最长持有→紧急60秒, 止盈/时间止损→5分钟
        is_urgent = ("止损" in sell_signal.get("reason", "")
                     or "最长持有" in sell_signal.get("reason", ""))
        _add_signal(
            action="sell",
            code=pos["code"],
            name=pos["name"],
            price=q["price"],
            reason=sell_signal["reason"],
            signal_type=sell_signal.get("type", "卖出"),
            extra={"pnl_preview": (q["price"] - pos.get("buy_price", 0)) * pos.get("shares", 0)},
            urgent=is_urgent,
        )
        result["sells"].append({"code": pos["code"], "name": pos["name"],
                                "price": q["price"], "reason": sell_signal["reason"],
                                "status": "预告中"})
        _log_to_db("INFO", "sell_signal",
                   f"卖出预告 {pos['name']}@{q['price']:.2f} [{sell_signal['reason']}] 将于5分钟后执行")



def _collect_candidates() -> list:
    """策略候选合并去重, shadow研究策略优先保存快照但不覆盖paper策略."""
    from src.strategy.registry import StrategyRegistry
    candidates = []
    seen = set()

    # 通过注册表获取候选(解耦: 不直接import策略文件)
    # 策略B_crash_v2候选只由_get_b_watchlist产生, 不走Registry(旧B已移除)
    strategy_b_cands = _get_b_watchlist()
    strategy_a_cands = StrategyRegistry.get_candidates("A")

    # 策略C v3候选
    strategy_c_cands = StrategyRegistry.get_candidates("C")
    strategy_c1_cands = StrategyRegistry.get_candidates("C1")
    strategy_c2_cands = StrategyRegistry.get_candidates("C2")

    # 合并去重: paper核心策略优先, C1/C2仅shadow研究
    for c in strategy_c_cands:
        if c["code"] not in seen:
            seen.add(c["code"])
            c["_strategy"] = "C"
            candidates.append(c)
    for c in strategy_c1_cands:
        if c["code"] not in seen:
            seen.add(c["code"])
            c["_strategy"] = "C1"
            candidates.append(c)
    for c in strategy_c2_cands:
        if c["code"] not in seen:
            seen.add(c["code"])
            c["_strategy"] = "C2"
            candidates.append(c)
    for c in strategy_b_cands:
        if c["code"] not in seen:
            seen.add(c["code"])
            c["_strategy"] = "B"
            candidates.append(c)
    for c in strategy_a_cands:
        if c["code"] not in seen:
            seen.add(c["code"])
            c["_strategy"] = "A"
            candidates.append(c)

    save_candidate_snapshots(candidates)

    # 候选日志
    if candidates:
        strategy_c_watch = [c for c in candidates if c.get("_strategy") == "C"][:5]
        if strategy_c_watch:
            logger.info(
                f"[策略C] 基本面驱动候选: "
                + ", ".join(f"{c.get('name',c['code'])}F={c.get('score',0)}分" for c in strategy_c_watch)
            )
        strategy_c1_watch = [c for c in candidates if c.get("_strategy") == "C1"][:5]
        if strategy_c1_watch:
            logger.info(
                f"[策略C1-shadow] 关注度动量候选: "
                + ", ".join(f"{c.get('name',c['code'])}分{c.get('score',0)}" for c in strategy_c1_watch)
            )
        strategy_c2_watch = [c for c in candidates if c.get("_strategy") == "C2"][:5]
        if strategy_c2_watch:
            logger.info(
                f"[策略C2-shadow] 恐慌反转候选: "
                + ", ".join(f"{c.get('name',c['code'])}分{c.get('score',0)}" for c in strategy_c2_watch)
            )
        strategy_b_watch = [c for c in candidates[:10] if c.get("_strategy") == "B"]
        if strategy_b_watch:
            logger.info(
                f"[策略B] 暴跌日狙击候选: "
                + ", ".join(f"{c.get('name',c['code'])}跌{c.get('_day_ret',0):.1f}%ROE{c.get('_roe',0):.0f}" for c in strategy_b_watch[:5])
            )
        strategy_a_watch = [c for c in candidates if c.get("_strategy") == "A"][:5]
        if strategy_a_watch:
            logger.info(
                f"[策略A] 龙头首阴反包候选: "
                + ", ".join(f"{c.get('name',c['code'])}首阴{c.get('_yin_date','')}" for c in strategy_a_watch)
            )
    else:
        logger.warning("[选股] 策略候选为0! A=%d B=%d C=%d C1=%d C2=%d — 可能数据异常",
                       len(strategy_a_cands), len(strategy_b_cands), len(strategy_c_cands),
                       len(strategy_c1_cands), len(strategy_c2_cands))
        _log_to_db("WARN", "scan", "三策略候选为0, 可能数据异常")

    return candidates


def _get_brain_buy_decision(**kwargs) -> dict:
    """Return a validated brain result; any failure is a hard block."""
    try:
        from src.agent.trading_brain import get_brain

        result = get_brain().think_before_buy(**kwargs)
        if not isinstance(result, dict):
            raise ValueError(f"返回类型无效: {type(result).__name__}")
        decision = result.get("decision")
        if decision not in ("buy", "pass"):
            raise ValueError(f"decision无效: {decision!r}")
        return result
    except Exception as exc:
        return {
            "decision": "pass",
            "score": 0,
            "confidence": 1.0,
            "reason": f"大脑异常拦截: {type(exc).__name__}: {str(exc)[:160]}",
            "_outcome": "brain_error_block",
        }


def _scan_buy(market: dict, result: dict) -> None:
    """买入扫描: 退潮市策略C / 正常市三策略买入"""
    global _last_c_buy_time
    # 预告计数器: 每策略最多生成N个预告(含当前持仓)
    _notice_count = {"A": 0, "B": 0, "C": 0}
    for h in get_held_positions():
        st = h.get("signal_type", "")
        if "首阴" in st: _notice_count["A"] += 1
        elif "回踩低吸" in st or "低开反弹" in st or "暴跌日狙击" in st or st in ("涨停低吸", "板块补涨", "涨停确认"): _notice_count["B"] += 1
        elif "趋势牛股" in st or "基本面" in st or "缩量反包" in st: _notice_count["C"] += 1

    # 策略C(趋势牛股)由_check_c_trend_realtime盘中实时检测, 不走_scan_buy候选遍历
    # 退潮市亦由_check_c_trend_realtime独立处理, 此处不再重复

    # 退潮冷却期检查 — 直接读daemon_risk模块变量(避免from import值拷贝导致状态脱节)
    import src.trader.daemon_risk as _risk_mod
    if _risk_mod._last_ebb_clear_time is not None:
        elapsed = (datetime.now() - _risk_mod._last_ebb_clear_time).total_seconds() / 60
        if elapsed < EBB_COOLDOWN_MINUTES:
            remaining = EBB_COOLDOWN_MINUTES - elapsed
            logger.warning(f"[退潮冷却] 极端退潮清仓后{elapsed:.0f}分钟, 需冷却{EBB_COOLDOWN_MINUTES}分钟(还剩{remaining:.0f}分钟), 暂停所有买入")
            return

    # 退潮市: 策略A/B不开仓, 策略C由_check_c_trend_realtime独立处理
    if not market["can_buy"]:
        logger.info(f"大盘情绪检查: {market.get('reason', '')}, 策略A/B暂停买入, 策略C走独立实时路径")
        return

    # === 正常市买入 ===
    # 风控检查
    acct = get_account()
    daily_pnl = acct.get("daily_pnl", 0)
    if daily_pnl <= DAILY_LOSS_LIMIT:
        logger.warning(f"日限亏触发: 今日已亏¥{daily_pnl:+,.0f}, 超过限额¥{DAILY_LOSS_LIMIT:.0f}, 停止买入")
        return
    if _check_consecutive_losses():
        logger.warning("连亏保护触发: 近3笔全亏, 今日暂停买入")
        return
    if _check_monthly_drawdown():
        logger.warning("月度回撤保护: 本月亏损>5%, 仓位减半")
        # 不return, 继续但仓位减半

    # 候选收集
    candidates = _collect_candidates()

    # 满仓换仓评估
    held = get_held_positions()
    held_codes = {h["code"] for h in held}
    if len(held) >= MAX_POSITIONS and candidates:
        _try_upgrade_positions(held, candidates, market)
    if len(held) >= MAX_POSITIONS:
        logger.info(f"[持仓] 满仓{len(held)}只, 等待卖出信号释放仓位")
    if len(held) >= MAX_POSITIONS or acct["cash"] < 1000 or not candidates:
        return

    # 止损冷却: 今天被止损的票不再买
    today_str = datetime.now().strftime("%Y-%m-%d")
    stopped_today = set()
    try:
        import sqlite3 as _sq
        _c = _sq.connect(str(DB_PATH))
        _rows = _c.execute(
            "SELECT code FROM daemon_positions WHERE status='closed' AND sell_time LIKE ? AND sell_reason LIKE '%止损%'",
            (f"{today_str}%",)
        ).fetchall()
        stopped_today = {r[0] for r in _rows}
        _c.close()
    except Exception:
        pass
    if stopped_today:
        before_cooldown = len(candidates)
        candidates = [c for c in candidates if c["code"] not in stopped_today]
        if len(candidates) < before_cooldown:
            logger.info(f"[冷却] 排除{before_cooldown - len(candidates)}只今日止损票: {stopped_today}")

    # 候选遍历买入
    cand_codes = [c["code"] for c in candidates if c["code"] not in held_codes]
    if not cand_codes:
        return
    cand_quotes = get_realtime(cand_codes[:25])

    # 缓存持仓数据,避免循环内重复DB查询(每轮scan只查一次)
    _cached_held = held  # 使用循环外已获取的held
    _held_by_strategy = {}
    for h in _cached_held:
        sig = h.get("signal_type", "")
        if "首阴" in sig:
            _held_by_strategy.setdefault("A", []).append(h)
        elif "回踩低吸" in sig or "低开反弹" in sig or "暴跌日狙击" in sig or sig in ("涨停低吸", "板块补涨", "涨停确认"):
            _held_by_strategy.setdefault("B", []).append(h)
        elif "趋势牛股" in sig or "基本面" in sig:
            _held_by_strategy.setdefault("C", []).append(h)

    for cand in candidates:
        if len(_cached_held) >= MAX_POSITIONS:
            break

        code = cand["code"]
        if code in held_codes:
            continue
        q = cand_quotes.get(code)
        if not q or "error" in q or q.get("price", 0) <= 0:
            continue

        # 仓位限制(使用缓存的持仓数据)
        cand_strategy = cand.get("_strategy", "A")
        strategy_meta_pre = get_strategy_metadata(cand_strategy)
        if cand_strategy in ("C1", "C2") or strategy_meta_pre["run_mode"] in ("shadow", "pause"):
            shadow_signal = {
                "signal_type": cand.get("signal_type", f"策略{cand_strategy} shadow"),
                "signal_reason": cand.get("reason", ""),
            }
            record_shadow_signal(
                candidate=cand,
                quote=q,
                buy_signal=shadow_signal,
                market=market,
                decision=strategy_meta_pre["run_mode"],
                block_reason=f"策略{cand_strategy}处于{strategy_meta_pre['run_mode']}模式",
            )
            result["skipped"].append({
                "code": code,
                "name": q.get("name", cand.get("name", "")),
                "reason": f"{strategy_meta_pre['strategy_version']}={strategy_meta_pre['run_mode']}, 仅记录shadow",
            })
            logger.info(
                f"[Shadow] {strategy_meta_pre['strategy_version']} {code} "
                f"{q.get('name', cand.get('name',''))} 仅记录候选假设"
            )
            continue
        if cand_strategy == "B":
            # B_crash_v2 has one pending-generation path only. Its crash-day,
            # consecutive-crash and market-emotion gates live in
            # _check_b_pullback_realtime().
            result["skipped"].append({
                "code": code,
                "name": q.get("name", cand.get("name", "")),
                "reason": "策略B候选仅供快照，买入由实时专用入口处理",
            })
            continue
        max_pos = {"A": A_MAX_POSITIONS, "B": B_MAX_POSITIONS, "C": STRATEGY_C_CONFIG['max_positions']}
        if _notice_count[cand_strategy] >= max_pos[cand_strategy]:
            continue
        if cand_strategy == "A":
            held_a = _held_by_strategy.get("A", [])
            if len(held_a) >= A_MAX_POSITIONS:
                result["skipped"].append({"code": code, "name": q.get("name", ""), "reason": f"策略A满仓{len(held_a)}只"})
                continue
        elif cand_strategy == "B":
            held_b = _held_by_strategy.get("B", [])
            if len(held_b) >= B_MAX_POSITIONS:
                result["skipped"].append({"code": code, "name": q.get("name", ""), "reason": f"策略B满仓{len(held_b)}只"})
                continue
        elif cand_strategy == "C":
            held_d_check = _held_by_strategy.get("C", [])
            if len(held_d_check) >= STRATEGY_C_CONFIG['max_positions']:
                result["skipped"].append({"code": code, "name": q.get("name", ""), "reason": f"策略C满仓{len(held_d_check)}只"})
                continue

        # 集中度风控
        if not _check_industry_concentration(code):
            result["skipped"].append({"code": code, "name": q.get("name", ""), "reason": "同行业集中度超限"})
            continue

        chg_now = q.get("change_pct_calc", 0)
        yclose = q.get("yesterday_close", 0)

        # 涨停/跌停跳过
        if chg_now >= 9.5:
            result["skipped"].append({"code": code, "name": q.get("name", ""), "reason": f"涨停{chg_now:+.1f}%无法买入"})
            continue
        if chg_now <= -9.5:
            result["skipped"].append({"code": code, "name": q.get("name", ""), "reason": f"跌停{chg_now:+.1f}%"})
            continue

        # 一字涨停跳过
        open_price = q.get("open", 0)
        ask1 = q.get("ask1", [0, 0])
        vol = q.get("volume", 0)
        if (yclose > 0 and open_price >= yclose * 1.095
                and ask1[1] == 0 and vol < 1000):
            result["skipped"].append({"code": code, "name": q.get("name", ""), "reason": "一字涨停,无量"})
            continue

        # 日K线数据
        daily_df = _get_daily_data(code, 120)

        is_strategy_a = cand.get("_strategy") == "A"
        is_strategy_c = cand.get("_strategy") == "C"
        buy_signal = None

        if is_strategy_a:
            # 策略A(龙头首阴反包): 次日确认买入 — 高开2%+翻红
            yclose = q.get("yesterday_close", 0) or q.get("pre_close", 0)
            current_price = q.get("price", 0)
            chg_a = chg_now if isinstance(chg_now, (int, float)) else 0
            open_price_a = q.get("open", 0) or current_price

            # 确认条件0: 数据完整性(yclose/open必须>0)
            if yclose <= 0 or open_price_a <= 0:
                result["skipped"].append({"code": code, "name": q.get("name", ""),
                    "reason": f"策略A数据缺失: 昨收{yclose:.2f}开盘{open_price_a:.2f}"})
                continue

            # 确认条件1: 高开>=2%
            if open_price_a < yclose * 1.02:
                result["skipped"].append({"code": code, "name": q.get("name", ""),
                    "reason": f"策略A未确认: 开盘{open_price_a:.2f}未高开2%(昨收{yclose:.2f})"})
                continue

            # 确认条件2: 翻红=现价>开盘价(非涨跌幅>0)
            if current_price <= open_price_a:
                result["skipped"].append({"code": code, "name": q.get("name", ""),
                    "reason": f"策略A未确认: 现价{current_price:.2f}<=开盘{open_price_a:.2f}未翻红"})
                continue

            # 只有confirmed/watch票可以买入
            tier = cand.get("_tier", "weak")
            if tier == "weak":
                result["skipped"].append({"code": code, "name": q.get("name", ""),
                    "reason": f"策略A偏弱票不买(龙头{cand.get('_dragon_total',0):.0f}分)"})
                continue

            buy_signal = {
                "signal_type": "首阴日内",
                "signal_reason": f"首阴反包确认: {cand.get('_lb',0)}连板首阴{cand.get('_yin_date','')} 高开{chg_a:+.1f}%翻红 龙头{cand.get('_dragon_total',0):.0f}分 止损¥{cand.get('_stop_loss',0):.2f}(首阴低)",
            }
            logger.info(f"[扫描][策略A] {code} {q.get('name','')} ¥{current_price:.2f} {chg_a:+.1f}% → ★ 首阴反包确认买入")

        elif is_strategy_c:
            # 开盘30分钟保护: 策略C是中长期策略, 不需要抢开盘时机
            # 回测用收盘价买入, 实盘也应等盘中信号确认后再买
            now_hm_c = datetime.now().hour * 100 + datetime.now().minute
            if now_hm_c < 1000:
                result["skipped"].append({"code": code, "name": q.get("name", ""),
                                          "reason": f"策略C开盘观察期({now_hm_c}<1000), 10:00后才可买入"})
                continue
            # 买入间隔保护: 避免短时间集中买入, 给大盘情绪变化留反应时间
            if _last_c_buy_time is not None:
                elapsed_c = (datetime.now() - _last_c_buy_time).total_seconds() / 60
                if elapsed_c < _C_BUY_COOLDOWN_MINUTES:
                    remaining_c = _C_BUY_COOLDOWN_MINUTES - elapsed_c
                    result["skipped"].append({"code": code, "name": q.get("name", ""),
                                              "reason": f"策略C冷静期, 距上次买入{elapsed_c:.0f}分钟<{_C_BUY_COOLDOWN_MINUTES}分钟(还剩{remaining_c:.0f}分钟)"})
                    continue
            chg_safe_c = chg_now if isinstance(chg_now, (int, float)) else 0
            # v3: 基本面驱动，放宽价格限制（-5%~8%），关键是基本面质量
            if -5 <= chg_safe_c <= 8:
                # Tier集中度: 同Tier(算力核心/配套)最多2只, 防止AI链同涨同跌
                if not _check_tier_concentration(code, _cached_held):
                    result["skipped"].append({"code": code, "name": q.get("name", ""),
                                              "reason": "同Tier集中度超限(策略C)"})
                    continue
                held_d = [p for p in get_held_positions()
                          if "趋势牛股" in p.get('signal_type', '') or "基本面" in p.get('signal_type', '')]
                if len(held_d) < STRATEGY_C_CONFIG['max_positions']:
                    # 从candidates获取评分和信号信息
                    score = cand.get("score", 0)
                    signals = cand.get("signals", [])
                    buy_signal = {
                        "signal_type": "基本面驱动(策略C)",
                        "signal_reason": f"F-Score={score}分 信号={','.join(str(s) for s in signals)} {cand.get('reason', '')}",
                    }
                    logger.info(f"[扫描][策略C] {code} {q.get('name','')} ¥{q.get('price',0):.2f} {chg_safe_c:+.1f}% → ★ 基本面驱动: F-Score={score} {signals}")
                else:
                    logger.debug(f"[策略C] {code} 仓位已满{len(held_d)}只, 跳过")
            else:
                result["skipped"].append({"code": code, "name": q.get("name", ""),
                                          "reason": f"策略C: 涨{chg_safe_c:+.1f}%超出范围(-5%~8%)"})

        elif daily_df is not None and len(daily_df) >= 20:
            buy_signal = check_buy_signals(code, q, daily_df)
            if buy_signal:
                logger.info(f"[扫描] {code} {q.get('name','')} ¥{q.get('price',0):.2f} {chg_now:+.1f}% → ★ {buy_signal['signal_type']}: {buy_signal['signal_reason']}")

        # 执行买入预告
        if buy_signal:
            cand_strategy = cand.get('_strategy', 'A')
            strategy_meta = get_strategy_metadata(cand_strategy)
            if strategy_meta["run_mode"] != "paper":
                record_shadow_signal(
                    candidate=cand,
                    quote=q,
                    buy_signal=buy_signal,
                    market=market,
                    decision=strategy_meta["run_mode"],
                    block_reason=f"策略{cand_strategy}处于{strategy_meta['run_mode']}模式",
                )
                result["skipped"].append({
                    "code": code,
                    "name": q.get("name", ""),
                    "reason": f"{strategy_meta['strategy_version']}={strategy_meta['run_mode']}, 仅记录shadow",
                })
                logger.info(
                    f"[Shadow] {strategy_meta['strategy_version']} {code} "
                    f"{q.get('name','')} 仅记录假设信号, 不生成买入预告"
                )
                continue

            # 交易记忆检查: 相似场景胜率<40%且>=5条记录 → 跳过
            try:
                from src.trader.trade_memory import query_similar_trades
                _mem_industry = cand.get("industry", "")
                mem = query_similar_trades(cand_strategy, industry=_mem_industry)
                if mem.get("total", 0) >= 5 and mem.get("win_rate", 0) < 0.4:
                    result["skipped"].append({"code": code, "name": q.get("name", ""),
                        "reason": f"记忆过滤: 策略{cand_strategy}+{_mem_industry or '全行业'}"
                                  f" 胜率{mem['win_rate']:.0%}({mem['total']}笔)<40%"})
                    logger.info(f"[记忆] {code} 策略{cand_strategy} "
                                f"历史胜率{mem['win_rate']:.0%}({mem['total']}笔) < 40% → 跳过")
                    continue
                elif mem.get("total", 0) >= 3:
                    logger.debug(f"[记忆] {code} 策略{cand_strategy} "
                                 f"历史胜率{mem['win_rate']:.0%}({mem['total']}笔) → 放行")
            except Exception:
                pass  # 记忆查询失败不影响正常买入

            # 交易大脑: 只有明确decision=buy才允许继续。
            _brain_result = _get_brain_buy_decision(
                code=code,
                name=q.get("name", cand.get("name", "")),
                signal_type=buy_signal.get("signal_type", ""),
                signal_reason=buy_signal.get("signal_reason", ""),
                strategy=cand_strategy,
                market_phase=market.get("phase", "正常"),
                ratio_now=market.get("ratio_now", -1),
                candidate=cand,
            )
            if _brain_result["decision"] != "buy":
                outcome = _brain_result.get("_outcome", "brain_pass")
                record_shadow_signal(
                    candidate=cand,
                    quote=q,
                    buy_signal=buy_signal,
                    market=market,
                    decision=outcome,
                    block_reason=_brain_result["reason"],
                )
                result["skipped"].append({
                    "code": code,
                    "name": q.get("name", ""),
                    "reason": f"大脑拦截: {_brain_result['reason']}",
                })
                log_fn = logger.warning if outcome == "brain_error_block" else logger.info
                log_fn(
                    f"[大脑] {code} 策略{cand_strategy}买入被拦截: "
                    f"{_brain_result['reason']} "
                    f"(置信{_brain_result.get('confidence', 1.0):.0%})"
                )
                continue
            logger.debug(f"[大脑] {code} 放行: {_brain_result.get('reason', '')}")

            is_a_buy = (cand_strategy == 'A')
            now_hm = datetime.now().hour * 100 + datetime.now().minute
            in_morning_window = 930 <= now_hm < 1130
            in_afternoon_window = 1300 <= now_hm < NEW_BUY_CUTOFF_HM
            in_buy_window = in_morning_window or in_afternoon_window
            if not in_buy_window:
                result["skipped"].append({"code": code, "name": q.get("name", ""),
                                          "reason": f"不在开盘窗口({now_hm}), 跳过"})
                continue

            _add_signal(
                action="buy",
                code=code,
                name=q.get("name", cand.get("name", code)),
                price=q["price"],
                reason=buy_signal["signal_reason"],
                signal_type=f"{buy_signal['signal_type']}(策略{cand_strategy})",
                extra={
                    **buy_signal.get("extra", {}),
                    "ml_score": cand.get("score", 0),
                    "strategy_code": strategy_meta["strategy_code"],
                    "strategy_version": strategy_meta["strategy_version"],
                    "run_mode": strategy_meta["run_mode"],
                    "entry_rule_id": strategy_meta["entry_rule_id"],
                    "exit_rule_id": strategy_meta["exit_rule_id"],
                    "candidate_score": cand.get("score", 0),
                    "market_phase": market.get("phase", ""),
                },
                urgent=is_a_buy,
            )
            _notice_count[cand_strategy] += 1
            result["buys"].append({
                "code": code,
                "name": q.get("name", cand.get("name", code)),
                "price": q["price"],
                "signal_type": buy_signal["signal_type"],
                "reason": buy_signal["signal_reason"],
                "status": "预告中",
            })
            held_codes.add(code)
            # 策略C: 记录买入时间(用于冷却期), 本轮不再买策略C
            if cand_strategy == "C":
                _last_c_buy_time = datetime.now()
                break  # 策略C每轮最多买1只
            delay_sec = 60 if is_a_buy else 300
            strategy_tag = f"[策略{cand_strategy}]"
            _log_to_db("INFO", "buy_signal",
                       f"买入预告 {strategy_tag} {q.get('name', code)}@{q['price']:.2f} [{buy_signal['signal_type']}] 将于{delay_sec//60}分钟后执行")


# ── 策略C买入节流: 记录上次买入时间, 避免短时间集中买入 ──
_last_c_buy_time = None
_C_BUY_COOLDOWN_MINUTES = 15  # 策略C两笔买入间隔至少15分钟


_last_zb_refresh_time = None  # 上次刷新zb_pool的时间
_zb_refresh_inflight = False
_market_perception_cache = None
_market_perception_refresh_time = None
_market_perception_inflight = False

def _refresh_zb_pool_if_needed():
    """Schedule a non-blocking intraday zb_pool refresh every 30 minutes.
    
    背景: zb_pool原来只在收盘后(cron_daily 15:35)入库, 盘中永远显示0%
    2026-05-21教训: 67%炸板率显示0%, 导致冰点日判"复苏"买入3只全亏
    """
    global _last_zb_refresh_time, _zb_refresh_inflight
    if not _is_trading_time():
        return
    
    now = datetime.now()
    if _last_zb_refresh_time and (now - _last_zb_refresh_time).seconds < 1800:
        return  # 30分钟内已刷新过
    if _zb_refresh_inflight:
        return

    _last_zb_refresh_time = now
    _zb_refresh_inflight = True

    def _worker():
        global _zb_refresh_inflight
        try:
            from src.data.sources import akshare_zt_pool
            from src.data.storage import Storage

            trade_date = now.strftime("%Y%m%d")
            df = akshare_zt_pool.fetch_zb_pool(trade_date, retries=1)
            if df is not None and len(df) > 0:
                count = akshare_zt_pool.save_zb_pool(df, Storage())
                logger.info("[炸板刷新] zb_pool后台更新: %d条", count)
            else:
                logger.warning("[炸板刷新] 后台接口无有效数据, 本轮沿用DB")
        except Exception as exc:
            logger.warning("[炸板刷新] 后台失败: %s", exc)
        finally:
            _zb_refresh_inflight = False

    threading.Thread(
        target=_worker,
        name="zb-pool-refresh",
        daemon=True,
    ).start()


def _get_market_perception_nonblocking(daemon_ratio: float) -> dict:
    """Return the latest perception snapshot and refresh it off the scan loop."""
    global _market_perception_cache
    global _market_perception_refresh_time, _market_perception_inflight

    now = datetime.now()
    cache_expired = (
        _market_perception_refresh_time is None
        or (now - _market_perception_refresh_time).total_seconds() >= 300
    )
    if cache_expired and not _market_perception_inflight:
        _market_perception_refresh_time = now
        _market_perception_inflight = True

        def _worker():
            global _market_perception_cache, _market_perception_inflight
            try:
                from src.agent.market_perception import perceive_market
                _market_perception_cache = perceive_market(daemon_ratio=daemon_ratio)
                logger.info("[盘面] 后台感知快照已更新")
            except Exception as exc:
                logger.warning("[盘面] 后台感知失败: %s", exc)
            finally:
                _market_perception_inflight = False

        threading.Thread(
            target=_worker,
            name="market-perception-refresh",
            daemon=True,
        ).start()

    snapshot = dict(_market_perception_cache or {})
    snapshot.setdefault("top5_up_sectors", [])
    snapshot.setdefault("top5_down_sectors", [])
    snapshot.setdefault("top_inflow", [])
    snapshot.setdefault("style", "未知")
    snapshot.setdefault("weight_red_pct", -1)
    snapshot["ratio_now"] = round(daemon_ratio, 1) if daemon_ratio >= 0 else -1
    return snapshot


def _record_shadow_candidates(market: dict, result: dict) -> None:
    """Record shadow-only strategy candidates independent of paper buy gates."""
    if not _is_new_buy_allowed_now():
        return

    from src.strategy.registry import StrategyRegistry

    shadow_candidates = []
    for strategy_code in ("C", "C1", "C2"):
        meta = get_strategy_metadata(strategy_code)
        if meta["run_mode"] != "shadow":
            continue
        for cand in StrategyRegistry.get_candidates(strategy_code):
            cand["_strategy"] = strategy_code
            shadow_candidates.append(cand)

    if not shadow_candidates:
        return

    save_candidate_snapshots(shadow_candidates)
    codes = [c["code"] for c in shadow_candidates if c.get("code")]
    quotes = get_realtime(codes[:30]) if codes else {}

    recorded = 0
    for cand in shadow_candidates:
        code = cand.get("code")
        if not code:
            continue
        q = quotes.get(code, {})
        if not q or "error" in q:
            q = {
                "code": code,
                "name": cand.get("name", code),
                "price": cand.get("_close", cand.get("price", 0)),
            }
        shadow_signal = {
            "signal_type": cand.get("signal_type", f"策略{cand.get('_strategy')} shadow"),
            "signal_reason": cand.get("reason", ""),
        }
        meta = get_strategy_metadata(cand.get("_strategy", "C"))
        record_shadow_signal(
            candidate=cand,
            quote=q,
            buy_signal=shadow_signal,
            market=market,
            decision="shadow",
            block_reason=f"{meta['strategy_version']} shadow采样",
        )
        recorded += 1

    result["skipped"].append({
        "code": "SHADOW",
        "name": "shadow采样",
        "reason": f"记录{recorded}条C/C1/C2 shadow候选",
    })
    logger.info(f"[Shadow] 独立采样记录{recorded}条C/C1/C2候选")


def scan_once() -> dict:
    """单次扫描: 检查卖出+买入信号"""
    import pandas as pd
    result = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "is_trading_time": _is_trading_time(),
        "sells": [], "buys": [], "skipped": [], "errors": [],
    }
    logger.info(f"[扫描] 开始, 交易时间={result['is_trading_time']}")

    # 新交易日重置
    if _is_new_day():
        _reset_daily_pnl()
        logger.info("新交易日, 已重置日损")

    # 市场情绪(卖出收紧 + 买入拦截共用)
    # 盘中每30分钟刷新一次zb_pool(炸板数据), 使炸板率检测生效
    _refresh_zb_pool_if_needed()
    
    market_sentiment = _check_market_sentiment()
    up_c = market_sentiment.get('up_count', 0)
    down_c = market_sentiment.get('down_count', 0)
    up_ratio = f"{up_c/(up_c+down_c)*100:.0f}%" if (up_c+down_c) > 0 else "?"

    # 情绪数据放入result供dashboard/调试使用
    result['emotion'] = {
        'phase': market_sentiment.get('phase', '未知'),
        'can_buy': market_sentiment.get('can_buy', False),
        'zt_count': market_sentiment.get('zt_count', 0),
        'up_ratio': up_ratio,
        'zb_rate': market_sentiment.get('zb_rate', 0),
    }

    logger.info(f"[扫描] 情绪: {market_sentiment['phase']}({market_sentiment['zt_count']}涨停) "
                f"涨跌{up_c}/{down_c}({up_ratio}) can_buy={market_sentiment['can_buy']}")

    # Shadow研究采样独立于paper买入闸门; 冰点/退潮日也需要收集反事实样本。
    try:
        _record_shadow_candidates(market_sentiment, result)
    except Exception as e:
        logger.warning(f"[Shadow] 独立采样失败: {e}")

    # 市场感知: 板块资金流向 + 风格 + 反转检测
    _reversal_signal = None
    _mp_data = None  # perceive_market()结果, 供detect_reversal复用
    phase = market_sentiment.get("phase", "未知")
    if _is_trading_time():
        # 盘面感知使用后台快照，禁止网络请求阻塞主扫描。
        try:
            _daemon_ratio = (up_c / (up_c + down_c) * 100) if (up_c + down_c) > 0 else -1
            _mp_data = _get_market_perception_nonblocking(_daemon_ratio)
            style = _mp_data.get("style", "?")
            ratio_mp = _mp_data.get("ratio_now", -1)
            weight_mp = _mp_data.get("weight_red_pct", -1)
            top5_up = _mp_data.get("top5_up_sectors", [])
            top5_down = _mp_data.get("top5_down_sectors", [])
            top_str = ""
            if top5_up:
                top_str = " 领涨:" + ",".join(f"{s['name']}({s['pct']:+.1f}%)" for s in top5_up[:3])
            down_str = ""
            if top5_down:
                down_str = " 领跌:" + ",".join(f"{s['name']}({s['pct']:+.1f}%)" for s in top5_down[:2])
            logger.info(f"[盘面] 风格={style} 涨跌比={ratio_mp}% 权重翻红={weight_mp}%{top_str}{down_str}")
        except Exception as e:
            logger.debug(f"[盘面] perceive_market失败: {e}")

        # 反转检测(独立try, 复用perceive_market数据)
        if phase in ("冰点", "退潮", "偏弱", "退潮预警"):
            try:
                from src.agent.market_perception import detect_reversal
                _reversal_signal = detect_reversal(market_data=_mp_data)
                sig = _reversal_signal
                logger.info(
                    f"[反转检测] {sig['signal']}: "
                    f"涨跌比={sig.get('ratio_now',-1)}%(最低{sig.get('ratio_min',-1)}%) "
                    f"跌停={sig.get('dt_count',-1)}只 权重翻红={sig.get('weight_red_pct',-1)}% "
                    f"— {sig.get('reason','')}"
                )
            except Exception as e:
                logger.warning(f"[反转检测] detect_reversal失败: {e}")

    # 第一步: 执行到期预告
    executed_signals = _execute_pending_signals()
    for esig in executed_signals:
        if esig.get("result", {}).get("success"):
            r = esig["result"]
            if esig["action"] == "sell":
                result["sells"].append(r.get("trade", {}))
            else:
                result["buys"].append(r.get("trade", {}))

    # 第二步: 持仓卖出扫描
    cb_level = ""  # 熔断等级(空=未触发)
    positions = get_held_positions()
    if positions:
        pos_codes = [p["code"] for p in positions]
        quotes = get_realtime(pos_codes)

        # 分时快照(每15秒积累)
        from src.trader.intraday_cache import get_cache
        cache = get_cache()
        for pos in positions:
            q = quotes.get(pos["code"], {})
            if q and q.get("price", 0) > 0:
                cache.snapshot(pos["code"], q["price"], q.get("volume", 0), q.get("open", 0))

        # 大盘急跌清仓
        _market_crash_clear(positions, quotes, market_sentiment, result)

        # 浮动亏损熔断(4级保护)
        cb_level = check_circuit_breaker(positions, quotes, result)

        # 策略C连续止损监控(纯日志, 不影响交易)
        check_c_consecutive_stops()

        # 卖出扫描
        already_pending = {s["code"] for s in _read_pending_signals() if s["status"] == "pending"}
        _scan_sell(positions, quotes, market_sentiment, already_pending, result)

    # 第2.5步: 策略B/C盘中实时监控
    # 策略B v2: 暴跌日好公司狙击 — 不受情绪限制(暴跌日本身就是触发条件)
    # 策略C: 趋势牛股 — 冰点/退潮不做
    # 两者都受退潮冷却期和熔断约束
    if _is_new_buy_allowed_now() and market_sentiment.get("can_buy", True) and not cb_level and not check_weekly_drawdown():
        now_hm = datetime.now().hour * 100 + datetime.now().minute
        if now_hm < 933:
            pass  # 开盘3分钟不做回踩监控
        else:
            phase = market_sentiment.get("phase", "未知")
            zt_count = market_sentiment.get("zt_count", 0)

            # 退潮冷却检查: 独立买入路径也必须受冷却期约束
            import src.trader.daemon_risk as _risk_mod2
            cooldown_active = False
            if _risk_mod2._last_ebb_clear_time is not None:
                elapsed2 = (datetime.now() - _risk_mod2._last_ebb_clear_time).total_seconds() / 60
                if elapsed2 < EBB_COOLDOWN_MINUTES:
                    logger.info(f"[实时检测] 退潮冷却中({elapsed2:.0f}/{EBB_COOLDOWN_MINUTES}分钟), 跳过")
                    cooldown_active = True

            if not cooldown_active:
                # 策略B v2: 暴跌日好公司狙击 — 不受情绪限制(暴跌日本身就是触发条件)
                if _strategy_allows_paper_orders("B"):
                    _check_b_pullback_realtime(result)
                # 策略C: 趋势牛股 — 冰点/退潮不做(反转确认也不做, 等涨跌比>35%)
                if (
                    _strategy_allows_paper_orders("C")
                    and phase not in ("冰点", "退潮")
                    and zt_count >= 50
                ):
                    _check_c_trend_realtime(result)

    # 第三步: 买入扫描
    # 【修复】开盘3分钟内不开新仓(2026-05-21复盘: 09:30情绪数据不稳定导致误判)
    if _is_new_buy_allowed_now():
        now_hm = datetime.now().hour * 100 + datetime.now().minute
        if now_hm < 933:
            logger.info(f"[开盘保护] {now_hm} < 0933, 开盘3分钟内不开新仓")
        else:
            # 反转期暂不开买: 等涨跌比>35%再放开
            if _reversal_signal and _reversal_signal["signal"] == "冰点反转中":
                if _reversal_signal["ratio_now"] < 35:
                    logger.info(f"[反转等待] 涨跌比{_reversal_signal['ratio_now']:.0f}%<35%, 暂不开买")
                else:
                    _scan_buy(market_sentiment, result)
            elif cb_level:
                logger.warning(f"[熔断{cb_level}] 跳过买入扫描")
            elif check_weekly_drawdown():
                logger.warning("[周级风控] 跳过买入扫描")
            else:
                _scan_buy(market_sentiment, result)

    return result


def run_daemon():
    """启动盘中守护进程"""
    import fcntl
    init_tables()
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    pending_active = [
        s for s in _read_pending_signals()
        if s.get("status") in ("pending", "executing")
    ]
    if pending_active:
        logger.error(f"存在{len(pending_active)}条待执行预告, 拒绝启动以避免清空/丢失信号")
        print(f"[ERROR] 存在{len(pending_active)}条待执行预告, 请先处理 pending_signals.json")
        return

    # 单例锁: 文件锁+PID检查双重保护
    pid_file = LOG_DIR / "daemon.pid"
    lock_file = LOG_DIR / "daemon.lock"
    
    # 文件锁: 防止两个daemon同时启动的竞态条件
    lock_fd = open(lock_file, 'w')
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except (IOError, OSError):
        logger.error("守护进程已在运行(文件锁占用), 退出")
        print("[ERROR] daemon已在运行(文件锁), 如需重启先 kill 旧进程并删除 output/trader/daemon.lock")
        return
    
    if pid_file.exists():
        try:
            old_pid = int(pid_file.read_text().strip())
            os.kill(old_pid, 0)  # 检查进程是否存在(不发送信号)
            logger.error(f"守护进程已在运行 PID={old_pid}, 退出(重复启动)")
            print(f"[ERROR] daemon已在运行 PID={old_pid}, 如需重启先 kill {old_pid}")
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
            return
        except (ProcessLookupError, ValueError, PermissionError):
            # 旧进程已死, 清理pid_file
            logger.info(f"旧PID文件残留, 清理后启动")
            pid_file.unlink(missing_ok=True)

    # ── P1a: 注册策略定义 + 创建 daemon run ──
    try:
        config_hash = register_all_strategy_definitions()
    except ValueError as _def_err:
        logger.critical(f"策略定义冲突, 拒绝启动: {_def_err}")
        print(f"[FATAL] {_def_err}")
        fcntl.flock(lock_fd, fcntl.LOCK_UN)
        return

    _write_heartbeat("starting")

    # 日志文件(先清除旧handler防止重复)
    for h in logger.handlers[:]:
        if isinstance(h, logging.FileHandler):
            logger.removeHandler(h)
    fh = logging.FileHandler(
        LOG_DIR / f"daemon_{date.today().isoformat()}.log",
        encoding="utf-8",
    )
    fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(fh)

    logger.info("=" * 60)
    logger.info("盘中交易守护进程启动")
    logger.info(f"资金: {INITIAL_CAPITAL:.0f} | 最多持仓: {MAX_POSITIONS}(B:{B_MAX_POSITIONS}+A:{A_MAX_POSITIONS}+C:{C_MAX_POSITIONS}) | 轮询: 盘中{POLL_INTERVAL_TRADING}s/盘后{POLL_INTERVAL}s")
    logger.info(f"配仓比例: B={B_POSITION_RATIO*100:.0f}%/只, A={A_POSITION_RATIO*100:.0f}%/只, C={C_POSITION_RATIO*100:.0f}%/只, 高价股超配上限{MAX_SINGLE_RATIO*100:.0f}%")
    logger.info(f"止损: {STOP_LOSS_PCT*100}% | 策略A trailing: {SELL_PARAMS['A']['trailing_stop_pct']*100:.1f}%/{SELL_PARAMS['A']['trailing_ebb_pct']*100:.1f}%/{SELL_PARAMS['A']['trailing_frost_pct']*100:.1f}% | 策略B trailing: {SELL_PARAMS['B']['trailing_stop_pct']*100:.1f}%/{SELL_PARAMS['B']['trailing_ebb_pct']*100:.1f}%/{SELL_PARAMS['B']['trailing_frost_pct']*100:.1f}% (正常/退潮/冰点)")
    logger.info(f"追高过滤: 策略B按情绪 退潮>3%/冰点>5%/正常>5%/高潮>8% (13576笔回测)")
    logger.info(f"炸板检测: 一字开盘后现价<涨停价99%跳过 (19700条分钟数据验证)")
    logger.info(f"策略A(龙头首阴反包): 绝对龙头首阴→次日高开2%+翻红确认→持2-3天→跌破首阴低止损")
    logger.info(f"策略B(暴跌日好公司狙击): 暴跌日(均跌>2%)+跌>5%+ROE>10%, 持7天, 止损-6%")
    logger.info("=" * 60)

    acct = get_account()
    held = get_held_positions()
    logger.info(f"当前: 现金{acct['cash']:.0f} 持仓{len(held)}只")
    if held:
        for p in held:
            logger.info(f"  持仓: {p.get('code')} {p.get('name','')} {p.get('shares')}股"
                        f" 买入{p.get('buy_price','?')} 信号={p.get('signal_type','')}")

    # 启动时用实时行情更新持仓最高价
    # (2026-05-14教训: cron进程秒退→09:36才启动→错过开盘跳空高开→highest_price失真)
    if held and _is_trading_time():
        try:
            pos_codes = [p["code"] for p in held]
            quotes = get_realtime(pos_codes)
            updated = 0
            for pos in held:
                q = quotes.get(pos["code"])
                if not q or q.get("price", 0) <= 0:
                    continue
                rt_high = q.get("high", 0)
                if rt_high > 0 and rt_high > pos.get("highest_price", 0):
                    _update_highest(pos["id"], rt_high)
                    updated += 1
                    logger.info(f"[启动] 更新{pos['name']}最高: {pos.get('highest_price',0)}→{rt_high}")
            if updated:
                logger.info(f"[启动] 从实时行情更新了{updated}只持仓最高价")
        except Exception as e:
            logger.warning(f"[启动] 更新持仓最高价失败: {e}")

    # 策略A为龙头首阴反包(无需ML预测刷新)
    # 旧ML预测线程已删除

    # 熔断计数
    error_count = 0

    run_id = ""
    try:
        run_id = create_daemon_run(os.getpid(), config_hash)
        set_run_context(run_id, config_hash)
        pid_file.write_text(str(os.getpid()))
        logger.info(f"PID={os.getpid()} 写入 {pid_file}")
        logger.info(f"[P1a] run_id={run_id[:8]} config_hash={config_hash[:12]}")
    except Exception:
        if run_id:
            update_daemon_run(run_id, "crashed")
        pid_file.unlink(missing_ok=True)
        fcntl.flock(lock_fd, fcntl.LOCK_UN)
        raise

    # 启动成功, 更新状态为 running
    update_daemon_run(run_id, "running")

    def _request_stop(signum, _frame):
        logger.info("收到信号%s, 准备安全停止", signum)
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, _request_stop)
    signal.signal(signal.SIGINT, _request_stop)

    try:
        while True:
            try:
                if not _is_trading_time():
                    now = datetime.now()
                    t = now.hour * 100 + now.minute
                    _write_heartbeat("idle", trading_time=False, hhmm=t)
                    if 920 <= t < 930 or 1250 <= t < 1300:
                        time.sleep(10)
                    elif (now.hour == 11 and now.minute >= 30) or now.hour == 12:
                        logger.info("午间休市, 等待13:00开盘...")
                        time.sleep(30)
                    else:
                        logger.debug(f"非交易时间 {t}, 等60秒")
                        time.sleep(60)
                    continue

                _write_heartbeat("scanning", trading_time=True)
                result = scan_once()
                _update_account_value()
                error_count = 0

                if result["sells"] or result["buys"]:
                    logger.info(
                        f"本轮: 卖出{len(result['sells'])}笔 买入{len(result['buys'])}笔"
                    )
                else:
                    _log_to_db("DEBUG", "scan", "扫描完成, 无操作")
                _write_heartbeat(
                    "scan_complete",
                    trading_time=True,
                    sells=len(result.get("sells", [])),
                    buys=len(result.get("buys", [])),
                )

            except KeyboardInterrupt:
                logger.info("守护进程被用户中断")
                _write_heartbeat("stopping", reason="keyboard_interrupt")
                break
            except Exception as e:
                logger.error(f"扫描异常: {e}", exc_info=True)
                _log_to_db("ERROR", "daemon", str(e))
                error_count += 1
                _write_heartbeat(
                    "scan_error",
                    trading_time=_is_trading_time(),
                    error_count=error_count,
                    error=str(e)[:500],
                )
                if error_count >= 3:
                    logger.critical(f"连续{error_count}次异常, 守护进程退出待cron重启")
                    _log_to_db("CRITICAL", "daemon", f"连续{error_count}次异常, 自动退出")
                    break

            _interval = POLL_INTERVAL_TRADING if _is_trading_time() else POLL_INTERVAL
            time.sleep(_interval)
    finally:
        # ── P1a: 关闭 daemon run ──
        final_status = "stopped"
        if error_count >= 3:
            final_status = "crashed"
        try:
            update_daemon_run(run_id, final_status)
            logger.info(f"[P1a] daemon run {run_id[:8]} closed as {final_status}")
        finally:
            pid_file.unlink(missing_ok=True)

def print_status():
    """打印当前状态"""
    init_tables()
    acct = get_account()
    held = get_held_positions()

    print(f"\n{'='*60}")
    print(f"盘中交易守护进程 - 状态报告")
    print(f"{'='*60}")
    print(f"日期: {date.today().isoformat()}")
    print(f"现金: ¥{acct['cash']:,.0f}")

    if held:
        codes = [h["code"] for h in held]
        quotes = get_realtime(codes)
        total_mv = 0
        print(f"\n持仓 ({len(held)}/{MAX_POSITIONS}):")
        print(f"{'代码':<8} {'名称':<8} {'买入价':>8} {'现价':>8} {'盈亏%':>8} {'最高':>8} {'天数':>4} {'信号'}")
        print("-" * 80)
        for h in held:
            q = quotes.get(h["code"], {})
            price = q.get("price", 0)
            pnl_pct = (price / h["buy_price"] - 1) * 100 if price > 0 else 0
            mv = price * h["shares"]
            total_mv += mv
            print(f"{h['code']:<8} {h['name']:<8} {h['buy_price']:>8.2f} {price:>8.2f} "
                  f"{pnl_pct:>+7.1f}% {h.get('highest_price', 0):>8.2f} "
                  f"{h.get('hold_days', '?'):>4} {h.get('signal_type', '')}")

        print(f"\n市值: ¥{total_mv:,.0f}  总资产: ¥{acct['cash']+total_mv:,.0f}")
    else:
        print("\n当前无持仓")

    # 最近交易
    conn = _get_conn()
    try:
        trades = conn.execute(
            "SELECT * FROM daemon_trades WHERE period=? ORDER BY trade_time DESC LIMIT 10",
            (CURRENT_PERIOD,)
        ).fetchall()
        if trades:
            print(f"\n最近交易:")
            print(f"{'时间':<20} {'操作':<4} {'代码':<8} {'名称':<8} {'价格':>8} {'盈亏':>10}")
            print("-" * 70)
            for t in trades:
                action_str = "买入" if t["action"] == "buy" else "卖出"
                pnl_str = f"¥{t['pnl']:+,.0f}" if t["action"] == "sell" else "-"
                print(f"{t['trade_time']:<20} {action_str:<4} {t['code']:<8} {t['name']:<8} "
                      f"{t['price']:>8.2f} {pnl_str:>10}")
    finally:
        conn.close()

    # 累计统计
    print(f"\n累计统计:")
    print(f"  总交易: {acct.get('total_trades', 0)}笔")
    print(f"  胜率: {acct.get('win_trades', 0) / max(acct.get('total_trades', 1), 1) * 100:.1f}%")
    print(f"  累计盈亏: ¥{acct.get('cumulative_pnl', 0):+,.0f}")
    print(f"{'='*60}\n")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import click

    @click.group()
    def cli():
        pass

    @cli.command()
    @click.option("--once", is_flag=True, help="单次扫描(不进入主循环)")
    def start(once):
        """启动盘中交易守护进程"""
        if MAINTENANCE_PAUSE_FILE.exists():
            raise click.ClickException(
                f"维护暂停标记存在，拒绝启动: {MAINTENANCE_PAUSE_FILE}"
            )
        if once:
            result = scan_once()
            print(json.dumps(result, ensure_ascii=False, indent=2))
        else:
            run_daemon()

    @cli.command()
    def status():
        """查看当前状态"""
        print_status()

    @cli.command()
    @click.confirmation_option(prompt="确定要重置模拟盘?")
    def reset():
        """重置模拟盘"""
        conn = _get_conn()
        try:
            conn.execute("DELETE FROM daemon_positions WHERE period=?", (CURRENT_PERIOD,))
            conn.execute("DELETE FROM daemon_trades WHERE period=?", (CURRENT_PERIOD,))
            conn.execute("DELETE FROM daemon_account WHERE period=?", (CURRENT_PERIOD,))
            conn.execute("DELETE FROM daemon_log")
            conn.commit()
        finally:
            conn.close()
        init_tables()
        get_account()  # 重新初始化
        print(f"模拟盘已重置! 期次{CURRENT_PERIOD} 初始资金: ¥{INITIAL_CAPITAL:,.0f}")

    @cli.command()
    def trades():
        """查看全部交易记录"""
        conn = _get_conn()
        try:
            rows = conn.execute(
                "SELECT * FROM daemon_trades ORDER BY trade_time DESC LIMIT 50"
            ).fetchall()
            if not rows:
                print("暂无交易记录")
                return
            print(f"\n{'时间':<20} {'操作':<4} {'代码':<8} {'名称':<8} "
                  f"{'价格':>8} {'股数':>6} {'金额':>10} {'盈亏':>10} {'原因'}")
            print("=" * 110)
            for t in rows:
                action_str = "买入" if t["action"] == "buy" else "卖出"
                pnl_str = f"¥{t['pnl']:+,.0f}" if t["pnl"] != 0 else "-"
                print(f"{t['trade_time']:<20} {action_str:<4} {t['code']:<8} {t['name']:<8} "
                      f"{t['price']:>8.2f} {t['shares']:>6} {t['amount']:>10,.0f} "
                      f"{pnl_str:>10} {t.get('reason', '')}")
        finally:
            conn.close()

    cli()
