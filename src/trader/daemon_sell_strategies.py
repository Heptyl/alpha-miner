"""策略卖出逻辑 — 从 trading_daemon.scan_once 拆出的独立方法

每个方法接收持仓 dict 和实时行情 dict，返回卖信号 dict 或 None。

用法:
    from src.trader.daemon_sell_strategies import (
        _check_sell_strategy_c,
        _check_sell_strategy_a,
        _check_sell_strategy_b,
    )
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

from src.trader.daemon_config import STRATEGY_C_CONFIG, SELL_PARAMS, STRATEGY_HARD_STOP_PCT
from src.trader.daemon_config import USE_ATR_STOP, ATR_STOP_FLOOR_PCT, ATR_STOP_CAP_PCT
from src.trader.daemon_db import _count_trading_days
from src.trader.tech_indicators import get_atr_cached

logger = logging.getLogger("trading_daemon")


def _calc_atr_stop(atr: float | None, buy_price: float, multiplier: float,
                   fallback_pct: float) -> float:
    """计算ATR动态止损百分比(负数). ATR为None时fallback到固定%."""
    if atr and buy_price > 0:
        atr_pct = -atr / buy_price * multiplier
        return max(ATR_STOP_CAP_PCT, min(ATR_STOP_FLOOR_PCT, atr_pct))
    return fallback_pct


# _log_to_db 和 check_sell_signals 在运行时由 trading_daemon 提供；
# 此处仅做类型提示，不强制导入（避免循环依赖）。
# 如需独立运行，请自行注入或 mock。


# ---------------------------------------------------------------------------
# 策略C: 趋势牛股卖出
# ---------------------------------------------------------------------------

def _check_sell_strategy_c(pos: dict, q: dict) -> Optional[dict]:
    """策略C v3 (基本面驱动+AI赛道) 卖出检查

    回测验证(2025全年AI赛道): PF=2.52, 胜率63%, 收益+7%
    
    规则(优先级从高到低):
      1. T+1 保护 — 买入当天不卖
      2. 硬止损 -8% (固定止损, 不受行情影响)
      3. 目标收益 +12% → 卖出(回测PF=2.52的核心规则)
      4. trailing止盈 8% (从最高点回落超8%则卖)
      5. 时间止损: 持>=20天涨幅<5% → 清仓
      6. 最长持有 30天 到期清仓
      7. 涨停豁免: 当天涨停不清仓
    """
    from src.trader.daemon_db import _log_to_db

    sell_signal = None

    chg_from_buy = (q["price"] / pos.get("buy_price", q["price"]) - 1)
    # 策略C: T+1保护(买入当天不能卖)
    buy_date_str_c = pos.get("buy_date", pos.get("buy_time", "")[:10])
    try:
        buy_dt_c = datetime.strptime(buy_date_str_c[:10], "%Y-%m-%d")
        if buy_dt_c.date() >= datetime.now().date():
            c_hold_days = 0
        else:
            c_hold_days = _count_trading_days(buy_date_str_c[:10])
    except ValueError:
        c_hold_days = 99  # 解析失败视为可卖
    
    if c_hold_days < 1:
        # 买入当天不卖, 只更新最高价
        pass
    
    # 1. 止损: ATR动态 或 固定%
    elif (chg_from_buy <= _calc_atr_stop(
        get_atr_cached(pos["code"]),
        pos.get("buy_price", q["price"]),
        SELL_PARAMS["C"]["atr_multiplier"],
        STRATEGY_C_CONFIG['stop_loss_pct'],
    ) if USE_ATR_STOP else chg_from_buy <= STRATEGY_C_CONFIG['stop_loss_pct']):
        if USE_ATR_STOP:
            atr = get_atr_cached(pos["code"])
            stop_info = f"ATR={atr:.2f}×{SELL_PARAMS['C']['atr_multiplier']}" if atr else "固定%"
        else:
            stop_info = f"固定{STRATEGY_C_CONFIG['stop_loss_pct']*100:.0f}%"
        sell_signal = {
            "reason": f"策略C止损: 跌{chg_from_buy*100:+.1f}% ({stop_info})",
            "type": "策略C止损",
        }
    
    # 2. 目标收益 +12% → 卖出(回测PF=2.52 vs 15%的1.40)
    elif chg_from_buy >= STRATEGY_C_CONFIG.get('target_profit', 0.12):
        sell_signal = {
            "reason": f"策略C目标收益: {chg_from_buy*100:+.1f}%",
            "type": "策略C止盈",
        }
    
    # 3. trailing止盈: 从最高点回落超8%
    elif pos.get("buy_price") and q.get("price", 0) > 0:
        buy_p = pos["buy_price"]
        current_p = q["price"]
        high_price = max(pos.get("highest_price", buy_p), current_p, buy_p)
        if high_price > buy_p:
            drawdown = (current_p / high_price - 1)
            trail_pct = SELL_PARAMS["C"].get("trailing_stop_pct", 0.08)
            if drawdown <= -trail_pct:
                pnl_trail = (current_p / buy_p - 1) * 100
                sell_signal = {
                    "reason": f"策略C trailing止盈: 从最高{high_price:.2f}回落{drawdown*100:+.1f}% 收益{pnl_trail:+.1f}%",
                    "type": "策略C止盈",
                }
    
    # 4. 时间止损: 持>=20天涨幅<5%
    if not sell_signal and c_hold_days >= SELL_PARAMS["C"].get("time_stop_days", 20):
        time_threshold = SELL_PARAMS["C"].get("time_stop_threshold", 0.05)
        if chg_from_buy < time_threshold:
            sell_signal = {
                "reason": f"策略C时间止损: 持{c_hold_days}天涨幅{chg_from_buy*100:+.1f}%",
                "type": "策略C清仓",
            }
    
    # 5. 最长持有: 30天到期无条件清仓
    if not sell_signal and c_hold_days >= STRATEGY_C_CONFIG['max_hold_days']:
        sell_signal = {
            "reason": f"策略C最长持有{STRATEGY_C_CONFIG['max_hold_days']}天: 收益{chg_from_buy*100:+.1f}%",
            "type": "策略C清仓",
        }
    
    # 6. 涨停豁免: 不卖
    if sell_signal and sell_signal.get("type") in ("策略C止盈", "策略C清仓"):
        chg_now = q.get("change_pct_calc", 0) or 0
        _code_pfx = pos.get("code", "")[:3]
        if _code_pfx.startswith(("300", "301")):
            is_limit_up = chg_now >= 19.5
        else:
            is_limit_up = chg_now >= 9.5
        if is_limit_up:
            logger.info(f"[策略C] {pos['code']} 涨停{chg_now:+.1f}%豁免,继续持有")
            try:
                _log_to_db("INFO", "strategy_c", f"{pos['code']}涨停豁免 pnl={chg_from_buy*100:+.2f}%")
            except Exception:
                pass
            sell_signal = None

    return sell_signal


# ---------------------------------------------------------------------------
# 策略A: 龙头首阴反包 卖出
# ---------------------------------------------------------------------------

def _check_sell_strategy_a(pos: dict, q: dict, market: dict = None) -> Optional[dict]:
    """策略A (龙头首阴反包) 卖出检查

    调研结论(7来源共识):
      1. 止损: 跌破首阴最低价(从buy_reason解析或用-5%兜底)
      2. 到期: 持>=3天且不涨停则清仓
      3. 收盘兜底: 第3天14:45清仓
      4. 涨停豁免: 封板继续持
    """
    sell_signal = None

    chg_from_buy_a = (q["price"] / pos.get("buy_price", q["price"]) - 1)
    buy_date_str_a = pos.get("buy_date", pos.get("buy_time", "")[:10])
    try:
        buy_dt_a = datetime.strptime(buy_date_str_a[:10], "%Y-%m-%d")
        if buy_dt_a.date() >= datetime.now().date():
            a_hold_days = 0
        else:
            a_hold_days = _count_trading_days(buy_date_str_a[:10])
    except ValueError:
        a_hold_days = 99

    chg_now_a = q.get("change_pct_calc", 0) or 0
    # 涨停豁免: 区分主板(10%)和创业板(20%)
    code_prefix = pos.get("code", "")[:3]
    if code_prefix.startswith(("300", "301")):
        is_limit_up_a = chg_now_a >= 19.5  # 创业板20%涨停
    else:
        is_limit_up_a = chg_now_a >= 9.5   # 主板10%涨停

    # 止损价: 优先用首阴最低价(从signal_reason解析), 兜底-5%
    yin_low_stop = None
    signal_reason = pos.get("signal_reason", "") or pos.get("buy_reason", "")
    import re
    m = re.search(r"止损[¥]?([\d.]+)", signal_reason)
    if m:
        try:
            yin_low_stop = float(m.group(1))
        except ValueError:
            pass

    # 1. 止损: 跌破首阴最低价 或 ATR动态/固定止损
    should_stop = False
    stop_reason = ""
    if USE_ATR_STOP:
        atr_a = get_atr_cached(pos["code"])
        stop_threshold_a = _calc_atr_stop(atr_a, pos.get("buy_price", q["price"]),
                                    SELL_PARAMS["A"]["atr_multiplier"],
                                    SELL_PARAMS["A"]["stop_loss_pct"])
        atr_info = f"ATR={atr_a:.2f}×{SELL_PARAMS['A']['atr_multiplier']}" if atr_a else "固定%"
    else:
        stop_threshold_a = SELL_PARAMS["A"]["stop_loss_pct"]
        atr_info = f"固定{SELL_PARAMS['A']['stop_loss_pct']*100:.0f}%"
    if yin_low_stop and yin_low_stop > 0 and q["price"] <= yin_low_stop:
        should_stop = True
        stop_reason = f"跌破首阴低¥{yin_low_stop:.2f}"
    elif chg_from_buy_a <= stop_threshold_a:
        should_stop = True
        stop_reason = f"止损{chg_from_buy_a*100:+.1f}%({atr_info})"

    if should_stop:
        # 策略硬止损: 浮亏超限无条件止损, 不等技术过滤
        if chg_from_buy_a <= STRATEGY_HARD_STOP_PCT:
            sell_signal = {
                "reason": f"策略A硬止损: {stop_reason}",
                "type": "策略A止损",
            }
        else:
            from src.trader.tech_indicators import should_skip_stop_loss
            skip, tech_reason = should_skip_stop_loss(pos["code"], chg_from_buy_a, "A", quote=q)
            if skip:
                logger.info(f"[技术过滤] {pos['code']} {pos['name']} {stop_reason} 但{tech_reason} → 跳过")
            else:
                # 分时数据不足时跳过本轮, 等数据积累后再判断
                if "分时不足" in tech_reason:
                    logger.debug(f"[策略A] {pos['code']} {pos['name']} {stop_reason} 但分时数据不足, 跳过本轮")
                else:
                    sell_signal = {
                        "reason": f"策略A止损: {stop_reason} ({tech_reason})",
                        "type": "策略A止损",
                    }

    # 2. 到期持仓(hold_days>=3): 开盘即卖
    if not sell_signal and a_hold_days >= SELL_PARAMS["A"]["max_hold_days"] and not is_limit_up_a:
        now_hm = datetime.now().hour * 100 + datetime.now().minute
        if now_hm >= 930:
            pnl_a = chg_from_buy_a * 100
            sell_signal = {
                "reason": f"策略A到期清仓(hold={a_hold_days}天): 收益{pnl_a:+.1f}%",
                "type": "策略A清仓",
            }

    # 3. 第3个持仓日收盘兜底: 14:45(让利润跑到尾盘)
    if not sell_signal and not is_limit_up_a:
        now_hm = datetime.now().hour * 100 + datetime.now().minute
        if now_hm >= 1445 and a_hold_days >= SELL_PARAMS["A"]["max_hold_days"]:
            pnl_a = chg_from_buy_a * 100
            sell_signal = {
                "reason": f"策略A尾盘清仓: 收益{pnl_a:+.1f}%",
                "type": "策略A清仓",
            }

    # 4. trailing止盈: 从买入后最高点回落超阈值则卖
    if not sell_signal and pos.get("buy_price"):
        buy_p = pos["buy_price"]
        current_p = q.get("price", 0)
        # 修复: 使用DB中的highest_price而非当前轮次max, 避免漏掉历史高点
        high_price = max(pos.get("highest_price", buy_p), current_p, buy_p)
        if high_price > buy_p and current_p > 0:
            drawdown = (current_p / high_price - 1)
            # 退潮/冰点时收紧trailing
            if market and market.get("can_buy") is False:
                trail_pct = SELL_PARAMS["A"].get("trailing_frost_pct", 0.015)
            else:
                trail_pct = SELL_PARAMS["A"].get("trailing_stop_pct", 0.03)
            if drawdown <= -trail_pct:
                pnl_trail = (current_p / buy_p - 1) * 100
                sell_signal = {
                    "reason": f"策略A trailing止盈: 从最高{high_price:.2f}回落{drawdown*100:+.1f}% 收益{pnl_trail:+.1f}%",
                    "type": "策略A止盈",
                }

    # 5. 涨停豁免: 不卖
    if is_limit_up_a and not sell_signal:
        pass  # 涨停不卖, 继续持有

    return sell_signal


# ---------------------------------------------------------------------------
# 策略B v2: 暴跌日好公司狙击 卖出
# T+1回测: PF=1.29/胜率51.3%, 止损-6%/持7天/trailing 5%
# ---------------------------------------------------------------------------

def _check_sell_strategy_b(pos: dict, q: dict, market: dict) -> Optional[dict]:
    """策略B v2 (暴跌日好公司狙击) 卖出检查

    T+1回测: PF=1.29/胜率51.3%
    参数来自 SELL_PARAMS["B"]: 止损-6%/持7天/trailing 5%(退潮3%/冰点2%)

    规则(优先级从高到低):
      1. 止损 -6% (SELL_PARAMS驱动)
      2. 持 7 天到期 14:45 清仓 (涨停豁免)
      3. trailing 止盈 (从最高点回落, 退潮/冰点收紧)

    Args:
        pos: 持仓 dict
        q: 实时行情 dict
        market: 市场情绪 dict, 需包含 ``"phase"`` 字段
    """

    sell_signal = None

    chg_from_buy_b = (q["price"] / pos.get("buy_price", q["price"]) - 1)
    buy_date_str_b = pos.get("buy_date", pos.get("buy_time", "")[:10])
    try:
        buy_dt_b = datetime.strptime(buy_date_str_b[:10], "%Y-%m-%d")
        if buy_dt_b.date() >= datetime.now().date():
            b_hold_days = 0
        else:
            b_hold_days = _count_trading_days(buy_date_str_b[:10])
    except ValueError:
        b_hold_days = 99

    # 止损: ATR动态 或 固定-6%
    should_stop_b = False
    stop_reason_b = ""
    if USE_ATR_STOP:
        atr_b = get_atr_cached(pos["code"])
        stop_threshold_b = _calc_atr_stop(atr_b, pos.get("buy_price", q["price"]),
                                    SELL_PARAMS["B"]["atr_multiplier"],
                                    SELL_PARAMS["B"]["stop_loss_pct"])
        atr_info_b = f"ATR={atr_b:.2f}×{SELL_PARAMS['B']['atr_multiplier']}" if atr_b else "固定%"
    else:
        stop_threshold_b = SELL_PARAMS["B"]["stop_loss_pct"]
        atr_info_b = f"固定{SELL_PARAMS['B']['stop_loss_pct']*100:.0f}%"
    if chg_from_buy_b <= stop_threshold_b:
        should_stop_b = True
        stop_reason_b = f"止损{chg_from_buy_b*100:+.1f}%({atr_info_b})"

    if should_stop_b:
        # 策略硬止损: 浮亏超限无条件止损, 不等技术过滤
        if chg_from_buy_b <= STRATEGY_HARD_STOP_PCT:
            sell_signal = {
                "reason": f"策略B硬止损: {stop_reason_b}",
                "type": "策略B止损",
            }
        else:
            from src.trader.tech_indicators import should_skip_stop_loss
            skip, tech_reason = should_skip_stop_loss(pos["code"], chg_from_buy_b, "B", quote=q)
            if skip:
                logger.info(f"[技术过滤] {pos['code']} {pos['name']} {stop_reason_b} 但{tech_reason} → 跳过")
            else:
                if "分时不足" in tech_reason:
                    logger.debug(f"[策略B] {pos['code']} {pos['name']} {stop_reason_b} 但分时数据不足, 跳过本轮")
                else:
                    sell_signal = {
                        "reason": f"策略B止损: {stop_reason_b} ({tech_reason})",
                        "type": "策略B止损",
                    }
    elif b_hold_days >= SELL_PARAMS["B"]["max_hold_days"]:
        # 持3天: 14:45清仓(涨停豁免)  # [GUARD-BYPASS]
        now_hm = datetime.now().hour * 100 + datetime.now().minute
        chg_now_b = q.get("change_pct_calc", 0) or 0
        _code_pfx_b = pos.get("code", "")[:3]
        if _code_pfx_b.startswith(("300", "301")):
            is_limit_up_b = chg_now_b >= 19.5
        else:
            is_limit_up_b = chg_now_b >= 9.5
        if now_hm >= 1445:
            if is_limit_up_b:
                sell_signal = None  # 涨停豁免, 继续持有
                logger.info(f"[策略B] {pos['code']} {pos['name']} 涨停{chg_now_b:+.1f}%豁免清仓, 继续持有")
            else:
                pnl_b = chg_from_buy_b * 100
                sell_signal = {
                    "reason": f"策略B持{SELL_PARAMS['B']['max_hold_days']}天清仓: 收益{pnl_b:+.1f}%",
                    "type": "策略B清仓",
                }
    elif b_hold_days >= 1:
        # trailing止盈(从最高点回落, 退潮/冰点收紧)
        if pos.get("buy_price") and q.get("price", 0) > 0:
            buy_p = pos["buy_price"]
            current_p = q["price"]
            high_price = max(pos.get("highest_price", buy_p), current_p, buy_p)
            if high_price > buy_p:
                drawdown = (current_p / high_price - 1)
                market_phase = market.get("phase", "未知") if market else "未知"
                if market_phase in ("冰点", "偏弱", "退潮预警"):
                    trail_pct = SELL_PARAMS["B"]["trailing_frost_pct"]
                elif market_phase == "退潮":
                    trail_pct = SELL_PARAMS["B"]["trailing_ebb_pct"]
                else:
                    trail_pct = SELL_PARAMS["B"]["trailing_stop_pct"]
                if drawdown <= -trail_pct:
                    pnl_trail = (current_p / buy_p - 1) * 100
                    sell_signal = {
                        "reason": f"策略B trailing止盈: 从最高{high_price:.2f}回落{drawdown*100:+.1f}% 收益{pnl_trail:+.1f}%",
                        "type": "策略B止盈",
                    }

    return sell_signal
