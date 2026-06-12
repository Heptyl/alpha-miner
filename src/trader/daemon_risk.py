"""daemon_risk.py — 风控检查与退潮保护

从 trading_daemon.py 拆分出的风控相关函数。
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, date, timedelta
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.trader.daemon_config import (
    DB_PATH, CURRENT_PERIOD, INITIAL_CAPITAL,
    MAX_SAME_INDUSTRY, DAILY_LOSS_LIMIT, EBB_COOLDOWN_MINUTES,
    GRACE_PERIOD_ENABLED, GRACE_PERIOD_MINUTES,
    MARKET_OPEN_AM, MARKET_CLOSE_PM,
    RISK_MODE, PAPER_CONSECUTIVE_LOSSES, PAPER_DAILY_LOSS_LIMIT_PCT,
    PAPER_WEEKLY_LOSS_LIMIT_PCT,
)
from src.trader.daemon_db import _get_conn, _log_to_db, get_held_positions
from src.trader.daemon_signals import _add_signal, _read_pending_signals
from src.trader.realtime_quote import get_realtime

logger = logging.getLogger("trading_daemon")

# 运行时可变状态(退潮冷却计时)
_last_ebb_clear_time = None

# 情绪平滑状态
_last_valid_up_ratio: float | None = None  # 上一轮有效涨跌比
_phase_history: list[str] = []              # 最近N次phase判定
_PHASE_SMOOTH_N = 2                         # 连续N次同phase才切换
_confirmed_phase: str | None = None         # 已确认的phase(平滑后)


def _is_paper_mode() -> bool:
    """模拟盘模式: 只记录风险事件, 不让保护逻辑阻断采样。"""
    return RISK_MODE == "paper"


def _get_confirmed_phase() -> str | None:
    return _confirmed_phase


def _set_confirmed_phase(phase: str):
    global _confirmed_phase
    _confirmed_phase = phase


def _check_lhb_filter(code: str, cand: dict) -> str | None:
    """龙虎榜过滤 — 排除LHB净卖出的涨停(游资出货)
    
    回测验证:
      全部ZT(不过滤): 胜率60.3%
      LHB净买入ZT: 胜率68.1% → 优选
      LHB净卖出ZT: 胜率51.7% → 排除
      首板+LHB净卖出: 胜率43.8% → 必须排除
    """
    conn = _get_conn()
    try:
        # 查昨日涨停日的龙虎榜数据
        # 涨停候选的cand里有trade_date或从日期推断
        row = conn.execute("""
            SELECT buy_amount, sell_amount, net_amount
            FROM lhb_detail
            WHERE stock_code = ?
            ORDER BY trade_date DESC LIMIT 1
        """, (code,)).fetchone()
        
        if row is None:
            return None  # 不在龙虎榜上, 放行
        
        net = row['net_amount'] if row['net_amount'] else 0
        # 净卖出>0 (sell > buy) → 游资出货
        if net < 0:
            return f"龙虎榜净卖出: {net/10000:.0f}万"
        
        return None  # 净买入或平衡, 放行
    except Exception:
        return None  # 查询失败, 放行
    finally:
        conn.close()




def _is_trading_time() -> bool:
    """判断是否在交易时间"""
    now = datetime.now()
    t = now.hour * 100 + now.minute
    weekday = now.weekday()
    if weekday >= 5:  # 周末
        return False
    # 9:30-11:30 或 13:00-15:00
    return (930 <= t <= 1130) or (1300 <= t <= 1500)




def _is_grace_period() -> bool:
    """开盘后GRACE_PERIOD_MINUTES分钟内, 止损延迟执行
    4452笔回测: 开盘30分钟波动剧烈, 止损延迟后净效果+0.52%/笔
    """
    now = datetime.now()
    open_minutes = now.hour * 60 + now.minute - (MARKET_OPEN_AM[0] * 60 + MARKET_OPEN_AM[1])
    return 0 <= open_minutes < GRACE_PERIOD_MINUTES




def _check_industry_concentration(code: str) -> bool:
    """检查同行业集中度, 返回True=可以买入, False=超过限制

    交易员视角: 买3只证券股=单一板块风险, 一个政策全亏
    数据源: stock_industry_mapping优先(东财行业), concept_mapping备用
    """
    if MAX_SAME_INDUSTRY <= 0:
        return True  # 未启用

    conn = _get_conn()
    try:
        # 查候选股票的行业(优先stock_industry_mapping)
        row = conn.execute(
            "SELECT industry_name FROM stock_industry_mapping WHERE stock_code = ?",
            (code,)
        ).fetchone()
        if not row:
            # fallback到concept_mapping
            row = conn.execute(
                "SELECT concept_name FROM concept_mapping WHERE stock_code = ? LIMIT 1",
                (code,)
            ).fetchone()
        if not row:
            return True  # 无行业数据, 放行

        industry = row[0]

        # 查当前持仓中同行业的数量
        positions = get_held_positions()
        held_codes = [p["code"] for p in positions]

        if not held_codes:
            return True

        # 查持仓中每只股票的行业
        placeholders = ",".join("?" * len(held_codes))
        rows = conn.execute(
            f"SELECT stock_code, industry_name FROM stock_industry_mapping WHERE stock_code IN ({placeholders})",
            held_codes
        ).fetchall()
        # stock_industry_mapping未覆盖的, fallback到concept_mapping
        covered_codes = {r[0] for r in rows}
        uncovered = [c for c in held_codes if c not in covered_codes]
        if uncovered:
            placeholders2 = ",".join("?" * len(uncovered))
            rows2 = conn.execute(
                f"SELECT stock_code, concept_name FROM concept_mapping WHERE stock_code IN ({placeholders2})",
                uncovered
            ).fetchall()
            rows = list(rows) + list(rows2)

        # 同行业计数
        same_count = sum(1 for _, ind in rows if ind == industry)

        if same_count >= MAX_SAME_INDUSTRY:
            held_names = [p["name"] for p in positions if p["code"] in [r[0] for r in rows if r[1] == industry]]
            logger.info(f"[集中度] {code} 行业={industry}, 已持{same_count}只({held_names}), 超限{MAX_SAME_INDUSTRY}, 跳过")
            return False

        return True
    except Exception as e:
        logger.warning(f"[集中度] 检查失败: {e}, 放行")
        return True
    finally:
        conn.close()




def _get_turnover_percentile() -> tuple[float, float]:
    """计算全市场均换手率在历史中的百分位排名

    取最近有效交易日(>500只股票有换手率数据)的均换手率,
    在过去240个有效交易日的分布中计算百分位。

    Returns:
        (percentile, avg_turnover), 百分位0-100。数据不足时返回(-1, 0)
    """
    conn = _get_conn()
    try:
        today = datetime.now().strftime("%Y-%m-%d")

        # 最近有效交易日(盘中数据可能不全, 取最近完整日)
        ref_row = conn.execute("""
            SELECT trade_date, AVG(turnover_rate) as avg_tr, COUNT(*) as cnt
            FROM daily_price
            WHERE turnover_rate > 0 AND trade_date <= ?
            GROUP BY trade_date
            HAVING cnt > 500
            ORDER BY trade_date DESC
            LIMIT 1
        """, (today,)).fetchone()

        if not ref_row:
            return -1, 0

        ref_date, today_tr, _ = ref_row

        # 历史分布(过去240个有效交易日)
        hist = conn.execute("""
            SELECT AVG(turnover_rate) as avg_tr
            FROM daily_price
            WHERE turnover_rate > 0 AND trade_date <= ?
            GROUP BY trade_date
            HAVING COUNT(*) > 500
            ORDER BY trade_date DESC
            LIMIT 240
        """, (ref_date,)).fetchall()

        if len(hist) < 20:
            return -1, today_tr

        turnovers = [r[0] for r in hist]
        rank = sum(1 for t in turnovers if t < today_tr)
        percentile = rank / len(turnovers) * 100

        return percentile, today_tr
    except Exception as e:
        logger.warning(f"换手率百分位查询异常: {e}")
        return -1, 0
    finally:
        conn.close()


def _check_market_sentiment() -> dict:
    """检查大盘情绪 — 涨跌比+换手率百分位二维判断

    v2增加换手率百分位维度(取最近有效交易日的均换手率在过去240天的百分位):
      恐慌: 换手率P<20 且 涨跌比<40% → 冰点
      谨慎: 换手率P20-40 或 涨跌比40-50% → 退潮
      正常: 换手率P40-70 且 涨跌比50-65% → 正常
      贪婪: 换手率P>70 且 涨跌比>65% → 正常(暂不特殊处理)

    保留涨跌比/涨停数作为辅助信息。
    换手率数据不足时回退到原有涨跌比判断。

    Returns:
        {"can_buy", "phase", "reason", "zt_count", "up_count", "down_count",
         "turnover_pct", "avg_turnover"}
    """
    try:
        from src.strategy.strategy_b import get_market_emotion
        emo = get_market_emotion()

        phase = emo.get("phase", "未知")
        can_buy = emo.get("can_buy", True)
        zt_count = emo.get("zt_count", 0)
        up_count = emo.get("up_count", 0)
        down_count = emo.get("down_count", 0)
        hint = emo.get("strategy_hint", "")
    except Exception as e:
        logger.warning(f"情绪检查异常: {e}, fail-closed暂停买入")
        return {"can_buy": False, "phase": "冰点", "reason": f"情绪异常: {e}",
                "zt_count": 0, "up_count": 0, "down_count": 0,
                "turnover_pct": -1, "avg_turnover": 0}

    # 换手率百分位
    tr_pct, avg_tr = _get_turnover_percentile()

    if tr_pct < 0:
        # 换手率数据不足, 用原有涨跌比判断
        logger.info(f"[情绪] 换手率数据不足, 用涨跌比判断: {phase} zt={zt_count}")
        return {
            "can_buy": can_buy,
            "phase": phase,
            "reason": hint,
            "zt_count": zt_count,
            "up_count": up_count,
            "down_count": down_count,
            "turnover_pct": -1,
            "avg_turnover": 0,
        }

    # 涨跌比(百分比)
    global _last_valid_up_ratio
    total = up_count + down_count
    if total > 0:
        up_ratio = up_count / total * 100
        _last_valid_up_ratio = up_ratio
    else:
        # API失败(up/down=0): 用上一轮有效值, 不要默认50%
        if _last_valid_up_ratio is not None:
            up_ratio = _last_valid_up_ratio
            logger.info(f"[情绪] 涨跌比API失败, 用上一轮有效值{up_ratio:.0f}%")
        else:
            # 启动后第一次就失败必须fail-closed。默认50%会把市场误判为中性。
            up_ratio = 0
            logger.warning("[情绪] 涨跌比API失败且无历史值, fail-closed按冰点处理")

    # v2: 换手率+涨跌比二维判定(优先级: 恐慌>贪婪>谨慎>正常)
    if tr_pct < 20 and up_ratio < 40:
        new_phase = "冰点"
        new_can_buy = False
        new_reason = f"恐慌: 换手率P{tr_pct:.0f}(<20) 涨跌比{up_ratio:.0f}%(<40%)"
    elif tr_pct > 70 and up_ratio > 65:
        new_phase = "正常"
        new_can_buy = True
        new_reason = f"贪婪: 换手率P{tr_pct:.0f}(>70) 涨跌比{up_ratio:.0f}%(>65%)"
    # [GUARD-BYPASS] 退潮阈值从40%降到30%, 让系统多交易积累记忆
    elif (20 <= tr_pct < 40) or (30 <= up_ratio < 40):
        new_phase = "退潮"
        new_can_buy = False
        new_reason = f"谨慎: 换手率P{tr_pct:.0f} 涨跌比{up_ratio:.0f}%"
    elif 40 <= tr_pct <= 70 and 40 <= up_ratio <= 65:
        new_phase = "正常"
        new_can_buy = True
        new_reason = f"正常: 换手率P{tr_pct:.0f} 涨跌比{up_ratio:.0f}%"
    else:
        # 未分类(如P<20且up>=50%, 或P>70且up<=65%), 用原有涨跌比判断
        new_phase = phase
        new_can_buy = can_buy
        new_reason = f"默认({phase}): 换手率P{tr_pct:.0f} 涨跌比{up_ratio:.0f}%"

    logger.info(f"[情绪] {new_reason} 涨停{zt_count} 均换手{avg_tr:.2f}% → {new_phase} can_buy={new_can_buy}")

    # 情绪平滑: 连续N次同phase才切换, 避免API抖动导致冰点/正常反复跳
    global _phase_history
    _phase_history.append(new_phase)
    if len(_phase_history) > _PHASE_SMOOTH_N:
        _phase_history = _phase_history[-_PHASE_SMOOTH_N:]

    if len(_phase_history) >= _PHASE_SMOOTH_N:
        if all(p == new_phase for p in _phase_history):
            # 连续N次一致, 确认切换
            pass
        else:
            # 不一致, 沿用上一次确认的phase
            confirmed = _get_confirmed_phase()
            if confirmed and confirmed != new_phase:
                logger.info(f"[情绪平滑] 原始={new_phase}, 确认沿用={confirmed} "
                            f"(近{_PHASE_SMOOTH_N}次: {_phase_history})")
                new_phase = confirmed
                # 重算can_buy
                if new_phase in ("冰点", "退潮"):
                    new_can_buy = False
                else:
                    new_can_buy = True

    _set_confirmed_phase(new_phase)

    return {
        "can_buy": new_can_buy,
        "phase": new_phase,
        "reason": new_reason,
        "zt_count": zt_count,
        "up_count": up_count,
        "down_count": down_count,
        "turnover_pct": round(tr_pct, 1),
        "avg_turnover": round(avg_tr, 4),
    }




def _check_consecutive_losses(n: int = 3) -> bool:
    """连亏保护: 最近N笔交易全部亏损 → 暂停买入

    逻辑: 连亏说明当前市场不适合操作, 需要冷静
    策略A回测: 最长连续亏损10笔, 连亏3笔后继续亏概率>60%
    """
    if _is_paper_mode():
        n = max(n, PAPER_CONSECUTIVE_LOSSES)

    conn = _get_conn()
    try:
        recent = conn.execute("""
            SELECT pnl FROM daemon_trades
            WHERE action='sell' AND period=? AND trade_date >= date('now', '-7 days') AND reason NOT LIKE '%作废%' AND reason NOT LIKE '%撤销%'
            ORDER BY trade_date DESC, id DESC LIMIT ?
        """, (CURRENT_PERIOD, n)).fetchall()
        if len(recent) < n:
            return False
        triggered = all(r[0] < 0 for r in recent)
        if triggered and _is_paper_mode():
            logger.warning(f"[模拟盘风控] 最近{n}笔全亏, 仅记录不暂停买入")
            return False
        return triggered
    finally:
        conn.close()




def _check_monthly_drawdown(limit_pct: float = -0.05) -> bool:
    """月度回撤保护: 本月累计亏损>5% → 仓位减半

    策略A月度收益表(886笔):
      8月PF=0.55, 9月PF=0.70, 3月PF=0.80 — 亏损月
      如果能及时降仓, 单月最大亏从-4.52%降到约-2.5%
    """
    conn = _get_conn()
    try:
        today = date.today()
        month_start = today.replace(day=1).isoformat()
        row = conn.execute("""
            SELECT SUM(daily_pnl) FROM daemon_account
            WHERE period=? AND date >= ?
        """, (CURRENT_PERIOD, month_start)).fetchone()
        if not row or row[0] is None:
            return False
        monthly_pnl = row[0]
        triggered = monthly_pnl < INITIAL_CAPITAL * limit_pct
        if triggered and _is_paper_mode():
            logger.warning(f"[模拟盘风控] 本月累计亏损¥{monthly_pnl:+,.0f}, 仅记录不降仓/暂停")
            return False
        return triggered
    finally:
        conn.close()




def _market_crash_clear(positions: list, quotes: dict, market_sentiment: dict, result: dict):
    """大盘退潮分级卖出 — 不一刀切

    核心原则: 退潮时按持仓强弱分批处理, 盈利票给机会, 亏损票快走
    
    分级逻辑:
      涨跌比<15% (绝对冰点): 全清(真正的恐慌性抛售)
      涨跌比15-25% (退潮): 按盈亏分级处理
        - 大亏(pnl<-3%): 立即止损
        - 微亏(-3%<=pnl<0): 设紧止损-2%(回撤就出)
        - 已盈利(pnl>=0): 不卖,让利润奔跑(除非盘中翻绿)
      大盘跌幅>2%: 同上分级处理
    
    交易员视角:
      - 强势票退潮时抗跌, 后面反弹起来就是龙头
      - 盈利票在退潮时还赚钱=真正的强, 不该卖
      - 只有真正的冰点(<15%涨跌比)才全清
    
    数据验证(783笔策略A):
      涨跌比<30%: PF=0.63, 亏1.67%/笔
      涨停<50: PF=0.38-0.93
    """
    # 【开盘保护】开盘10分钟内API数据不稳定,涨跌比严重失真,禁止冰点清仓
    # 2026-05-22事故: 09:32 ulist返回61涨/2721跌(实际2161涨/2721跌→44%)
    # 开盘2分钟数据失真→假冰点→恐慌清仓建投能源+南威软件→亏损876元
    now_hm = datetime.now().hour * 100 + datetime.now().minute
    if now_hm < 940:
        return

    up_ratio = None
    up_c = market_sentiment.get("up_count", 0)
    down_c = market_sentiment.get("down_count", 0)
    if (up_c + down_c) > 0:
        up_ratio = up_c / (up_c + down_c)

    # 获取上证指数实时数据
    idx_quote = get_realtime(["000001"])
    idx_q = idx_quote.get("000001", {})
    idx_chg = idx_q.get("change_pct_calc", 0) or 0

    crash_reason = None
    is_extreme = False  # 绝对冰点标记
    if up_ratio is not None and up_ratio < 0.15:
        crash_reason = f"绝对冰点: 涨跌比{up_ratio:.0%}({up_c}涨{down_c}跌)"
        is_extreme = True
    elif up_ratio is not None and up_ratio < 0.25:
        crash_reason = f"退潮: 涨跌比{up_ratio:.0%}({up_c}涨{down_c}跌)"
    elif isinstance(idx_chg, (int, float)) and idx_chg < -2.0:
        crash_reason = f"大盘急跌: 上证{idx_chg:+.1f}%"

    if not crash_reason:
        return

    global _last_ebb_clear_time
    logger.warning(f"[退潮保护] {crash_reason} → 分级处理 (极端={is_extreme})")

    already_pending = {s["code"] for s in _read_pending_signals() if s["status"] == "pending"}
    sold_any = False

    for pos in positions:
        if pos["code"] in already_pending:
            continue
        # T+1铁律: 当天买入的不能卖
        buy_date = pos.get("buy_date", "")
        if buy_date == date.today().isoformat():
            continue

        q = quotes.get(pos["code"], {})
        price = q.get("price", 0)
        if price <= 0:
            continue
        
        # 计算当前盈亏
        buy_price = pos.get("buy_price", 0)
        pnl_pct = ((price / buy_price) - 1) * 100 if buy_price > 0 else 0

        # === 分级决策 ===
        should_sell = False
        sell_reason = ""
        
        if is_extreme:
            # 绝对冰点: 全清(恐慌性抛售, 保命第一)
            should_sell = True
            sell_reason = f"恐慌清仓: {crash_reason} 盈亏{pnl_pct:+.1f}%"
        elif pnl_pct < -3.0:
            # 大亏: 立即止损, 不犹豫
            should_sell = True
            sell_reason = f"退潮止损: {crash_reason} 亏{pnl_pct:.1f}%"
        elif pnl_pct < 0:
            # 微亏: 设紧止损-2%(再跌就出)
            day_chg = q.get("change_pct_calc", 0) or 0
            if day_chg < -2.0:
                should_sell = True
                sell_reason = f"退潮加速下跌: {crash_reason} 盘中跌{day_chg:.1f}%"
            # 否则: 暂时观察, 下轮扫描再评估
        else:
            # 已盈利: 不卖! 让利润奔跑
            # 除非盘中翻绿(从涨变跌且跌幅>1%)
            day_chg = q.get("change_pct_calc", 0) or 0
            if day_chg < -1.5:
                should_sell = True
                sell_reason = f"盈利回吐: {crash_reason} 盘中跌{day_chg:.1f}% 锁定利润{pnl_pct:+.1f}%"
            # 否则: 持有不动, 退潮里的强势票是宝贝
        
        if not should_sell:
            continue

        _add_signal(
            action="sell",
            code=pos["code"],
            name=pos["name"],
            price=price,
            reason=sell_reason,
            signal_type="退潮保护",
            urgent=True,
        )
        result["sells"].append({
            "code": pos["code"], "name": pos["name"],
            "price": price, "reason": sell_reason, "status": "预告中",
        })
        sold_any = True

    if sold_any:
        _last_ebb_clear_time = datetime.now()
        logger.warning(f"[退潮保护] 已卖出持仓, 启动{EBB_COOLDOWN_MINUTES}分钟买入冷却")


# ---------------------------------------------------------------------------
# 浮动亏损硬熔断 — 4级保护
# ---------------------------------------------------------------------------

CB_L1 = DAILY_LOSS_LIMIT * 0.5   # -900 (50%)
CB_L2 = DAILY_LOSS_LIMIT * 0.8   # -1440 (80%)
CB_L3 = DAILY_LOSS_LIMIT         # -1800 (100%)
CB_L4 = DAILY_LOSS_LIMIT * 1.5   # -2700 (150%)


def check_circuit_breaker(positions: list, quotes: dict, result: dict) -> str:
    """浮动亏损硬熔断 — 4级保护

    计算总浮动亏损(已实现+未实现), 触发对应等级的保护动作。
    熔断卖出通过信号系统执行, 自然跳过观察期(不经过_scan_sell)。

    L1警告: 总浮亏>-900(50%) → 停止买入+日志
    L2减仓: 总浮亏>-1440(80%) → 卖出亏损最严重的1只
    L3清仓: 总浮亏>-1800(100%) → 卖出所有亏损持仓(盈利保留)
    L4全清: 总浮亏>-2700(150%) → 全部清仓
    """
    if not positions:
        return ""

    # 非交易时间不触发卖出(仅计算日志)
    is_trading = _is_trading_time()

    from src.trader.daemon_db import get_account, _get_conn

    acct = get_account()

    # 今日已实现盈亏 = 今天所有已平仓交易的pnl之和
    # 注意: 不能用daily_pnl, 因为它已包含未实现盈亏, 会导致双重计算
    realized = 0.0
    try:
        _c = _get_conn()
        _r = _c.execute(
            "SELECT SUM(pnl) FROM daemon_positions "
            "WHERE status='closed' AND sell_date=? AND period=?",
            (date.today().isoformat(), CURRENT_PERIOD)
        ).fetchone()
        _c.close()
        if _r and _r[0]:
            realized = float(_r[0])
    except Exception:
        pass

    # 计算每只持仓的浮动盈亏
    pos_pnls = []
    total_unrealized = 0.0
    today = date.today().isoformat()
    for pos in positions:
        code = pos["code"]
        # T+1: 当天买入的不能卖
        buy_date = pos.get("buy_date", "")
        if buy_date == today:
            continue
        q = quotes.get(code, {})
        price = q.get("price", 0)
        if price <= 0:
            continue
        buy_price = pos.get("buy_price", 0)
        shares = pos.get("shares", 0)
        floating = (price - buy_price) * shares
        total_unrealized += floating
        pos_pnls.append({
            "code": code,
            "name": pos.get("name", ""),
            "price": price,
            "buy_price": buy_price,
            "shares": shares,
            "floating": floating,
            "pnl_pct": (price / buy_price - 1) * 100 if buy_price > 0 else 0,
        })

    total = realized + total_unrealized

    if total > CB_L1:
        return ""  # 未触发

    if _is_paper_mode():
        paper_limit = INITIAL_CAPITAL * PAPER_DAILY_LOSS_LIMIT_PCT
        level = "WARN" if total > paper_limit else "SEVERE"
        logger.warning(
            f"[模拟盘熔断{level}] 已实现¥{realized:+,.0f}+浮亏¥{total_unrealized:+,.0f}"
            f"=¥{total:+,.0f}, 仅记录不清仓/不暂停买入"
        )
        return ""

    # L4: 全部清仓
    if total <= CB_L4:
        pending_sells = {s["code"] for s in _read_pending_signals()
                         if s["action"] == "sell" and s.get("status") == "pending"}
        targets = [p for p in pos_pnls if p["code"] not in pending_sells]
        logger.warning(f"[熔断L4] 总浮亏¥{total:+,.0f}≤{CB_L4:.0f}(150%), "
                       f"清仓{len(targets)}只(跳过{len(pos_pnls)-len(targets)}只pending)")
        if is_trading:
            for p in targets:
                _add_signal("sell", p["code"], p["name"], p["price"],
                            f"熔断L4清仓: 总浮亏¥{total:+,.0f}(已实现¥{realized:+,.0f}+浮亏¥{total_unrealized:+,.0f})",
                            "熔断清仓", urgent=True)
                result["sells"].append({
                    "code": p["code"], "name": p["name"],
                    "price": p["price"], "reason": f"熔断L4清仓 总浮亏¥{total:+,.0f}",
                })
        _notify_cb_webhook("L4", f"全部清仓{len(targets)}只, 总浮亏¥{total:+,.0f}")
        return "L4"

    # L3: 卖出所有亏损持仓
    if total <= CB_L3:
        pending_sells = {s["code"] for s in _read_pending_signals()
                         if s["action"] == "sell" and s.get("status") == "pending"}
        losers = [p for p in pos_pnls if p["floating"] < 0 and p["code"] not in pending_sells]
        logger.warning(f"[熔断L3] 总浮亏¥{total:+,.0f}≤{CB_L3:.0f}(100%), "
                       f"卖出{len(losers)}只亏损(保留盈利{len(pos_pnls)-len(losers)}只)")
        if is_trading:
            for p in losers:
                _add_signal("sell", p["code"], p["name"], p["price"],
                            f"熔断L3: 总浮亏¥{total:+,.0f} 此票亏¥{p['floating']:+,.0f}({p['pnl_pct']:+.1f}%)",
                            "熔断清仓", urgent=True)
                result["sells"].append({
                    "code": p["code"], "name": p["name"],
                    "price": p["price"], "reason": f"熔断L3 亏¥{p['floating']:+,.0f}",
                })
        _notify_cb_webhook("L3", f"卖出{len(losers)}只亏损, 总浮亏¥{total:+,.0f}")
        return "L3"

    # L2: 卖出亏损最严重的1只
    if total <= CB_L2:
        pending_sells = {s["code"] for s in _read_pending_signals()
                         if s["action"] == "sell" and s.get("status") == "pending"}
        losers = [p for p in pos_pnls if p["floating"] < 0 and p["code"] not in pending_sells]
        if losers:
            worst = min(losers, key=lambda x: x["floating"])
            logger.warning(f"[熔断L2] 总浮亏¥{total:+,.0f}≤{CB_L2:.0f}(80%), "
                           f"减仓: {worst['code']} {worst['name']} 亏¥{worst['floating']:+,.0f}")
            if is_trading:
                _add_signal("sell", worst["code"], worst["name"], worst["price"],
                            f"熔断L2减仓: 总浮亏¥{total:+,.0f} 此票亏¥{worst['floating']:+,.0f}",
                            "熔断减仓", urgent=True)
                result["sells"].append({
                    "code": worst["code"], "name": worst["name"],
                    "price": worst["price"], "reason": f"熔断L2 亏¥{worst['floating']:+,.0f}",
                })
            _notify_cb_webhook("L2", f"减仓{worst['name']}({worst['code']}), 总浮亏¥{total:+,.0f}")
        else:
            logger.warning(f"[熔断L2] 总浮亏¥{total:+,.0f}≤{CB_L2:.0f}, 无亏损持仓可减, 停止买入")
        return "L2"

    # L1: 停止买入
    logger.warning(f"[熔断L1] 总浮亏¥{total:+,.0f}≤{CB_L1:.0f}(50%), 停止买入")
    _notify_cb_webhook("L1", f"停止买入, 总浮亏¥{total:+,.0f}")
    return "L1"


def check_c_consecutive_stops(n: int = 5) -> None:
    """策略C连续止损监控 — 连续N笔止损打WARNING建议复盘

    策略C持仓期从20天缩到5天(外部验证: 实践3-5天/学术20天),
    如果实盘连续止损, 可能说明5天太短或过滤条件需调整。
    """
    conn = _get_conn()
    try:
        rows = conn.execute("""
            SELECT pnl, reason FROM daemon_trades
            WHERE action='sell' AND period=?
              AND (signal_type LIKE '%趋势牛股%' OR signal_type LIKE '%策略C%')
            ORDER BY trade_date DESC, id DESC LIMIT ?
        """, (CURRENT_PERIOD, n)).fetchall()
        if len(rows) >= n and all(r[0] < 0 for r in rows):
            avg_loss = sum(r[0] for r in rows) / len(rows)
            logger.warning(
                f"[策略C监控] 连续{len(rows)}笔止损! "
                f"均亏¥{avg_loss:+,.0f}, 建议复盘持仓期(当前hold=5天)"
            )
            try:
                from src.trader.notification_webhook import notify_anomaly
                notify_anomaly(f"策略C连续{len(rows)}笔止损, 均亏¥{avg_loss:+,.0f}")
            except Exception:
                pass
    except Exception as e:
        logger.debug(f"策略C止损监控异常: {e}")
    finally:
        conn.close()


# 周级回撤限制 — 外部验证建议: 仅日级熔断不够, 连续多日小亏可累积成大亏
WEEKLY_LOSS_LIMIT = INITIAL_CAPITAL * 0.05  # -4500元(9万的5%)


def check_weekly_drawdown() -> bool:
    """周级回撤限制 — 本周累计亏损>5%停止买入

    只影响买入, 不影响卖出(止损/止盈仍正常执行)。
    外部验证: 专业量化基金普遍建议加周级5-10%回撤限制。
    周一重置(按自然周计算)。
    """
    conn = _get_conn()
    try:
        today = date.today()
        monday = today - timedelta(days=today.weekday())
        row = conn.execute("""
            SELECT SUM(pnl) FROM daemon_trades
            WHERE action='sell' AND period=?
              AND trade_date >= ?
              AND reason NOT LIKE '%作废%' AND reason NOT LIKE '%撤销%'
        """, (CURRENT_PERIOD, monday.isoformat())).fetchone()
        if not row or row[0] is None:
            return False
        weekly_loss = row[0]
        limit = INITIAL_CAPITAL * PAPER_WEEKLY_LOSS_LIMIT_PCT if _is_paper_mode() else -abs(WEEKLY_LOSS_LIMIT)
        if weekly_loss < limit:
            logger.warning(f"[周级风控] 本周累计亏损¥{weekly_loss:+,.0f} "
                          f"超限¥{abs(limit):,.0f}, "
                          f"{'模拟盘仅记录不停止买入' if _is_paper_mode() else '停止买入至下周一'}")
            if _is_paper_mode():
                return False
            return True
        return False
    except Exception as e:
        logger.warning(f"周级回撤检查异常: {e}")
        return False
    finally:
        conn.close()


def _notify_cb_webhook(level: str, detail: str):
    """熔断webhook通知"""
    try:
        from src.trader.notification_webhook import notify_circuit_breaker
        notify_circuit_breaker(f"熔断{level}: {detail}")
    except Exception:
        pass
