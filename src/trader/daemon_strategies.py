"""daemon_strategies.py — 策略候选生成与盘中回踩检测

从 trading_daemon.py 拆分出的策略相关函数。
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, date, timedelta
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.trader.daemon_config import (
    DB_PATH, CURRENT_PERIOD, SIGNAL_DIR,
    STRATEGY_C_CONFIG, STRATEGY_B_CONFIG,
    B_ENABLED, B_INITIAL_CAPITAL, B_MAX_POSITIONS, B_POSITION_RATIO,
    A_INITIAL_CAPITAL, A_MAX_POSITIONS,
    BREAKOUT_MIN_CHG, BREAKOUT_MAX_CHG, BREAKOUT_VOL_RATIO,
    PULLBACK_MA_DIST, PULLBACK_VOL_RATIO,
)
from src.trader.daemon_db import _get_conn, _log_to_db, get_account, get_held_positions
from src.trader.daemon_signals import _add_signal, _read_pending_signals
from src.trader.realtime_quote import get_realtime
from src.trader.daemon_risk import _check_industry_concentration

logger = logging.getLogger("trading_daemon")


def _try_upgrade_positions(held: list, candidates: list, market_sentiment: dict):
    """满仓换仓评估 — 数学决策

    条件(必须同时满足):
      1. 当前持仓中有亏损的(浮亏>0.2%, 覆盖双边成本0.125%)
      2. 新候选质量分 > 亏损持仓的质量分
      3. 市场环境允许(can_buy=True)
      4. 同策略内替换(不跨策略, 因为资金隔离)

    实际交易成本:
      买入手续费: 万2.5 = 0.025%
      卖出手续费: 万2.5 = 0.025%
      卖出印花税: 万5 = 0.05%
      换仓总成本: 0.125% (卖旧+买新)
    """
    if not market_sentiment.get("can_buy", True):
        return

    # 按策略分组当前持仓
    held_by_strategy = {"A": [], "B": [], "C": []}
    for h in held:
        sig_type = h.get("signal_type", "")
        if "首阴" in sig_type:
            held_by_strategy["A"].append(h)
        elif "回踩低吸" in sig_type or "低开反弹" in sig_type or "暴跌日狙击" in sig_type:
            held_by_strategy["B"].append(h)
        else:
            held_by_strategy["C"].append(h)

    # 按策略分组新候选
    new_by_strategy = {"A": [], "B": [], "C": []}
    for c in candidates:
        strat = c.get("_strategy", "C")
        new_by_strategy[strat].append(c)

    # 逐策略检查
    for strat in ["A", "B"]:
        held_list = held_by_strategy[strat]
        new_list = new_by_strategy[strat]
        if not held_list or not new_list:
            continue

        # 找亏损最严重的持仓
        worst_held = None
        worst_pnl = 0
        for h in held_list:
            buy_p = h.get("buy_price", 0)
            cur_p = h.get("current_price", 0) or h.get("realtime_price", 0)
            if buy_p > 0 and cur_p > 0:
                pnl = (cur_p - buy_p) / buy_p * 100
                if pnl < worst_pnl:
                    worst_pnl = pnl
                    worst_held = h

        # 亏损>0.2%才考虑换(覆盖双边交易成本0.125%+安全边际)
        if worst_pnl > -0.2 or not worst_held:
            continue

        # 换仓条件: 新候选跌幅更大(有效因子) 且 当前持仓亏损
        # 有效因子验证: 跌幅5-7% PF=1.82, 跌幅越大收益越好(区分度1.96)
        new_drop = new_list[0].get("_yin_drop_pct", 0)
        held_drop = worst_held.get("_yin_drop_pct", 0)

        if new_drop <= held_drop:
            continue

        # 换仓条件满足 → 卖出亏损持仓
        code = worst_held["code"]
        name = worst_held["name"]
        reason = f"换仓: 亏{worst_pnl:.1f}%, 新候选跌{new_drop}%>持仓{held_drop}%"
        _add_signal("sell", code, name, worst_held.get("current_price", 0),
                    reason, worst_held.get("signal_type", ""), urgent=True)
        logger.info(f"[换仓] 卖出{code} {name} {reason}")


# 策略B v2: 暴跌日好公司狙击候选缓存(日内缓存, 当天有效)
_b_crash_cache = {"data": [], "date": None, "ts": 0}


def _validate_b_crash_candidate(day_ret, roe) -> str | None:
    """策略B最终数据校验.

    API缺失时常见默认值是0, 这里必须 fail closed, 不能把0当成真实跌幅/ROE。
    返回None表示通过, 返回字符串表示拒绝原因。
    """
    crash_stock_drop_pct = STRATEGY_B_CONFIG.get("crash_stock_drop", -0.05) * 100
    roe_min = STRATEGY_B_CONFIG.get("roe_min", 10)

    try:
        day_ret_f = float(day_ret)
    except (TypeError, ValueError):
        return f"日跌幅无效({day_ret!r})"
    try:
        roe_f = float(roe)
    except (TypeError, ValueError):
        return f"ROE无效({roe!r})"

    if day_ret_f == 0:
        return "日跌幅=0, 疑似行情/API缺失"
    if roe_f == 0:
        return "ROE=0, 疑似财务/API缺失"
    if day_ret_f > crash_stock_drop_pct:
        return f"日跌幅{day_ret_f:+.1f}%未达到{crash_stock_drop_pct:.1f}%"
    if roe_f < roe_min:
        return f"ROE={roe_f:.1f}%<{roe_min}%"
    return None


def _mark_b_candidate_validated(candidate: dict) -> bool:
    """Apply the canonical B data check and mark valid candidates."""
    candidate["_data_validated"] = (
        _validate_b_crash_candidate(
            candidate.get("_day_ret"),
            candidate.get("_roe"),
        )
        is None
    )
    return candidate["_data_validated"]


def _is_crash_day() -> bool:
    """判断今天是否是暴跌日(全市场均跌>2%)

    优先用daily_price(收盘后), 盘中无数据时回退到上证指数实时行情。
    """
    threshold = STRATEGY_B_CONFIG.get("crash_market_threshold", -0.02)

    # 1. 收盘后: daily_price有完整数据
    conn = _get_conn()
    try:
        today = datetime.now().strftime("%Y-%m-%d")
        row = conn.execute("""
            SELECT AVG((close - pre_close) / pre_close) as avg_ret, COUNT(*) as cnt
            FROM daily_price
            WHERE trade_date = ? AND pre_close > 0 AND close IS NOT NULL
        """, (today,)).fetchone()
        if row and row[1] > 1000:
            return row[0] < threshold
    except Exception as e:
        logger.debug(f"暴跌日DB查询异常: {e}")
    finally:
        conn.close()

    # 2. 盘中: daily_price无当天数据, 用上证指数实时跌幅
    try:
        import subprocess as _sp
        url = "http://qt.gtimg.cn/q=sh000001"
        r = _sp.run(
            ["/mnt/c/Windows/System32/curl.exe", "-s", "--max-time", "10", url],
            capture_output=True, timeout=15,
        )
        raw = r.stdout.decode("gbk", errors="replace")
        for line in raw.strip().split(";"):
            line = line.strip()
            if not line or "=" not in line:
                continue
            _, _, val = line.partition("=")
            val = val.strip('"').strip()
            if not val:
                continue
            parts = val.split("~")
            if len(parts) > 32 and parts[32]:
                idx_chg = float(parts[32])
                is_crash = idx_chg < threshold * 100
                logger.debug(f"[暴跌日] 实时判断: 上证涨跌{idx_chg:.2f}%, "
                             f"{'暴跌!' if is_crash else '非暴跌'}")
                return is_crash
    except Exception as e:
        logger.debug(f"暴跌日实时查询异常: {e}")

    return False


def _get_b_watchlist() -> list[dict]:
    """策略B v2: 暴跌日好公司狙击候选列表

    只在暴跌日(全市场均跌>2%)产生候选, 非暴跌日返回空列表。
    盘中模式: 基本面预筛 + 实时行情跌幅过滤(不依赖当天daily_price)。
    盘后模式: daily_price当天数据查询(原逻辑)。
    """
    import time as _time

    if not B_ENABLED:
        return []

    today = datetime.now().strftime("%Y-%m-%d")
    now = _time.time()

    # 缓存: 同一天内5分钟刷新一次
    if _b_crash_cache["date"] == today and now - _b_crash_cache.get("ts", 0) < 300:
        return _b_crash_cache["data"]

    # 暴跌日判断
    if not _is_crash_day():
        _b_crash_cache["data"] = []
        _b_crash_cache["date"] = today
        _b_crash_cache["ts"] = now
        logger.debug("[策略B] 非暴跌日，无候选")
        return []

    # 暴跌日! 检测数据源: daily_price有当天数据→盘后, 否则→盘中实时
    conn = _get_conn()
    try:
        cnt = conn.execute(
            "SELECT COUNT(*) FROM daily_price WHERE trade_date = ?", (today,)
        ).fetchone()[0]
    finally:
        conn.close()

    if cnt > 1000:
        candidates = _get_b_watchlist_db(today)
    else:
        candidates = _get_b_watchlist_realtime(today)

    if candidates:
        logger.info(f"[暴跌日狙击] {today} 暴跌日! 发现{len(candidates)}只候选 "
                    f"(跌幅: {candidates[0]['_day_ret']:.1f}%~{candidates[-1]['_day_ret']:.1f}%)")
    else:
        logger.info(f"[暴跌日狙击] {today} 暴跌日! 但无符合条件的候选")

    _b_crash_cache["data"] = candidates
    _b_crash_cache["date"] = today
    _b_crash_cache["ts"] = now
    return candidates


def _get_b_watchlist_db(today: str) -> list[dict]:
    """盘后模式: daily_price有完整当天数据, 用原逻辑查询"""
    conn = _get_conn()
    try:
        crash_stock_drop = STRATEGY_B_CONFIG.get("crash_stock_drop", -0.05)
        roe_min = STRATEGY_B_CONFIG.get("roe_min", 10)
        max_pos = STRATEGY_B_CONFIG.get("max_positions", 3)

        rows = conn.execute("""
            SELECT dp.stock_code, dp.close, dp.pre_close,
                   (dp.close - dp.pre_close) / dp.pre_close as day_ret,
                   fs.roe, dp.trade_date
            FROM daily_price dp
            LEFT JOIN (
                SELECT stock_code, roe FROM financial_summary
                WHERE (stock_code, report_date) IN (
                    SELECT stock_code, MAX(report_date) FROM financial_summary GROUP BY stock_code
                )
            ) fs ON dp.stock_code = fs.stock_code
            WHERE dp.trade_date = ?
              AND dp.pre_close > 0 AND dp.close IS NOT NULL
              AND (dp.close - dp.pre_close) / dp.pre_close <= ?
              AND fs.roe IS NOT NULL AND fs.roe >= ?
              AND dp.stock_code NOT LIKE '688%%'
              AND NOT (LENGTH(dp.stock_code) = 6 AND (dp.stock_code LIKE '8%%' OR dp.stock_code LIKE '4%%'))
            ORDER BY dp.amount ASC
            LIMIT ?
        """, (today, crash_stock_drop, roe_min, max_pos)).fetchall()

        mkt_row = conn.execute(
            "SELECT AVG((close - pre_close) / pre_close) FROM daily_price "
            "WHERE trade_date = ? AND pre_close > 0 AND close IS NOT NULL",
            (today,),
        ).fetchone()
        crash_market_ret = (
            mkt_row[0] * 100
            if mkt_row and mkt_row[0] is not None
            else None
        )

        candidates = []
        for r in rows:
            code = r[0]
            day_ret_pct = r[3] * 100
            roe = r[4]
            candidate = {
                "code": code,
                "name": "",
                "_strategy": "B",
                "_strategy_version": "B_crash_v2",
                "_day_ret": day_ret_pct if day_ret_pct != 0 else None,
                "_roe": roe if roe != 0 else None,
                "_crash_market_ret": crash_market_ret,
                "_zt_date": today,
                "_signal_type": "暴跌日狙击",
                "_data_validated": False,
                "_source": "db_crash_scan",
            }
            if _mark_b_candidate_validated(candidate):
                candidates.append(candidate)
        return candidates

    except Exception as e:
        logger.warning(f"策略B候选收集(DB模式)异常: {e}")
        return []
    finally:
        conn.close()


def _get_b_watchlist_realtime(today: str) -> list[dict]:
    """盘中模式: 基本面预筛 + 实时行情跌幅过滤

    1. 从financial_summary预筛ROE>10%/非科创北交的候选池
    2. 批量获取实时行情, 筛选跌幅>5%的个股
    3. 按成交额升序(小市值优先), 回测验证: 市值最小PF=3.24 vs 跌幅最大PF=0.81
    """
    crash_stock_drop = STRATEGY_B_CONFIG.get("crash_stock_drop", -0.05)
    roe_min = STRATEGY_B_CONFIG.get("roe_min", 10)
    max_pos = STRATEGY_B_CONFIG.get("max_positions", 3)

    # Step 1: 基本面预筛(不依赖当天daily_price)
    conn = _get_conn()
    try:
        rows = conn.execute("""
            SELECT fs.stock_code, fs.roe
            FROM financial_summary fs
            WHERE (fs.stock_code, fs.report_date) IN (
                SELECT stock_code, MAX(report_date) FROM financial_summary GROUP BY stock_code
            )
            AND fs.roe IS NOT NULL AND fs.roe >= ?
            AND fs.stock_code NOT LIKE '688%%'
            AND NOT (LENGTH(fs.stock_code) = 6 AND (fs.stock_code LIKE '8%%' OR fs.stock_code LIKE '4%%'))
        """, (roe_min,)).fetchall()
    except Exception as e:
        logger.warning(f"策略B基本面预筛异常: {e}")
        return []
    finally:
        conn.close()

    if not rows:
        logger.info("[策略B] 基本面预筛无结果")
        return []

    code_to_roe = {r[0]: r[1] for r in rows}
    codes = list(code_to_roe.keys())
    logger.debug(f"[策略B] 基本面预筛{len(codes)}只, 获取实时行情...")

    # Step 2: 批量获取实时行情(分块, 防止腾讯API URL过长)
    candidates = []
    CHUNK = 200
    for i in range(0, len(codes), CHUNK):
        chunk = codes[i:i + CHUNK]
        quotes = get_realtime(chunk)
        if not quotes:
            continue
        for code in chunk:
            q = quotes.get(code, {})
            if not q or "error" in q:
                continue
            chg = q.get("change_pct_calc", 0) or 0
            if chg > crash_stock_drop * 100:
                continue
            candidates.append({
                "code": code,
                "name": q.get("name", ""),
                "_strategy": "B",
                "_strategy_version": "B_crash_v2",
                "_day_ret": chg if chg != 0 else None,
                "_roe": code_to_roe.get(code) or None,
                "_crash_market_ret": None,  # 盘中无全市场均值, 由_is_crash_day()门控
                "_zt_date": today,
                "_signal_type": "暴跌日狙击",
                "_data_validated": False,  # 待硬过滤后置True
                "_source": "realtime_crash_scan",
                "_amount_wan": q.get("amount_wan", 0),
            })

    # Step 2.5: 流动性过滤 — 日均成交额>500万(最近5个交易日均值)
    # 外部验证: 聚宽社区警告小市值策略在狭窄市值区间容易过拟合, 流动性幻觉导致回测高估
    if candidates:
        cand_codes = [c["code"] for c in candidates]
        placeholders = ",".join("?" * len(cand_codes))
        conn = _get_conn()
        try:
            rows = conn.execute(f"""
                SELECT stock_code, AVG(amount) as avg_amount
                FROM daily_price
                WHERE trade_date IN (
                    SELECT DISTINCT trade_date FROM daily_price
                    WHERE trade_date < ?
                    ORDER BY trade_date DESC LIMIT 5
                )
                AND stock_code IN ({placeholders})
                GROUP BY stock_code
            """, (today, *cand_codes)).fetchall()
            avg_amount_map = {r[0]: r[1] for r in rows}
        except Exception as e:
            logger.warning(f"[策略B] 流动性查询异常: {e}, 跳过过滤")
            avg_amount_map = {}
        finally:
            conn.close()

        MIN_AVG_AMOUNT = 5_000_000  # 日均成交额>500万(单位:元)
        before = len(candidates)
        candidates = [c for c in candidates
                      if avg_amount_map.get(c["code"], 0) >= MIN_AVG_AMOUNT]
        if before - len(candidates) > 0:
            logger.info(f"[策略B] 流动性过滤: {before}→{len(candidates)}只 "
                        f"(剔除{before - len(candidates)}只日均成交<500万)")

    # Step 2.7: 硬过滤 — 数据铁律(数据源不可信时必须设卡)
    if candidates:
        before = len(candidates)
        candidates = [c for c in candidates if _mark_b_candidate_validated(c)]
        if before - len(candidates) > 0:
            logger.info(f"[策略B] 硬过滤: {before}→{len(candidates)}只 "
                        f"(剔除{before - len(candidates)}只数据缺失或不符合条件)")

    # Step 3: 按成交额升序(小市值优先), 回测验证: 市值最小PF=3.24 vs 跌幅最大PF=0.81
    candidates.sort(key=lambda x: x.get("_amount_wan", 0))
    return candidates[:max_pos]


def _check_b_pullback_realtime(result: dict):
    """策略B v2: 暴跌日好公司狙击 — 盘中实时买入

    只在暴跌日触发, 选跌>5%+ROE>10%的好公司。
    额外门控:
      - 连续暴跌过滤: 前一交易日大盘跌>2%时不做(6/6亏损日全是连续下跌)
      - 退潮/冰点完全关闭: 涨跌比<40%时不买入(5/9亏损因退潮被动卖出)
    非暴跌日不产生任何买入信号。
    """
    if not B_ENABLED:
        return

    today = datetime.now().strftime("%Y-%m-%d")

    # ── 连续暴跌过滤(P0-1): 前一交易日大盘跌>2%不做 ──
    # 过拟合分析: 6/6亏损日全是"连续下跌", 前日跌>2%+当日跌 = 接飞刀
    conn = _get_conn()
    try:
        prev_row = conn.execute("""
            SELECT trade_date, AVG((close - pre_close) / pre_close) as avg_ret
            FROM daily_price
            WHERE trade_date < ? AND pre_close > 0 AND close IS NOT NULL
            GROUP BY trade_date
            ORDER BY trade_date DESC LIMIT 1
        """, (today,)).fetchone()
        if prev_row and prev_row[1] < -0.02:
            logger.info(f"[暴跌日狙击] 连续暴跌过滤: 前日{prev_row[0]}跌{prev_row[1]*100:.1f}%>2%, 不做策略B")
            return
    except Exception as e:
        logger.debug(f"[连续暴跌过滤] 查询异常: {e}")
    finally:
        conn.close()

    # ── 退潮/冰点完全关闭(P0-2): 涨跌比<40%不做策略B ──
    # 过拟合分析: 5/9亏损因退潮/冰点被动卖出, 当前daily_price盘中无数据导致检查失效
    # 修复: 用实时情绪数据(daily_price无数据时回退到akshare实时)
    try:
        from src.strategy.strategy_b import get_market_emotion
        emo = get_market_emotion()
        phase = emo.get("phase", "未知")
        up_ratio = emo.get("up_ratio")
        if phase in ("退潮", "冰点", "退潮预警"):
            logger.info(f"[暴跌日狙击] {phase}期完全关闭: 涨跌比{up_ratio:.0%}({phase}), 不做策略B")
            return
        # 数据不足且非正常→保守关闭
        if phase in ("偏弱", "未知") and up_ratio is not None and up_ratio < 0.40:
            logger.info(f"[暴跌日狙击] {phase}涨跌比{up_ratio:.0%}(<40%), 不做策略B")
            return
    except Exception as e:
        logger.debug(f"[退潮检查] 实时情绪异常: {e}, 用daily_price兜底")
        # 兜底: daily_price查询
        conn = _get_conn()
        try:
            row = conn.execute("""
                SELECT SUM(CASE WHEN (close-pre_close)/pre_close > 0 THEN 1 ELSE 0 END) as up,
                       SUM(CASE WHEN (close-pre_close)/pre_close < 0 THEN 1 ELSE 0 END) as down
                FROM daily_price
                WHERE trade_date = ? AND pre_close > 0 AND close IS NOT NULL
            """, (today,)).fetchone()
            if row and row[0] is not None and (row[0] + row[1]) > 0:
                up_ratio = row[0] / (row[0] + row[1])
                if up_ratio < 0.40:
                    logger.info(f"[暴跌日狙击] 兜底涨跌比{up_ratio:.0%}(<40%), 不做策略B")
                    return
        except Exception:
            pass
        finally:
            conn.close()

    watchlist = _get_b_watchlist()
    if not watchlist:
        return

    # 策略B持仓检查(兼容旧信号)
    held = get_held_positions()
    held_codes = {h["code"] for h in held}
    b_held = sum(1 for h in held if any(
        kw in h.get("signal_type", "")
        for kw in ("回踩低吸", "低开反弹", "暴跌日狙击")))
    if b_held >= B_MAX_POSITIONS:
        return

    # 批量获取实时行情
    codes = [w["code"] for w in watchlist if w["code"] not in held_codes]
    if not codes:
        return
    quotes = get_realtime(codes)
    if not quotes:
        return

    for w in watchlist:
        code = w["code"]
        if code in held_codes:
            continue

        q = quotes.get(code, {})
        if not q or "error" in q:
            continue
        current_price = q.get("price", 0)
        if current_price <= 0:
            continue

        chg = q.get("change_pct_calc", 0) or 0
        if chg >= 9.5:
            continue

        # 检查是否已有预告
        pending = _read_pending_signals()
        dup_key = f"buy_{code}"
        if any(f"{s['action']}_{s['code']}" == dup_key and s.get("status") == "pending"
               for s in pending):
            continue

        # 行业集中度检查
        if not _check_industry_concentration(code):
            result["skipped"].append({"code": code, "name": q.get("name", ""), "reason": "同行业集中度超限"})
            continue

        # ★ 暴跌日好公司! 买入预告
        day_ret = w.get("_day_ret")
        roe = w.get("_roe")
        reject_reason = _validate_b_crash_candidate(day_ret, roe)
        if reject_reason:
            logger.info(f"[策略B] {code} 跳过: {reject_reason}")
            continue
        buy_reason = (f"暴跌日狙击: {w.get('_zt_date','')}跌{day_ret:.1f}% "
                      f"ROE={roe:.1f}% 现¥{current_price:.2f}")
        _add_signal(
            action="buy",
            code=code,
            name=q.get("name", w.get("name", "")),
            price=current_price,
            reason=buy_reason,
            signal_type="暴跌日狙击(策略B)",
            extra={
                "crash_date": w.get("_zt_date", ""),
                "crash_day_ret": day_ret,
                "roe": roe,
            },
            delay_sec=30,
        )
        logger.info(f"[暴跌日狙击] {code} {q.get('name','')} ¥{current_price:.2f} "
                    f"暴跌日跌{day_ret:.1f}% ROE={roe:.1f}% → ★ 买入!")
        result["buys"].append({
            "code": code, "name": q.get("name", ""),
            "price": current_price, "reason": f"暴跌日狙击 跌{day_ret:.1f}%",
        })
        break  # 每轮最多买1只


def _check_c_trend_realtime(result: dict):
    """策略C盘中实时趋势确认检测

    核心逻辑:
      1. 获取趋势牛股监控列表(均线多头排列+金叉)
      2. 批量获取实时行情
      3. 当前价突破昨天高点 → 立刻买入预告(30秒执行)

    回测: PF=1.25 / 161笔 / 扣成本+1.11%/笔
    """
    from src.trader.daemon_config import C_MAX_POSITIONS

    watchlist = _get_c_watchlist_cached()
    if not watchlist:
        return

    # 策略C持仓检查
    held = get_held_positions()
    held_codes = {h["code"] for h in held}
    d_held = sum(1 for h in held if "趋势牛股" in h.get("signal_type", ""))
    if d_held >= C_MAX_POSITIONS:
        return  # 策略C满仓

    # 批量获取实时行情
    codes = [w["code"] for w in watchlist if w["code"] not in held_codes]
    if not codes:
        return
    quotes = get_realtime(codes)
    if not quotes:
        return

    for w in watchlist:
        code = w["code"]
        if code in held_codes:
            continue

        # P0-4修复: tier=watch的票量比<5, 不应触发买入
        tier = w.get("_tier", "")
        if tier == "watch":
            logger.debug(f"[趋势牛股] {code} tier=watch(量比<5), 跳过买入")
            continue

        # 精选限制: 排序前5
        w_idx = watchlist.index(w)
        if w_idx >= 5:
            logger.debug(f"[趋势牛股] {code} 排序第{w_idx+1}, 超过精选前5, 跳过")
            continue

        breakout_target = w.get("_buy_target", 0)
        if breakout_target <= 0:
            continue

        q = quotes.get(code, {})
        if not q or "error" in q:
            continue
        current_price = q.get("price", 0)
        if current_price <= 0:
            continue

        # 突破检测: 当前价 > 昨天最高价
        if current_price <= breakout_target:
            continue

        chg = q.get("change_pct_calc", 0) or 0

        # 不追涨停(>=9.5%)
        if chg >= 9.5:
            continue
        # 不买跌停(<=-9.5%)
        if chg <= -9.5:
            logger.debug(f"[趋势牛股] {code} 跌停{chg:.1f}%, 跳过")
            continue

        # 精选评分卡
        from src.trader.selection_score import score_candidate
        w["realtime_price"] = current_price
        w["realtime_chg"] = chg
        w["name"] = w.get("name") or q.get("name", "")
        sc = score_candidate(w, strategy="C")
        if sc.get("veto") or sc["score"] < 60:
            logger.debug(f"[趋势牛股] {code} 评分{sc['score']:.0f}{(' veto:'+sc['veto']) if sc.get('veto') else ''}, <60分跳过")
            continue

        # 策略C仓位检查(实时)
        d_held_now = sum(1 for h in get_held_positions()
                        if "趋势牛股" in h.get("signal_type", ""))
        if d_held_now >= C_MAX_POSITIONS:
            return

        # 检查是否已有预告
        pending = _read_pending_signals()
        dup_key = f"buy_{code}"
        if any(f"{s['action']}_{s['code']}" == dup_key and s.get("status") == "pending" for s in pending):
            continue

        # 行业集中度检查
        if not _check_industry_concentration(code):
            result["skipped"].append({"code": code, "name": q.get("name", ""), "reason": "同行业集中度超限"})
            continue

        # 突破确认! 买入预告
        dist_pct = (current_price / breakout_target - 1) * 100
        _add_signal(
            action="buy",
            code=code,
            name=q.get("name", w.get("name", "")),
            price=current_price,
            reason=f"趋势牛股突破: ¥{current_price:.2f}突破{breakout_target:.2f}+{dist_pct:.1f}%",
            signal_type="趋势牛股(策略C)",
            extra={
                "golden": w.get("_golden", ""),
                "ma60_pct": w.get("_ma60_pct", 0),
                "atr": w.get("_atr", 0),
                "breakout_target": breakout_target,
                "dist_pct": round(dist_pct, 2),
            },
            delay_sec=30,
        )
        logger.info(f"[趋势牛股] {code} {q.get('name','')} ¥{current_price:.2f} "
                    f"突破{breakout_target:.2f}+{dist_pct:.1f}% → ★ 趋势牛股买入!")
        result["buys"].append({
            "code": code, "name": q.get("name", ""),
            "price": current_price, "reason": f"趋势牛股突破+{dist_pct:.1f}%",
        })
        break  # 每轮最多买1只


# 策略C watchlist缓存 — 避免每15秒查DB
_c_watchlist_cache = {"data": [], "ts": 0}

def _get_c_watchlist_cached() -> list[dict]:
    """策略C watchlist缓存, 60秒刷新一次"""
    import time
    now = time.time()
    if now - _c_watchlist_cache["ts"] > 60:
        from src.strategy.strategy_c import get_strategy_c_watchlist
        _c_watchlist_cache["data"] = get_strategy_c_watchlist()
        _c_watchlist_cache["ts"] = now
    return _c_watchlist_cache["data"]
