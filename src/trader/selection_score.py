"""selection_score.py — 精选评分卡(IC/ICIR驱动 + 经验fallback)

策略A/B/C候选精选评分, 在daemon买入前调用。
每只候选打分(0-100), 低于阈值直接排除。

权重来源:
  优先IC驱动(每周末更新), 冷启动期用经验权重fallback。
  通过 src.trader.factor_weights.get_weights() 获取。

集成点:
  - trading_daemon._filter_candidates_realtime() 最后加 _score_filter()
  - daemon_strategies.check_realtime_pullback() 回踩前加 _score_filter()

调用: from src.trader.selection_score import score_candidate, filter_by_score
"""
from datetime import datetime, timedelta
from src.trader.daemon_db import _get_conn


def _sentiment_adj(code: str) -> tuple[int, str]:
    """新闻情感加减分(+3/-3/0). 无新闻返回0."""
    try:
        conn = _get_conn()
        since = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        rows = conn.execute(
            "SELECT sentiment_score, title FROM news "
            "WHERE stock_code = ? AND publish_time >= ? AND sentiment_score IS NOT NULL",
            (code, since),
        ).fetchall()
        conn.close()
        if not rows:
            return 0, ""

        scores = [r[0] for r in rows if r[0] is not None]
        if not scores:
            return 0, ""

        avg = sum(scores) / len(scores)
        bullish = sum(1 for s in scores if s > 0.1)
        bearish = sum(1 for s in scores if s < -0.1)

        if avg >= 0.3 and bullish >= 2:
            return 3, f"情感看多({avg:+.2f},{bullish}条正面/{len(scores)}条)"
        elif avg <= -0.3 and bearish >= 2:
            return -3, f"情感偏空({avg:+.2f},{bearish}条负面/{len(scores)}条)"
        return 0, ""
    except Exception:
        return 0, ""


def _perception_adj(c: dict) -> tuple[int, str]:
    """个股感知涨因加减分(基于perceive_stock的LLM涨因分析)."""
    perception = c.get("_perception")
    if not perception:
        return 0, ""
    logic = perception.get("logic", "")
    confidence = perception.get("logic_confidence", 0)
    if not logic or confidence < 0.5:
        return 0, ""
    if "资金" in logic:
        return 2, f"资金驱动({logic},conf={confidence:.1f})"
    if "概念" in logic:
        return -1, f"概念炒作({logic},conf={confidence:.1f})"
    return 0, ""
    """获取动态权重(IC驱动优先, fallback经验权重)"""
    try:
        from src.trader.factor_weights import get_weights
        return get_weights(strategy)
    except Exception:
        return {}  # fallback到函数内硬编码


def score_candidate(candidate: dict, strategy: str = None) -> dict:
    """给候选打分(0-100)

    Args:
        candidate: 必须含 code, realtime_price, realtime_chg
                   策略B额外需要: _zt_open(涨停开盘价), _zt_date(涨停日期)
                   策略A额外需要: consecutive_zt(连板数), first_drop_pct(首阴跌幅)
                   策略C额外需要: ma5/ma20/ma60, vol_ratio, rsi, macd
        strategy: "A" 或 "B" 或 "C" (不传则从candidate._strategy读取)

    Returns:
        {"score": 0-100, "details": {...}, "veto": None|str}
    """
    if strategy is None:
        strategy = candidate.get("_strategy", "B")
    code = candidate.get("code", "")
    price = candidate.get("realtime_price", 0)
    
    # ── 一票否决(通用) ──
    name = candidate.get("name", "")
    if "ST" in name.upper():
        return {"score": 0, "details": {"veto": "ST股"}, "veto": "ST股"}
    
    # 科创板(688)/北交所(8开头4位) - 20%涨跌幅不适用
    if code.startswith("688") or (code.startswith("8") and len(code) == 6 and code[1:4].isdigit()):
        return {"score": 0, "details": {"veto": "科创板/北交所"}, "veto": "科创板/北交所"}
    
    # 成交额<2亿(用候选自带的amount_wan或查DB)
    amt = candidate.get("amount_wan", 0) or 0
    if amt < 20000:
        if not _check_amount(code):
            return {"score": 0, "details": {"veto": f"成交额不足({amt:.0f}万<2亿)"}, "veto": "成交额不足"}
    
    if strategy == "A":
        return _score_strategy_a(candidate)
    elif strategy == "C":
        return _score_strategy_c(candidate)
    else:
        return _score_strategy_b(candidate)


def _score_strategy_a(c: dict) -> dict:
    """策略A(龙头首阴反包)评分: 龙头评分 + 龙虎榜席位加分/减分

    strategy_a.py已计算_dragon_total(连板30+梯队20+封板20+市值15+形态15=满分100)
    这里做归一化 + 龙虎榜席位融合 + 一票否决
    """
    details = {}

    # 直接用strategy_a的龙头评分
    dragon = c.get("_dragon_total", 0)
    tier = c.get("_tier", "weak")
    body_pct = c.get("_yin_body_pct", 99)

    # 一票否决: 实体跌幅>5%(抛压太重) 或 weak档
    if tier == "weak":
        details["veto"] = f"龙头评分不足({dragon:.0f}分)"
        return {"score": 0, "details": details, "veto": details["veto"]}

    if body_pct > 5:
        details["veto"] = f"实体跌幅{body_pct:.1f}%>5%(抛压过重)"
        return {"score": 0, "details": details, "veto": details["veto"]}

    # 分档加分
    tier_bonus = {"confirmed": 20, "watch": 10, "weak": 0}
    bonus = tier_bonus.get(tier, 0)

    score = min(100, dragon + bonus)

    # ── 龙虎榜席位融合 ──
    lhb_adj = 0
    try:
        from src.data.sources.lhb_seats import get_seat_summary
        code = c.get("code", "")
        zt_date = c.get("_zt_date", "")
        summary = get_seat_summary(code, zt_date)
        inst_net = summary.get("inst_net", 0)
        inst_buy = summary.get("inst_buy_count", 0)
        hm_net = summary.get("hot_money_net", 0)

        # 机构大买: 净买入>500万且>=2个席位 → +5分
        if inst_net > 5_000_000 and inst_buy >= 2:
            lhb_adj += 5
            details["lhb_inst"] = f"机构净买{inst_net/10000:.0f}万(+5)"
        elif inst_net > 0:
            lhb_adj += 2
            details["lhb_inst"] = f"机构净买{inst_net/10000:.0f}万(+2)"

        # 游资对倒(买卖都上且净额接近0) → -3分
        if hm_net and abs(hm_net) < 500_000 and inst_net <= 0:
            lhb_adj -= 3
            details["lhb_hot"] = f"游资对倒(-3)"

        score = max(0, min(100, score + lhb_adj))
    except Exception:
        pass

    details["dragon"] = f"{dragon:.0f}/100"
    details["tier"] = tier
    details["body_pct"] = f"{body_pct:.1f}%"
    details["lb"] = c.get("_lb", 0)
    details["tidao"] = c.get("_tidao", 0)
    if lhb_adj:
        details["lhb_adj"] = f"{lhb_adj:+d}"

    # ── 北向/主力资金 ──
    nb_adj, nb_msg = _northbound_adj(code)
    if nb_adj:
        score = max(0, min(100, score + nb_adj))
        details["northbound"] = nb_msg

    # ── 解禁风险 ──
    lk_adj, lk_msg = _lockup_risk_adj(code)
    if lk_adj:
        score = max(0, min(100, score + lk_adj))
        details["lockup"] = lk_msg

    # ── 新闻情感 ──
    sent_adj, sent_msg = _sentiment_adj(code)
    if sent_adj:
        score = max(0, min(100, score + sent_adj))
        details["sentiment"] = sent_msg

    # ── 个股感知涨因 ──
    perc_adj, perc_msg = _perception_adj(c)
    if perc_adj:
        score = max(0, min(100, score + perc_adj))
        details["perception"] = perc_msg

    return {"score": round(score, 1), "details": details, "veto": None}


def _score_strategy_b(c: dict) -> dict:
    """策略B(回踩低吸)评分: 回踩时机35% + 缩量25% + 回踩精度20% + 折扣20%"""
    details = {}
    
    zt_date = c.get("_zt_date", "")
    zt_open = c.get("_zt_open", 0)
    price = c.get("realtime_price", 0)
    
    # 1. 回踩时机(35分)
    days_since_zt = _days_since(zt_date) if zt_date else 3
    if days_since_zt <= 1:
        timing_score = 5
    elif days_since_zt <= 3:
        timing_score = 3
    elif days_since_zt <= 5:
        timing_score = 2
    else:
        timing_score = 1
    details["timing_score"] = timing_score
    details["days_since_zt"] = days_since_zt
    
    # 2. 缩量程度(25分)
    vol_ratio = c.get("volume_ratio", 0.5)
    if 0.3 <= vol_ratio <= 0.5:
        shrink_score = 5
    elif vol_ratio < 0.3:
        shrink_score = 4
    elif vol_ratio <= 0.8:
        shrink_score = 2
    else:
        shrink_score = 1
    details["shrink_score"] = shrink_score
    
    # 3. 回踩精度(20分)
    if zt_open > 0 and price > 0:
        dist_pct = abs(price / zt_open - 1) * 100
        if dist_pct < 0.5:
            precision_score = 5
        elif dist_pct < 1.0:
            precision_score = 2
        elif dist_pct <= 2.0:
            precision_score = 4
        else:
            precision_score = 0
        details["dist_pct"] = round(dist_pct, 2)
    else:
        precision_score = 2
    details["precision_score"] = precision_score
    
    # 4. 买入折扣(20分)
    if zt_open > 0 and price > 0:
        discount = (zt_open / price - 1) * 100
        if discount > 3:
            disc_score = 5
        elif discount > 1:
            disc_score = 4
        elif discount >= -1:
            disc_score = 3
        else:
            disc_score = 1
        details["discount_val"] = round(discount, 2)
    else:
        disc_score = 3
    details["disc_score"] = disc_score
    
    # 加权: 使用IC驱动权重(fallback到经验权重)
    w = _get_dynamic_weights("B")
    w_timing = w.get("timing", 0.35)
    w_shrink = w.get("shrink", 0.25)
    w_precision = w.get("precision", 0.20)
    w_discount = w.get("discount", 0.20)

    raw = (timing_score * 5 * (w_timing / 0.35) +
           shrink_score * 5 * (w_shrink / 0.25) +
           precision_score * 5 * (w_precision / 0.20) +
           disc_score * 5 * (w_discount / 0.20))
    score = min(100, raw) if raw > 0 else 0
    
    details["timing"] = f"{timing_score}/5"
    details["shrink"] = f"{shrink_score}/5"
    details["precision"] = f"{precision_score}/5"
    details["discount"] = f"{disc_score}/5"

    # ── 北向/主力资金 ──
    code = c.get("code", "")
    nb_adj, nb_msg = _northbound_adj(code)
    if nb_adj:
        score = max(0, min(100, score + nb_adj))
        details["northbound"] = nb_msg

    # ── 解禁风险 ──
    lk_adj, lk_msg = _lockup_risk_adj(code)
    if lk_adj:
        score = max(0, min(100, score + lk_adj))
        details["lockup"] = lk_msg

    # ── 新闻情感 ──
    sent_adj, sent_msg = _sentiment_adj(code)
    if sent_adj:
        score = max(0, min(100, score + sent_adj))
        details["sentiment"] = sent_msg

    # ── 个股感知涨因 ──
    perc_adj, perc_msg = _perception_adj(c)
    if perc_adj:
        score = max(0, min(100, score + perc_adj))
        details["perception"] = perc_msg

    return {"score": round(score, 1), "details": details, "veto": None}


def _score_strategy_c(c: dict) -> dict:
    """策略C(趋势牛股)v2评分: 量比(核心)40% + MA60距离25% + RSI 20% + 档位15%
    
    基于strategy_c.py v2输出字段: _tier/_ma60_pct/_vol_ratio/_rsi/score
    回测数据: 量比>8→PF=2.21/60%胜率, 量比5-8→PF=1.57/54%胜率
    """
    details = {}
    veto = ""
    
    # 从strategy_c候选获取趋势数据
    trend_score_raw = c.get("score", 0)  # strategy_c算的趋势分(0-100)
    tier = c.get("_tier", "watch")       # hot/normal/watch
    ma60_pct = c.get("_ma60_pct", 0)     # 距MA60涨幅%
    vol_ratio = c.get("_vol_ratio", 0)
    rsi = c.get("_rsi", 0)
    
    # 1. 量比(40分) — 核心因子, 回测验证量比>8是甜蜜区
    if vol_ratio >= 15:
        vol_score = 40
    elif vol_ratio >= 8:
        vol_score = 35
    elif vol_ratio >= 5:
        vol_score = 25
    elif vol_ratio >= 3:
        vol_score = 15
    else:
        vol_score = 5
        if vol_ratio < 2:
            veto = "量比<2, 放量不足"
    details["vol_ratio"] = f"{vol_ratio:.1f}→{vol_score}/40"
    
    # 2. MA60距离(25分) — 距MA60越近(刚突破)越好
    if ma60_pct <= 0:
        ma_score = 5   # 在MA60以下
    elif ma60_pct <= 3:
        ma_score = 25  # 刚突破, 最佳
    elif ma60_pct <= 5:
        ma_score = 20
    elif ma60_pct <= 8:
        ma_score = 15
    elif ma60_pct <= 10:
        ma_score = 10
    else:
        ma_score = 5
        if ma60_pct > 15:
            veto = "距MA60>15%, 追高风险大"
    details["ma60_dist"] = f"+{ma60_pct:.1f}%→{ma_score}/25"
    
    # 3. RSI(20分) — 55-65最佳(回测验证50-70区间)
    if rsi and 55 <= rsi <= 65:
        rsi_score = 20
    elif rsi and 50 <= rsi <= 70:
        rsi_score = 15
    elif rsi and 40 <= rsi <= 75:
        rsi_score = 10
    elif rsi and rsi > 80:
        rsi_score = 3   # 超买
        veto = "RSI>80超买"
    else:
        rsi_score = 5
    details["rsi"] = f"RSI{rsi:.0f}→{rsi_score}/20"
    
    # 4. 档位(15分) — hot/normal/watch
    tier_score = {"hot": 15, "normal": 10, "watch": 3}.get(tier, 3)
    details["tier"] = f"{tier}→{tier_score}/15"

    # IC驱动权重(fallback到经验权重)
    w = _get_dynamic_weights("C")
    w_vol = w.get("vol_ratio", 0.40)
    w_ma = w.get("ma60_dist", 0.25)
    w_rsi = w.get("rsi", 0.20)
    w_tier = w.get("tier", 0.15)

    total = (vol_score * (w_vol / 0.40) +
             ma_score * (w_ma / 0.25) +
             rsi_score * (w_rsi / 0.20) +
             tier_score * (w_tier / 0.15))
    details["total"] = total

    # ── 北向/主力资金 ──
    code = c.get("code", "")
    nb_adj, nb_msg = _northbound_adj(code)
    if nb_adj:
        total = max(0, min(100, total + nb_adj))
        details["northbound"] = nb_msg

    # ── 解禁风险 ──
    lk_adj, lk_msg = _lockup_risk_adj(code)
    if lk_adj:
        total = max(0, min(100, total + lk_adj))
        details["lockup"] = lk_msg

    # ── 新闻情感 ──
    sent_adj, sent_msg = _sentiment_adj(code)
    if sent_adj:
        total = max(0, min(100, total + sent_adj))
        details["sentiment"] = sent_msg

    # ── 个股感知涨因 ──
    perc_adj, perc_msg = _perception_adj(c)
    if perc_adj:
        total = max(0, min(100, total + perc_adj))
        details["perception"] = perc_msg

    return {"score": total, "details": details, "veto": veto}

def filter_by_score(candidates: list[dict], strategy: str = "B",
                    min_score: float = 60, phase: str = "正常") -> list[dict]:
    """批量评分+过滤, 返回通过精选的候选(按分数降序)
    
    Args:
        candidates: 候选列表
        strategy: "A" 或 "B"
        min_score: 最低分数线(退潮时提高到70)
        phase: 市场情绪阶段
    """
    # 退潮/冰点时提高门槛
    if phase in ("退潮", "冰点", "退潮预警"):
        min_score = max(min_score, 70)
    
    scored = []
    for c in candidates:
        result = score_candidate(c, strategy)
        c["_score"] = result["score"]
        c["_score_details"] = result["details"]
        c["_veto"] = result.get("veto")

        if result.get("veto"):
            continue
        if result["score"] < min_score:
            continue
        scored.append(c)

    scored.sort(key=lambda x: x.get("_score", 0), reverse=True)

    # 辩论式信号融合(可选, 默认关闭)
    try:
        from src.trader.daemon_config import DEBATE_ENABLED, DEBATE_MIN_CONFIDENCE
        if DEBATE_ENABLED and scored:
            from src.agent.debate_agent import debate_batch
            scored = debate_batch(scored, strategy)
    except Exception:
        pass

    return scored


# ── 内部辅助 ──

def _check_amount(code: str) -> bool:
    """检查最近一日成交额是否>=2亿"""
    try:
        conn = _get_conn()
        r = conn.execute(
            "SELECT amount FROM daily_price WHERE stock_code=? ORDER BY trade_date DESC LIMIT 1",
            (code,)
        ).fetchone()
        conn.close()
        if r and r[0]:
            return r[0] / 10000 >= 20000
    except Exception:
        pass
    return False


def _get_zt_open_times(code: str) -> int | None:
    """从zt_pool查最近涨停日的开板次数"""
    try:
        conn = _get_conn()
        r = conn.execute(
            "SELECT open_times FROM zt_pool WHERE stock_code=? ORDER BY trade_date DESC LIMIT 1",
            (code,)
        ).fetchone()
        conn.close()
        if r:
            return r[0]
    except Exception:
        pass
    return None


def _get_seal_time(code: str) -> str | None:
    """从zt_pool查涨停封板时间"""
    try:
        conn = _get_conn()
        r = conn.execute(
            "SELECT first_time FROM zt_pool WHERE stock_code=? ORDER BY trade_date DESC LIMIT 1",
            (code,)
        ).fetchone()
        conn.close()
        if r and r[0]:
            return r[0][:5] if len(r[0]) >= 5 else r[0]
    except Exception:
        pass
    return None


def _days_since(date_str: str) -> int:
    """计算距今天多少天"""
    try:
        d = datetime.strptime(date_str[:10], "%Y-%m-%d")
        return (datetime.now() - d).days
    except Exception:
        return 3


def _northbound_adj(code: str) -> tuple[int, str]:
    """北向/主力资金加分: 最近5天main_net持续净流入+3, 持续净流出-3

    Returns:
        (adjustment, detail_str)
    """
    try:
        conn = _get_conn()
        rows = conn.execute("""
            SELECT trade_date, super_large_net + large_net as main_net
            FROM fund_flow
            WHERE stock_code = ?
            ORDER BY trade_date DESC LIMIT 5
        """, (code,)).fetchall()
        conn.close()
        if len(rows) < 3:
            return 0, ""
        nets = [r[1] or 0 for r in rows]
        total = sum(nets)
        if all(n > 0 for n in nets) and total > 0:
            return 3, f"主力5日持续净流入{total/10000:.0f}万(+3)"
        if all(n < 0 for n in nets) and total < 0:
            return -3, f"主力5日持续净流出{total/10000:.0f}万(-3)"
        return 0, ""
    except Exception:
        return 0, ""


def _lockup_risk_adj(code: str) -> tuple[int, str]:
    """解禁风险减分: 未来30天有解禁且市值>日均成交额50% → -5

    Returns:
        (adjustment, detail_str)
    """
    try:
        conn = _get_conn()
        lockups = conn.execute("""
            SELECT free_date, lift_market_cap
            FROM lockup_calendar
            WHERE stock_code = ? AND free_date >= date('now') AND free_date <= date('now', '+30 days')
            ORDER BY free_date
        """, (code,)).fetchall()
        if not lockups:
            conn.close()
            return 0, ""

        # 最近5日平均成交额(万元)
        avg_amt = conn.execute("""
            SELECT AVG(amount) FROM (
                SELECT amount FROM daily_price
                WHERE stock_code = ? ORDER BY trade_date DESC LIMIT 5
            )
        """, (code,)).fetchone()
        conn.close()
        avg_daily = (avg_amt[0] or 0) / 10000  # 元→万元
        if avg_daily <= 0:
            return 0, ""

        # 最大解禁市值 vs 日均成交额
        max_cap = max(l[1] or 0 for l in lockups)  # 万元
        if max_cap > avg_daily * 0.5:
            pct = max_cap / avg_daily * 100
            return -5, f"解禁风险: {lockups[0][0]}解禁{max_cap/10000:.0f}万(日均{pct:.0f}%)(-5)"
        return 0, ""
    except Exception:
        return 0, ""
