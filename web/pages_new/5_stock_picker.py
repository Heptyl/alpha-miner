"""选股中心 — 三策略统一入口

Tab1: 策略A(龙头首阴反包) + 策略B(回踩低吸) + 策略C(基本面驱动)
Tab2: 9维选股 TOP20
Tab3: 板块热度 + 连板龙头
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import streamlit as st
import pandas as pd
import sqlite3
from datetime import datetime

from web.styles import inject_styles
from web.components import metric_card, signal_tag, fmt_pct, pnl_color
from web.services.data_service import (
    get_ml_candidates, get_strategy_b_candidates, get_strategy_c_candidates, get_all_candidates,
    get_zt_ladder, get_hot_sectors, get_market_overview,
    get_factor_weights, get_stock_sentiment, get_stock_trade_memory,
    get_industry_concentration, get_northbound_flow, get_lockup_risk, get_lhb_detail,
)

inject_styles()

st.markdown("## 🎯 选股中心")

# ── 增强数据预加载 ──
fw = get_factor_weights()
industry_conc = get_industry_concentration()


def _sentiment_badge(score: float) -> str:
    """新闻情感得分 → 带颜色标签"""
    if score >= 0.5:
        color, label = "#4caf50", "强多"
    elif score >= 0.3:
        color, label = "#66bb6a", "偏多"
    elif score > -0.3:
        color, label = "#808080", "中性"
    elif score > -0.5:
        color, label = "#ef5350", "偏空"
    else:
        color, label = "#c62828", "强空"
    return f'<span style="color:{color};font-size:0.7rem;font-weight:600;">{label}({score:+.1f})</span>'


def _trade_memory_tag(code: str) -> str:
    """交易记忆标签"""
    mem = get_stock_trade_memory(code)
    n = mem["trades"]
    if n == 0:
        return '<span style="color:#607d8b;font-size:0.65rem;">无历史</span>'
    wr = mem["win_rate"]
    avg = mem["avg_pnl"]
    wr_color = "#4caf50" if wr >= 55 else "#ffa726" if wr >= 40 else "#ef5350"
    return (f'<span style="font-size:0.65rem;">'
            f'<span style="color:{wr_color};">历史{n}笔/胜率{wr:.0f}%</span>'
            f' <span style="color:#808080;">均收{avg:+.1f}%</span></span>')


def _industry_conc_tag(code: str) -> str:
    """行业集中度标签"""
    try:
        conn = sqlite3.connect(str(Path(__file__).parent.parent.parent / "data" / "alpha_miner.db"))
        row = conn.execute(
            "SELECT industry_name FROM stock_industry_mapping WHERE stock_code=?", (code,)
        ).fetchone()
        conn.close()
        if not row or not row[0]:
            return ""
        ind = row[0]
        cnt = industry_conc.get(ind, 0)
        if cnt >= 2:
            return f'<span style="color:#ef5350;font-size:0.65rem;">⚠{ind}已持{cnt}只</span>'
        elif cnt >= 1:
            return f'<span style="color:#ffa726;font-size:0.65rem;">{ind}持{cnt}只</span>'
        return ""
    except Exception:
        return ""


def _northbound_tag(code: str) -> str:
    """北向资金标签"""
    nb = get_northbound_flow(code)
    total = nb.get("total_net", 0)
    if not nb.get("flows"):
        return ""
    color = "#4caf50" if total > 0 else "#ef5350"
    direction = "净流入" if total > 0 else "净流出"
    val = abs(total / 10000)
    unit = "万" if val < 1 else ""
    return f'<span style="color:{color};font-size:0.65rem;">北向5日{direction}{val:.0f}{unit}</span>'


def _lockup_tag(code: str) -> str:
    """解禁风险标签"""
    lockups = get_lockup_risk(code)
    if not lockups:
        return ""
    l = lockups[0]
    cap = (l.get("cap") or 0) / 10000
    return f'<span style="color:#ef5350;font-size:0.65rem;">⚠{l["date"]}解禁{cap:.0f}万</span>'


def _ic_weights_html(strategy: str) -> str:
    """IC驱动权重HTML"""
    weights = fw.get(strategy, {})
    if not weights:
        return ""
    parts = []
    for fname, info in weights.items():
        w = info.get("weight", 0)
        method = info.get("method", "empirical")
        ic = info.get("ic_mean", 0)
        tag = "IC" if method == "ic_driven" else "经验"
        parts.append(f"{fname[:4]}:{w:.0f}%({tag})")
    return '<span style="font-size:0.6rem;color:#ab47bc;">' + " ".join(parts) + '</span>'


def _lhb_html(code: str) -> str:
    """龙虎榜明细HTML"""
    lhb = get_lhb_detail(code)
    if not lhb:
        return ""
    l = lhb[0]
    net = (l.get("net") or 0) / 10000
    color = "#4caf50" if net > 0 else "#ef5350"
    buy_d = (l.get("buy_dept") or "")[:12]
    sell_d = (l.get("sell_dept") or "")[:12]
    return (f'<div style="font-size:0.65rem;color:#808080;">'
            f'龙虎榜{l["date"]} 净{net:+.0f}万 '
            f'<span style="color:#66bb6a;">买:{buy_d}</span> '
            f'<span style="color:#ef5350;">卖:{sell_d}</span></div>')


def _debate_html(code: str) -> str:
    """辩论结果HTML"""
    try:
        from src.agent.debate_agent import get_debate_result
        d = get_debate_result(code)
    except Exception:
        return ""
    if not d:
        return ""
    conf = d.get("confidence", 0)
    verdict = d.get("verdict", "")
    reasoning = d.get("reasoning", "")
    if verdict == "bull":
        v_color, v_label = "#4caf50", "BULL"
    elif verdict == "bear":
        v_color, v_label = "#ef5350", "BEAR"
    else:
        v_color, v_label = "#ffa726", "NEUTRAL"
    conf_color = "#4caf50" if conf >= 65 else "#ffa726" if conf >= 50 else "#ef5350"
    risk = d.get("key_risk", "")
    risk_span = f' <span style="color:#ef5350;">⚠{risk[:20]}</span>' if risk else ""
    return (f'<div style="font-size:0.65rem;color:#808080;">'
            f'辩论 <span style="color:{v_color};font-weight:600;">{v_label}</span> '
            f'<span style="color:{conf_color};">{conf}/100</span>'
            f'{risk_span}'
            f'</div>')

# 选股防御: 数据完整性检查
try:
    all_cands = get_all_candidates()
    sa_cnt = len([c for c in all_cands if c.get("_strategy") == "A"])
    sb_cnt = len([c for c in all_cands if c.get("_strategy") == "B"])
    sc_cnt = len([c for c in all_cands if c.get("_strategy") == "C"])
    if sa_cnt == 0:
        st.error("策略A返回0只候选! 请立即检查 daily_price 数据和因子计算")
    elif sa_cnt < 5:
        st.markdown(
            '<div style="background:#3e2723;border:1px solid #ef5350;border-radius:8px;'
            'padding:8px 14px;margin-bottom:10px;font-size:0.85rem;color:#ef5350;">'
            f'⚠️ 策略A仅{sa_cnt}只候选(正常>10只) — 可能数据不完整或因子计算异常</div>',
            unsafe_allow_html=True,
        )
    if sb_cnt == 0 and sa_cnt == 0:
        st.error("三策略均返回0只候选! 系统可能存在数据故障")
except Exception as e:
    st.warning(f"选股检查异常: {e}")

tab1, tab2, tab3 = st.tabs(["三策略候选", "9维选股", "板块热度"])

# ============================================================
# Tab1: 三策略候选
# ============================================================
with tab1:
    col_a, col_b, col_c = st.columns(3)

    # 策略A(龙头首阴反包) — 确认 + 观察 + 偏弱
    with col_a:
        st.markdown("### 策略A — 龙头首阴反包(3万)")
        st.caption("绝对龙头首阴·次日高开2%+翻红确认·持2-3天·跌破首阴低止损 | 7来源调研")
        sa_cands = [c for c in all_cands if c.get("_strategy") == "A"]
        sa_confirmed = [c for c in sa_cands if c.get("_tier") == "confirmed"]
        sa_watch = [c for c in sa_cands if c.get("_tier") == "watch"]
        sa_weak = [c for c in sa_cands if c.get("_tier") == "weak"]

        if sa_confirmed:
            st.markdown('<div style="color:#4caf50;font-size:0.85rem;font-weight:600;">✅ 确认(龙头分>=40+实体<3%+有下影线)</div>', unsafe_allow_html=True)
            for c in sa_confirmed[:5]:
                name = c.get("name", c.get("code", ""))
                code = c.get("code", "")
                yin_date = c.get("_yin_date", "")
                stop_loss = c.get("_stop_loss", 0)
                body_pct = c.get("_yin_body_pct", 0)
                dragon = c.get("_dragon_total", 0)
                lb = c.get("_lb", 0)
                ind = c.get("_industry", "")
                tidao = c.get("_tidao", 0)
                price = c.get("realtime_price", c.get("price", 0))
                chg = c.get("realtime_chg", c.get("change_pct", 0))
                # 增强数据
                sent = get_stock_sentiment(code)
                sent_badge = _sentiment_badge(sent["score"]) if sent["count"] > 0 else ""
                mem_tag = _trade_memory_tag(code)
                conc_tag = _industry_conc_tag(code)
                nb_tag = _northbound_tag(code)
                lock_tag = _lockup_tag(code)
                ic_html = _ic_weights_html("A")
                lhb_html = _lhb_html(code)
                debate_html = _debate_html(code)
                # 增强信息行
                tags = [t for t in [sent_badge, mem_tag, conc_tag, nb_tag, lock_tag] if t]
                tags_line = " · ".join(tags)
                tags_div = f'<div style="font-size:0.7rem;margin-top:2px;">{tags_line}</div>' if tags_line else ""
                ic_div = f'<div style="margin-top:2px;">{ic_html}</div>' if ic_html else ""
                st.markdown(
                    f'<div style="background:#1a2332;border:1px solid #66bb6a;border-radius:6px;padding:8px 12px;margin:3px 0;">'
                    f'<div style="display:flex;justify-content:space-between;">'
                    f'<div><strong>{name}</strong> <span style="color:#808080;font-size:0.75rem;">{code}</span>'
                    f' <span style="color:#4caf50;font-size:0.7rem;">★确认</span>'
                    f' <span style="color:#ab47bc;font-size:0.7rem;">龙头{dragon:.0f}分</span></div>'
                    f'<div class="{pnl_color(chg)}" style="font-weight:600;">{chg:+.1f}%</div></div>'
                    f'<div style="font-size:0.75rem;color:#808080;">{lb}连板首阴{yin_date} · 实体{body_pct:.1f}% · 止损¥{stop_loss:.2f}(首阴低)</div>'
                    f'<div style="font-size:0.75rem;color:#808080;">板块{ind}({tidao}只) · 现价¥{price:.2f}</div>'
                    f'{tags_div}{ic_div}{lhb_html}{debate_html}'
                    f'</div>',
                    unsafe_allow_html=True,
                )

        if sa_watch:
            st.markdown('<div style="color:#ffa726;font-size:0.85rem;font-weight:600;margin-top:8px;">👁 观察(连板>=3或龙头分>=30, 等次日确认)</div>', unsafe_allow_html=True)
            for c in sa_watch[:10]:
                name = c.get("name", c.get("code", ""))
                code = c.get("code", "")
                yin_date = c.get("_yin_date", "")
                stop_loss = c.get("_stop_loss", 0)
                body_pct = c.get("_yin_body_pct", 0)
                dragon = c.get("_dragon_total", 0)
                lb = c.get("_lb", 0)
                ind = c.get("_industry", "")
                tidao = c.get("_tidao", 0)
                price = c.get("realtime_price", c.get("price", 0))
                chg = c.get("realtime_chg", c.get("change_pct", 0))
                st.markdown(
                    f'<div style="background:#1a2332;border:1px solid #37474f;border-radius:6px;padding:6px 12px;margin:2px 0;">'
                    f'<div style="display:flex;justify-content:space-between;">'
                    f'<div><strong>{name}</strong> <span style="color:#808080;font-size:0.75rem;">{code}</span>'
                    f' <span style="color:#ffa726;font-size:0.7rem;">观察</span>'
                    f' <span style="color:#ab47bc;font-size:0.7rem;">龙头{dragon:.0f}分</span></div>'
                    f'<div class="{pnl_color(chg)}" style="font-weight:600;font-size:0.85rem;">{chg:+.1f}%</div></div>'
                    f'<div style="font-size:0.75rem;color:#607d8b;">{lb}连板 · 实体{body_pct:.1f}% · 板块{ind}({tidao}只)</div>'
                    f'</div>',
                    unsafe_allow_html=True,
                )

        if sa_weak:
            st.markdown('<div style="color:#78909c;font-size:0.85rem;font-weight:600;margin-top:8px;">⚠ 偏弱(不自动买入)</div>', unsafe_allow_html=True)
            for c in sa_weak[:5]:
                name = c.get("name", c.get("code", ""))
                code = c.get("code", "")
                dragon = c.get("_dragon_total", 0)
                lb = c.get("_lb", 0)
                st.markdown(
                    f'<div style="font-size:0.75rem;color:#546e7a;">{name}({code}) {lb}连板 龙头{dragon:.0f}分</div>',
                    unsafe_allow_html=True,
                )

        if not sa_cands:
            st.info("策略A暂无候选(需连板龙头出现首阴)")

    # 策略B
    with col_b:
        st.markdown("### 策略B — 回踩低吸(3万)")
        st.caption("首板回踩涨停开盘价·盘中实时监控·15秒扫描 | 理想价PF=2.50")
        sb_cands = get_strategy_b_candidates()
        if sb_cands:
            for c in sb_cands[:10]:
                name = c.get("name", "")
                code = c.get("code", "")
                chg = c.get("realtime_chg", c.get("change_pct", 0))
                price = c.get("realtime_price", c.get("price", 0))
                shrink = c.get("_shrink_pct", 0)
                support = c.get("_support_dist", 0)
                reason = c.get("reason", "")
                st.markdown(
                    f'<div style="background:#1a2332;border:1px solid #42a5f5;border-radius:6px;padding:8px 12px;margin:3px 0;">'
                    f'<div style="display:flex;justify-content:space-between;">'
                    f'<div><strong>{name}</strong> <span style="color:#808080;font-size:0.75rem;">{code}</span></div>'
                    f'<div class="{pnl_color(chg)}" style="font-weight:600;">{chg:+.1f}%</div></div>'
                    f'<div style="font-size:0.75rem;color:#808080;">缩{shrink:.0f}% 回踩{support:+.1f}% ¥{price:.2f}</div>'
                    f'<div style="font-size:0.65rem;color:#555;">{reason}</div>'
                    f'</div>',
                    unsafe_allow_html=True,
                )
        else:
            st.info("策略B暂无已回踩候选")

        # 策略B待监控watchlist
        st.markdown("---")
        try:
            from src.strategy.strategy_b import get_strategy_b_watchlist
            wl = get_strategy_b_watchlist()
            wl_count = len(wl) if wl else 0
            st.markdown(f"#### 待回踩监控({wl_count}只)")
            st.caption("daemon盘中15秒扫描, 回踩到涨停开盘价±1%时30秒买入")
            if wl:
                for w in wl[:30]:
                    name = w.get("name", "")
                    code = w.get("code", "")
                    target = w.get("_buy_target", 0)
                    zt_date = w.get("_zt_date", "")
                    days = w.get("_days_since_zt", 0)
                    dist_pct = w.get("_dist_to_target_pct", 0)
                    current_price = w.get("_current_price", 0)

                    # 跳过无最新日线的票(防御)
                    if not current_price or current_price <= 0:
                        continue

                    # 回踩时机标记: 综合距离和时间
                    if abs(dist_pct) > 5:
                        # 距离超过5%, 偏离过大
                        timing_tag = '<span style="color:#ef5350;">⚠偏离过大</span>'
                    elif days >= 3:
                        timing_tag = '<span style="color:#4caf50;">★时机好</span>'
                    else:
                        timing_tag = '<span style="color:#808080;">等待中</span>'

                    # 距离颜色: 接近0绿色, 远离0灰色, >5%红色
                    if abs(dist_pct) <= 1.5:
                        dist_color = "#4caf50"
                    elif abs(dist_pct) <= 5:
                        dist_color = "#ffa726"
                    else:
                        dist_color = "#ef5350"

                    # 增强数据
                    mem_tag = _trade_memory_tag(code)
                    conc_tag = _industry_conc_tag(code)
                    lock_tag = _lockup_tag(code)
                    tags = [t for t in [mem_tag, conc_tag, lock_tag] if t]
                    tags_line = " · ".join(tags)
                    tags_line_extra = f'<br><span style="font-size:0.65rem;">{tags_line}</span>' if tags_line else ""

                    st.markdown(
                        f'<div style="background:#1a2332;border:1px solid #1e88e5;border-radius:6px;padding:6px 10px;margin:2px 0;font-size:0.8rem;">'
                        f'<strong>{name}</strong> <span style="color:#808080;">{code}</span> {timing_tag}'
                        f' <span style="color:#ab47bc;font-size:0.7rem;">龙{w.get("_dragon_grade","?")}{w.get("_dragon_score",0):.0f}</span>'
                        f' <span style="color:{dist_color};font-size:0.75rem;">距目标{dist_pct:+.1f}%</span><br>'
                        f'<span style="color:#808080;">目标¥{target:.2f}(主力成本) 现价¥{current_price:.2f} 涨停{zt_date} 第{days}天</span>'
                        f'{tags_line_extra}'
                        f'</div>',
                        unsafe_allow_html=True,
                    )
                if len(wl) > 30:
                    st.caption(f"... 共{len(wl)}只待监控")
            else:
                st.info("暂无待监控票")
        except Exception as e:
            st.markdown("#### 待回踩监控")
            st.warning(f"watchlist加载失败: {e}")

    # 策略C — 基本面驱动+AI赛道(5万)
    with col_c:
        st.markdown("### 🧠 策略C — 基本面驱动+AI赛道(5万)")
        st.caption("四层漏斗: 行业景气→基本面55+→技术信号→仓位 | v3 PF=3.85")
        sc_cands = get_strategy_c_candidates()

        if sc_cands:
            for x in sc_cands[:10]:
                code = x.get("code", x.get("stock_code", ""))
                name = x.get("name", x.get("stock_name", ""))
                score = x.get("score", 0)
                signals = ", ".join(x.get("signals", []))
                details = x.get("details", {})
                a_s = details.get("A_profitability", {}).get("score", 0)
                b_s = details.get("B_growth", {}).get("score", 0)
                d_s = details.get("D_track", {}).get("score", 0)
                e_s = details.get("E_signals", {}).get("score", 0)
                tier = details.get("D_track", {}).get("tier", "")
                ind = details.get("D_track", {}).get("industry", "")
                price = x.get("realtime_price", x.get("price", 0))
                chg = x.get("realtime_chg", x.get("change_pct", 0))
                # 增强数据
                sent = get_stock_sentiment(code)
                sent_badge = _sentiment_badge(sent["score"]) if sent["count"] > 0 else ""
                mem_tag = _trade_memory_tag(code)
                conc_tag = _industry_conc_tag(code)
                nb_tag = _northbound_tag(code)
                lock_tag = _lockup_tag(code)
                ic_html = _ic_weights_html("C")
                lhb_html = _lhb_html(code)
                debate_html = _debate_html(code)
                tags = [t for t in [sent_badge, mem_tag, conc_tag, nb_tag, lock_tag] if t]
                tags_line = " · ".join(tags)
                tags_div = f'<div style="font-size:0.7rem;margin-top:2px;">{tags_line}</div>' if tags_line else ""
                ic_div = f'<div style="margin-top:2px;">{ic_html}</div>' if ic_html else ""
                st.markdown(
                    f'<div style="background:#1a2332;border:1px solid #7e57c2;border-radius:6px;padding:8px 12px;margin:3px 0;">'
                    f'<div style="display:flex;justify-content:space-between;">'
                    f'<div><strong>{name or code}</strong> <span style="color:#808080;font-size:0.75rem;">{code}</span>'
                    f' <span style="color:#7e57c2;font-size:0.7rem;">{tier}</span></div>'
                    f'<div class="{pnl_color(chg)}" style="font-weight:600;">{chg:+.1f}%</div></div>'
                    f'<div style="font-size:0.75rem;color:#b39ddb;">基本面{score}分 · 盈利{a_s} 成长{b_s} 赛道{d_s} 信号{e_s}</div>'
                    f'<div style="font-size:0.75rem;color:#808080;">板块{ind} · ¥{price:.2f} · {signals}</div>'
                    f'{tags_div}{ic_div}{lhb_html}{debate_html}'
                    f'</div>',
                    unsafe_allow_html=True,
                )
        else:
            st.info("策略C暂无候选(AI赛道基本面55分+技术信号)")

# Tab2: 9维选股
# ============================================================
with tab2:
    st.markdown("### 🔍 9维选股 TOP20")
    st.caption("技术35% + 资金35% + 基本面10% + 板块10% + 风控10%")

    try:
        from src.screener.engine import ScreenerEngine
        from datetime import datetime
        today = datetime.now().strftime('%Y-%m-%d')
        se = ScreenerEngine()
        ranking = se.run(today)
        if ranking:
            df_data = []
            for r in ranking:
                # r可能是对象或dict
                if hasattr(r, '__dict__'):
                    d = r.__dict__
                else:
                    d = r if isinstance(r, dict) else {}
                df_data.append({
                    "代码": d.get("stock_code", ""),
                    "名称": d.get("stock_name", ""),
                    "综合分": f'{d.get("total_score", 0):.2f}',
                    "技术": f'{d.get("category_scores", {}).get("technical", 0):.2f}',
                    "资金": f'{d.get("category_scores", {}).get("capital", 0):.2f}',
                    "板块": d.get("industry", ""),
                    "信号": d.get("signal_level", ""),
                })
            st.dataframe(pd.DataFrame(df_data), use_container_width=True, hide_index=True)
        else:
            st.info("暂无9维选股数据")
    except Exception as e:
        st.info(f"9维选股模块加载中... ({e})")

# ============================================================
# Tab3: 板块热度
# ============================================================
with tab3:
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### 🔥 热门板块 TOP10")
        sectors = get_hot_sectors()
        if sectors:
            for i, s in enumerate(sectors[:10]):
                zt_count = s.get("zt_count", 0)
                industry = s.get("industry", "")
                # 热度条
                max_zt = sectors[0].get("zt_count", 1) if sectors else 1
                bar_pct = int(zt_count / max_zt * 100) if max_zt else 0
                st.markdown(
                    f'<div style="margin:4px 0;">'
                    f'<span style="font-size:0.9rem;color:#42a5f5;">{industry}</span> '
                    f'<span class="am-tag am-tag-up">{zt_count}只涨停</span>'
                    f'<div style="height:4px;background:#2a3a4e;border-radius:2px;margin-top:2px;">'
                    f'<div style="height:100%;width:{bar_pct}%;background:#42a5f5;border-radius:2px;"></div>'
                    f'</div></div>',
                    unsafe_allow_html=True,
                )
        else:
            st.info("暂无板块数据")

    with col2:
        st.markdown("### 🏆 连板梯队")
        ladder = get_zt_ladder()
        if ladder:
            for boards in sorted(ladder.keys(), reverse=True):
                stocks = ladder[boards]
                st.markdown(
                    f'<div style="margin:8px 0;">'
                    f'<span class="am-tag am-tag-up" style="font-size:0.85rem;">{boards}连板 ({len(stocks)}只)</span>',
                    unsafe_allow_html=True,
                )
                for s in stocks[:5]:
                    st.markdown(
                        f'<span style="font-size:0.85rem;padding-left:12px;">'
                        f'{s["name"]}({s["code"]}) <span class="am-sector">{s.get("industry","")}</span>'
                        f'</span>',
                        unsafe_allow_html=True,
                    )
        else:
            st.info("暂无涨停数据")
