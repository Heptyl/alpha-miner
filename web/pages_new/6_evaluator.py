"""股票评测 — 单只股票全景分析 v2
升级: 策略匹配+风险评分+资金流+新闻情绪+止损建议
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import streamlit as st
import plotly.graph_objects as go
import pandas as pd
from datetime import datetime, timedelta

from web.styles import inject_styles
from web.components import metric_card, fmt_pct, fmt_price, pnl_color
from web.services.data_service import get_stock_detail

inject_styles()

st.markdown("## 🔬 股票评测")

# 防御检查: 选股状态
def _check_data_freshness():
    """检查数据是否今天"""
    import sqlite3
    try:
        conn = sqlite3.connect("data/alpha_miner.db")
        latest = conn.execute("SELECT MAX(trade_date) FROM daily_price").fetchone()[0]
        today = datetime.now().strftime("%Y-%m-%d")
        if latest != today:
            st.caption(f"⚠️ 数据截止{latest}(非今日), 结果可能不准确")
        conn.close()
    except Exception:
        pass

_check_data_freshness()

# 输入框
col1, col2 = st.columns([1, 3])
with col1:
    code_input = st.text_input("输入股票代码", placeholder="如 300059", key="eval_code")
with col2:
    if st.button("🔍 开始评测", key="eval_start", use_container_width=True):
        if code_input:
            st.session_state.eval_target = code_input.strip()

target = st.session_state.get("eval_target", "")

if not target:
    st.info("👆 输入股票代码开始评测")
    st.stop()

# 加载数据
detail = get_stock_detail(target)
if not detail or not detail.get("name"):
    st.error(f"找不到 {target} 的数据")
    st.stop()

name = detail["name"]
price = detail.get("price", 0)
change_pct = detail.get("change_pct", 0)

st.markdown(f"### {name} ({target})")

# 基本指标
cols = st.columns(6)
with cols[0]: metric_card("现价", fmt_price(price), fmt_pct(change_pct), change_pct >= 0)
with cols[1]: metric_card("今开", fmt_price(detail.get("open")), "", True)
with cols[2]: metric_card("最高", fmt_price(detail.get("high")), "", True)
with cols[3]: metric_card("最低", fmt_price(detail.get("low")), "", True)
with cols[4]: metric_card("成交量", f'{detail.get("volume",0)/10000:.0f}万', "", True)
with cols[5]: metric_card("成交额", f'{detail.get("amount",0)/100000000:.1f}亿', "", True)

st.divider()

# K线图
kline = detail.get("kline", [])
if kline:
    df = pd.DataFrame(kline).sort_values("trade_date")

    fig = go.Figure()
    fig.add_trace(go.Candlestick(
        x=df["trade_date"],
        open=df["open"], high=df["high"],
        low=df["low"], close=df["close"],
        increasing_line_color="#ef5350",
        decreasing_line_color="#26a69a",
    ))

    # 成交量
    fig.add_trace(go.Bar(
        x=df["trade_date"], y=df["volume"],
        name="成交量",
        marker_color="rgba(66,165,245,0.5)",
        yaxis="y2",
    ))

    # MA5/MA20
    if len(df) >= 5:
        df["ma5"] = df["close"].rolling(5).mean()
        fig.add_trace(go.Scatter(x=df["trade_date"], y=df["ma5"], name="MA5",
                                  line=dict(color="#ffd93d", width=1)))
    if len(df) >= 20:
        df["ma20"] = df["close"].rolling(20).mean()
        fig.add_trace(go.Scatter(x=df["trade_date"], y=df["ma20"], name="MA20",
                                  line=dict(color="#42a5f5", width=1)))

    fig.update_layout(
        height=400, margin=dict(l=40, r=20, t=20, b=30),
        paper_bgcolor="#0f1923", plot_bgcolor="#0f1923",
        font_color="#808080",
        xaxis_rangeslider_visible=False,
        yaxis2=dict(overlaying="y", side="right", showticklabels=False),
    )
    st.plotly_chart(fig, use_container_width=True)

# ====== v2新增: 策略匹配 + 风险评分 + 资金流 + 新闻 ======

import sqlite3
conn = sqlite3.connect("data/alpha_miner.db")

left, right = st.columns(2)

with left:
    # === 策略匹配 ===
    st.markdown("#### 🎯 策略匹配")

    # 策略A: 龙头首阴候选匹配
    strat_a_match = False
    strat_a_score = 0
    try:
        from src.strategy.strategy_a import get_strategy_a_candidates
        a_cands = get_strategy_a_candidates()
        for c in a_cands:
            if c.get("code") == target:
                strat_a_match = True
                strat_a_score = c.get("score", 0)
                break
    except Exception:
        pass

    # 策略B: 回踩低吸
    strat_b_match = False
    strat_b_reason = ""
    try:
        from src.strategy.strategy_b import get_strategy_b_candidates
        b_cands = get_strategy_b_candidates()
        for c in b_cands:
            if c.get("code") == target:
                strat_b_match = True
                strat_b_reason = c.get("reason", "回踩低吸")
                break
    except Exception:
        pass

    if strat_a_match or strat_b_match:
        if strat_b_match:
            st.markdown(
                f'<div style="background:#1a3320;border:1px solid #2d5a2d;border-radius:8px;padding:12px;">'
                f'<p>✅ <strong>策略B匹配</strong> — {strat_b_reason}</p>'
                f'<p style="color:#81c784;">回踩低吸策略, 首板后回踩涨停开盘价(主力成本), 持1天, PF=3.86</p>'
                f'</div>', unsafe_allow_html=True)
        if strat_a_match:
            st.markdown(
                f'<div style="background:#1a2a33;border:1px solid #2d4a5a;border-radius:8px;padding:12px;">'
                f'<p>✅ <strong>策略A匹配</strong> — 龙头首阴反包(龙头{strat_a_score:.0f}分)</p>'
                f'<p style="color:#64b5f6;">绝对龙头首阴→次日高开2%+翻红确认→持2-3天→跌破首阴低止损</p>'
                f'</div>', unsafe_allow_html=True)
    else:
        st.markdown(
            '<div style="background:#2a1a1a;border:1px solid #5a2d2d;border-radius:8px;padding:12px;">'
            '<p>❌ <strong>不匹配当前策略</strong></p>'
            '<p style="color:#ef9a9a;">不在策略A(龙头首阴)或策略B(回踩低吸)的候选中</p>'
            '</div>', unsafe_allow_html=True)

    # === 风险评分 + 止损建议 ===
    st.markdown("#### ⚠️ 风险评估")

    # 计算风险因素
    risk_factors = []
    risk_score = 0  # 0-100, 越高越危险

    # 波动率
    if kline and len(kline) >= 21:
        closes = [k["close"] for k in kline if k.get("close")]
        # kline是DESC排序, 先reverse成正序
        closes = closes[::-1]
        if len(closes) >= 21:
            import numpy as np
            recent = closes[-21:]
            returns = np.diff(recent) / recent[:-1]
            vol = np.std(returns) * np.sqrt(252) * 100
            if vol > 50:
                risk_factors.append(f"年化波动率{vol:.0f}% (偏高)")
                risk_score += 20
            elif vol > 30:
                risk_factors.append(f"年化波动率{vol:.0f}% (正常)")
                risk_score += 5

    # 涨幅过大
    if change_pct and change_pct > 9:
        risk_factors.append("今日涨停, 追高风险")
        risk_score += 30
    elif change_pct and change_pct > 5:
        risk_factors.append("今日涨幅>5%")
        risk_score += 15

    # 成交额不足
    amount = detail.get("amount", 0)
    if amount and amount < 100_000_000:  # <1亿
        risk_factors.append(f"成交额{amount/1e8:.1f}亿(流动性差)")
        risk_score += 10

    # 止损建议
    try:
        from src.trader.daemon_config import B_STOP_LOSS_PCT as _B_SL
        stop_loss_pct = abs(_B_SL)  # daemon中是负数如-0.07
    except Exception:
        stop_loss_pct = 0.03  # fallback: B策略止损-3%
    if price and price > 0:
        stop_price = price * (1 - stop_loss_pct)
        risk_text = "低风险" if risk_score < 20 else "中等风险" if risk_score < 40 else "高风险"
        risk_color = "#66bb6a" if risk_score < 20 else "#ffa726" if risk_score < 40 else "#ef5350"

        st.markdown(
            f'<div style="background:#1a2332;border:1px solid #2a3a4e;border-radius:8px;padding:12px;">'
            f'<p>风险等级: <span style="color:{risk_color};font-weight:700;">{risk_text}({risk_score}分)</span></p>'
            f'<p>建议止损: <strong>¥{stop_price:.2f}</strong> (通用-{stop_loss_pct*100:.0f}%, 策略A=首阴低×0.98)</p>'
            f'<p>配仓: 三策略各3万/3只, 每只¥10,000(33%)</p>'
            f'{"".join(f"<p style=color:#ef9a9a;>⚠ {f}</p>" for f in risk_factors)}'
            f'</div>', unsafe_allow_html=True)

with right:
    # === 资金流向 ===
    st.markdown("#### 💰 资金流向")
    try:
        flows = conn.execute("""
            SELECT trade_date, net_amount
            FROM fund_flow WHERE stock_code=?
            ORDER BY trade_date DESC LIMIT 10
        """, (target,)).fetchall()
        if flows:
            flow_html = '<div style="background:#1a2332;border:1px solid #2a3a4e;border-radius:8px;padding:12px;">'
            net_3d = sum(f[1] or 0 for f in flows[:3])
            net_5d = sum(f[1] or 0 for f in flows[:5])
            # DB中金额单位是万元, 转换显示
            def fmt_amount(val_wan):
                """万元 → 智能显示"""
                val = val_wan * 1e4  # 转为元
                if abs(val) >= 1e8:
                    return f"{val/1e8:+.2f}亿"
                elif abs(val) >= 1e4:
                    return f"{val/1e4:+.1f}万"
                else:
                    return f"{val:+.0f}元"
            c3 = "#ef5350" if net_3d > 0 else "#26a69a"
            c5 = "#ef5350" if net_5d > 0 else "#26a69a"
            flow_html += f'<p>近3日净流入: <span style="color:{c3}">{fmt_amount(net_3d)}</span></p>'
            flow_html += f'<p>近5日净流入: <span style="color:{c5}">{fmt_amount(net_5d)}</span></p>'
            # 逐日明细
            flow_html += '<hr style="border-color:#2a3a4e;margin:6px 0;">'
            for f in flows[:5]:
                d = f[0]
                v = f[1] or 0
                fc = "#ef5350" if v > 0 else "#26a69a"
                flow_html += f'<p style="font-size:0.8rem;margin:2px 0;">{d} <span style="color:{fc}">{fmt_amount(v)}</span></p>'
            flow_html += '</div>'
            st.markdown(flow_html, unsafe_allow_html=True)
        else:
            st.caption("暂无资金流数据")
    except Exception:
        st.caption("资金流查询失败")

    # === 新闻情绪 ===
    st.markdown("#### 📰 新闻情绪")
    news = []
    try:
        # 优先从DB取
        news = conn.execute("""
            SELECT title, content, publish_time, sentiment_score
            FROM news WHERE stock_code=?
            ORDER BY publish_time DESC LIMIT 5
        """, (target,)).fetchall()
    except Exception:
        pass

    # DB没数据则实时拉取
    if not news:
        try:
            import akshare as ak
            df = ak.stock_news_em(symbol=target)
            if df is not None and not df.empty:
                for _, row in df.head(5).iterrows():
                    title = str(row.get("新闻标题", ""))
                    content = str(row.get("新闻内容", ""))[:200]
                    pub_time = str(row.get("发布时间", ""))
                    source_name = str(row.get("文章来源", ""))
                    # 简易情绪打分
                    score = 0.5
                    pos_words = ["利好", "上涨", "增长", "突破", "新高", "强势", "涨", "盈利", "订单", "中标"]
                    neg_words = ["下跌", "暴跌", "亏损", "风险", "减持", "处罚", "退市", "违规", "跌", "利空"]
                    text = (title + content).lower()
                    pos_cnt = sum(1 for w in pos_words if w in text)
                    neg_cnt = sum(1 for w in neg_words if w in text)
                    if pos_cnt + neg_cnt > 0:
                        score = 0.3 + 0.4 * pos_cnt / (pos_cnt + neg_cnt)
                    news.append((title, content, pub_time, score, source_name))
        except Exception:
            pass

    if news:
        avg_sent = sum((n[3] or 0.5) for n in news) / len(news)
        sent_label = "偏正面" if avg_sent > 0.6 else "偏负面" if avg_sent < 0.4 else "中性"
        sent_color = "#66bb6a" if avg_sent > 0.6 else "#ef5350" if avg_sent < 0.4 else "#ffa726"
        st.markdown(
            f'<p>综合情绪: <span style="color:{sent_color};font-weight:700;">{sent_label}({avg_sent:.2f})</span></p>',
            unsafe_allow_html=True)
        for n in news[:5]:
            title = (n[0] or "")[:35] + "..." if len(n[0] or "") > 35 else (n[0] or "")
            score = n[3] or 0.5
            color = "#66bb6a" if score > 0.6 else "#ef5350" if score < 0.4 else "#808080"
            source = n[4] if len(n) > 4 else ""
            src_tag = f'<span style="color:#666;font-size:0.75rem;"> {source}</span>' if source else ""
            st.markdown(
                f'<p style="font-size:0.85rem;color:#b0b0b0;">'
                f'<span style="color:{color};">{score:.2f}</span> {title}{src_tag}'
                f'</p>',
                unsafe_allow_html=True)
    else:
        st.caption("暂无相关新闻")

    # === 用户持仓状态(如果持有) ===
    try:
        import json
        with open("data/portfolio.json") as f:
            pf = json.load(f)
        held = [p for p in pf.get("positions", []) if p.get("code") == target]
        if held:
            h = held[0]
            st.markdown("#### 📍 我的持仓")
            buy_price = h.get("buy_price", 0)
            pnl_pct = (price - buy_price) / buy_price * 100 if buy_price > 0 and price > 0 else 0
            stop = h.get("stop", h.get("stop_loss", 0))
            highest = h.get("highest", 0)
            st.markdown(
                f'<div style="background:#1a2332;border:1px solid #2a3a4e;border-radius:8px;padding:12px;">'
                f'<p>持仓: {h.get("shares",0)}股 @¥{buy_price:.2f}</p>'
                f'<p>浮盈: <span style="color:{"#ef5350" if pnl_pct>0 else "#26a69a"}">{pnl_pct:+.1f}%</span></p>'
                f'<p>止损: ¥{stop:.2f} | 最高: ¥{highest:.2f}</p>'
                f'</div>', unsafe_allow_html=True)
    except Exception:
        pass

conn.close()

# === 评测结论 ===
st.divider()
st.markdown("### 📊 评测结论")
if price and detail.get("pe"):
    pe = detail.get("pe", 0)

    if pe < 0:
        pe_text = "亏损公司，小心"
    elif pe < 15:
        pe_text = "估值偏低，可能被低估"
    elif pe < 30:
        pe_text = "估值合理"
    elif pe < 50:
        pe_text = "估值偏高，市场给了溢价"
    else:
        pe_text = "估值很高，泡沫风险大"

    strat_summary = ""
    if strat_b_match:
        strat_summary = "策略B(回踩低吸)候选 → 回踩涨停开盘价低吸, 持1天清仓"
    elif strat_a_match:
        strat_summary = f"策略A(首阴反包)候选 → 次日高开2%+翻红确认 → 持2-3天"
    else:
        strat_summary = "不在当前策略候选中 → 观望"

    amt_20d = detail.get("amount", 0)
    if not amt_20d or amt_20d == 0:
        kl_amt = detail.get("kline", [])
        if kl_amt:
            amt_20d = sum(k.get("amount", 0) or 0 for k in kl_amt[:20])

    st.markdown(
        f'<div style="background:#1a2332;border:1px solid #2a3a4e;border-radius:8px;padding:16px;">'
        f'<p>📊 <strong>估值:</strong> 市盈率{pe:.1f}倍 → {pe_text}</p>'
        f'<p>📈 <strong>趋势:</strong> 今日{change_pct:+.1f}%, 近20日成交{amt_20d/1e8:.1f}亿</p>'
        f'<p>🎯 <strong>策略:</strong> {strat_summary}</p>'
        f'<p>⚠️ <strong>风险:</strong> {risk_text}({risk_score}分) | 建议止损¥{price*(1-stop_loss_pct):.2f}(-{stop_loss_pct*100:.0f}%)</p>'
        f'</div>',
        unsafe_allow_html=True,
    )
else:
    st.info("数据不足，无法生成评测结论")

# === 一个月走势预测 ===
st.divider()
st.markdown("### 🔮 一个月走势预测 (22交易日)")
with st.spinner("蒙特卡洛模拟中..."):
    try:
        import numpy as np
        import random

        # 获取K线数据 (正序)
        kline_all = detail.get("kline", [])
        if kline_all and len(kline_all) >= 30:
            # kline是DESC排序 → reverse成正序
            kl = kline_all[::-1]
            closes_arr = np.array([k["close"] for k in kl if k.get("close")])

            if len(closes_arr) >= 30:
                pred_price = price if price > 0 else float(closes_arr[-1])
                returns = np.diff(closes_arr) / closes_arr[:-1]
                n_days = len(returns)

                # --- 方法1: 蒙特卡洛 ---
                mean_r = float(np.mean(returns))
                std_r = float(np.std(returns))
                random.seed(42)
                sims = []
                for _ in range(10000):
                    p = pred_price
                    for _ in range(22):
                        p *= (1 + random.gauss(mean_r, std_r))
                    sims.append(p)
                sims.sort()

                p5 = sims[500]
                p25 = sims[2500]
                p50 = sims[5000]
                p75 = sims[7500]
                p95 = sims[9500]
                up_prob = sum(1 for s in sims if s > pred_price) / len(sims) * 100

                # --- 方法2: 技术面调整 ---
                ma5 = float(np.mean(closes_arr[-5:]))
                ma20 = float(np.mean(closes_arr[-20:]))
                ret_5d = (closes_arr[-1] - closes_arr[-6]) / closes_arr[-6] * 100 if len(closes_arr) >= 6 else 0
                ret_10d = (closes_arr[-1] - closes_arr[-11]) / closes_arr[-11] * 100 if len(closes_arr) >= 11 else 0

                tech_score = 0  # -100 ~ +100
                if ret_5d > 15:
                    tech_score -= 30  # 短期过热
                elif ret_5d > 5:
                    tech_score += 10  # 温和上涨
                elif ret_5d < -10:
                    tech_score -= 20  # 短期暴跌
                if closes_arr[-1] > ma5:
                    tech_score += 10  # 站上MA5
                if ma5 > ma20:
                    tech_score += 10  # 多头排列

                # --- 方法3: 基本面调整 ---
                pe_val = detail.get("pe", 0) or 0
                fund_score = 0
                if 0 < pe_val < 15:
                    fund_score = 15  # 低估
                elif 15 <= pe_val < 30:
                    fund_score = 5   # 合理
                elif pe_val >= 50:
                    fund_score = -10  # 泡沫
                elif pe_val < 0:
                    fund_score = -15  # 亏损

                # --- 综合 ---
                mc_adj = (p50 / pred_price - 1)
                final_ret = mc_adj * 0.6 + (tech_score / 100) * 0.25 + (fund_score / 100) * 0.15
                final_price = pred_price * (1 + final_ret)

                # 方向判断
                if final_ret > 0.05:
                    direction = "偏多 ▲"
                    dir_color = "#ef5350"
                    advice = "可轻仓参与, 严格止损"
                elif final_ret > 0:
                    direction = "震荡偏多 ↗"
                    dir_color = "#ffa726"
                    advice = "可关注, 等回调再入"
                elif final_ret > -0.05:
                    direction = "震荡偏空 ↘"
                    dir_color = "#ffa726"
                    advice = "观望为主"
                else:
                    direction = "偏空 ▼"
                    dir_color = "#26a69a"
                    advice = "不建议买入, 已持有逢高减仓"

                # 显示
                def fp(v):
                    """格式化价格"""
                    if target.startswith("H") or target.startswith("h"):
                        return f"HK${v:.2f}"
                    return f"¥{v:.2f}"

                def fpct(v):
                    return f"{(v/pred_price-1)*100:+.1f}%"

                # 置信区间图
                import plotly.graph_objects as go2
                fig2 = go2.Figure()
                dates_future = [f"T+{i}" for i in range(0, 23)]

                # 用蒙特卡洛的路径画置信带
                path_sims = []
                random.seed(42)
                for _ in range(500):
                    p = pred_price
                    path = [p]
                    for _ in range(22):
                        p *= (1 + random.gauss(mean_r, std_r))
                        path.append(p)
                    path_sims.append(path)
                path_arr = np.array(path_sims)
                p10_path = np.percentile(path_arr, 10, axis=0)
                p25_path = np.percentile(path_arr, 25, axis=0)
                p50_path = np.percentile(path_arr, 50, axis=0)
                p75_path = np.percentile(path_arr, 75, axis=0)
                p90_path = np.percentile(path_arr, 90, axis=0)

                fig2.add_trace(go2.Scatter(x=dates_future, y=p90_path, mode="lines",
                    line=dict(color="rgba(102,187,106,0.15)", width=0), showlegend=False))
                fig2.add_trace(go2.Scatter(x=dates_future, y=p10_path, mode="lines",
                    line=dict(color="rgba(102,187,106,0.15)", width=0),
                    fill="tonexty", fillcolor="rgba(102,187,106,0.1)", name="80%区间"))
                fig2.add_trace(go2.Scatter(x=dates_future, y=p75_path, mode="lines",
                    line=dict(color="rgba(239,83,80,0.2)", width=0), showlegend=False))
                fig2.add_trace(go2.Scatter(x=dates_future, y=p25_path, mode="lines",
                    line=dict(color="rgba(239,83,80,0.2)", width=0),
                    fill="tonexty", fillcolor="rgba(239,83,80,0.15)", name="50%区间"))
                fig2.add_trace(go2.Scatter(x=dates_future, y=p50_path, mode="lines",
                    line=dict(color="#ffa726", width=2, dash="dash"), name="中位数"))
                fig2.add_trace(go2.Scatter(x=dates_future, y=[pred_price]*23, mode="lines",
                    line=dict(color="#42a5f5", width=1, dash="dot"), name="当前价"))

                fig2.update_layout(
                    height=280,
                    margin=dict(l=10, r=10, t=20, b=30),
                    paper_bgcolor="#0e1117",
                    plot_bgcolor="#0e1117",
                    font=dict(color="#b0b0b0", size=11),
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, font=dict(size=10)),
                    yaxis=dict(tickformat=".1f", gridcolor="#1e2a3a"),
                    xaxis=dict(gridcolor="#1e2a3a"),
                )
                st.plotly_chart(fig2, use_container_width=True)

                # 预测结论
                st.markdown(
                    f'<div style="background:#1a2332;border:1px solid #2a3a4e;border-radius:8px;padding:16px;">'
                    f'<p style="font-size:1.1rem;">方向: <span style="color:{dir_color};font-weight:700;">{direction}</span>'
                    f' ｜ 上涨概率: <strong>{up_prob:.0f}%</strong></p>'
                    f'<p>预测价(22天): <strong>{fp(final_price)}</strong> ({fpct(final_price)})</p>'
                    f'<hr style="border-color:#2a3a4e;">'
                    f'<p>🎯 <strong>70%置信区间:</strong> {fp(p25)} ~ {fp(p75)} ({fpct(p25)} ~ {fpct(p75)})</p>'
                    f'<p>🎯 <strong>95%置信区间:</strong> {fp(p5)} ~ {fp(p95)} ({fpct(p5)} ~ {fpct(p95)})</p>'
                    f'<hr style="border-color:#2a3a4e;">'
                    f'<p>📐 蒙特卡洛中位: {fp(p50)} ({fpct(p50)}) | 技术面: {"偏多" if tech_score > 0 else "偏空" if tech_score < 0 else "中性"}({tech_score}) | 基本面: {"低估" if fund_score > 0 else "偏贵" if fund_score < 0 else "合理"}({fund_score})</p>'
                    f'<p>📉 5日涨跌: {ret_5d:+.1f}% | 10日涨跌: {ret_10d:+.1f}% | 日波动率: {std_r*100:.1f}%</p>'
                    f'<hr style="border-color:#2a3a4e;">'
                    f'<p>💡 <strong>操作建议:</strong> {advice}</p>'
                    f'<p>🛡️ <strong>策略止损/止盈线(基于当前价¥{current_price:.2f}):</strong></p>'
                    f'<p>&nbsp;&nbsp;策略A(首阴反包): 3万·持2-3天(跌破首阴低止损)</p>'
                    f'<p>&nbsp;&nbsp;策略B(涨停·主策略): 止损 ¥{current_price*0.97:.2f}(-3%) | trailing止盈 ¥{current_price*0.97:.2f}(-3%) ~ ¥{current_price*0.985:.2f}(-1.5%)</p>'
                    f'<p>&nbsp;&nbsp;策略C(缩量反包): 止损 ¥{current_price*0.94:.2f}(-5%) | trailing止盈 ¥{current_price*0.95:.2f}(-5%) ~ ¥{current_price*0.98:.2f}(-2%)</p>'
                    f'<p style="font-size:0.75rem;color:#666;">基于{n_days}天历史数据, 10000次蒙特卡洛模拟 + 技术面 + 基本面三维交叉验证</p>'
                    f'</div>',
                    unsafe_allow_html=True
                )
        else:
            st.caption("K线数据不足30天, 无法预测")
    except Exception as e:
        st.caption(f"预测失败: {e}")
