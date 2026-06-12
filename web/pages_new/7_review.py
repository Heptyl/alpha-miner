"""复盘日志 — 每日复盘 + 策略对比 + 收益曲线 + 实盘持仓"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import json
from datetime import datetime

from web.styles import inject_styles
from web.components import metric_card, pnl_color
from web.services.data_service import get_sim_trades, get_sim_account, get_sim_positions, _conn

inject_styles()

st.markdown("## 📝 复盘日志")

# ============================================================
# Section 1: 实盘持仓概览(用户最关心的)
# ============================================================
st.markdown("### 💼 实盘持仓概览")

try:
    with open("data/portfolio.json") as f:
        pf = json.load(f)
    positions = pf.get("positions", [])

    if positions:
        # 获取实时行情
        codes = [p["code"] for p in positions]
        try:
            from src.trader.realtime_quote import get_realtime
            quotes = get_realtime(codes)
        except Exception:
            quotes = {}

        total_mv = 0
        total_cost = 0
        total_pnl = 0

        for p in positions:
            q = quotes.get(p["code"], {})
            price = q.get("price", 0)
            buy_price = p.get("buy_price", 0)
            shares = p.get("shares", 0)
            stop = p.get("stop", p.get("stop_loss", 0))
            highest = p.get("highest", 0)

            mv = price * shares if price > 0 else buy_price * shares
            cost = buy_price * shares
            pnl = (price - buy_price) * shares if price > 0 else 0
            pnl_pct = (price / buy_price - 1) * 100 if buy_price > 0 and price > 0 else 0
            total_mv += mv
            total_cost += cost
            total_pnl += pnl

            # 回撤计算
            drawdown = (highest - price) / highest * 100 if highest > 0 and price > 0 else 0

            color = "#ef5350" if pnl_pct >= 0 else "#26a69a"
            dd_color = "#ffa726" if drawdown > 2 else "#66bb6a"

            st.markdown(
                f'<div style="background:#1a2332;border:1px solid #2a3a4e;border-radius:6px;padding:10px;margin:4px 0;">'
                f'<span style="font-weight:700;">{p.get("name","?")} ({p["code"]})</span> '
                f'<span style="color:{color};font-weight:700;">{pnl_pct:+.1f}%</span> '
                f'<span style="color:#808080;">¥{price:.2f}</span> '
                f'<span style="color:#808080;">| 持{shares}股@¥{buy_price:.2f}</span> '
                f'<span style="color:#808080;">| 浮盈<span style="color:{color};">¥{pnl:+,.0f}</span></span> '
                f'<span style="color:#808080;">| 回撤<span style="color:{dd_color};">{drawdown:.1f}%</span></span> '
                f'<span style="color:#808080;">| 止损¥{stop:.2f}</span>'
                f'</div>',
                unsafe_allow_html=True,
            )

        total_pct = (total_mv / total_cost - 1) * 100 if total_cost > 0 else 0
        tc = "#ef5350" if total_pct >= 0 else "#26a69a"
        st.markdown(
            f'<div style="background:#1a2332;border:1px solid #42a5f5;border-radius:8px;padding:12px;margin-top:8px;">'
            f'<strong>总市值:</strong> ¥{total_mv:,.0f} | '
            f'<strong>总浮盈:</strong> <span style="color:{tc};font-weight:700;">¥{total_pnl:+,.0f} ({total_pct:+.1f}%)</span>'
            f'</div>',
            unsafe_allow_html=True,
        )
    else:
        st.info("portfolio.json无持仓数据")
except Exception as e:
    st.caption(f"实盘持仓读取失败: {e}")

st.divider()

# ============================================================
# Section 2: 复盘报告
# ============================================================
report_dir = Path(__file__).parent.parent.parent / "output" / "reports"
reports = sorted(report_dir.glob("daily_review_*.md"), reverse=True) if report_dir.exists() else []

if reports:
    dates = [r.stem.replace("daily_review_", "") for r in reports]
    selected = st.selectbox("选择日期", dates, index=0, key="review_date")
    report_file = report_dir / f"daily_review_{selected}.md"
    if report_file.exists():
        content = report_file.read_text()
        st.markdown(content)
else:
    st.info("暂无复盘报告，每天收盘后自动生成")

st.divider()

# ============================================================
# Section 3: 模拟盘收益曲线
# ============================================================
st.markdown("### 📈 模拟盘收益曲线")

conn = _conn()
try:
    # 从daemon_account读历史净值
    acct_rows = conn.execute("""
        SELECT date, cash, total_assets FROM daemon_account
        WHERE period=(SELECT MAX(period) FROM daemon_account) AND date <= date('now')
        ORDER BY date
    """).fetchall()

    if acct_rows and len(acct_rows) > 1:
        dates = [r[0] for r in acct_rows]
        assets = [r[2] for r in acct_rows]
        initial = assets[0] if assets else 90000  # fallback, 正常从DB读取
        returns = [(a / initial - 1) * 100 for a in assets]

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=dates, y=assets,
            mode="lines+markers",
            name="总资产",
            line=dict(color="#42a5f5", width=2),
            marker=dict(size=4),
            yaxis="y",
        ))
        # 收益率百分比(右侧Y轴)
        fig.add_trace(go.Scatter(
            x=dates, y=returns,
            mode="lines",
            name="收益率%",
            line=dict(color="#26a69a", width=1.5, dash="dot"),
            yaxis="y2",
        ))

        # 起始线
        if assets:
            fig.add_hline(y=assets[0], line_dash="dash", line_color="#808080",
                          annotation_text=f"起始 ¥{assets[0]:,.0f}")

        fig.update_layout(
            height=300, margin=dict(l=50, r=60, t=20, b=30),
            paper_bgcolor="#0f1923", plot_bgcolor="#0f1923",
            font_color="#808080",
            yaxis_title="总资产(¥)",
            yaxis2=dict(title="收益率%", overlaying="y", side="right", tickformat=".1f"),
            legend=dict(orientation="h", y=1.12),
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.caption("模拟盘历史不足, 无法画收益曲线")
except Exception as e:
    st.caption(f"收益曲线: {e}")

# ============================================================
# Section 4: 交易统计 + 策略对比
# ============================================================
st.markdown("### 📊 模拟盘累计统计")
trades = get_sim_trades(999)
if trades:
    df = pd.DataFrame(trades)

    total = len(df)
    buys = len(df[df["action"] == "buy"])
    sells = len(df[df["action"] == "sell"])

    sell_trades = df[df["action"] == "sell"]
    wins = len(sell_trades[sell_trades.get("pnl", pd.Series([0])) > 0]) if "pnl" in sell_trades.columns else 0
    losses = len(sell_trades[sell_trades.get("pnl", pd.Series([0])) <= 0]) if "pnl" in sell_trades.columns else 0
    win_rate = wins / (wins + losses) * 100 if (wins + losses) > 0 else 0

    cols = st.columns(5)
    with cols[0]: metric_card("总交易", f'{total}笔', f'买{buys} 卖{sells}', True)
    with cols[1]: metric_card("胜率", f'{win_rate:.0f}%', f'赢{wins} 亏{losses}', win_rate >= 50)
    with cols[2]:
        acct = get_sim_account()
        metric_card("总收益", f'¥{acct["pnl"]:+,.0f}', f'{acct["pnl_pct"]:+.1f}%', acct["pnl"] >= 0)
    with cols[3]:
        positions = get_sim_positions()
        metric_card("持仓数", f'{len(positions)}只', f'A:3只+B:3只+C:3只', len(positions) <= 8)
    with cols[4]:
        if "strategy" in df.columns:
            strategies = df["strategy"].value_counts().to_dict()
            st_info = " ".join(f'{k}:{v}' for k, v in strategies.items())
            metric_card("策略分布", st_info or "N/A", "", True)
        else:
            metric_card("策略", "N/A", "", True)

    # 策略差异化统计
    if "strategy" in df.columns:
        st.markdown("### 📊 策略对比")
        strat_cols = st.columns(3)
        for i, strat in enumerate(["A", "B", "C"]):
            with strat_cols[i]:
                strat_name = {"A": "策略A · 龙头首阴", "B": "策略B · 回踩低吸", "C": "策略C · 缩量反包"}[strat]
                st.markdown(f'<div style="color:#42a5f5;font-size:0.9rem;font-weight:600;">{strat_name}</div>', unsafe_allow_html=True)
                strat_sells = sell_trades[sell_trades.get("strategy", pd.Series()) == strat] if "strategy" in sell_trades.columns else pd.DataFrame()
                if len(strat_sells) > 0:
                    s_wins = len(strat_sells[strat_sells.get("pnl", pd.Series([0])) > 0])
                    s_total = len(strat_sells)
                    s_wr = s_wins / s_total * 100 if s_total > 0 else 0
                    s_pnl = strat_sells.get("pnl", pd.Series([0])).sum()
                    s_cls = pnl_color(s_pnl)
                    st.markdown(
                        f'<div style="font-size:0.85rem;">'
                        f'交易{s_total}笔 | 胜率{s_wr:.0f}% | '
                        f'<span class="{s_cls}">盈亏¥{s_pnl:+,.0f}</span></div>',
                        unsafe_allow_html=True,
                    )
                else:
                    st.caption("暂无已平仓交易")

    # 策略淘汰评估
    st.markdown("### 🏁 策略淘汰评估")
    try:
        from src.strategy.elimination import generate_elimination_report
        elim_report = generate_elimination_report()
        for s in elim_report["strategies"]:
            icon = {"active": "🟢", "yellow_card": "🟡", "red_card": "🔴"}.get(s["status"], "⚪")
            eval_tag = "✓可评估" if s["evaluable"] else f"样本{s['closed_trades']}/20"
            pf_val = s["profit_factor"] if isinstance(s["profit_factor"], str) else f"{s['profit_factor']:.2f}"
            st.markdown(
                f'<div style="font-size:0.85rem;padding:4px 0;">'
                f'{icon} <b>{s["name"]}</b>({s["label"]}) '
                f'<span style="color:#808080;">{eval_tag}</span> '
                f'| {s["closed_trades"]}笔 | 胜率{s["win_rate"]:.0%} | PF={pf_val} | '
                f'<span class="{pnl_color(s["total_pnl"])}">pnl={s["total_pnl"]:+.0f}</span> '
                f'| {s["recommendation"]}'
                f'</div>',
                unsafe_allow_html=True,
            )
    except Exception as e:
        st.caption(f"淘汰评估: {e}")

    # 盈亏分布直方图
    if "pnl" in sell_trades.columns and len(sell_trades) > 0:
        st.markdown("### 📊 盈亏分布")
        fig2 = go.Figure()
        profit_pnl = sell_trades[sell_trades["pnl"] > 0]["pnl"]
        loss_pnl = sell_trades[sell_trades["pnl"] <= 0]["pnl"]
        if len(profit_pnl) > 0:
            fig2.add_trace(go.Histogram(x=profit_pnl, name="盈利", marker_color="#ef5350", nbinsx=15))
        if len(loss_pnl) > 0:
            fig2.add_trace(go.Histogram(x=loss_pnl, name="亏损", marker_color="#26a69a", nbinsx=15))
        fig2.add_vline(x=0, line_dash="dash", line_color="#808080")
        fig2.update_layout(
            height=250, margin=dict(l=50, r=20, t=20, b=30),
            paper_bgcolor="#0f1923", plot_bgcolor="#0f1923",
            font_color="#808080",
            xaxis_title="盈亏(¥)",
        )
        st.plotly_chart(fig2, use_container_width=True)

    # 数据验证清单
    st.markdown("### ✅ 数据验证清单")
    checks = []
    today = datetime.now().strftime("%Y-%m-%d")
    cnt = conn.execute("SELECT COUNT(*) FROM daily_price WHERE trade_date=?", (today,)).fetchone()[0]
    checks.append(("日K线完整性", f"{cnt}只", cnt >= 5000))
    zt_cnt = conn.execute("SELECT COUNT(*) FROM zt_pool WHERE trade_date=?", (today,)).fetchone()[0]
    checks.append(("涨停池", f"{zt_cnt}只", zt_cnt >= 0))
    news_cnt = conn.execute("SELECT COUNT(*) FROM news WHERE date(publish_time)=?", (today,)).fetchone()[0]
    checks.append(("今日新闻", f"{news_cnt}条", news_cnt > 0))
    acc = get_sim_account()
    held_val = conn.execute("SELECT COALESCE(SUM(shares*buy_price),0) FROM daemon_positions WHERE status='held' AND period=(SELECT MAX(period) FROM daemon_account)").fetchone()[0]
    diff = abs(acc["cash"] + held_val - acc["total"])
    checks.append(("账户对账", f"差{diff:.0f}元", diff < acc["total"] * 0.05))

    for name, val, ok in checks:
        icon = "✅" if ok else "❌"
        color = "#26a69a" if ok else "#ef5350"
        st.markdown(
            f'<div style="font-size:0.8rem;padding:2px 0;">'
            f'{icon} <span style="color:{color};">{name}: {val}</span></div>',
            unsafe_allow_html=True,
        )

    # 最近交易明细
    st.markdown("### 📋 最近交易")
    display_cols = ["trade_date", "action", "code", "name", "price", "shares", "reason"]
    available_cols = [c for c in display_cols if c in df.columns]
    if available_cols:
        st.dataframe(df[available_cols].tail(30), use_container_width=True, hide_index=True)
else:
    st.info("暂无交易记录")

try:
    conn.close()
except Exception:
    pass
