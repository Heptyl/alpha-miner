"""实盘监控 — 用户5只真实持仓的实时盯盘

实时行情 + 盈亏 + 止损线 + 分时图 + 新闻
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import streamlit as st
import plotly.graph_objects as go
from datetime import datetime

from web.styles import inject_styles
from web.components import metric_card, position_row, fmt_pct, fmt_price, pnl_color
from web.services.data_service import get_portfolio_realtime, get_news, get_system_status

inject_styles()


def _get_cash():
    """从portfolio.json读取用户现金，而非硬编码"""
    try:
        from src.config.portfolio import _load_raw
        raw = _load_raw()
        return raw.get("cash", 10189)
    except Exception:
        return 10189

# === 自动刷新 ===
now = datetime.now()
is_trading = (now.weekday() < 5 and
              ((now.hour == 9 and now.minute >= 30) or now.hour == 10 or
               (now.hour == 11 and now.minute <= 30) or
               now.hour in (13, 14) or (now.hour == 15 and now.minute == 0)))

if is_trading:
    try:
        from streamlit_autorefresh import st_autorefresh
        st_autorefresh(interval=15000, key="monitor_refresh")
    except ImportError:
        pass

st.markdown("## 👁 实盘监控")

# 总资产概览
portfolio = get_portfolio_realtime()
if portfolio:
    total_mv = sum(p.get("price", 0) * p.get("shares", 0) for p in portfolio)
    total_cost = sum(p.get("cost", 0) * p.get("shares", 0) for p in portfolio)
    total_pnl = total_mv - total_cost
    pnl_cls = pnl_color(total_pnl)

    cols = st.columns(5)
    with cols[0]:
        metric_card("总市值", f'¥{total_mv:,.0f}', "", True)
    with cols[1]:
        metric_card("总成本", f'¥{total_cost:,.0f}', "", True)
    with cols[2]:
        metric_card("总浮盈", f'¥{total_pnl:+,.0f}', f'{(total_pnl/total_cost*100) if total_cost else 0:+.1f}%', total_pnl >= 0)
    with cols[3]:
        cash = _get_cash()
        metric_card("可用现金", f'¥{cash:,.0f}', "", True)
    with cols[4]:
        metric_card("总资产", f'¥{(total_mv + cash):,.0f}', "", True)

    st.divider()

    # 每只持仓详情卡片
    for p in portfolio:
        code = p["code"]
        name = p["name"]
        price = p.get("price", 0)
        cost = p.get("cost", 0)
        shares = p.get("shares", 0)
        pnl = p.get("pnl", 0)
        pnl_pct = p.get("pnl_pct", 0)
        change_pct = p.get("change_pct", 0)
        stop_loss = p.get("stop_loss", 0)
        industry = p.get("industry", "")

        pnl_cls = pnl_color(pnl)
        chg_cls = pnl_color(change_pct)

        # 风险等级
        if price <= stop_loss * 1.02:
            risk = "🔴 危险"
            risk_color = "#ef5350"
        elif price <= stop_loss * 1.05:
            risk = "🟡 警告"
            risk_color = "#ffd93d"
        else:
            risk = "🟢 安全"
            risk_color = "#26a69a"

        # 离止损距离
        stop_dist = (price - stop_loss) / price * 100 if price else 0

        # 移动止盈线(从高点回落, 按策略差异化)
        highest = p.get("highest", price)
        strategy = p.get("strategy", "")
        trailing_pct = {"B": 0.03, "C": 0.05}.get(strategy, 0.03)  # A:3%, B:3%, C:5%
        trailing_stop = highest * (1 - trailing_pct) if highest else None
        trailing_tag = ""
        if trailing_stop and price > 0:
            trailing_dist = (price - trailing_stop) / price * 100
            if trailing_dist < 1:
                trailing_tag = '<span style="color:#ef5350;font-size:0.7rem;">⚠️接近止盈线</span>'
            else:
                trailing_tag = f'<span style="color:#607d8b;font-size:0.7rem;">止盈¥{trailing_stop:.2f}(距{trailing_dist:.1f}%)</span>'

        # 策略标签
        strategy_label = f' 策略{strategy}' if strategy else ''
        strategy_cls = {"A": "am-tag-warning", "B": "am-tag-accent", "C": "am-tag-info"}.get(strategy, "am-tag-warning")
        strategy_tag = f' <span class="am-tag {strategy_cls}">{strategy_label.strip()}</span>' if strategy else ''

        with st.container():
            st.markdown(
                f'<div style="background:#1a2332;border:1px solid #2a3a4e;border-radius:10px;'
                f'padding:16px;margin:8px 0;">'
                f'<div style="display:flex;justify-content:space-between;align-items:center;">'
                f'<div>'
                f'<span style="font-size:1.2rem;font-weight:700;color:#e0e0e0;">{name}</span> '
                f'<span style="color:#808080;">{code}</span> '
                f'<span class="am-sector">{industry}</span>'
                f'{strategy_tag}'
                f'</div>'
                f'<div style="text-align:right;">'
                f'<div style="font-size:1.5rem;font-weight:700;" class="{chg_cls}">¥{price:.2f}</div>'
                f'<div class="{chg_cls}" style="font-size:0.9rem;">{change_pct:+.2f}%</div>'
                f'</div></div>'
                f'<div style="display:flex;gap:24px;margin-top:10px;font-size:0.85rem;">'
                f'<span>持仓 <strong>{shares}</strong>股</span>'
                f'<span>成本 <strong>¥{cost:.2f}</strong></span>'
                f'<span class="{pnl_cls}">浮盈 <strong>{pnl:+,.0f}</strong> ({pnl_pct:+.1f}%)</span>'
                f'<span>止损 <strong>¥{stop_loss:.2f}</strong> (距{stop_dist:.1f}%)</span>'
                f'<span style="color:{risk_color};">{risk}</span>'
                f'</div>'
                f'<div style="font-size:0.8rem;color:#808080;margin-top:2px;">{trailing_tag}</div>'
                f'</div>',
                unsafe_allow_html=True,
            )

            # 止损距离进度条
            if stop_loss and price:
                # 止损距离 = (当前价 - 止损价) / 当前价
                safe_pct = min(max((price - stop_loss) / price * 100, 0), 20)
                bar_pct = safe_pct / 20 * 100
                bar_color = "#ef5350" if safe_pct < 3 else ("#ffd93d" if safe_pct < 5 else "#26a69a")
                st.markdown(
                    f'<div style="margin:0 16px 8px;">'
                    f'<div style="height:4px;background:#2a3a4e;border-radius:2px;">'
                    f'<div style="height:100%;width:{bar_pct}%;background:{bar_color};border-radius:2px;"></div>'
                    f'</div></div>',
                    unsafe_allow_html=True,
                )

        # K线图(懒加载)
        with st.expander(f"📈 {name} K线图", expanded=False):
            try:
                from web.services.data_service import get_stock_detail
                detail = get_stock_detail(code)
                kline = detail.get("kline", [])
                if kline:
                    import pandas as pd
                    df = pd.DataFrame(kline)
                    df = df.sort_values("trade_date")

                    fig = go.Figure()
                    fig.add_trace(go.Candlestick(
                        x=df["trade_date"],
                        open=df["open"], high=df["high"],
                        low=df["low"], close=df["close"],
                        increasing_line_color="#ef5350",
                        decreasing_line_color="#26a69a",
                    ))
                    # 止损线
                    if stop_loss:
                        fig.add_hline(
                            y=stop_loss, line_dash="dash",
                            line_color="#ef5350",
                            annotation_text=f"止损 ¥{stop_loss}",
                        )
                    # 成本线
                    if cost:
                        fig.add_hline(
                            y=cost, line_dash="dot",
                            line_color="#ffd93d",
                            annotation_text=f"成本 ¥{cost:.2f}",
                        )
                    fig.update_layout(
                        height=300, margin=dict(l=40, r=20, t=20, b=30),
                        paper_bgcolor="#0f1923",
                        plot_bgcolor="#0f1923",
                        font_color="#808080",
                        xaxis_rangeslider_visible=False,
                    )
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.caption("暂无K线数据")
            except Exception as e:
                st.caption(f"加载失败: {e}")

    st.divider()

    # 新闻关联
    st.markdown("### 📰 相关新闻")
    news = get_news(10)
    if news:
        for n in news:
            sent_cls = "up" if n.get("sentiment") == "正面" else ("down" if n.get("sentiment") == "负面" else "")
            st.markdown(
                f'<div style="padding:6px 0;border-bottom:1px solid #2a3a4e;">'
                f'<span style="font-size:0.9rem;">{n["title"]}</span><br>'
                f'<span style="font-size:0.7rem;color:#808080;">{n.get("time","")} '
                f'<span class="{sent_cls}">{n.get("sentiment","")}</span> '
                f'{n.get("source","")}</span></div>',
                unsafe_allow_html=True,
            )
    else:
        st.caption("暂无新闻")

else:
    st.info("暂无持仓数据，请在系统设置中配置持仓")

# 刷新按钮
if st.button("🔄 刷新实时数据", key="monitor_refresh_btn"):
    st.cache_data.clear()
    st.rerun()
