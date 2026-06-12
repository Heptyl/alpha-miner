"""共享UI组件 — 统一渲染

使用: from web.components import metric_card, position_row, ...
"""

import streamlit as st


def metric_card(label: str, value: str, sub: str = "", up: bool = True):
    """指标卡片"""
    cls = "up" if up else "down"
    sub_html = f'<div class="sub {cls}">{sub}</div>' if sub else ""
    st.markdown(
        f'<div class="am-metric">'
        f'<div class="label">{label}</div>'
        f'<div class="value {cls}">{value}</div>'
        f'{sub_html}'
        f'</div>',
        unsafe_allow_html=True,
    )


def position_row(code: str, name: str, shares: int, cost: float,
                 price: float, pnl: float, pnl_pct: float,
                 change_pct: float = 0, stop_loss: float = 0,
                 industry: str = ""):
    """持仓行"""
    pnl_cls = "up" if pnl >= 0 else "down"
    chg_cls = "up" if change_pct >= 0 else "down"
    stop_html = ""
    if stop_loss and price <= stop_loss * 1.05:
        stop_html = f'<span class="am-tag am-tag-warning">接近止损 {stop_loss:.2f}</span>'

    st.markdown(
        f'<div class="am-position">'
        f'<div><div class="name">{name}</div><div class="code">{code}</div></div>'
        f'<div><div class="price {chg_cls}">¥{price:.2f}</div>'
        f'<div class="detail {chg_cls}">{change_pct:+.1f}%</div></div>'
        f'<div><div class="detail">持仓 {shares}股</div>'
        f'<div class="detail">成本 ¥{cost:.2f}</div></div>'
        f'<div><div class="pnl {pnl_cls}">{pnl:+.0f}</div>'
        f'<div class="detail {pnl_cls}">{pnl_pct:+.1f}%</div></div>'
        f'<div>{stop_html}'
        f'<span class="am-sector">{industry}</span></div>'
        f'</div>',
        unsafe_allow_html=True,
    )


def trade_card(action: str, code: str, name: str, shares: int,
               price: float, reason: str, time: str = "",
               strategy: str = "", pnl: float = None):
    """交易记录卡片"""
    cls = "buy" if action == "buy" else "sell"
    icon = "🟢" if action == "buy" else "🔴"
    action_text = "买入" if action == "buy" else "卖出"
    pnl_html = ""
    if pnl is not None:
        pnl_cls = "up" if pnl >= 0 else "down"
        pnl_html = f'<span class="pnl {pnl_cls}">盈亏 {pnl:+.0f}</span>'
    strategy_html = f'<span class="am-tag am-tag-accent">策略{strategy}</span>' if strategy else ""

    st.markdown(
        f'<div class="am-trade-card {cls}">'
        f'{icon} <strong>{action_text}</strong> {name}({code}) '
        f'{shares}股@¥{price:.2f} '
        f'{strategy_html} {pnl_html}'
        f'<div class="detail" style="margin-top:4px">{reason}</div>'
        f'<div class="detail">{time}</div>'
        f'</div>',
        unsafe_allow_html=True,
    )


def signal_tag(signal_type: str):
    """信号标签"""
    mapping = {
        "breakout": ("放量突破", "am-signal-breakout"),
        "reversal": ("回踩支撑", "am-signal-reversal"),
        "momentum": ("超跌反弹", "am-signal-momentum"),
        "板块补涨": ("板块补涨", "am-signal-board"),
        "涨停低吸": ("涨停低吸", "am-signal-confirm"),
    }
    text, cls = mapping.get(signal_type, (signal_type, "am-signal-board"))
    return f'<span class="am-signal {cls}">{text}</span>'


def status_dot(running: bool):
    """状态指示灯"""
    cls = "running" if running else "stopped"
    text = "运行中" if running else "已停止"
    return f'<span class="am-status-dot {cls}"></span>{text}'


def fmt_pct(pct) -> str:
    """格式化涨跌幅"""
    if not isinstance(pct, (int, float)):
        return "--"
    return f"{pct:+.2f}%"


def fmt_price(p) -> str:
    """格式化价格"""
    if not isinstance(p, (int, float)) or p == 0:
        return "--"
    return f"¥{p:.2f}"


def pnl_color(pnl) -> str:
    """盈亏颜色类"""
    return "up" if (isinstance(pnl, (int, float)) and pnl >= 0) else "down"
