"""Alpha Miner 统一样式 — TradingView风格深色主题

使用: from web.styles import inject_styles
在页面开头调用 inject_styles() 即可
"""


def inject_styles():
    """注入全局CSS"""
    import streamlit as st
    st.markdown(STYLES, unsafe_allow_html=True)


STYLES = """
<style>
/* ═══════════════════════════════════════════
   Alpha Miner Design System v3
   参考 TradingView / 同花顺 深色主题
   ═══════════════════════════════════════════ */

/* ── 全局 ── */
:root {
    --bg-primary: #0f1923;
    --bg-card: #1a2332;
    --bg-card-hover: #1f2b3d;
    --border: #2a3a4e;
    --text-primary: #e0e0e0;
    --text-secondary: #808080;
    --text-muted: #5a5a5a;
    --up: #ef5350;
    --up-bg: #ef535020;
    --down: #26a69a;
    --down-bg: #26a69a20;
    --accent: #42a5f5;
    --accent-bg: #42a5f520;
    --warning: #ffd93d;
    --danger: #ef5350;
    --success: #26a69a;
}

.block-container {
    padding-top: 1rem;
    padding-bottom: 0;
    max-width: 1400px;
}

[data-testid="stSidebar"] {
    background: var(--bg-primary) !important;
    border-right: 1px solid var(--border);
}

[data-testid="stSidebarNav"] {
    padding-top: 0.5rem;
}

/* ── 指标卡片 ── */
.am-metric {
    background: var(--bg-card);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 12px 16px;
    text-align: center;
    transition: background 0.2s;
}
.am-metric:hover { background: var(--bg-card-hover); }
.am-metric .label {
    font-size: 0.75rem;
    color: var(--text-secondary);
    margin-bottom: 4px;
    text-transform: uppercase;
    letter-spacing: 0.5px;
}
.am-metric .value {
    font-size: 1.4rem;
    font-weight: 700;
    color: var(--text-primary);
}
.am-metric .sub {
    font-size: 0.8rem;
    margin-top: 2px;
}

/* ── 涨跌色 ── */
.up { color: var(--up) !important; font-weight: 600; }
.down { color: var(--down) !important; font-weight: 600; }
.up-bg { background: var(--up-bg); }
.down-bg { background: var(--down-bg); }

/* ── 持仓行 ── */
.am-position {
    background: var(--bg-card);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 12px 16px;
    margin: 4px 0;
    display: flex;
    align-items: center;
    gap: 16px;
}
.am-position .name {
    font-weight: 600;
    font-size: 0.95rem;
    color: var(--text-primary);
    min-width: 80px;
}
.am-position .code {
    font-size: 0.75rem;
    color: var(--text-secondary);
}
.am-position .price {
    font-size: 1.1rem;
    font-weight: 700;
}
.am-position .pnl {
    font-size: 0.9rem;
    font-weight: 600;
}
.am-position .detail {
    font-size: 0.75rem;
    color: var(--text-secondary);
}

/* ── 交易卡片 ── */
.am-trade-card {
    background: var(--bg-card);
    border-left: 3px solid var(--accent);
    border-radius: 8px;
    padding: 12px 16px;
    margin: 4px 0;
}
.am-trade-card.buy { border-left-color: var(--up); }
.am-trade-card.sell { border-left-color: var(--down); }

/* ── 标签 ── */
.am-tag {
    display: inline-block;
    padding: 2px 8px;
    border-radius: 4px;
    font-size: 0.7rem;
    font-weight: 600;
    margin: 0 2px;
}
.am-tag-up { background: var(--up-bg); color: var(--up); border: 1px solid var(--up); }
.am-tag-down { background: var(--down-bg); color: var(--down); border: 1px solid var(--down); }
.am-tag-accent { background: var(--accent-bg); color: var(--accent); border: 1px solid var(--accent); }
.am-tag-warning { background: #ffd93d20; color: var(--warning); border: 1px solid var(--warning); }

/* ── 板块标签 ── */
.am-sector {
    display: inline-block;
    padding: 3px 10px;
    border-radius: 12px;
    font-size: 0.75rem;
    background: var(--bg-card);
    border: 1px solid var(--border);
    color: var(--accent);
    margin: 2px;
}

/* ── 顶栏 ── */
.am-topbar {
    background: var(--bg-card);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 8px 16px;
    margin-bottom: 12px;
    display: flex;
    align-items: center;
    gap: 20px;
}

/* ── 状态指示灯 ── */
.am-status-dot {
    width: 8px;
    height: 8px;
    border-radius: 50%;
    display: inline-block;
    margin-right: 4px;
}
.am-status-dot.running { background: var(--success); box-shadow: 0 0 6px var(--success); }
.am-status-dot.stopped { background: var(--danger); }
.am-status-dot.unknown { background: var(--warning); }

/* ── 信号标签 ── */
.am-signal {
    display: inline-block;
    padding: 2px 8px;
    border-radius: 4px;
    font-size: 0.7rem;
    font-weight: 600;
}
.am-signal-breakout { background: #ff980020; color: #ff9800; border: 1px solid #ff9800; }
.am-signal-reversal { background: #4caf5020; color: #4caf50; border: 1px solid #4caf50; }
.am-signal-momentum { background: #9c27b020; color: #9c27b0; border: 1px solid #9c27b0; }
.am-signal-board { background: #2196f320; color: #2196f3; border: 1px solid #2196f3; }
.am-signal-confirm { background: #00bcd420; color: #00bcd4; border: 1px solid #00bcd4; }

/* ── 隐藏Streamlit默认元素 ── */
#MainMenu { visibility: hidden; }
footer { visibility: hidden; }
header { visibility: hidden; }

/* ── 表格 ── */
.am-table {
    width: 100%;
    border-collapse: collapse;
    font-size: 0.85rem;
}
.am-table th {
    background: var(--bg-primary);
    color: var(--text-secondary);
    padding: 8px 12px;
    text-align: left;
    font-weight: 600;
    font-size: 0.75rem;
    text-transform: uppercase;
    letter-spacing: 0.5px;
    border-bottom: 2px solid var(--border);
}
.am-table td {
    padding: 8px 12px;
    border-bottom: 1px solid var(--border);
    color: var(--text-primary);
}
.am-table tr:hover td {
    background: var(--bg-card-hover);
}
</style>
"""
