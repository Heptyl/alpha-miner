"""Alpha Miner v3 — 统一入口

10页结构:
  1. 系统导航   — 版本日志 + 使用教程 + 架构说明
  2. Dashboard  — 首页全景(指数+持仓+情绪)
  3. 盘中交易   — 模拟盘操作+三策略执行
  4. 实盘监控   — 5只持仓盯盘
  5. 新闻热点   — 新闻情绪+策略联动
  6. 选股中心   — 三策略候选+9维选股
  7. 股票评测   — 单只全景分析
  8. 复盘日志   — 每日复盘+统计
  9. 因子看板   — IC+漂移检测
  10.系统设置   — 持仓+参数+数据+日志
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

import streamlit as st
from web.styles import inject_styles

inject_styles()

pages = [
    st.Page("pages_new/0_guide.py",         title="系统导航",  icon="📖"),
    st.Page("pages_new/1_dashboard.py",     title="Dashboard", icon="📊"),
    st.Page("pages_new/2_trading.py",       title="盘中交易",  icon="⚡"),
    st.Page("pages_new/3_monitor.py",       title="实盘监控",  icon="👁"),
    st.Page("pages_new/4_news.py",          title="新闻热点",  icon="📰"),
    st.Page("pages_new/5_stock_picker.py",  title="选股中心",  icon="🎯"),
    st.Page("pages_new/6_evaluator.py",     title="股票评测",  icon="🔍"),
    st.Page("pages_new/7_review.py",        title="复盘日志",  icon="📋"),
    st.Page("pages_new/8_factors.py",       title="因子看板",  icon="📈"),
    st.Page("pages_new/9_settings.py",      title="系统设置",  icon="⚙️"),
]

pg = st.navigation(pages)
pg.run()
