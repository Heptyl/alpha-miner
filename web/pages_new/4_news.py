"""新闻热点 v5 — 交易员全视角 (v3适配版)

页面结构:
  1. 🌍 隔夜外盘 + 板块资金 (前置信息)
  2. 🎯 交易员简报 (大盘+持仓+操作建议+风险预警)
  3. 📰 持仓相关新闻
  4. 📋 全市场新闻 (四Tab: 全部/持仓/情绪/历史)

数据源(v5):
  - 新浪7x24+财经: 全球/A股/美股新闻
  - 东财搜索30+词: A股+持仓+热门板块
  - 证券时报: 政策+市场要闻
  - 新浪行业板块: 49个行业涨跌+领涨股
  - 腾讯API: 美股三大指数+富时A50+恒生指数

刷新机制:
  - 交易时段: 每5分钟自动刷新
  - 盘后/休市: 每30分钟自动刷新
"""

import sys
import json
import time
import streamlit as st
import pandas as pd
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(ROOT))

from web.styles import inject_styles
inject_styles()

# ============================================================
# 自动刷新 — 盘中5分钟，盘后30分钟
# ============================================================

from streamlit_autorefresh import st_autorefresh

now = datetime.now()
is_trading = (
    now.weekday() < 5
    and ((now.hour == 9 and now.minute >= 15) or (10 <= now.hour <= 14) or (now.hour == 15 and now.minute <= 5))
)
refresh_sec = 300 if is_trading else 1800
st_autorefresh(interval=refresh_sec * 1000, key="news_autorefresh")

# ============================================================
# 加载数据 — 自动检测过期并拉取
# ============================================================

NEWS_FILE = ROOT / "data" / "news_today.json"

def _news_age_minutes():
    if not NEWS_FILE.exists():
        return 999
    return (time.time() - NEWS_FILE.stat().st_mtime) / 60

STALE_MINUTES = 30 if is_trading else 120

def load_brief():
    try:
        from src.data.sources.trader_brief import load_brief as _load
        return _load()
    except Exception:
        return {}

def load_news():
    age = _news_age_minutes()
    if age > STALE_MINUTES:
        st.info(f"数据已{age:.0f}分钟未更新，正在自动采集...")
        try:
            from src.data.sources.eastmoney_news import fetch_today_news
            fetch_today_news()
        except Exception as e:
            st.warning(f"自动采集失败: {e}，使用缓存数据")
    try:
        from src.data.sources.eastmoney_news import load_today_news
        return load_today_news()
    except Exception:
        return {
            "date": datetime.now().strftime("%Y-%m-%d"),
            "updated": "无数据",
            "stats": {"total": 0, "portfolio_related": 0,
                      "sentiment_dist": {}, "category_dist": {}, "sources": {}},
            "sector_flow": {"industry": [], "concept": []},
            "us_market": {},
            "news": [],
        }

brief = load_brief()
snapshot = load_news()
news_list = snapshot.get("news", [])
stats = snapshot.get("stats", {})
sector_flow = snapshot.get("sector_flow", {})
us_market = snapshot.get("us_market", {})
advice = brief.get("advice", {})

# 噪音过滤
NOISE_WORDS = ["彩票", "足彩", "竞彩", "双色球", "大乐透", "排列三", "排列五",
               "快乐8", "竞足", "胜负彩", "彩果", "奖号", "开奖", "投注"]
news_list = [n for n in news_list if not any(w in n.get("title", "") for w in NOISE_WORDS)]

st.title("📰 新闻热点")

# ============================================================
# 第一部分: 隔夜外盘 + 板块资金
# ============================================================

st.markdown("### 🌍 隔夜外盘 + 板块资金")

# 美股外盘
col_us = st.columns(len(us_market)) if us_market else []
if col_us:
    for i, (code, d) in enumerate(us_market.items()):
        with col_us[i]:
            pct = d.get("change_pct", 0)
            st.metric(d.get("name", code),
                      f"{d.get('price', '-')}",
                      f"{pct:+.2f}%",
                      delta_color="off")
else:
    st.caption("美股数据暂未采集")

# 行业板块TOP10
industries = sector_flow.get("industry", [])
if industries:
    st.markdown("#### 📊 行业板块涨跌排行")
    for row_start in [0, 5, 10, 15]:
        row_items = industries[row_start:row_start+5]
        if not row_items:
            break
        cols = st.columns(5)
        for i, sec in enumerate(row_items):
            with cols[i]:
                pct = sec.get("change_pct", 0)
                leader = sec.get("leader", "")
                leader_str = f"领涨:{leader}" if leader else ""
                st.metric(
                    sec["name"],
                    f"{pct:+.2f}%",
                    leader_str,
                    delta_color="off"
                )
else:
    st.caption("板块数据暂未采集")

st.divider()

# ============================================================
# 第二部分: 交易员简报(置顶)
# ============================================================

st.markdown("### 🎯 交易员简报 — 开盘前必看")

# 大盘总览
market_view = advice.get("market_view", "数据加载中...")
indices = brief.get("market", {}).get("indices", {})

if "全线下跌" in market_view or "偏弱" in market_view:
    st.error(f"📊 **{market_view}**")
elif "全线飘红" in market_view or "偏强" in market_view:
    st.success(f"📊 **{market_view}**")
else:
    st.info(f"📊 **{market_view}**")

# 指数卡片
idx_cols = st.columns(len(indices)) if indices else []
for i, (name, d) in enumerate(indices.items()):
    with idx_cols[i]:
        pct = d.get("change_pct", 0)
        st.metric(name, f"{d.get('price', '-')}", f"{pct}%",
                   delta_color="inverse" if pct < 0 else "normal")

# 持仓概览
st.markdown("#### 💰 持仓实时监控")
port = brief.get("market", {}).get("portfolio", {})
port_advice = advice.get("portfolio_advice", [])

for item in port_advice:
    name = item["name"]
    d = port.get(name, {})
    cost = d.get("cost", 0)
    stop = d.get("stop_loss", 0)
    price = item.get("price", 0)
    change = item.get("change", 0)
    pnl = item.get("pnl", 0)
    mv = d.get("market_value", 0)
    stop_dist = item.get("stop_distance", 0)
    action = item.get("action", "")
    alert = item.get("alert", "")
    catalyst = item.get("catalyst", "")

    with st.expander(f"{'🔴' if pnl < -10 else '🟡' if pnl < 0 else '🟢'} {name} "
                     f"({d.get('code','')}) | {price}元 ({change}%) | 浮盈{pnl}%",
                     expanded=bool(alert)):
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            st.metric("现价", f"{price}元", f"{change}%")
        with c2:
            st.metric("成本", f"{cost}元")
        with c3:
            st.metric("浮盈亏", f"{pnl}%", f"{item.get('pnl_amt', 0):.0f}元")
        with c4:
            st.metric("市值", f"{mv:.0f}元")

        c5, c6 = st.columns(2)
        with c5:
            st.metric("止损线", f"{stop}元")
        with c6:
            st.metric("距止损", f"{stop_dist}%",
                       delta="安全" if stop_dist > 10 else "危险" if stop_dist < 5 else "关注",
                       delta_color="off")

        if alert:
            st.warning(alert)
        if catalyst:
            if "利空" in catalyst or "压制" in catalyst:
                st.error(catalyst)
            elif "利好" in catalyst or "催化" in catalyst:
                st.success(catalyst)
            else:
                st.info(catalyst)
        st.info(f"**操作建议:** {action}")

# 风险预警
risk_alerts = advice.get("risk_alerts", [])
if risk_alerts:
    st.markdown("#### ⚠️ 风险预警")
    for r in risk_alerts:
        st.error(r)

# 证券板块
sec_data = brief.get("market", {}).get("sector_top", {})
if sec_data:
    st.markdown("#### 🏦 证券板块实时")
    sec_str = " | ".join(
        f"{'🔴' if d['change_pct'] < 0 else '🟢'}{name} {d['change_pct']}%"
        for name, d in sec_data.items()
    )
    st.markdown(sec_str)

st.divider()

# ============================================================
# 第三部分: 国家政策解读专区
# ============================================================

st.markdown("### 🏛️ 国家政策解读 — 影响持仓的关键政策")

# 从DB读取政策新闻
try:
    import sqlite3 as _sql
    _conn = _sql.connect(str(ROOT / "data" / "alpha_miner.db"))
    _today = datetime.now().strftime("%Y-%m-%d")

    # 政策新闻
    _policy_rows = _conn.execute(
        "SELECT title, content, sentiment_score, news_type, category, is_policy, publish_time "
        "FROM news WHERE (is_policy=1 OR category='政策') AND publish_time LIKE ? "
        "ORDER BY publish_time DESC",
        (f"{_today}%",)
    ).fetchall()
    _conn.close()

    # 政策利好/利空关键词
    _bull_words = ["支持", "利好", "促进", "鼓励", "补贴", "减税", "降息", "降准",
                   "放松", "开放", "振兴", "扶持", "激励", "专项", "新质生产力"]
    _bear_words = ["收紧", "限制", "监管", "处罚", "禁止", "叫停", "严控", "整顿",
                   "加息", "缩减", "退市", "核查"]

    # 持仓板块关键词 → 政策影响映射（动态从portfolio读取）
    from src.config.portfolio import get_portfolio, get_portfolio_sectors
    _portfolio_sectors = {}
    for p in get_portfolio():
        code = p["code"]
        name = p["name"]
        sectors = get_portfolio_sectors().get(code, [])
        for kw in sectors:
            if kw not in _portfolio_sectors:
                _portfolio_sectors[kw] = []
            if name not in _portfolio_sectors[kw]:
                _portfolio_sectors[kw].append(name)

    if _policy_rows:
        # 政策信号总览
        bull_count = 0
        bear_count = 0
        for r in _policy_rows:
            text = f"{r[0]} {r[1] or ''}"
            if any(w in text for w in _bull_words):
                bull_count += 1
            if any(w in text for w in _bear_words):
                bear_count += 1

        pc1, pc2, pc3, pc4 = st.columns(4)
        with pc1:
            st.metric("政策新闻", f"{len(_policy_rows)}条")
        with pc2:
            st.metric("政策利好", f"{bull_count}条", delta="偏多" if bull_count > bear_count else "")
        with pc3:
            st.metric("政策利空", f"{bear_count}条", delta="偏空" if bear_count > bull_count else "")
        with pc4:
            if bull_count > bear_count * 2:
                signal_text = "🟢 政策偏暖"
            elif bear_count > bull_count * 2:
                signal_text = "🔴 政策偏紧"
            else:
                signal_text = "🟡 中性"
            st.metric("政策信号", signal_text)

        # 政策对持仓的影响分析
        st.markdown("#### 📌 政策对持仓的影响")
        portfolio_impacts = {}
        for r in _policy_rows:
            title = r[0]
            content = r[1] or ""
            text = f"{title} {content}"
            is_bull = any(w in text for w in _bull_words)
            is_bear = any(w in text for w in _bear_words)
            sentiment = r[2] or 0

            for sector, stocks in _portfolio_sectors.items():
                sector_kw_map = {
                    "券商": ["券商", "证券", "资本市场", "注册制", "两融", "印花税", "基金"],
                    "互联网金融": ["互联网", "金融科技", "基金销售", "财富管理"],
                    "传媒": ["传媒", "游戏", "影视", "文化", "广电", "短视频", "AI"],
                    "电力设备": ["电力", "新能源", "核电", "风电", "光伏", "储能", "特高压"],
                    "医药": ["医药", "创新药", "医保", "医疗", "生物制药", "中药"],
                }
                kws = sector_kw_map.get(sector, [sector])
                if any(kw in text for kw in kws):
                    for stock in stocks:
                        if stock not in portfolio_impacts:
                            portfolio_impacts[stock] = {"bull": 0, "bear": 0, "news": []}
                        if is_bull:
                            portfolio_impacts[stock]["bull"] += 1
                        if is_bear:
                            portfolio_impacts[stock]["bear"] += 1
                        portfolio_impacts[stock]["news"].append(title[:50])

        if portfolio_impacts:
            imp_cols = st.columns(len(portfolio_impacts))
            for i, (stock, imp) in enumerate(portfolio_impacts.items()):
                with imp_cols[i]:
                    bull = imp["bull"]
                    bear = imp["bear"]
                    net = bull - bear
                    if net > 0:
                        verdict = f"🟢 利好({bull}条)"
                        delta_color = "normal"
                    elif net < 0:
                        verdict = f"🔴 利空({bear}条)"
                        delta_color = "inverse"
                    else:
                        verdict = "🟡 中性"
                        delta_color = "off"
                    st.metric(stock, verdict, f"政策{bull + bear}条相关", delta_color=delta_color)
        else:
            st.caption("今日政策新闻暂未直接涉及持仓板块")

        # 政策新闻列表
        st.markdown("#### 📜 政策新闻列表")
        with st.expander(f"查看全部 {len(_policy_rows)} 条政策新闻", expanded=False):
            for r in _policy_rows:
                title = r[0]
                content = r[1] or ""
                text = f"{title} {content}"
                is_bull = any(w in text for w in _bull_words)
                is_bear = any(w in text for w in _bear_words)
                time_str = r[6][11:16] if r[6] and len(r[6]) > 10 else ""

                if is_bull and not is_bear:
                    icon = "🔴利好"
                elif is_bear and not is_bull:
                    icon = "🟢利空"
                elif is_bull and is_bear:
                    icon = "🟡混合"
                else:
                    icon = "⚪中性"

                # 关联持仓
                related = []
                for sector, stocks in _portfolio_sectors.items():
                    sector_kw_map = {
                        "券商": ["券商", "证券", "资本市场", "注册制"],
                        "互联网金融": ["互联网", "金融科技", "基金"],
                        "传媒": ["传媒", "游戏", "AI"],
                        "电力设备": ["电力", "新能源", "核电", "风电"],
                        "医药": ["医药", "创新药", "医保"],
                    }
                    kws = sector_kw_map.get(sector, [sector])
                    if any(kw in text for kw in kws):
                        related.extend(stocks)

                related_str = f" → 影响: {', '.join(set(related))}" if related else ""
                st.markdown(f"**[{icon}]** [{time_str}] {title[:70]}{related_str}")
                if content and len(content) > 15:
                    st.markdown(f"<div style='margin-left:2em;color:#8b949e;font-size:0.85em'>{content[:200]}</div>",
                                 unsafe_allow_html=True)
                st.markdown("<div style='margin:0.1em 0'></div>", unsafe_allow_html=True)
    else:
        st.info("今日暂无政策新闻采集。点击底部「重新采集新闻」获取最新数据。")

except Exception as e:
    st.warning(f"政策解读加载失败: {e}")

# 策略信号（来自新闻情绪桥接）
try:
    from src.data.sources.eastmoney_news import get_news_sentiment_for_strategy
    _ns = get_news_sentiment_for_strategy()
    if _ns["news_count"] > 0:
        st.markdown("#### 🎯 策略信号（新闻情绪驱动）")
        sc1, sc2, sc3 = st.columns(3)
        with sc1:
            sig = _ns["signal"]
            sig_icon = {"bullish": "🟢强势偏多", "slightly_bullish": "🟡略偏多",
                        "neutral": "⚪中性", "slightly_bearish": "🟡略偏空",
                        "bearish": "🔴弱势偏空"}.get(sig, sig)
            st.metric("情绪信号", sig_icon)
        with sc2:
            st.metric("综合情绪", f"{_ns['overall_sentiment']:+.2f}")
        with sc3:
            adj = _ns["position_adjust"]
            adj_str = f"x{adj}" if adj != 1.0 else "不调整"
            st.metric("仓位建议", adj_str)
        if _ns["sector_hot"]:
            st.caption(f"热门板块: {' | '.join(_ns['sector_hot'])}")
except Exception:
    pass

st.divider()

# ============================================================
# 第四部分: 持仓相关新闻
# ============================================================

portfolio_news = brief.get("portfolio_news", [])
if portfolio_news:
    st.markdown(f"### 📰 持仓相关新闻 ({len(portfolio_news)}条)")
    with st.expander("查看持仓相关新闻详情", expanded=True):
        for n in portfolio_news[:15]:
            sent = n.get("sentiment", "中性")
            sent_icon = {"利好": "🔴", "偏多": "🟠", "中性": "⚪", "偏空": "🟢", "利空": "🟢"}.get(sent, "⚪")
            stock_name = n.get("stock", "")
            time_str = n.get("date", "")[11:16] if len(n.get("date", "")) > 10 else ""

            st.markdown(f"{sent_icon} **[{stock_name}]** [{time_str}] {n['title'][:60]}")
            content = n.get("content", "")
            if content and len(content) > 15:
                st.markdown(f"<div style='margin-left:2em;color:#8b949e;font-size:0.85em'>{content[:200]}</div>",
                             unsafe_allow_html=True)
            if n.get("url"):
                st.markdown(f"<div style='margin-left:2em;font-size:0.8em'><a href='{n['url']}'>查看详情</a></div>",
                             unsafe_allow_html=True)
            st.markdown("<div style='margin:0.2em 0'></div>", unsafe_allow_html=True)

st.divider()

# ============================================================
# 第四部分: 全市场新闻(四Tab)
# ============================================================

st.markdown("### 📋 全市场新闻")
st.caption("新浪财经 + 7x24快讯 + 东财搜索(30+词) + 证券时报 + 新浪行业板块 + 腾讯外盘 · 每日3次自动采集")
st.markdown(f"**更新时间:** {snapshot.get('updated', '无')} | **总新闻:** {stats.get('total', 0)}条")

# 指标
sent_dist = stats.get("sentiment_dist", {})
pos_count = sent_dist.get("利好", 0) + sent_dist.get("偏多", 0)
neg_count = sent_dist.get("利空", 0) + sent_dist.get("偏空", 0)

c1, c2, c3, c4 = st.columns(4)
with c1:
    st.metric("总新闻", f"{stats.get('total', 0)}条")
with c2:
    st.metric("利好/偏多", f"{pos_count}条")
with c3:
    st.metric("利空/偏空", f"{neg_count}条")
with c4:
    cat_dist = stats.get("category_dist", {})
    top_cat = max(cat_dist, key=cat_dist.get) if cat_dist else "无"
    st.metric("最热分类", top_cat)

# 筛选
f1, f2, f3 = st.columns(3)
with f1:
    cats = ["全部"] + sorted(cat_dist.keys())
    cat_filter = st.selectbox("分类", cats, key="cat_f")
with f2:
    sents = ["全部", "利好", "偏多", "中性", "偏空", "利空"]
    sent_filter = st.selectbox("情感", sents, key="sent_f")
with f3:
    search = st.text_input("搜索", placeholder="输入关键词...", key="search_f")

# 过滤
filtered = news_list
if cat_filter != "全部":
    filtered = [n for n in filtered if n.get("category") == cat_filter]
if sent_filter != "全部":
    filtered = [n for n in filtered if n.get("sentiment") == sent_filter]
if search:
    filtered = [n for n in filtered if search in n.get("title", "") or search in n.get("content", "")]

st.caption(f"共 {len(filtered)} 条")

# 四Tab
tab_all, tab_portfolio, tab_sentiment, tab_history = st.tabs([
    "📋 全部新闻", "⭐ 持仓相关", "📊 市场情绪", "📅 历史归档"
])

with tab_all:
    # 重磅新闻优先展示
    heavy = [n for n in filtered if n.get("category") == "重磅"]
    normal = [n for n in filtered if n.get("category") != "重磅"]
    display = heavy + normal

    for n in display[:50]:
        sent = n.get("sentiment", "中性")
        icon = {"利好": "🔴", "偏多": "🟠", "中性": "⚪", "偏空": "🟢", "利空": "🟢"}.get(sent, "⚪")
        time_str = n.get("date", "")[11:16] if len(n.get("date", "")) > 10 else ""
        cat = n.get("category", "")
        is_heavy = cat == "重磅"

        # 重磅新闻红色醒目样式
        if is_heavy:
            title_line = f"🔴🔥 **[{time_str}]** [{cat}] {n['title'][:70]}"
        else:
            title_line = f"{icon} **[{time_str}]** [{cat}] {n['title'][:60]}"
        if n.get("url"):
            title_line += f" [🔗]({n['url']})"
        st.markdown(title_line)

        content = n.get("content", "")
        if content and len(content) > 15:
            content_color = "#ff6b6b" if is_heavy else "#8b949e"
            st.markdown(f"<div style='margin-left:2em;color:{content_color};font-size:0.85em'>{content[:200]}</div>",
                         unsafe_allow_html=True)

        trade_impact = n.get("trade_impact", "")
        if trade_impact:
            st.markdown(f"<div style='margin-left:2em;color:#f0883e;font-size:0.85em'>⚡ {trade_impact[:100]}</div>",
                         unsafe_allow_html=True)

        related = n.get("related_stocks", [])
        if related:
            tags = " ".join(f"`{s['name']}`" for s in related)
            st.markdown(f"<div style='margin-left:2em;font-size:0.8em'>关联: {tags}</div>",
                         unsafe_allow_html=True)
        st.markdown("<div style='margin:0.1em 0'></div>", unsafe_allow_html=True)

with tab_portfolio:
    port_news = [n for n in filtered if n.get("portfolio_related")]
    if port_news:
        for n in port_news[:20]:
            sent = n.get("sentiment", "中性")
            icon = {"利好": "🔴", "偏多": "🟠", "中性": "⚪", "偏空": "🟢", "利空": "🟢"}.get(sent, "⚪")
            st.markdown(f"{icon} {n['title'][:60]}")
            content = n.get("content", "")
            if content and len(content) > 15:
                st.markdown(f"<div style='margin-left:2em;color:#8b949e;font-size:0.85em'>{content[:200]}</div>",
                             unsafe_allow_html=True)
            if n.get("trade_impact"):
                st.markdown(f"<div style='margin-left:2em;color:#f0883e;font-size:0.85em'>⚡ {n['trade_impact'][:100]}</div>",
                             unsafe_allow_html=True)
    else:
        st.info("暂无持仓相关新闻")

with tab_sentiment:
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("#### 情感分布")
        if sent_dist:
            df = pd.DataFrame(list(sent_dist.items()), columns=["情感", "数量"]).sort_values("数量")
            st.bar_chart(df, x="情感", y="数量")
    with c2:
        st.markdown("#### 分类分布")
        if cat_dist:
            df = pd.DataFrame(list(cat_dist.items()), columns=["分类", "数量"]).sort_values("数量")
            st.bar_chart(df, x="分类", y="数量")

    st.markdown("#### ⚡ 高影响力新闻")
    high = [n for n in news_list if n.get("impact") == "高"]
    for n in high[:10]:
        st.markdown(f"- [{n.get('sentiment','')}] {n['title'][:60]}")

    total = max(stats.get("total", 1), 1)
    pos_r = pos_count / total * 100
    neg_r = neg_count / total * 100
    if pos_r > neg_r * 2:
        env = "🟢 偏暖 — 利好消息多于利空"
    elif neg_r > pos_r * 2:
        env = "🔴 偏冷 — 利空消息占优"
    else:
        env = "🟡 中性 — 多空均衡"
    st.info(f"{env} (利好{pos_count} vs 利空{neg_count})")

with tab_history:
    history_dir = ROOT / "data" / "news_history"
    if history_dir.exists():
        for f in sorted(history_dir.glob("*.json"), reverse=True)[:15]:
            try:
                d = json.loads(f.read_text())
                s = d.get("stats", {})
                sd = s.get("sentiment_dist", {})
                pos = sd.get("利好", 0) + sd.get("偏多", 0)
                neg = sd.get("利空", 0) + sd.get("偏空", 0)
                st.markdown(f"- **{f.stem}** — {s.get('total',0)}条 | 利好{pos}/利空{neg}")
            except Exception:
                pass
    else:
        st.info("暂无历史归档数据")

# ============================================================
# 底部操作
# ============================================================

st.divider()

refresh_label = f"每{refresh_sec // 60}分钟自动刷新"
trading_label = "🟢 交易时段" if is_trading else "🔴 盘后/休市"
st.caption(f"{trading_label} | {refresh_label} | 当前时间: {now.strftime('%H:%M:%S')}")

col1, col2 = st.columns(2)
with col1:
    if st.button("🔄 立即刷新", use_container_width=True):
        st.rerun()
with col2:
    if st.button("🔃 重新采集新闻", use_container_width=True):
        with st.spinner("采集中(约1-2分钟)..."):
            try:
                from src.data.sources.eastmoney_news import fetch_today_news
                fetch_today_news()
            except Exception as e:
                st.error(f"采集失败: {e}")
        st.rerun()

st.caption("数据源: 新浪7x24 + 新浪财经 + 东财搜索(30+词) + 证券时报 + 新浪行业板块 + 腾讯外盘")
