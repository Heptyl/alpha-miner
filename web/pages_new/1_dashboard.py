"""Dashboard — Alpha Miner 首页

一眼看完: 大盘指数 + 实盘盈亏 + 市场情绪 + 模拟盘 + 系统状态
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import streamlit as st
from datetime import datetime

from web.styles import inject_styles
from web.components import metric_card, position_row, status_dot, fmt_pct, fmt_price, pnl_color
from web.services.data_service import (
    get_index_quotes, get_portfolio_realtime, get_sim_account,
    get_sim_positions, get_market_overview, get_system_status,
    get_hot_sectors, get_news,
)

inject_styles()

# === 自动刷新 ===
now = datetime.now()
is_trading = (now.weekday() < 5 and
              ((now.hour == 9 and now.minute >= 30) or now.hour == 10 or
               (now.hour == 11 and now.minute <= 30) or
               now.hour in (13, 14) or (now.hour == 15 and now.minute == 0)))

if is_trading:
    try:
        from streamlit_autorefresh import st_autorefresh
        st_autorefresh(interval=15000, key="dash_refresh")
    except ImportError:
        pass

# ============================================================
# 顶栏: 5大指数
# ============================================================
indices = get_index_quotes()
cols = st.columns(len(indices))
for i, idx in enumerate(indices):
    with cols[i]:
        metric_card(
            label=idx["name"],
            value=f"{idx['price']:.2f}" if idx['price'] else "--",
            sub=fmt_pct(idx['pct']),
            up=idx.get('up', True),
        )

# 时钟 + 交易状态
import streamlit.components.v1 as components
components.html("""
<div style="text-align:center;padding:2px 0;font-family:sans-serif;">
    <span id="am-clock" style="font-size:0.85rem;color:#808080;"></span>
    <span id="am-status" style="font-size:0.85rem;font-weight:600;"></span>
</div>
<script>
(function(){
    function tick(){
        var now=new Date();
        var timeStr=now.getFullYear()+'-'+String(now.getMonth()+1).padStart(2,'0')+'-'+
            String(now.getDate()).padStart(2,'0')+' '+
            String(now.getHours()).padStart(2,'0')+':'+String(now.getMinutes()).padStart(2,'0')+':'+
            String(now.getSeconds()).padStart(2,'0');
        var dow=now.getDay(),h=now.getHours(),m=now.getMinutes();
        var status='',color='#aaa';
        if(dow===0||dow===6){status='周末休市';color='#808080';}
        else if(h<9||(h===9&&m<30)){status='盘前准备';color='#ffd93d';}
        else if((h===11&&m>30)||(h===12)){status='午间休市';color='#ffd93d';}
        else if((h===9&&m>=30)||h===10||(h===11&&m<=30)||(h>=13&&h<15)){status='交易中';color='#ef5350';}
        else{status='已收盘';color='#808080';}
        document.getElementById('am-clock').textContent=timeStr+'  ';
        var el=document.getElementById('am-status');el.textContent=status;el.style.color=color;
    }
    tick();setInterval(tick,1000);
})();
</script>
""", height=30)

st.divider()

# ============================================================
# 主区域: 3列布局
# ============================================================
left, center, right = st.columns([2, 3, 2])

# ── 左列: 实盘持仓 ──
with left:
    st.markdown("### 📊 实盘持仓")
    portfolio = get_portfolio_realtime()
    if portfolio:
        total_pnl = sum(p.get("pnl", 0) for p in portfolio)
        total_mv = sum(p.get("price", 0) * p.get("shares", 0) for p in portfolio)
        pnl_cls = pnl_color(total_pnl)

        st.markdown(
            f'<div style="text-align:center;margin-bottom:8px;">'
            f'<span style="font-size:0.8rem;color:#808080;">总市值</span><br>'
            f'<span style="font-size:1.3rem;font-weight:700;">¥{total_mv:,.0f}</span><br>'
            f'<span class="{pnl_cls}" style="font-size:0.9rem;">总浮盈 {total_pnl:+,.0f}</span>'
            f'</div>',
            unsafe_allow_html=True,
        )
        for p in portfolio:
            position_row(
                code=p["code"], name=p["name"], shares=p["shares"],
                cost=p["cost"], price=p.get("price", 0),
                pnl=p.get("pnl", 0), pnl_pct=p.get("pnl_pct", 0),
                change_pct=p.get("change_pct", 0),
                stop_loss=p.get("stop_loss", 0),
                industry=p.get("industry", ""),
            )
    else:
        st.info("暂无持仓数据")

    # 模拟盘
    st.markdown("### 🎮 模拟盘")
    acct = get_sim_account()
    pnl_cls = pnl_color(acct["pnl"])
    cols = st.columns(2)
    with cols[0]:
        metric_card("总资产", f'¥{acct["total"]:,.0f}',
                     f'{acct["pnl"]:+,.0f}', acct["pnl"] >= 0)
    with cols[1]:
        metric_card("现金", f'¥{acct["cash"]:,.0f}',
                     f'收益率 {acct["pnl_pct"]:+.1f}%', acct["pnl"] >= 0)

    sim_pos = get_sim_positions()
    if sim_pos:
        for p in sim_pos:
            strategy_tag = f'[策略{p.get("strategy","A")}]'
            st.markdown(
                f'<div style="background:#1a2332;border:1px solid #2a3a4e;border-radius:6px;padding:8px 12px;margin:3px 0;">'
                f'<strong>{p["name"]}</strong> {p["code"]} {strategy_tag}<br>'
                f'<span class="{pnl_color(p.get("pnl",0))}">'
                f'{p.get("shares",0)}股 成本¥{(p.get("cost") or 0):.2f} 现价¥{(p.get("price") or 0):.2f} '
                f'盈亏{(p.get("pnl") or 0):+.0f}</span></div>',
                unsafe_allow_html=True,
            )

# ── 中列: 市场情绪 ──
with center:
    st.markdown("### 🌐 市场情绪")

    overview = get_market_overview()
    phase = overview.get("phase", "未知")
    zt = overview.get("zt_count", 0)
    zt_total = overview.get("zt_count_total", zt)
    real_zt = overview.get("real_zt", zt)
    zb = overview.get("zb_count", 0)
    zb_rate = overview.get("zb_rate", 0)
    max_lb = overview.get("max_consecutive", 0)
    lb_count = overview.get("lb_count", 0)
    can_buy = overview.get("can_buy", False)
    position = overview.get("suggested_position", 0)
    hint = overview.get("strategy_hint", "")
    data_date = overview.get("data_date", "")
    today_str = datetime.now().strftime("%Y-%m-%d")
    data_source = overview.get("data_source", "db")
    is_realtime = data_source == "realtime"
    date_note = "实时数据(60s刷新)" if is_realtime else ("今日数据" if data_date == today_str else f"截至 {data_date}")
    # 过期告警: 非实时且数据超过2天
    is_stale = False
    if not is_realtime and data_date and data_date != today_str:
        try:
            from datetime import date as date_cls
            gap_days = (date_cls.today() - date_cls.fromisoformat(data_date)).days
            if gap_days >= 2:
                is_stale = True
                date_note = f"⚠️ 数据过期{gap_days}天 (截至{data_date})"
        except Exception:
            pass
    up_count = overview.get("up_count", 0)
    down_count = overview.get("down_count", 0)
    dt_count = overview.get("dt_count", 0)
    real_dt = overview.get("real_dt", 0)
    activity = overview.get("activity", "")
    total_count = up_count + down_count
    up_ratio_val = up_count / total_count if total_count > 0 else 0.5

    # === 情绪得分(0-100) ===
    # 方法: 等权百分位法(CNN Fear & Greed Index)
    # 每个指标在历史数据中的百分位排名, 然后等权平均
    # 不需要拍权重, 结果自然落在0-100
    score = overview.get("score", 50)

    phase_emoji = {
        "冰点": "❄️", "退潮": "📉", "退潮预警": "🚨",
        "偏弱": "⚠️", "分化": "🔀", "正常": "📊", "未知": "❓"
    }.get(phase, "❓")

    phase_color = {
        "冰点": "#42a5f5", "退潮": "#ffd93d", "退潮预警": "#ff9800",
        "偏弱": "#ff7043", "分化": "#26a69a", "正常": "#66bb6a", "未知": "#808080"
    }.get(phase, "#808080")

    # 可操作标记
    action_tag = '<span style="background:#26a69a;color:#fff;padding:2px 8px;border-radius:4px;font-size:0.7rem;">可开仓</span>' if can_buy else '<span style="background:#ef5350;color:#fff;padding:2px 8px;border-radius:4px;font-size:0.7rem;">观望</span>'

    # ── 主卡片 ──
    st.markdown(
        f'<div style="background:#1a2332;border:1px solid #2a3a4e;border-radius:8px;padding:16px;text-align:center;margin-bottom:8px;">'
        f'<div style="font-size:2rem;">{phase_emoji}</div>'
        f'<div style="font-size:1.3rem;font-weight:700;color:{phase_color};">{phase}</div>'
        f'<div style="font-size:0.8rem;color:#808080;">情绪得分 {score:.0f}/100 {action_tag}</div>'
        f'<div style="font-size:0.7rem;color:{"#ef5350" if is_stale else "#90caf9"};margin-top:2px;">📅 {date_note}</div>'
        f'</div>',
        unsafe_allow_html=True,
    )

    # ── 详细数据卡片 ──
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.markdown(
            f'<div style="background:#1a2332;border:1px solid #2a3a4e;border-radius:6px;padding:10px;text-align:center;">'
            f'<div style="font-size:0.7rem;color:#808080;">涨停</div>'
            f'<div style="font-size:1.4rem;font-weight:700;color:#ef5350;">{zt_total}</div>'
            f'<div style="font-size:0.6rem;color:#607080;">真实{real_zt}(非ST)</div>'
            f'</div>',
            unsafe_allow_html=True,
        )
    with col2:
        st.markdown(
            f'<div style="background:#1a2332;border:1px solid #2a3a4e;border-radius:6px;padding:10px;text-align:center;">'
            f'<div style="font-size:0.7rem;color:#808080;">跌停</div>'
            f'<div style="font-size:1.4rem;font-weight:700;color:#26a69a;">{dt_count}</div>'
            f'<div style="font-size:0.6rem;color:#607080;">真实{real_dt}(非ST)</div>'
            f'</div>',
            unsafe_allow_html=True,
        )
    with col3:
        st.markdown(
            f'<div style="background:#1a2332;border:1px solid #2a3a4e;border-radius:6px;padding:10px;text-align:center;">'
            f'<div style="font-size:0.7rem;color:#808080;">炸板</div>'
            f'<div style="font-size:1.4rem;font-weight:700;color:#ffa726;">{zb}</div>'
            f'<div style="font-size:0.6rem;color:#607080;">炸板率{zb_rate:.0%}</div>'
            f'</div>',
            unsafe_allow_html=True,
        )
    with col4:
        st.markdown(
            f'<div style="background:#1a2332;border:1px solid #2a3a4e;border-radius:6px;padding:10px;text-align:center;">'
            f'<div style="font-size:0.7rem;color:#808080;">涨跌比</div>'
            f'<div style="font-size:1.1rem;font-weight:700;color:#42a5f5;">{up_count}</div>'
            f'<div style="font-size:0.7rem;color:#607080;">/ {down_count}</div>'
            f'<div style="font-size:0.6rem;color:#607080;">上涨占比{up_ratio_val:.1%}</div>'
            f'</div>',
            unsafe_allow_html=True,
        )

    # 连板+仓位+活跃度
    info_line = f"最高{max_lb}连板 | 连板股{lb_count}只 | 建议仓位{position:.0%}"
    if activity:
        info_line += f" | 活跃度{activity}"
    st.caption(info_line)

    # 策略提示
    if hint:
        st.markdown(
            f'<div style="font-size:0.75rem;color:#90caf9;padding:4px 8px;background:#1a2332;border-radius:4px;">💡 {hint}</div>',
            unsafe_allow_html=True,
        )

    # 机制说明(折叠)
    with st.expander("ℹ️ 情绪判断机制 & 得分公式 (v3)"):
        st.markdown("""
        **一、阶段判断 — 涨跌比为主(60%) + 涨停数辅助(25%) + 炸板率辅助(15%):**
        
        **涨跌比是盘中最可靠的实时指标。** 涨停数开盘10分钟返回前日数据(已验证),
        且盘中包含未炸板票(5-21盘中50+涨停, 收盘34只)。涨跌比反映全市场温度。
        
        | 阶段 | 涨跌比 | 仓位 | can_buy | 说明 |
        |------|--------|------|---------|------|
        | 正常📊 | ≥55% | 80% | True | 多数票在涨, 可操作 |
        | 分化🔀 | 45-55% | 50% | True | 涨跌各半, 半仓 |
        | 偏弱⚠️ | 40-45% | 30% | True | 跌多涨少, 轻仓谨慎 |
        | 退潮📉 | 30-40% | 10% | False | 明确弱势, 不开新仓 |
        | 冰点❄️ | <30% | 0% | False | 极端弱势, 绝对不开 |
        | 退潮预警🚨 | 涨跌比快速下滑(Δ<-10%) | 10% | False | 趋势恶化 |
        
        **辅助修正:**
        - 涨停数≥80(10:00后): 正常日加仓10%
        - 涨停数<50(10:00后): 减仓20%
        - 炸板率>40%: 强制退潮(不管涨跌比)
        
        **二、保护机制:**
        - **开盘10分钟(09:30-09:40)**: 涨停数强制忽略(返回前日数据), 只用涨跌比
        - **涨跌比趋势**: 30分钟内下降>10% → 退潮预警
        - **炸板率覆盖**: 炸板率>40% → 强制退潮
        - **数据缺失**: 涨跌比+涨停均不可用 → 保守不开仓
        
        **三、实盘验证(5-15~5-21):**
        - 5-19(正常日): 涨跌比44%→66%, 全天稳定, 新系统判"正常" ✓
        - 5-20(退潮日): 涨跌比17%→31%, 全天低迷, 新系统判"退潮" ✓  
        - 5-21(冰点日): 涨跌比65%→13%, 炸板67%, 新系统判"退潮" ✓
        - 5-15(尾盘崩): 涨跌比20%→57%→33%, 炸板41%, 新系统判"退潮" ✓
        
        **四、情绪得分 — 等权百分位法(0-100):**
        ```
        指标1: 涨停数 → 30天历史百分位
        指标2: 涨跌比 → 历史百分位
        指标3: 炸板率 → (1-炸板率)×100
        指标4: 最高连板 → 30天历史百分位
        得分 = (指标1+2+3+4) / 4
        冰点/退潮惩罚: 得分×0.5
        ```
        
        **五、数据来源与可靠性:**
        - **涨跌比(主)**: 东财ulist接口(上证+深证成分股统计), 盘中实时, 不受前日缓存影响
        - **涨停数(辅)**: 东财clist接口(涨幅≥9.8%), 开盘10分钟可能返回前日数据
        - **炸板数(辅)**: DB zb_pool表, 盘中每30分钟刷新一次
        - 30秒缓存 + 三层降级(requests→curl→DB) + 异常自动重拉
        """)

    # 热门板块
    st.markdown("#### 🔥 热门板块")
    sectors = get_hot_sectors()
    if sectors:
        sector_cols = st.columns(2)
        for i, s in enumerate(sectors[:8]):
            with sector_cols[i % 2]:
                zt_count = s.get("zt_count", 0)
                st.markdown(
                    f'<div style="background:#1a2332;border:1px solid #2a3a4e;border-radius:6px;padding:6px 10px;margin:3px 0;">'
                    f'<span style="color:#42a5f5;font-weight:600;">{s.get("industry","")}</span> '
                    f'<span class="am-tag am-tag-up">{zt_count}涨停</span></div>',
                    unsafe_allow_html=True,
                )
    else:
        st.caption("暂无板块数据")

    # 新闻摘要
    st.markdown("#### 📰 最新动态")
    news = get_news(5)
    if news:
        for n in news:
            sent_cls = "up" if n.get("sentiment") == "正面" else ("down" if n.get("sentiment") == "负面" else "")
            st.markdown(
                f'<div style="padding:4px 0;border-bottom:1px solid #2a3a4e;">'
                f'<span style="font-size:0.85rem;color:#e0e0e0;">{n["title"][:50]}</span><br>'
                f'<span style="font-size:0.7rem;color:#808080;">{n.get("time","")} '
                f'<span class="{sent_cls}">{n.get("sentiment","")}</span></span></div>',
                unsafe_allow_html=True,
            )
    else:
        st.caption("暂无新闻")

# ── 右列: 系统状态 ──
with right:
    st.markdown("### ⚙️ 系统状态")
    sys_status = get_system_status()

    daemon_status = status_dot(sys_status["daemon_running"])
    st.markdown(
        f'<div style="background:#1a2332;border:1px solid #2a3a4e;border-radius:8px;padding:12px;margin-bottom:8px;">'
        f'<div style="font-size:0.85rem;">守护进程: {daemon_status}</div>'
        f'<div style="font-size:0.75rem;color:#808080;">'
        f'PID: {sys_status.get("daemon_pid", "N/A")} | '
        f'最后扫描: {sys_status.get("last_scan", "N/A")}</div></div>',
        unsafe_allow_html=True,
    )

    # 数据新鲜度
    st.markdown("#### 📅 数据更新")
    freshness = sys_status.get("data_freshness", {})
    for table, date_val in freshness.items():
        table_name = {
            "daily_price": "日K线",
            "zt_pool": "涨停池",
            "fund_flow": "资金流",
            "news": "新闻",
        }.get(table, table)
        is_fresh = date_val == datetime.now().strftime("%Y-%m-%d") if date_val else False
        dot_cls = "am-status-dot running" if is_fresh else "am-status-dot stopped"
        st.markdown(
            f'<div style="font-size:0.8rem;padding:2px 0;">'
            f'<span class="{dot_cls}"></span>'
            f'{table_name}: {date_val or "无数据"}</div>',
            unsafe_allow_html=True,
        )

    # 选股防御检查
    st.markdown("#### 🎯 选股状态")
    try:
        from web.services.data_service import get_all_candidates
        cands = get_all_candidates()
        sa_cnt = len([c for c in cands if c.get("_strategy") == "A"])
        sb_cnt = len([c for c in cands if c.get("_strategy") == "B"])
        sc_cnt = len([c for c in cands if c.get("_strategy") == "C"])
        sa_color = "#26a69a" if sa_cnt >= 10 else ("#ffd93d" if sa_cnt >= 5 else "#ef5350")
        sb_color = "#26a69a" if sb_cnt >= 3 else "#ef5350"
        sc_color = "#26a69a" if sc_cnt >= 3 else ("#ffd93d" if sc_cnt >= 1 else "#808080")
        st.markdown(
            f'<div style="font-size:0.8rem;padding:2px 0;">'
            f'<span style="color:#26a69a;">策略A: 龙头首阴反包(3万·3天)</span></div>'
            f'<div style="font-size:0.8rem;padding:2px 0;">'
            f'<span style="color:{sb_color};">策略B: {sb_cnt}只候选(回踩低吸·盘中实时)</span></div>'
            f'<div style="font-size:0.8rem;padding:2px 0;">'
            f'<span style="color:#666;">策略C: 缩量反包(3万·3只 PF=1.25)</span></div>',
            unsafe_allow_html=True,
        )
        if sa_cnt < 5:
            st.markdown(
                '<div style="background:#3e2723;border:1px solid #ef5350;border-radius:4px;'
                'padding:6px 10px;margin:4px 0;font-size:0.75rem;color:#ef5350;">'
                '⚠️ 策略A首阴反包候选不足, 可能数据不完整</div>',
                unsafe_allow_html=True,
            )
    except Exception as e:
        st.caption(f"选股检查失败: {e}")

    # 快捷操作
    st.markdown("#### 🔧 快捷操作")
    col1, col2 = st.columns(2)
    with col1:
        if st.button("🔄 刷新数据", use_container_width=True, key="dash_refresh_btn"):
            st.cache_data.clear()
            st.rerun()
    with col2:
        if st.button("📊 采集今日数据", use_container_width=True, key="dash_collect_btn"):
            with st.spinner("采集中..."):
                try:
                    import subprocess
                    result = subprocess.run(
                        ["uv", "run", "python", "-m", "cli.collect", "--today"],
                        capture_output=True, text=True, timeout=120,
                        cwd=str(Path(__file__).parent.parent.parent),
                    )
                    if result.returncode == 0:
                        st.success("采集完成")
                    else:
                        st.error(f"采集失败: {result.stderr[:200]}")
                    st.cache_data.clear()
                except Exception as e:
                    st.error(f"采集出错: {e}")

    if st.button("🤖 启动守护进程", use_container_width=True, key="dash_start_daemon"):
        import subprocess
        subprocess.Popen(
            ["uv", "run", "python", "-m", "src.trader.trading_daemon", "start"],
            cwd=str(Path(__file__).parent.parent.parent),
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        st.success("守护进程启动中...")
        st.cache_data.clear()
        st.rerun()
