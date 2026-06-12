"""盘中交易 — 模拟盘实时操作面板

核心交易页: 三策略执行 + 操作流水 + 信号日志 + 自动刷新
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import streamlit as st
import pandas as pd
from datetime import datetime

from web.styles import inject_styles
from web.components import (
    metric_card, trade_card, signal_tag, status_dot,
    fmt_pct, fmt_price, pnl_color,
)
from web.services.data_service import (
    get_sim_account, get_sim_positions, get_sim_trades,
    get_all_candidates, get_daemon_log, get_system_status,
    get_pending_signals,
)

inject_styles()


# === 辅助渲染函数 ===
def _render_candidate(c: dict, held: bool = False):
    """渲染策略B候选卡片"""
    chg = c.get("realtime_chg", c.get("change_pct", c.get("pct", 0)))
    price = c.get("realtime_price", c.get("price", c.get("latest_price", 0)))
    source = c.get("source", c.get("_source", ""))
    code = c.get("code", "")
    source_map = {"回踩低吸": "回踩低吸", "缩量回踩": "缩量回踩(旧)", "首阴": "首阴", "涨停低吸": "回踩低吸(旧)", "涨停确认": "回踩低吸(旧)", "板块补涨": "板块补涨", "热门板块": "热门板块", "连板龙头": "连板龙头"}
    source_label = source_map.get(source, source)
    held_tag = '<span style="color:#2196f3;font-size:0.7rem;"> 已持仓</span>' if held else ""
    bg = "#1a2a1a" if held else "#1a2332"
    border = "#4caf50" if not held else "#2a3a4e"
    st.markdown(
        f'<div style="background:{bg};border-left:3px solid {border};border-radius:4px;padding:5px 10px;margin:2px 0;font-size:0.85rem;">'
        f'<strong>{c.get("name","")}</strong> '
        f'<span style="color:#808080;">{code}</span> '
        f'<span class="am-tag am-tag-accent" style="font-size:0.65rem;">{source_label}</span> '
        f'<span class="{pnl_color(chg)}">¥{price:.2f} {chg:+.1f}%</span>'
        f'{held_tag}</div>',
        unsafe_allow_html=True,
    )


def _render_candidate_compact(c: dict):
    """渲染已涨停候选（紧凑版）"""
    chg = c.get("realtime_chg", c.get("change_pct", c.get("pct", 0)))
    price = c.get("realtime_price", c.get("price", c.get("latest_price", 0)))
    code = c.get("code", "")
    name = c.get("name", "")
    st.markdown(
        f'<div style="font-size:0.75rem;color:#808080;padding:1px 0;">'
        f'{name} {code} ¥{price:.2f} <span style="color:#ef5350;">{chg:+.1f}%</span></div>',
        unsafe_allow_html=True,
    )


# === 自动刷新 ===
now = datetime.now()
is_trading = (now.weekday() < 5 and
              ((now.hour == 9 and now.minute >= 30) or now.hour == 10 or
               (now.hour == 11 and now.minute <= 30) or
               now.hour in (13, 14) or (now.hour == 15 and now.minute == 0)))

if is_trading:
    try:
        from streamlit_autorefresh import st_autorefresh
        st_autorefresh(interval=10000, key="trade_refresh")
    except ImportError:
        pass

st.markdown("## ⚡ 盘中交易实况")

# === 市场状态条(含退潮警示) ===
sys_status = get_system_status()
daemon_running = sys_status["daemon_running"]
status_html = status_dot(daemon_running)
last_scan = sys_status.get("last_scan", "N/A")

# 市场情绪(退潮检测)
try:
    from src.strategy.strategy_b import get_market_emotion
    emotion = get_market_emotion()
    phase = emotion.get("phase", "未知")
    can_buy = emotion.get("can_buy", True)
    up_ratio = emotion.get("up_ratio", 0.5)
    phase_icons = {"冰点": "❄️", "退潮": "📉", "退潮预警": "🚨", "偏弱": "⚠️", "分化": "🔀", "正常": "📊", "未知": "❓"}
    phase_colors = {"冰点": "#42a5f5", "退潮": "#ffd93d", "退潮预警": "#ff9800", "偏弱": "#ff7043", "分化": "#26a69a", "正常": "#66bb6a", "未知": "#808080"}
    phase_icon = phase_icons.get(phase, "❓")
    phase_color = phase_colors.get(phase, "#808080")
    ebb_tag = '<span style="background:#ef5350;color:#fff;padding:2px 8px;border-radius:4px;font-size:0.75rem;animation:blink 1s infinite;">⚠️退潮不开仓</span>' if not can_buy else ""
except Exception:
    phase = "未知"
    phase_icon = "❓"
    phase_color = "#808080"
    ebb_tag = ""
    up_ratio = 0.5

st.markdown(
    f'<div style="background:#1a2332;border:1px solid #2a3a4e;border-radius:8px;'
    f'padding:8px 16px;margin-bottom:12px;display:flex;align-items:center;gap:20px;">'
    f'<span>守护进程: {status_html}</span>'
    f'<span style="color:#808080;font-size:0.8rem;">最后扫描: {last_scan}</span>'
    f'<span style="color:#808080;font-size:0.8rem;">PID: {sys_status.get("daemon_pid","N/A")}</span>'
    f'<span style="color:{phase_color};font-size:0.85rem;">{phase_icon} {phase}</span>'
    f'<span style="color:#808080;font-size:0.8rem;">涨跌比 {up_ratio:.0%}</span>'
    f'{ebb_tag}'
    f'</div>',
    unsafe_allow_html=True,
)

# === 选股状态提示 ===
candidates = get_all_candidates()
sa_count = len([c for c in candidates if c.get("_strategy") == "A"])
sb_count = len([c for c in candidates if c.get("_strategy") == "B"])

from datetime import datetime
_now = datetime.now()
_is_trading_hours = (_now.hour >= 9 and _now.hour < 15) or (_now.hour == 9 and _now.minute >= 30)
_is_weekday = _now.weekday() < 5

if _is_trading_hours and _is_weekday and sa_count == 0 and sb_count == 0:
    st.markdown(
        '<div style="background:#3e2723;border:1px solid #ef5350;border-radius:8px;'
        'padding:10px 16px;margin-bottom:12px;color:#ef5350;">'
        f'⚠️ 盘中选股异常: 策略A和B均0只候选。请检查数据完整性!</div>',
        unsafe_allow_html=True,
    )
elif not _is_trading_hours or not _is_weekday:
    st.markdown(
        '<div style="background:#1a2332;border:1px solid #66bb6a;border-radius:8px;'
        'padding:10px 16px;margin-bottom:12px;color:#81c784;">'
        f'📊 非交易时段 · 策略A {sa_count}只 / 策略B {sb_count}只候选(开盘后实时更新)</div>',
        unsafe_allow_html=True,
    )

# ============================================================
# 账户概览
# ============================================================
acct = get_sim_account()
pnl_cls = pnl_color(acct["pnl"])

cols = st.columns(6)
with cols[0]:
    metric_card("总资产", f'¥{acct["total"]:,.0f}', pnl_cls + f' {acct["pnl"]:+,.0f}', acct["pnl"] >= 0)
with cols[1]:
    metric_card("现金", f'¥{acct["cash"]:,.0f}', "", True)
with cols[2]:
    metric_card("市值", f'¥{acct["market_value"]:,.0f}', "", True)
with cols[3]:
    metric_card("收益率", f'{acct["pnl_pct"]:+.1f}%', f'初始¥{acct["initial"]:,.0f}', acct["pnl"] >= 0)
with cols[4]:
    sim_pos = get_sim_positions()
    metric_card("持仓数", f'{len(sim_pos)}只', f'A:3只+B:3只+C:3只', len(sim_pos) <= 8)
with cols[5]:
    trades = get_sim_trades(999)
    today_trades = [t for t in trades if str(t.get("trade_date", "")) == datetime.now().strftime("%Y-%m-%d")]
    metric_card("今日操作", f'{len(today_trades)}笔', f'累计{len(trades)}笔', True)

st.divider()

# ============================================================
# 主体: 3列
# ============================================================
left, center, right = st.columns([2, 3, 2])

# ── 左列: 当前持仓 ──
with left:
    st.markdown("### 📊 当前持仓")
    positions = get_sim_positions()
    if positions:
        # 从交易记录取真实策略标签
        recent_trades = get_sim_trades(50)
        trade_strategy = {}
        for t in recent_trades:
            sig = t.get("signal_type", t.get("signal", ""))
            if "缩量反包" in sig:
                trade_strategy[t.get("code", "")] = "C"
            elif "策略A" in sig or "首阴" in sig or "因子" in sig:
                trade_strategy[t.get("code", "")] = "A"
            else:
                trade_strategy[t.get("code", "")] = "B"

        for p in positions:
            code = p.get("code", "")
            # 优先用交易记录的策略, 再用持仓自带
            strategy = trade_strategy.get(code, p.get("strategy", "A"))
            signal = p.get("signal", "")
            # 从信号提取买入原因
            buy_reason = ""
            if "缩量反包" in signal:
                buy_reason = "缩量反包"
            elif "首阴" in signal or "因子低吸" in signal:
                buy_reason = "首阴反包"
            elif "因子追涨" in signal:
                buy_reason = "首阴反包"
            elif "涨停确认" in signal or "涨停低吸" in signal or "缩量" in signal:
                buy_reason = "回踩低吸"
            elif "板块补涨" in signal:
                buy_reason = "板块补涨"
            elif "breakout" in signal:
                buy_reason = "放量突破"

            strategy_cls = {"A": "am-tag-warning", "B": "am-tag-accent", "C": "am-tag-info"}.get(strategy, "am-tag-warning")
            strategy_tag = f'<span class="am-tag {strategy_cls}">策略{strategy}</span>'
            if buy_reason:
                strategy_tag += f' <span style="color:#607d8b;font-size:0.7rem;">{buy_reason}</span>'

            # 从buy_date计算持有天数，而非依赖DB中可能未更新的hold_days字段
            hold_days = p.get("hold_days", 0)
            buy_date_str = p.get("buy_date", "")
            if buy_date_str and (hold_days == 0 or hold_days is None):
                try:
                    from datetime import datetime, date
                    bd = datetime.strptime(buy_date_str, "%Y-%m-%d").date()
                    hold_days = max(1, (date.today() - bd).days + 1)
                except (ValueError, TypeError):
                    hold_days = 0

            # 策略差异化卖出倒计时(交易员核心关注)
            try:
                from src.trader.daemon_config import SELL_PARAMS
                # 策略C: T+3尾盘清仓, 等效max_hold=2天(买入日T+2+持有1天T+3清), time_stop=2
                sp = SELL_PARAMS.get(strategy, {"max_hold_days": 2, "time_stop_days": 2} if strategy == "C" else {})
                max_hold = sp.get("max_hold_days", 7)
                time_stop = sp.get("time_stop_days", 5)
                remain_hold = max(0, max_hold - hold_days)
                remain_time = max(0, time_stop - hold_days)
                if remain_hold <= 1:
                    countdown_tag = '<span style="color:#ef5350;font-size:0.7rem;font-weight:700;">⏰明日强制清仓</span>'
                elif remain_time <= 0:
                    countdown_tag = '<span style="color:#ffd93d;font-size:0.7rem;">⏰已超时检止损</span>'
                elif remain_hold <= 2:
                    countdown_tag = f'<span style="color:#ffd93d;font-size:0.7rem;">⏰{remain_hold}天后清仓</span>'
                else:
                    countdown_tag = f'<span style="color:#607d8b;font-size:0.7rem;">⏰{remain_hold}天/{time_stop}天</span>'
            except ImportError:
                countdown_tag = ""

            # 移动止盈线(从highest_price计算, 按策略差异化trailing)
            highest = p.get("highest", p.get("highest_price", 0))
            cur_price = p.get("price", 0) or 0
            trailing_tag = ""
            if highest and highest > 0 and cur_price > 0:
                # 策略B trailing 3%, 策略A trailing 3%, 策略C trailing 5%
                trailing_pct = {"B": 0.03, "C": 0.05}.get(strategy, 0.03)
                trailing_stop = highest * (1 - trailing_pct)
                trailing_dist = (cur_price - trailing_stop) / cur_price * 100
                if trailing_dist < 1:
                    trailing_tag = ' <span style="color:#ef5350;font-size:0.7rem;">⚠️接近止盈线</span>'
                else:
                    trailing_tag = f' <span style="color:#607d8b;font-size:0.65rem;">止盈¥{trailing_stop:.2f}</span>'

            chg_cls = pnl_color(p.get("change_pct", 0) or 0)

            st.markdown(
                f'<div style="background:#1a2332;border:1px solid #2a3a4e;border-radius:8px;padding:10px 14px;margin:4px 0;">'
                f'<div style="display:flex;justify-content:space-between;">'
                f'<div><strong style="font-size:1rem;">{p["name"]}</strong> '
                f'<span style="color:#808080;font-size:0.8rem;">{code}</span> '
                f'{strategy_tag}</div>'
                f'<div style="text-align:right;">'
                f'<div style="font-size:1.1rem;font-weight:700;" class="{chg_cls}">¥{(p.get("price") or 0):.2f}</div>'
                f'<div style="font-size:0.8rem;" class="{chg_cls}">{(p.get("change_pct") or 0):+.1f}%</div>'
                f'</div></div>'
                f'<div style="display:flex;gap:16px;margin-top:6px;font-size:0.8rem;color:#808080;">'
                f'<span>{p.get("shares",0)}股</span>'
                f'<span>成本¥{(p.get("cost") or 0):.2f}</span>'
                f'<span class="{pnl_color(p.get("pnl",0) or 0)}">盈亏{(p.get("pnl") or 0):+.0f}</span>'
                f'<span>持有{hold_days}天</span>'
                f'{countdown_tag}'
                f'{trailing_tag}'
                f'</div></div>',
                unsafe_allow_html=True,
            )
    else:
        st.info("空仓中，等待买点信号...")

    # 操作按钮
    st.markdown("### 🎮 手动操作")
    col1, col2 = st.columns(2)
    with col1:
        if st.button("🟢 启动守护进程", use_container_width=True, key="start_daemon"):
            import subprocess
            subprocess.Popen(
                ["uv", "run", "python", "-m", "src.trader.trading_daemon", "start"],
                cwd=str(Path(__file__).parent.parent.parent),
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            )
            st.success("启动中...")
            st.rerun()
    with col2:
        if st.button("🔴 停止守护进程", use_container_width=True, key="stop_daemon"):
            import subprocess
            subprocess.run(["pkill", "-f", "trading_daemon"], capture_output=True)
            st.success("已停止")
            st.rerun()

# ── 中列: 交易流水 ──
with center:
    # 操作预告区域
    pending = get_pending_signals()
    if pending:
        st.markdown("### 🔔 待执行操作")
        for sig in pending:
            if sig.get("status") != "pending":
                continue
            action_cn = "买入" if sig["action"] == "buy" else "卖出"
            icon = "🟢" if sig["action"] == "buy" else "🔴"
            execute_at = sig.get("execute_at", "?")
            urgent_tag = ' <span style="color:#f44336;font-size:0.7rem;">⚠️紧急</span>' if sig.get("urgent") else ""
            sig_id = sig.get("id", "")

            col_info, col_btn = st.columns([5, 1])
            with col_info:
                st.markdown(
                    f'<div style="background:#1a2332;border:1px solid #2a3a4e;border-radius:6px;padding:8px 12px;margin:4px 0;">'
                    f'{icon} <b>{action_cn}</b> {sig.get("name","")}({sig.get("code","")}) '
                    f'¥{sig.get("price",0):.2f} '
                    f'<span style="color:#ff9800;font-size:0.8rem;">{sig.get("signal_type","")}</span>{urgent_tag}<br>'
                    f'<span style="color:#808080;font-size:0.75rem;">{sig.get("reason","")}</span><br>'
                    f'<span style="color:#90caf9;font-size:0.7rem;">⏰ {execute_at} 执行</span>'
                    f'</div>',
                    unsafe_allow_html=True,
                )
            with col_btn:
                if st.button("❌", key=f"cancel_{sig_id}", help="取消此操作"):
                    # 读取 → 过滤 → 写回
                    import json as _json
                    sp = Path(__file__).resolve().parents[2] / "output" / "trader" / "signals" / "pending_signals.json"
                    if sp.exists():
                        all_sigs = _json.loads(sp.read_text(encoding="utf-8"))
                        all_sigs = [s for s in all_sigs if s.get("id") != sig_id]
                        tmp = sp.with_suffix(".tmp")
                        tmp.write_text(_json.dumps(all_sigs, ensure_ascii=False, indent=2), encoding="utf-8")
                        tmp.rename(sp)
                    st.rerun()
    else:
        st.markdown(
            '<div style="color:#4a5568;font-size:0.8rem;padding:4px;">暂无待执行操作预告</div>',
            unsafe_allow_html=True,
        )

    st.markdown("### 📋 交易流水")

    trades = get_sim_trades(30)
    if trades:
        df_data = []
        for t in trades:
            df_data.append({
                "时间": str(t.get("trade_date", "")) + " " + str(t.get("trade_time", "")),
                "操作": "买入" if t.get("action") == "buy" else "卖出",
                "股票": f'{t.get("name","")}({t.get("code","")})',
                "价格": t.get("price", 0),
                "数量": t.get("shares", 0),
                "金额": t.get("amount", 0),
                "策略": t.get("strategy", ""),
                "原因": str(t.get("reason", ""))[:30],
            })
        df = pd.DataFrame(df_data)

        def highlight_action(val):
            if val == "买入":
                return "color: #ef5350; font-weight: 600;"
            elif val == "卖出":
                return "color: #26a69a; font-weight: 600;"
            return ""

        styled = df.style.map(highlight_action, subset=["操作"])
        st.dataframe(styled, use_container_width=True, hide_index=True, height=400)
    else:
        st.info("暂无交易记录")

# ── 右列: 候选股 + 日志 ──
with right:
    st.markdown("### 🎯 三策略候选")
    # candidates已在选股防御警告处获取
    if candidates:
        sb_cands = [c for c in candidates if c.get("_strategy") == "B"]
        sa_cands = [c for c in candidates if c.get("_strategy") == "A"]

        # 持仓代码，已持仓的候选标灰
        held_codes = set(p.get("code", "") for p in get_sim_positions())

        # --- 策略B: 按可买性排序 ---
        if sb_cands:
            # 分三档: 可买(未涨停且未持仓) > 已持仓 > 已涨停
            buyable = []
            held_list = []
            limit_up = []
            for c in sb_cands:
                code = c.get("code", "")
                chg = c.get("realtime_chg", c.get("change_pct", c.get("pct", 0)))
                source = c.get("source", c.get("_source", ""))

                if code in held_codes:
                    held_list.append(c)
                elif abs(chg) >= 9.5 or source == "连板龙头":
                    # 连板龙头大多是涨停状态
                    if abs(chg) >= 9.5:
                        limit_up.append(c)
                    else:
                        buyable.append(c)
                else:
                    buyable.append(c)

            st.markdown('<span style="color:#ff9800;font-size:0.8rem;">🔥 策略B · 市场驱动</span>', unsafe_allow_html=True)

            # 可买候选 — 最重要，置顶
            if buyable:
                st.markdown('<div style="color:#4caf50;font-size:0.7rem;margin:2px 0;">✅ 可买入</div>', unsafe_allow_html=True)
                for c in buyable[:5]:
                    _render_candidate(c, held=False)

            # 已持仓
            if held_list:
                st.markdown('<div style="color:#2196f3;font-size:0.7rem;margin:2px 0;">📦 已持仓</div>', unsafe_allow_html=True)
                for c in held_list[:3]:
                    _render_candidate(c, held=True)

            # 已涨停 — 明日关注
            if limit_up:
                with st.expander(f"🔒 已涨停 {len(limit_up)}只（明日关注）", expanded=False):
                    for c in limit_up[:8]:
                        _render_candidate_compact(c)

        # --- 策略A (首阴反包日内) ---
        st.markdown('<span style="color:#42a5f5;font-size:0.8rem;">策略A · 首阴反包反包(3万)</span>', unsafe_allow_html=True)

        # --- 策略C (缩量反包) ---
        sc_cands = [c for c in candidates if c.get("_strategy") == "C"]
        if sc_cands:
            st.markdown('<span style="color:#666;font-size:0.8rem;">✅ 策略C · 缩量反包(3万·3只 PF=1.25)</span>', unsafe_allow_html=True)
            for c in sc_cands[:5]:
                name = c.get("name", "")
                code = c.get("code", "")
                price = c.get("price", c.get("realtime_price", 0))
                reason = c.get("reason", "题材龙头")
                st.markdown(
                    f'<div style="background:#1a2332;border:1px solid #ff9800;border-radius:6px;padding:5px 10px;margin:2px 0;'
                    f'font-size:0.85rem;">'
                    f'<strong>{name}</strong> '
                    f'<span style="color:#808080;">{code}</span> '
                    f'<span style="color:#ff9800;font-size:0.75rem;">{reason}</span> '
                    f'<span style="color:#808080;">¥{price:.2f}</span>'
                    f'</div>',
                    unsafe_allow_html=True,
                )

        if not sb_cands and not sa_cands and not sc_cands:
            st.info("暂无候选")
    else:
        st.info("暂无候选")

    # 守护进程日志 — 折叠展示
    with st.expander("📝 实时日志", expanded=False):
        logs = get_daemon_log(30)
        if logs:
            log_text = "\n".join(logs[-20:])
            st.code(log_text, language="log")
        else:
            st.caption("暂无日志")
