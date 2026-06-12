"""因子看板 — 因子健康度监控

什么是因子？
  因子就是"选股维度"。比如"连板数"因子 — 如果连板多的股票明天涨得好，这个因子就有用。
  我们用IC(信息系数)来衡量因子质量: IC>0说明因子方向对, IC越高越准。

页面结构:
  Tab1: 因子体检表 — 每个因子的IC均值/胜率/趋势, 一眼看出哪个因子好用
  Tab2: IC趋势图 — 因子IC随时间的变化, 发现因子是否失效
  Tab3: 因子说明 — 每个因子是干什么的, 怎么用在交易里
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import streamlit as st
import pandas as pd
import json

from web.styles import inject_styles
from web.services.data_service import _conn

inject_styles()

st.markdown("## 🧬 因子看板")

# ============================================================
# 因子说明字典
# ============================================================
FACTOR_INFO = {
    "theme_crowding": {
        "name": "主题拥挤度",
        "desc": "衡量板块里有多少资金在扎堆。拥挤度高=大家都在追这个板块，可能快到顶了。",
        "trade_use": "拥挤度高时小心追高，拥挤度低时可能有机会",
    },
    "leader_clarity": {
        "name": "龙头清晰度",
        "desc": "板块里有没有明确的领涨股。龙头清晰=资金集中，板块更强。",
        "trade_use": "龙头清晰度高的板块更适合追入",
    },
    "consecutive_board": {
        "name": "连板数",
        "desc": "连续涨停的天数。连板越多，市场关注度越高。",
        "trade_use": "2连板以上值得关注，3连板以上是强势信号",
    },
    "lhb_institution": {
        "name": "龙虎榜机构",
        "desc": "机构买入力度。机构大买=专业资金看好。",
        "trade_use": "机构净买入的股票更有持续性",
    },
    "main_flow_intensity": {
        "name": "主力资金强度",
        "desc": "主力(大单)净流入占总成交的比例。正值=主力在买。",
        "trade_use": "主力持续流入的股票值得关注",
    },
    "narrative_velocity": {
        "name": "叙事速度",
        "desc": "新闻/消息传播的速度。传播快=市场关注度高。",
        "trade_use": "叙事加速的股票短期可能异动",
    },
    "theme_lifecycle": {
        "name": "主题生命周期",
        "desc": "判断板块处于萌芽/发酵/高潮/退潮哪个阶段。",
        "trade_use": "萌芽期潜伏，高潮期追龙头，退潮期离场",
    },
    "turnover_rank": {
        "name": "换手率排名",
        "desc": "股票换手率在全市场的百分位排名。换手率高=交易活跃。",
        "trade_use": "换手率突然放大可能是启动信号",
    },
}

# IC质量评级
def ic_grade(ic_mean, count):
    """根据IC均值和样本数评级"""
    if count < 5:
        return "⚠️", "数据不足"
    if ic_mean > 0.05:
        return "🟢", "优秀"
    elif ic_mean > 0.03:
        return "🟢", "良好"
    elif ic_mean > 0.01:
        return "🟡", "一般"
    elif ic_mean > -0.01:
        return "⚪", "无效"
    else:
        return "🔴", "反向"

# ============================================================
# 加载IC数据
# ============================================================
@st.cache_data(ttl=300, show_spinner=False)
def load_ic_data():
    try:
        conn = _conn()
        df = pd.read_sql(
            "SELECT factor_name, trade_date, ic_value, forward_days FROM ic_series ORDER BY trade_date DESC",
            conn
        )
        conn.close()
        return df
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=300, show_spinner=False)
def load_feature_importance():
    """从LightGBM模型文件读特征重要性"""
    try:
        import lightgbm as lgb
        model_path = Path(__file__).parent.parent.parent / "output" / "ml" / "latest_model.txt"
        meta_path = Path(__file__).parent.parent.parent / "output" / "ml" / "latest_model_meta.json"

        if not model_path.exists():
            return []

        model = lgb.Booster(model_file=str(model_path))
        imp = model.feature_importance(importance_type='gain')
        col_names = model.feature_name()

        # 读取feature_cols映射 Column_N -> 真实特征名
        real_names = {}
        if meta_path.exists():
            meta = json.loads(meta_path.read_text())
            feature_cols = meta.get("feature_cols", [])
            for i, name in enumerate(feature_cols):
                real_names[f"Column_{i}"] = name

        pairs = sorted(
            [(real_names.get(cn, cn), float(score)) for cn, score in zip(col_names, imp)],
            key=lambda x: x[1], reverse=True
        )
        return pairs
    except Exception:
        return []

ic_df = load_ic_data()

tab1, tab2, tab3, tab4 = st.tabs(["因子体检表", "IC趋势", "因子说明", "策略A(龙头首阴)"])

# ============================================================
# Tab1: 因子体检表
# ============================================================
with tab1:
    st.markdown("""
    ### 因子体检表

    **怎么看：** IC均值越高(>0.03)因子越有用，IC方向对(正数)=因子预测方向正确。
    **胜率：** IC>0的天数占比，>55%算及格。
    """)

    if not ic_df.empty:
        # 计算每个因子的统计
        stats = []
        for fname in ic_df["factor_name"].unique():
            sub = ic_df[ic_df["factor_name"] == fname]
            ic_mean = sub["ic_value"].mean()
            ic_std = sub["ic_value"].std()
            count = len(sub)
            win_rate = (sub["ic_value"] > 0).mean()
            icir = ic_mean / ic_std if ic_std > 0 else 0
            latest_ic = sub.iloc[0]["ic_value"]
            latest_date = sub.iloc[0]["trade_date"]

            grade_icon, grade_text = ic_grade(ic_mean, count)
            info = FACTOR_INFO.get(fname, {})
            display_name = info.get("name", fname)

            stats.append({
                "评级": f"{grade_icon} {grade_text}",
                "因子": display_name,
                "代码": fname,
                "IC均值": round(ic_mean, 4),
                "ICIR": round(icir, 2),
                "胜率": f"{win_rate:.0%}",
                "样本数": count,
                "最新IC": round(latest_ic, 4),
                "最新日期": latest_date,
            })

        stats_df = pd.DataFrame(stats)
        st.dataframe(
            stats_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "IC均值": st.column_config.NumberColumn(format="%.4f"),
                "ICIR": st.column_config.NumberColumn(format="%.2f"),
                "最新IC": st.column_config.NumberColumn(format="%.4f"),
            }
        )
    else:
        st.info("暂无IC数据。运行 `uv run python -m cli.backtest --compute-all` 生成。")

# ============================================================
# Tab2: IC趋势图
# ============================================================
with tab2:
    st.markdown("""
    ### IC趋势图

    **怎么看：** 选一个因子，看IC随时间变化。IC长期在0以上=因子稳定有效。
    IC突然下降=因子可能在失效(漂移)。
    """)

    if not ic_df.empty:
        factors = ic_df["factor_name"].unique()
        factor_options = {FACTOR_INFO.get(f, {}).get("name", f): f for f in factors}
        selected = st.selectbox("选择因子", list(factor_options.keys()), key="ic_factor")
        fname = factor_options[selected]

        sub = ic_df[ic_df["factor_name"] == fname].sort_values("trade_date")

        if len(sub) > 1:
            import plotly.graph_objects as go
            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=sub["trade_date"],
                y=sub["ic_value"],
                name="IC值",
                marker_color=["#26a69a" if v > 0 else "#ef5350" for v in sub["ic_value"]],
            ))
            fig.add_hline(y=0, line_dash="dash", line_color="#808080")
            fig.add_hline(y=0.03, line_dash="dot", line_color="#26a69a",
                          annotation_text="良好线 0.03")
            fig.update_layout(
                title=f"{selected} IC趋势",
                xaxis_title="日期",
                yaxis_title="IC值",
                height=400,
                paper_bgcolor="#0e1117",
                plot_bgcolor="#0e1117",
                font_color="#e0e0e0",
                margin=dict(l=40, r=20, t=50, b=40),
            )
            st.plotly_chart(fig, use_container_width=True)

            # 简单趋势判断
            recent = sub.head(5)["ic_value"].mean()
            older = sub.tail(max(5, len(sub)))["ic_value"].mean() if len(sub) > 5 else recent
            if recent > 0.03:
                st.success(f"近期IC均值 {recent:.4f} — 因子状态良好")
            elif recent > 0:
                st.warning(f"近期IC均值 {recent:.4f} — 因子偏弱，注意监控")
            else:
                st.error(f"近期IC均值 {recent:.4f} — 因子可能失效，建议检查")
        else:
            st.info(f"{selected} 数据点不足({len(sub)}条)，无法画趋势图")
    else:
        st.info("暂无IC数据")

# ============================================================
# Tab3: 因子说明
# ============================================================
with tab3:
    st.markdown("""
    ### 因子说明

    **什么是因子？** 因子就是选股的维度。我们系统有8个因子，分两类：

    - **公式因子(5个)**: 纯数学计算，从K线/资金数据算出来 — 连板数/换手率排名/主力资金/龙虎榜/主题拥挤
    - **叙事因子(3个)**: 从新闻/舆情算出来 — 叙事速度/主题生命周期/龙头清晰度

    **因子怎么用在交易里？** ML模型把79个特征(含8个因子值)喂给LightGBM，预测明天哪些股票涨得好。
    """)
    st.divider()

    for fname, info in FACTOR_INFO.items():
        # 查这个因子的IC统计
        if not ic_df.empty and fname in ic_df["factor_name"].values:
            sub = ic_df[ic_df["factor_name"] == fname]
            ic_mean = sub["ic_value"].mean()
            count = len(sub)
            grade_icon, grade_text = ic_grade(ic_mean, count)
            status = f"{grade_icon} IC={ic_mean:.4f} ({grade_text})"
        else:
            status = "⚪ 暂无数据"

        with st.expander(f"{info['name']} — {status}"):
            st.markdown(f"**代码:** `{fname}`")
            st.markdown(f"**含义:** {info['desc']}")
            st.markdown(f"**实战用法:** {info['trade_use']}")

    # 特征重要性TOP20
    st.divider()
    st.markdown("### ML模型特征重要性 TOP20")
    feat_imp = load_feature_importance()
    if feat_imp:
        df_imp = pd.DataFrame(feat_imp[:20], columns=["特征名", "重要性(gain)"])
        st.dataframe(df_imp, use_container_width=True, hide_index=True)
    else:
        st.info("需要LightGBM模型文件才能显示特征重要性")

    st.caption("因子IC越高，ML模型选股越准。IC长期为负的因子可以考虑剔除。")

# ============================================================
# Tab4: 策略A(龙头首阴反包)
# ============================================================
with tab4:
    st.markdown("""
    ### 策略A — 龙头首阴反包(3万)

    策略A已改为**龙头首阴反包策略(7来源调研驱动, 龙头评分+形态确认)。

    **选股条件：**
    - 涨停池中2连板龙头(consecutive_zt=2)
    - 涨停后首个收阴日(close<open 且 close<prev_close)

    **持仓规则：**
    - 分配资金：3万(独立)
    - 次日高开2%+翻红确认买入, 持2-3天, trailing 3%/退潮1.5%止盈, 跌破首阴低×0.98止损
    - 止损: 盘中跌破首阴最低价-2%

    > 回测: 331笔 PF=1.86, 均赚+2.05%/笔, 扣成本仍盈利
    """)

