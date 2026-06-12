"""系统设置 — 持仓编辑 + 策略参数 + 数据管理 + 压测报告 + Cron状态"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import streamlit as st
import json
from datetime import datetime

from web.styles import inject_styles

inject_styles()

st.markdown("## ⚙️ 系统设置")

PORTFOLIO_PATH = Path(__file__).parent.parent.parent / "data" / "portfolio.json"
CONFIG_PATH = Path(__file__).parent.parent.parent / "config" / "strategy_params.json"
REPORT_DIR = Path(__file__).parent.parent.parent / "output" / "test_reports"

tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "持仓管理", "策略参数", "数据管理", "压测报告", "Cron状态", "运行日志"
])

def _save_portfolio(portfolio: dict):
    """保存持仓,保持原始JSON格式"""
    if PORTFOLIO_PATH.exists():
        raw = json.loads(PORTFOLIO_PATH.read_text())
    else:
        raw = {}
    raw["positions"] = list(portfolio.values())
    PORTFOLIO_PATH.write_text(json.dumps(raw, ensure_ascii=False, indent=2))

# ============================================================
# Tab1: 持仓管理
# ============================================================
with tab1:
    st.markdown("### 📊 实盘持仓编辑")

    portfolio = {}
    if PORTFOLIO_PATH.exists():
        raw = json.loads(PORTFOLIO_PATH.read_text())
        if "positions" in raw:
            portfolio = {p["code"]: p for p in raw["positions"]}
        else:
            portfolio = {k: v for k, v in raw.items() if isinstance(v, dict)}

    if portfolio:
        st.markdown("#### 当前持仓")
        for code, info in portfolio.items():
            cols = st.columns([2, 2, 1, 1, 1, 1])
            with cols[0]:
                st.text(f'{info.get("name","")}({code})')
            with cols[1]:
                st.text(f'{info.get("shares",0)}股')
            with cols[2]:
                st.text(f'成本¥{info.get("buy_price", info.get("cost",0)):.2f}')
            with cols[3]:
                st.text(f'止损¥{info.get("stop_loss", info.get("stop",0)):.2f}')
            with cols[4]:
                highest = info.get("highest_price", info.get("buy_price", 0))
                st.text(f'最高¥{highest:.2f}')
            with cols[5]:
                if st.button("删除", key=f"del_{code}"):
                    del portfolio[code]
                    _save_portfolio(portfolio)
                    st.success(f"已删除 {code}")
                    st.rerun()

    # 添加新持仓
    st.markdown("#### 添加持仓")
    with st.form("add_position"):
        new_code = st.text_input("股票代码", placeholder="300059")
        new_name = st.text_input("股票名称", placeholder="东方财富")
        new_shares = st.number_input("持仓数量(股)", min_value=100, step=100)
        new_cost = st.number_input("成本价", min_value=0.01, step=0.01, format="%.3f")
        new_stop = st.number_input("止损价", min_value=0.01, step=0.01, format="%.2f")

        if st.form_submit_button("添加"):
            if new_code and new_name:
                portfolio[new_code] = {
                    "code": new_code,
                    "name": new_name,
                    "buy_price": float(new_cost),
                    "shares": int(new_shares),
                    "stop": float(new_stop),
                    "stop_loss": float(new_stop),
                    "cost": float(new_cost) * int(new_shares),
                    "highest_price": float(new_cost),
                    "reason": "",
                    "buy_date": datetime.now().strftime("%Y-%m-%d"),
                }
                _save_portfolio(portfolio)
                st.success(f"已添加 {new_name}({new_code})")
                st.rerun()

# ============================================================
# Tab2: 策略参数
# ============================================================
with tab2:
    st.markdown("### 🎯 当前策略参数(代码常量)")

    # 从代码读取实际参数展示
    st.markdown("""
    | 参数 | 当前值 | 说明 |
    |------|--------|------|
    | **资金与期次** | | |
    | CURRENT_PERIOD | 3 | 正式期(5/22起, 三策略等分A:3万+B:3万+C:3万) |
    | INITIAL_CAPITAL | ¥90,000(三等分A:3万+B:3万+C:3万) |
    | A_MAX_POSITIONS | 3 | A策略独立3只 |
    | B_MAX_POSITIONS | 3 | B策略独立3只 |
    | **配仓** | | |
    | A_POSITION_RATIO | 33%/只(¥10,000) | A首阴反包按比例 |
    | B_POSITION_RATIO | 33%/只(¥13,000) | B回踩低吸按比例 |
    | **止损** | | |
    | A止损 | 跌破首阴最低价(兜底-5%) / 3天到期清仓 | 首阴反包 |
    | B止损 | -3% | 回踩低吸 |
    | **移动止盈(策略A)** | | |
    | 正常/退潮/冰点 | 3%/2%/1.5% | 从最高点回落触发 |
    | **移动止盈(策略B)** | | |
    | 正常/退潮/冰点 | 3%/2%/1.5% | 从最高点回落触发 |
    | **持有时间** | | |
    | 策略A | 持2-3天,跌破首阴低止损 | 首阴反包(3万) |
    | 策略B | 持2天+止损-3% | 回踩低吸·首板回踩主力成本 |
    | **退潮保护(新)** | | |
    | 涨停数阈值 | <50不开仓(冰点), >150分化 | 783笔验证PF=0.38-0.93 |
    | 涨跌比阈值 | <30%不开仓 | PF=0.63 |
    | 大盘急跌 | -2%强制清仓 | 所有持仓 |
    | **回撤控制(新)** | | |
    | 连亏暂停 | 3笔 | 暂停1天 |
    | 月亏降仓 | >5% | 仓位减半 |
    | 日限亏 | -¥1,800 | 当天停止买入 |
    | **执行优化(新)** | | |
    | 盘中轮询 | 15秒(原60秒) | 策略B实时回踩 |
    | 回踩执行 | 30秒(原120秒) | 回踩到涨停开盘价±1% |
    | watchlist | 30只精选前5 | 按回踩时机+封板质量排序 |
    | **精选排序(新)** | | |
    | 策略A | 龙头评分>=40+实体<3%+下影线 | 7来源调研 |
    | 策略B | 回踩时机+封板质量+只做首板 | 7200笔验证 |
    | **风控** | | |
    | GRACE_PERIOD | 开盘30分钟 | 止损延迟,等波动消化 |
    | HARD_STOP_PCT | -10% | 硬止损,grace period内也执行 |
    | MAX_SAME_INDUSTRY | 2只 | 同行业集中度上限 |
    | 屏蔽 | 688/689/8/9/200/900 | 科创+北交+B股 |
    | 成交额阈值 | ¥100万 | 开盘前30分钟 |

    > ⚠️ 参数修改需编辑 `src/trader/trading_daemon.py` 顶部的常量, 修改后重启守护进程生效。
    """)

# ============================================================
# Tab3: 数据管理
# ============================================================
with tab3:
    st.markdown("### 📦 数据管理")

    col1, col2 = st.columns(2)
    with col1:
        if st.button("🔄 采集今日数据", use_container_width=True, key="settings_collect"):
            import subprocess
            with st.spinner("采集中..."):
                result = subprocess.run(
                    ["uv", "run", "python", "-m", "cli.collect", "--today"],
                    capture_output=True, text=True, timeout=300,
                    cwd=str(Path(__file__).parent.parent.parent),
                )
                if result.returncode == 0:
                    st.success("采集完成")
                else:
                    st.error(f"失败: {result.stderr[:300]}")

        if st.button("🤖 训练ML模型", use_container_width=True, key="settings_train"):
            import subprocess
            with st.spinner("训练中(可能需要几分钟)..."):
                result = subprocess.run(
                    ["uv", "run", "python", "-m", "cli.ml_model", "train"],
                    capture_output=True, text=True, timeout=300,
                    cwd=str(Path(__file__).parent.parent.parent),
                )
                if result.returncode == 0:
                    st.success("训练完成")
                else:
                    st.error(f"失败: {result.stderr[:300]}")

    with col2:
        if st.button("📊 基本面数据采集", use_container_width=True, key="settings_predict"):
            import subprocess
            with st.spinner("采集解禁/增减持/财报数据..."):
                result = subprocess.run(
                    ["uv", "run", "python", "-c",
                     "from src.data.sources.fundamental import collect_all; collect_all()"],
                    capture_output=True, text=True, timeout=120,
                    cwd=str(Path(__file__).parent.parent.parent),
                )
                if result.returncode == 0:
                    st.success("基本面数据采集完成")
                else:
                    st.error(f"失败: {result.stderr[:300]}")

        if st.button("🧹 清除缓存", use_container_width=True, key="settings_clear_cache"):
            st.cache_data.clear()
            st.success("缓存已清除")

    # 数据库状态
    st.markdown("### 📊 数据库状态")
    db_path = Path(__file__).parent.parent.parent / "data" / "alpha_miner.db"
    if db_path.exists():
        size_mb = db_path.stat().st_size / 1024 / 1024
        st.text(f"数据库大小: {size_mb:.1f} MB")

        import sqlite3
        conn = sqlite3.connect(str(db_path))
        tables = ["daily_price", "zt_pool", "zb_pool", "lhb_detail", "fund_flow",
                  "news", "market_emotion", "factor_values", "daemon_positions",
                  "daemon_trades", "daemon_account"]
        for t in tables:
            try:
                cnt = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
                latest = ""
                try:
                    dt = conn.execute(f"SELECT MAX(date) FROM {t}").fetchone()[0]
                    latest = f"最新 {dt}" if dt else ""
                except Exception:
                    pass
                st.text(f"  {t}: {cnt:,}条 {latest}")
            except Exception:
                pass
        conn.close()

# ============================================================
# Tab4: 压测报告
# ============================================================
with tab4:
    st.markdown("### 🧪 压测报告 (42项)")

    # 运行压测按钮
    if st.button("▶️ 运行压测(约20秒)", use_container_width=True, key="run_stress_test"):
        import subprocess
        with st.spinner("压测运行中..."):
            result = subprocess.run(
                ["uv", "run", "python", "tests/test_data_integrity.py"],
                capture_output=True, text=True, timeout=120,
                cwd=str(Path(__file__).parent.parent.parent),
            )
            if "ALL PASS" in result.stdout:
                st.success("42项全通过!")
            else:
                st.warning("有失败项, 查看下方报告")
            st.code(result.stdout, language="log")

    # 显示最新报告
    latest_report = REPORT_DIR / "latest.txt"
    if latest_report.exists():
        st.markdown("#### 最新报告")
        report_text = latest_report.read_text()
        st.code(report_text, language="log")
    else:
        st.info("暂无压测报告。运行压测后会自动保存到 output/test_reports/")

    # 历史报告列表
    if REPORT_DIR.exists():
        reports = sorted(REPORT_DIR.glob("report_*.txt"), reverse=True)
        if len(reports) > 1:
            st.markdown("#### 历史报告")
            for r in reports[:5]:
                ts = r.stem.replace("report_", "")
                st.markdown(f"- {ts}: [{r.name}]({r})")

# ============================================================
# Tab5: Cron状态
# ============================================================
with tab5:
    st.markdown("### ⏰ Cron定时任务")

    # 显示cron配置
    st.markdown("""
    | 时间 | 任务 | 说明 |
    |------|------|------|
    | 周一至周五 09:25 | 基本面数据 | 策略A参考用 |
    | 周一至周五 15:00 | 数据采集+压测+日报 | scripts/daily_collect.py |

    ### 脚本位置
    - 基本面采集: `src/data/sources/fundamental.py`
    - 每日采集: `scripts/daily_collect.py`
    - 脚本内容包含: 数据采集 → 压测验证(42项) → 模拟盘日报 → 报告保存
    """)

    # Cron运行日志(从daily_collect输出)
    st.markdown("#### 最近运行记录")
    if REPORT_DIR.exists():
        daily_files = sorted(REPORT_DIR.glob("daily_*.txt"), reverse=True)
        if daily_files:
            for df in daily_files[:3]:
                date_str = df.stem.replace("daily_", "")
                content = df.read_text()
                lines = [l for l in content.strip().split("\n") if l.strip()]
                st.markdown(f"**{date_str}** ({len(lines)}行)")
                with st.expander("查看详情"):
                    st.code(content[:2000], language="log")
        else:
            st.info("暂无运行记录")
    else:
        st.info("output/test_reports/ 目录不存在")

# ============================================================
# Tab6: 运行日志
# ============================================================
with tab6:
    st.markdown("### 📋 守护进程日志")

    log_dir = Path(__file__).parent.parent.parent / "output" / "trader" / "daemon_logs"
    today = datetime.now().strftime("%Y-%m-%d")
    log_file = log_dir / f"daemon_{today}.log"

    if log_file.exists():
        lines = log_file.read_text().strip().split("\n")
        st.markdown(f"共 {len(lines)} 行 | 今日日志")

        # 显示最近100行
        display_lines = lines[-100:]
        st.code("\n".join(display_lines), language="log")
    else:
        st.info("今日无日志")

    # 历史日志
    all_logs = sorted(log_dir.glob("daemon_*.log"), reverse=True) if log_dir.exists() else []
    if len(all_logs) > 1:
        st.markdown("#### 历史日志")
        for lf in all_logs[1:7]:
            date_str = lf.stem.replace("daemon_", "")
            line_count = len(lf.read_text().strip().split("\n"))
            st.markdown(f"- {date_str}: {line_count}行")
