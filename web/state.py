"""全局状态管理 — 所有页面共享

使用方式:
  from web.state import init_state, select_stock, get_selected_stock, add_to_watchlist

  init_state()  # 页面开头调用一次
"""

import streamlit as st
from pathlib import Path
import json

PORTFOLIO_PATH = Path("data/portfolio.json")
TRADEPLAN_DIR = Path("output/recommendations")

# 默认持仓 (硬编码兜底，优先从portfolio.json读取)
# 默认持仓 — 统一从 portfolio.json 读取（同源）
from src.config.portfolio import get_legacy_portfolio_dict as _get_portfolio, get_cash as _get_cash
DEFAULT_PORTFOLIO = _get_portfolio()
DEFAULT_CASH = _get_cash()


def init_state():
    """初始化全局状态，每个页面开头调用"""
    # 持仓数据
    if "portfolio" not in st.session_state:
        st.session_state.portfolio = _load_portfolio()

    if "available_cash" not in st.session_state:
        st.session_state.available_cash = DEFAULT_CASH

    # 当前选中股票
    if "selected_stock" not in st.session_state:
        st.session_state.selected_stock = None
        st.session_state.selected_stock_name = None

    # 自选股
    if "watchlist" not in st.session_state:
        st.session_state.watchlist = _load_watchlist()

    # ML候选 (由11 ML选股页面写入)
    if "ml_candidates" not in st.session_state:
        st.session_state.ml_candidates = _load_ml_candidates()

    # 统一信号池
    if "signals" not in st.session_state:
        st.session_state.signals = []


def select_stock(code: str, name: str = None):
    """选中一只股票 — 其他页面响应"""
    st.session_state.selected_stock = code
    st.session_state.selected_stock_name = name or code


def get_selected_stock() -> tuple[str, str]:
    """获取当前选中股票 (code, name)"""
    code = st.session_state.get("selected_stock")
    name = st.session_state.get("selected_stock_name", code)
    return code, name


def add_to_watchlist(code: str, name: str, source: str = ""):
    """加入自选"""
    wl = st.session_state.get("watchlist", [])
    if code not in [w["code"] for w in wl]:
        wl.append({"code": code, "name": name, "source": source})
        st.session_state.watchlist = wl
        _save_watchlist(wl)


def remove_from_watchlist(code: str):
    """移除自选"""
    wl = st.session_state.get("watchlist", [])
    wl = [w for w in wl if w["code"] != code]
    st.session_state.watchlist = wl
    _save_watchlist(wl)


def get_portfolio() -> dict:
    """获取持仓数据"""
    return st.session_state.get("portfolio", DEFAULT_PORTFOLIO)


def get_cash() -> float:
    """获取可用现金"""
    return st.session_state.get("available_cash", DEFAULT_CASH)


def get_ml_candidates() -> list:
    """获取ML候选股"""
    return st.session_state.get("ml_candidates", [])


def get_watchlist() -> list:
    """获取自选股列表"""
    return st.session_state.get("watchlist", [])


# === 内部方法 ===

def _load_portfolio() -> dict:
    """从portfolio.json加载持仓"""
    try:
        if PORTFOLIO_PATH.exists():
            data = json.loads(PORTFOLIO_PATH.read_text())
            positions = data.get("positions", data)
            if isinstance(positions, dict) and len(positions) > 0:
                return positions
    except Exception:
        pass
    return DEFAULT_PORTFOLIO


def _load_watchlist() -> list:
    """加载自选股"""
    try:
        p = Path("data/watchlist.json")
        if p.exists():
            return json.loads(p.read_text())
    except Exception:
        pass
    return []


def _save_watchlist(watchlist: list):
    """保存自选股"""
    try:
        Path("data/watchlist.json").write_text(json.dumps(watchlist, ensure_ascii=False, indent=2))
    except Exception:
        pass


def _load_ml_candidates() -> list:
    """从trading_daemon的get_ml_candidates读取(统一5层过滤)"""
    try:
        from src.trader.trading_daemon import get_ml_candidates
        return get_ml_candidates()
    except Exception:
        pass
    # fallback: 直接读文件
    try:
        p = Path("output/ml/latest_prediction.json")
        if p.exists():
            data = json.loads(p.read_text())
            return data.get("top7", data.get("predictions", []))[:7]
    except Exception:
        pass
    return []
