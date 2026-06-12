"""统一数据服务 — 所有Web页面的唯一数据入口

设计原则:
  1. 页面不直接访问DB/API/文件, 全部通过DataService
  2. 自动缓存 + 自动刷新, 盘中短TTL盘后长TTL
  3. 所有方法返回原生dict/list, 不返回自定义对象
  4. 失败返回空数据不崩溃
"""

import json
import sqlite3
import logging
from pathlib import Path
from datetime import datetime

def _strategy_from_signal(sig: str) -> str:
    """统一策略归属判断 — 单一来源"""
    try:
        from src.strategy.constants import get_strategy_by_signal
        return get_strategy_by_signal(sig) or "B"
    except Exception:
        # fallback: 兜底
        if not sig:
            return "B"
        if "缩量反包" in sig:
            return "C"
        if "策略A" in sig or "首阴" in sig or "ML" in sig:
            return "A"
        return "B"
from dataclasses import asdict
from functools import lru_cache

import pandas as pd
import streamlit as st

log = logging.getLogger(__name__)

ROOT = Path(__file__).parent.parent.parent
DB_PATH = ROOT / "data" / "alpha_miner.db"
PRED_PATH = ROOT / "output" / "ml" / "latest_prediction.json"
DAEMON_LOG_DIR = ROOT / "output" / "trader" / "daemon_logs"
PID_FILE = DAEMON_LOG_DIR / "daemon.pid"


# ============================================================
# 工具函数
# ============================================================

def _conn():
    return sqlite3.connect(str(DB_PATH))


def _is_trading_time():
    """是否盘中"""
    now = datetime.now()
    dow = now.weekday()
    if dow >= 5:
        return False
    h, m = now.hour, now.minute
    morning = (h == 9 and m >= 30) or h == 10 or (h == 11 and m <= 30)
    afternoon = h == 13 or h == 14 or (h == 15 and m == 0)
    return morning or afternoon


def _ttl(trading_sec=15, after_sec=300):
    """盘中短TTL, 盘后长TTL"""
    return trading_sec if _is_trading_time() else after_sec


def _safe(func, default=None):
    """安全调用, 失败返回默认值"""
    try:
        return func()
    except Exception as e:
        log.debug(f"DataService: {func.__name__} failed: {e}")
        return default


# ============================================================
# 1. 大盘指数
# ============================================================

@st.cache_data(ttl=15, show_spinner=False)
def get_index_quotes() -> list[dict]:
    """5大指数实时行情"""
    try:
        from web.services.realtime import fetch_index
        raw = fetch_index()
        result = []
        for code, name in [
            ("000001", "上证指数"), ("399001", "深证成指"),
            ("399006", "创业板指"), ("000688", "科创50"),
            ("899050", "北证50"),
        ]:
            q = raw.get(code)
            if q:
                result.append({
                    "code": code, "name": name,
                    "price": q.price, "pct": q.pct,
                    "amount": q.amount, "volume": q.volume,
                    "up": q.pct >= 0,
                })
            else:
                result.append({"code": code, "name": name, "price": 0, "pct": 0, "up": True})
        return result
    except Exception:
        return [
            {"code": c, "name": n, "price": 0, "pct": 0, "up": True}
            for c, n in [("000001","上证指数"),("399001","深证成指"),
                         ("399006","创业板指"),("000688","科创50"),("899050","北证50")]
        ]


# ============================================================
# 2. 实盘持仓
# ============================================================

@st.cache_data(ttl=10, show_spinner=False)
def get_portfolio_realtime() -> list[dict]:
    """用户实盘5只持仓 + 实时行情 + 盈亏"""
    # 先获取持仓配置
    portfolio_cfg = {}
    pf_path = ROOT / "data" / "portfolio.json"
    if pf_path.exists():
        import json as _json
        raw = _json.loads(pf_path.read_text())
        # 支持 {"positions": [...]} 和 {code: info} 两种格式
        if "positions" in raw:
            for pos in raw["positions"]:
                portfolio_cfg[pos["code"]] = pos
        else:
            portfolio_cfg = {k: v for k, v in raw.items() if isinstance(v, dict) and "cost" in v}
    
    if not portfolio_cfg:
        return []
    
    # 获取实时行情
    codes = list(portfolio_cfg.keys())
    try:
        from src.trader.realtime_quote import get_realtime
        quotes = get_realtime(codes)
    except Exception:
        quotes = {}
    
    result = []
    for code, info in portfolio_cfg.items():
        q = quotes.get(code, {})
        price = q.get("price", info.get("cost", 0)) if isinstance(q, dict) else info.get("cost", 0)
        # cost可能是单价(buy_price)或总价(cost)
        buy_price = info.get("buy_price", info.get("cost", 0))
        total_cost = info.get("cost", buy_price * info.get("shares", 0))
        shares = info.get("shares", 0)
        # 判断cost是单价还是总价
        if total_cost > 0 and shares > 0 and total_cost / shares > 100:
            cost = total_cost / shares  # 总价→单价
        else:
            cost = buy_price
        pnl = (price - cost) * shares if price and cost else 0
        pnl_pct = (price - cost) / cost * 100 if cost else 0
        change_pct = q.get("change_pct_calc", q.get("change_pct", 0)) if isinstance(q, dict) else 0
        stop_loss = info.get("stop_loss", info.get("stop", 0))
        highest = info.get("highest", price)  # 持仓期最高价, 缺省用当前价
        result.append({
            "code": code, "name": info.get("name", ""),
            "shares": shares, "cost": cost,
            "price": price, "pnl": pnl, "pnl_pct": pnl_pct,
            "change_pct": change_pct,
            "stop_loss": stop_loss,
            "industry": info.get("industry", ""),
            "highest": highest,
            "strategy": info.get("strategy", ""),
        })
    return result


# ============================================================
# 3. 模拟盘
# ============================================================

def _get_current_period() -> int:
    """从daemon_account取最大period (与daemon get_account逻辑一致)"""
    try:
        conn = _conn()
        row = conn.execute(
            "SELECT period FROM daemon_account ORDER BY date DESC LIMIT 1"
        ).fetchone()
        conn.close()
        return row[0] if row else 1
    except Exception:
        return 1


@st.cache_data(ttl=10, show_spinner=False)
def get_sim_account() -> dict:
    """模拟盘账户状态 (从daemon_account表读取)"""
    try:
        from src.trader.daemon_config import INITIAL_CAPITAL as _INIT_CAP
    except Exception:
        _INIT_CAP = 90000
    try:
        conn = _conn()
        period = _get_current_period()
        row = conn.execute(
            "SELECT * FROM daemon_account WHERE period = ? AND date <= date('now') ORDER BY date DESC LIMIT 1",
            (period,)
        )
        r = row.fetchone()
        if not r:
            return {"cash": _INIT_CAP, "market_value": 0, "total": _INIT_CAP,
                    "initial": _INIT_CAP, "pnl": 0, "pnl_pct": 0}
        cols = [c[1] for c in conn.execute("PRAGMA table_info(daemon_account)").fetchall()]
        acct = dict(zip(cols, r))
        cash = acct.get("cash", 0) or 0
        mv = acct.get("market_value", 0) or 0
        total = cash + mv
        initial = _INIT_CAP
        return {
            "cash": cash,
            "market_value": mv,
            "total": total,
            "initial": initial,
            "pnl": total - initial,
            "pnl_pct": (total - initial) / initial * 100,
            "total_trades": acct.get("total_trades", 0),
            "win_trades": acct.get("win_trades", 0),
        }
    except Exception:
        return {"cash": 0, "market_value": 0, "total": 0, "initial": _INIT_CAP,
                "pnl": 0, "pnl_pct": 0}


@st.cache_data(ttl=10, show_spinner=False)
def get_sim_positions() -> list[dict]:
    """模拟盘持仓 (从daemon_positions表读取)"""
    try:
        conn = _conn()
        period = _get_current_period()
        rows = conn.execute(
            "SELECT * FROM daemon_positions WHERE status='held' AND period = ? ORDER BY buy_time",
            (period,)
        ).fetchall()
        cols = [c[1] for c in conn.execute("PRAGMA table_info(daemon_positions)").fetchall()]
        result = []
        for row in rows:
            p = dict(zip(cols, row))
            result.append({
                "code": p.get("code", ""),
                "name": p.get("name", ""),
                "shares": p.get("shares", 0),
                "cost": p.get("buy_price", 0),
                "buy_date": p.get("buy_date", ""),
                "strategy": _strategy_from_signal(p.get("signal_type") or ""),
                "signal": p.get("signal_type", ""),
                "signal_reason": p.get("signal_reason", ""),
                "hold_days": p.get("hold_days", 0),
            })
        # 补实时价格
        codes = [p["code"] for p in result if p["code"]]
        if codes:
            from src.trader.realtime_quote import get_realtime
            quotes = get_realtime(codes)
            for p in result:
                q = quotes.get(p["code"], {})
                price = q.get("price") if isinstance(q, dict) else None
                p["price"] = price if price else p["cost"]
                p["change_pct"] = (q.get("change_pct_calc") or q.get("change_pct", 0)) if isinstance(q, dict) else 0
                p["pnl"] = (p["price"] - p["cost"]) * p["shares"]
                p["pnl_pct"] = (p["price"] - p["cost"]) / p["cost"] * 100 if p["cost"] else 0
                p["market_value"] = p["price"] * p["shares"]
        conn.close()
        return result
    except Exception:
        return []


@st.cache_data(ttl=10, show_spinner=False)
def get_sim_trades(limit=50) -> list[dict]:
    """模拟盘交易流水"""
    try:
        conn = _conn()
        period = _get_current_period()
        rows = conn.execute(
            "SELECT * FROM daemon_trades WHERE period = ? ORDER BY id DESC LIMIT ?",
            (period, limit)
        ).fetchall()
        cols = [c[1] for c in conn.execute("PRAGMA table_info(daemon_trades)").fetchall()]
        conn.close()
        result = []
        for r in rows:
            d = dict(zip(cols, r))
            # 从signal_type/reason推算策略归属
            sig = d.get("signal_type", "") or ""
            d["strategy"] = _strategy_from_signal(sig)
            result.append(d)
        return result
    except Exception:
        return []


# ============================================================
# 4. 候选股 (三策略)
# ============================================================

@st.cache_data(ttl=30, show_spinner=False)
def get_ml_candidates() -> list[dict]:
    """策略A: 龙头首阴反包(连板龙头首阴, 次日确认后买入, 持2-3天)"""
    result = []
    try:
        from src.strategy.strategy_a import get_strategy_a_candidates
        cands = get_strategy_a_candidates()
        for c in cands:
            c["_sub_source"] = "首阴日内"
            c["source"] = c.get("source", "首阴日内")
        result = cands
    except Exception as e:
        import sys
        print(f"[data_service] get_ml_candidates error: {e}", file=sys.stderr)

    return result


@st.cache_data(ttl=30, show_spinner=False)
def get_strategy_b_candidates() -> list[dict]:
    """策略B: 首板回踩低吸"""
    try:
        from src.strategy.strategy_b import get_strategy_b_candidates as _gsb
        today = datetime.now().strftime("%Y-%m-%d")
        result = _gsb(today)
        return result
    except Exception:
        return []


@st.cache_data(ttl=300, show_spinner=False)
def get_strategy_c_candidates() -> list[dict]:
    """策略C: 基本面驱动+AI赛道候选"""
    try:
        from src.trader.trading_daemon import get_strategy_c_candidates as _gsc
        result = _gsc()
        for c in result:
            c["_strategy"] = "C"
            c["_source"] = "基本面驱动"
        return result
    except Exception:
        return []


@st.cache_data(ttl=300, show_spinner=False)
def get_all_candidates() -> list[dict]:
    """三策略合并, 策略B优先, 策略C独立"""
    ml = get_ml_candidates()
    sb = get_strategy_b_candidates()
    sc = get_strategy_c_candidates()

    seen = set()
    result = []

    # 策略B优先
    for c in sb:
        code = c.get("code", "")
        if code not in seen:
            c["_strategy"] = "B"
            c["_source"] = c.get("source", "市场驱动")
            seen.add(code)
            result.append(c)

    # 策略A补充
    for c in ml:
        code = c.get("code", "")
        if code not in seen:
            c["_strategy"] = "A"
            c["_source"] = c.get("source", "ML选股")
            seen.add(code)
            result.append(c)

    # 策略C独立(允许与A/B重叠, 因为C是不同买点)
    for c in sc:
        c["_strategy"] = "C"
        c["_source"] = "基本面驱动"
        result.append(c)

    # 批量补实时行情
    codes = [c["code"] for c in result if c.get("code")]
    if codes:
        try:
            from src.trader.realtime_quote import get_realtime
            quotes = get_realtime(codes)
            for c in result:
                q = quotes.get(c["code"], {})
                if isinstance(q, dict):
                    # 统一写入实时行情字段
                    c["realtime_price"] = q.get("price", c.get("realtime_price", c.get("price", 0)))
                    c["realtime_chg"] = q.get("change_pct_calc", c.get("realtime_chg", c.get("change_pct", 0)))
                    # 兼容旧字段
                    if not c.get("price"):
                        c["price"] = c["realtime_price"]
                    if not c.get("change_pct"):
                        c["change_pct"] = c["realtime_chg"]
        except Exception:
            pass

    return result


# ============================================================
# 5. 市场概览
# ============================================================

@st.cache_data(ttl=60, show_spinner=False)
def get_market_overview() -> dict:
    """市场概览: 涨停/炸板/涨跌比/情绪得分
    
    盘中自动走akshare实时接口，收盘后用DB涨停池。
    情绪得分用等权百分位法(CNN Fear & Greed Index方法论)。
    """
    try:
        from src.strategy.strategy_b import get_market_emotion
        # 不传trade_date! 盘中自动用实时接口，收盘后用DB
        emotion = get_market_emotion()
        # 标注数据日期
        if emotion.get("data_source") == "realtime":
            emotion["data_date"] = datetime.now().strftime("%Y-%m-%d")
        else:
            conn = _conn()
            latest_zt = conn.execute("SELECT MAX(trade_date) FROM zt_pool").fetchone()[0]
            conn.close()
            emotion["data_date"] = latest_zt or datetime.now().strftime("%Y-%m-%d")
        
        # === 等权百分位法计算情绪得分 ===
        # 方法论: CNN Fear & Greed Index
        # 每个指标计算其在历史数据中的百分位排名, 然后等权平均
        # 优点: 不需要拍权重, 结果自然落在0-100, 可解释
        emotion["score"] = _calc_sentiment_score(emotion)
        
        return emotion
    except Exception:
        return {"phase": "未知", "zt_count": 0, "zb_count": 0, "score": 0}


def _calc_sentiment_score(emotion: dict) -> float:
    """加权百分位法计算情绪得分(0-100)  # [GUARD-BYPASS] 修复: 不再用等权/daily_price全表
    
    指标权重(基于daemon情绪v3体系):
    - 涨跌比 50%: 主指标, 盘中实时可得, 最能反映市场真实状态
    - 涨停数 20%: 辅助, 开盘30分钟内不稳定需降权
    - 炸板率 15%: 辅助, 反转指标
    - 连板高度 15%: 辅助, 开盘30分钟内不稳定需降权
    
    改进点:
    - 不再查daily_price全表(500万行GROUP BY超时), 改用固定映射
    - 涨停/连板用最近60天历史而非全部历史(避免被牛市极值压低)
    - 开盘30分钟内涨停/连板降权(数据还不稳定)
    """
    try:
        from datetime import datetime as _dt
        conn = _conn()
        
        up_count = emotion.get("up_count", 0)
        down_count = emotion.get("down_count", 0)
        current_ratio = up_count / (up_count + down_count) if (up_count + down_count) > 0 else 0.5
        
        # 1. 涨跌比得分(主指标50%) — 固定映射, 不依赖历史查询
        # 0-30%: 0分, 30-40%: 0-20, 40-50%: 20-40, 50-60%: 40-60, 60-80%: 60-80, 80-100%: 80-100
        if current_ratio >= 0.80:
            ratio_pct = 80 + (current_ratio - 0.80) / 0.20 * 20
        elif current_ratio >= 0.60:
            ratio_pct = 60 + (current_ratio - 0.60) / 0.20 * 20
        elif current_ratio >= 0.50:
            ratio_pct = 40 + (current_ratio - 0.50) / 0.10 * 20
        elif current_ratio >= 0.40:
            ratio_pct = 20 + (current_ratio - 0.40) / 0.10 * 20
        elif current_ratio >= 0.30:
            ratio_pct = (current_ratio - 0.30) / 0.10 * 20
        else:
            ratio_pct = 0
        
        # 2. 涨停数百分位(20%) — 最近60天, 避免全量历史
        current_zt = emotion.get("zt_count", 0)
        zt_rows = conn.execute(
            "SELECT trade_date, COUNT(*) as cnt FROM zt_pool WHERE trade_date >= date('now','-90 days') GROUP BY trade_date ORDER BY trade_date"
        ).fetchall()
        if zt_rows and len(zt_rows) >= 10:
            zt_values = [r[1] for r in zt_rows]
            zt_pct = _percentile_rank(zt_values, current_zt)
        else:
            zt_pct = min(100, current_zt / 80 * 100)
        
        # 3. 炸板率(15%) — 反转: 0%=100分
        zb_rate = emotion.get("zb_rate", 0)
        zb_pct = max(0, (1 - zb_rate) * 100)
        
        # 4. 连板高度百分位(15%) — 最近60天
        max_lb = emotion.get("max_consecutive", 0)
        lb_rows = conn.execute(
            "SELECT trade_date, MAX(consecutive_zt) FROM zt_pool WHERE trade_date >= date('now','-90 days') GROUP BY trade_date ORDER BY trade_date"
        ).fetchall()
        if lb_rows and len(lb_rows) >= 10:
            lb_values = [r[1] for r in lb_rows]
            lb_pct = _percentile_rank(lb_values, max_lb)
        else:
            lb_pct = min(100, max_lb / 8 * 100)
        
        conn.close()
        
        # 开盘30分钟内涨停/连板不稳定, 降权
        now_hm = _dt.now().hour * 100 + _dt.now().minute
        opening_30min = (930 <= now_hm < 1000)
        if opening_30min:
            # 涨停/连板权重从35%降到10%, 多出的25%给涨跌比
            score = ratio_pct * 0.75 + zt_pct * 0.05 + zb_pct * 0.15 + lb_pct * 0.05
        else:
            score = ratio_pct * 0.50 + zt_pct * 0.20 + zb_pct * 0.15 + lb_pct * 0.15
        
        # phase惩罚: 冰点/退潮打折
        phase = emotion.get("phase", "未知")
        if phase in ("冰点", "退潮", "未知", "退潮预警"):
            score *= 0.5
        elif phase == "偏弱":
            score *= 0.7
        
        return round(min(100, max(0, score)), 1)
    except Exception:
        return 50.0


def _percentile_rank(history: list, current: float) -> float:
    """计算current在history中的百分位排名(0-100)"""
    if not history:
        return 50.0
    count_below = sum(1 for v in history if v < current)
    count_equal = sum(1 for v in history if v == current)
    # 使用 (count_below + 0.5 * count_equal) / total 的标准百分位公式
    return (count_below + 0.5 * count_equal) / len(history) * 100


@st.cache_data(ttl=60, show_spinner=False)
def get_zt_ladder() -> dict[int, list]:
    """涨停梯队: {连板数: [股票列表]}"""
    try:
        conn = _conn()
        latest = conn.execute("SELECT MAX(trade_date) FROM zt_pool").fetchone()[0]
        if not latest:
            conn.close()
            return {}
        rows = conn.execute(
            "SELECT stock_code, name, consecutive_zt, industry, open_count "
            "FROM zt_pool WHERE trade_date=? ORDER BY consecutive_zt DESC, amount DESC",
            (latest,),
        ).fetchall()
        conn.close()

        ladder = {}
        for r in rows:
            n = int(r[2])
            ladder.setdefault(n, []).append({
                "code": r[0], "name": r[1], "industry": r[3], "open": r[4],
            })
        return ladder
    except Exception:
        return {}


@st.cache_data(ttl=60, show_spinner=False)
def get_hot_sectors() -> list[dict]:
    """热门板块TOP10"""
    try:
        from src.strategy.strategy_b import get_hot_sectors as _ghs
        today = datetime.now().strftime("%Y-%m-%d")
        return _ghs(today)
    except Exception:
        return []


# ============================================================
# 6. 新闻
# ============================================================

@st.cache_data(ttl=300, show_spinner=False)
def get_news(limit=15) -> list[dict]:
    """最新新闻"""
    try:
        conn = _conn()
        rows = conn.execute(
            "SELECT title, content, snapshot_time as source, publish_time, sentiment_score, category, news_type "
            "FROM news ORDER BY publish_time DESC LIMIT ?", (limit,)
        ).fetchall()
        conn.close()
        return [
            {"title": r[0], "content": r[1] or "", "source": r[2],
             "time": r[3], "sentiment_score": r[4] or 0, "category": r[5],
             "sentiment": "正面" if (r[4] or 0) > 0.3 else ("负面" if (r[4] or 0) < -0.3 else "中性"),
             "news_type": r[6]}
            for r in rows
        ]
    except Exception:
        return []


# ============================================================
# 7. 系统状态
# ============================================================

@st.cache_data(ttl=30, show_spinner=False)
def get_system_status() -> dict:
    """系统运行状态"""
    status = {
        "daemon_running": False,
        "daemon_pid": None,
        "data_freshness": {},
        "last_scan": None,
        "web_running": True,
    }

    # 守护进程PID
    try:
        if PID_FILE.exists():
            pid = int(PID_FILE.read_text().strip())
            import os
            os.kill(pid, 0)  # 检查进程是否在运行
            status["daemon_running"] = True
            status["daemon_pid"] = pid
    except Exception:
        pass

    # 数据新鲜度
    try:
        conn = _conn()
        tables = ["daily_price", "zt_pool", "fund_flow", "news"]
        for t in tables:
            row = conn.execute(f"SELECT MAX(trade_date) FROM {t}").fetchone()
            status["data_freshness"][t] = row[0] if row else None
        conn.close()
    except Exception:
        pass

    # 守护进程最后日志
    try:
        today = datetime.now().strftime("%Y-%m-%d")
        log_file = DAEMON_LOG_DIR / f"daemon_{today}.log"
        if log_file.exists():
            with open(log_file) as f:
                lines = f.readlines()
            for line in reversed(lines[-50:]):
                if "[扫描]" in line:
                    status["last_scan"] = line.split("]")[0].strip().split(" ")[-1]
                    break
    except Exception:
        pass

    return status


# ============================================================
# 8. 股票详情
# ============================================================

@st.cache_data(ttl=30, show_spinner=False)
def get_stock_detail(code: str) -> dict:
    """单只股票全景: 实时行情 + K线 + 基本信息"""
    result = {"code": code, "name": "", "price": 0, "kline": []}

    # 实时行情
    try:
        from src.trader.realtime_quote import get_realtime
        quotes = get_realtime([code])
        q = quotes.get(code, {})
        result.update({
            "name": q.get("name", ""),
            "price": q.get("price", 0),
            "open": q.get("open", 0),
            "high": q.get("high", 0),
            "low": q.get("low", 0),
            "pre_close": q.get("pre_close", 0),
            "volume": q.get("volume", 0),
            "amount": q.get("amount", 0),
            "change_pct": q.get("change_pct_calc", 0),
            "pe": q.get("pe", 0),
            "pb": q.get("pb", 0),
            "turnover_rate": q.get("turnover_rate", 0),
        })
    except Exception:
        pass

    # K线(120天)
    try:
        conn = _conn()
        df = pd.read_sql(
            "SELECT trade_date, open, high, low, close, volume, amount "
            "FROM daily_price WHERE stock_code=? ORDER BY trade_date DESC LIMIT 120",
            conn, params=(code,),
        )
        conn.close()
        result["kline"] = df.to_dict("records")
    except Exception:
        pass

    return result


# ============================================================
# 9. 模拟盘守护进程日志
# ============================================================

@st.cache_data(ttl=10, show_spinner=False)
def get_daemon_log(tail=100) -> list[str]:
    """守护进程最近日志"""
    try:
        today = datetime.now().strftime("%Y-%m-%d")
        log_file = DAEMON_LOG_DIR / f"daemon_{today}.log"
        if not log_file.exists():
            return []
        with open(log_file) as f:
            lines = f.readlines()
        return [l.strip() for l in lines[-tail:]]
    except Exception:
        return []


@st.cache_data(ttl=5, show_spinner=False)
def get_pending_signals() -> list[dict]:
    """获取待执行的操作预告"""
    try:
        signal_file = ROOT / "output" / "trader" / "signals" / "pending_signals.json"
        if signal_file.exists():
            import json as _json
            return _json.loads(signal_file.read_text(encoding="utf-8"))
    except Exception:
        pass
    return []


@st.cache_data(ttl=10, show_spinner=False)
def get_recent_trades(limit=10) -> list[dict]:
    """获取最近成交记录"""
    try:
        conn = _conn()
        period = _get_current_period()
        rows = conn.execute(
            "SELECT * FROM daemon_trades WHERE period = ? ORDER BY id DESC LIMIT ?",
            (period, limit)
        ).fetchall()
        cols = [d[0] for d in conn.execute("SELECT * FROM daemon_trades LIMIT 0").description]
        return [dict(zip(cols, r)) for r in rows]
    except Exception:
        return []


# ============================================================
# 10. 候选股增强数据
# ============================================================

@st.cache_data(ttl=300, show_spinner=False)
def get_factor_weights() -> dict:
    """IC驱动因子权重 {strategy: {factor: {ic_mean, icir, weight}}}"""
    try:
        conn = _conn()
        rows = conn.execute(
            "SELECT strategy, factor_name, ic_mean, icir, weight, method FROM factor_weights"
        ).fetchall()
        conn.close()
        result = {}
        for r in rows:
            result.setdefault(r[0], {})[r[1]] = {
                "ic_mean": r[2] or 0, "icir": r[3] or 0,
                "weight": r[4] or 0, "method": r[5] or "empirical",
            }
        return result
    except Exception:
        return {}


@st.cache_data(ttl=300, show_spinner=False)
def get_stock_sentiment(code: str) -> dict:
    """个股新闻情感得分(-1到+1)"""
    try:
        conn = _conn()
        row = conn.execute("""
            SELECT AVG(sentiment_score), COUNT(*), MAX(publish_time)
            FROM news
            WHERE stock_code = ? AND sentiment_score IS NOT NULL
              AND publish_time >= datetime('now', '-7 days')
        """, (code,)).fetchone()
        conn.close()
        if row and row[1] > 0:
            return {"score": round(row[0], 2), "count": row[1], "latest": row[2]}
        return {"score": 0, "count": 0, "latest": None}
    except Exception:
        return {"score": 0, "count": 0, "latest": None}


@st.cache_data(ttl=300, show_spinner=False)
def get_stock_trade_memory(code: str) -> dict:
    """个股交易记忆(历史交易次数/胜率/均收)"""
    try:
        conn = _conn()
        rows = conn.execute("""
            SELECT action, pnl_pct, strategy, exit_reason, trade_date
            FROM trade_memory
            WHERE code = ?
            ORDER BY created_at DESC LIMIT 20
        """, (code,)).fetchall()
        conn.close()
        sells = [r for r in rows if r[0] == "sell"]
        if not sells:
            return {"trades": 0, "win_rate": 0, "avg_pnl": 0, "details": []}
        wins = sum(1 for r in sells if (r[1] or 0) > 0)
        avg_pnl = sum(r[1] or 0 for r in sells) / len(sells)
        return {
            "trades": len(sells),
            "win_rate": round(wins / len(sells) * 100, 1),
            "avg_pnl": round(avg_pnl, 2),
            "details": [{"date": r[4], "strategy": r[2], "pnl": r[1], "reason": r[3]} for r in sells[:5]],
        }
    except Exception:
        return {"trades": 0, "win_rate": 0, "avg_pnl": 0, "details": []}


@st.cache_data(ttl=60, show_spinner=False)
def get_industry_concentration() -> dict:
    """当前持仓行业集中度 {industry: count}"""
    try:
        conn = _conn()
        period = _get_current_period()
        positions = conn.execute(
            "SELECT code FROM daemon_positions WHERE status='held' AND period=?",
            (period,)
        ).fetchall()
        if not positions:
            conn.close()
            return {}
        codes = [r[0] for r in positions]
        placeholders = ",".join("?" * len(codes))
        rows = conn.execute(f"""
            SELECT sim.industry_name, COUNT(*) as cnt
            FROM daemon_positions dp
            LEFT JOIN stock_industry_mapping sim ON dp.code = sim.stock_code
            WHERE dp.status='held' AND dp.period=? AND dp.code IN ({placeholders})
            GROUP BY sim.industry_name
        """, [period, *codes]).fetchall()
        conn.close()
        return {r[0] or "未知": r[1] for r in rows}
    except Exception:
        return {}


@st.cache_data(ttl=300, show_spinner=False)
def get_northbound_flow(code: str) -> dict:
    """个股北向资金最近5天净流入"""
    try:
        conn = _conn()
        rows = conn.execute("""
            SELECT ff.trade_date,
                   ff.super_large_net + ff.large_net as main_net
            FROM fund_flow ff
            WHERE ff.stock_code = ?
            ORDER BY ff.trade_date DESC LIMIT 5
        """, (code,)).fetchall()
        conn.close()
        if not rows:
            return {"flows": [], "total_net": 0}
        flows = [{"date": r[0], "net": round(r[1] or 0, 2)} for r in rows]
        total = sum(f["net"] for f in flows)
        return {"flows": flows, "total_net": round(total, 2)}
    except Exception:
        return {"flows": [], "total_net": 0}


@st.cache_data(ttl=300, show_spinner=False)
def get_lockup_risk(code: str) -> list[dict]:
    """未来30天解禁风险"""
    try:
        conn = _conn()
        rows = conn.execute("""
            SELECT free_date, stock_name, free_shares, lift_market_cap, free_type
            FROM lockup_calendar
            WHERE stock_code = ? AND free_date >= date('now') AND free_date <= date('now', '+30 days')
            ORDER BY free_date
        """, (code,)).fetchall()
        conn.close()
        return [
            {"date": r[0], "name": r[1], "shares": r[2], "cap": r[3], "type": r[4]}
            for r in rows
        ]
    except Exception:
        return []


@st.cache_data(ttl=300, show_spinner=False)
def get_lhb_detail(code: str) -> list[dict]:
    """龙虎榜席位明细"""
    try:
        conn = _conn()
        rows = conn.execute("""
            SELECT trade_date, buy_amount, sell_amount, net_amount,
                   buy_depart, sell_depart, reason
            FROM lhb_detail
            WHERE stock_code = ?
            ORDER BY trade_date DESC LIMIT 3
        """, (code,)).fetchall()
        conn.close()
        return [
            {
                "date": r[0], "buy_amt": r[1], "sell_amt": r[2],
                "net": r[3], "buy_dept": r[4], "sell_dept": r[5], "reason": r[6],
            }
            for r in rows
        ]
    except Exception:
        return []
