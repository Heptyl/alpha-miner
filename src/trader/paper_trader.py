"""
模拟交易系统 (Paper Trading) — 参考Qlib回测引擎最佳实践

严谨的A股模拟交易，遵循以下规则:
1. 严格时间隔离: T日信号 → T+1日开盘价成交
2. 交易成本: 买入万2.5，卖出万2.5+印花税千1
3. 滑点: 0.1%
4. 涨跌停限制: 涨停无法买入，跌停无法卖出
5. 停牌: volume=0时跳过
6. T+1交易: 当天买入次日才能卖出(A股规则)
7. 每日净值快照，计算Sharpe/最大回撤/胜率

数据库表:
  sim_account    — 模拟账户每日快照
  sim_positions  — 当前持仓 (pending/held/closed)
  sim_trades     — 交易记录
  sim_daily_snap — 每日快照(净值曲线)
"""

from __future__ import annotations

import json
import sqlite3
import warnings
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Optional

import numpy as np

warnings.filterwarnings("ignore")

# ---------------------------------------------------------------------------
# 路径
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DB_PATH = PROJECT_ROOT / "data" / "alpha_miner.db"
OUTPUT_DIR = PROJECT_ROOT / "output" / "trader"

# ---------------------------------------------------------------------------
# 可配置参数
# ---------------------------------------------------------------------------
INITIAL_CAPITAL: float = 100_000.0       # 10万
MAX_POSITIONS: int = 3                    # 最多同时持有3只(集中火力)
HOLD_DAYS_MAX: int = 10                   # 最多持有10天(给足空间)
STOP_LOSS_PCT: float = -0.08             # 止损 -8%(不被震出)
TAKE_PROFIT_PCT: float = 0.10            # 止盈上限 +10%
TRAILING_STOP_PCT: float = 0.05          # 移动止盈(从高点回落5%即卖)
TIME_STOP_DAYS: int = 3                  # 时间止损(3天不涨就卖)
TOP_N_BUY: int = 3                        # 每天最多新买3只
BUY_AMOUNT_PCT: float = 0.25             # 每只用25%资金(3只仓位制)
COMMISSION_RATE: float = 0.00025          # 手续费万2.5
STAMP_DUTY_RATE: float = 0.001           # 印花税千1 (仅卖出)
SLIPPAGE: float = 0.001                   # 滑点 0.1%
MIN_COMMISSION: float = 5.0              # 最低手续费5元
MIN_SCORE: float = 0.01                   # 最低ML得分门槛(模型输出为预期收益率,1%即可)


# ---------------------------------------------------------------------------
# 工具函数
# ---------------------------------------------------------------------------
def _get_conn() -> sqlite3.Connection:
    """获取数据库连接"""
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


def _ensure_output_dir() -> None:
    """确保输出目录存在"""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# 表初始化
# ---------------------------------------------------------------------------
def init_tables() -> None:
    """创建/升级模拟交易所需的表"""
    conn = _get_conn()
    try:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS sim_account (
                date                TEXT PRIMARY KEY,
                cash                REAL NOT NULL,
                market_value        REAL NOT NULL DEFAULT 0,
                total_assets        REAL NOT NULL,
                daily_return        REAL DEFAULT 0,
                cumulative_return   REAL DEFAULT 0,
                benchmark_return    REAL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS sim_positions (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                code            TEXT NOT NULL,
                name            TEXT DEFAULT '',
                buy_date        TEXT NOT NULL,
                buy_price       REAL NOT NULL,
                shares          INTEGER NOT NULL,
                cost            REAL NOT NULL,
                commission      REAL DEFAULT 0,
                score           REAL DEFAULT 0,
                hold_days       INTEGER DEFAULT 0,
                status          TEXT DEFAULT 'pending',  -- pending/held/closed
                sell_date       TEXT,
                sell_price      REAL,
                sell_reason     TEXT,
                pnl             REAL DEFAULT 0,
                pnl_pct         REAL DEFAULT 0,
                sell_commission REAL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS sim_trades (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                code        TEXT NOT NULL,
                name        TEXT DEFAULT '',
                action      TEXT NOT NULL,      -- buy/sell
                date        TEXT NOT NULL,
                price       REAL NOT NULL,
                shares      INTEGER NOT NULL,
                amount      REAL NOT NULL,
                commission  REAL DEFAULT 0,
                stamp_duty  REAL DEFAULT 0,
                reason      TEXT,
                score       REAL DEFAULT 0,
                pnl         REAL DEFAULT 0,
                pnl_pct     REAL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS sim_daily_snap (
                date                TEXT PRIMARY KEY,
                cash                REAL NOT NULL,
                market_value        REAL NOT NULL,
                total_assets        REAL NOT NULL,
                positions_count     INTEGER DEFAULT 0,
                daily_return        REAL DEFAULT 0,
                cumulative_return   REAL DEFAULT 0,
                benchmark_return    REAL DEFAULT 0,
                benchmark_cum       REAL DEFAULT 0
            );
        """)

        # ---- 升级旧表: 补充缺失的列 ----
        # sim_positions
        _add_column_if_missing(conn, "sim_positions", "commission", "REAL DEFAULT 0")
        _add_column_if_missing(conn, "sim_positions", "highest_price", "REAL DEFAULT 0")
        _add_column_if_missing(conn, "sim_positions", "sell_commission", "REAL DEFAULT 0")
        # sim_trades
        _add_column_if_missing(conn, "sim_trades", "commission", "REAL DEFAULT 0")
        _add_column_if_missing(conn, "sim_trades", "stamp_duty", "REAL DEFAULT 0")
        # sim_daily_snap
        _add_column_if_missing(conn, "sim_daily_snap", "positions_count", "INTEGER DEFAULT 0")
        _add_column_if_missing(conn, "sim_daily_snap", "benchmark_cum", "REAL DEFAULT 0")
        # sim_account
        _add_column_if_missing(conn, "sim_account", "benchmark_return", "REAL DEFAULT 0")

        conn.commit()
    finally:
        conn.close()


def _add_column_if_missing(conn: sqlite3.Connection, table: str, column: str, col_type: str) -> None:
    """如果表中缺少某列，就加上"""
    existing = {r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    if column not in existing:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}")


# ---------------------------------------------------------------------------
# 行情数据查询
# ---------------------------------------------------------------------------
def get_latest_trade_date() -> Optional[str]:
    """获取数据库中最新的交易日期"""
    conn = _get_conn()
    try:
        row = conn.execute(
            "SELECT MAX(trade_date) FROM daily_price"
        ).fetchone()
        return row[0] if row else None
    finally:
        conn.close()


def get_trade_dates(n_days: int = 60) -> list[str]:
    """获取最近N个交易日的日期列表 (升序)"""
    conn = _get_conn()
    try:
        rows = conn.execute(
            "SELECT DISTINCT trade_date FROM daily_price ORDER BY trade_date DESC LIMIT ?",
            (n_days,),
        ).fetchall()
        return [r[0] for r in reversed(rows)]
    finally:
        conn.close()


def get_prices_on_date(codes: list[str], trade_date: str) -> dict[str, dict]:
    """获取指定日期的完整行情数据 (开/收/高/低/前收/成交量)"""
    if not codes:
        return {}
    conn = _get_conn()
    try:
        placeholders = ",".join("?" * len(codes))
        sql = f"""
            SELECT stock_code, open, close, high, low, pre_close, volume, amount
            FROM daily_price
            WHERE trade_date = ? AND stock_code IN ({placeholders})
        """
        rows = conn.execute(sql, [trade_date] + codes).fetchall()
        result = {}
        for r in rows:
            code = r["stock_code"]
            result[code] = {
                "open": r["open"],
                "close": r["close"],
                "high": r["high"],
                "low": r["low"],
                "pre_close": r["pre_close"],
                "volume": r["volume"],
                "amount": r["amount"],
            }
        return result
    finally:
        conn.close()


def get_name_map(codes: list[str]) -> dict[str, str]:
    """获取股票名称，从多个可能的数据源中查找"""
    if not codes:
        return {}
    conn = _get_conn()
    try:
        names: dict[str, str] = {}
        placeholders = ",".join("?" * len(codes))
        for table, code_col, name_col in [
            ("zt_pool", "code", "name"),
            ("strong_pool", "code", "name"),
            ("fund_flow", "code", "name"),
        ]:
            try:
                rows = conn.execute(
                    f"SELECT DISTINCT {code_col}, {name_col} FROM {table} WHERE {code_col} IN ({placeholders})",
                    codes,
                ).fetchall()
                for r in rows:
                    if r[name_col]:
                        names[r[code_col]] = r[name_col]
            except Exception:
                pass
        return names
    finally:
        conn.close()


def get_benchmark_price(trade_date: str) -> Optional[float]:
    """获取沪深300基准的收益率 (简化: 用全市场均价变动近似)"""
    conn = _get_conn()
    try:
        # 用全市场等权平均收益率作为基准近似
        row = conn.execute("""
            SELECT AVG(close / pre_close - 1) as avg_ret
            FROM daily_price
            WHERE trade_date = ? AND pre_close > 0 AND volume > 0
        """, (trade_date,)).fetchone()
        return row["avg_ret"] if row and row["avg_ret"] is not None else 0.0
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 涨跌停 / 停牌 / 交易成本计算
# ---------------------------------------------------------------------------
def is_limit_up(code: str, trade_date: str, prices: Optional[dict] = None) -> bool:
    """判断涨停: close >= pre_close * 1.099 (考虑浮点误差)

    创业板/科创板20%涨跌幅:
      300xxx/301xxx -> 1.199
      688xxx/689xxx -> 1.199
    北交所30%:
      8xxxxx/4xxxxx -> 1.299
    其他主板/中小板: 10% -> 1.099
    """
    p = prices
    if p is None:
        p = get_prices_on_date([code], trade_date).get(code)
    if not p or not p.get("pre_close") or p["pre_close"] <= 0:
        return False

    ratio = 1.099  # 默认10%涨跌幅
    if code.startswith(("300", "301")):
        ratio = 1.199  # 创业板20%
    elif code.startswith(("688", "689")):
        ratio = 1.199  # 科创板20%
    elif code.startswith(("8", "4")):
        ratio = 1.299  # 北交所30%

    return p["close"] >= p["pre_close"] * ratio


def is_limit_down(code: str, trade_date: str, prices: Optional[dict] = None) -> bool:
    """判断跌停: close <= pre_close * 0.901 (考虑浮点误差)"""
    p = prices
    if p is None:
        p = get_prices_on_date([code], trade_date).get(code)
    if not p or not p.get("pre_close") or p["pre_close"] <= 0:
        return False

    ratio = 0.901
    if code.startswith(("300", "301")):
        ratio = 0.801
    elif code.startswith(("688", "689")):
        ratio = 0.801
    elif code.startswith(("8", "4")):
        ratio = 0.701

    return p["close"] <= p["pre_close"] * ratio


def is_suspended(code: str, trade_date: str, prices: Optional[dict] = None) -> bool:
    """判断停牌: volume == 0 或 当天无数据"""
    p = prices
    if p is None:
        p = get_prices_on_date([code], trade_date).get(code)
    if p is None:
        return True  # 无数据 = 停牌
    if p.get("volume") is None or p["volume"] == 0:
        return True
    return False


def calc_buy_price(open_price: float) -> float:
    """买入成交价 = open_price * (1 + SLIPPAGE)"""
    return round(open_price * (1 + SLIPPAGE), 3)


def calc_sell_price(open_price: float) -> float:
    """卖出成交价 = open_price * (1 - SLIPPAGE)"""
    return round(open_price * (1 - SLIPPAGE), 3)


def calc_commission(amount: float, is_sell: bool = False) -> tuple[float, float]:
    """计算手续费

    Returns:
        (commission, stamp_duty)
        commission = max(amount * COMMISSION_RATE, MIN_COMMISSION)
        stamp_duty = amount * STAMP_DUTY_RATE (仅卖出)
    """
    commission = max(amount * COMMISSION_RATE, MIN_COMMISSION)
    stamp_duty = amount * STAMP_DUTY_RATE if is_sell else 0.0
    return round(commission, 2), round(stamp_duty, 2)


def calc_shares(buy_amount: float, price: float) -> int:
    """计算可买股数 (100股的整数倍)"""
    if price <= 0:
        return 0
    return int(buy_amount / price / 100) * 100


# ---------------------------------------------------------------------------
# 账户状态
# ---------------------------------------------------------------------------
def get_account_state() -> dict[str, Any]:
    """获取当前模拟账户状态"""
    conn = _get_conn()
    try:
        row = conn.execute(
            "SELECT * FROM sim_account ORDER BY date DESC LIMIT 1"
        ).fetchone()

        if row is None:
            # 初始化账户
            today = get_latest_trade_date() or date.today().isoformat()
            conn.execute(
                "INSERT INTO sim_account (date, cash, market_value, total_assets, daily_return, cumulative_return, benchmark_return) "
                "VALUES (?, ?, 0, ?, 0, 0, 0)",
                (today, INITIAL_CAPITAL, INITIAL_CAPITAL),
            )
            conn.execute(
                "INSERT INTO sim_daily_snap (date, cash, market_value, total_assets, positions_count, daily_return, cumulative_return, benchmark_return, benchmark_cum) "
                "VALUES (?, ?, 0, ?, 0, 0, 0, 0, 0)",
                (today, INITIAL_CAPITAL, INITIAL_CAPITAL),
            )
            conn.commit()
            return {
                "date": today, "cash": INITIAL_CAPITAL,
                "market_value": 0.0, "total_assets": INITIAL_CAPITAL,
                "daily_return": 0.0, "cumulative_return": 0.0, "benchmark_return": 0.0,
            }

        return dict(row)
    finally:
        conn.close()


def get_open_positions(status_filter: Optional[list[str]] = None) -> list[dict]:
    """获取持仓列表"""
    if status_filter is None:
        status_filter = ["pending", "held"]
    conn = _get_conn()
    try:
        placeholders = ",".join("?" * len(status_filter))
        rows = conn.execute(
            f"SELECT * FROM sim_positions WHERE status IN ({placeholders}) ORDER BY buy_date",
            status_filter,
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# ML预测
# ---------------------------------------------------------------------------
def ml_predict_on_date(date_str: str, model=None, feature_cols: list[str] | None = None) -> list[dict]:
    """在指定日期用ML模型预测

    优先使用factor_values缓存，如果不可用则调用FeaturePipeline。

    Args:
        date_str: 预测日期 (用截至该日的数据)
        model: LightGBM Booster (可选，懒加载)
        feature_cols: 特征列名列表 (可选)

    Returns:
        TOP20 预测结果 [{code, name, score, close}, ...]
    """
    import lightgbm as lgb

    # ---- 懒加载模型 ----
    if model is None:
        model_path = PROJECT_ROOT / "output" / "ml" / "latest_model.txt"
        if not model_path.exists():
            return []
        model = lgb.Booster(model_file=str(model_path))

    if feature_cols is None:
        meta_path = PROJECT_ROOT / "output" / "ml" / "latest_model_meta.json"
        if meta_path.exists():
            meta = json.loads(meta_path.read_text())
            feature_cols = meta.get("feature_cols", [])
        else:
            feature_cols = [f"feat_{i}" for i in range(model.num_feature())]

    # ---- 尝试从factor_values缓存构建特征 ----
    df = _predict_from_factor_cache(date_str, feature_cols)

    # ---- 检查cache特征匹配率, 不足50%则用FeaturePipeline ----
    n_model_features = model.num_feature()
    cache_match_rate = 0
    if df is not None and not df.empty and feature_cols:
        available = [c for c in feature_cols if c in df.columns]
        cache_match_rate = len(available) / len(feature_cols) if feature_cols else 0

    if df is None or df.empty or cache_match_rate < 0.5:
        from src.ml.feature_pipeline import FeaturePipeline
        pipeline = FeaturePipeline(db_path=str(DB_PATH))
        full_df = pipeline.build_features()
        if full_df.empty:
            return []
        # 取截至date_str的最新数据
        full_df = full_df[full_df["trade_date"] <= date_str]
        if full_df.empty:
            return []
        # 每只股票取最新一天
        df = full_df.sort_values("trade_date").groupby("stock_code").last().reset_index()
        # 补上trade_date列
        if "trade_date" not in df.columns:
            df["trade_date"] = date_str

    if df.empty:
        return []

    # 排除科创板/北交所/ST
    df = df[~df["stock_code"].astype(str).str.startswith(("688", "689", "200", "8", "9"))]
    if "name" in df.columns:
        df = df[~df["name"].str.contains("ST", na=False)]

    # 预测
    # 匹配模型期望的特征数
    n_model_features = model.num_feature()
    available_cols = [c for c in feature_cols if c in df.columns]
    if not available_cols:
        # 尝试所有数值列
        available_cols = [c for c in df.columns
                         if c not in ("stock_code", "trade_date", "name")
                         and df[c].dtype.kind in ("f", "i", "u")]
    if not available_cols:
        return []

    X = df[available_cols].fillna(0).values

    # 如果特征数不匹配模型，进行填充或截断
    if X.shape[1] != n_model_features:
        import numpy as _np
        if X.shape[1] < n_model_features:
            # 不足的列用0填充
            pad = _np.zeros((X.shape[0], n_model_features - X.shape[1]))
            X = _np.hstack([X, pad])
        else:
            # 多余的列截断
            X = X[:, :n_model_features]

    try:
        scores = model.predict(X)
    except Exception:
        return []

    df = df.copy()
    df["score"] = scores

    # 排序取TOP20
    top = df.nlargest(20, "score")
    codes = top["stock_code"].tolist()
    names = get_name_map(codes)

    results = []
    for _, row in top.iterrows():
        code = str(row["stock_code"])
        name = names.get(code, str(row.get("name", "")))
        close = float(row.get("close", 0))
        results.append({
            "code": code,
            "name": name,
            "score": float(row["score"]),
            "close": close,
        })

    return results


def _predict_from_factor_cache(date_str: str, feature_cols: list[str]) -> Optional[Any]:
    """尝试从factor_values表直接构建特征矩阵 (快速路径)

    Returns:
        DataFrame 或 None
    """
    import pandas as pd

    conn = _get_conn()
    try:
        # 检查是否有足够的数据
        cnt = conn.execute(
            "SELECT COUNT(DISTINCT stock_code) FROM factor_values WHERE trade_date = ?",
            (date_str,),
        ).fetchone()[0]
        if cnt < 50:
            return None

        # pivot factor_values
        fv = pd.read_sql_query(
            "SELECT stock_code, trade_date, factor_name, factor_value "
            "FROM factor_values WHERE trade_date <= ? AND trade_date >= date(?, '-30 days')",
            conn, params=(date_str, date_str),
        )
    finally:
        conn.close()

    if fv.empty:
        return None

    # pivot
    pivot = fv.pivot_table(
        index=["stock_code", "trade_date"],
        columns="factor_name",
        values="factor_value",
        aggfunc="last",
    ).reset_index()

    # 取每只股票截至date_str的最新一行
    pivot = pivot.sort_values("trade_date").groupby("stock_code").last().reset_index()

    # 合入价格数据
    price_conn = _get_conn()
    try:
        prices = pd.read_sql_query(
            "SELECT stock_code AS stock_code_p, open, close, high, low, volume, amount "
            "FROM daily_price WHERE trade_date = ?",
            price_conn, params=(date_str,),
        )
    finally:
        price_conn.close()

    if not prices.empty:
        pivot = pivot.merge(
            prices, left_on="stock_code", right_on="stock_code_p", how="left"
        )
        if "stock_code_p" in pivot.columns:
            pivot.drop(columns=["stock_code_p"], inplace=True)

    return pivot


# ---------------------------------------------------------------------------
# 核心每日运行
# ---------------------------------------------------------------------------
def run_daily(today: Optional[str] = None) -> dict[str, Any]:
    """每日模拟交易核心逻辑

    流程:
    1. 获取最新交易日T
    2. 处理pending: 用T日开盘价(加滑点)买入。检查涨停/停牌
    3. 处理held: 用T日收盘价检查止盈/止损/到期。T+1规则(当天买的不能当天卖)
    4. ML预测: 生成新的pending(下一个交易日执行)
    5. 更新账户快照
    6. 返回当日报告
    """
    init_tables()
    conn = _get_conn()
    try:
        return _run_daily_impl(conn, today)
    finally:
        conn.close()


def _run_daily_impl(conn: sqlite3.Connection, today: Optional[str]) -> dict[str, Any]:
    """run_daily 的内部实现 (使用已有连接)"""
    # ---- 1. 获取交易日 ----
    if today is None:
        today = get_latest_trade_date()
    if not today:
        return {"error": "无行情数据"}

    report: dict[str, Any] = {
        "date": today,
        "buys": [],
        "sells": [],
        "holds": [],
        "cancels": [],
        "new_pending": [],
        "stats": {},
    }

    # ---- 加载账户状态 ----
    acct = conn.execute(
        "SELECT * FROM sim_account ORDER BY date DESC LIMIT 1"
    ).fetchone()
    if acct is None:
        cash = INITIAL_CAPITAL
        prev_total = INITIAL_CAPITAL
    else:
        cash = acct["cash"]
        prev_total = acct["total_assets"]

    # ---- 2. 处理 pending → 用今日开盘价(加滑点)买入 ----
    pending = conn.execute(
        "SELECT * FROM sim_positions WHERE status = 'pending'"
    ).fetchall()

    pending_codes = [p["code"] for p in pending]
    pending_prices = get_prices_on_date(pending_codes, today) if pending_codes else {}

    for pos in pending:
        code = pos["code"]
        p = pending_prices.get(code)

        # 停牌检查
        if is_suspended(code, today, p):
            conn.execute(
                "UPDATE sim_positions SET status='closed', sell_date=?, sell_reason='停牌取消' WHERE id=?",
                (today, pos["id"]),
            )
            report["cancels"].append({"code": code, "name": pos["name"], "reason": "停牌取消"})
            continue

        # 涨停检查 (涨停买不进)
        if is_limit_up(code, today, p):
            conn.execute(
                "UPDATE sim_positions SET status='closed', sell_date=?, sell_reason='涨停无法买入' WHERE id=?",
                (today, pos["id"]),
            )
            report["cancels"].append({"code": code, "name": pos["name"], "reason": "涨停无法买入"})
            continue

        if not p or not p["open"] or p["open"] <= 0:
            conn.execute(
                "UPDATE sim_positions SET status='closed', sell_date=?, sell_reason='无行情数据' WHERE id=?",
                (today, pos["id"]),
            )
            report["cancels"].append({"code": code, "name": pos["name"], "reason": "无行情数据"})
            continue

        # 计算真实买入价 (开盘价 + 滑点)
        buy_price = calc_buy_price(p["open"])
        actual_cost = buy_price * pos["shares"]
        commission, _ = calc_commission(actual_cost, is_sell=False)

        if actual_cost + commission > cash:
            conn.execute(
                "UPDATE sim_positions SET status='closed', sell_date=?, sell_reason='资金不足' WHERE id=?",
                (today, pos["id"]),
            )
            report["cancels"].append({"code": code, "name": pos["name"], "reason": "资金不足"})
            continue

        # 执行买入
        conn.execute("""
            UPDATE sim_positions
            SET status='held', buy_price=?, cost=?, commission=?, buy_date=?
            WHERE id=?
        """, (buy_price, actual_cost, commission, today, pos["id"]))

        cash -= (actual_cost + commission)

        conn.execute("""
            INSERT INTO sim_trades (code, name, action, date, price, shares, amount, commission, stamp_duty, reason, score)
            VALUES (?, ?, 'buy', ?, ?, ?, ?, ?, 0, ?, ?)
        """, (
            code, pos["name"], today, buy_price, pos["shares"], actual_cost,
            commission, f"ML模拟买入 得分={pos['score']:.4f}", pos["score"],
        ))

        report["buys"].append({
            "code": code, "name": pos["name"], "price": buy_price,
            "shares": pos["shares"], "amount": actual_cost,
            "commission": commission, "score": pos["score"],
        })

    # ---- 3. 处理 held → 检查止盈/止损/到期 ----
    # 注意T+1规则: 当天买的不能当天卖
    held = conn.execute(
        "SELECT * FROM sim_positions WHERE status = 'held'"
    ).fetchall()

    held_codes = [h["code"] for h in held]
    held_prices = get_prices_on_date(held_codes, today) if held_codes else {}

    for pos in held:
        code = pos["code"]
        p = held_prices.get(code)

        # T+1规则: buy_date == today 的不能卖 (刚从pending转来的)
        if pos["buy_date"] == today:
            report["holds"].append({
                "code": code, "name": pos["name"],
                "buy_price": pos["buy_price"], "cur_price": p["close"] if p else pos["buy_price"],
                "shares": pos["shares"],
                "pnl_pct": ((p["close"] / pos["buy_price"] - 1) if p and p["close"] and pos["buy_price"] > 0 else 0),
                "hold_days": pos["hold_days"],
                "note": "T+1锁定",
            })
            continue

        # 停牌: 无法卖出
        if is_suspended(code, today, p):
            report["holds"].append({
                "code": code, "name": pos["name"],
                "buy_price": pos["buy_price"], "cur_price": pos["buy_price"],
                "shares": pos["shares"], "pnl_pct": 0,
                "hold_days": pos["hold_days"], "note": "停牌",
            })
            continue

        if not p or not p["close"]:
            continue

        cur_price = p["close"]
        hold_days_new = pos["hold_days"] + 1
        pnl_pct = (cur_price / pos["buy_price"] - 1) if pos["buy_price"] > 0 else 0
        sell_reason = None

        # 跟踪最高价(用于移动止盈)
        _hp = pos["highest_price"] if "highest_price" in pos.keys() else 0
        if not _hp or _hp <= 0:
            _hp = pos["buy_price"]
        highest = max(_hp, cur_price)
        drawdown_from_high = (cur_price / highest - 1) if highest > 0 else 0

        # 1. 止损(最高优先)
        if pnl_pct <= STOP_LOSS_PCT:
            sell_reason = f"止损 {pnl_pct:.1%}"
        # 2. 固定止盈上限
        elif pnl_pct >= TAKE_PROFIT_PCT:
            sell_reason = f"止盈上限 {pnl_pct:.1%}"
        # 3. 移动止盈(从高点回落5%)
        elif highest > pos["buy_price"] and drawdown_from_high <= -TRAILING_STOP_PCT:
            sell_reason = f"移动止盈 高点{highest:.2f}回落{drawdown_from_high:.1%}"
        # 4. 时间止损(3天不涨=涨幅<1%)
        elif hold_days_new >= TIME_STOP_DAYS and pnl_pct < 0.01:
            sell_reason = f"时间止损 {hold_days_new}天仅{pnl_pct:.1%}"
        # 5. 最大持有期到期
        elif hold_days_new >= HOLD_DAYS_MAX:
            sell_reason = f"持有{hold_days_new}天到期"

        if sell_reason:
            # 跌停检查: 跌停卖不出
            if is_limit_down(code, today, p):
                conn.execute(
                    "UPDATE sim_positions SET hold_days=? WHERE id=?",
                    (hold_days_new, pos["id"]),
                )
                report["holds"].append({
                    "code": code, "name": pos["name"],
                    "buy_price": pos["buy_price"], "cur_price": cur_price,
                    "shares": pos["shares"],
                    "pnl_pct": pnl_pct, "hold_days": hold_days_new,
                    "note": f"跌停无法卖出 ({sell_reason})",
                })
                continue

            # 计算卖出价 (收盘价 - 滑点)
            sell_price = calc_sell_price(cur_price)
            sell_amount = sell_price * pos["shares"]
            commission, stamp_duty = calc_commission(sell_amount, is_sell=True)
            total_cost = commission + stamp_duty

            pnl = (sell_price - pos["buy_price"]) * pos["shares"] - pos["commission"] - total_cost
            pnl_pct_net = pnl / (pos["cost"] + pos["commission"]) if (pos["cost"] + pos["commission"]) > 0 else 0

            conn.execute("""
                UPDATE sim_positions
                SET status='closed', sell_date=?, sell_price=?, sell_reason=?, hold_days=?,
                    pnl=?, pnl_pct=?, sell_commission=?
                WHERE id=?
            """, (today, sell_price, sell_reason, hold_days_new, pnl, pnl_pct_net, total_cost, pos["id"]))

            cash += (sell_amount - total_cost)

            conn.execute("""
                INSERT INTO sim_trades (code, name, action, date, price, shares, amount, commission, stamp_duty, reason, score, pnl, pnl_pct)
                VALUES (?, ?, 'sell', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                code, pos["name"], today, sell_price, pos["shares"], sell_amount,
                commission, stamp_duty, sell_reason, pos["score"], pnl, pnl_pct_net,
            ))

            report["sells"].append({
                "code": code, "name": pos["name"],
                "buy_price": pos["buy_price"], "sell_price": sell_price,
                "shares": pos["shares"], "pnl": round(pnl, 2),
                "pnl_pct": round(pnl_pct_net, 4), "reason": sell_reason,
                "hold_days": hold_days_new,
                "commission": commission, "stamp_duty": stamp_duty,
            })
        else:
            # 未触发卖出, 更新hold_days和highest_price
            conn.execute(
                "UPDATE sim_positions SET hold_days=?, highest_price=? WHERE id=?",
                (hold_days_new, highest, pos["id"]),
            )
            report["holds"].append({
                "code": code, "name": pos["name"],
                "buy_price": pos["buy_price"], "cur_price": cur_price,
                "shares": pos["shares"],
                "pnl_pct": round(pnl_pct, 4), "hold_days": hold_days_new,
                "highest": highest,
            })

    # ---- 4. ML预测 → 新建pending (下一个交易日执行) ----
    # 获取当前held持仓的code
    current_held = conn.execute(
        "SELECT code FROM sim_positions WHERE status = 'held'"
    ).fetchall()
    held_code_set = {r["code"] for r in current_held}
    open_count = len(held_code_set)
    slots = MAX_POSITIONS - open_count

    # ---- 情绪周期过滤: 冰点期不开新仓 ----
    try:
        from src.trader.emotion_cycle import get_emotion_state
        emotion = get_emotion_state(today)
        emotion_info = {
            "phase": emotion.phase.value,
            "score": emotion.score,
            "can_open": emotion.can_open,
            "position_ratio": emotion.position_ratio,
        }
        if not emotion.can_open:
            report["emotion"] = emotion_info
            report["note"] = f"情绪{emotion.phase.value}({emotion.score}分), 不开新仓"
            # 即使不开新仓, 仍然完成持仓管理(止损止盈不受影响)
        else:
            # 高潮期仓位打满, 复苏期限制仓位
            if emotion.phase.value == "复苏":
                slots = min(slots, max(1, int(MAX_POSITIONS * emotion.position_ratio)))
    except Exception:
        emotion_info = {"phase": "未知", "score": 0, "can_open": True, "position_ratio": 1.0}

    report["emotion"] = emotion_info if "emotion" not in report else report["emotion"]

    new_pending_count = 0
    if slots > 0 and emotion_info.get("can_open", True):
        # 加载ML预测
        prediction = _load_prediction()
        if prediction:
            top_n = prediction.get("top7", prediction.get("top20", []))
            names = get_name_map([item.get("code", "") for item in top_n])

            # ---- 预演三问过滤 ----
            try:
                from src.trader.three_questions import filter_stock_list
                filtered = filter_stock_list(
                    [{"code": it.get("code",""), "name": names.get(it.get("code",""), it.get("name","")), "score": it.get("score",0)}
                     for it in top_n],
                    target_date=today,
                    min_score=3,  # 回测用宽松阈值(3分)
                )
                # 用过滤后的列表替换原始top_n
                if filtered:
                    filtered_codes = {f["code"] for f in filtered}
                    top_n = [it for it in top_n if it.get("code","") in filtered_codes]
                    report["three_q_filtered"] = len(filtered)
            except Exception:
                pass  # 过滤失败不影响流程

            for item in top_n:
                if new_pending_count >= min(slots, TOP_N_BUY):
                    break

                code = item.get("code", "")
                # 跳过已持仓
                if code in held_code_set:
                    continue
                # 跳过科创板/北交所
                if code.startswith(("688", "689", "200", "8", "9")):
                    continue
                # 跳过ST
                name = names.get(code, item.get("name", ""))
                if "ST" in name.upper():
                    continue
                # 得分门槛
                score = item.get("score", 0)
                if score < MIN_SCORE:
                    continue

                ref_price = item.get("close", 0)
                if ref_price <= 0:
                    continue

                buy_amount = (cash if cash > 0 else INITIAL_CAPITAL) * BUY_AMOUNT_PCT
                shares = calc_shares(buy_amount, ref_price)
                if shares < 100:
                    continue

                cost = ref_price * shares

                conn.execute("""
                    INSERT INTO sim_positions (code, name, buy_date, buy_price, shares, cost, score, status)
                    VALUES (?, ?, ?, ?, ?, ?, ?, 'pending')
                """, (code, name, today, ref_price, shares, cost, score))

                new_pending_count += 1
                report["new_pending"].append({
                    "code": code, "name": name, "score": score,
                    "ref_price": ref_price, "shares": shares,
                })

    # ---- 5. 更新账户快照 ----
    all_held = conn.execute(
        "SELECT code, shares, buy_price FROM sim_positions WHERE status = 'held'"
    ).fetchall()
    held_codes2 = [h["code"] for h in all_held]
    mkt_prices = get_prices_on_date(held_codes2, today) if held_codes2 else {}
    market_value = sum(
        mkt_prices.get(h["code"], {}).get("close", h["buy_price"]) * h["shares"]
        for h in all_held
    )
    total = cash + market_value

    # 计算收益率
    daily_ret = (total / prev_total - 1) if prev_total > 0 else 0.0
    cum_ret = (total / INITIAL_CAPITAL - 1)

    # 基准
    bench_ret = get_benchmark_price(today) or 0.0
    prev_bench_cum = conn.execute(
        "SELECT benchmark_cum FROM sim_daily_snap ORDER BY date DESC LIMIT 1"
    ).fetchone()
    prev_bc = prev_bench_cum["benchmark_cum"] if prev_bench_cum else 0.0
    bench_cum = (1 + prev_bc) * (1 + bench_ret) - 1

    conn.execute("""
        INSERT OR REPLACE INTO sim_account
        (date, cash, market_value, total_assets, daily_return, cumulative_return, benchmark_return)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (today, round(cash, 2), round(market_value, 2), round(total, 2),
          round(daily_ret, 6), round(cum_ret, 6), round(bench_ret, 6)))

    conn.execute("""
        INSERT OR REPLACE INTO sim_daily_snap
        (date, cash, market_value, total_assets, positions_count, daily_return, cumulative_return, benchmark_return, benchmark_cum)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (today, round(cash, 2), round(market_value, 2), round(total, 2),
          len(all_held), round(daily_ret, 6), round(cum_ret, 6),
          round(bench_ret, 6), round(bench_cum, 6)))

    conn.commit()

    # ---- 6. 统计 ----
    report["stats"] = {
        "cash": round(cash, 2),
        "market_value": round(market_value, 2),
        "total_assets": round(total, 2),
        "daily_return": f"{daily_ret:.2%}",
        "cumulative_return": f"{cum_ret:.2%}",
        "benchmark_return": f"{bench_ret:.2%}",
        "open_positions": len(all_held),
        "new_pending": new_pending_count,
    }

    # 保存报告
    _ensure_output_dir()
    report_path = OUTPUT_DIR / "sim_report.json"
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str))

    return report


def _load_prediction() -> Optional[dict]:
    """加载最新ML预测结果"""
    pred_path = PROJECT_ROOT / "output" / "ml" / "latest_prediction.json"
    if pred_path.exists():
        return json.loads(pred_path.read_text())
    return None


# ---------------------------------------------------------------------------
# 模拟回放
# ---------------------------------------------------------------------------
def run_simulation_backtest(days: int = 30) -> dict[str, Any]:
    """批量回放历史数据

    核心逻辑:
    - 逐日循环
    - T-1日做ML预测 → T日用开盘价执行买入
    - 买入时检查涨停(涨停买不进)
    - 卖出时检查跌停(跌停卖不出)
    - 每笔交易扣除手续费
    - 记录每日净值

    Args:
        days: 回放天数

    Returns:
        回测统计结果
    """
    import lightgbm as lgb

    init_tables()

    # ---- 加载模型 ----
    model_path = PROJECT_ROOT / "output" / "ml" / "latest_model.txt"
    if not model_path.exists():
        return {"error": "模型文件不存在，请先训练模型"}

    model = lgb.Booster(model_file=str(model_path))
    meta_path = PROJECT_ROOT / "output" / "ml" / "latest_model_meta.json"
    if meta_path.exists():
        meta = json.loads(meta_path.read_text())
        feature_cols = meta.get("feature_cols", [])
    else:
        feature_cols = [f"feat_{i}" for i in range(model.num_feature())]

    # ---- 获取交易日 ----
    trade_dates = get_trade_dates(days + 15)  # 多取一些用于预热
    if len(trade_dates) < 5:
        return {"error": "交易日不足"}

    # 只保留后 days 天 (去掉预热期)
    start_idx = max(0, len(trade_dates) - days)
    trade_dates = trade_dates[start_idx:]

    # ---- 清理旧数据 ----
    conn = _get_conn()
    try:
        conn.execute("DELETE FROM sim_account")
        conn.execute("DELETE FROM sim_positions")
        conn.execute("DELETE FROM sim_trades")
        conn.execute("DELETE FROM sim_daily_snap")
        conn.commit()
    finally:
        conn.close()

    print(f"回放区间: {trade_dates[0]} ~ {trade_dates[-1]} ({len(trade_dates)}天)")
    print(f"模型特征: {len(feature_cols)}个")
    print()

    # ---- 内部状态 ----
    all_results: list[dict] = []
    cash = INITIAL_CAPITAL
    # positions: code -> position dict
    positions: dict[str, dict] = {}

    # 缓存前一天预测的top20
    cached_top20: list[dict] = []
    # 净值历史 (用于计算日收益率)
    nav_history: list[float] = [INITIAL_CAPITAL]

    for i, today in enumerate(trade_dates):
        # 进度
        if i % 5 == 0:
            pct = i / len(trade_dates) * 100
            print(f"  [{pct:5.1f}%] {today} | 持仓{len(positions)}只 | 现金¥{cash:,.0f}")

        # === 1. 处理held持仓: 检查卖出 ===
        if positions:
            held_codes = list(positions.keys())
            held_prices = get_prices_on_date(held_codes, today)

            to_remove: list[str] = []
            for code, pos in positions.items():
                p = held_prices.get(code)
                hold_days_new = pos["hold_days"] + 1

                # T+1规则: 前一天买的不能今天卖 (hold_days=0 → 今天是第1天)
                # 在回放中, 买入时hold_days=0, 第二天+1变为1, 可以在第2天卖
                # 但实际上A股T+1: 买入当天不能卖, 次日可以卖
                # 所以hold_days至少为1才能卖
                can_sell = (hold_days_new >= 2)  # 买入日=0, 次日=1, 次日可卖所以>=2不对
                # 修正: 买入日hold_days设为0, 当天不能再操作.
                # 下一个交易日hold_days变为1, 此时可以卖.
                can_sell = (hold_days_new >= 1) and (pos["buy_date"] != today)

                if not p or not p["close"] or p["close"] <= 0:
                    pos["hold_days"] = hold_days_new
                    # 停牌检查
                    if is_suspended(code, today, p):
                        continue
                    continue

                cur_price = p["close"]
                pos["hold_days"] = hold_days_new
                pnl_pct = (cur_price / pos["buy_price"] - 1) if pos["buy_price"] > 0 else 0

                # 跟踪最高价(移动止盈)
                highest = max(pos.get("highest_price", pos["buy_price"]) or pos["buy_price"], cur_price)
                pos["highest_price"] = highest
                drawdown_from_high = (cur_price / highest - 1) if highest > 0 else 0

                sell_reason = None
                if can_sell:
                    if pnl_pct <= STOP_LOSS_PCT:
                        sell_reason = f"止损 {pnl_pct:.1%}"
                    elif pnl_pct >= TAKE_PROFIT_PCT:
                        sell_reason = f"止盈上限 {pnl_pct:.1%}"
                    elif highest > pos["buy_price"] and drawdown_from_high <= -TRAILING_STOP_PCT:
                        sell_reason = f"移动止盈 高点{highest:.2f}回落{drawdown_from_high:.1%}"
                    elif hold_days_new >= TIME_STOP_DAYS and pnl_pct < 0.01:
                        sell_reason = f"时间止损 {hold_days_new}天仅{pnl_pct:.1%}"
                    elif hold_days_new >= HOLD_DAYS_MAX:
                        sell_reason = f"持有{hold_days_new}天到期"

                if sell_reason:
                    # 跌停检查
                    if is_limit_down(code, today, p):
                        continue  # 跌停卖不出

                    sell_price = calc_sell_price(cur_price)
                    sell_amount = sell_price * pos["shares"]
                    commission, stamp_duty = calc_commission(sell_amount, is_sell=True)
                    total_fee = commission + stamp_duty

                    pnl = (sell_price - pos["buy_price"]) * pos["shares"] - pos.get("commission", 0) - total_fee
                    buy_total_cost = pos["cost"] + pos.get("commission", 0)
                    pnl_pct_net = pnl / buy_total_cost if buy_total_cost > 0 else 0

                    cash += (sell_amount - total_fee)

                    all_results.append({
                        "code": code, "name": pos["name"],
                        "buy_date": pos["buy_date"], "sell_date": today,
                        "buy_price": pos["buy_price"], "sell_price": sell_price,
                        "shares": pos["shares"], "pnl": round(pnl, 2),
                        "pnl_pct": round(pnl_pct_net, 4),
                        "hold_days": hold_days_new, "reason": sell_reason,
                        "score": pos["score"],
                        "commission": commission, "stamp_duty": stamp_duty,
                    })
                    to_remove.append(code)

            for code in to_remove:
                del positions[code]

        # === 2. ML预测 (用前一天的数据) ===
        if i > 0:
            pred_date = trade_dates[i - 1]
        else:
            pred_date = trade_dates[0]

        # 每3天重新预测 (或无持仓时每天都预测)
        if i % 3 == 0 or not positions:
            try:
                cached_top20 = ml_predict_on_date(pred_date, model, feature_cols)
            except Exception as e:
                cached_top20 = []
                if i == 0:
                    pass  # 第一个交易日可能没有前置数据

        # === 3. 新建买入 ===
        slots = MAX_POSITIONS - len(positions)

        # ---- 情绪周期过滤 ----
        emotion_can_open = True
        try:
            from src.trader.emotion_cycle import get_emotion_state
            emotion = get_emotion_state(today)
            emotion_can_open = emotion.can_open
            if not emotion_can_open:
                pass  # 冰点/退潮期不开新仓
            elif emotion.phase.value == "复苏":
                slots = min(slots, max(1, int(MAX_POSITIONS * emotion.position_ratio)))
        except Exception:
            pass

        if cached_top20 and slots > 0 and emotion_can_open:
            # ---- 预演三问过滤(回测模式) ----
            try:
                from src.trader.three_questions import filter_stock_list
                filtered = filter_stock_list(
                    [{"code": it["code"], "name": it.get("name",""), "score": it.get("score",0)}
                     for it in cached_top20],
                    target_date=today,
                    min_score=3,
                )
                if filtered:
                    filtered_codes = {f["code"] for f in filtered}
                    cached_top20 = [it for it in cached_top20 if it["code"] in filtered_codes]
            except Exception:
                pass

            buy_codes = [item["code"] for item in cached_top20[:TOP_N_BUY + 5]]
            buy_prices_data = get_prices_on_date(buy_codes, today)

            bought = 0
            for item in cached_top20:
                if bought >= min(slots, TOP_N_BUY):
                    break

                code = item["code"]
                if code in positions:
                    continue
                # 跳过科创板/北交所
                if code.startswith(("688", "689", "200", "8", "9")):
                    continue

                p = buy_prices_data.get(code)
                if not p or not p.get("open") or p["open"] <= 0:
                    continue

                # 停牌检查
                if is_suspended(code, today, p):
                    continue
                # 涨停检查
                if is_limit_up(code, today, p):
                    continue

                # 买入价 = 开盘价 + 滑点
                buy_price = calc_buy_price(p["open"])
                buy_amount = cash * BUY_AMOUNT_PCT
                shares = calc_shares(buy_amount, buy_price)
                if shares < 100:
                    continue

                cost = buy_price * shares
                commission, _ = calc_commission(cost, is_sell=False)
                total_cost = cost + commission

                if total_cost > cash:
                    continue

                cash -= total_cost
                positions[code] = {
                    "buy_price": buy_price,
                    "shares": shares,
                    "cost": cost,
                    "commission": commission,
                    "buy_date": today,
                    "hold_days": 0,
                    "name": item.get("name", ""),
                    "score": item.get("score", 0),
                }
                bought += 1

        # === 4. 记录每日快照 ===
        if positions:
            held_codes = list(positions.keys())
            mkt_p = get_prices_on_date(held_codes, today)
            market_value = sum(
                mkt_p.get(c, {}).get("close", positions[c]["buy_price"]) * positions[c]["shares"]
                for c in positions
            )
        else:
            market_value = 0.0

        total = cash + market_value
        # 计算日收益率
        if i == 0:
            daily_ret = total / INITIAL_CAPITAL - 1
        else:
            prev_nav_snap = nav_history[-1] if nav_history else INITIAL_CAPITAL
            daily_ret = (total / prev_nav_snap - 1) if prev_nav_snap > 0 else 0

        # 基准
        bench_ret = get_benchmark_price(today) or 0.0

        conn = _get_conn()
        try:
            # 获取前一天的累计基准
            prev_snap = conn.execute(
                "SELECT benchmark_cum FROM sim_daily_snap ORDER BY date DESC LIMIT 1"
            ).fetchone()
            prev_bc = prev_snap["benchmark_cum"] if prev_snap else 0.0
            bench_cum = (1 + prev_bc) * (1 + bench_ret) - 1

            conn.execute("""
                INSERT OR REPLACE INTO sim_daily_snap
                (date, cash, market_value, total_assets, positions_count, daily_return, cumulative_return, benchmark_return, benchmark_cum)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                today, round(cash, 2), round(market_value, 2), round(total, 2),
                len(positions), round(daily_ret, 6), round(total / INITIAL_CAPITAL - 1, 6),
                round(bench_ret, 6), round(bench_cum, 6),
            ))
            conn.commit()
        finally:
            conn.close()

        # 记录净值历史
        nav_history.append(total)

    # ---- 最终统计 ----
    return _finalize_backtest(all_results, trade_dates, cash, positions)


def run_simulation_backtest_v2(days: int = 30) -> dict[str, Any]:
    """run_simulation_backtest 的改进版 — 用DB持久化所有中间状态

    更接近真实每日run_daily的行为，便于验证。
    """
    import lightgbm as lgb

    init_tables()

    model_path = PROJECT_ROOT / "output" / "ml" / "latest_model.txt"
    if not model_path.exists():
        return {"error": "模型文件不存在，请先训练模型"}

    model = lgb.Booster(model_file=str(model_path))
    meta_path = PROJECT_ROOT / "output" / "ml" / "latest_model_meta.json"
    if meta_path.exists():
        meta = json.loads(meta_path.read_text())
        feature_cols = meta.get("feature_cols", [])
    else:
        feature_cols = [f"feat_{i}" for i in range(model.num_feature())]

    trade_dates = get_trade_dates(days + 15)
    if len(trade_dates) < 5:
        return {"error": "交易日不足"}

    start_idx = max(0, len(trade_dates) - days)
    trade_dates = trade_dates[start_idx:]

    # 清理旧数据
    conn = _get_conn()
    try:
        conn.execute("DELETE FROM sim_account")
        conn.execute("DELETE FROM sim_positions")
        conn.execute("DELETE FROM sim_trades")
        conn.execute("DELETE FROM sim_daily_snap")
        conn.commit()
    finally:
        conn.close()

    print(f"[v2] 回放区间: {trade_dates[0]} ~ {trade_dates[-1]} ({len(trade_dates)}天)")

    # 初始账户
    conn = _get_conn()
    try:
        conn.execute("""
            INSERT INTO sim_account (date, cash, market_value, total_assets, daily_return, cumulative_return, benchmark_return)
            VALUES (?, ?, 0, ?, 0, 0, 0)
        """, (trade_dates[0], INITIAL_CAPITAL, INITIAL_CAPITAL))
        conn.commit()
    finally:
        conn.close()

    all_results: list[dict] = []
    cached_top20: list[dict] = []

    for i, today in enumerate(trade_dates):
        if i % 5 == 0:
            print(f"  [{i/len(trade_dates)*100:5.1f}%] {today}")

        conn = _get_conn()
        try:
            # ---- T-1日ML预测 ----
            if i > 0:
                pred_date = trade_dates[i - 1]
                if i % 3 == 0:
                    try:
                        cached_top20 = ml_predict_on_date(pred_date, model, feature_cols)
                    except Exception:
                        cached_top20 = []
            elif i == 0 and not cached_top20:
                try:
                    cached_top20 = ml_predict_on_date(trade_dates[0], model, feature_cols)
                except Exception:
                    cached_top20 = []

            # ---- 创建pending (模拟T-1日信号) ----
            acct = conn.execute(
                "SELECT * FROM sim_account ORDER BY date DESC LIMIT 1"
            ).fetchone()
            cash = acct["cash"] if acct else INITIAL_CAPITAL

            held_codes = {r["code"] for r in conn.execute(
                "SELECT code FROM sim_positions WHERE status = 'held'"
            ).fetchall()}
            slots = MAX_POSITIONS - len(held_codes)

            # ---- 情绪周期过滤 ----
            emotion_can_open = True
            try:
                from src.trader.emotion_cycle import get_emotion_state
                emotion = get_emotion_state(today)
                emotion_can_open = emotion.can_open
                if emotion.phase.value == "复苏":
                    slots = min(slots, max(1, int(MAX_POSITIONS * emotion.position_ratio)))
            except Exception:
                pass

            new_pending = 0
            if cached_top20 and slots > 0 and emotion_can_open:
                # ---- 预演三问过滤(v2回测) ----
                try:
                    from src.trader.three_questions import filter_stock_list
                    filtered = filter_stock_list(
                        [{"code": it["code"], "name": it.get("name",""), "score": it.get("score",0)}
                         for it in cached_top20],
                        target_date=today,
                        min_score=3,
                    )
                    if filtered:
                        filtered_codes = {f["code"] for f in filtered}
                        cached_top20 = [it for it in cached_top20 if it["code"] in filtered_codes]
                except Exception:
                    pass

                for item in cached_top20:
                    if new_pending >= min(slots, TOP_N_BUY):
                        break
                    code = item["code"]
                    if code in held_codes:
                        continue
                    if code.startswith(("688", "689", "200", "8", "9")):
                        continue

                    score = item.get("score", 0)
                    if score < MIN_SCORE:
                        continue

                    ref_price = item.get("close", 0)
                    if ref_price <= 0:
                        continue

                    buy_amount = max(cash, INITIAL_CAPITAL) * BUY_AMOUNT_PCT
                    shares = calc_shares(buy_amount, ref_price)
                    if shares < 100:
                        continue

                    name = item.get("name", "")
                    conn.execute("""
                        INSERT INTO sim_positions (code, name, buy_date, buy_price, shares, cost, score, status)
                        VALUES (?, ?, ?, ?, ?, ?, ?, 'pending')
                    """, (code, name, today, ref_price, shares, ref_price * shares, score))
                    new_pending += 1

            # ---- 执行daily逻辑 ----
            report = _run_daily_impl(conn, today)

            # 收集卖出结果
            if report.get("sells"):
                all_results.extend(report["sells"])

        finally:
            conn.close()

    # 统计
    conn = _get_conn()
    try:
        snaps = conn.execute(
            "SELECT * FROM sim_daily_snap ORDER BY date"
        ).fetchall()
        trades = conn.execute(
            "SELECT * FROM sim_trades WHERE action='sell' ORDER BY date"
        ).fetchall()
    finally:
        conn.close()

    return _build_backtest_stats(all_results, snaps, trades, trade_dates)


# ---------------------------------------------------------------------------
# 内部辅助函数
# ---------------------------------------------------------------------------
def _finalize_backtest(
    all_results: list[dict],
    trade_dates: list[str],
    final_cash: float,
    positions: dict,
) -> dict[str, Any]:
    """旧版回测的统计汇总"""
    conn = _get_conn()
    try:
        snaps = conn.execute(
            "SELECT * FROM sim_daily_snap ORDER BY date"
        ).fetchall()
    finally:
        conn.close()

    if not all_results:
        return {
            "error": "没有产生任何交易",
            "total_days": len(trade_dates),
            "final_assets": round(final_cash, 2),
        }

    return _build_backtest_stats(all_results, snaps, [], trade_dates)


def _build_backtest_stats(
    all_results: list[dict],
    snaps: list,
    trades_from_db: list,
    trade_dates: list[str],
) -> dict[str, Any]:
    """构建回测统计"""
    pnls = [r.get("pnl_pct", 0) for r in all_results]
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p <= 0]
    win_rate = len(wins) / len(pnls) * 100 if pnls else 0
    avg_pnl = float(np.mean(pnls)) if pnls else 0
    total_pnl = sum(r.get("pnl", 0) for r in all_results)

    # Sharpe
    nav_series = [dict(s)["total_assets"] for s in snaps] if snaps else []
    daily_returns = []
    for j in range(1, len(nav_series)):
        if nav_series[j - 1] > 0:
            daily_returns.append(nav_series[j] / nav_series[j - 1] - 1)

    if len(daily_returns) > 1 and np.std(daily_returns) > 0:
        sharpe = float(np.mean(daily_returns) / np.std(daily_returns) * np.sqrt(252))
    else:
        sharpe = 0.0

    # 最大回撤
    max_dd = 0.0
    peak = nav_series[0] if nav_series else INITIAL_CAPITAL
    for nav in nav_series:
        if nav > peak:
            peak = nav
        dd = (nav - peak) / peak
        if dd < max_dd:
            max_dd = dd

    # 持有期分布
    hold_dist: dict[str, int] = {}
    for r in all_results:
        d = r.get("hold_days", 0)
        key = f"{d}天"
        hold_dist[key] = hold_dist.get(key, 0) + 1

    # 卖出原因分布
    reason_dist: dict[str, int] = {}
    for r in all_results:
        reason = r.get("reason", "其他")
        # 只取关键词
        reason_key = reason.split()[0] if reason else "其他"
        reason_dist[reason_key] = reason_dist.get(reason_key, 0) + 1

    # 净值曲线(最近30天)
    nav_curve = []
    for s in snaps[-30:]:
        d = dict(s)
        nav_curve.append({
            "date": d["date"],
            "total_assets": d["total_assets"],
            "daily_return": d["daily_return"],
            "cumulative_return": d["cumulative_return"],
            "benchmark_return": d.get("benchmark_return", 0),
            "benchmark_cum": d.get("benchmark_cum", 0),
        })

    # 最近20笔交易
    recent_trades = []
    for r in all_results[-20:]:
        recent_trades.append(r)

    final_total = nav_series[-1] if nav_series else INITIAL_CAPITAL

    stats = {
        "total_days": len(trade_dates),
        "total_trades": len(all_results),
        "win_trades": len(wins),
        "loss_trades": len(losses),
        "win_rate": round(win_rate, 1),
        "avg_pnl_pct": round(avg_pnl * 100, 2),
        "total_pnl": round(total_pnl, 2),
        "max_win_pct": round(max(pnls) * 100, 2) if pnls else 0,
        "max_loss_pct": round(min(pnls) * 100, 2) if pnls else 0,
        "sharpe": round(sharpe, 2),
        "max_drawdown": round(max_dd * 100, 2),
        "final_total": round(final_total, 2),
        "cumulative_return": round((final_total / INITIAL_CAPITAL - 1) * 100, 2),
        "hold_period_dist": hold_dist,
        "sell_reason_dist": reason_dist,
    }

    output = {
        "stats": stats,
        "trades": all_results,
        "nav_curve": nav_curve,
        "recent_trades": recent_trades,
        "params": {
            "initial_capital": INITIAL_CAPITAL,
            "max_positions": MAX_POSITIONS,
            "hold_days_max": HOLD_DAYS_MAX,
            "stop_loss": f"{STOP_LOSS_PCT:.0%}",
            "take_profit": f"{TAKE_PROFIT_PCT:.0%}",
            "trailing_stop": f"{TRAILING_STOP_PCT:.0%}",
            "time_stop_days": TIME_STOP_DAYS,
            "top_n_buy": TOP_N_BUY,
            "buy_amount_pct": f"{BUY_AMOUNT_PCT:.0%}",
            "commission_rate": f"{COMMISSION_RATE:.5f}",
            "stamp_duty_rate": f"{STAMP_DUTY_RATE:.4f}",
            "slippage": f"{SLIPPAGE:.4f}",
        },
    }

    _ensure_output_dir()
    bt_path = OUTPUT_DIR / "sim_backtest.json"
    bt_path.write_text(json.dumps(output, ensure_ascii=False, indent=2, default=str))

    return output


# ---------------------------------------------------------------------------
# 统计报告
# ---------------------------------------------------------------------------
def get_simulation_stats() -> dict[str, Any]:
    """获取模拟盘统计报告

    返回:
    - 总交易次数/胜率/平均盈亏
    - Sharpe/最大回撤
    - 持仓期分布/卖出原因分布
    - 净值曲线(最近30天)
    - 最近20笔交易
    - 与沪深300基准对比
    """
    init_tables()
    conn = _get_conn()
    try:
        return _get_simulation_stats_impl(conn)
    finally:
        conn.close()


def _get_simulation_stats_impl(conn: sqlite3.Connection) -> dict[str, Any]:
    """get_simulation_stats 的内部实现"""

    # ---- 基本统计 ----
    total_trades = conn.execute(
        "SELECT COUNT(*) FROM sim_trades WHERE action='sell'"
    ).fetchone()[0]

    win_trades = conn.execute(
        "SELECT COUNT(*) FROM sim_trades WHERE action='sell' AND pnl > 0"
    ).fetchone()[0]

    total_pnl = conn.execute(
        "SELECT COALESCE(SUM(pnl), 0) FROM sim_trades WHERE action='sell'"
    ).fetchone()[0]

    avg_pnl_pct = conn.execute(
        "SELECT COALESCE(AVG(pnl_pct), 0) FROM sim_trades WHERE action='sell'"
    ).fetchone()[0]

    max_win = conn.execute(
        "SELECT COALESCE(MAX(pnl_pct), 0) FROM sim_trades WHERE action='sell'"
    ).fetchone()[0]

    max_loss = conn.execute(
        "SELECT COALESCE(MIN(pnl_pct), 0) FROM sim_trades WHERE action='sell'"
    ).fetchone()[0]

    # 总手续费
    total_commission = conn.execute(
        "SELECT COALESCE(SUM(commission), 0) FROM sim_trades"
    ).fetchone()[0]
    total_stamp_duty = conn.execute(
        "SELECT COALESCE(SUM(stamp_duty), 0) FROM sim_trades"
    ).fetchone()[0]

    # 当前持仓
    open_pos = conn.execute(
        "SELECT COUNT(*) FROM sim_positions WHERE status IN ('held', 'pending')"
    ).fetchone()[0]

    # 最新账户
    acct = conn.execute(
        "SELECT * FROM sim_account ORDER BY date DESC LIMIT 1"
    ).fetchone()

    # ---- Sharpe / 最大回撤 ----
    snaps = conn.execute(
        "SELECT * FROM sim_daily_snap ORDER BY date"
    ).fetchall()

    nav_series = [dict(s)["total_assets"] for s in snaps]
    daily_returns = []
    for j in range(1, len(nav_series)):
        if nav_series[j - 1] > 0:
            daily_returns.append(nav_series[j] / nav_series[j - 1] - 1)

    sharpe = 0.0
    if len(daily_returns) > 1 and np.std(daily_returns) > 0:
        sharpe = float(np.mean(daily_returns) / np.std(daily_returns) * np.sqrt(252))

    max_dd = 0.0
    peak = nav_series[0] if nav_series else INITIAL_CAPITAL
    for nav in nav_series:
        if nav > peak:
            peak = nav
        dd = (nav - peak) / peak
        if dd < max_dd:
            max_dd = dd

    # ---- 持仓期分布 ----
    hold_dist: dict[str, int] = {}
    closed = conn.execute(
        "SELECT hold_days FROM sim_positions WHERE status='closed' AND hold_days > 0"
    ).fetchall()
    for r in closed:
        key = f"{r[0]}天"
        hold_dist[key] = hold_dist.get(key, 0) + 1

    # ---- 卖出原因分布 ----
    reason_dist: dict[str, int] = {}
    reasons = conn.execute(
        "SELECT sell_reason FROM sim_positions WHERE status='closed' AND sell_reason IS NOT NULL"
    ).fetchall()
    for r in reasons:
        reason = r[0].split()[0] if r[0] else "其他"
        reason_dist[reason] = reason_dist.get(reason, 0) + 1

    # ---- 净值曲线(最近30天) ----
    recent_snaps = conn.execute(
        "SELECT * FROM sim_daily_snap ORDER BY date DESC LIMIT 30"
    ).fetchall()
    nav_curve = [dict(s) for s in reversed(recent_snaps)]

    # ---- 最近20笔交易 ----
    recent_trades_raw = conn.execute(
        "SELECT date, action, code, name, price, shares, amount, commission, stamp_duty, pnl, pnl_pct, reason "
        "FROM sim_trades ORDER BY date DESC, id DESC LIMIT 20"
    ).fetchall()

    recent_trades = []
    trade_cols = ["date", "action", "code", "name", "price", "shares", "amount",
                  "commission", "stamp_duty", "pnl", "pnl_pct", "reason"]
    for r in recent_trades_raw:
        recent_trades.append(dict(zip(trade_cols, r)))

    # ---- 与基准对比 ----
    bench_data = conn.execute(
        "SELECT date, cumulative_return, benchmark_cum FROM sim_daily_snap ORDER BY date DESC LIMIT 30"
    ).fetchall()
    benchmark_compare = [dict(r) for r in reversed(bench_data)]

    win_rate = (win_trades / total_trades * 100) if total_trades > 0 else 0

    result = {
        "total_trades": total_trades,
        "win_trades": win_trades,
        "loss_trades": total_trades - win_trades,
        "win_rate": f"{win_rate:.1f}%",
        "total_pnl": round(total_pnl, 2),
        "avg_pnl_pct": f"{avg_pnl_pct:.2%}",
        "max_win": f"{max_win:.2%}",
        "max_loss": f"{max_loss:.2%}",
        "total_commission": round(total_commission, 2),
        "total_stamp_duty": round(total_stamp_duty, 2),
        "total_cost": round(total_commission + total_stamp_duty, 2),
        "open_positions": open_pos,
        "sharpe": round(sharpe, 2),
        "max_drawdown": f"{max_dd:.2%}",
        "account": {
            "date": acct["date"] if acct else None,
            "cash": round(acct["cash"], 2) if acct else INITIAL_CAPITAL,
            "market_value": round(acct["market_value"], 2) if acct else 0,
            "total_assets": round(acct["total_assets"], 2) if acct else INITIAL_CAPITAL,
            "daily_return": f"{acct['daily_return']:.2%}" if acct and acct["daily_return"] else "0%",
            "cumulative_return": f"{acct['cumulative_return']:.2%}" if acct and acct["cumulative_return"] else "0%",
            "benchmark_return": f"{acct['benchmark_return']:.2%}" if acct and acct["benchmark_return"] is not None else "0%",
        },
        "hold_period_dist": hold_dist,
        "sell_reason_dist": reason_dist,
        "nav_curve": nav_curve,
        "recent_trades": recent_trades,
        "benchmark_compare": benchmark_compare,
    }

    # 保存统计
    _ensure_output_dir()
    stats_path = OUTPUT_DIR / "sim_stats.json"
    stats_path.write_text(json.dumps(result, ensure_ascii=False, indent=2, default=str))

    return result


# ---------------------------------------------------------------------------
# 重置模拟
# ---------------------------------------------------------------------------
def reset_simulation() -> dict[str, str]:
    """重置所有模拟数据"""
    conn = _get_conn()
    try:
        conn.execute("DELETE FROM sim_account")
        conn.execute("DELETE FROM sim_positions")
        conn.execute("DELETE FROM sim_trades")
        conn.execute("DELETE FROM sim_daily_snap")
        conn.commit()
    finally:
        conn.close()
    return {"status": "已重置模拟数据"}


# ---------------------------------------------------------------------------
# CLI入口
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("用法:")
        print("  python -m src.trader.paper_trader daily     # 每日运行")
        print("  python -m src.trader.paper_trader backtest   # 30天回放")
        print("  python -m src.trader.paper_trader backtest 60  # 60天回放")
        print("  python -m src.trader.paper_trader stats      # 查看统计")
        print("  python -m src.trader.paper_trader reset      # 重置")
        sys.exit(0)

    cmd = sys.argv[1]

    if cmd == "daily":
        report = run_daily()
        print(json.dumps(report, ensure_ascii=False, indent=2, default=str))

    elif cmd == "backtest":
        n_days = int(sys.argv[2]) if len(sys.argv) > 2 else 30
        result = run_simulation_backtest_v2(n_days)
        stats = result.get("stats", {})
        print("\n" + "=" * 60)
        print("模拟回放统计")
        print("=" * 60)
        print(f"  回放天数:     {stats.get('total_days', 0)}")
        print(f"  总交易次数:   {stats.get('total_trades', 0)}")
        print(f"  胜率:         {stats.get('win_rate', 0)}%")
        print(f"  平均盈亏:     {stats.get('avg_pnl_pct', 0)}%")
        print(f"  总盈亏:       ¥{stats.get('total_pnl', 0):,.2f}")
        print(f"  Sharpe:       {stats.get('sharpe', 0)}")
        print(f"  最大回撤:     {stats.get('max_drawdown', 0)}%")
        print(f"  最终资产:     ¥{stats.get('final_total', 0):,.2f}")
        print(f"  累计收益:     {stats.get('cumulative_return', 0)}%")
        print(f"  持仓期分布:   {stats.get('hold_period_dist', {})}")
        print(f"  卖出原因分布: {stats.get('sell_reason_dist', {})}")

    elif cmd == "stats":
        stats = get_simulation_stats()
        print(json.dumps(stats, ensure_ascii=False, indent=2, default=str))

    elif cmd == "reset":
        result = reset_simulation()
        print(result)

    else:
        print(f"未知命令: {cmd}")
