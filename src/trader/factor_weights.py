"""因子动态权重引擎 — IC/ICIR驱动

核心思路:
  1. 对精选评分中的每个因子(量比/MA60距离/RSI/缩量/回踩精度等)
     计算截面Spearman IC(因子值 vs 未来1日/3日收益)
  2. IC显著(>0.03)且ICIR好(>0.5)的因子保留，不显著的降权/剔除
  3. 每周末更新一次权重，冷启动期用经验权重fallback
  4. 权重持久化到DB(factor_weights表)

用法:
  from src.trader.factor_weights import get_weights, update_weights
  weights = get_weights("C")  # 返回策略C的因子权重
"""

from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import numpy as np

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

DB_PATH = Path(__file__).resolve().parents[2] / "data" / "alpha_miner.db"

logger = logging.getLogger(__name__)

# ── 经验权重(冷启动fallback) ──
EMPIRICAL_WEIGHTS = {
    "B": {
        "timing": 0.35,
        "shrink": 0.25,
        "precision": 0.20,
        "discount": 0.20,
    },
    "C": {
        "vol_ratio": 0.40,
        "ma60_dist": 0.25,
        "rsi": 0.20,
        "tier": 0.15,
    },
}

# IC阈值
IC_SIGNIFICANT = 0.03      # IC > 0.03 视为显著
ICIR_GOOD = 0.5            # ICIR > 0.5 视为有效
MIN_OBSERVATIONS = 20      # 最少观测日数
MAX_IC_AGE_DAYS = 14       # IC数据超过14天需要重新计算


def _get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def _ensure_table(conn: sqlite3.Connection):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS factor_weights (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            strategy     TEXT NOT NULL,
            factor_name  TEXT NOT NULL,
            ic_mean      REAL DEFAULT 0,
            ic_std       REAL DEFAULT 0,
            icir         REAL DEFAULT 0,
            ic_win_rate  REAL DEFAULT 0,
            weight       REAL DEFAULT 0,
            method       TEXT DEFAULT 'empirical',
            updated_at   TEXT DEFAULT (datetime('now')),
            UNIQUE(strategy, factor_name)
        )
    """)


def _compute_factor_ic_series(factor_values: dict, forward_returns: dict) -> list[float]:
    """计算截面IC序列

    Args:
        factor_values: {date: {stock_code: value}}
        forward_returns: {date: {stock_code: return}}

    Returns:
        IC值列表
    """
    from scipy import stats as scipy_stats

    ic_list = []
    for date in sorted(factor_values.keys()):
        fv = factor_values[date]
        fr = forward_returns.get(date, {})
        if not fv or not fr:
            continue

        common = set(fv.keys()) & set(fr.keys())
        if len(common) < 10:
            continue

        fv_arr = np.array([fv[s] for s in common])
        fr_arr = np.array([fr[s] for s in common])

        # 过滤NaN
        mask = np.isfinite(fv_arr) & np.isfinite(fr_arr)
        fv_arr = fv_arr[mask]
        fr_arr = fr_arr[mask]
        if len(fv_arr) < 10:
            continue

        # 常量保护
        if np.std(fv_arr) < 1e-12 or np.std(fr_arr) < 1e-12:
            continue

        try:
            import warnings
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", "An input array is constant")
                corr, _ = scipy_stats.spearmanr(fv_arr, fr_arr)
            if np.isfinite(corr):
                ic_list.append(float(corr))
        except Exception:
            continue

    return ic_list


def _get_volume_ratio_values(conn: sqlite3.Connection, dates: list[str]) -> dict:
    """获取量比因子截面值

    量比 = 今日成交量 / MA5成交量
    """
    if not dates:
        return {}

    date_set = ",".join(f"'{d}'" for d in dates)
    rows = conn.execute(f"""
        SELECT a.stock_code, a.trade_date,
               a.volume as today_vol,
               (SELECT AVG(b.volume) FROM daily_price b
                WHERE b.stock_code = a.stock_code
                  AND b.trade_date <= a.trade_date
                  AND b.volume > 0
                ORDER BY b.trade_date DESC LIMIT 5) as ma5_vol
        FROM daily_price a
        WHERE a.trade_date IN ({date_set}) AND a.volume > 0
    """).fetchall()

    result = {}
    for r in rows:
        if r["ma5_vol"] and r["ma5_vol"] > 0:
            date = r["trade_date"]
            if date not in result:
                result[date] = {}
            result[date][r["stock_code"]] = r["today_vol"] / r["ma5_vol"]
    return result


def _get_ma60_dist_values(conn: sqlite3.Connection, dates: list[str]) -> dict:
    """获取距MA60距离因子截面值"""
    if not dates:
        return {}

    date_set = ",".join(f"'{d}'" for d in dates)
    rows = conn.execute(f"""
        SELECT dp.stock_code, dp.trade_date, dp.close,
               (SELECT AVG(b.close) FROM daily_price b
                WHERE b.stock_code = dp.stock_code
                  AND b.trade_date <= dp.trade_date
                  AND b.close > 0
                ORDER BY b.trade_date DESC LIMIT 60) as ma60
        FROM daily_price dp
        WHERE dp.trade_date IN ({date_set}) AND dp.close > 0
    """).fetchall()

    result = {}
    for r in rows:
        if r["ma60"] and r["ma60"] > 0:
            date = r["trade_date"]
            if date not in result:
                result[date] = {}
            result[date][r["stock_code"]] = (r["close"] / r["ma60"] - 1) * 100
    return result


def _get_rsi_values(conn: sqlite3.Connection, dates: list[str], period: int = 14) -> dict:
    """获取RSI因子截面值"""
    if not dates:
        return {}

    # 批量获取所有相关日期的价格数据
    min_date = (datetime.strptime(min(dates), "%Y-%m-%d") - timedelta(days=period * 3)).strftime("%Y-%m-%d")
    date_set = ",".join(f"'{d}'" for d in dates)

    rows = conn.execute(f"""
        SELECT stock_code, trade_date, close FROM daily_price
        WHERE trade_date >= '{min_date}'
        ORDER BY stock_code, trade_date
    """).fetchall()

    # 按股票分组
    stock_data = {}
    for r in rows:
        code = r["stock_code"]
        if code not in stock_data:
            stock_data[code] = []
        stock_data[code].append((r["trade_date"], r["close"]))

    date_set_lookup = set(dates)
    result = {d: {} for d in dates}

    for code, prices in stock_data.items():
        if len(prices) < period + 1:
            continue
        prices.sort()

        # 计算RSI
        deltas = [prices[i][1] - prices[i-1][1] for i in range(1, len(prices))]
        gains = [max(0, d) for d in deltas]
        losses = [max(0, -d) for d in deltas]

        # 简单移动平均计算RSI
        for i in range(period, len(deltas)):
            date = prices[i + 1][0]  # +1 因为deltas偏移了1
            if date not in date_set_lookup:
                continue
            avg_gain = sum(gains[i-period+1:i+1]) / period
            avg_loss = sum(losses[i-period+1:i+1]) / period
            if avg_loss < 1e-12:
                rsi = 100.0
            else:
                rs = avg_gain / avg_loss
                rsi = 100 - 100 / (1 + rs)
            result[date][code] = rsi

    return result


def _get_turnover_values(conn: sqlite3.Connection, dates: list[str]) -> dict:
    """获取换手率因子截面值"""
    if not dates:
        return {}

    date_set = ",".join(f"'{d}'" for d in dates)
    rows = conn.execute(f"""
        SELECT stock_code, trade_date, turnover_rate
        FROM daily_price
        WHERE trade_date IN ({date_set}) AND turnover_rate IS NOT NULL AND turnover_rate > 0
    """).fetchall()

    result = {}
    for r in rows:
        date = r["trade_date"]
        if date not in result:
            result[date] = {}
        result[date][r["stock_code"]] = r["turnover_rate"]
    return result


def _get_forward_returns(conn: sqlite3.Connection, dates: list[str],
                         forward_days: int = 1) -> dict:
    """获取未来N日收益率"""
    if not dates:
        return {}

    all_dates = [r[0] for r in conn.execute(
        "SELECT DISTINCT trade_date FROM daily_price ORDER BY trade_date"
    ).fetchall()]

    date_to_idx = {d: i for i, d in enumerate(all_dates)}

    result = {}
    for date in dates:
        if date not in date_to_idx:
            continue
        idx = date_to_idx[date]
        target_idx = idx + forward_days
        if target_idx >= len(all_dates):
            continue
        target_date = all_dates[target_idx]

        rows = conn.execute("""
            SELECT a.stock_code, (b.close - a.close) / a.close as fwd_ret
            FROM daily_price a
            JOIN daily_price b ON a.stock_code = b.stock_code
            WHERE a.trade_date = ? AND b.trade_date = ? AND a.close > 0
        """, (date, target_date)).fetchall()

        result[date] = {r["stock_code"]: r["fwd_ret"] for r in rows if r["fwd_ret"] is not None}

    return result


# 因子获取函数映射
FACTOR_EXTRACTORS = {
    "vol_ratio": _get_volume_ratio_values,
    "ma60_dist": _get_ma60_dist_values,
    "rsi": _get_rsi_values,
    "turnover": _get_turnover_values,
}


def compute_factor_ic(strategy: str, lookback_days: int = 60) -> dict:
    """计算策略相关因子的IC统计

    Returns:
        {factor_name: {ic_mean, ic_std, icir, ic_win_rate, n_obs}}
    """
    conn = _get_conn()
    try:
        # 获取最近lookback_days的交易日列表(每周取1天减少计算量)
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")

        all_dates = [r[0] for r in conn.execute(
            "SELECT DISTINCT trade_date FROM daily_price "
            "WHERE trade_date >= ? AND trade_date <= ? ORDER BY trade_date",
            (start_date, end_date),
        ).fetchall()]

        if not all_dates:
            return {}

        # 每周取1天(减少计算量, 60天≈8-9个样本)
        dates = all_dates[::5]
        if not dates:
            dates = all_dates[-5:]

        # 计算未来收益
        fwd_returns = _get_forward_returns(conn, dates, forward_days=1)

        # 确定要分析的因子
        factors = list(EMPIRICAL_WEIGHTS.get(strategy, {}).keys())
        if strategy == "C":
            factors = ["vol_ratio", "ma60_dist", "rsi", "turnover"]
        elif strategy == "B":
            # B的因子(timing/shrink/precision/discount)来自候选数据,
            # 用代理因子: turnover(缩量代理), volatility(折扣代理)
            factors = ["turnover", "vol_ratio"]

        results = {}
        for factor_name in factors:
            extractor = FACTOR_EXTRACTORS.get(factor_name)
            if not extractor:
                continue

            logger.info(f"[IC] 计算 {factor_name} IC...")
            try:
                factor_values = extractor(conn, dates)
            except Exception as e:
                logger.warning(f"[IC] {factor_name} 计算失败: {e}")
                continue

            ic_list = _compute_factor_ic_series(factor_values, fwd_returns)

            if len(ic_list) < MIN_OBSERVATIONS // 3:
                logger.info(f"[IC] {factor_name}: 观测不足({len(ic_list)}条), 跳过")
                continue

            ic_arr = np.array(ic_list)
            ic_mean = float(np.mean(ic_arr))
            ic_std = float(np.std(ic_arr))
            icir = ic_mean / ic_std if ic_std > 1e-12 else 0
            ic_win_rate = float(np.mean(ic_arr > 0))

            results[factor_name] = {
                "ic_mean": round(ic_mean, 4),
                "ic_std": round(ic_std, 4),
                "icir": round(icir, 4),
                "ic_win_rate": round(ic_win_rate, 4),
                "n_obs": len(ic_list),
            }
            logger.info(
                f"[IC] {factor_name}: IC={ic_mean:+.4f} ICIR={icir:.2f} "
                f"WinRate={ic_win_rate:.0%} ({len(ic_list)}obs)"
            )

        return results

    finally:
        conn.close()


def _ic_to_weights(ic_results: dict, strategy: str) -> dict:
    """将IC结果转换为权重

    规则:
      1. IC显著(>0.03)且ICIR好(>0.5): 保留, 权重 ∝ |IC|
      2. IC显著但ICIR差(0.3-0.5): 降权
      3. IC不显著(<0.03): 剔除(权重→0)
      4. 无IC数据: fallback到经验权重
    """
    empirical = EMPIRICAL_WEIGHTS.get(strategy, {})
    if not ic_results:
        return dict(empirical)

    weights = {}
    total_ic = 0

    for factor_name, ic_data in ic_results.items():
        ic_mean = abs(ic_data["ic_mean"])
        icir = abs(ic_data["icir"])
        n_obs = ic_data["n_obs"]

        # 数据不足 → fallback
        if n_obs < MIN_OBSERVATIONS // 3:
            weights[factor_name] = empirical.get(factor_name, 0.25)
            continue

        # IC不显著 → 降权到1/3
        if ic_mean < IC_SIGNIFICANT:
            weights[factor_name] = empirical.get(factor_name, 0.25) / 3
            continue

        # ICIR差 → 降权
        multiplier = 1.0
        if icir < ICIR_GOOD:
            multiplier = 0.5

        weights[factor_name] = ic_mean * multiplier
        total_ic += ic_mean * multiplier

    # 归一化
    if total_ic > 0:
        weights = {k: round(v / total_ic, 4) for k, v in weights.items()}

    # 补充缺失因子(用经验权重)
    for factor_name, emp_weight in empirical.items():
        if factor_name not in weights:
            weights[factor_name] = emp_weight

    # 再次归一化
    total = sum(weights.values())
    if total > 0:
        weights = {k: round(v / total, 4) for k, v in weights.items()}

    return weights


def update_weights(strategy: str = None) -> dict:
    """更新因子权重(周末调用)

    Args:
        strategy: 指定策略(空=更新全部)

    Returns:
        {strategy: {factor_name: weight}}
    """
    conn = _get_conn()
    try:
        _ensure_table(conn)

        strategies = [strategy] if strategy else ["B", "C"]
        all_weights = {}

        for strat in strategies:
            logger.info(f"[权重] 更新策略{strat}因子权重...")

            # 计算IC
            ic_results = compute_factor_ic(strat)

            # IC → 权重
            weights = _ic_to_weights(ic_results, strat)

            # 写入DB
            for factor_name, weight in weights.items():
                ic_data = ic_results.get(factor_name, {})
                method = "ic_driven" if factor_name in ic_results else "empirical"

                conn.execute("""
                    INSERT OR REPLACE INTO factor_weights
                    (strategy, factor_name, ic_mean, ic_std, icir, ic_win_rate, weight, method, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
                """, (
                    strat, factor_name,
                    ic_data.get("ic_mean", 0), ic_data.get("ic_std", 0),
                    ic_data.get("icir", 0), ic_data.get("ic_win_rate", 0),
                    weight, method,
                ))

            all_weights[strat] = weights
            logger.info(f"[权重] 策略{strat}: {weights}")

        conn.commit()
        return all_weights

    finally:
        conn.close()


def get_weights(strategy: str) -> dict:
    """获取策略的因子权重(优先IC驱动, fallback经验权重)

    Returns:
        {factor_name: weight} (归一化, 和=1)
    """
    conn = _get_conn()
    try:
        _ensure_table(conn)

        rows = conn.execute(
            "SELECT factor_name, weight, method, updated_at FROM factor_weights WHERE strategy=?",
            (strategy,),
        ).fetchall()

        if not rows:
            return dict(EMPIRICAL_WEIGHTS.get(strategy, {}))

        # 检查是否过期
        if rows:
            updated = rows[0]["updated_at"]
            if updated:
                try:
                    age = (datetime.now() - datetime.strptime(updated[:19], "%Y-%m-%d %H:%M:%S")).days
                    if age > MAX_IC_AGE_DAYS:
                        logger.info(f"[权重] 策略{strategy}权重已{age}天未更新, fallback经验权重")
                        return dict(EMPIRICAL_WEIGHTS.get(strategy, {}))
                except Exception:
                    pass

        weights = {r["factor_name"]: r["weight"] for r in rows}

        # 归一化
        total = sum(weights.values())
        if total > 0:
            weights = {k: round(v / total, 4) for k, v in weights.items()}

        return weights

    finally:
        conn.close()


def show_factor_status(strategy: str = None):
    """显示因子权重状态"""
    conn = _get_conn()
    try:
        _ensure_table(conn)

        strategies = [strategy] if strategy else ["B", "C"]

        for strat in strategies:
            rows = conn.execute(
                "SELECT factor_name, ic_mean, icir, ic_win_rate, weight, method, updated_at "
                "FROM factor_weights WHERE strategy=? ORDER BY weight DESC",
                (strat,),
            ).fetchall()

            if not rows:
                emp = EMPIRICAL_WEIGHTS.get(strat, {})
                print(f"\n策略{strat}: 无IC数据, 使用经验权重")
                for k, v in emp.items():
                    print(f"  {k:15s}: {v:.0%}")
                continue

            print(f"\n策略{strat} 因子权重:")
            print(f"  {'因子':15s} | {'IC':>7s} | {'ICIR':>6s} | {'胜率':>5s} | {'权重':>6s} | {'方法':10s}")
            print(f"  {'-'*15}-+-{'-'*7}-+-{'-'*6}-+-{'-'*5}-+-{'-'*6}-+-{'-'*10}")
            for r in rows:
                ic = f"{r['ic_mean']:+.4f}" if r['ic_mean'] else "N/A"
                icir = f"{r['icir']:.2f}" if r['icir'] else "N/A"
                wr = f"{r['ic_win_rate']:.0%}" if r['ic_win_rate'] else "N/A"
                print(f"  {r['factor_name']:15s} | {ic:>7s} | {icir:>6s} | {wr:>5s} | {r['weight']:.2f} | {r['method']}")

    finally:
        conn.close()


if __name__ == "__main__":
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="因子动态权重引擎")
    parser.add_argument("--update", action="store_true", help="更新权重(周末执行)")
    parser.add_argument("--status", action="store_true", help="显示因子权重状态")
    parser.add_argument("--strategy", type=str, help="指定策略(B/C)")
    args = parser.parse_args()

    if args.update:
        update_weights(args.strategy)
    if args.status or not args.update:
        show_factor_status(args.strategy)
