"""每日管线运行器 — 跑通 IC → Drift → Regime → 持久化。

从已有 factor_values + daily_price 计算，不需要额外数据源。
"""

import sqlite3
import warnings
from datetime import datetime

import numpy as np
import pandas as pd

from src.data.storage import Storage
from src.drift.ic_tracker import ICTracker
from src.drift.cusum import detect_changepoints
from src.drift.regime import RegimeDetector


def run_ic_pipeline(db: Storage, start_date: str | None = None,
                    end_date: str | None = None, forward_days: int = 1) -> dict:
    """运行 IC 管线：计算所有因子的 IC 并持久化。
    
    Returns:
        dict: {factor_name: {"dates": N, "valid_ic": M, "avg_ic": X}}
    """
    tracker = ICTracker(db)
    
    # 获取有 factor_values 的日期范围
    conn = db._get_conn()
    try:
        if not start_date:
            start_date = conn.execute("SELECT MIN(trade_date) FROM factor_values").fetchone()[0]
        if not end_date:
            end_date = conn.execute("SELECT MAX(trade_date) FROM factor_values").fetchone()[0]
        
        factors = [r[0] for r in conn.execute(
            "SELECT DISTINCT factor_name FROM factor_values ORDER BY factor_name"
        ).fetchall()]
    finally:
        conn.close()
    
    print(f"IC管线: {start_date} ~ {end_date}, {len(factors)} 个因子")
    
    results = {}
    for fn in factors:
        ic_df = tracker.compute_ic_series(fn, start_date, end_date,
                                           forward_days=forward_days, window=5, persist=True)
        valid = ic_df["ic"].dropna() if not ic_df.empty else pd.Series(dtype=float)
        results[fn] = {
            "dates": len(ic_df),
            "valid_ic": len(valid),
            "avg_ic": float(valid.mean()) if len(valid) > 0 else np.nan,
        }
    
    return results


def run_drift_pipeline(db: Storage, threshold: float = 1.5) -> list[dict]:
    """运行漂移检测管线：对有IC数据的因子做CUSUM变点检测。
    
    Returns:
        list of drift event dicts
    """
    conn = db._get_conn()
    try:
        factors = [r[0] for r in conn.execute(
            "SELECT DISTINCT factor_name FROM ic_series"
        ).fetchall()]
    finally:
        conn.close()
    
    events = []
    for fn in factors:
        conn = db._get_conn()
        try:
            ic_data = pd.read_sql_query(
                "SELECT trade_date, ic_value FROM ic_series "
                "WHERE factor_name = ? AND forward_days = 1 "
                "ORDER BY trade_date",
                conn, params=(fn,),
            )
        finally:
            conn.close()
        
        if len(ic_data) < 10:
            print(f"  {fn}: IC数据不足 ({len(ic_data)} 天), 跳过漂移检测")
            continue
        
        ic_series = ic_data.set_index("trade_date")["ic_value"]
        result = detect_changepoints(ic_series, threshold=threshold)
        
        for cp_idx in result.changepoints:
            if cp_idx < len(ic_data):
                event = {
                    "factor_name": fn,
                    "event_date": ic_data.iloc[cp_idx]["trade_date"],
                    "event_type": "cusum_changepoint",
                    "description": f"CUSUM变点 detected at index {cp_idx}, "
                                   f"IC={ic_series.iloc[cp_idx]:.4f}, "
                                   f"threshold={threshold}",
                }
                events.append(event)
                print(f"  DRIFT: {fn} @ {event['event_date']} — {event['description']}")
    
    # 持久化 drift_events
    if events:
        df = pd.DataFrame(events)
        count = db.insert("drift_events", df)
        print(f"  写入 {count} 条 drift_events")
    
    return events


def run_regime_pipeline(db: Storage, date: str | None = None) -> dict | None:
    """运行 regime 识别管线。
    
    Returns:
        RegimeInfo as dict, or None if failed
    """
    detector = RegimeDetector(db)
    
    if not date:
        conn = db._get_conn()
        try:
            date = conn.execute(
                "SELECT MAX(trade_date) FROM daily_price"
            ).fetchone()[0]
        finally:
            conn.close()
    
    regime_info = detector.detect(datetime.strptime(date, "%Y-%m-%d"))
    
    # 持久化
    row = pd.DataFrame([{
        "trade_date": date,
        "regime_type": regime_info.regime,
        "confidence": regime_info.confidence,
    }])
    count = db.insert("regime_state", row)
    
    result = {
        "date": date,
        "regime": regime_info.regime,
        "confidence": regime_info.confidence,
        "details": regime_info.details,
        "persisted": count > 0,
    }
    print(f"  Regime: {date} → {regime_info.regime} (conf={regime_info.confidence:.2f})")
    if regime_info.details:
        for k, v in regime_info.details.items():
            print(f"    {k}: {v}")
    
    return result


def run_full_pipeline(db_path: str = "data/alpha_miner.db") -> dict:
    """运行完整管线：IC → Drift → Regime。
    
    Returns:
        summary dict
    """
    warnings.filterwarnings("ignore")
    db = Storage(db_path)
    db.init_db()
    
    print("=" * 60)
    print("Alpha Miner — 全管线运行")
    print("=" * 60)
    
    # 1. IC
    print("\n[1/3] IC 计算...")
    ic_results = run_ic_pipeline(db)
    for fn, info in ic_results.items():
        ic_str = f"{info['avg_ic']:.4f}" if info['avg_ic'] == info['avg_ic'] else "N/A"
        print(f"  {fn:25s}: {info['valid_ic']}/{info['dates']} valid IC, avg={ic_str}")
    
    # 2. Drift
    print("\n[2/3] 漂移检测...")
    drift_events = run_drift_pipeline(db)
    if not drift_events:
        print("  无漂移事件（数据不足或IC稳定）")
    
    # 3. Regime
    print("\n[3/3] Regime 识别...")
    regime_result = run_regime_pipeline(db)
    
    # 汇总
    summary = {
        "ic_factors": len(ic_results),
        "ic_with_data": sum(1 for v in ic_results.values() if v["valid_ic"] > 0),
        "drift_events": len(drift_events),
        "regime": regime_result["regime"] if regime_result else "unknown",
    }
    
    print("\n" + "=" * 60)
    print(f"完成! IC因子: {summary['ic_factors']}, 有效IC: {summary['ic_with_data']}, "
          f"漂移事件: {summary['drift_events']}, Regime: {summary['regime']}")
    print("=" * 60)
    
    return summary


if __name__ == "__main__":
    run_full_pipeline()
