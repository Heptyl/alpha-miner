"""智能数据回填 v3 — 每个数据源独立超时，分阶段，断点续传。

用法:
  .venv/bin/python scripts/backfill_smart.py --days 30
  .venv/bin/python scripts/backfill_smart.py --days 30 --phase 1
  .venv/bin/python scripts/backfill_smart.py --days 30 --skip-kline
"""

import argparse
import signal
import sqlite3
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

import akshare as ak
import pandas as pd
from src.data.storage import Storage
from src.data.sources import akshare_zt_pool, akshare_lhb, akshare_news

DB_PATH = PROJECT_ROOT / "data" / "alpha_miner.db"

# 每个源的超时秒数
TIMEOUT_ZT = 15
TIMEOUT_LHB = 30
TIMEOUT_LEGU = 15
TIMEOUT_NEWS = 60
TIMEOUT_KLINE = 10  # 每只股票


def get_trade_dates(n: int) -> list[str]:
    dates = []
    current = datetime.now()
    while len(dates) < n:
        current -= timedelta(days=1)
        if current.weekday() < 5:
            dates.append(current.strftime("%Y-%m-%d"))
    return list(reversed(dates))


def get_existing_dates(table: str) -> set[str]:
    try:
        conn = sqlite3.connect(str(DB_PATH))
        rows = conn.execute(f"SELECT DISTINCT trade_date FROM [{table}]").fetchall()
        conn.close()
        return {r[0] for r in rows}
    except Exception:
        return set()


def count_rows(table: str) -> int:
    conn = sqlite3.connect(str(DB_PATH))
    cnt = conn.execute(f"SELECT COUNT(*) FROM [{table}]").fetchone()[0]
    conn.close()
    return cnt


def _run_with_timeout(fn, timeout_sec, *args):
    """在线程池中运行函数，超时则返回 None。"""
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(fn, *args)
        try:
            return future.result(timeout=timeout_sec)
        except FuturesTimeout:
            return None
        except Exception as e:
            return e


def phase1_day(date_str: str, db: Storage) -> dict:
    """采集一天轻量数据，每个源独立超时。"""
    results = {}
    
    # 1. 涨停池
    r = _run_with_timeout(akshare_zt_pool.fetch_zt_pool, TIMEOUT_ZT, date_str)
    if isinstance(r, pd.DataFrame) and not r.empty:
        results["zt_pool"] = akshare_zt_pool.save_zt_pool(r, db)
    elif isinstance(r, Exception):
        results["zt_pool"] = f"ERR"
    else:
        results["zt_pool"] = 0
    
    time.sleep(0.2)
    
    # 2. 炸板池
    r = _run_with_timeout(akshare_zt_pool.fetch_zb_pool, TIMEOUT_ZT, date_str)
    if isinstance(r, pd.DataFrame) and not r.empty:
        results["zb_pool"] = akshare_zt_pool.save_zb_pool(r, db)
    elif isinstance(r, Exception):
        results["zb_pool"] = f"ERR"
    else:
        results["zb_pool"] = 0
    
    time.sleep(0.2)
    
    # 3. 强势股
    r = _run_with_timeout(akshare_zt_pool.fetch_strong_pool, TIMEOUT_ZT, date_str)
    if isinstance(r, pd.DataFrame) and not r.empty:
        results["strong_pool"] = akshare_zt_pool.save_strong_pool(r, db)
    elif isinstance(r, Exception):
        results["strong_pool"] = f"ERR"
    else:
        results["strong_pool"] = 0
    
    time.sleep(0.2)
    
    # 4. 龙虎榜
    r = _run_with_timeout(akshare_lhb.fetch, TIMEOUT_LHB, date_str)
    if isinstance(r, pd.DataFrame) and not r.empty:
        results["lhb_detail"] = akshare_lhb.save(r, db)
    elif isinstance(r, Exception):
        results["lhb_detail"] = f"ERR"
    else:
        results["lhb_detail"] = 0
    
    time.sleep(0.2)
    
    # 5. 市场情绪
    def _fetch_emotion():
        ma_df = ak.stock_market_activity_legu()
        if ma_df is None or ma_df.empty:
            return None
        data = dict(zip(ma_df["item"], ma_df["value"]))
        zt_count = int(data.get("真实涨停", 0) or 0)
        dt_count = int(data.get("真实跌停", 0) or 0)
        activity = str(data.get("活跃度", "0%"))
        up_count = int(data.get("上涨", 0) or 0)
        down_count = int(data.get("下跌", 0) or 0)
        
        highest_board = 0
        zt_df = db.query("zt_pool", datetime(2099, 1, 1), where="trade_date = ?", params=(date_str,))
        if not zt_df.empty and "consecutive_zt" in zt_df.columns:
            highest_board = int(zt_df["consecutive_zt"].max())
        
        if zt_count > 100 or highest_board >= 8:
            level = "extreme_greed"
        elif zt_count > 60 or highest_board >= 5:
            level = "greed"
        elif zt_count > 30:
            level = "neutral"
        elif zt_count > 10:
            level = "fear"
        else:
            level = "extreme_fear"
        
        return pd.DataFrame([{
            "trade_date": date_str,
            "zt_count": zt_count, "dt_count": dt_count,
            "up_count": up_count, "down_count": down_count,
            "highest_board": highest_board, "activity": activity,
            "sentiment_level": level,
        }])
    
    r = _run_with_timeout(_fetch_emotion, TIMEOUT_LEGU)
    if isinstance(r, pd.DataFrame):
        db.insert("market_emotion", r)
        results["market_emotion"] = 1
    else:
        results["market_emotion"] = 0
    
    return results


def phase2_day(date_str: str, db: Storage) -> int:
    """为涨停+龙虎榜股票补日K线。"""
    conn = db._get_conn()
    codes = []
    for table in ["zt_pool", "zb_pool", "strong_pool", "lhb_detail"]:
        try:
            rows = conn.execute(
                f"SELECT DISTINCT stock_code FROM [{table}] WHERE trade_date = ?",
                (date_str,),
            ).fetchall()
            codes.extend([r[0] for r in rows])
        except Exception:
            pass
    conn.close()
    codes = list(dict.fromkeys(codes))
    if not codes:
        return 0
    
    existing = db.query("daily_price", datetime(2099, 1, 1), where="trade_date = ?", params=(date_str,))
    existing_codes = set(existing["stock_code"].tolist()) if not existing.empty else set()
    codes = [c for c in codes if c not in existing_codes]
    if not codes:
        return 0
    
    success = 0
    for code in codes:
        r = _run_with_timeout(_fetch_kline_one, TIMEOUT_KLINE, code, date_str)
        if isinstance(r, pd.DataFrame):
            db.insert("daily_price", r)
            success += 1
        time.sleep(0.3)
    return success


def _fetch_kline_one(code: str, date_str: str) -> pd.DataFrame:
    prefix = "sh" if code.startswith("6") or code.startswith("9") else "sz"
    df = ak.stock_zh_a_daily(
        symbol=f"{prefix}{code}",
        start_date=date_str.replace("-", ""),
        end_date=date_str.replace("-", ""),
        adjust="",
    )
    if df is not None and not df.empty:
        row = df.iloc[0]
        return pd.DataFrame([{
            "stock_code": code, "trade_date": date_str,
            "open": float(row.get("open", 0)),
            "high": float(row.get("high", 0)),
            "low": float(row.get("low", 0)),
            "close": float(row.get("close", 0)),
            "volume": float(row.get("volume", 0)),
            "amount": float(row.get("amount", 0)),
            "turnover": float(row.get("turnover", 0)),
            "pct_change": 0.0, "pre_close": 0.0,
        }])
    return None


def phase3_day(date_str: str, db: Storage) -> int:
    """拉资金流向（同花顺源）。"""
    from src.data.sources.akshare_fund_flow import fetch as fetch_ff, save as save_ff
    r = _run_with_timeout(fetch_ff, 120, date_str)
    if isinstance(r, pd.DataFrame) and not r.empty:
        return save_ff(r, db, dedup=True)
    return 0


def print_status():
    print("\n" + "-" * 40)
    tables = ["daily_price", "zt_pool", "zb_pool", "strong_pool",
              "lhb_detail", "fund_flow", "news", "market_emotion", "factor_values"]
    for t in tables:
        print(f"  {t:20s}: {count_rows(t):>6} rows")
    for t in ["daily_price", "zt_pool", "lhb_detail", "market_emotion"]:
        dates = get_existing_dates(t)
        if dates:
            sd = sorted(dates)
            print(f"  {t:20s}: {len(dates)} days ({sd[0]} ~ {sd[-1]})")
    print("-" * 40)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--skip-kline", action="store_true")
    parser.add_argument("--skip-fund-flow", action="store_true")
    parser.add_argument("--phase", type=int, choices=[1, 2, 3])
    args = parser.parse_args()
    
    db = Storage(str(DB_PATH))
    db.init_db()
    dates = get_trade_dates(args.days)
    
    print(f"回填 {len(dates)} 天 ({dates[0]} ~ {dates[-1]})")
    print_status()
    
    # Phase 1
    if args.phase is None or args.phase == 1:
        print("\n=== Phase 1: 轻量接口 ===")
        existing = get_existing_dates("zt_pool")
        need = [d for d in dates if d not in existing]
        print(f"已有 {len(existing)} 天, 需回填 {len(need)} 天")
        
        ok, empty, fail = 0, 0, 0
        for i, d in enumerate(need, 1):
            print(f"\n[{i}/{len(need)}] {d}", end=" ", flush=True)
            try:
                r = phase1_day(d, db)
                parts = [f"{k}={v}" for k, v in r.items() if v and v != 0]
                total = sum(v for v in r.values() if isinstance(v, int))
                if total > 0:
                    ok += 1
                    print(f"OK ({', '.join(parts)})")
                else:
                    empty += 1
                    print("EMPTY")
            except Exception as e:
                fail += 1
                print(f"FAIL: {str(e)[:60]}")
            
            if i < len(need):
                time.sleep(1)
        
        print(f"\nPhase 1: {ok} ok, {empty} empty, {fail} fail")
        print_status()
    
    # Phase 2
    if not args.skip_kline and (args.phase is None or args.phase == 2):
        print("\n=== Phase 2: 日K线 (重点股票) ===")
        zt_dates = get_existing_dates("zt_pool")
        need = [d for d in dates if d in zt_dates]
        
        total = 0
        for i, d in enumerate(need, 1):
            try:
                cnt = phase2_day(d, db)
                total += cnt
                print(f"  [{i}/{len(need)}] {d}: +{cnt}")
            except Exception as e:
                print(f"  [{i}/{len(need)}] {d}: ERR {str(e)[:40]}")
        
        print(f"\nPhase 2: 共 {total} 行")
    
    # Phase 3
    if not args.skip_fund_flow and (args.phase is None or args.phase == 3):
        print("\n=== Phase 3: 资金流向 ===")
        existing = get_existing_dates("fund_flow")
        zt_dates = get_existing_dates("zt_pool")
        need = [d for d in dates if d in zt_dates and d not in existing]
        print(f"需补 {len(need)} 天")
        
        total = 0
        for i, d in enumerate(need, 1):
            print(f"  [{i}/{len(need)}] {d}", end=" ", flush=True)
            try:
                cnt = phase3_day(d, db)
                total += cnt
                print(f"+{cnt}")
            except Exception as e:
                print(f"FAIL")
            time.sleep(2)
        
        print(f"\nPhase 3: 共 {total} 行")
    
    print_status()
    print("\nDone!")


if __name__ == "__main__":
    main()
