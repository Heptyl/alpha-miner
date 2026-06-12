#!/usr/bin/env python3
"""Fill 2026-05-20 daily_price data using baostock (primary) + stock_zh_a_daily (fallback)."""
import sys
import time
import logging
import sqlite3
import pandas as pd

sys.path.insert(0, "/home/ccy/alpha-miner")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger(__name__)

import akshare as ak
import baostock as bs
from src.data.storage import Storage

TRADE_DATE = "2026-05-20"
DATE_STR = TRADE_DATE.replace("-", "")
DB_PATH = "/home/ccy/alpha-miner/data/alpha_miner.db"

def get_missing_codes():
    """Get stock codes that have 5-19 data but no 5-20 data."""
    conn = sqlite3.connect(DB_PATH)
    stocks_519 = set(r[0] for r in conn.execute(
        "SELECT DISTINCT stock_code FROM daily_price WHERE trade_date = '2026-05-19'"
    ).fetchall())
    stocks_520 = set(r[0] for r in conn.execute(
        "SELECT DISTINCT stock_code FROM daily_price WHERE trade_date = '2026-05-20'"
    ).fetchall())
    conn.close()
    missing = sorted(stocks_519 - stocks_520)
    logger.info(f"Missing 5-20 data: {len(missing)} stocks (have {len(stocks_520)}, need {len(stocks_519)})")
    return missing


def fetch_baostock_batch(codes):
    """Fetch missing codes via baostock."""
    lg = bs.login()
    if lg.error_code != "0":
        logger.error(f"baostock login failed: {lg.error_msg}")
        return []
    
    results = []
    fields = "date,code,open,high,low,close,preclose,volume,amount,turn"
    
    try:
        for i, code in enumerate(codes):
            bs_prefix = "sh" if code.startswith("6") else "sz"
            bs_code = f"{bs_prefix}.{code}"
            
            try:
                rs = bs.query_history_k_data_plus(
                    code=bs_code,
                    fields=fields,
                    start_date=DATE_STR,
                    end_date=DATE_STR,
                    frequency="d",
                    adjustflag="3",
                )
                if rs.error_code != "0":
                    continue
                
                while rs.error_code == "0" and rs.next():
                    row = rs.get_row_data()
                    try:
                        close_val = float(row[5]) if row[5] else 0.0
                        if close_val <= 0:
                            continue
                        results.append({
                            "stock_code": code,
                            "trade_date": TRADE_DATE,
                            "open": float(row[2]) if row[2] else None,
                            "high": float(row[3]) if row[3] else None,
                            "low": float(row[4]) if row[4] else None,
                            "close": close_val,
                            "pre_close": float(row[6]) if row[6] else None,
                            "volume": float(row[7]) if row[7] else None,
                            "amount": float(row[8]) if row[8] else None,
                            "turnover_rate": float(row[9]) if row[9] else None,
                        })
                    except (ValueError, IndexError):
                        continue
            except Exception as e:
                logger.debug(f"baostock failed for {code}: {e}")
                continue
            
            if (i + 1) % 500 == 0:
                logger.info(f"baostock progress: {i+1}/{len(codes)}, got {len(results)}")
        
    finally:
        bs.logout()
    
    return results


def fetch_sina_fallback(codes):
    """Fetch remaining codes via stock_zh_a_daily (新浪源)."""
    results = []
    consecutive_fail = 0
    
    for i, code in enumerate(codes):
        if consecutive_fail >= 20:
            logger.warning(f"Sina: {consecutive_fail} consecutive failures, stopping")
            break
        
        time.sleep(0.15)
        try:
            prefix = "sh" if code.startswith("6") else "sz"
            df = ak.stock_zh_a_daily(
                symbol=f"{prefix}{code}",
                start_date=DATE_STR,
                end_date=DATE_STR,
            )
            if df is not None and not df.empty:
                row = df.iloc[0]
                close_val = float(row.get("close", 0))
                if close_val > 0:
                    results.append({
                        "stock_code": code,
                        "trade_date": TRADE_DATE,
                        "open": float(row.get("open", 0)),
                        "high": float(row.get("high", 0)),
                        "low": float(row.get("low", 0)),
                        "close": close_val,
                        "volume": float(row.get("volume", 0)),
                        "amount": float(row.get("amount", 0)),
                        "turnover_rate": float(row.get("turnover", 0)),
                    })
                    consecutive_fail = 0
                    continue
        except Exception as e:
            logger.debug(f"Sina failed for {code}: {e}")
        
        consecutive_fail += 1
        
        if (i + 1) % 200 == 0:
            logger.info(f"Sina progress: {i+1}/{len(codes)}, got {len(results)}, consec_fail={consecutive_fail}")
    
    return results


def main():
    missing = get_missing_codes()
    if not missing:
        logger.info("No missing stocks. Done.")
        return

    # Phase 1: baostock (bulk, free, reliable)
    logger.info(f"Phase 1: baostock for {len(missing)} stocks")
    results = fetch_baostock_batch(missing)
    logger.info(f"Phase 1 done: got {len(results)} from baostock")
    
    # Check which are still missing
    fetched_codes = set(r["stock_code"] for r in results)
    still_missing = [c for c in missing if c not in fetched_codes]
    
    # Phase 2: Sina fallback for remaining
    if still_missing:
        logger.info(f"Phase 2: Sina fallback for {len(still_missing)} remaining stocks")
        sina_results = fetch_sina_fallback(still_missing)
        results.extend(sina_results)
        logger.info(f"Phase 2 done: got {len(sina_results)} from Sina")
    
    if not results:
        logger.error("No data fetched at all!")
        return
    
    # Save to DB
    df = pd.DataFrame(results)
    df = df[df["close"] > 0]
    logger.info(f"After filtering close>0: {len(df)} rows")
    
    storage = Storage()
    storage.init_db()
    count = storage.insert("daily_price", df, dedup=True)
    logger.info(f"Inserted {count} rows into daily_price")
    
    # Verify
    conn = sqlite3.connect(DB_PATH)
    final_cnt = conn.execute(
        "SELECT COUNT(*) FROM daily_price WHERE trade_date = ?", (TRADE_DATE,)
    ).fetchone()[0]
    final_stocks = conn.execute(
        "SELECT COUNT(DISTINCT stock_code) FROM daily_price WHERE trade_date = ?", (TRADE_DATE,)
    ).fetchone()[0]
    conn.close()
    logger.info(f"Final 5-20 daily_price: {final_cnt} rows, {final_stocks} unique stocks")


if __name__ == "__main__":
    main()
