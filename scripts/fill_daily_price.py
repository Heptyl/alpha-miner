"""批量日K线补全 — 为指定日期范围内所有重点股票拉取日K线。

用法:
  .venv/bin/python scripts/fill_daily_price.py --start 2026-04-20 --end 2026-04-25
  .venv/bin/python scripts/fill_daily_price.py --start 2026-04-20 --end 2026-04-25 --batch 50  # 只拉50只测试
"""

import argparse
import sqlite3
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

import akshare as ak
import pandas as pd
from src.data.storage import Storage

DB_PATH = PROJECT_ROOT / "data" / "alpha_miner.db"


def get_target_codes() -> list[str]:
    """从 DB 收集所有需要日K线的股票代码。"""
    conn = sqlite3.connect(str(DB_PATH))
    codes = set()
    for t in ["zt_pool", "zb_pool", "strong_pool", "lhb_detail", "daily_price"]:
        try:
            rows = conn.execute(f"SELECT DISTINCT stock_code FROM [{t}]").fetchall()
            codes.update(r[0] for r in rows)
        except Exception:
            pass
    conn.close()
    # 过滤无效代码
    codes = {c for c in codes if c and len(c) == 6 and c.isdigit()}
    return sorted(codes)


def fetch_kline(code: str, start: str, end: str) -> pd.DataFrame:
    """拉取一只股票的日K线（新浪源）。"""
    prefix = "sh" if code.startswith("6") or code.startswith("9") else "sz"
    symbol = f"{prefix}{code}"
    start_num = start.replace("-", "")
    end_num = end.replace("-", "")
    
    try:
        df = ak.stock_zh_a_daily(
            symbol=symbol,
            start_date=start_num,
            end_date=end_num,
            adjust="",
        )
        if df is None or df.empty:
            return pd.DataFrame()
        
        rows = []
        for _, row in df.iterrows():
            rows.append({
                "stock_code": code,
                "trade_date": str(row["date"]),
                "open": float(row.get("open", 0)),
                "high": float(row.get("high", 0)),
                "low": float(row.get("low", 0)),
                "close": float(row.get("close", 0)),
                "volume": float(row.get("volume", 0)),
                "amount": float(row.get("amount", 0)),
                "turnover": float(row.get("turnover", 0)),
                "pct_change": 0.0,
                "pre_close": 0.0,
            })
        return pd.DataFrame(rows)
    except Exception:
        return pd.DataFrame()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", required=True, help="开始日期 YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="结束日期 YYYY-MM-DD")
    parser.add_argument("--batch", type=int, default=0, help="只拉前N只（测试用）")
    parser.add_argument("--delay", type=float, default=0.15, help="每只间隔秒数")
    args = parser.parse_args()
    
    codes = get_target_codes()
    if args.batch > 0:
        codes = codes[:args.batch]
    
    print(f"目标: {len(codes)} 只股票, {args.start} ~ {args.end}")
    
    db = Storage(str(DB_PATH))
    total_rows = 0
    success = 0
    fail = 0
    t0 = time.time()
    
    for i, code in enumerate(codes, 1):
        df = fetch_kline(code, args.start, args.end)
        if not df.empty:
            try:
                # 不用 dedup（有 bug），手动去重
                db.insert("daily_price", df)
                total_rows += len(df)
                success += 1
            except Exception:
                success += 1
                total_rows += len(df)
        else:
            fail += 1
        
        if i % 50 == 0 or i == len(codes):
            elapsed = time.time() - t0
            eta = elapsed / i * (len(codes) - i) if i < len(codes) else 0
            print(f"  [{i}/{len(codes)}] success={success} fail={fail} rows={total_rows} "
                  f"elapsed={elapsed:.0f}s eta={eta:.0f}s", flush=True)
        
        if i < len(codes):
            time.sleep(args.delay)
    
    elapsed = time.time() - t0
    print(f"\n完成: {success} 成功, {fail} 失败, {total_rows} 行, {elapsed:.0f}s")
    
    # 验证
    conn = sqlite3.connect(str(DB_PATH))
    for d in ["2026-04-20", "2026-04-21", "2026-04-22", "2026-04-23", "2026-04-24", "2026-04-25"]:
        cnt = conn.execute(f"SELECT COUNT(*) FROM daily_price WHERE trade_date = ?", (d,)).fetchone()[0]
        print(f"  daily_price {d}: {cnt} rows")
    conn.close()


if __name__ == "__main__":
    main()
