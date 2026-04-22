"""批量回填日K线 — 用腾讯行情 qt.gtimg.cn 批量拉取。

限制：腾讯行情只有实时/最新数据，无法拉历史日期。
所以这个脚本实际上只能拉"今天"的数据，用于补全当天的全量快照。

对于真正的历史回填，需要：
1. stock_zh_a_hist_tx（腾讯源逐只历史K线）
2. 或者 akshare 其他批量历史接口
"""

import sqlite3
import time
from datetime import datetime, timedelta

import pandas as pd
import requests

from src.data.storage import Storage

PREFIX_MAP = {'6': 'sh', '0': 'sz', '3': 'sz', '4': 'bj', '8': 'bj'}


def _code_to_tencent(code: str) -> str:
    prefix = PREFIX_MAP.get(code[0], "sz")
    return f"{prefix}{code}"


def fetch_tencent_today(trade_date: str, codes: list[str] | None = None) -> pd.DataFrame:
    """用腾讯行情批量拉全市场当日数据。
    
    注意：腾讯行情返回的是"最新"快照，如果盘中调用会返回实时价，
    如果盘后调用会返回当日收盘价。不能用于拉历史日期。
    """
    if codes is None:
        codes = _get_all_codes_from_db()
    if not codes:
        return pd.DataFrame()

    tc_codes = [_code_to_tencent(c) for c in codes]
    all_rows = []
    batch_size = 800

    for i in range(0, len(tc_codes), batch_size):
        batch = tc_codes[i:i + batch_size]
        url = "https://qt.gtimg.cn/q=" + ",".join(batch)
        try:
            r = requests.get(url, timeout=15)
            for line in r.text.split(";"):
                line = line.strip()
                if not line or "~" not in line:
                    continue
                parts = line.split("~")
                if len(parts) < 40:
                    continue
                try:
                    raw_code = parts[0]
                    tc_code = raw_code.split("_")[1].split("=")[0] if "_" in raw_code and "=" in raw_code else ""
                    stock_code = tc_code[2:] if len(tc_code) > 2 else ""
                    if not stock_code:
                        continue
                    row = {
                        "stock_code": stock_code,
                        "trade_date": trade_date,
                        "open": float(parts[5]) if parts[5] else None,
                        "close": float(parts[3]) if parts[3] else None,
                        "pre_close": float(parts[4]) if parts[4] else None,
                        "high": float(parts[33]) if parts[33] else None,
                        "low": float(parts[34]) if parts[34] else None,
                        "volume": float(parts[37]) if parts[37] else None,
                        "turnover_rate": float(parts[38]) if parts[38] else None,
                    }
                    try:
                        amt_parts = parts[35].split("/")
                        row["amount"] = float(amt_parts[2]) if len(amt_parts) > 2 else None
                    except (ValueError, IndexError):
                        row["amount"] = None
                    if row["close"] and row["close"] > 0:
                        all_rows.append(row)
                except (ValueError, IndexError):
                    continue
        except Exception:
            continue
        time.sleep(0.3)

    return pd.DataFrame(all_rows) if all_rows else pd.DataFrame()


def _get_all_codes_from_db() -> list[str]:
    """从DB已有数据获取全A股代码列表。"""
    try:
        conn = sqlite3.connect("data/alpha_miner.db")
        codes = [r[0] for r in conn.execute(
            "SELECT DISTINCT stock_code FROM daily_price"
        ).fetchall()]
        conn.close()
        return sorted(set(codes))
    except Exception:
        return []


def backfill_via_tencent_hist(trade_date: str, codes: list[str] | None = None,
                               batch_delay: float = 0.2, max_workers: int = 5) -> pd.DataFrame:
    """用 stock_zh_a_hist_tx（腾讯源历史K线）逐只拉取。
    
    比新浪源快，而且腾讯接口在国内直接可用。
    多线程并行 + 限流。
    """
    import akshare as ak
    import concurrent.futures
    import threading
    
    if codes is None:
        codes = _get_all_codes_from_db()
    if not codes:
        print("ERROR: 无可用股票代码")
        return pd.DataFrame()

    date_str = trade_date.replace("-", "")
    results = []
    lock = threading.Lock()
    fail_count = 0

    def _fetch_one(code: str) -> dict | None:
        nonlocal fail_count
        prefix = "sh" if code.startswith("6") else "sz"
        try:
            df = ak.stock_zh_a_hist_tx(
                symbol=f"{prefix}{code}",
                start_date=date_str,
                end_date=date_str,
            )
            if df is not None and not df.empty:
                row = df.iloc[0]
                return {
                    "stock_code": code,
                    "trade_date": trade_date,
                    "open": float(row.get("open", 0)),
                    "high": float(row.get("high", 0)),
                    "low": float(row.get("low", 0)),
                    "close": float(row.get("close", 0)),
                    "volume": float(row.get("volume", 0)) if "volume" in df.columns else None,
                    "amount": float(row.get("amount", 0)),
                    "turnover_rate": None,  # 腾讯源无换手率
                }
        except Exception:
            with lock:
                fail_count += 1
        return None

    total = len(codes)
    done = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_fetch_one, c): c for c in codes}
        for future in concurrent.futures.as_completed(futures):
            done += 1
            result = future.result()
            if result:
                results.append(result)
            if done % 500 == 0:
                print(f"  [{done}/{total}] {len(results)} OK, {fail_count} failed")
            with lock:
                if fail_count > 50:
                    print(f"  连续失败过多 ({fail_count})，终止")
                    for f in futures:
                        f.cancel()
                    break

    print(f"  完成: {len(results)}/{total} OK, {fail_count} failed")
    return pd.DataFrame(results) if results else pd.DataFrame()


def backfill_multi_days(start_date: str, end_date: str, skip_existing: bool = True) -> dict:
    """回填多天的日K线数据。
    
    策略：对每个交易日，用腾讯源逐只拉历史K线。
    
    Returns:
        dict: {date: row_count}
    """
    db = Storage("data/alpha_miner.db")
    db.init_db()
    
    # 获取交易日历（简单：排除周末）
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    trade_dates = []
    current = start
    while current <= end:
        if current.weekday() < 5:
            trade_dates.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)
    
    # 获取代码列表
    codes = _get_all_codes_from_db()
    if not codes:
        print("ERROR: 无股票代码，请先采集至少一天的全量数据")
        return {}
    
    print(f"回填范围: {start_date} ~ {end_date}, {len(trade_dates)} 个工作日, {len(codes)} 只股票")
    
    results = {}
    for i, date in enumerate(trade_dates, 1):
        # 跳过已有数据的日期
        if skip_existing:
            conn = sqlite3.connect("data/alpha_miner.db")
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM daily_price WHERE trade_date = ?", (date,))
            existing = cur.fetchone()[0]
            conn.close()
            if existing >= 4000:  # 已有足够数据
                print(f"[{i}/{len(trade_dates)}] {date}: 已有 {existing} 行，跳过")
                results[date] = existing
                continue
        
        print(f"[{i}/{len(trade_dates)}] {date}: 拉取中...")
        df = backfill_via_tencent_hist(date, codes)
        if not df.empty:
            count = db.insert("daily_price", df, dedup=True)
            results[date] = count
            print(f"  写入 {count} 行")
        else:
            results[date] = 0
            print(f"  无数据")
        
        # 天间延迟
        if i < len(trade_dates):
            time.sleep(1)
    
    return results


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 3:
        print("用法: python backfill_price.py START_DATE END_DATE")
        print("例: python backfill_price.py 2026-04-01 2026-04-19")
        sys.exit(1)
    
    results = backfill_multi_days(sys.argv[1], sys.argv[2])
    total = sum(results.values())
    print(f"\n总计: {total} rows from {len(results)} dates")
