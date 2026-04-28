"""批量日K线补全 — 直接调新浪API，绕开 akshare 超时问题。

用法:
  .venv/bin/python scripts/fill_daily_price_v2.py
"""

import re
import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

DB_PATH = PROJECT_ROOT / "data" / "alpha_miner.db"
SINA_URL = "https://quotes.sina.cn/cn/api/jsonp_v2.php/=/CN_MarketDataService.getKLineData"


def get_target_codes() -> list[str]:
    conn = sqlite3.connect(str(DB_PATH))
    codes = set()
    for t in ["zt_pool", "zb_pool", "strong_pool", "lhb_detail", "daily_price"]:
        try:
            rows = conn.execute(f"SELECT DISTINCT stock_code FROM [{t}]").fetchall()
            codes.update(r[0] for r in rows)
        except Exception:
            pass
    conn.close()
    return sorted(c for c in codes if c and len(c) == 6 and c.isdigit())


def fetch_kline_sina(code: str, datalen: int = 10) -> list[dict]:
    """直接调新浪 API 拉日K线。"""
    prefix = "sh" if code.startswith("6") or code.startswith("9") else "sz"
    params = {"symbol": f"{prefix}{code}", "scale": "240", "ma": "no", "datalen": str(datalen)}
    try:
        r = requests.get(SINA_URL, params=params, timeout=8)
        if r.status_code != 200:
            return []
        # 解析 JSONP: =([...])
        text = r.text
        m = re.search(r'=\((\[.*\])\)', text, re.DOTALL)
        if not m:
            return []
        import json
        data = json.loads(m.group(1))
        return data
    except Exception:
        return []


def main():
    codes = get_target_codes()
    print(f"共 {len(codes)} 只股票，拉最近 10 个交易日日K线", flush=True)
    
    conn = sqlite3.connect(str(DB_PATH))
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    
    total = 0
    success = 0
    fail = 0
    t0 = time.time()
    
    # 过滤已有哪些股票（跳过已有的）
    existing = set()
    try:
        rows = conn.execute("SELECT DISTINCT stock_code FROM daily_price").fetchall()
        existing = {r[0] for r in rows}
    except Exception:
        pass
    
    need = [c for c in codes if c not in existing]
    print(f"已存在 {len(existing)} 只，需拉 {len(need)} 只", flush=True)
    
    for i, code in enumerate(need, 1):
        data = fetch_kline_sina(code, datalen=10)
        if not data:
            fail += 1
        else:
            day_count = 0
            for item in data:
                day = item.get("day", "")
                # 只要 4.20-4.25
                if day < "2026-04-20" or day > "2026-04-25":
                    continue
                try:
                    conn.execute(
                        """INSERT OR IGNORE INTO daily_price 
                           (stock_code, trade_date, open, high, low, close, volume, amount, turnover_rate, pre_close, snapshot_time)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?)""",
                        (code, day,
                         float(item.get("open", 0)), float(item.get("high", 0)),
                         float(item.get("low", 0)), float(item.get("close", 0)),
                         float(item.get("volume", 0)), 0, 0, now_str)
                    )
                    day_count += 1
                except Exception:
                    pass
            if day_count > 0:
                success += 1
                total += day_count
            else:
                fail += 1
        
        if i % 50 == 0 or i == len(need):
            conn.commit()
            elapsed = time.time() - t0
            eta = elapsed / i * (len(need) - i) if i < len(need) else 0
            print(f"  [{i}/{len(need)}] ok={success} fail={fail} rows={total} "
                  f"elapsed={elapsed:.0f}s eta={eta:.0f}s", flush=True)
        
        time.sleep(0.1)
    
    conn.commit()
    conn.close()
    
    elapsed = time.time() - t0
    print(f"\n完成: {success} ok, {fail} fail, {total} rows, {elapsed:.0f}s")
    
    # 验证
    conn = sqlite3.connect(str(DB_PATH))
    for d in ["2026-04-20", "2026-04-21", "2026-04-22", "2026-04-23", "2026-04-24", "2026-04-25"]:
        cnt = conn.execute("SELECT COUNT(*) FROM daily_price WHERE trade_date = ?", (d,)).fetchone()[0]
        print(f"  daily_price {d}: {cnt} rows")
    total_all = conn.execute("SELECT COUNT(*) FROM daily_price").fetchone()[0]
    print(f"  total: {total_all}")
    conn.close()


if __name__ == "__main__":
    main()
