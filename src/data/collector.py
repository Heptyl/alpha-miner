"""数据采集调度器 — 统一调用各数据源，单源失败不影响整体。"""

from datetime import datetime
from typing import Optional

from src.data.storage import Storage
from src.data.sources import akshare_price, akshare_zt_pool, akshare_lhb, akshare_fund_flow


def collect_date(trade_date: str, db: Optional[Storage] = None) -> dict[str, int]:
    """采集指定日期的全市场数据。

    逐个调用数据源，单源失败不影响其他源。
    返回各数据源的入库行数。

    Args:
        trade_date: 交易日期 YYYY-MM-DD
        db: 数据库实例，None 时使用默认路径

    Returns:
        dict: {source_name: row_count}
    """
    if db is None:
        db = Storage()
        db.init_db()

    results = {}

    # 1. 日K线
    try:
        df = akshare_price.fetch(trade_date)
        count = akshare_price.save(df, db)
        results["daily_price"] = count
        print(f"  [OK] daily_price: {count} rows")
    except Exception as e:
        results["daily_price"] = 0
        print(f"  [FAIL] daily_price: {e}")

    # 2. 涨停池
    try:
        df = akshare_zt_pool.fetch_zt_pool(trade_date)
        count = akshare_zt_pool.save_zt_pool(df, db)
        results["zt_pool"] = count
        print(f"  [OK] zt_pool: {count} rows")
    except Exception as e:
        results["zt_pool"] = 0
        print(f"  [FAIL] zt_pool: {e}")

    # 3. 炸板池
    try:
        df = akshare_zt_pool.fetch_zb_pool(trade_date)
        count = akshare_zt_pool.save_zb_pool(df, db)
        results["zb_pool"] = count
        print(f"  [OK] zb_pool: {count} rows")
    except Exception as e:
        results["zb_pool"] = 0
        print(f"  [FAIL] zb_pool: {e}")

    # 4. 强势股
    try:
        df = akshare_zt_pool.fetch_strong_pool(trade_date)
        count = akshare_zt_pool.save_strong_pool(df, db)
        results["strong_pool"] = count
        print(f"  [OK] strong_pool: {count} rows")
    except Exception as e:
        results["strong_pool"] = 0
        print(f"  [FAIL] strong_pool: {e}")

    # 5. 龙虎榜
    try:
        df = akshare_lhb.fetch(trade_date)
        count = akshare_lhb.save(df, db)
        results["lhb_detail"] = count
        print(f"  [OK] lhb_detail: {count} rows")
    except Exception as e:
        results["lhb_detail"] = 0
        print(f"  [FAIL] lhb_detail: {e}")

    # 6. 资金流向
    try:
        df = akshare_fund_flow.fetch(trade_date)
        count = akshare_fund_flow.save(df, db)
        results["fund_flow"] = count
        print(f"  [OK] fund_flow: {count} rows")
    except Exception as e:
        results["fund_flow"] = 0
        print(f"  [FAIL] fund_flow: {e}")

    total = sum(results.values())
    print(f"  Total: {total} rows from {len(results)} sources")
    return results
