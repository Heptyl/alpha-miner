"""Read-only live smoke test for the two full-market data sources."""

import argparse
import time
from datetime import date

from src.data.sources import akshare_fund_flow, akshare_price
from src.data.storage import Storage


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=date.today().isoformat())
    parser.add_argument("--db", default="data/alpha_miner.db")
    args = parser.parse_args()

    db = Storage(args.db)
    # Deliberately call bounded primary-source probes. A failed primary source
    # must not trigger the 5,000-stock fallback during a connectivity check.
    for name, fetcher in (
        ("price", lambda trade_date, db: akshare_price._fetch_tencent_full(trade_date, db=db)),
        (
            "fund_flow_page_1",
            lambda trade_date, db: akshare_fund_flow._fetch_ths_page(1, trade_date, retries=1),
        ),
    ):
        started = time.perf_counter()
        try:
            frame = fetcher(args.date, db)
        except Exception as exc:
            print(f"{name}: error={exc!r} elapsed={time.perf_counter() - started:.2f}s")
            continue
        print(f"{name}: rows={len(frame)} elapsed={time.perf_counter() - started:.2f}s")


if __name__ == "__main__":
    main()
