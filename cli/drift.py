"""漂移报告 CLI — python -m cli.drift --date 2024-06-15"""

import argparse
import sys
from datetime import datetime

from src.data.storage import Storage
from src.drift.report import DriftReport


def main():
    parser = argparse.ArgumentParser(description="Alpha Miner 漂移报告")
    parser.add_argument("--date", type=str, default=None, help="报告日期 (YYYY-MM-DD)")
    parser.add_argument("--db", type=str, default="data/alpha_miner.db", help="数据库路径")
    parser.add_argument("--ic-window", type=int, default=20, help="IC 滚动窗口")
    args = parser.parse_args()

    if args.date:
        as_of = datetime.strptime(args.date, "%Y-%m-%d")
    else:
        as_of = datetime.now()

    db = Storage(args.db)
    reporter = DriftReport(db)
    report = reporter.generate(as_of, ic_window=args.ic_window)
    print(reporter.format_rich(report))


if __name__ == "__main__":
    main()
