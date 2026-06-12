"""龙虎榜席位明细采集脚本

用法:
  # 采集今天
  uv run python scripts/collect_lhb_seats.py --today

  # 采集指定日期
  uv run python scripts/collect_lhb_seats.py --date 2026-05-15

  # 回填最近N天
  uv run python scripts/collect_lhb_seats.py --backfill 10
"""

import argparse
import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.data.sources.lhb_seats import fetch_date_seats, save


def main():
    parser = argparse.ArgumentParser(description="龙虎榜席位明细采集")
    parser.add_argument("--today", action="store_true", help="采集今天")
    parser.add_argument("--date", type=str, help="采集指定日期 YYYY-MM-DD")
    parser.add_argument("--backfill", type=int, help="回填最近N天")
    args = parser.parse_args()

    if args.today:
        dates = [datetime.now().strftime("%Y-%m-%d")]
    elif args.date:
        dates = [args.date]
    elif args.backfill:
        dates = []
        for i in range(args.backfill):
            d = datetime.now() - timedelta(days=i)
            if d.weekday() < 5:  # 跳过周末
                dates.append(d.strftime("%Y-%m-%d"))
    else:
        parser.print_help()
        return

    total = 0
    for d in dates:
        print(f"\n{'='*50}")
        print(f"采集 {d}")
        print(f"{'='*50}")
        rows = fetch_date_seats(d)
        cnt = save(rows)
        total += cnt
        print(f"  写入 {cnt} 条席位明细")

    print(f"\n总计写入 {total} 条席位明细 ({len(dates)} 天)")


if __name__ == "__main__":
    main()
