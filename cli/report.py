"""日报 CLI — python -m cli.report

用法:
  python -m cli.report --date 2026-04-17
  python -m cli.report           # 默认今天
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path

from src.data.storage import Storage
from src.drift.daily_report import DailyReport


def main():
    parser = argparse.ArgumentParser(description="Alpha Miner 日报")
    parser.add_argument("--date", type=str, default=None, help="报告日期 YYYY-MM-DD，默认今天")
    parser.add_argument("--db", type=str, default="data/alpha_miner.db", help="数据库路径")
    parser.add_argument("--log", type=str, default="data/mining_log.jsonl", help="挖掘日志路径")
    parser.add_argument("--save", type=str, default=None, help="保存路径 (默认 reports/YYYY-MM-DD.txt)")
    args = parser.parse_args()

    if args.date:
        as_of = datetime.strptime(args.date, "%Y-%m-%d")
    else:
        as_of = datetime.now()

    db = Storage(args.db)

    print(f"[INFO] 生成日报: {as_of.strftime('%Y-%m-%d')}")
    report = DailyReport(db, mining_log_path=args.log)
    text = report.generate(as_of)

    # 终端输出
    print(text)

    # 保存到文件
    save_path = args.save or f"reports/{as_of.strftime('%Y-%m-%d')}.txt"
    Path(save_path).parent.mkdir(parents=True, exist_ok=True)
    Path(save_path).write_text(text, encoding="utf-8")
    print(f"\n[INFO] 日报已保存: {save_path}")


if __name__ == "__main__":
    main()
