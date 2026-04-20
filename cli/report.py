"""日报 CLI — python -m cli.report

用法:
  python -m cli report --date 2026-04-17
  python -m cli report --brief                  # 盘后决策简报
  python -m cli report --brief --holdings 600xxx,000xxx
  python -m cli report                          # 默认日报
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path

from src.data.storage import Storage


def main():
    parser = argparse.ArgumentParser(description="Alpha Miner 日报")
    parser.add_argument("--date", type=str, default=None, help="报告日期 YYYY-MM-DD，默认今天")
    parser.add_argument("--db", type=str, default="data/alpha_miner.db", help="数据库路径")
    parser.add_argument("--log", type=str, default="data/mining_log.jsonl", help="挖掘日志路径")
    parser.add_argument("--save", type=str, default=None, help="保存路径 (默认 reports/YYYY-MM-DD.txt)")
    parser.add_argument("--brief", action="store_true", help="盘后决策简报模式（温度计+候选卡+持仓预警）")
    parser.add_argument("--holdings", type=str, default=None, help="持仓代码，逗号分隔（如 600xxx,000xxx）")
    parser.add_argument("--top", type=int, default=10, help="候选卡片数量（默认10）")
    args = parser.parse_args()

    if args.date:
        as_of = datetime.strptime(args.date, "%Y-%m-%d")
    else:
        as_of = datetime.now()

    db = Storage(args.db)

    if args.brief:
        # 盘后决策简报模式
        from src.drift.daily_brief import DailyBrief

        print(f"[INFO] 生成盘后决策简报: {as_of.strftime('%Y-%m-%d')}")
        brief = DailyBrief(db)

        holdings = []
        if args.holdings:
            holdings = [h.strip() for h in args.holdings.split(",") if h.strip()]

        text = brief.generate_full_report(as_of, holdings=holdings or None, top_n=args.top)
        print(text)

        # 保存
        save_path = args.save or f"reports/{as_of.strftime('%Y-%m-%d')}_brief.txt"
        Path(save_path).parent.mkdir(parents=True, exist_ok=True)
        Path(save_path).write_text(text, encoding="utf-8")
        print(f"\n[INFO] 简报已保存: {save_path}")
    else:
        # 传统日报模式
        from src.drift.daily_report import DailyReport

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
