"""日报 CLI — python -m cli.report

用法:
  python -m cli report --date 2026-04-17
  python -m cli report --brief                  # 盘后决策简报
  python -m cli report --brief --holdings '600000,000001'
  python -m cli report                          # 默认日报
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path

_PLAIN_TEXT_REPLACEMENTS = {
    "❄️": "[极弱]",
    "☁️": "[弱]",
    "⛅": "[中性]",
    "⚡": "[偏强]",
    "🔥": "[强]",
    "✅": "[正常]",
    "⚠️": "[注意]",
    "❌": "[不可用]",
    "⬜": "[无数据]",
    "🟢": "[买入]",
    "🟡": "[观望]",
    "🔴": "[回避]",
    "⚪": "[未分类]",
    "┌": "+",
    "┐": "+",
    "└": "+",
    "┘": "+",
    "├": "+",
    "┤": "+",
    "─": "-",
    "│": "|",
    "↑": "^",
    "↓": "v",
    "→": "->",
    "■": "#",
    "□": ".",
    "⚠": "[注意]",
    "▸": ">",
}


def _parse_holdings(value: str) -> list[str]:
    """Parse comma-separated holdings without loading data/science dependencies."""
    holdings = []
    for raw_code in value.split(","):
        code = raw_code.strip()
        if len(code) != 6 or not code.isascii() or not code.isdigit():
            raise argparse.ArgumentTypeError(
                f"持仓代码必须是 6 位数字字符串，收到: {raw_code!r}"
            )
        holdings.append(code)
    return holdings


def _terminal_safe_text(text: str, encoding: str | None = None) -> str:
    """Return rich text when supported, otherwise a deterministic plain-text form."""
    output_encoding = encoding or getattr(sys.stdout, "encoding", None) or "utf-8"
    try:
        text.encode(output_encoding)
        return text
    except UnicodeEncodeError:
        plain = text
        for rich, fallback in _PLAIN_TEXT_REPLACEMENTS.items():
            plain = plain.replace(rich, fallback)

        # Database-provided names may still contain a character outside the
        # console code page. Escape only those individual characters so the
        # report remains printable and the surrounding Chinese stays readable.
        safe_parts = []
        for char in plain:
            try:
                char.encode(output_encoding)
                safe_parts.append(char)
            except UnicodeEncodeError:
                safe_parts.append(f"\\u{ord(char):04X}")
        return "".join(safe_parts)


def main():
    parser = argparse.ArgumentParser(description="Alpha Miner 日报")
    parser.add_argument("--date", type=str, default=None, help="报告日期 YYYY-MM-DD，默认今天")
    parser.add_argument("--db", type=str, default="data/alpha_miner.db", help="数据库路径")
    parser.add_argument("--log", type=str, default="data/mining_log.jsonl", help="挖掘日志路径")
    parser.add_argument("--save", type=str, default=None, help="保存 UTF-8 文本到指定路径（默认不保存）")
    parser.add_argument("--brief", action="store_true", help="盘后决策简报模式（温度计+候选卡+持仓预警）")
    parser.add_argument(
        "--holdings",
        type=_parse_holdings,
        default=None,
        help="6 位持仓代码，逗号分隔（PowerShell 示例: '600000,000001'）",
    )
    parser.add_argument("--top", type=int, default=10, help="候选卡片数量（默认10）")
    args = parser.parse_args()

    if args.date:
        report_date = args.date
        # as_of 用 report_date 当天 23:59:59，确保 snapshot_time < as_of 能查到当天数据
        as_of = datetime.strptime(args.date, "%Y-%m-%d").replace(hour=23, minute=59, second=59)
        # 但数据通常是次日采集的(snapshot_time > trade_date)，所以加1天
        from datetime import timedelta
        as_of = as_of + timedelta(days=1)
    else:
        report_date = datetime.now().strftime("%Y-%m-%d")
        as_of = datetime.now()

    from src.data.storage import Storage

    db = Storage(args.db)

    if args.brief:
        # 盘后决策简报模式
        from src.drift.daily_brief import DailyBrief

        brief = DailyBrief(db)

        holdings = args.holdings or []

        print(f"[INFO] 生成盘后决策简报: {report_date}")

        text = brief.generate_full_report(as_of, holdings=holdings or None, top_n=args.top, report_date=report_date)
        print(_terminal_safe_text(text))

        if args.save:
            save_path = Path(args.save)
            save_path.parent.mkdir(parents=True, exist_ok=True)
            save_path.write_text(text, encoding="utf-8")
            print(_terminal_safe_text(f"\n[INFO] 简报已保存: {save_path}"))
    else:
        # 传统日报模式
        from src.drift.daily_report import DailyReport

        print(f"[INFO] 生成日报: {report_date}")
        report = DailyReport(db, mining_log_path=args.log)
        text = report.generate(as_of, report_date=report_date)

        # 终端输出
        print(_terminal_safe_text(text))

        if args.save:
            save_path = Path(args.save)
            save_path.parent.mkdir(parents=True, exist_ok=True)
            save_path.write_text(text, encoding="utf-8")
            print(_terminal_safe_text(f"\n[INFO] 日报已保存: {save_path}"))


if __name__ == "__main__":
    main()
