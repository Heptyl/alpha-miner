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
        report_date = args.date
        # as_of 用 report_date 当天 23:59:59，确保 snapshot_time < as_of 能查到当天数据
        as_of = datetime.strptime(args.date, "%Y-%m-%d").replace(hour=23, minute=59, second=59)
        # 但数据通常是次日采集的(snapshot_time > trade_date)，所以加1天
        from datetime import timedelta
        as_of = as_of + timedelta(days=1)
    else:
        report_date = datetime.now().strftime("%Y-%m-%d")
        as_of = datetime.now()

    db = Storage(args.db)

    if args.brief:
        # 盘后决策简报模式
        from src.drift.daily_brief import DailyBrief

        print(f"[INFO] 生成盘后决策简报: {report_date}")
        brief = DailyBrief(db)

        holdings = []
        if args.holdings:
            holdings = [h.strip() for h in args.holdings.split(",") if h.strip()]

        text = brief.generate_full_report(as_of, holdings=holdings or None, top_n=args.top, report_date=report_date)
        print(text)

        # 保存
        save_path = args.save or f"reports/{report_date}_brief.txt"
        Path(save_path).parent.mkdir(parents=True, exist_ok=True)
        Path(save_path).write_text(text, encoding="utf-8")
        print(f"\n[INFO] 简报已保存: {save_path}")
    else:
        # 传统日报模式
        from src.drift.daily_report import DailyReport

        print(f"[INFO] 生成日报: {report_date}")
        report = DailyReport(db, mining_log_path=args.log)
        text = report.generate(as_of, report_date=report_date)

        # 终端输出
        print(text)

        # 保存到文件
        save_path = args.save or f"reports/{report_date}.txt"
        Path(save_path).parent.mkdir(parents=True, exist_ok=True)
        Path(save_path).write_text(text, encoding="utf-8")
        print(f"\n[INFO] 日报已保存: {save_path}")


if __name__ == "__main__":
    main()


def main_script():
    """市场剧本 CLI — python -m cli script"""
    parser = argparse.ArgumentParser(description="生成市场剧本")
    parser.add_argument("--date", type=str, default=None, help="报告日期 YYYY-MM-DD")
    parser.add_argument("--db", type=str, default="data/alpha_miner.db", help="数据库路径")
    parser.add_argument("--save", action="store_true", help="存入 market_scripts 表")
    parser.add_argument("--llm", action="store_true", help="启用 LLM 生成（默认纯规则）")
    args = parser.parse_args()

    if args.date:
        report_date = args.date
        as_of = datetime.strptime(args.date, "%Y-%m-%d").replace(hour=23, minute=59, second=59)
        from datetime import timedelta
        as_of = as_of + timedelta(days=1)
    else:
        report_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        as_of = datetime.now()

    db = Storage(args.db)
    db.init_db()

    llm_client = None
    if args.llm:
        from cli.mine import _build_llm_client
        llm_client, source = _build_llm_client()
        print(f"[INFO] LLM: {source}")

    from src.narrative.script_engine import ScriptEngine
    engine = ScriptEngine(db, llm_client=llm_client)
    script = engine.generate(as_of, report_date=report_date)

    print("=" * 60)
    print(f"  市场剧本 — {script.date}")
    print("=" * 60)
    print(f"\n标题: {script.script_title}")
    print(f"\n{script.script_narrative}")

    if script.theme_verdicts:
        print("\n[题材判定]")
        for tv in script.theme_verdicts:
            print(f"  {tv.get('theme', '?')} ({tv.get('stage', '?')}): {tv.get('verdict', '')}")

    if script.tomorrow_playbook:
        pb = script.tomorrow_playbook
        print(f"\n[明日策略] {pb.get('primary_strategy', '')}")
        if pb.get("watch_list"):
            print(f"  关注: {', '.join(pb['watch_list'])}")
        if pb.get("avoid_list"):
            print(f"  回避: {', '.join(pb['avoid_list'])}")
        print(f"  仓位: {pb.get('position_advice', '')}")

    if script.risk_alerts:
        print("\n[风险提示]")
        for a in script.risk_alerts:
            print(f"  - {a}")

    if args.save:
        engine.save_script(script)
        print(f"\n[INFO] 剧本已保存到数据库")


def main_replay():
    """复盘 CLI — python -m cli replay"""
    parser = argparse.ArgumentParser(description="复盘昨日剧本")
    parser.add_argument("--date", type=str, default=None, help="被复盘的日期 YYYY-MM-DD")
    parser.add_argument("--db", type=str, default="data/alpha_miner.db", help="数据库路径")
    parser.add_argument("--save", action="store_true", help="存入 replay_log 表")
    parser.add_argument("--stats", action="store_true", help="显示准确率统计")
    parser.add_argument("--llm", action="store_true", help="启用 LLM 生成（默认纯规则）")
    args = parser.parse_args()

    db = Storage(args.db)
    db.init_db()

    llm_client = None
    if args.llm:
        from cli.mine import _build_llm_client
        llm_client, source = _build_llm_client()

    if args.stats:
        from src.narrative.replay_engine import ReplayEngine
        engine = ReplayEngine(db)
        stats = engine.get_accuracy_stats()
        print("=" * 40)
        print("  复盘准确率统计")
        print("=" * 40)
        print(f"  样本数: {stats['total']}")
        print(f"  regime 准确率: {stats['regime_accuracy']:.0%}")
        print(f"  平均命中题材: {stats['avg_hits']:.1f}")
        print(f"  题材命中率: {stats['hit_rate']:.0%}")
        return

    if args.date:
        target_date = args.date
        as_of = datetime.strptime(args.date, "%Y-%m-%d").replace(hour=23, minute=59, second=59)
        from datetime import timedelta
        as_of = as_of + timedelta(days=1)
    else:
        from datetime import timedelta
        target_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        as_of = datetime.now()

    from src.narrative.replay_engine import ReplayEngine
    engine = ReplayEngine(db, llm_client=llm_client)
    result = engine.replay(as_of, target_date=target_date)

    print("=" * 60)
    print(f"  复盘 — {result.date}")
    print("=" * 60)
    print(f"\n{result.narrative}")

    if result.playbook_hits:
        print(f"\n[命中] {', '.join(result.playbook_hits)}")
    if result.playbook_misses:
        print(f"[错过] {', '.join(result.playbook_misses)}")
    if result.surprise_events:
        print("\n[异常事件]")
        for ev in result.surprise_events:
            print(f"  [{ev['type']}] {ev['detail']}")
    if result.lessons:
        print("\n[教训]")
        for l in result.lessons:
            print(f"  - {l}")
    if result.adjustment_suggestions:
        print("\n[调整建议]")
        for s in result.adjustment_suggestions:
            print(f"  - {s}")

    if args.save:
        engine.save_replay(result)
        print(f"\n[INFO] 复盘已保存到数据库")
