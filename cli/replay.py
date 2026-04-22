"""复盘 CLI — python -m cli.replay

用法:
  python -m cli.replay --date 2026-04-21
  python -m cli.replay --date 2026-04-21 --save
  python -m cli.replay --stats
  python -m cli.replay --date 2026-04-21 --llm
"""

import argparse
from datetime import datetime, timedelta

from src.data.storage import Storage


def main():
    parser = argparse.ArgumentParser(description="复盘昨日剧本")
    parser.add_argument("--date", type=str, default=None,
                        help="被复盘的日期 YYYY-MM-DD（默认昨天）")
    parser.add_argument("--db", type=str, default="data/alpha_miner.db",
                        help="数据库路径")
    parser.add_argument("--save", action="store_true",
                        help="存入 replay_log 表")
    parser.add_argument("--stats", action="store_true",
                        help="显示准确率统计")
    parser.add_argument("--llm", action="store_true",
                        help="启用 LLM 生成（默认纯规则）")
    args = parser.parse_args()

    db = Storage(args.db)
    db.init_db()

    llm_client = None
    if args.llm:
        from cli.mine import _build_llm_client
        llm_client, source = _build_llm_client()
        print(f"[INFO] LLM: {source}")

    from src.narrative.replay_engine import ReplayEngine

    if args.stats:
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

    # 确定复盘日期
    if args.date:
        target_date = args.date
        as_of = datetime.strptime(args.date, "%Y-%m-%d").replace(
            hour=23, minute=59, second=59)
        as_of = as_of + timedelta(days=1)
    else:
        target_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        as_of = datetime.now()

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


if __name__ == "__main__":
    main()
