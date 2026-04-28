#!/usr/bin/env python3
"""盘后复盘脚本 — 每日15:30运行。

流程：
1. 确认今天是交易日且有收盘数据
2. 采集今日最新数据
3. 对比昨日推荐 vs 今日实际走势
4. 生成复盘报告
5. 保存 + 推送到微信

用法:
  uv run python scripts/evening_review.py
  uv run python scripts/evening_review.py --dry-run
  uv run python scripts/evening_review.py --date 2026-04-29
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


def main():
    parser = argparse.ArgumentParser(description="盘后复盘 — 15:30 运行")
    parser.add_argument("--dry-run", action="store_true", help="只生成不推送")
    parser.add_argument("--date", type=str, default=None, help="复盘日期 YYYY-MM-DD")
    args = parser.parse_args()

    now = datetime.now()
    review_date = args.date or now.strftime("%Y-%m-%d")

    print(f"{'='*60}")
    print(f"  Alpha Miner 盘后复盘")
    print(f"  运行时间: {now.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  复盘日期: {review_date}")
    print(f"{'='*60}")

    # 确认有当日数据
    import sqlite3
    conn = sqlite3.connect("data/alpha_miner.db")
    cnt = conn.execute(
        "SELECT COUNT(*) FROM daily_price WHERE trade_date = ?",
        (review_date,),
    ).fetchone()[0]
    conn.close()

    if cnt < 10:
        print(f"  ⚠ {review_date} 无足够数据（仅{cnt}条），可能还未收盘")
        print(f"  提示: 请在收盘后（15:30后）运行")
        return

    print(f"  ✅ {review_date} 有 {cnt} 条K线数据")

    # 运行复盘
    from src.strategy.review import run_review, format_review_wechat

    review = run_review(review_date, db_path="data/alpha_miner.db")
    if review is None:
        print("  ❌ 无推荐记录可复盘")
        return

    # 打印结果
    print()
    print(review.to_text())

    # 保存
    import json
    Path("recommendations").mkdir(exist_ok=True)
    json_file = Path(f"recommendations/{review_date}_review.json")
    json_file.write_text(
        json.dumps(review.to_dict(), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"\n  复盘JSON: {json_file}")

    # 更新累计统计
    _update_cumulative_stats(review)

    # 推送
    if not args.dry_run:
        msg = format_review_wechat(review)
        push_file = Path(f"recommendations/{review_date}_review_push.txt")
        push_file.write_text(msg, encoding="utf-8")
        print(f"\n{'─'*60}")
        print(msg)
        print(f"{'─'*60}")

    print(f"\n✅ 盘后复盘完成 — {now.strftime('%Y-%m-%d %H:%M:%S')}")


def _update_cumulative_stats(review) -> None:
    """更新累计统计到 reviews_stats.json。"""
    import json
    from pathlib import Path

    stats_file = Path("recommendations/review_stats.json")
    if stats_file.exists():
        with open(stats_file, "r", encoding="utf-8") as f:
            stats = json.load(f)
    else:
        stats = {
            "total_days": 0,
            "total_picks": 0,
            "total_hit_buy": 0,
            "total_hit_target": 0,
            "total_hit_stop": 0,
            "all_profits": [],
            "daily_log": [],
        }

    stats["total_days"] += 1
    stats["total_picks"] += review.total
    stats["total_hit_buy"] += review.hit_buy_count
    stats["total_hit_target"] += review.hit_target_count
    stats["total_hit_stop"] += review.hit_stop_count

    for s in review.stocks:
        if s.hit_buy_zone:
            stats["all_profits"].append(s.profit_pct)

    stats["daily_log"].append({
        "date": review.review_date,
        "rec_date": review.rec_date,
        "total": review.total,
        "hit_buy": review.hit_buy_count,
        "hit_target": review.hit_target_count,
        "hit_stop": review.hit_stop_count,
        "avg_profit": review.avg_profit_pct,
        "win_rate": review.win_rate,
    })

    # 只保留最近30天
    stats["daily_log"] = stats["daily_log"][-30:]

    with open(stats_file, "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)

    # 打印累计
    total_picks = stats["total_picks"]
    if total_picks > 0:
        buy_rate = stats["total_hit_buy"] / total_picks * 100
        target_rate = stats["total_hit_target"] / total_picks * 100
        stop_rate = stats["total_hit_stop"] / total_picks * 100
        profits = stats["all_profits"]
        avg_p = sum(profits) / len(profits) if profits else 0
        wins = sum(1 for p in profits if p > 0)
        wr = wins / len(profits) * 100 if profits else 0

        print(f"\n  📊 累计统计（{stats['total_days']}天）:")
        print(f"     总推荐: {total_picks}只")
        print(f"     买点命中率: {buy_rate:.1f}%")
        print(f"     目标命中率: {target_rate:.1f}%")
        print(f"     止损触发率: {stop_rate:.1f}%")
        print(f"     平均盈亏: {avg_p:+.2f}%")
        print(f"     总胜率: {wr:.1f}%")

    print(f"  统计文件: {stats_file}")


if __name__ == "__main__":
    main()
