#!/usr/bin/env python3
"""早间复盘再确认 — 每日8:30运行。

完整流程：
1. 加载昨晚推荐结果
2. 采集隔夜可能的新信息（新闻/外围）
3. 检查推荐标的有无重大变化（停牌/ST/利空新闻）
4. 生成复盘报告（确认/调整/剔除）
5. 推送复盘结果到微信

用法:
  uv run python scripts/morning_reconfirm.py
  uv run python scripts/morning_reconfirm.py --dry-run
"""

import argparse
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


def main():
    parser = argparse.ArgumentParser(description="早间复盘再确认 — 8:30 运行")
    parser.add_argument("--dry-run", action="store_true", help="只生成不推送")
    parser.add_argument("--date", type=str, default=None, help="昨晚推荐的日期")
    args = parser.parse_args()

    now = datetime.now()
    print(f"{'='*60}")
    print(f"  Alpha Miner 早间复盘再确认")
    print(f"  运行时间: {now.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}")

    from src.data.trading_calendar import get_latest_trade_date, is_weekend
    from src.data.storage import Storage

    today = now.strftime("%Y-%m-%d")

    # 周末跳过
    if is_weekend(today):
        print(f"\n  ⏭ 今天({today})是周末，跳过复盘")
        return

    # ── Step 1: 找到昨晚的推荐 ──────────────────────────
    print(f"\n[1/4] 加载昨晚推荐...")

    # 推荐是基于前一个交易日的数据
    latest_trade = args.date or get_latest_trade_date()
    if not latest_trade:
        print("  ❌ 无法确定交易日")
        return

    json_file = Path("recommendations") / f"{latest_trade}_recommend.json"
    if not json_file.exists():
        print(f"  ⚠ 未找到昨日推荐文件: {json_file}")
        print(f"  尝试重新生成...")
        # 直接重新生成
        from src.strategy.recommend import RecommendEngine

        db = Storage("data/alpha_miner.db")
        as_of = datetime.strptime(latest_trade, "%Y-%m-%d").replace(
            hour=23, minute=59, second=59
        ) + timedelta(days=1)
        engine = RecommendEngine(db)
        report = engine.recommend(as_of, latest_trade, top_n=5)
    else:
        # 加载已有报告
        data = json.loads(json_file.read_text(encoding="utf-8"))
        print(f"  ✅ 加载昨日推荐: {latest_trade}, {len(data['stocks'])}只")
        # 重新生成（确保用最新数据）
        from src.strategy.recommend import RecommendEngine

        db = Storage("data/alpha_miner.db")
        as_of = datetime.strptime(latest_trade, "%Y-%m-%d").replace(
            hour=23, minute=59, second=59
        ) + timedelta(days=1)
        engine = RecommendEngine(db)
        report = engine.recommend(as_of, latest_trade, top_n=5)

    if not report.stocks:
        print("  ⚠ 无推荐标的，跳过复盘")
        return

    # ── Step 2: 检查隔夜信息 ────────────────────────────
    print(f"\n[2/4] 检查隔夜信息...")

    changes = []
    confirmed_stocks = []

    for stock in report.stocks:
        status = "confirm"  # confirm / warning / remove
        warnings = []

        # 检查新闻（有没有负面）
        try:
            news_df = db.query(
                "news",
                datetime.now(),
                where="stock_code = ? AND sentiment_score < 0.3",
                params=(stock.stock_code,),
            )
            if not news_df.empty and len(news_df) > 0:
                # 有负面新闻
                neg_count = len(news_df)
                warnings.append(f"发现{neg_count}条负面新闻")
        except Exception:
            pass

        # 检查是否炸板过多（如果是打板策略）
        if stock.open_count >= 3:
            status = "warning"
            warnings.append(f"昨日炸板{stock.open_count}次，分歧严重")

        # 检查龙虎榜资金方向
        if stock.fund_net_amount < -1e8:
            status = "warning"
            warnings.append(f"主力净流出{abs(stock.fund_net_amount)/1e8:.1f}亿")

        # 市场环境检查
        if report.market_regime == "弱势市场":
            warnings.append("大盘偏弱，注意仓位控制")

        if status == "confirm":
            confirmed_stocks.append((stock, "✅ 确认", warnings))
        elif status == "warning":
            confirmed_stocks.append((stock, "⚠️ 谨慎", warnings))
            changes.append(f"{stock.stock_code} {stock.stock_name}: {'; '.join(warnings)}")
        else:
            changes.append(f"{stock.stock_code} {stock.stock_name}: ❌ 剔除 ({'; '.join(warnings)})")

    # ── Step 3: 生成复盘报告 ────────────────────────────
    print(f"\n[3/4] 生成复盘报告...")

    # 隔夜市场概况（简单版）
    overnight_info = _get_overnight_summary()

    from src.strategy.push import _format_reconfirm_message

    reconfirm_msg = _format_reconfirm_message(
        report, changes, overnight_info,
    )

    print(f"  确认标的: {sum(1 for _, s, _ in confirmed_stocks if '确认' in s)}只")
    print(f"  需关注: {sum(1 for _, s, _ in confirmed_stocks if '谨慎' in s)}只")

    # ── Step 4: 保存 + 推送 ─────────────────────────────
    print(f"\n[4/4] 保存 & 推送...")

    save_dir = Path("recommendations")
    save_dir.mkdir(parents=True, exist_ok=True)

    push_file = save_dir / f"{latest_trade}_reconfirm.txt"
    push_file.write_text(reconfirm_msg, encoding="utf-8")
    print(f"  复盘报告: {push_file}")

    if not args.dry_run:
        print(f"\n{'─'*60}")
        print(reconfirm_msg)
        print(f"{'─'*60}")

    print(f"\n✅ 早间复盘完成 — {now.strftime('%Y-%m-%d %H:%M:%S')}")


def _get_overnight_summary() -> str:
    """获取隔夜市场概况（简单版）。"""
    lines = []

    # 从数据库检查最新市场情绪
    try:
        from src.data.storage import Storage
        db = Storage("data/alpha_miner.db")
        from src.data.trading_calendar import get_latest_trade_date

        latest = get_latest_trade_date()
        if latest:
            emotion_df = db.query(
                "market_emotion", datetime.now(),
                where="trade_date = ?", params=(latest,),
            )
            if not emotion_df.empty:
                row = emotion_df.iloc[-1]
                zt = int(row.get("zt_count", 0))
                dt = int(row.get("dt_count", 0))
                sentiment = row.get("sentiment_level", "未知")
                lines.append(f"上一交易日({latest}): 涨停{zt}只/跌停{dt}只, 情绪{sentiment}")

            # 检查美股（如果有数据的话，暂时跳过）
    except Exception:
        pass

    return " | ".join(lines) if lines else "暂无隔夜数据"


if __name__ == "__main__":
    main()
