#!/usr/bin/env python3
"""深度复盘脚本 — 对比昨日推荐 vs 今日实际走势 + LLM深度分析。

流程：
1. 对比昨日推荐5只 vs 今日实际走势（数据对比）
2. 对比精选2只的操作建议 vs 实际走势
3. LLM深度推理：
   a) 每只推荐股的操作逻辑是否合理
   b) 集合竞价/盘中实际走势与预判的差异
   c) 推荐系统的选股逻辑是否需要优化
4. 生成复盘报告 + 优化建议
5. 推送到微信

用法:
  uv run python scripts/deep_review.py --date 2026-04-30
  uv run python scripts/deep_review.py  # 自动用今天
"""

import argparse
import json
import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


def gather_review_data(review_date: str, rec_date: str, rec_stocks: list, db_path: str) -> str:
    """收集复盘所需的全部数据。"""
    conn = sqlite3.connect(db_path)
    sections = []

    for s in rec_stocks:
        code = s["stock_code"]
        name = s.get("stock_name", code)

        lines = [f"\n--- {code} {name} [{s.get('signal_level','')}] ---"]

        # 推荐参数
        lines.append(f"推荐: 买入{s.get('buy_price',0):.2f}({s.get('buy_zone_low',0):.2f}~{s.get('buy_zone_high',0):.2f})"
                     f" 目标{s.get('target_price',0):.2f} 止损{s.get('stop_loss',0):.2f}")
        lines.append(f"推荐理由: {' | '.join(s.get('reasons',[])[:3])}")
        lines.append(f"风险提示: {' | '.join(s.get('risks',[])[:2])}")

        # 昨日K线（推荐日）
        row = conn.execute(
            "SELECT open, close, high, low, volume FROM daily_price "
            "WHERE trade_date=? AND stock_code=?", (rec_date, code)
        ).fetchone()
        if row:
            lines.append(f"推荐日({rec_date}): 开{row[0]:.2f} 收{row[1]:.2f} 高{row[2]:.2f} 低{row[3]:.2f} 量{row[4]:.0f}")

        # 今日K线（复盘日）
        row = conn.execute(
            "SELECT open, close, high, low, volume FROM daily_price "
            "WHERE trade_date=? AND stock_code=?", (review_date, code)
        ).fetchone()
        if row:
            chg = ((row[1] - s.get("buy_price", 0)) / s.get("buy_price", 1) * 100) if s.get("buy_price", 0) > 0 else 0
            lines.append(f"今日({review_date}): 开{row[0]:.2f} 收{row[1]:.2f} 高{row[2]:.2f} 低{row[3]:.2f} 量{row[4]:.0f}")
            # 与买入价对比
            buy = s.get("buy_price", 0)
            if buy > 0:
                lines.append(f"vs买入价{buy:.2f}: 开{((row[0]/buy-1)*100):+.1f}% 收{((row[1]/buy-1)*100):+.1f}% 高{((row[2]/buy-1)*100):+.1f}% 低{((row[3]/buy-1)*100):+.1f}%")

            # 是否触及买入区间
            bz_low = s.get("buy_zone_low", 0)
            bz_high = s.get("buy_zone_high", 0)
            target = s.get("target_price", 0)
            stop = s.get("stop_loss", 0)
            hit_buy = row[3] <= bz_high and row[2] >= bz_low if bz_low > 0 else False
            hit_target = row[2] >= target if target > 0 else False
            hit_stop = row[3] <= stop if stop > 0 else False
            lines.append(f"命中判定: 买点{'✅' if hit_buy else '❌'} 目标{'✅' if hit_target else '❌'} 止损{'✅' if hit_stop else '❌'}")
        else:
            lines.append(f"今日({review_date}): 无数据")

        # 近5天走势
        rows = conn.execute(
            "SELECT trade_date, open, close, high, low, volume FROM daily_price "
            "WHERE stock_code=? AND trade_date<=? ORDER BY trade_date DESC LIMIT 5",
            (code, review_date),
        ).fetchall()
        if rows:
            lines.append("近5天:")
            for r in rows:
                chg = ((r[2]-r[1])/r[1]*100) if r[1]>0 else 0
                lines.append(f"  {r[0]} 开{r[1]:.2f} 收{r[2]:.2f} 高{r[3]:.2f} 低{r[4]:.2f} 量{r[5]:.0f} {chg:+.1f}%")

        sections.append("\n".join(lines))

    conn.close()
    return "\n".join(sections)


def gather_deep_pick_data(review_date: str, rec_date: str, deep_pick_file: Path, db_path: str) -> str:
    """收集精选2只的操作建议对比数据。"""
    if not deep_pick_file.exists():
        return "（无精选操作建议记录）"

    deep_text = deep_pick_file.read_text(encoding="utf-8")
    # 提取LLM给出的具体价格
    # 直接返回原文 + 今日实际走势
    conn = sqlite3.connect(db_path)

    lines = ["\n=== 精选2只操作建议原文 ==="]
    lines.append(deep_text[:2000])  # 限制长度

    lines.append("\n=== 今日实际走势 ===")
    # 从精选文件中提取代码（简单匹配6位数字）
    import re
    codes = re.findall(r'\b(\d{6})\b', deep_text)
    seen = set()
    for code in codes:
        if code in seen or code.startswith('20') and int(code) < 200000:
            continue
        row = conn.execute(
            "SELECT open, close, high, low, volume FROM daily_price "
            "WHERE trade_date=? AND stock_code=?", (review_date, code)
        ).fetchone()
        if row:
            lines.append(f"{code}: 开{row[0]:.2f} 收{row[1]:.2f} 高{row[2]:.2f} 低{row[3]:.2f} 量{row[4]:.0f}")
            seen.add(code)

    conn.close()
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="深度复盘")
    parser.add_argument("--date", type=str, default=None, help="复盘日期 YYYY-MM-DD")
    parser.add_argument("--dry-run", action="store_true", help="只生成不推送")
    args = parser.parse_args()

    now = datetime.now()
    review_date = args.date or now.strftime("%Y-%m-%d")

    print(f"{'='*60}")
    print(f"  Alpha Miner 深度复盘")
    print(f"  复盘日期: {review_date}")
    print(f"{'='*60}")

    # 找昨日推荐文件
    rec_path = Path("recommendations")
    review_dt = datetime.strptime(review_date, "%Y-%m-%d")
    rec_json = None
    rec_date = None
    for delta in range(1, 5):
        candidate = review_dt - timedelta(days=delta)
        candidate_str = candidate.strftime("%Y-%m-%d")
        f = rec_path / f"{candidate_str}_recommend.json"
        if f.exists():
            rec_json = f
            rec_date = candidate_str
            break

    if rec_json is None:
        print(f"❌ 找不到 {review_date} 之前的推荐文件")
        return

    print(f"推荐文件: {rec_json}")

    with open(rec_json, "r", encoding="utf-8") as f:
        rec_data = json.load(f)

    rec_stocks = rec_data.get("stocks", [])
    if not rec_stocks:
        print("❌ 推荐列表为空")
        return

    # 先用基础复盘模块生成数据对比
    from src.strategy.review import run_review, format_review_wechat
    review = run_review(review_date, db_path="data/alpha_miner.db")
    if review is None:
        print("❌ 无数据可复盘")
        return

    basic_text = review.to_text()
    print(basic_text)

    # 收集详细数据
    print("\n收集复盘数据...")
    stocks_data = gather_review_data(review_date, rec_date, rec_stocks, "data/alpha_miner.db")

    deep_pick_file = rec_path / f"{rec_date}_deep_pick.txt"
    deep_data = gather_deep_pick_data(review_date, rec_date, deep_pick_file, "data/alpha_miner.db")

    # LLM 深度分析
    print("LLM深度分析中...")

    from src.strategy.llm_analysis import _default_llm_call

    prompt = f"""你是一位资深A股量化策略师，正在复盘昨日推荐策略的表现。请基于以下数据做深度分析。

=== 复盘日期: {review_date} ===
=== 推荐基于: {rec_date} 收盘数据 ===

=== 基础复盘结果 ===
总推荐: {review.total}只 | 触及买点: {review.hit_buy_count}/{review.total} | 命中目标: {review.hit_target_count}/{review.total} | 触发止损: {review.hit_stop_count}/{review.total}
平均盈亏: {review.avg_profit_pct:+.2f}% | 胜率: {review.win_rate:.0f}%

=== 各股详细对比 ===
{stocks_data}

{deep_data}

=== 请从以下角度分析（控制在30行以内）===

1. 【逐股点评】每只推荐股的操作逻辑是否正确（2-3行/只）
   - 推荐理由是否站得住脚
   - 买入价位是否合理（对比今日实际走势）
   - 如果重新推荐，会选哪些？

2. 【操作建议复盘】（如果有精选2只的操作建议）
   - 集合竞价预判 vs 实际开盘是否吻合
   - 盘中买入价位是否有机会执行
   - 止盈止损是否触发

3. 【系统优化建议】（3-5条具体可执行的建议）
   - 选股逻辑哪里需要调整
   - 买入价计算公式哪里需要修正
   - 过滤条件是否需要放宽/收紧
   - 因子权重是否需要调整"""

    result = _default_llm_call(prompt)
    if not result:
        print("❌ LLM分析失败")
        result = "LLM分析失败，仅输出基础复盘"

    # 组合推送消息
    msg_parts = []
    msg_parts.append(f"📊 Alpha Miner 深度复盘")
    msg_parts.append(f"📅 {review_date} | 回顾 {rec_date} 推荐")
    msg_parts.append("")
    msg_parts.append(f"📈 汇总: {review.total}只推荐")
    msg_parts.append(f"  触及买点: {review.hit_buy_count}/{review.total}")
    msg_parts.append(f"  命中目标: {review.hit_target_count}/{review.total}")
    msg_parts.append(f"  触发止损: {review.hit_stop_count}/{review.total}")
    if review.hit_buy_count > 0:
        msg_parts.append(f"  平均盈亏: {review.avg_profit_pct:+.2f}%")
        msg_parts.append(f"  胜率: {review.win_rate:.0f}%")
    msg_parts.append("")

    for i, s in enumerate(review.stocks, 1):
        if s.hit_target:
            status = "🎯达标"
        elif s.hit_stop_loss:
            status = "🛑止损"
        elif s.profit_pct > 0:
            status = f"📈+{s.profit_pct:.1f}%"
        else:
            status = f"📉{s.profit_pct:.1f}%"
        buy_icon = "✅" if s.hit_buy_zone else "❌"
        msg_parts.append(
            f"{i}. {status} {s.stock_code} {s.stock_name}\n"
            f"   收{s.today_close:.2f}({s.today_change_pct:+.1f}%) "
            f"买点{buy_icon} 盈亏{s.profit_pct:+.1f}%"
        )

    msg_parts.append("")
    msg_parts.append("🔍 LLM深度分析:")
    msg_parts.append(result)
    msg_parts.append("")
    msg_parts.append("⚠ 仅供参考")

    msg = "\n".join(msg_parts)

    # 保存
    Path("recommendations").mkdir(exist_ok=True)
    review_file = rec_path / f"{review_date}_deep_review.txt"
    review_file.write_text(msg, encoding="utf-8")

    json_file = rec_path / f"{review_date}_review.json"
    json_file.write_text(json.dumps(review.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8")

    # 保存优化建议到反馈文件
    _save_feedback(review_date, result)

    # 更新累计统计
    _update_cumulative_stats(review)

    print(f"\n{'─'*60}")
    print(msg)
    print(f"{'─'*60}")

    print(f"\n✅ 深度复盘完成")
    print(f"  复盘文件: {review_file}")
    print(f"  数据文件: {json_file}")


def _save_feedback(review_date: str, llm_result: str) -> None:
    """将LLM优化建议保存到反馈文件，供推荐引擎参考。"""
    feedback_dir = Path("recommendations/feedback")
    feedback_dir.mkdir(parents=True, exist_ok=True)

    # 追加写入
    feedback_file = feedback_dir / "optimization_log.txt"
    with open(feedback_file, "a", encoding="utf-8") as f:
        f.write(f"\n{'='*50}\n")
        f.write(f"复盘日期: {review_date}\n")
        f.write(f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"{'='*50}\n")
        f.write(llm_result)
        f.write("\n")

    # 只保留最近10次
    content = feedback_file.read_text(encoding="utf-8")
    blocks = content.split("=" * 50 + "\n")
    if len(blocks) > 22:  # 每次复盘占2个分隔块
        kept = blocks[-22:]
        feedback_file.write_text(("=" * 50 + "\n").join(kept), encoding="utf-8")

    print(f"  反馈日志: {feedback_file}")


def _update_cumulative_stats(review) -> None:
    """更新累计统计。"""
    stats_file = Path("recommendations/review_stats.json")
    if stats_file.exists():
        stats = json.loads(stats_file.read_text(encoding="utf-8"))
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

    stats["daily_log"] = stats["daily_log"][-30:]
    stats_file.write_text(json.dumps(stats, ensure_ascii=False, indent=2), encoding="utf-8")

    # 打印累计
    total_picks = stats["total_picks"]
    if total_picks > 0:
        profits = stats["all_profits"]
        avg_p = sum(profits) / len(profits) if profits else 0
        wins = sum(1 for p in profits if p > 0)
        wr = wins / len(profits) * 100 if profits else 0
        print(f"\n  📊 累计统计({stats['total_days']}天):")
        print(f"     推荐{total_picks}只 | 买点命中{stats['total_hit_buy']/total_picks*100:.1f}%")
        print(f"     目标命中{stats['total_hit_target']/total_picks*100:.1f}% | 止损触发{stats['total_hit_stop']/total_picks*100:.1f}%")
        print(f"     平均盈亏{avg_p:+.2f}% | 胜率{wr:.1f}%")


if __name__ == "__main__":
    main()
