#!/usr/bin/env python3
"""盘中买点分析 — 基于实时行情给出具体买入价位和操作信号。

用法:
  uv run python scripts/intraday_analysis.py                  # 用最新推荐
  uv run python scripts/intraday_analysis.py --date 2026-04-30  # 指定推荐日期

推送时间线:
  09:25  集合竞价结束后 → 判断高开/低开/竞价量
  10:00  开盘30分钟后  → 确认走势，给出明确买点
  10:30  半小时确认期  → 最终操作建议

买点逻辑:
  1. 基准价 = 前日收盘价（不溢价）
  2. 低开 -1%~+1%: 最佳买点，接近收盘价买入
  3. 高开 +1%~+3%: 观察，回踩到收盘价附近再入
  4. 高开 >+3%: 放弃或等回调（追高风险大）
  5. 低开 <-3%: 谨慎，可能趋势反转

核心原则: 次日才能卖（T+1），所以买入价必须合理，
         不能追高，要有足够安全边际。
"""

import argparse
import json
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


def _get_realtime_quote(code: str) -> dict | None:
    """获取单只股票实时行情。"""
    try:
        import akshare as ak
        df = ak.stock_bid_ask_em(symbol=code)
        data = dict(zip(df["item"], df["value"]))
        return {
            "code": code,
            "price": float(data.get("最新", 0)),
            "open": float(data.get("今开", 0)),
            "prev_close": float(data.get("昨收", 0)),
            "high": float(data.get("最高", 0)),
            "low": float(data.get("最低", 0)),
            "volume": float(data.get("总手", 0)),
            "amount": float(data.get("金额", 0)),
            "turnover": float(data.get("换手", 0)),
            "volume_ratio": float(data.get("量比", 0)),
            "limit_up": float(data.get("涨停", 0)),
            "limit_down": float(data.get("跌停", 0)),
            "change_pct": float(data.get("涨幅", 0)),
            "buy_1": float(data.get("buy_1", 0)),
            "buy_1_vol": float(data.get("buy_1_vol", 0)),
            "sell_1": float(data.get("sell_1", 0)),
            "sell_1_vol": float(data.get("sell_1_vol", 0)),
        }
    except Exception as e:
        print(f"  ⚠ {code} 行情获取失败: {e}")
        return None


def _get_recommend_stocks(date: str) -> list[dict]:
    """读取指定日期的推荐股票。"""
    json_file = Path(f"recommendations/{date}_recommend.json")
    if not json_file.exists():
        return []
    report = json.loads(json_file.read_text(encoding="utf-8"))
    return report.get("stocks", [])


def _get_prev_close(code: str, date: str) -> float | None:
    """从数据库获取前日收盘价。"""
    conn = sqlite3.connect("data/alpha_miner.db")
    row = conn.execute(
        "SELECT close FROM daily_price WHERE stock_code=? AND trade_date=?",
        (code, date),
    ).fetchone()
    conn.close()
    return row[0] if row else None


def _get_stock_name(code: str) -> str:
    """获取股票名称。"""
    conn = sqlite3.connect("data/alpha_miner.db")
    row = conn.execute(
        "SELECT name FROM zt_pool WHERE stock_code=? LIMIT 1", (code,)
    ).fetchone()
    if not row:
        row = conn.execute(
            "SELECT name FROM strong_pool WHERE stock_code=? LIMIT 1", (code,)
        ).fetchone()
    conn.close()
    return row[0] if row else code


def _classify_open(open_pct: float) -> str:
    """分类开盘类型。"""
    if open_pct <= -3:
        return "大幅低开"
    elif open_pct <= -1:
        return "低开"
    elif open_pct < 1:
        return "平开"
    elif open_pct < 3:
        return "高开"
    elif open_pct < 5:
        return "大幅高开"
    else:
        return "涨停附近"


def _generate_signal(
    code: str,
    name: str,
    prev_close: float,
    quote: dict,
    target: float,
    stop_loss: float,
) -> dict:
    """生成单只股票的盘中操作信号。"""
    open_pct = ((quote["open"] - prev_close) / prev_close * 100) if prev_close > 0 else 0
    current_pct = ((quote["price"] - prev_close) / prev_close * 100) if prev_close > 0 else 0
    open_type = _classify_open(open_pct)

    # 量比判断
    vol_signal = ""
    if quote["volume_ratio"] > 2:
        vol_signal = "放量"
    elif quote["volume_ratio"] > 1.5:
        vol_signal = "温和放量"
    elif quote["volume_ratio"] < 0.7:
        vol_signal = "缩量"
    else:
        vol_signal = "正常"

    # 操作建议
    action = ""
    buy_price = 0.0
    reason_lines = []

    if open_pct <= -1:
        # 低开: 机会
        if current_pct <= 0:
            action = "可买入"
            buy_price = round(min(quote["buy_1"], prev_close * 0.99), 2)
            reason_lines.append(f"低开{open_pct:+.1f}%后仍在低位,安全边际充足")
        else:
            action = "观察"
            reason_lines.append(f"低开后反弹至{current_pct:+.1f}%,等回踩")
            buy_price = round(prev_close * 0.98, 2)
    elif open_pct < 1:
        # 平开: 最佳
        if current_pct <= 1:
            action = "可买入"
            buy_price = round(min(quote["buy_1"], prev_close * 1.005), 2)
            reason_lines.append("平开,在收盘价附近买入,安全边际好")
        else:
            action = "观望"
            reason_lines.append(f"已涨至{current_pct:+.1f}%,等回调")
            buy_price = round(prev_close * 0.99, 2)
    elif open_pct < 3:
        # 高开1-3%: 谨慎
        if current_pct <= 0:
            action = "可买入"
            buy_price = round(prev_close * 1.005, 2)
            reason_lines.append("高开后回落到收盘价附近,可入")
        elif current_pct <= 1:
            action = "轻仓"
            buy_price = round(prev_close * 1.01, 2)
            reason_lines.append("小幅上涨,可轻仓试探")
        else:
            action = "放弃"
            reason_lines.append(f"高开后继续走高{current_pct:+.1f}%,追高风险大")
    elif open_pct < 5:
        # 高开3-5%: 风险大
        if current_pct <= 1:
            action = "可买入"
            buy_price = round(prev_close * 1.01, 2)
            reason_lines.append("高开后大幅回落,接近收盘价")
        else:
            action = "放弃"
            reason_lines.append(f"高开{open_pct:+.1f}%且维持高位,追高风险大")
    else:
        # 涨停附近: 坚决不追
        action = "放弃"
        reason_lines.append("涨停附近开盘,追高风险极大")

    # 买一卖一挂单分析
    bid_ask_info = ""
    if quote["buy_1"] > 0 and quote["sell_1"] > 0:
        spread = quote["sell_1"] - quote["buy_1"]
        bid_ask_info = f"买一{quote['buy_1']:.2f}({quote['buy_1_vol']/10000:.0f}万手) 卖一{quote['sell_1']:.2f}({quote['sell_1_vol']/10000:.0f}万手) 价差{spread:.2f}"

    return {
        "code": code,
        "name": name,
        "prev_close": prev_close,
        "open": quote["open"],
        "open_pct": open_pct,
        "open_type": open_type,
        "current_price": quote["price"],
        "current_pct": current_pct,
        "high": quote["high"],
        "low": quote["low"],
        "volume_ratio": quote["volume_ratio"],
        "vol_signal": vol_signal,
        "action": action,
        "buy_price": buy_price,
        "target": target,
        "stop_loss": stop_loss,
        "reasons": reason_lines,
        "bid_ask_info": bid_ask_info,
    }


def format_intraday_message(signals: list[dict], trade_date: str, now_str: str) -> str:
    """格式化盘中分析推送消息。"""
    lines = []
    lines.append(f"📊 Alpha Miner 盘中买点分析")
    lines.append(f"⏰ {now_str}")
    lines.append("📋 基于T-1推荐 (推荐日" + trade_date + ")")
    lines.append("━" * 28)

    # 先按操作建议排序: 可买入 > 轻仓 > 观察 > 观望 > 放弃
    action_order = {"可买入": 0, "轻仓": 1, "观察": 2, "观望": 3, "放弃": 4}
    signals.sort(key=lambda s: action_order.get(s["action"], 9))

    can_buy = [s for s in signals if s["action"] in ("可买入", "轻仓")]
    watch = [s for s in signals if s["action"] in ("观察", "观望")]
    skip = [s for s in signals if s["action"] == "放弃"]

    if can_buy:
        lines.append("")
        lines.append(f"✅ 可操作 ({len(can_buy)}只)")
        for s in can_buy:
            lines.append(f"  {s['code']} {s['name']}")
            lines.append(f"    开盘{s['open_type']} {s['open_pct']:+.1f}% | 现{s['current_pct']:+.1f}% | {s['vol_signal']}")
            lines.append(f"    👉 买入: {s['buy_price']:.2f} | 目标: {s['target']:.2f} (+{(s['target']/s['prev_close']-1)*100:.1f}%) | 止损: {s['stop_loss']:.2f}")
            for r in s["reasons"]:
                lines.append(f"    💡 {r}")

    if watch:
        lines.append("")
        lines.append(f"👀 观察 ({len(watch)}只)")
        for s in watch:
            lines.append(f"  {s['code']} {s['name']}")
            lines.append(f"    开盘{s['open_type']} {s['open_pct']:+.1f}% | 现{s['current_pct']:+.1f}%")
            for r in s["reasons"]:
                lines.append(f"    💡 {r}")

    if skip:
        lines.append("")
        lines.append(f"⏭ 放弃 ({len(skip)}只)")
        for s in skip:
            lines.append(f"  {s['code']} {s['name']} {s['open_type']} {s['open_pct']:+.1f}%")
            for r in s["reasons"]:
                lines.append(f"    💡 {r}")

    lines.append("")
    lines.append("━" * 28)
    lines.append("⚠ T+1规则: 买入后次日才能卖出,控制仓位")
    lines.append("⚠ 以上仅供参考，不构成投资建议")

    msg = "\n".join(lines)
    # 修复模板变量
    msg = msg.replace("{T-1}", "前日")
    return msg


def main():
    parser = argparse.ArgumentParser(description="盘中买点分析")
    parser.add_argument("--date", type=str, default=None, help="推荐日期 YYYY-MM-DD")
    parser.add_argument("--dry-run", action="store_true", help="只分析不推送")
    args = parser.parse_args()

    now = datetime.now()
    print(f"{'=' * 60}")
    print(f"  Alpha Miner 盘中买点分析")
    print(f"  运行时间: {now.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'=' * 60}")

    # 确定推荐日期
    conn = sqlite3.connect("data/alpha_miner.db")
    if args.date:
        trade_date = args.date
    else:
        row = conn.execute(
            "SELECT MAX(trade_date) FROM daily_price"
        ).fetchone()
        trade_date = row[0] if row else None
    conn.close()

    if not trade_date:
        print("❌ 无可用数据")
        return

    print(f"\n推荐日期: {trade_date}")

    # 加载推荐
    stocks = _get_recommend_stocks(trade_date)
    if not stocks:
        print(f"❌ 未找到 {trade_date} 的推荐文件")
        return

    print(f"推荐个股: {[s['stock_code'] for s in stocks]}")

    # 获取实时行情并生成信号
    print("\n获取实时行情...")
    signals = []
    for s in stocks:
        code = s["stock_code"]
        name = _get_stock_name(code)
        prev_close = _get_prev_close(code, trade_date)
        target = s.get("target_price", 0)
        stop_loss = s.get("stop_loss", 0)

        if not prev_close:
            print(f"  ⚠ {code} 无前日收盘价")
            continue

        quote = _get_realtime_quote(code)
        if not quote:
            # 非交易时间无法获取实时数据，用模拟数据做回测
            print(f"  ⚠ {code} 实时行情不可用(非交易时间?)")
            continue

        signal = _generate_signal(code, name, prev_close, quote, target, stop_loss)
        signals.append(signal)
        print(f"  {code} {name}: {signal['open_type']} {signal['open_pct']:+.1f}% → {signal['action']}")

    if not signals:
        print("\n❌ 无可用信号（非交易时间或行情获取失败）")
        return

    # 格式化消息
    now_str = now.strftime("%m月%d日 %H:%M")
    msg = format_intraday_message(signals, trade_date, now_str)

    # 保存
    Path("recommendations").mkdir(exist_ok=True)
    out_file = Path(f"recommendations/{trade_date}_intraday.txt")
    out_file.write_text(msg, encoding="utf-8")
    print(f"\n✅ 分析已保存: {out_file}")

    # 输出
    print(f"\n{'═' * 50}")
    print(msg)
    print(f"{'═' * 50}")


if __name__ == "__main__":
    main()
