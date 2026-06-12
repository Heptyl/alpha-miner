#!/usr/bin/env python3
"""盘中买点分析 — 基于 tradeplan 文件"""
import json
import sys
import sqlite3
from datetime import datetime
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


def get_realtime_quote(code: str) -> dict | None:
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
            "change_pct": float(data.get("涨幅", 0)),
            "buy_1": float(data.get("buy_1", 0)),
            "buy_1_vol": float(data.get("buy_1_vol", 0)),
            "sell_1": float(data.get("sell_1", 0)),
            "sell_1_vol": float(data.get("sell_1_vol", 0)),
        }
    except Exception as e:
        print(f"  ⚠ {code} 行情获取失败: {e}")
        return None


def classify_open(pct: float) -> str:
    if pct <= -3: return "大幅低开"
    elif pct <= -1: return "低开"
    elif pct < 1: return "平开"
    elif pct < 3: return "高开"
    elif pct < 5: return "大幅高开"
    else: return "涨停附近"


def vol_label(vr: float) -> str:
    if vr > 2: return "放量"
    elif vr > 1.5: return "温和放量"
    elif vr < 0.7: return "缩量"
    else: return "正常"


def analyze_stock(s: dict, quote: dict) -> dict:
    code = s["code"]
    name = s["name"]
    prev_close = s["close"]  # T-1 收盘价
    target = s["target_price"]
    stop = s["stop_price"]
    entry_target = s["entry_target"]
    entry_max = s["entry_max"]
    entry_min = s["entry_min"]

    open_pct = ((quote["open"] - prev_close) / prev_close * 100) if prev_close > 0 else 0
    cur_pct = ((quote["price"] - prev_close) / prev_close * 100) if prev_close > 0 else 0
    ot = classify_open(open_pct)
    vr = quote.get("volume_ratio", 1.0)

    # 操作建议
    action = ""
    buy_price = 0.0
    reasons = []

    if open_pct <= -1:
        if cur_pct <= 0:
            action = "✅ 可买入"
            buy_price = round(min(quote["buy_1"], entry_target), 2)
            reasons.append(f"低开{open_pct:+.1f}%后仍在低位，安全边际充足")
        else:
            action = "👀 观察"
            reasons.append(f"低开后反弹至{cur_pct:+.1f}%，等回踩")
            buy_price = entry_target
    elif open_pct < 1:
        if cur_pct <= 1:
            action = "✅ 可买入"
            buy_price = round(min(quote["buy_1"], entry_max), 2)
            reasons.append("平开，在收盘价附近可入")
        else:
            action = "👀 观望"
            reasons.append(f"已涨至{cur_pct:+.1f}%，等回调")
            buy_price = entry_target
    elif open_pct < 3:
        if cur_pct <= 0:
            action = "✅ 可买入"
            buy_price = round(min(quote["buy_1"], entry_max), 2)
            reasons.append("高开后回落到收盘价附近")
        elif cur_pct <= 1.5:
            action = "⚡ 轻仓"
            buy_price = entry_max
            reasons.append("小幅上涨，可轻仓试探")
        else:
            action = "⏭ 放弃"
            reasons.append(f"高开后继续走高{cur_pct:+.1f}%，追高风险大")
    elif open_pct < 5:
        if cur_pct <= 1:
            action = "⚡ 轻仓"
            buy_price = entry_target
            reasons.append("高开后大幅回落，接近目标价")
        else:
            action = "⏭ 放弃"
            reasons.append(f"高开{open_pct:+.1f}%维持高位，风险大")
    else:
        action = "⏭ 放弃"
        reasons.append("涨停附近开盘，坚决不追")

    # 距离入场目标价
    dist_to_entry = ((quote["price"] - entry_target) / entry_target * 100) if entry_target > 0 else 0

    return {
        "code": code, "name": name,
        "prev_close": prev_close,
        "open": quote["open"], "open_pct": open_pct, "open_type": ot,
        "current_price": quote["price"], "current_pct": cur_pct,
        "high": quote["high"], "low": quote["low"],
        "volume_ratio": vr, "vol_label": vol_label(vr),
        "action": action, "buy_price": buy_price,
        "entry_target": entry_target, "entry_max": entry_max, "entry_min": entry_min,
        "target": target, "stop": stop,
        "dist_to_entry": dist_to_entry,
        "reasons": reasons,
    }


def main():
    now = datetime.now()
    print(f"{'='*60}")
    print(f"  Alpha Miner 盘中买点分析 (TradePlan)")
    print(f"  运行时间: {now.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}")

    tp_file = Path("recommendations/2026-05-12_tradeplan.json")
    if not tp_file.exists():
        print("❌ 未找到 tradeplan 文件")
        sys.exit(1)

    tp = json.loads(tp_file.read_text(encoding="utf-8"))
    all_stocks = tp.get("top", []) + tp.get("backup", [])
    strategy = tp.get("strategy", {})

    print(f"\n📋 交易计划: {tp['report_date']} → {tp['target_date']}")
    print(f"   策略: 止损{strategy.get('stop_loss',3)}% | 止盈{strategy.get('take_profit',5)}% | 仓位{strategy.get('position_pct',35)}%")
    print(f"   买入窗口: {strategy.get('buy_window','09:40-10:30')}")
    print(f"   关注个股: {len(all_stocks)}只\n")

    signals = []
    for s in all_stocks:
        code = s["code"]
        name = s["name"]
        print(f"  获取 {code} {name} 行情...")
        quote = get_realtime_quote(code)
        if not quote or quote["price"] == 0:
            print(f"    ⚠ 行情不可用")
            continue
        sig = analyze_stock(s, quote)
        signals.append(sig)
        print(f"    {sig['open_type']} {sig['open_pct']:+.1f}% → 现{sig['current_pct']:+.1f}% → {sig['action']}")

    if not signals:
        print("\n❌ 无可用信号")
        sys.exit(1)

    # 排序
    action_order = {"✅ 可买入": 0, "⚡ 轻仓": 1, "👀 观察": 2, "👀 观望": 3, "⏭ 放弃": 4}
    signals.sort(key=lambda x: action_order.get(x["action"], 9))

    # 格式化消息
    lines = []
    lines.append("📊 Alpha Miner 盘中买点分析")
    lines.append(f"⏰ {now.strftime('%m月%d日 %H:%M')}")
    lines.append(f"📋 基于 {tp['report_date']} 交易计划")
    lines.append("━" * 30)

    can_buy = [s for s in signals if "可买入" in s["action"] or "轻仓" in s["action"]]
    watch = [s for s in signals if "观察" in s["action"] or "观望" in s["action"]]
    skip = [s for s in signals if "放弃" in s["action"]]

    if can_buy:
        lines.append("")
        lines.append(f"✅ 可操作 ({len(can_buy)}只)")
        for s in can_buy:
            lines.append(f"  {s['code']} {s['name']} ({s['name']})")
            lines.append(f"    开盘{s['open_type']} {s['open_pct']:+.1f}% | 现价{s['current_price']:.2f} ({s['current_pct']:+.1f}%) | {s['vol_label']}")
            lines.append(f"    👉 买入价: {s['buy_price']:.2f} | 目标: {s['target']:.2f} (+{(s['target']/s['prev_close']-1)*100:.1f}%) | 止损: {s['stop']:.2f}")
            lines.append(f"    📐 入场区间: {s['entry_min']:.2f} ~ {s['entry_max']:.2f} (目标{s['entry_target']:.2f})")
            for r in s["reasons"]:
                lines.append(f"    💡 {r}")

    if watch:
        lines.append("")
        lines.append(f"👀 观察 ({len(watch)}只)")
        for s in watch:
            lines.append(f"  {s['code']} {s['name']}")
            lines.append(f"    开盘{s['open_type']} {s['open_pct']:+.1f}% | 现{s['current_pct']:+.1f}% | 目标价{s['entry_target']:.2f}")
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
    lines.append("━" * 30)
    lines.append(f"💡 买入窗口: {strategy.get('buy_window','09:40-10:30')}")
    lines.append("⚠ T+1规则: 买入后次日才能卖出，控制仓位")
    lines.append("⚠ 以上仅供参考，不构成投资建议")

    msg = "\n".join(lines)

    # 保存
    out_file = Path(f"recommendations/2026-05-12_intraday.txt")
    out_file.write_text(msg, encoding="utf-8")

    print(f"\n{'═'*50}")
    print(msg)
    print(f"{'═'*50}")
    print(f"\n✅ 分析已保存: {out_file}")

    # 输出纯文本供后续发送
    print("\n---WEIXIN_MSG_START---")
    print(msg)
    print("---WEIXIN_MSG_END---")


if __name__ == "__main__":
    main()
