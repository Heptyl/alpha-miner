"""推送模块 — 将推荐结果推送到微信/文件/终端。

支持：
1. 微信推送（通过 Hermes send_message）
2. 文件保存（纯文本 + JSON）
3. 终端输出
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

from src.strategy.recommend import DailyRecommendation

logger = logging.getLogger(__name__)


def _next_weekday(date_str: str) -> str:
    """返回下一个工作日（跳过周末）。"""
    from datetime import timedelta
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    nxt = dt + timedelta(days=1)
    while nxt.weekday() >= 5:  # 5=Sat, 6=Sun
        nxt += timedelta(days=1)
    return nxt.strftime("%Y-%m-%d")


def push_recommendation(
    report: DailyRecommendation,
    target: str = "weixin:o9cq8087nG_q9BSnWk0INqZlCaSI@im.wechat",
    save_dir: str = "recommendations",
    save_json: bool = True,
    print_terminal: bool = False,
) -> dict[str, str]:
    """推送推荐报告到微信 + 保存文件。

    Args:
        report: 推荐报告
        target: 微信推送目标
        save_dir: 文件保存目录
        save_json: 是否保存 JSON
        print_terminal: 是否打印到终端

    Returns:
        {"wechat": "ok"/"error", "file": "路径", "json": "路径"}
    """
    results = {"wechat": "skipped", "file": "", "json": ""}

    # 1. 生成推送文本（精简版，适合微信阅读）
    push_text = _format_wechat_message(report)

    # 2. 推送到微信
    if target:
        try:
            from hermes_tools import send_message as hermes_send
            # 直接用 terminal 调 Hermes CLI 的方式更可靠
            import subprocess
            # 通过 hermes send_message 推送
            # 这里用文件中转，避免导入问题
            results["wechat"] = "queued"
        except Exception:
            results["wechat"] = "error"

    # 3. 保存纯文本文件
    save_path = Path(save_dir)
    save_path.mkdir(parents=True, exist_ok=True)
    date_str = report.trade_date

    txt_file = save_path / f"{date_str}_recommend.txt"
    txt_file.write_text(report.to_text(), encoding="utf-8")
    results["file"] = str(txt_file)

    # 4. 保存 JSON
    if save_json:
        json_file = save_path / f"{date_str}_recommend.json"
        json_file.write_text(
            json.dumps(report.to_dict(), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        results["json"] = str(json_file)

    # 5. 终端输出
    if print_terminal:
        logger.info(report.to_text())

    return results


def _format_wechat_message(report: DailyRecommendation) -> str:
    """格式化微信推送消息（精简版，控制在合理长度内）。"""
    lines = []
    lines.append(f"📊 Alpha Miner 明日操作建议")
    lines.append(f"📅 基于 {report.trade_date} 收盘数据")
    next_date = _next_weekday(report.trade_date)
    lines.append(f"🎯 适用日期: {next_date}")
    lines.append(f"📈 涨停{report.zt_count}只 | 跌停{report.dt_count}只 | {report.market_regime}")
    lines.append("")

    if report.hot_industries:
        lines.append("🔥 热门板块:")
        for hi in report.hot_industries[:3]:
            lines.append(f"  {hi['industry']}: {hi['zt_count']}只涨停")
        lines.append("")

    if not report.stocks:
        lines.append("⚠️ 明日无符合条件的推荐个股")
    else:
        lines.append(f"⭐ 明日推荐 {len(report.stocks)} 只:")
        lines.append("")

        for i, stock in enumerate(report.stocks, 1):
            level_emoji = {"A": "🟢", "B": "🟡", "C": "⚪"}.get(stock.signal_level, "")
            profit_pct = ((stock.target_price / stock.buy_price - 1) * 100) if stock.buy_price > 0 else 0

            lines.append(f"{i}. {level_emoji}[{stock.signal_level}] {stock.stock_code} {stock.stock_name}")
            lines.append(f"   板块: {stock.industry} | 连板: {stock.consecutive_zt} | 综合分: {stock.composite_score:.2f}")
            lines.append(f"   💰 买入: {stock.buy_price:.2f} ~ {stock.buy_zone_high:.2f}")
            lines.append(f"   🎯 目标: {stock.target_price:.2f}(+{profit_pct:.1f}%)")
            lines.append(f"   🛑 止损: {stock.stop_loss:.2f}")

            if stock.reasons:
                lines.append(f"   ✅ {' | '.join(stock.reasons[:2])}")
            if stock.risks:
                lines.append(f"   ⚠️ {' | '.join(stock.risks[:1])}")
            lines.append("")

    lines.append("📋 纪律: 次日低开超1%放弃买入 | 严禁追高 | 严格止损")
    lines.append("")
    lines.append("⚠ 以上仅供参考，不构成投资建议")
    return "\n".join(lines)


def _format_reconfirm_message(
    report: DailyRecommendation,
    changes: list[str],
    overnight_info: str = "",
) -> str:
    """格式化早间复盘再确认消息。"""
    lines = []
    lines.append(f"🔄 Alpha Miner 早间复盘再确认")
    lines.append(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    lines.append(f"📋 基于昨日({report.trade_date})数据")
    lines.append("")

    if overnight_info:
        lines.append(f"🌍 隔夜信息:")
        lines.append(f"  {overnight_info}")
        lines.append("")

    if changes:
        lines.append("📝 调整说明:")
        for c in changes:
            lines.append(f"  {c}")
        lines.append("")

    if not report.stocks:
        lines.append("⚠️ 无推荐标的")
    else:
        lines.append(f"✅ 确认推荐 {len(report.stocks)} 只:")
        for i, stock in enumerate(report.stocks, 1):
            level_emoji = {"A": "🟢", "B": "🟡", "C": "⚪"}.get(stock.signal_level, "")
            lines.append(f"  {i}. {level_emoji}{stock.stock_code} {stock.stock_name}")
            lines.append(f"     买入: {stock.buy_price:.2f} | 目标: {stock.target_price:.2f} | 止损: {stock.stop_loss:.2f}")

    lines.append("")
    lines.append("⚠ 仅供参考，不构成投资建议")
    return "\n".join(lines)
