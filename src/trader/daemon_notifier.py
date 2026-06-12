"""daemon_notifier.py — 交易通知推送

从 trading_daemon.py 拆分出的通知模块。
"""

from __future__ import annotations

import sys
import logging
import subprocess
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.trader.daemon_config import SIGNAL_NOTIFY_SCRIPT
from src.trader.daemon_db import _log_to_db

logger = logging.getLogger("trading_daemon")


def _send_batch_notifications(batch: list[tuple[dict, dict]]):
    """批量合并推送 — 多条信号合成一条消息, 避免iLink频率限制
    
    问题: 逐条推送时6条消息在84秒内发完, iLink限流导致用户只收到1条
    方案: 同一轮执行的所有信号合并成一条消息
    """
    if not batch:
        return
    
    try:
        import subprocess
        msg_parts = []
        
        buys = [(s, r) for s, r in batch if s["action"] == "buy"]
        sells = [(s, r) for s, r in batch if s["action"] == "sell"]
        
        if sells:
            msg_parts.append(f"🔴 卖出 {len(sells)}笔:")
            for sig, result in sells:
                trade = result.get("trade", {})
                price = result.get("price", sig.get("price", 0))
                buy_price = trade.get("buy_price", 0)
                hold_days = trade.get("hold_days", 0)
                pnl = trade.get("pnl", 0)
                pnl_pct = trade.get("pnl_pct", 0)
                line = f"  {sig['name']}({sig['code']}) ¥{price:.2f}"
                if buy_price > 0:
                    line += f" [买入¥{buy_price:.2f} 持{hold_days}天]"
                line += f" 盈亏¥{pnl:+.0f}({pnl_pct:+.1f}%)"
                msg_parts.append(line)
            msg_parts.append(f"  原因: {sells[0][0].get('reason', '')}")
        
        if buys:
            msg_parts.append(f"\n🟢 买入 {len(buys)}笔:")
            for sig, result in buys:
                price = result.get("price", sig.get("price", 0))
                msg_parts.append(f"  {sig['name']}({sig['code']}) ¥{price:.2f}")
                msg_parts.append(f"    {sig.get('signal_type','')} | {sig.get('reason','')}")
        
        msg = "\n".join(msg_parts)
        subprocess.run(
            [sys.executable, str(SIGNAL_NOTIFY_SCRIPT), msg],
            timeout=30, capture_output=True,
        )

        # Webhook推送(钉钉/飞书)
        _push_webhook_batch(buys, sells)
    except Exception as e:
        logger.error(f"[推送] 批量推送失败: {e}")




def _send_trade_notification(sig: dict, result: dict):
    """成交通知 — 调用通知脚本推送到微信
    
    推送内容包含:
    - 买入: 信号类型/原因/策略
    - 卖出: 买入价/持仓天数/盈亏金额+百分比/卖出原因
    """
    try:
        import subprocess
        trade = result.get("trade", {})
        action_cn = "买入" if sig["action"] == "buy" else "卖出"
        price = result.get("price", sig.get("price", 0))

        msg_parts = [
            f"{'🟢' if sig['action']=='buy' else '🔴'} 模拟盘{action_cn}",
            f"{sig['name']}({sig['code']})",
            f"价格: ¥{price:.2f}",
            f"信号: {sig['signal_type']}",
            f"原因: {sig['reason']}",
        ]

        # 卖出时补充完整持仓信息
        if sig["action"] == "sell":
            buy_price = trade.get("buy_price", 0)
            hold_days = trade.get("hold_days", 0)
            pnl = trade.get("pnl", 0)
            pnl_pct = trade.get("pnl_pct", 0)
            strategy = trade.get("strategy", sig.get("signal_type", ""))
            
            if buy_price > 0:
                msg_parts.append(f"买入价: ¥{buy_price:.2f}")
                msg_parts.append(f"持仓: {hold_days}天")
            msg_parts.append(f"盈亏: ¥{pnl:+.0f} ({pnl_pct:+.1f}%)")

        msg = "\n".join(msg_parts)

        # 调用通知脚本(同步等待结果)
        proc = subprocess.run(
            [sys.executable, str(SIGNAL_NOTIFY_SCRIPT), msg],
            capture_output=True, text=True, timeout=30,
        )
        if proc.returncode == 0:
            logger.info(f"[通知] 已发送{action_cn}通知: {sig['name']}")
        else:
            logger.warning(f"[通知] {action_cn}通知发送失败(code={proc.returncode}): {sig['name']} | {proc.stderr[:100]}")

        # Webhook推送(钉钉/飞书) — 微信推送后异步发送
        _push_webhook_single(sig, result)
    except Exception as e:
        logger.debug(f"通知发送失败: {e}")


def _push_webhook_batch(buys, sells):
    """批量webhook推送 — 合成一条消息发给钉钉/飞书"""
    try:
        from src.trader.notification_webhook import notify_webhook

        parts = []
        if sells:
            parts.append(f"卖出 {len(sells)}笔:")
            for sig, result in sells:
                trade = result.get("trade", {})
                price = result.get("price", sig.get("price", 0))
                pnl_pct = trade.get("pnl_pct", 0)
                pnl = trade.get("pnl", 0)
                parts.append(
                    f"  {sig['name']}({sig['code']}) {price:.2f}元 "
                    f"盈亏{pnl:+.0f}元({pnl_pct:+.1f}%)"
                )
        if buys:
            parts.append(f"买入 {len(buys)}笔:")
            for sig, result in buys:
                price = result.get("price", sig.get("price", 0))
                parts.append(f"  {sig['name']}({sig['code']}) {price:.2f}元")

        if parts:
            title = "交易通知"
            content = "\n\n".join(parts)
            notify_webhook(title, content)
    except Exception:
        pass


def _push_webhook_single(sig: dict, result: dict):
    """单笔webhook推送"""
    try:
        from src.trader.notification_webhook import notify_trade

        trade = result.get("trade", {})
        notify_trade(
            action=sig["action"],
            name=sig["name"],
            code=sig["code"],
            price=result.get("price", sig.get("price", 0)),
            reason=sig.get("reason", ""),
            strategy=sig.get("signal_type", ""),
            buy_price=trade.get("buy_price", 0),
            hold_days=trade.get("hold_days", 0),
            pnl=trade.get("pnl", 0),
            pnl_pct=trade.get("pnl_pct", 0),
        )
    except Exception:
        pass