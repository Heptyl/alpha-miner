"""notification_webhook.py — 钉钉/飞书Webhook通知

支持的通知类型:
  - 买入/卖出预告
  - 熔断触发
  - 异常检测

配置: daemon_config.py 中的 DINGTALK_WEBHOOK_URL / FEISHU_WEBHOOK_URL
不配URL则不发送, 不影响现有微信推送。
"""

from __future__ import annotations

import logging
from datetime import datetime

from src.trader.daemon_config import DINGTALK_WEBHOOK_URL, DINGTALK_KEYWORD, FEISHU_WEBHOOK_URL

logger = logging.getLogger("trading_daemon")


def notify_webhook(title: str, content: str, level: str = "info"):
    """统一入口: 向所有已配置的webhook发送通知

    Args:
        title: 消息标题
        content: Markdown格式正文
        level: info/warning/error — 影响飞书卡片颜色
    """
    if DINGTALK_WEBHOOK_URL:
        _send_dingtalk(title, content)

    if FEISHU_WEBHOOK_URL:
        _send_feishu(title, content, level)


def notify_trade(action: str, name: str, code: str, price: float,
                 reason: str = "", strategy: str = "",
                 buy_price: float = 0, hold_days: int = 0,
                 pnl: float = 0, pnl_pct: float = 0):
    """交易通知(买入/卖出)

    Args:
        action: "buy" 或 "sell"
        name/code: 股票名称/代码
        price: 成交价格
        reason: 信号原因
        strategy: 策略A/B/C
        buy_price/hold_days/pnl/pnl_pct: 卖出时的持仓信息
    """
    action_cn = "买入" if action == "buy" else "卖出"
    icon = "##green_circle;## " if action == "buy" else "##red_circle;## "

    lines = [
        f"### {DINGTALK_KEYWORD} - {icon}{action_cn}通知",
        f"**{name}**({code})",
        f"价格: {price:.2f}元",
    ]
    if strategy:
        lines.append(f"策略: {strategy}")
    if reason:
        lines.append(f"原因: {reason}")

    if action == "sell" and buy_price > 0:
        lines.append(f"---")
        lines.append(f"买入价: {buy_price:.2f}元 | 持仓: {hold_days}天")
        pnl_icon = "+" if pnl >= 0 else ""
        lines.append(f"盈亏: {pnl_icon}{pnl:.0f}元 ({pnl_pct:+.1f}%)")

    lines.append(f"\n_{datetime.now().strftime('%H:%M:%S')}_")

    content = "\n\n".join(lines)
    level = "warning" if action == "sell" and pnl_pct < -3 else "info"
    notify_webhook(f"{action_cn} {name}({code})", content, level)


def notify_circuit_breaker(message: str):
    """熔断/风控通知"""
    content = f"### {DINGTALK_KEYWORD} - ##warning## 熔断触发\n\n{message}"
    notify_webhook("熔断触发", content, "error")


def notify_anomaly(message: str):
    """异常检测通知"""
    content = f"### {DINGTALK_KEYWORD} - ##rotating_light## 异常检测\n\n{message}"
    notify_webhook("异常检测", content, "warning")


# ── 钉钉 ──

def _send_dingtalk(title: str, content: str):
    """钉钉Webhook: markdown消息格式"""
    try:
        import requests

        data = {
            "msgtype": "markdown",
            "markdown": {
                "title": f"{DINGTALK_KEYWORD} - {title}",
                "text": content,
            },
        }
        resp = requests.post(
            DINGTALK_WEBHOOK_URL,
            json=data,
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
        result = resp.json()
        if result.get("errcode") == 0:
            logger.info(f"[钉钉] 发送成功: {title}")
        else:
            logger.warning(f"[钉钉] 发送失败: {result.get('errmsg', resp.text[:100])}")
    except Exception as e:
        logger.debug(f"[钉钉] 发送异常: {e}")


# ── 飞书 ──

_FEISHU_COLORS = {
    "info": "blue",
    "warning": "orange",
    "error": "red",
}


def _send_feishu(title: str, content: str, level: str = "info"):
    """飞书Webhook: interactive card消息格式"""
    try:
        import requests

        # 飞书card不支持完整markdown, 用fields+div展示
        color = _FEISHU_COLORS.get(level, "blue")
        lines = content.split("\n")
        # 简化: 把内容放进一个div里
        text_lines = []
        for line in lines:
            # 跳过markdown标题前缀和空行
            clean = line.strip()
            if clean.startswith("###"):
                clean = clean.lstrip("#").strip()
            if clean:
                text_lines.append(clean)

        data = {
            "msg_type": "interactive",
            "card": {
                "header": {
                    "title": {
                        "content": f"{DINGTALK_KEYWORD} - {title}",
                        "tag": "plain_text",
                    },
                    "template": color,
                },
                "elements": [
                    {
                        "tag": "div",
                        "text": {
                            "content": "\n".join(text_lines),
                            "tag": "lark_md",
                        },
                    },
                    {"tag": "hr"},
                    {
                        "tag": "note",
                        "elements": [
                            {
                                "tag": "plain_text",
                                "content": f"Alpha Miner | {datetime.now().strftime('%H:%M:%S')}",
                            }
                        ],
                    },
                ],
            },
        }
        resp = requests.post(
            FEISHU_WEBHOOK_URL,
            json=data,
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
        result = resp.json()
        if result.get("code") == 0:
            logger.info(f"[飞书] 发送成功: {title}")
        else:
            logger.warning(f"[飞书] 发送失败: {result.get('msg', resp.text[:100])}")
    except Exception as e:
        logger.debug(f"[飞书] 发送异常: {e}")
