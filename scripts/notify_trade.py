"""成交通知 — 即时推送微信(秒级)

被trading_daemon调用:
  python notify_trade.py "消息内容"

直接通过Hermes weixin gateway的send_weixin_direct发送。
"""
import asyncio
import json
import os
import sys
from pathlib import Path
from datetime import datetime

PROJECT_ROOT = Path(__file__).resolve().parents[1]
NOTIFY_QUEUE = PROJECT_ROOT / "output" / "trader" / "signals" / "notifications.jsonl"


def notify(msg: str) -> bool:
    """即时推送到微信"""
    try:
        sys.path.insert(0, str(PROJECT_ROOT))
        from src.drift.push import _load_latest_account
        config = _load_latest_account()
        if not config:
            print("微信配置未找到", file=sys.stderr)
            _write_queue(msg, False)
            return False

        # import gateway模块
        agent_dir = str(Path.home() / ".hermes" / "hermes-agent")
        if agent_dir not in sys.path:
            sys.path.insert(0, agent_dir)
        original_cwd = os.getcwd()
        os.chdir(agent_dir)
        try:
            from gateway.platforms.weixin import send_weixin_direct
            extra = {
                "account_id": config.account_id,
                "base_url": config.base_url,
                "cdn_base_url": config.cdn_base_url,
            }
            result = asyncio.run(send_weixin_direct(
                extra=extra,
                token=config.token,
                chat_id=config.chat_id,
                message=msg,
            ))
            if result.get("success"):
                return True
            else:
                print(f"发送失败: {result.get('error')}", file=sys.stderr)
                _write_queue(msg, False)
                return False
        finally:
            os.chdir(original_cwd)
    except Exception as e:
        print(f"推送异常: {e}", file=sys.stderr)
        _write_queue(msg, False)
        return False


def _write_queue(msg: str, sent: bool):
    """写入队列(cron补发)"""
    try:
        NOTIFY_QUEUE.parent.mkdir(parents=True, exist_ok=True)
        entry = {
            "timestamp": datetime.now().isoformat(),
            "message": msg,
            "sent": sent,
        }
        with open(NOTIFY_QUEUE, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception:
        pass


if __name__ == "__main__":
    msg = sys.argv[1] if len(sys.argv) > 1 else "测试通知"
    ok = notify(msg)
    print(f"sent={ok}")
    sys.exit(0 if ok else 1)
