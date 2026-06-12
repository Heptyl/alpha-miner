#!/usr/bin/env python3
"""Send a message via Weixin using the Alpha Miner push module.

用法:
  echo "消息内容" | uv run python scripts/send_wechat.py
  uv run python scripts/send_wechat.py message.txt
  uv run python scripts/send_wechat.py --brief          # 发送盘后简报
  uv run python scripts/send_wechat.py --brief --date 2026-04-30
"""
import argparse
import asyncio
import json
import os
import sys

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)


def main():
    parser = argparse.ArgumentParser(description="微信推送")
    parser.add_argument("msg_file", nargs="?", help="消息文本文件路径")
    parser.add_argument("--brief", action="store_true", help="发送盘后简报")
    parser.add_argument("--date", type=str, default="", help="简报日期 YYYY-MM-DD")
    parser.add_argument("--db", type=str, default=f"{project_root}/data/alpha_miner.db", help="数据库路径")
    args = parser.parse_args()

    if args.brief:
        from src.drift.push import format_daily_brief_for_wechat, push_message_sync

        msg = format_daily_brief_for_wechat(args.db, date=args.date)
        print("--- 简报内容 ---")
        print(msg)
        print("--- 推送中 ---")
    else:
        from src.drift.push import push_message_sync

        if args.msg_file:
            with open(args.msg_file) as f:
                msg = f.read()
        else:
            msg = sys.stdin.read()
        msg = msg.strip()
        if not msg:
            print("[ERROR] 消息为空")
            sys.exit(1)

    result = push_message_sync(msg)
    print(json.dumps(result, ensure_ascii=False, indent=2))

    if result.get("success"):
        print("[OK] 推送成功！")
    else:
        print(f"[FAIL] 推送失败: {result.get('error', '未知错误')}")
        sys.exit(1)


if __name__ == "__main__":
    main()
