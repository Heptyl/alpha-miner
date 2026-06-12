#!/usr/bin/env python3
"""Send a message to Weixin via Hermes send_message tool infrastructure."""
import asyncio
import json
import os
import sys

# Set up Hermes environment
hermes_home = os.path.expanduser("~/.hermes")
os.environ["HERMES_HOME"] = hermes_home

sys.path.insert(0, os.path.join(hermes_home, "hermes-agent"))

# Load .env if present
dotenv_path = os.path.join(hermes_home, ".env")
if os.path.exists(dotenv_path):
    with open(dotenv_path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, _, val = line.partition("=")
                os.environ.setdefault(key.strip(), val.strip().strip('"').strip("'"))

from gateway.config import PlatformConfig

async def send_one(chat_id, message):
    from gateway.platforms.weixin import send_weixin_direct
    
    WEIXIN_CONFIG = {
        "account_id": "b031c522a73d@im.bot",
        "base_url": "https://ilinkai.weixin.qq.com",
        "cdn_base_url": "https://novac2c.cdn.weixin.qq.com/c2c",
    }
    WEIXIN_TOKEN = "b031c522a73d@im.bot:06000026e24abc012929bb88ee3382772dce1f"
    
    result = await send_weixin_direct(
        extra=WEIXIN_CONFIG,
        token=WEIXIN_TOKEN,
        chat_id=chat_id,
        message=message,
    )
    return result

async def main():
    if len(sys.argv) < 3:
        print("Usage: send_weixin.py <chat_id> <message>")
        sys.exit(1)
    
    chat_id = sys.argv[1]
    message = sys.argv[2]
    
    print(f"Sending to {chat_id}...")
    result = await send_one(chat_id, message)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    
    if result.get("error"):
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())
