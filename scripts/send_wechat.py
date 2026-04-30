#!/usr/bin/env python3
"""Send a message via Weixin using the Hermes gateway internals."""
import asyncio
import sys
import os

sys.path.insert(0, '/home/ccy/.hermes/hermes-agent')
os.chdir('/home/ccy/.hermes/hermes-agent')

import yaml
from gateway.platforms.weixin import send_weixin_direct
from gateway.config import PlatformConfig

# Read Hermes config
with open('/home/ccy/.hermes/config.yaml') as f:
    config = yaml.safe_load(f)

weixin_cfg = config.get('platforms', {}).get('weixin', {})
token = weixin_cfg.get('token', '')
extra = weixin_cfg.get('extra', {})

# Read message from file
if len(sys.argv) > 1:
    msg_file = sys.argv[1]
    with open(msg_file) as f:
        msg = f.read()
else:
    msg = sys.stdin.read()

# The full chat_id from the environment
chat_id = os.environ.get('WECHAT_CHAT_ID', 'o9cq8087nG_q9BSnWk0INqZlCaSI@im.wechat')

result = asyncio.run(send_weixin_direct(
    extra=extra,
    token=token,
    chat_id=chat_id,
    message=msg.strip(),
))

print(f"Result: {result}")
if result.get('error'):
    sys.exit(1)
