#!/usr/bin/env python3
"""Send deep pick content to WeChat, split into segments <=400 chars."""
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

from gateway.platforms.weixin import send_weixin_direct

WEIXIN_CONFIG = {
    "account_id": "b031c522a73d@im.bot",
    "base_url": "https://ilinkai.weixin.qq.com",
    "cdn_base_url": "https://novac2c.cdn.weixin.qq.com/c2c",
}
# Read the actual token from the account file
account_path = os.path.join(hermes_home, "weixin", "accounts", "b031c522a73d@im.bot.json")
with open(account_path) as f:
    account_data = json.load(f)
WEIXIN_TOKEN = account_data["token"]

CHAT_ID = "o9cq8087nG_q9BSnWk0INqZlCaSI@im.wechat"

# The three message segments
SEGMENTS = [
    # Segment 1: 5只简要分析
    """🎯 Alpha Miner | 05月01日操作指南
📅 基于04-30收盘数据

【简要分析】
1. 002149 西部材料(63.51)：小金属前期妖股缩量企稳后光头长阳反包涨停，机构净买入，短线进入二波主升。
2. 688498 源杰科技(1571)：半导体核心，沿5日线强趋势连创60日新高，量价健康资金锁仓，有加速赶顶迹象。
3. 600726 华电能源(6.59)：电力高标昨日爆量长上影后今日低开闷杀，主力大幅流出，短线见顶坚决规避！
4. 002937 兴瑞科技(33.55)：汽车零部件趋势股，碎步连阳不断创60日新高，量能温和放大，筹码稳定。
5. 301310 鑫宏业(66.65)：汽车零部件箱体平台后放量突破涨停创新高，资金强度好，溢价预期较高。""",

    # Segment 2: 前3只操作建议
    """【前3只操作建议】

① 002149 西部材料(63.51)
竞价63.50参与，低开破62放弃。
高开+1%~+3%企稳不破均价线轻仓试错。
买入：63.50/61.60 | 止盈：66.50/70.00 | 止损：61.60

② 688498 源杰科技(1571)
观察竞价，小幅高开可挂1570。
低开1540以下暂缓，高开0~+2%企稳半仓。
买入：1568/1539 | 止盈：1650/1720 | 止损：1524

③ 600726 华电能源(6.59) ⚠见顶股
任何红盘高开仅执行卖出，绝对禁止抄底！
高开反弹6.65-6.70果断清仓。
低开5分钟不收复6.59无条件减仓。""",

    # Segment 3: 后2只操作建议+纪律
    """【后2只操作建议】

④ 002937 兴瑞科技(33.55)
竞价33.50参与，低开破33放弃。
高开+1%~+2%企稳不破均价线轻仓试错。
买入：33.50/32.80 | 止盈：34.50/35.80 | 止损：32.20

⑤ 301310 鑫宏业(66.65)
观察竞价，小幅高开可挂66.50。
低开65以下暂缓，高开0~+3%企稳半仓。
买入：66.50/64.80 | 止盈：70.00/73.30 | 止损：64.50

📋 操作纪律：
1.严格止损：触发止损位无条件平仓
2.分仓管理：单只不超过总资金30%
3.见好就收：达第一止盈位减仓一半

⚠ 以上仅供参考，不构成投资建议""",
]


async def send_one(chat_id: str, message: str) -> dict:
    result = await send_weixin_direct(
        extra=WEIXIN_CONFIG,
        token=WEIXIN_TOKEN,
        chat_id=chat_id,
        message=message,
    )
    return result


async def main():
    # Check segment lengths
    for i, seg in enumerate(SEGMENTS):
        print(f"Segment {i+1}: {len(seg)} chars")
        if len(seg) > 400:
            print(f"  WARNING: Segment {i+1} exceeds 400 chars!")

    any_success = False
    rate_limited = False

    for i, seg in enumerate(SEGMENTS):
        print(f"\nSending segment {i+1}/{len(SEGMENTS)}...")
        try:
            result = await send_one(CHAT_ID, seg)
            print(f"  Result: {json.dumps(result, ensure_ascii=False)}")
            if result.get("success"):
                any_success = True
                print(f"  ✅ Segment {i+1} sent successfully")
            elif result.get("error"):
                error = result["error"]
                print(f"  ❌ Error: {error}")
                # Check for rate limiting (ret=-2)
                if "ret" in str(error) and "-2" in str(error):
                    rate_limited = True
                    print("  🚫 iLink rate limited detected")
                    break
        except Exception as e:
            print(f"  ❌ Exception: {e}")
            # Don't retry, move to next segment
    
    if rate_limited:
        print("\n🚫 iLink限流中，下次重试")
    elif any_success:
        print("\n✅ 推送成功")
    else:
        print("\n❌ 推送失败")


if __name__ == "__main__":
    asyncio.run(main())
