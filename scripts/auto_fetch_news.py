"""定时新闻采集脚本 — 供cron调用"""
import sys
sys.path.insert(0, "/home/ccy/alpha-miner")

from src.data.sources.eastmoney_news import fetch_today_news

result = fetch_today_news()
if result:
    total = result.get("stats", {}).get("total", 0)
    news = result.get("news", [])
    latest = news[0]["date"] if news else "无"
    print(f"新闻采集完成: {total}条, 最新: {latest}")
else:
    print("新闻采集失败")
    sys.exit(1)
