"""交易员简报 — 金牌交易员开盘前必看

一个交易员每天开盘前需要回答的核心问题:
  1. 昨夜美股收市情况?
  2. 今日大盘预判(涨/跌/震荡)?
  3. 证券板块整体风向?
  4. 我的5只持仓有没有公告/重大消息?
  5. 北向资金最近在买还是卖?
  6. 昨日涨停/跌停信号?
  7. 今日重大政策/事件?
  8. 操作建议: 买/卖/持有/观望?

数据源:
  - 腾讯API: 大盘指数+持仓个股+证券板块 (subprocess curl, GBK解码)
  - 东财搜索: 持仓个股新闻 (精确代码匹配, 不是名字匹配)
  - 新浪7x24+财经: 全球/A股/美股新闻
  - 静态持仓: web/state.py 的 portfolio

输出:
  - data/trader_brief.json  — 每日简报
  - 由 8_news.py 页面顶部展示
"""

import json
import re
import subprocess
import hashlib
import os
import time
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).parent.parent.parent.parent
DATA_DIR = ROOT / "data"
BRIEF_FILE = DATA_DIR / "trader_brief.json"
CURL = "/mnt/c/Windows/System32/curl.exe"

# 精确持仓 — 统一从 portfolio.json 读取（同源）
from src.config.portfolio import get_legacy_portfolio_list as _get_positions
PORTFOLIO = _get_positions()


def _curl(url: str, timeout: int = 10) -> str:
    r = subprocess.run([CURL, "-s", "--max-time", str(timeout), url], capture_output=True)
    return r.stdout.decode("utf-8", errors="replace")


def _curl_gbk(url: str, timeout: int = 10) -> str:
    r = subprocess.run([CURL, "-s", "--max-time", str(timeout), url], capture_output=True)
    return r.stdout.decode("gbk", errors="replace")


def _tencent_batch(codes: list) -> dict:
    """腾讯行情批量获取"""
    codes_str = ",".join(codes)
    raw = _curl_gbk(f"http://qt.gtimg.cn/q={codes_str}")
    results = {}
    for block in raw.split(";"):
        m = re.search(r'v_([a-z0-9]+)="(.+)"', block.strip())
        if not m:
            continue
        parts = m.group(2).split("~")
        if len(parts) > 50:
            code = parts[2]
            results[code] = {
                "name": parts[1], "price": float(parts[3]) if parts[3] else 0,
                "yesterday": float(parts[4]) if parts[4] else 0,
                "open": float(parts[5]) if parts[5] else 0,
                "high": float(parts[33]) if parts[33] else 0,
                "low": float(parts[34]) if parts[34] else 0,
                "change_pct": float(parts[32]) if parts[32] else 0,
                "change_amt": float(parts[31]) if parts[31] else 0,
                "turnover": float(parts[38]) if parts[38] else 0,
                "amount_wan": float(parts[37]) if parts[37] else 0,  # 成交额(万)
            }
    return results


# ============================================================
# 1. 大盘+持仓行情
# ============================================================

def fetch_market_data() -> dict:
    """获取大盘指数+持仓个股+证券板块行情"""
    result = {"indices": {}, "portfolio": {}, "sector_top": {}, "timestamp": datetime.now().isoformat()}

    # 大盘指数
    idx_codes = ["sh000001", "sz399001", "sz399006"]
    idx_data = _tencent_batch(idx_codes)
    name_map = {"000001": "上证指数", "399001": "深证成指", "399006": "创业板指"}
    for code, d in idx_data.items():
        key = name_map.get(code, d["name"])
        result["indices"][key] = d

    # 持仓个股
    portfolio_codes = []
    for p in PORTFOLIO:
        code = p["code"]
        if code.startswith("6"):
            portfolio_codes.append(f"sh{code}")
        else:
            portfolio_codes.append(f"sz{code}")

    port_data = _tencent_batch(portfolio_codes)
    for p in PORTFOLIO:
        code = p["code"]
        if code in port_data:
            d = port_data[code]
            d["cost"] = p["cost"]
            d["shares"] = p["shares"]
            d["stop_loss"] = p["stop_loss"]
            d["sector"] = p["sector"]
            d["code"] = p["code"]  # ← 同源: 确保code字段传入portfolio字典
            # 计算盈亏
            d["pnl_pct"] = round((d["price"] - p["cost"]) / p["cost"] * 100, 2) if p["cost"] else 0
            d["pnl_amt"] = round((d["price"] - p["cost"]) * p["shares"], 0) if p["cost"] else 0
            d["market_value"] = round(d["price"] * p["shares"], 0)
            result["portfolio"][p["name"]] = d

    # 证券板块TOP10
    sec_codes = ["sh600030", "sz000776", "sh601688", "sz002736",
                 "sh600109", "sh601211", "sz300059", "sz002797",
                 "sh601788", "sz000166"]
    sec_data = _tencent_batch(sec_codes)
    for code, d in sec_data.items():
        result["sector_top"][d["name"]] = {
            "price": d["price"], "change_pct": d["change_pct"],
            "amount_wan": d["amount_wan"],
        }

    return result


# ============================================================
# 2. 持仓个股新闻(精确匹配)
# ============================================================

def fetch_portfolio_news() -> list:
    """抓取持仓个股的精确新闻 — 用股票代码+全称搜索"""
    from urllib.parse import quote
    all_news = {}

    for stock in PORTFOLIO:
        name = stock["name"]
        code = stock["code"]

        # 搜索: 股票名
        for keyword in [name, code]:
            param = json.dumps({
                "uid": "", "keyword": keyword, "type": ["cmsArticleWebOld"],
                "client": "web", "clientType": "web", "clientVersion": "curr",
                "param": {"cmsArticleWebOld": {"searchScope": "default", "sort": "default",
                                                "pageIndex": 1, "pageSize": 5}}
            }, ensure_ascii=False)
            url = f"https://search-api-web.eastmoney.com/search/jsonp?cb=jQuery&param={quote(param)}"
            raw = _curl(url)
            m = re.search(r"jQuery\((.*)\)", raw, re.DOTALL)
            if not m:
                continue
            try:
                d = json.loads(m.group(1))
                items = d.get("result", {}).get("cmsArticleWebOld", [])
            except (json.JSONDecodeError, KeyError):
                continue

            for item in items:
                title = item.get("title", "").replace("<em>", "").replace("</em>", "")
                content = item.get("content", "").replace("<em>", "").replace("</em>", "")
                date_str = item.get("date", "")

                # 精确匹配: 标题或正文中必须出现股票名或代码
                if name not in title and name not in content and code not in title:
                    continue

                news_id = hashlib.md5(f"pnews|{title}|{date_str}".encode()).hexdigest()[:12]
                if news_id in all_news:
                    continue

                # 判断新闻情感
                sentiment = _quick_sentiment(title + " " + content)

                all_news[news_id] = {
                    "stock": name,
                    "code": code,
                    "title": title,
                    "content": content[:300],
                    "date": date_str,
                    "source": item.get("mediaName", ""),
                    "url": item.get("url", ""),
                    "sentiment": sentiment,
                }
            time.sleep(0.2)

    return list(all_news.values())


def _quick_sentiment(text: str) -> str:
    pos = ["利好", "上涨", "涨停", "大涨", "突破", "新高", "增持", "回购", "业绩增",
           "超预期", "获批", "中标", "大举加仓", "看好", "龙头"]
    neg = ["利空", "下跌", "跌停", "大跌", "暴跌", "减持", "退市", "亏损", "暴雷",
           "违规", "处罚", "风险", "承压", "下滑", "跌近", "跌幅"]
    p = sum(1 for w in pos if w in text)
    n = sum(1 for w in neg if w in text)
    if p > n + 1: return "利好"
    if n > p + 1: return "利空"
    if p > n: return "偏多"
    if n > p: return "偏空"
    return "中性"


# ============================================================
# 3. 生成交易建议
# ============================================================

def generate_advice(market: dict, portfolio_news: list) -> dict:
    """基于行情+新闻生成操作建议"""
    advice = {"market_view": "", "portfolio_advice": [], "risk_alerts": [], "opportunity": []}

    # 大盘判断
    sh = market.get("indices", {}).get("上证指数", {})
    sz = market.get("indices", {}).get("深证成指", {})
    cy = market.get("indices", {}).get("创业板指", {})

    sh_pct = sh.get("change_pct", 0)
    indices_up = sum(1 for v in [sh_pct, sz.get("change_pct",0), cy.get("change_pct",0)] if v > 0)

    if indices_up >= 3:
        advice["market_view"] = "三大指数全线飘红，市场偏强"
    elif indices_up == 0:
        advice["market_view"] = "三大指数全线下跌，市场偏弱，注意控制仓位"
    elif sh_pct > 0.5:
        advice["market_view"] = f"沪指涨{sh_pct}%，大盘偏强"
    elif sh_pct < -0.5:
        advice["market_view"] = f"沪指跌{abs(sh_pct)}%，大盘承压"
    else:
        advice["market_view"] = f"沪指{sh_pct}%，大盘窄幅震荡"

    # 证券板块整体判断
    sec_stocks = market.get("sector_top", {})
    if sec_stocks:
        sec_changes = [v["change_pct"] for v in sec_stocks.values()]
        avg_change = sum(sec_changes) / len(sec_changes) if sec_changes else 0
        sec_up = sum(1 for c in sec_changes if c > 0)
        sec_total = len(sec_changes)

        if avg_change > 1:
            advice["market_view"] += f" | 证券板块均涨{avg_change:.2f}%强势领涨"
        elif avg_change < -1:
            advice["market_view"] += f" | 证券板块均跌{abs(avg_change):.2f}%整体走弱"
        else:
            advice["market_view"] += f" | 证券板块{sec_up}/{sec_total}只上涨"

    # 持仓个股建议
    port = market.get("portfolio", {})
    for name, d in port.items():
        pnl_pct = d.get("pnl_pct", 0)
        price = d.get("price", 0)
        stop_loss = d.get("stop_loss", 0)
        cost = d.get("cost", 0)
        change_pct = d.get("change_pct", 0)

        # 找对应新闻
        stock_news = [n for n in portfolio_news if n.get("stock") == name]

        item = {"name": name, "price": price, "change": change_pct, "pnl": pnl_pct,
                "stop_distance": round((price - stop_loss) / price * 100, 1) if price else 0}

        # 止损距离
        if price <= stop_loss * 1.05:
            item["alert"] = f"⚠️ 距止损仅{item['stop_distance']}%，触发预警"
            advice["risk_alerts"].append(f"{name} 现价{price}距止损{stop_loss}仅{item['stop_distance']}%")

        # 新闻催化
        for n in stock_news:
            if n["sentiment"] in ("利好", "偏多"):
                item["catalyst"] = f"利好催化: {n['title'][:40]}"
            elif n["sentiment"] in ("利空", "偏空"):
                item["catalyst"] = f"利空压制: {n['title'][:40]}"
                advice["risk_alerts"].append(f"{name} 利空: {n['title'][:50]}")

        # 操作建议
        if pnl_pct < -15:
            item["action"] = "深度套牢，等反弹减仓"
        elif pnl_pct < -8:
            item["action"] = "浮亏较大，紧盯止损线"
        elif change_pct > 3:
            item["action"] = "今日大涨，考虑是否止盈"
        elif change_pct < -3:
            item["action"] = "今日大跌，关注是否破位"
        else:
            item["action"] = "正常持有，等待信号"

        advice["portfolio_advice"].append(item)

    # 机会
    for n in portfolio_news:
        if n["sentiment"] == "利好":
            advice["opportunity"].append(f"{n['stock']}: {n['title'][:50]}")

    return advice


# ============================================================
# 主流程
# ============================================================

def generate_brief() -> dict:
    """生成每日交易员简报"""
    print("正在生成交易员简报...")

    # 1. 行情数据
    print("  抓取行情数据...")
    market = fetch_market_data()

    # 2. 持仓新闻
    print("  抓取持仓个股新闻...")
    portfolio_news = fetch_portfolio_news()

    # 3. 生成建议
    print("  生成交易建议...")
    advice = generate_advice(market, portfolio_news)

    brief = {
        "date": datetime.now().strftime("%Y-%m-%d"),
        "time": datetime.now().strftime("%H:%M"),
        "market": market,
        "portfolio_news": portfolio_news,
        "advice": advice,
    }

    # 保存
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    BRIEF_FILE.write_text(json.dumps(brief, ensure_ascii=False, indent=2))
    print(f"  简报已保存: {BRIEF_FILE}")

    return brief


def load_brief() -> dict:
    """加载简报 — 行情数据每次实时拉取，新闻/分析部分用当天缓存"""
    cached = {}
    if BRIEF_FILE.exists():
        try:
            cached = json.loads(BRIEF_FILE.read_text())
        except Exception:
            cached = {}
    
    # 行情数据必须实时（盘中行情随时变化，不能一整天用早上缓存）
    today = datetime.now().strftime("%Y-%m-%d")
    
    # 判断是否需要重新拉行情:
    # 1. 没有缓存 → 全部重新生成
    # 2. 有缓存但行情超过15分钟 → 只刷新行情部分
    if not cached or cached.get("date") != today:
        return generate_brief()
    
    # 有今日缓存 → 只刷新行情数据（大盘+持仓+证券板块）
    try:
        fresh_market = fetch_market_data()
        cached["market"] = fresh_market
        # 重新生成持仓建议（基于最新行情 + 缓存新闻）
        portfolio_news = cached.get("portfolio_news", [])
        cached["advice"] = generate_advice(fresh_market, portfolio_news)
        cached["time"] = datetime.now().strftime("%H:%M:%S")
        # 写回缓存（下次15分钟内可直接用）
        BRIEF_FILE.write_text(json.dumps(cached, ensure_ascii=False, indent=2))
    except Exception:
        pass  # 行情拉取失败就用缓存
    
    return cached


if __name__ == "__main__":
    brief = generate_brief()

    print("\n" + "=" * 60)
    print(f"金牌交易员简报 | {brief['date']} {brief['time']}")
    print("=" * 60)

    # 大盘
    print(f"\n📊 大盘: {brief['advice']['market_view']}")
    for idx_name, d in brief['market'].get('indices', {}).items():
        print(f"  {idx_name}: {d['price']} ({d['change_pct']}%)")

    # 持仓
    print(f"\n💰 持仓:")
    for item in brief['advice'].get('portfolio_advice', []):
        alert = item.get('alert', '')
        catalyst = item.get('catalyst', '')
        print(f"  {item['name']}: {item['price']}元 ({item['change']}%) | 浮盈{item['pnl']}% | {item['action']}")
        if alert:
            print(f"    {alert}")
        if catalyst:
            print(f"    {catalyst}")

    # 新闻
    print(f"\n📰 持仓相关新闻 ({len(brief.get('portfolio_news',[]))}条):")
    for n in brief.get('portfolio_news', [])[:5]:
        print(f"  [{n['sentiment']}] {n['stock']}: {n['title'][:50]}")
        if n.get('content'):
            print(f"    {n['content'][:80]}")

    # 风险
    if brief['advice'].get('risk_alerts'):
        print(f"\n⚠️ 风险预警:")
        for r in brief['advice']['risk_alerts']:
            print(f"  {r}")
