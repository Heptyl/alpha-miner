"""A股新闻采集器 v4 — 交易员全视角

数据源(6个):
  1. 新浪7x24全球快讯 (全球时事)
  2. 新浪财经栏目 (A股/美股/港股/期货，有摘要)
  3. 东财搜索 (A股市场+持仓+热门板块，有正文摘要)
  4. 证券时报 (政策+市场要闻，证监会指定媒体)
  5. 东财板块资金流 (行业+概念涨幅/净流入)
  6. 美股收盘+富时A50 (隔夜外盘方向)

v4升级(vs v3):
  - 砍掉网易财经(只有标题没正文，价值低)
  - 新增证券时报(155条/天，政策第一手来源)
  - 新增东财行业+概念板块资金流(TOP15)
  - 新增美股三大指数+富时A50期货(隔夜外盘)
  - 东财搜索关键词16→30+(加入AI/半导体/低空经济/机器人等热门板块)
  - 新浪7x24噪音过滤增强(彩票/足彩/竞彩/双色球/大乐透)

设计参考:
  - Bloomberg Terminal: 行情+新闻+板块资金 一体化
  - 财联社电报: 实时滚动+情绪标记+影响标注
  - 东方财富: 要闻分类+个股关联+板块资金流
"""

import json
import hashlib
import logging
import os
import re
import time
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import quote

logger = logging.getLogger(__name__)

ROOT = Path(__file__).parent.parent.parent.parent  # alpha-miner/
DATA_DIR = ROOT / "data"
TODAY_FILE = DATA_DIR / "news_today.json"
HISTORY_DIR = DATA_DIR / "news_history"

CURL = "/mnt/c/Windows/System32/curl.exe"

# 用户持仓 — 统一从 portfolio.json 读取（同源）
from src.config.portfolio import (
    get_legacy_name_map as _get_name_map,
    get_portfolio_aliases as _get_aliases,
    get_portfolio_sectors as _get_sectors,
)
PORTFOLIO_MAP = _get_name_map()       # {name: code}
PORTFOLIO_ALIASES = _get_aliases()     # {code: [别名]}
PORTFOLIO_SECTOR_KEYWORDS = _get_sectors()  # {code: [板块关键词]}

# 噪音过滤词
NOISE_WORDS = ["彩票", "足彩", "竞彩", "双色球", "大乐透", "排列三", "排列五",
               "快乐8", "竞足", "胜负彩", "彩果", "奖号", "开奖", "投注"]


def _curl_get(url: str, timeout: int = 15) -> str:
    cmd = f'{CURL} -s --max-time {timeout} "{url}"'
    return os.popen(cmd).read()


def _curl_get_gbk(url: str, timeout: int = 15) -> str:
    """curl + gbk转utf-8（腾讯API用gbk编码）"""
    cmd = f'{CURL} -s --max-time {timeout} "{url}" | iconv -f gbk -t utf-8 2>/dev/null'
    return os.popen(cmd).read()


def _is_noise(title: str) -> bool:
    """判断是否是噪音新闻（彩票/足彩等）"""
    return any(w in title for w in NOISE_WORDS)


def _is_recent(date_str: str, days: int = 3) -> bool:
    """检查日期是否在最近N天内(包容周末)"""
    try:
        news_date = datetime.strptime(date_str[:10], "%Y-%m-%d")
        cutoff = datetime.now() - timedelta(days=days)
        return news_date >= cutoff
    except (ValueError, TypeError):
        return False


# ============================================================
# 数据源1: 新浪7x24全球快讯 (增强噪音过滤)
# ============================================================

def _fetch_sina_7x24(page_size: int = 60) -> list:
    url = (
        f"https://zhibo.sina.com.cn/api/zhibo/feed?"
        f"page=1&page_size={page_size}&zhibo_id=152"
        f"&tag_id=0&direction=0&dpi=2&type=0"
    )
    raw = _curl_get(url)
    try:
        d = json.loads(raw)
        items = d.get("result", {}).get("data", {}).get("feed", {}).get("list", [])
    except (json.JSONDecodeError, KeyError, TypeError):
        return []

    news_list = []
    today = datetime.now().strftime("%Y-%m-%d")
    for item in items:
        text = re.sub(r"<[^>]+>", "", item.get("rich_text", "")).strip()
        if not text or len(text) < 10:
            continue
        if _is_noise(text):
            continue
        create_time = item.get("create_time", "")
        if not create_time.startswith(today):
            continue

        news_list.append({
            "news_id": hashlib.md5(f"sina|{item.get('id', '')}".encode()).hexdigest()[:16],
            "title": text[:80],
            "content": text,
            "date": create_time,
            "source": "新浪7x24",
            "url": item.get("docurl", ""),
            "keyword": "7x24快讯",
        })
    return news_list


# ============================================================
# 数据源2: 新浪财经A股专用 (A股/美股/全球/期货)
# ============================================================

SINA_FINANCE_CHANNELS = [
    (2509, "A股要闻"), (2510, "股票新闻"), (2511, "全球市场"),
    (2512, "美股"), (2513, "港股"), (2515, "期货"),
]


def _fetch_sina_finance() -> list:
    all_items = []
    for lid, channel_name in SINA_FINANCE_CHANNELS:
        url = (
            f"https://feed.mix.sina.com.cn/api/roll/get?"
            f"pageid=153&lid={lid}&num=20&versionNumber=1.2.4"
        )
        raw = _curl_get(url)
        try:
            d = json.loads(raw)
            items = d.get("result", {}).get("data", [])
        except (json.JSONDecodeError, KeyError, TypeError):
            time.sleep(0.3)
            continue

        for item in items:
            ctime_ts = item.get("ctime", 0)
            try:
                dt = datetime.fromtimestamp(int(ctime_ts))
                create_time = dt.strftime("%Y-%m-%d %H:%M:%S")
            except (ValueError, TypeError, OSError):
                continue

            title = item.get("title", "").strip()
            if not title or len(title) < 8:
                continue
            if _is_noise(title):
                continue
            if not _is_recent(create_time):
                continue

            summary = item.get("summary", "") or item.get("wapsummary", "") or ""

            all_items.append({
                "news_id": hashlib.md5(f"sina_f|{item.get('docid', title)}".encode()).hexdigest()[:16],
                "title": title,
                "content": summary[:500] if summary else "",
                "date": create_time,
                "source": item.get("media_name", "") or f"新浪{channel_name}",
                "url": item.get("url", ""),
                "keyword": channel_name,
                "channel": channel_name,
            })
        time.sleep(0.3)
    return all_items


# ============================================================
# 数据源3: 东财搜索 (扩展到30+关键词)
# ============================================================

EASTMONEY_KEYWORDS = [
    # A股市场
    "A股", "涨停", "利好", "券商", "证券", "北向资金", "资金流入",
    "政策", "降息", "降准", "央行",
    # 热门板块(v4新增)
    "AI", "半导体", "低空经济", "机器人", "新能源", "算力",
    "量子计算", "光伏", "锂电池", "军工", "医药",
    # 国际重磅(v6新增)
    "特朗普", "中美", "访华", "关税", "贸易", "外交",
    # 持仓
    "东方财富", "第一创业", "电广传媒", "东方电气", "人福医药",
]


def _fetch_eastmoney_search(keyword: str, page_size: int = 10) -> list:
    param = json.dumps({
        "uid": "", "keyword": keyword, "type": ["cmsArticleWebOld"],
        "client": "web", "clientType": "web", "clientVersion": "curr",
        "param": {"cmsArticleWebOld": {"searchScope": "default", "sort": "default",
                                        "pageIndex": 1, "pageSize": page_size}}
    }, ensure_ascii=False)
    url = f"https://search-api-web.eastmoney.com/search/jsonp?cb=jQuery&param={quote(param)}"
    raw = _curl_get(url)

    m = re.search(r"jQuery\((.*)\)", raw, re.DOTALL)
    if not m:
        return []
    try:
        d = json.loads(m.group(1))
        items = d.get("result", {}).get("cmsArticleWebOld", [])
    except (json.JSONDecodeError, KeyError):
        return []

    news_list = []
    for item in items:
        title = item.get("title", "").replace("<em>", "").replace("</em>", "")
        content = item.get("content", "").replace("<em>", "").replace("</em>", "")
        date_str = item.get("date", "")
        if not title or not _is_recent(date_str):
            continue
        if _is_noise(title):
            continue

        news_list.append({
            "news_id": hashlib.md5(f"em|{title}|{date_str}".encode()).hexdigest()[:16],
            "title": title,
            "content": content[:500],
            "date": date_str,
            "source": item.get("mediaName", "") or "东财",
            "url": item.get("url", ""),
            "keyword": keyword,
        })
    return news_list


# ============================================================
# 数据源4: 证券时报 (v4新增，替代网易财经)
# ============================================================

def _fetch_stcn_news() -> list:
    """证券时报 — 证监会指定信息披露媒体，政策第一手来源"""
    raw = _curl_get("https://www.stcn.com/", timeout=15)
    if not raw:
        return []

    # 提取title属性中的新闻标题
    titles = re.findall(r'title="([^"]{10,80})"', raw)
    seen = set()
    news_list = []
    for title in titles:
        title = title.strip()
        if len(title) < 10 or title in seen:
            continue
        if _is_noise(title):
            continue
        seen.add(title)

        news_list.append({
            "news_id": hashlib.md5(f"stcn|{title}".encode()).hexdigest()[:16],
            "title": title,
            "content": "",  # 证券时报首页只有标题
            "date": datetime.now().strftime("%Y-%m-%d"),
            "source": "证券时报",
            "url": "",
            "keyword": "证券时报",
        })
    return news_list[:50]  # 最多50条


# ============================================================
# 数据源5: 新浪行业板块资金流 (v4新增)
# 注: 东财push2 API从WSL被封，改用新浪行业板块接口(gbk)
# ============================================================

def _fetch_sector_fund_flow() -> dict:
    """新浪行业板块(49个) + 按涨幅排序"""
    result = {"industry": [], "concept": []}

    # 新浪行业板块(gbk编码)
    cmd = f'{CURL} -s --max-time 10 "https://vip.stock.finance.sina.com.cn/q/view/newSinaHy.php" | iconv -f gbk -t utf-8 2>/dev/null'
    raw = os.popen(cmd).read()
    if not raw:
        return result

    # 解析 JS对象 {"key":"code,name,count,avg_price,change_pct,volume,turnover,leader_code,leader_price,leader_change,..."}
    items = re.findall(r'"(\w+)":"([^"]+)"', raw)
    sectors = []
    for key, val in items:
        fields = val.split(",")
        if len(fields) < 8:
            continue
        try:
            name = fields[1]
            count = int(fields[2])
            change_pct = float(fields[4]) * 100 if abs(float(fields[4])) < 1 else float(fields[4])
            turnover = float(fields[6]) if fields[6] else 0
            leader = fields[-1] if fields[-1] else ""
            sectors.append({
                "name": name,
                "change_pct": round(change_pct, 2),
                "stock_count": count,
                "turnover": round(turnover / 1e8, 2),  # 亿
                "leader": leader,
            })
        except (ValueError, IndexError):
            continue

    # 按涨幅排序
    sectors.sort(key=lambda x: x["change_pct"], reverse=True)
    result["industry"] = sectors[:20]
    return result


# ============================================================
# 数据源6: 美股收盘+富时A50 (v4新增)
# ============================================================

def _fetch_us_market() -> dict:
    """美股三大指数 + 富时A50 + 恒生指数"""
    codes = "usIXIC,usDJI,usSPX,CHA50CFD,hkHSI"
    raw = _curl_get_gbk(f"https://qt.gtimg.cn/q={codes}")
    if not raw:
        return {}

    result = {}
    for line in raw.strip().split(";"):
        line = line.strip()
        if not line or "=" not in line:
            continue
        parts = line.split("~")
        if len(parts) < 35:
            continue

        code_prefix = line.split("=")[0].split("_")[-1] if "_" in line else ""
        name = parts[1]
        price = parts[3]
        pct = parts[32] if len(parts) > 32 else "0"

        label = {
            "usIXIC": "纳斯达克", "usNDX": "纳斯达克100", "usDJI": "道琼斯", "usSPX": "标普500",
            "CHA50CFD": "富时A50", "hkHSI": "恒生指数",
        }.get(code_prefix, name)

        try:
            result[code_prefix] = {
                "name": label,
                "price": float(price) if price else 0,
                "change_pct": float(pct) if pct else 0,
            }
        except (ValueError, TypeError):
            continue

    return result


# ============================================================
# 分析引擎 (不变)
# ============================================================

def _analyze_sentiment(title: str, content: str) -> dict:
    text = f"{title} {content}"
    positive_words = [
        "利好", "上涨", "涨停", "大涨", "突破", "新高", "反弹", "爆发",
        "增持", "回购", "业绩预增", "超预期", "获批", "中标", "订单",
        "政策支持", "强势", "拉升", "资金流入", "放量", "翻红", "回暖",
        "复苏", "增长", "盈利", "降息", "降准", "刺激", "提振",
        "大举加仓", "看好", "牛市", "龙头",
    ]
    negative_words = [
        "利空", "下跌", "跌停", "大跌", "暴跌", "破位", "新低", "闪崩",
        "减持", "退市", "亏损", "风险", "违规", "处罚", "被查",
        "下调", "预警", "暴雷", "违约", "资金流出", "缩量", "破发",
        "承压", "下滑", "回落", "制裁", "贸易战", "衰退", "通胀",
        "强制退市", "立案调查", "黑嘴",
    ]

    pos_count = sum(1 for w in positive_words if w in text)
    neg_count = sum(1 for w in negative_words if w in text)

    try:
        from snownlp import SnowNLP
        snow_score = SnowNLP(title).sentiments
    except Exception:
        snow_score = 0.5

    if pos_count > neg_count + 1:
        sentiment = "利好"
    elif neg_count > pos_count + 1:
        sentiment = "利空"
    elif pos_count > neg_count:
        sentiment = "偏多"
    elif neg_count > pos_count:
        sentiment = "偏空"
    else:
        sentiment = "中性"

    high_impact_words = ["政策", "降息", "降准", "注册制", "IPO", "重大", "突发",
                         "国务院", "美联储", "制裁", "关税", "战争", "地震",
                         "退市", "强制退市", "立案", "暴雷",
                         "访华", "国事访问", "中美关系", "联合声明", "贸易战",
                         "贸易协议", "外交", "元首", "反制", "双边协议"]
    impact = "高" if any(w in text for w in high_impact_words) else \
             "中" if (pos_count + neg_count) >= 3 else "低"

    return {
        "sentiment": sentiment,
        "sentiment_score": round(snow_score, 3),
        "impact": impact,
        "pos_signals": pos_count,
        "neg_signals": neg_count,
    }


def _classify_news(title: str, content: str) -> str:
    text = f"{title} {content}"

    # 重磅新闻优先检测(外交/国事/政策重大事件, 优先级最高)
    heavy_keywords = ["访华", "国事访问", "联合声明", "中美关系", "中俄", "中日", "中欧",
                      "贸易战", "贸易协议", "制裁", "反制", "外交", "习近平",
                      "国家主席", "领导人会谈", "双边", "元首"]
    if any(k in text for k in heavy_keywords):
        return "重磅"

    categories = {
        "A股": ["A股", "涨停", "跌停", "北向", "沪指", "深成指", "创业板", "沪深",
                "两市", "成交额", "换手", "资金流入", "主力", "证券", "券商", "金融",
                "选股", "打板", "连板", "龙头", "牛市", "熊市", "震荡", "十倍股"],
        "政策": ["政策", "国务院", "发改委", "证监会", "央行", "降息", "降准",
                 "法规", "监管", "规划", "战略", "通知", "印发", "部署",
                 "总理", "部长", "会议", "人大", "政协"],
        "重磅": [],  # 已在上面优先处理, 这里留空防止重复
        "宏观": ["GDP", "CPI", "PMI", "就业", "通胀", "汇率", "利率", "经济数据",
                 "进出口", "财政", "货币", "复苏", "增长", "衰退"],
        "美股": ["美股", "纳斯达克", "道琼斯", "标普", "美联储", "纳指", "苹果",
                 "英伟达", "特斯拉", "非农", "鲍威尔", "纽交所", "关税"],
        "行业": ["板块", "行业", "产业链", "新能源", "芯片", "AI", "人工智能",
                 "医药", "消费", "地产", "汽车", "半导体", "光伏", "电池",
                 "储能", "风电", "核电", "稀土", "钢铁", "煤炭", "有色",
                 "银行", "保险", "信托", "机器人", "低空经济", "算力",
                 "量子计算", "锂电池", "军工"],
        "全球": ["全球", "欧洲", "亚太", "日本", "韩国", "英国", "德国",
                 "原油", "黄金", "地缘", "中东", "伊朗", "以色列", "俄罗斯",
                 "乌克兰", "北约", "联合国", "世卫", "OPEC", "沙特"],
        "公司": ["公司", "公告", "业绩", "财报", "增持", "减持", "回购",
                 "分红", "IPO", "上市", "收购", "合并", "调研"],
    }
    for cat, keywords in categories.items():
        if keywords and any(k in text for k in keywords):
            return cat
    return "其他"


def _match_stocks(title: str, content: str) -> list:
    """匹配新闻中提到的持仓股票 — 全名+别名+板块关键词"""
    text = f"{title} {content}"
    # 基础股票池(含知名个股)
    stock_map = {
        **PORTFOLIO_MAP,
        "中信证券": "600030", "贵州茅台": "600519", "宁德时代": "300750",
        "比亚迪": "002594", "中国平安": "601318", "招商银行": "600036",
        "TCL科技": "000100", "紫金矿业": "601899", "寒武纪": "688256",
        "美的集团": "000333", "格力电器": "000651", "万科A": "000002",
    }
    matched = [{"name": n, "code": c} for n, c in stock_map.items() if n in text]

    # 别名匹配(只匹配持仓)
    for code, aliases in PORTFOLIO_ALIASES.items():
        # 跳过已经通过全名匹配的
        if any(m["code"] == code for m in matched):
            continue
        name = [n for n, c in PORTFOLIO_MAP.items() if c == code][0]
        for alias in aliases:
            if alias in text and alias != name:
                matched.append({"name": name, "code": code, "match": alias})
                break

    return matched


def _analyze_trade_impact(news: dict) -> str:
    """分析新闻对用户持仓的具体交易影响"""
    title = news.get("title", "")
    content = news.get("content", "")
    text = f"{title} {content}"
    sentiment = news.get("sentiment", "中性")
    related = news.get("related_stocks", [])

    impacts = []

    # 1. 直接关联持仓
    portfolio_hits = [s for s in related if s["code"] in PORTFOLIO_MAP.values()]
    if portfolio_hits:
        for stock in portfolio_hits:
            name = stock["name"]
            sector = "/".join(PORTFOLIO_SECTOR_KEYWORDS.get(stock["code"], [])[:2])
            if sentiment in ("利好", "偏多"):
                impacts.append(f"持仓{name}({sector})受利好催化，关注是否放量突破")
            elif sentiment in ("利空", "偏空"):
                impacts.append(f"持仓{name}({sector})承压，注意止损纪律")
            else:
                impacts.append(f"持仓{name}({sector})相关，留意盘面反应")

    # 2. 板块层面影响
    sector_keywords = {
        "证券": ["东方财富", "第一创业"],
        "券商": ["东方财富", "第一创业"],
        "金融": ["东方财富", "第一创业"],
        "传媒": ["电广传媒"],
        "游戏": ["电广传媒"],
        "电力": ["东方电气"],
        "新能源": ["东方电气"],
        "医药": ["人福医药"],
    }
    for sector, stocks in sector_keywords.items():
        if sector in text:
            for s in stocks:
                if not any(p["name"] == s for p in portfolio_hits):
                    impacts.append(f"{sector}板块异动可能影响持仓{s}")

    # 3. 大盘层面
    market_words = ["降息", "降准", "政策", "利好", "资金流入", "北向资金"]
    if any(w in text for w in market_words) and sentiment in ("利好", "偏多"):
        impacts.append("大盘环境偏暖，持仓可继续持有")
    market_neg = ["利空", "暴跌", "闪崩", "贸易战", "制裁"]
    if any(w in text for w in market_neg) and sentiment in ("利空", "偏空"):
        impacts.append("市场风险上升，注意控制仓位")

    return "；".join(impacts) if impacts else ""


# ============================================================
# 主采集流程
# ============================================================

def fetch_today_news() -> dict:
    """采集当日全部新闻 + 板块资金流 + 美股外盘"""
    today = datetime.now().strftime("%Y-%m-%d")
    all_news = {}

    # 1. 新浪7x24
    try:
        items = _fetch_sina_7x24(page_size=60)
        for item in items:
            if item["news_id"] not in all_news:
                all_news[item["news_id"]] = item
        logger.info(f"新浪7x24: {len(items)}条")
    except Exception as e:
        logger.warning(f"新浪7x24失败: {e}")

    # 2. 新浪财经
    try:
        items = _fetch_sina_finance()
        for item in items:
            if item["news_id"] not in all_news:
                all_news[item["news_id"]] = item
        logger.info(f"新浪财经: {len(items)}条")
    except Exception as e:
        logger.warning(f"新浪财经失败: {e}")

    # 3. 东财搜索 (30+关键词)
    for kw in EASTMONEY_KEYWORDS:
        try:
            items = _fetch_eastmoney_search(kw, page_size=8)
            for item in items:
                if item["news_id"] not in all_news:
                    item["portfolio_related"] = kw in PORTFOLIO_MAP
                    all_news[item["news_id"]] = item
            time.sleep(0.15)
        except Exception as e:
            logger.warning(f"东财搜索[{kw}]失败: {e}")

    # 4. 证券时报 (替代网易财经)
    try:
        items = _fetch_stcn_news()
        for item in items:
            if item["news_id"] not in all_news:
                all_news[item["news_id"]] = item
        logger.info(f"证券时报: {len(items)}条")
    except Exception as e:
        logger.warning(f"证券时报失败: {e}")

    # 5. 分析每条新闻
    news_list = list(all_news.values())
    for news in news_list:
        sent = _analyze_sentiment(news["title"], news.get("content", ""))
        news.update(sent)
        news["category"] = _classify_news(news["title"], news.get("content", ""))
        stocks = _match_stocks(news["title"], news.get("content", ""))
        if stocks:
            news["related_stocks"] = stocks
        if "portfolio_related" not in news:
            # 直接匹配: 新闻提到了持仓股票名/别名
            direct_match = len(stocks) > 0 and any(
                s["code"] in PORTFOLIO_MAP.values() for s in stocks
            )
            # 板块关联: 新闻分类跟持仓板块匹配(排除公司名干扰)
            sector_match = False
            if not direct_match:
                text = f"{news['title']} {news.get('content', '')}"
                cat = news.get("category", "")
                for code, sector_kws in PORTFOLIO_SECTOR_KEYWORDS.items():
                    for kw in sector_kws:
                        if kw not in text:
                            continue
                        # 排除: 关键词只出现在公司名/媒体名中(如"东方财富证券""证券时报")
                        clean_text = text
                        for stock_name in PORTFOLIO_MAP:
                            clean_text = clean_text.replace(stock_name, "")
                        for media in ["证券时报", "证券日报", "证券报", "上海证券报", "中国证券报"]:
                            clean_text = clean_text.replace(media, "")
                        if kw not in clean_text:
                            continue
                        # 必须是行业/政策/宏观类新闻
                        if cat not in ("行业", "政策", "宏观", "A股"):
                            continue
                        sector_match = True
                        name = [n for n, c in PORTFOLIO_MAP.items() if c == code][0]
                        if not any(s["code"] == code for s in stocks):
                            if "related_stocks" not in news:
                                news["related_stocks"] = list(stocks)
                            news["related_stocks"].append({"name": name, "code": code, "match": "板块关联"})
                        break
                    if sector_match:
                        break
            news["portfolio_related"] = direct_match or sector_match
        news["trade_impact"] = _analyze_trade_impact(news)

    # 6. 按时间倒序
    news_list.sort(key=lambda x: x.get("date", ""), reverse=True)

    # 7. 板块资金流
    sector_flow = _fetch_sector_fund_flow()

    # 8. 美股外盘
    us_market = _fetch_us_market()

    # 9. 统计
    sentiment_dist = {"利好": 0, "偏多": 0, "中性": 0, "偏空": 0, "利空": 0}
    category_dist = {}
    portfolio_count = 0
    for n in news_list:
        s = n.get("sentiment", "中性")
        sentiment_dist[s] = sentiment_dist.get(s, 0) + 1
        c = n.get("category", "其他")
        category_dist[c] = category_dist.get(c, 0) + 1
        if n.get("portfolio_related"):
            portfolio_count += 1

    snapshot = {
        "date": today,
        "updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "stats": {
            "total": len(news_list),
            "portfolio_related": portfolio_count,
            "sentiment_dist": sentiment_dist,
            "category_dist": category_dist,
            "sources": {
                "sina_7x24": sum(1 for n in news_list if n.get("source") == "新浪7x24"),
                "sina_finance": sum(1 for n in news_list if "新浪" in n.get("source", "") and n.get("source") != "新浪7x24"),
                "eastmoney": sum(1 for n in news_list if n.get("source", "") == "东财" or (n.get("source", "") not in ("新浪7x24", "证券时报") and "新浪" not in n.get("source", ""))),
                "stcn": sum(1 for n in news_list if n.get("source") == "证券时报"),
            },
        },
        "sector_flow": sector_flow,
        "us_market": us_market,
        "news": news_list,
    }

    # 10. 保存
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    TODAY_FILE.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2))
    HISTORY_DIR.mkdir(parents=True, exist_ok=True)
    (HISTORY_DIR / f"{today}.json").write_text(
        json.dumps(snapshot, ensure_ascii=False, indent=2)
    )

    # 11. 同步到DB（含政策新闻）
    try:
        sync_news_to_db(snapshot)
    except Exception as e:
        logger.warning(f"新闻同步DB失败: {e}")

    return snapshot


def load_today_news() -> dict:
    if TODAY_FILE.exists():
        data = json.loads(TODAY_FILE.read_text())
        today = datetime.now().strftime("%Y-%m-%d")
        # 兼容跨日: 凌晨0-9点显示昨日数据(交易日数据到次日早盘前都有效)
        if data.get("date") == today:
            return data
        now_h = datetime.now().hour
        if now_h < 9 and data.get("date"):
            # 凌晨时段, 昨日新闻仍有参考价值
            return data
    return {
        "date": datetime.now().strftime("%Y-%m-%d"),
        "updated": "无数据",
        "stats": {"total": 0, "portfolio_related": 0,
                  "sentiment_dist": {}, "category_dist": {}, "sources": {}},
        "sector_flow": {"industry": [], "concept": []},
        "us_market": {},
        "news": [],
    }


if __name__ == "__main__":
    import click

    @click.group()
    def cli():
        pass

    @cli.command()
    def fetch():
        """手动采集当日新闻"""
        result = fetch_today_news()
        stats = result["stats"]
        click.echo(f"采集完成! 共{stats['total']}条新闻")
        click.echo(f"  持仓相关: {stats['portfolio_related']}条")
        click.echo(f"  情感分布: {stats['sentiment_dist']}")
        click.echo(f"  分类分布: {stats['category_dist']}")
        click.echo(f"  数据源: {stats.get('sources', {})}")
        sf = result.get("sector_flow", {})
        click.echo(f"  板块资金: 行业{len(sf.get('industry',[]))}个, 概念{len(sf.get('concept',[]))}个")
        um = result.get("us_market", {})
        if um:
            parts = [f"{v['name']}{v['change_pct']:+.2f}%" for v in um.values()]
            click.echo(f"  美股外盘: {', '.join(parts)}")

    @cli.command()
    def show():
        """显示当日新闻"""
        data = load_today_news()
        for n in data.get("news", [])[:30]:
            sent = n.get("sentiment", "中性")
            icon = {"利好": "🔴", "偏多": "🟠", "中性": "⚪",
                    "偏空": "🟢", "利空": "🟢"}.get(sent, "⚪")
            port = "⭐" if n.get("portfolio_related") else "  "
            cat = n.get("category", "")
            impact = n.get("trade_impact", "")
            content_preview = n.get("content", "")[:50]
            click.echo(f"{icon}{port} [{n.get('date','')[11:16]}] [{cat:3}] {n['title'][:50]}")
            if content_preview:
                click.echo(f"      摘要: {content_preview}")
            if impact:
                click.echo(f"      影响: {impact[:80]}")

        # 板块资金流
        sf = data.get("sector_flow", {})
        if sf.get("industry"):
            click.echo("\n=== 行业板块TOP10 ===")
            for s in sf["industry"][:10]:
                leader = s.get("leader", "")
                leader_str = f" 领涨:{leader}" if leader else ""
                click.echo(f"  {s['name']}: {s['change_pct']:+.2f}% 成交{s.get('turnover',0):.0f}亿{leader_str}")

        # 美股外盘
        um = data.get("us_market", {})
        if um:
            click.echo("\n=== 美股外盘 ===")
            for code, d in um.items():
                click.echo(f"  {d['name']}: {d['price']} ({d['change_pct']:+.2f}%)")

    cli()


# ============================================================
# 政策新闻采集 + DB同步 + 情绪策略桥接
# ============================================================

# 政策关键词（比 _classify_news 更精准）
POLICY_KEYWORDS = [
    "国务院", "发改委", "证监会", "央行", "财政部", "商务部", "工信部", "科技部",
    "降息", "降准", "LPR", "印花税", "注册制", "IPO", "退市",
    "两会", "政治局", "中央经济", "五年规划", "专项债", "国债",
    "政策支持", "产业政策", "财政政策", "货币政策", "监管新规",
    "重大部署", "战略规划", "十四五", "十五五", "双碳", "新质生产力",
]


def _is_policy_news(title: str, content: str = "") -> bool:
    """判断是否为政策类新闻"""
    text = f"{title} {content}"
    return any(kw in text for kw in POLICY_KEYWORDS)


def _fetch_policy_news_stcn() -> list:
    """采集证券时报政策频道 — 政策第一手来源"""
    items = []
    try:
        # 证券时报 — 要闻/政策频道
        raw = _curl_get("https://news.stcn.com/sd/index.shtml", timeout=15)
        if raw:
            titles = re.findall(r'title="([^"]{10,100})"', raw)
            seen = set()
            for t in titles:
                t = t.strip()
                if len(t) < 10 or t in seen or _is_noise(t):
                    continue
                seen.add(t)
                items.append({
                    "news_id": hashlib.md5(f"stcn_policy|{t}".encode()).hexdigest()[:16],
                    "title": t,
                    "content": "",
                    "date": datetime.now().strftime("%Y-%m-%d"),
                    "source": "证券时报政策",
                    "url": "",
                    "keyword": "政策",
                })
    except Exception as e:
        logger.warning(f"证券时报政策采集失败: {e}")

    try:
        # 证券时报 — 公司/政策栏目
        raw2 = _curl_get("https://company.stcn.com/gsxw/", timeout=15)
        if raw2:
            titles2 = re.findall(r'title="([^"]{10,100})"', raw2)
            for t in titles2:
                t = t.strip()
                if len(t) < 10 or t in seen or _is_noise(t):
                    continue
                if not _is_policy_news(t):
                    continue
                seen.add(t)
                items.append({
                    "news_id": hashlib.md5(f"stcn_company|{t}".encode()).hexdigest()[:16],
                    "title": t,
                    "content": "",
                    "date": datetime.now().strftime("%Y-%m-%d"),
                    "source": "证券时报",
                    "url": "",
                    "keyword": "政策",
                })
    except Exception as e:
        logger.warning(f"证券时报公司频道采集失败: {e}")

    return items[:30]


def _fetch_policy_news_sina() -> list:
    """采集新浪7x24中的政策新闻（从已有数据过滤）"""
    # 新浪7x24已在主流程采集，这里额外拉一遍政策频道
    items = []
    try:
        raw = _curl_get(
            "https://feed.mix.sina.com.cn/api/roll/get?"
            "pageid=155&lid=2509&k=&num=50&page=1&r=0."
            + str(int(time.time())),
            timeout=15,
        )
        if not raw:
            return items
        data = json.loads(raw)
        for item in data.get("result", {}).get("data", []):
            title = item.get("title", "").strip()
            if len(title) < 10 or _is_noise(title):
                continue
            if not _is_policy_news(title):
                continue
            content = item.get("intro", item.get("summary", ""))
            items.append({
                "news_id": hashlib.md5(f"sina_policy|{title}".encode()).hexdigest()[:16],
                "title": title,
                "content": content,
                "date": datetime.now().strftime("%Y-%m-%d"),
                "source": "新浪政策",
                "url": item.get("url", ""),
                "keyword": "政策",
            })
    except Exception as e:
        logger.warning(f"新浪政策采集失败: {e}")
    return items[:20]


def fetch_policy_news() -> list:
    """采集全部政策新闻（多源合并去重）"""
    all_items = []
    seen_titles = set()

    for items in [_fetch_policy_news_stcn(), _fetch_policy_news_sina()]:
        for item in items:
            if item["title"] not in seen_titles:
                seen_titles.add(item["title"])
                # 标记为政策类
                item["category"] = "政策"
                item["is_policy"] = True
                all_items.append(item)

    logger.info(f"政策新闻采集: {len(all_items)}条")
    return all_items


def sync_news_to_db(snapshot: dict = None) -> int:
    """将新闻数据同步到DB（带category/情绪/政策标记）
    
    解决问题: eastmoney_news 采集的新闻只存JSON不存DB,
    导致策略B/因子计算读不到新闻情绪。
    """
    import sqlite3 as _sql

    if snapshot is None:
        snapshot = load_today_news()

    news_list = snapshot.get("news", [])
    if not news_list:
        return 0

    conn = _sql.connect("data/alpha_miner.db")

    # 确保 news 表有 category 和 is_policy 字段
    try:
        conn.execute("ALTER TABLE news ADD COLUMN category TEXT DEFAULT ''")
    except Exception:
        pass
    try:
        conn.execute("ALTER TABLE news ADD COLUMN is_policy INTEGER DEFAULT 0")
    except Exception:
        pass

    # 同时同步政策新闻
    policy_news = fetch_policy_news()
    news_list = news_list + policy_news

    count = 0
    for news in news_list:
        nid = news.get("news_id", "")
        title = news.get("title", "")
        if not nid or not title:
            continue

        # 去重
        existing = conn.execute(
            "SELECT news_id FROM news WHERE news_id = ?", (nid,)
        ).fetchone()
        if existing:
            continue

        content = news.get("content", "")[:2000]
        sentiment = news.get("sentiment_score", 0)
        category = news.get("category", news.get("news_type", "其他"))
        is_policy = 1 if (news.get("is_policy") or _is_policy_news(title, content)) else 0

        # 覆盖category为政策（如果is_policy=1）
        if is_policy:
            category = "政策"

        conn.execute("""
            INSERT OR IGNORE INTO news 
            (news_id, stock_code, title, publish_time, content, 
             sentiment_score, news_type, classify_confidence, category, is_policy)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            nid,
            news.get("stock_code", ""),
            title,
            news.get("date", datetime.now().strftime("%Y-%m-%d")),
            content,
            sentiment,
            news.get("news_type", category),
            news.get("classify_confidence", 0.5),
            category,
            is_policy,
        ))
        count += 1

    conn.commit()
    conn.close()
    logger.info(f"新闻同步到DB: {count}条新增, 其中政策{sum(1 for n in news_list if _is_policy_news(n.get('title',''), n.get('content','')))}条")
    return count


def get_news_sentiment_for_strategy(trade_date: str = None) -> dict:
    """新闻情绪 → 策略B信号桥接
    
    从DB读取新闻情绪，输出策略B可用的信号:
    - overall_sentiment: 综合情绪分数 (-1~1)
    - policy_bullish: 政策利好数
    - policy_bearish: 政策利空数
    - sector_hot: 热门板块关键词
    - portfolio_signal: 持仓关联信号
    
    策略B调用:
        from src.data.sources.eastmoney_news import get_news_sentiment_for_strategy
        signal = get_news_sentiment_for_strategy()
        if signal['policy_bullish'] > 2:
            # 政策利好多，可适当加仓
            position_ratio *= 1.2
    """
    import sqlite3 as _sql

    if trade_date is None:
        trade_date = datetime.now().strftime("%Y-%m-%d")

    conn = _sql.connect("data/alpha_miner.db")

    # 当日新闻
    rows = conn.execute(
        "SELECT title, content, sentiment_score, news_type, category, is_policy "
        "FROM news WHERE publish_time LIKE ?",
        (f"{trade_date}%",)
    ).fetchall()
    conn.close()

    if not rows:
        return {
            "date": trade_date,
            "news_count": 0,
            "overall_sentiment": 0,
            "policy_count": 0,
            "policy_bullish": 0,
            "policy_bearish": 0,
            "portfolio_related": [],
            "sector_hot": [],
            "signal": "neutral",
            "position_adjust": 1.0,
        }

    # 综合情绪
    sentiments = [r[2] for r in rows if r[2] is not None and r[2] != 0]
    overall = sum(sentiments) / len(sentiments) if sentiments else 0

    # 政策新闻
    policy_rows = [r for r in rows if r[5] == 1 or r[4] == "政策" or _is_policy_news(r[0], r[1] or "")]
    
    # 政策利好/利空
    bullish_words = ["支持", "利好", "促进", "鼓励", "补贴", "减税", "降息", "降准",
                     "放松", "开放", "振兴", "扶持", "激励", "专项"]
    bearish_words = ["收紧", "限制", "监管", "处罚", "禁止", "叫停", "严控", "整顿",
                     "加息", "缩减", "退市", "核查"]

    policy_bullish = 0
    policy_bearish = 0
    for r in policy_rows:
        text = f"{r[0]} {r[1] or ''}"
        if any(w in text for w in bullish_words):
            policy_bullish += 1
        if any(w in text for w in bearish_words):
            policy_bearish += 1

    # 热门板块（从行业类新闻提取）
    sector_keywords = set()
    for r in rows:
        cat = r[4] or r[3] or ""
        title = r[0]
        if cat in ("行业", "政策"):
            for kw in ["券商", "AI", "半导体", "新能源", "医药", "电力", "传媒",
                       "军工", "消费", "地产", "银行", "汽车", "光伏", "锂电池"]:
                if kw in title:
                    sector_keywords.add(kw)

    # 综合信号
    if overall > 0.2 and policy_bullish >= 2:
        signal = "bullish"
        adjust = 1.2
    elif overall > 0.1:
        signal = "slightly_bullish"
        adjust = 1.1
    elif overall < -0.2 and policy_bearish >= 2:
        signal = "bearish"
        adjust = 0.8
    elif overall < -0.1:
        signal = "slightly_bearish"
        adjust = 0.9
    else:
        signal = "neutral"
        adjust = 1.0

    return {
        "date": trade_date,
        "news_count": len(rows),
        "overall_sentiment": round(overall, 4),
        "policy_count": len(policy_rows),
        "policy_bullish": policy_bullish,
        "policy_bearish": policy_bearish,
        "portfolio_related": [],
        "sector_hot": list(sector_keywords),
        "signal": signal,
        "position_adjust": adjust,
    }
