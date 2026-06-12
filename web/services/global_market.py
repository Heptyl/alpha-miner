"""全球市场数据服务 — 外围行情 + 商品 + 汇率 + 恐慌指数

数据源:
  1. akshare stock_info_global_em  — 全球财经新闻(东财7x24)
  2. akshare index_global_hist_em  — 全球指数历史
  3. akshare currency_latest       — 汇率
  4. akshare energy_oil_hist       — 原油
  5. akshare macro_cons_gold       — 黄金
  6. 本地DB daily_price            — A股指数兜底

设计参考:
  - FinnewsHunter: 多源聚合 + 实时推送
  - stock-sentiment-cn: 多数据源融合
  - FICC-trading-assistant: 宏观盘面监控
"""

import json
import logging
import sqlite3
import time
from datetime import datetime, timedelta
from pathlib import Path

import akshare as ak
import pandas as pd

log = logging.getLogger(__name__)

ROOT = Path(__file__).parent.parent.parent  # alpha-miner/
DB_PATH = ROOT / "data" / "alpha_miner.db"
CACHE_PATH = ROOT / "data" / "global_market.json"
CACHE_TTL = 300  # 5分钟缓存


# ────────────── 全球关键指数映射 ──────────────

GLOBAL_INDICES = {
    # 美股
    "道琼斯": {"symbol": "DJI", "region": "美股", "color": "#ff4b4b"},
    "标普500": {"symbol": "SPX", "region": "美股", "color": "#ff6b6b"},
    "纳斯达克": {"symbol": "IXIC", "region": "美股", "color": "#ff8c00"},
    # 亚太
    "恒生指数": {"symbol": "HSI", "region": "港股", "color": "#ffd93d"},
    "日经225": {"symbol": "N225", "region": "日本", "color": "#00d4aa"},
    "韩国KOSPI": {"symbol": "KS11", "region": "韩国", "color": "#4fc3f7"},
    "富时新加坡": {"symbol": "STI", "region": "新加坡", "color": "#81c784"},
    # 欧洲
    "富时100": {"symbol": "FTSE", "region": "英国", "color": "#ab47bc"},
    "德国DAX": {"symbol": "GDAXI", "region": "德国", "color": "#7e57c2"},
    "法国CAC40": {"symbol": "FCHI", "region": "法国", "color": "#5c6bc0"},
}

A_STOCK_INDICES = {
    "上证指数": "000001",
    "深证成指": "399001",
    "创业板指": "399006",
    "沪深300": "000300",
    "中证500": "000905",
    "科创50": "000688",
}


# ────────────── 数据采集 ──────────────

def fetch_global_news() -> list[dict]:
    """东财7x24全球财经新闻。"""
    try:
        df = ak.stock_info_global_em()
        if df is None or df.empty:
            return []
        news = []
        for _, row in df.head(50).iterrows():
            title = str(row.get("标题", ""))
            summary = str(row.get("摘要", ""))
            pub_time = str(row.get("发布时间", ""))
            url = str(row.get("链接", ""))
            news.append({
                "title": title,
                "summary": summary,
                "time": pub_time,
                "url": url,
                "category": _classify_global_news(title + " " + summary),
                "sentiment": _quick_sentiment(title + " " + summary),
                "impact": _assess_impact(title + " " + summary),
            })
        return news
    except Exception as e:
        log.warning("全球新闻采集失败: %s", e)
        return []


def fetch_a_stock_indices() -> list[dict]:
    """A股主要指数。优先从实时行情取，DB兜底。"""
    results = []

    # 优先从顶部实时行情获取
    try:
        from services.realtime import fetch as rt_fetch
        codes_map = {
            "上证指数": "000001", "深证成指": "399001", "创业板指": "399006",
            "沪深300": "000300", "中证500": "000905", "科创50": "000688",
        }
        codes_list = list(codes_map.values())
        quotes = rt_fetch(codes_list)
        for name, code in codes_map.items():
            q = quotes.get(code)
            if q and q.price > 100:  # 合理价格范围
                results.append({
                    "name": name, "code": code,
                    "close": round(q.price, 2),
                    "pct": round(q.pct, 2),
                    "high": round(q.high, 2),
                    "low": round(q.low, 2),
                    "volume": q.volume,
                    "date": q.date,
                })
        if results:
            return results
    except Exception:
        pass

    # DB兜底
    conn = sqlite3.connect(str(DB_PATH))
    for name, code in A_STOCK_INDICES.items():
        try:
            row = conn.execute(
                "SELECT trade_date, open, high, low, close, volume "
                "FROM daily_price WHERE stock_code=? "
                "ORDER BY trade_date DESC LIMIT 1", (code,)
            ).fetchone()
            if row:
                prev_row = conn.execute(
                    "SELECT close FROM daily_price WHERE stock_code=? "
                    "AND trade_date<? ORDER BY trade_date DESC LIMIT 1",
                    (code, row[0])
                ).fetchone()
                pre_close = prev_row[0] if prev_row else row[4]
                pct = (row[4] - pre_close) / pre_close * 100 if pre_close else 0
                results.append({
                    "name": name, "code": code,
                    "close": round(row[4], 2),
                    "pct": round(pct, 2),
                    "high": round(row[3], 2),
                    "low": round(row[2], 2),
                    "volume": row[5],
                    "date": row[0],
                })
        except Exception:
            pass
    conn.close()
    return results


def fetch_global_indices_snapshot(global_news: list[dict] = None) -> list[dict]:
    """从东财全球新闻中提取美股/外围收盘信息。"""
    news = global_news or fetch_global_news()
    market_news = []
    for n in news:
        title = n["title"]
        # 提取包含关键指数/商品的新闻
        market_kws = ["道琼斯", "标普", "纳斯达克", "恒生", "日经", "富时", "DAX",
                      "原油", "黄金", "美元", "美联储", "非农", "CPI", "降息", "加息",
                      "英伟达", "特斯拉", "苹果", "AMD", "英特尔", "高通", "阿斯麦",
                      "原油", "布伦特", "WTI", "铜", "比特币"]
        if any(kw in title for kw in market_kws):
            market_news.append(n)
    return market_news


def fetch_commodity_snapshot() -> dict:
    """商品快照（从新闻中提取价格信息+DB兜底）。"""
    try:
        # 尝试快速获取汇率
        df = ak.currency_latest()
        usd_cny = None
        if df is not None and not df.empty:
            for _, row in df.iterrows():
                name = str(row.iloc[0]) if len(row) > 0 else ""
                if "美元/人民币" in name or "美元人民币" in name:
                    usd_cny = float(row.iloc[1]) if len(row) > 1 else None
                    break
        return {"usd_cny": usd_cny}
    except Exception as e:
        log.warning("汇率获取失败: %s", e)
        return {"usd_cny": None}


# ────────────── 分类/情感/影响分析 ──────────────

def _classify_global_news(text: str) -> str:
    """新闻分类。"""
    categories = {
        "美股行情": ["美股", "道琼斯", "标普", "纳斯达克", "英伟达", "特斯拉", "苹果",
                     "AMD", "英特尔", "高通", "Meta", "微软", "谷歌", "亚马逊"],
        "美联储/利率": ["美联储", "降息", "加息", "利率", "CPI", "非农", "就业", "通胀"],
        "地缘政治": ["战争", "冲突", "制裁", "谈判", "军事", "伊朗", "俄罗斯", "乌克兰",
                    "朝鲜", "台海", "霍尔木兹", "红海"],
        "商品/大宗": ["原油", "黄金", "铜", "铁矿石", "煤炭", "天然气", "布伦特", "WTI",
                     "比特币", "锂", "镍"],
        "港股/亚太": ["港股", "恒生", "日经", "韩国", "印度", "东南亚", "日本央行"],
        "欧洲市场": ["欧洲", "德国", "法国", "英国", "DAX", "富时", "阿斯麦"],
        "宏观政策": ["GDP", "PMI", "进出口", "贸易", "关税", "财政", "货币政策"],
        "行业产业": ["芯片", "半导体", "AI", "新能源", "医药", "消费", "汽车", "机器人",
                    "光模块", "HBM", "GPU"],
    }
    for cat, kws in categories.items():
        if any(kw in text for kw in kws):
            return cat
    return "其他"


def _quick_sentiment(text: str) -> str:
    """快速情感判断（基于关键词）。"""
    positive_kws = ["上涨", "涨超", "大涨", "飙升", "创新高", "利好", "突破",
                    "超预期", "增长", "盈利", "复苏", "反弹", "回暖", "强劲"]
    negative_kws = ["下跌", "暴跌", "崩盘", "衰退", "风险", "危机", "制裁",
                    "冲突", "战争", "恐慌", "暴跌", "跌超", "下调", "亏损",
                    "违约", "封锁", "紧张", "承压", "疲软", "降级"]
    pos = sum(1 for kw in positive_kws if kw in text)
    neg = sum(1 for kw in negative_kws if kw in text)
    if pos > neg:
        return "偏多"
    elif neg > pos:
        return "偏空"
    return "中性"


def _assess_impact(text: str) -> str:
    """评估对A股影响方向。"""
    high_impact_kws = ["美联储", "降息", "加息", "非农", "CPI", "地缘", "战争",
                       "制裁", "关税", "GDP", "PMI", "黑天鹅"]
    medium_kws = ["美股", "欧洲", "原油", "黄金", "美元", "日元", "港股"]
    low_kws = ["个股", "公司", "财报"]

    if any(kw in text for kw in high_impact_kws):
        return "高"
    elif any(kw in text for kw in medium_kws):
        return "中"
    return "低"


# ────────────── 市场情绪综合评分 ──────────────

def compute_market_mood(global_news: list[dict]) -> dict:
    """计算综合市场情绪。

    参考:
      - fear-greed index 概念
      - 多维度加权(新闻情绪+外围走势+商品)
    """
    if not global_news:
        return {"score": 50, "label": "中性", "detail": "无数据"}

    # 新闻情绪统计
    sent_counts = {"偏多": 0, "偏空": 0, "中性": 0}
    impact_scores = {"高": 0, "中": 0, "低": 0}
    categories = {}

    for n in global_news:
        s = n.get("sentiment", "中性")
        sent_counts[s] = sent_counts.get(s, 0) + 1

        imp = n.get("impact", "低")
        impact_scores[imp] = impact_scores.get(imp, 0) + 1

        cat = n.get("category", "其他")
        if cat not in categories:
            categories[cat] = {"count": 0, "positive": 0, "negative": 0}
        categories[cat]["count"] += 1
        if s == "偏多":
            categories[cat]["positive"] += 1
        elif s == "偏空":
            categories[cat]["negative"] += 1

    total = len(global_news)
    positive_ratio = sent_counts["偏多"] / total if total else 0
    negative_ratio = sent_counts["偏空"] / total if total else 0

    # 综合评分 0-100 (50为中性)
    score = 50 + (positive_ratio - negative_ratio) * 60
    # 高影响新闻加权
    if impact_scores.get("高", 0) > 0:
        high_neg = sum(1 for n in global_news
                       if n.get("impact") == "高" and n.get("sentiment") == "偏空")
        score -= high_neg * 3
    score = max(0, min(100, score))

    if score >= 70:
        label = "乐观"
    elif score >= 55:
        label = "偏乐观"
    elif score >= 45:
        label = "中性"
    elif score >= 30:
        label = "偏谨慎"
    else:
        label = "谨慎"

    # 找到最热的主题
    top_cats = sorted(categories.items(), key=lambda x: -x[1]["count"])[:5]

    return {
        "score": round(score),
        "label": label,
        "positive_ratio": round(positive_ratio * 100, 1),
        "negative_ratio": round(negative_ratio * 100, 1),
        "total_news": total,
        "top_categories": [
            {"name": c, "count": v["count"],
             "positive": v["positive"], "negative": v["negative"]}
            for c, v in top_cats
        ],
        "high_impact_count": impact_scores.get("高", 0),
    }


# ────────────── A股操作建议生成 ──────────────

def generate_trading_guidance(mood: dict, global_news: list[dict],
                              a_indices: list[dict]) -> dict:
    """基于综合情绪+外围+A股状态生成操作建议。

    参考 stock-sentiment-cn 的AI驱动分析思路,
    但用规则引擎实现(避免LLM延迟)。
    """
    score = mood.get("score", 50)
    total = mood.get("total_news", 0)
    high_impact = mood.get("high_impact_count", 0)

    # A股趋势
    a_trend = "unknown"
    if a_indices:
        main_idx = next((i for i in a_indices if "上证" in i["name"]), None)
        if main_idx:
            a_trend = "up" if main_idx["pct"] > 0.3 else "down" if main_idx["pct"] < -0.3 else "flat"

    # 生成建议
    if score >= 70 and a_trend in ("up", "flat"):
        stance = "积极做多"
        suggestion = "外围利好叠加A股偏强，可适当加仓，优先关注外资偏好蓝筹和热点题材。"
        risk_level = "低"
    elif score >= 55 and a_trend != "down":
        stance = "偏多操作"
        suggestion = "外围环境中性偏好，可持股为主，短线可跟热点但注意仓位控制。"
        risk_level = "中低"
    elif score >= 45:
        stance = "观望为主"
        suggestion = "外围信号不明，A股震荡概率大。建议持股不动，等待方向明确。"
        risk_level = "中"
    elif score >= 30:
        stance = "谨慎防御"
        suggestion = "外围偏空信号较多，建议减仓或对冲，关注防御性板块(医药/公用/高股息)。"
        risk_level = "中高"
    else:
        stance = "空仓/轻仓"
        suggestion = "外围重大利空，建议大幅减仓或空仓观望，等待风险释放。"
        risk_level = "高"

    # 提取关键风险事件
    risks = []
    for n in global_news:
        if n.get("impact") == "高" and n.get("sentiment") == "偏空":
            risks.append(n["title"][:60])
    risks = risks[:5]

    # 提取潜在机会
    opportunities = []
    for n in global_news:
        if n.get("sentiment") == "偏多" and n.get("impact") in ("高", "中"):
            cat = n.get("category", "")
            if cat in ("行业产业", "美股行情", "宏观政策"):
                opportunities.append(f"[{cat}] {n['title'][:60]}")
    opportunities = opportunities[:5]

    return {
        "stance": stance,
        "suggestion": suggestion,
        "risk_level": risk_level,
        "mood_score": score,
        "mood_label": mood.get("label", "中性"),
        "a_trend": a_trend,
        "key_risks": risks,
        "opportunities": opportunities,
        "updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


# ────────────── 缓存/主入口 ──────────────

def fetch_all(refresh: bool = False) -> dict:
    """获取全部全球市场数据（带缓存）。"""
    # 检查缓存
    if not refresh and CACHE_PATH.exists():
        try:
            cache = json.loads(CACHE_PATH.read_text())
            cached_time = datetime.fromisoformat(cache.get("updated", "2000-01-01"))
            if datetime.now() - cached_time < timedelta(seconds=CACHE_TTL):
                return cache
        except Exception:
            pass

    start = time.time()
    log.info("[global_market] 开始采集全球市场数据...")

    # 并行采集
    global_news = fetch_global_news()
    a_indices = fetch_a_stock_indices()
    commodity = fetch_commodity_snapshot()
    market_indices_news = fetch_global_indices_snapshot(global_news)

    # 分析
    mood = compute_market_mood(global_news)
    guidance = generate_trading_guidance(mood, global_news, a_indices)

    result = {
        "updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "global_news": global_news,
        "market_indices_news": market_indices_news,
        "a_indices": a_indices,
        "commodity": commodity,
        "mood": mood,
        "guidance": guidance,
    }

    # 缓存
    CACHE_PATH.write_text(json.dumps(result, ensure_ascii=False, indent=2))
    elapsed = time.time() - start
    log.info("[global_market] 完成: %d条新闻, 情绪%d(%s), 耗时%.1fs",
             len(global_news), mood["score"], mood["label"], elapsed)

    return result
