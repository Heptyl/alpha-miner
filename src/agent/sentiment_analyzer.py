"""新闻情感分析器 — LLM 7级评分 + 来源可信度加权

功能:
  1. 对新闻标题/正文做LLM情感分析, 7级评分(-1到+1)
  2. 来源可信度权重: 交易所公告>央行/财政部>主流财经媒体>门户网站>自媒体
  3. 信号映射: >=0.3看多, <=-0.3看空, 中间中性
  4. 写入news表sentiment_score字段(新范围-1到+1)
  5. 个股情感汇总: 近N天加权平均情感分

用法:
  from src.agent.sentiment_analyzer import SentimentAnalyzer
  analyzer = SentimentAnalyzer()
  analyzer.analyze_and_store(stock_code="600519", days=7)
  summary = analyzer.get_stock_sentiment("600519", days=30)
"""

from __future__ import annotations

import json
import logging
import os
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

DB_PATH = Path(__file__).resolve().parents[2] / "data" / "alpha_miner.db"

logger = logging.getLogger(__name__)

# ── 7级评分标准 ──
SENTIMENT_LEVELS = {
    "+1.0": "极其积极 — 重大利好(超预期业绩、重大政策支持、大型订单)",
    "+0.5~+0.9": "积极 — 业绩增长、新项目、订单获取",
    "+0.1~+0.4": "轻微积极 — 小合同、正常经营正面",
    "0.0": "中性 — 常规公告、人事变动",
    "-0.1~-0.4": "轻微消极 — 小额诉讼、非核心业务亏损",
    "-0.5~-0.9": "消极 — 业绩下滑、客户流失、政策收紧",
    "-1.0": "极其消极 — 重大违规、核心业务亏损、监管处罚",
}

# ── 来源可信度权重 ──
SOURCE_CREDIBILITY = {
    "交易所公告": 1.0,
    "证监会": 1.0,
    "央行": 0.95,
    "财政部": 0.95,
    "发改委": 0.9,
    "财新": 0.85,
    "证券时报": 0.85,
    "上证报": 0.85,
    "中国证券报": 0.85,
    "21世纪经济报道": 0.8,
    "经济观察报": 0.8,
    "第一财经": 0.8,
    "新浪财经": 0.7,
    "东方财富": 0.7,
    "同花顺": 0.7,
    "腾讯财经": 0.65,
    "网易财经": 0.6,
    "搜狐财经": 0.6,
    "自媒体": 0.4,
    "股吧": 0.3,
    "雪球": 0.4,
}

DEFAULT_CREDIBILITY = 0.6

# ── 信号映射 ──
BULLISH_THRESHOLD = 0.3
BEARISH_THRESHOLD = -0.3


@dataclass
class SentimentResult:
    score: float        # -1 to +1
    signal: str         # "bullish" / "bearish" / "neutral"
    confidence: float   # 0 to 1
    reasoning: str
    method: str         # "llm" / "rule"


def _get_signal(score: float) -> str:
    if score >= BULLISH_THRESHOLD:
        return "bullish"
    elif score <= BEARISH_THRESHOLD:
        return "bearish"
    return "neutral"


def _get_credibility(title: str, content: str = "") -> float:
    """根据新闻来源关键词判断可信度权重"""
    text = title + " " + content[:200]
    for source, weight in SOURCE_CREDIBILITY.items():
        if source in text:
            return weight
    return DEFAULT_CREDIBILITY


# ── LLM Prompt ──
SYSTEM_PROMPT = """你是A股新闻情感分析专家。请对以下新闻进行情感评分。

评分标准(7级):
  +1.0: 极其积极 — 重大利好(超预期业绩、重大政策支持、大型订单)
  +0.5~+0.9: 积极 — 业绩增长、新项目、订单获取
  +0.1~+0.4: 轻微积极 — 小合同、正常经营正面消息
  0.0: 中性 — 常规公告、人事变动、无实质影响
  -0.1~-0.4: 轻微消极 — 小额诉讼、非核心业务亏损
  -0.5~-0.9: 消极 — 业绩下滑、客户流失、政策收紧
  -1.0: 极其消极 — 重大违规、核心业务亏损、监管处罚

分析维度:
  1. 业绩相关: 财报、业绩预告、营收利润变化
  2. 政策影响: 行业政策、监管变化
  3. 市场表现: 市占率、竞争格局
  4. 资本运作: 并购、定增、股权激励
  5. 风险事件: 诉讼、处罚、债务危机
  6. 行业地位: 技术突破、专利、市场份额

来源可信度考量:
  交易所公告/证监会 > 央行/财政部 > 财新/证券时报等主流媒体 > 一般门户网站 > 自媒体/论坛

输出格式(JSON):
{
  "score": <float, -1到+1>,
  "signal": "<bullish/bearish/neutral>",
  "confidence": <float, 0到1>,
  "reasoning": "<一句话中文分析>"
}"""


def _create_llm_client():
    """创建LLM客户端(委托给统一LLM客户端)"""
    from src.agent.llm_client import get_client
    c = get_client()
    return c.get_anthropic_client()


class SentimentAnalyzer:
    """新闻情感分析器"""

    # 规则关键词 — 高置信度时不调LLM
    RULE_POSITIVE = [
        "业绩预增", "净利润增长", "营收增长", "中标", "签约", "订单",
        "获批", "投产", "涨停", "增持", "回购", "超预期", "业绩大增",
        "重大合同", "战略合作", "突破", "创新高",
    ]
    RULE_NEGATIVE = [
        "处罚", "违规", "退市", "ST", "立案调查", "暴雷", "减持",
        "亏损", "业绩预减", "下修", "警示", "诉讼", "债务违约",
        "暴跌", "跌停", "被查", "监管函",
    ]
    # 常规数据报告标题模式 — 直接标中性, 不调LLM
    NOISE_PATTERNS = [
        r"资金流向?日报",
        r"行业.*资金流入?榜",
        r".*龙虎榜.*席位.*详情",
        r".*龙虎榜.*净[买](?:入|卖)额?前\d+",
        r".*[大中小]单.*净流[出入]",
        r"主力动向[：:]",
        r"活跃股获主力",
        r"资金出逃股",
        r".*\d+日.*[买](?:入|卖)详情",
        r"北向资金.*日报",
    ]

    def __init__(self, llm_client=None, model: str = "glm-4-plus"):
        self.llm_client = llm_client
        self.model = model
        if not self.llm_client:
            self.llm_client, self.model = _create_llm_client()

    def analyze(self, title: str, content: str = "",
                stock_code: str = "") -> SentimentResult:
        """分析单条新闻情感

        优先规则匹配(快速), 置信度<0.7时调LLM
        """
        # Step 1: 规则匹配
        rule_result = self._rule_analyze(title, content)
        if rule_result.confidence >= 0.7:
            return rule_result

        # Step 2: LLM分析
        if self.llm_client:
            return self._llm_analyze(title, content, stock_code)

        return rule_result

    def analyze_batch(self, items: list[dict]) -> list[SentimentResult]:
        """批量分析"""
        return [self.analyze(i.get("title", ""), i.get("content", ""),
                             i.get("stock_code", "")) for i in items]

    def analyze_and_store(self, stock_code: str = "", days: int = 7,
                          force: bool = False) -> int:
        """对指定范围的新闻做情感分析并写回DB

        Args:
            stock_code: 股票代码(空=全部)
            days: 分析最近N天的新闻
            force: 是否强制重新分析(已分析的也重做)

        Returns:
            更新的新闻条数
        """
        conn = sqlite3.connect(str(DB_PATH))
        conn.row_factory = sqlite3.Row
        try:
            since = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

            conditions = ["publish_time >= ?"]
            params: list = [since]

            if stock_code:
                conditions.append("stock_code = ?")
                params.append(stock_code)

            if not force:
                conditions.append("(sentiment_score IS NULL OR (sentiment_score >= 0 AND sentiment_score <= 1))")

            where = " AND ".join(conditions)
            news = conn.execute(
                f"SELECT news_id, stock_code, title, content FROM news WHERE {where} ORDER BY publish_time DESC",
                params,
            ).fetchall()

            if not news:
                logger.info(f"[情感分析] 无新增新闻需要处理")
                return 0

            logger.info(f"[情感分析] 待分析{len(news)}条新闻")

            updated = 0
            for news_item in news:
                title = news_item["title"] or ""
                content = news_item["content"] or ""
                if not title and not content:
                    continue

                result = self.analyze(title, content[:500], news_item["stock_code"] or "")

                conn.execute(
                    "UPDATE news SET sentiment_score = ? WHERE news_id = ?",
                    (result.score, news_item["news_id"]),
                )
                updated += 1

                if updated % 50 == 0:
                    conn.commit()
                    logger.info(f"[情感分析] 已处理{updated}/{len(news)}")

            conn.commit()
            logger.info(f"[情感分析] 完成, 更新{updated}条新闻")
            return updated

        finally:
            conn.close()

    def get_stock_sentiment(self, stock_code: str, days: int = 30) -> dict:
        """获取个股情感汇总(加权平均)

        Returns:
            {
                "avg_score": float,       # 加权平均情感分
                "weighted_score": float,   # 来源可信度加权后的分数
                "total_news": int,
                "bullish_count": int,
                "bearish_count": int,
                "neutral_count": int,
                "top_positive": list,      # 最看多的3条
                "top_negative": list,      # 最看空的3条
            }
        """
        conn = sqlite3.connect(str(DB_PATH))
        conn.row_factory = sqlite3.Row
        try:
            since = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
            rows = conn.execute(
                """SELECT news_id, title, sentiment_score, publish_time
                   FROM news
                   WHERE stock_code = ? AND publish_time >= ?
                     AND sentiment_score IS NOT NULL
                   ORDER BY publish_time DESC""",
                (stock_code, since),
            ).fetchall()

            if not rows:
                return {"avg_score": 0, "weighted_score": 0, "total_news": 0,
                        "bullish_count": 0, "bearish_count": 0, "neutral_count": 0,
                        "top_positive": [], "top_negative": []}

            scores = []
            weighted_scores = []
            signals = {"bullish": 0, "bearish": 0, "neutral": 0}

            for row in rows:
                score = row["sentiment_score"]
                if score is None:
                    continue

                scores.append(score)
                cred = _get_credibility(row["title"] or "")
                weighted_scores.append(score * cred)
                signals[_get_signal(score)] += 1

            if not scores:
                return {"avg_score": 0, "weighted_score": 0, "total_news": len(rows),
                        "bullish_count": 0, "bearish_count": 0, "neutral_count": 0,
                        "top_positive": [], "top_negative": []}

            avg = sum(scores) / len(scores)
            wavg = sum(weighted_scores) / len(weighted_scores) if weighted_scores else 0

            top_pos = sorted(
                [{"title": r["title"], "score": r["sentiment_score"]}
                 for r in rows if r["sentiment_score"] and r["sentiment_score"] > 0.3],
                key=lambda x: x["score"], reverse=True,
            )[:3]

            top_neg = sorted(
                [{"title": r["title"], "score": r["sentiment_score"]}
                 for r in rows if r["sentiment_score"] and r["sentiment_score"] < -0.3],
                key=lambda x: x["score"],
            )[:3]

            return {
                "avg_score": round(avg, 3),
                "weighted_score": round(wavg, 3),
                "total_news": len(scores),
                "bullish_count": signals["bullish"],
                "bearish_count": signals["bearish"],
                "neutral_count": signals["neutral"],
                "top_positive": top_pos,
                "top_negative": top_neg,
            }

        finally:
            conn.close()

    def _rule_analyze(self, title: str, content: str) -> SentimentResult:
        """基于关键词的规则分析"""
        # 常规数据报告直接标中性
        for pat in self.NOISE_PATTERNS:
            if re.search(pat, title):
                return SentimentResult(score=0.0, signal="neutral",
                                       confidence=0.9, method="rule",
                                       reasoning="常规数据报告, 标中性")

        text = title + " " + content

        pos_hits = sum(1 for kw in self.RULE_POSITIVE if kw in text)
        neg_hits = sum(1 for kw in self.RULE_NEGATIVE if kw in text)

        if pos_hits > 0 and neg_hits == 0:
            score = min(0.3 + 0.2 * (pos_hits - 1), 0.9)
            confidence = min(0.7 + 0.1 * pos_hits, 0.95)
            return SentimentResult(score=score, signal=_get_signal(score),
                                   confidence=confidence, method="rule",
                                   reasoning=f"规则匹配{pos_hits}个正面关键词")

        if neg_hits > 0 and pos_hits == 0:
            score = max(-0.3 - 0.2 * (neg_hits - 1), -0.9)
            confidence = min(0.7 + 0.1 * neg_hits, 0.95)
            return SentimentResult(score=score, signal=_get_signal(score),
                                   confidence=confidence, method="rule",
                                   reasoning=f"规则匹配{neg_hits}个负面关键词")

        if pos_hits > 0 and neg_hits > 0:
            net = pos_hits - neg_hits
            score = 0.1 * net
            return SentimentResult(score=score, signal=_get_signal(score),
                                   confidence=0.5, method="rule",
                                   reasoning=f"正{pos_hits}负{neg_hits}关键词冲突")

        return SentimentResult(score=0.0, signal="neutral",
                               confidence=0.3, method="rule",
                               reasoning="无关键词命中")

    def _llm_analyze(self, title: str, content: str,
                     stock_code: str) -> SentimentResult:
        """LLM情感分析"""
        user_prompt = f"股票代码: {stock_code or '未知'}\n标题: {title}\n正文: {content[:300]}"
        try:
            response = self.llm_client.messages.create(
                model=self.model,
                max_tokens=200,
                temperature=0.1,
                system=SYSTEM_PROMPT,
                messages=[{"role": "user", "content": user_prompt}],
            )
            text = response.content[0].text

            # 清理JSON
            if "```json" in text:
                text = text.split("```json")[1].split("```")[0]
            elif "```" in text:
                text = text.split("```")[1].split("```")[0]

            data = json.loads(text.strip())
            score = float(data.get("score", 0))
            score = max(-1.0, min(1.0, score))

            return SentimentResult(
                score=score,
                signal=data.get("signal", _get_signal(score)),
                confidence=float(data.get("confidence", 0.5)),
                reasoning=data.get("reasoning", ""),
                method="llm",
            )
        except Exception as e:
            logger.warning("LLM情感分析失败: %s", str(e)[:100])
            return self._rule_analyze(title, content)


def score_sentiment_signal(conn: sqlite3.Connection, stock_code: str,
                           days: int = 30) -> tuple[int, dict]:
    """精选评分卡中的情感因子评分(满分10分)

    用于 fundamental_scorer.py 的 score_signals() 中。
    基于近N天新闻的加权平均情感分:
      weighted_score >= 0.5: +10分(强看多)
      weighted_score >= 0.3: +7分
      weighted_score >= 0.1: +4分
      weighted_score <= -0.3: +0分(看空)
      其他: +2分(中性)
    """
    since = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

    rows = conn.execute(
        """SELECT sentiment_score, title FROM news
           WHERE stock_code = ? AND publish_time >= ?
             AND sentiment_score IS NOT NULL""",
        (stock_code, since),
    ).fetchall()

    if not rows:
        return 0, {"sentiment_score": 0, "sentiment_count": 0, "sentiment_signal": "无数据"}

    scores = []
    for r in rows:
        s = r["sentiment_score"]
        if s is not None:
            cred = _get_credibility(r["title"] or "")
            scores.append((s, cred))

    if not scores:
        return 0, {"sentiment_score": 0, "sentiment_count": 0, "sentiment_signal": "无数据"}

    weighted = sum(s * c for s, c in scores) / sum(c for _, c in scores)
    avg = sum(s for s, _ in scores) / len(scores)

    if weighted >= 0.5:
        pts = 10
    elif weighted >= 0.3:
        pts = 7
    elif weighted >= 0.1:
        pts = 4
    elif weighted <= -0.3:
        pts = 0
    else:
        pts = 2

    return pts, {
        "sentiment_avg": round(avg, 3),
        "sentiment_weighted": round(weighted, 3),
        "sentiment_count": len(scores),
        "sentiment_signal": _get_signal(avg),
    }


if __name__ == "__main__":
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="新闻情感分析器")
    parser.add_argument("--analyze", action="store_true", help="分析最近N天新闻")
    parser.add_argument("--stock", type=str, help="指定股票代码")
    parser.add_argument("--days", type=int, default=7, help="分析天数(默认7)")
    parser.add_argument("--force", action="store_true", help="强制重新分析")
    parser.add_argument("--summary", type=str, help="查看个股情感汇总(股票代码)")
    args = parser.parse_args()

    analyzer = SentimentAnalyzer()

    if args.analyze:
        count = analyzer.analyze_and_store(
            stock_code=args.stock or "",
            days=args.days,
            force=args.force,
        )
        print(f"更新{count}条新闻情感分")

    if args.summary:
        result = analyzer.get_stock_sentiment(args.summary, days=30)
        print(f"\n{args.summary} 情感汇总(近30天):")
        print(f"  新闻数: {result['total_news']}")
        print(f"  平均情感: {result['avg_score']:+.3f}")
        print(f"  加权情感: {result['weighted_score']:+.3f}")
        print(f"  看多/看空/中性: {result['bullish_count']}/{result['bearish_count']}/{result['neutral_count']}")
        if result['top_positive']:
            print(f"  最正面:")
            for t in result['top_positive']:
                print(f"    {t['score']:+.2f} {t['title'][:50]}")
        if result['top_negative']:
            print(f"  最负面:")
            for t in result['top_negative']:
                print(f"    {t['score']:+.2f} {t['title'][:50]}")

    if not args.analyze and not args.summary:
        parser.print_help()
