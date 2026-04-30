"""新闻采集 — akshare stock_news_em + snownlp 情感分析。"""

import hashlib
import logging
import time
from datetime import datetime

import akshare as ak
import pandas as pd
from snownlp import SnowNLP

from src.data.storage import Storage

logger = logging.getLogger(__name__)


def _make_news_id(title: str, publish_time: str) -> str:
    """用 title + publish_time 的 hash 生成 news_id 去重。"""
    raw = f"{title}|{publish_time}"
    return hashlib.md5(raw.encode("utf-8")).hexdigest()[:16]


def fetch(stock_code: str = "", trade_date: str = "", retries: int = 3) -> pd.DataFrame:
    """拉取新闻数据。

    Args:
        stock_code: 股票代码，空则拉市场热点新闻
        trade_date: 交易日期 YYYY-MM-DD
        retries: 重试次数

    Returns:
        DataFrame: news_id, stock_code, title, publish_time, content, sentiment_score
    """
    for attempt in range(retries):
        try:
            if stock_code:
                df = ak.stock_news_em(symbol=stock_code)
            else:
                # 无单股票参数时拉全市场热点
                df = ak.stock_news_em(symbol="")

            if df is None or df.empty:
                return pd.DataFrame()

            # 构造结果
            result = pd.DataFrame()
            result["title"] = df.get("新闻标题", df.iloc[:, 0]).astype(str).values
            result["publish_time"] = df.get("发布时间", df.iloc[:, 1] if df.shape[1] > 1 else "").astype(str).values
            result["content"] = df.get("新闻内容", df.get("新闻标题", "")).astype(str).values
            result["stock_code"] = stock_code

            # 生成 news_id
            result["news_id"] = result.apply(
                lambda row: _make_news_id(row["title"], str(row["publish_time"])),
                axis=1,
            )

            # 情感分析（snownlp）
            result["sentiment_score"] = result["content"].apply(_sentiment)

            # 新闻分类（规则引擎，不调 LLM）
            _classify_news_inplace(result)

            # 过滤日期
            if trade_date:
                result["publish_time"] = pd.to_datetime(result["publish_time"], errors="coerce")
                result = result.dropna(subset=["publish_time"])
                if not result.empty:
                    result = result[result["publish_time"].dt.strftime("%Y-%m-%d") == trade_date]

            return result

        except Exception as e:
            if attempt < retries - 1:
                time.sleep(2)
            else:
                logger.warning("[news] 拉取失败: %s", e)
                return pd.DataFrame()

    return pd.DataFrame()


def _sentiment(text: str) -> float:
    """用 snownlp 计算情感分数 (0-1, >0.5 偏正面)。"""
    try:
        if not text or len(text) < 2:
            return 0.5
        s = SnowNLP(text)
        return s.sentiments
    except Exception:
        return 0.5


def save(df: pd.DataFrame, db: Storage) -> int:
    """将新闻数据写入数据库。"""
    if df.empty:
        return 0
    return db.insert("news", df)


def _classify_news_inplace(df: pd.DataFrame) -> None:
    """对新闻 DataFrame 就地填充 news_type 和 classify_confidence。"""
    from src.narrative.news_classifier import NewsClassifier

    clf = NewsClassifier()  # 不传 llm_client，采集时只用规则引擎
    news_types = []
    confidences = []
    for _, row in df.iterrows():
        result = clf.classify(
            title=str(row.get("title", "")),
            content=str(row.get("content", "")),
            stock_code=str(row.get("stock_code", "")),
        )
        news_types.append(result.news_type.value)
        confidences.append(result.confidence)
    df["news_type"] = news_types
    df["classify_confidence"] = confidences
