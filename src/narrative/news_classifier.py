"""新闻分类器 — 规则优先，LLM 兜底。

把新闻从"数量"升级到"类型"，为 narrative_velocity 因子提供加权依据。
"""

import logging
import json
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

PROMPT_PATH = Path(__file__).parent / "prompts" / "news_classify.md"


class NewsType(str, Enum):
    """新闻对股价的影响类型。"""
    THEME_IGNITE    = "theme_ignite"     # 题材首次点燃（新概念/新政策）
    THEME_FERMENT   = "theme_ferment"    # 题材发酵中（后续跟踪报道）
    CATALYST_REAL   = "catalyst_real"    # 实质性利好（订单/业绩/中标）
    CATALYST_EXPECT = "catalyst_expect"  # 预期类利好（传闻/规划/预告）
    GOOD_REALIZE    = "good_realize"     # 利好兑现（已公告的利好落地）
    NEGATIVE        = "negative"        # 利空消息
    NOISE           = "noise"           # 无关噪音


@dataclass
class ClassifyResult:
    """分类结果。"""
    news_type: NewsType
    confidence: float
    method: str       # "rule" | "llm"
    reasoning: str

    def to_dict(self) -> dict:
        return {
            "news_type": self.news_type.value,
            "confidence": self.confidence,
            "method": self.method,
            "reasoning": self.reasoning,
        }


class NewsClassifier:
    """新闻分类器：规则优先，LLM 兜底。"""

    # 规则关键词表
    RULES: dict[NewsType, dict] = {
        NewsType.THEME_IGNITE: {
            "keywords": ["首次", "突破", "划时代", "颠覆", "新赛道", "政策出台",
                         "国务院发布", "重磅政策", "全新", "开创性"],
            "anti_keywords": ["继续", "持续", "延续"],
        },
        NewsType.THEME_FERMENT: {
            "keywords": ["持续", "发酵", "扩散", "跟进", "产业链", "相关公司",
                         "概念股", "板块联动"],
            "anti_keywords": [],
        },
        NewsType.CATALYST_REAL: {
            "keywords": ["中标", "签约", "订单", "净利润增长", "营收增长", "业绩预增",
                         "业绩大增", "获批准", "斩获", "合同"],
            "anti_keywords": [],
        },
        NewsType.CATALYST_EXPECT: {
            "keywords": ["预期", "规划", "有望", "或将", "传闻", "预告", "计划",
                         "拟建", "筹备中"],
            "anti_keywords": [],
        },
        NewsType.GOOD_REALIZE: {
            "keywords": ["正式发布", "已完成", "落地", "通过审批", "获批", "投产",
                         "正式上线", "交付"],
            "anti_keywords": [],
        },
        NewsType.NEGATIVE: {
            "keywords": ["处罚", "违规", "下修", "业绩预减", "退市", "ST", "立案调查",
                         "亏损", "减持", "暴雷", "警示"],
            "anti_keywords": [],
        },
    }

    def __init__(self, llm_client=None):
        self.llm_client = llm_client

    def classify(self, title: str, content: str = "",
                 stock_code: str = "") -> ClassifyResult:
        """分类一条新闻。

        Args:
            title: 新闻标题
            content: 新闻正文（可选）
            stock_code: 关联股票代码（可选，传给 LLM 时有用）

        Returns:
            ClassifyResult
        """
        # Step 1: 规则匹配
        result = self._rule_classify(title, content)
        if result.confidence >= 0.7:
            return result

        # Step 2: LLM 分类（如果可用）
        if self.llm_client:
            return self._llm_classify(title, content, stock_code)

        # Step 3: 无 LLM 则返回规则结果（即使低置信度）
        return result

    def classify_batch(self, items: list[dict]) -> list[ClassifyResult]:
        """批量分类。每项需包含 title 键，可选 content 和 stock_code。"""
        return [
            self.classify(
                title=item.get("title", ""),
                content=item.get("content", ""),
                stock_code=item.get("stock_code", ""),
            )
            for item in items
        ]

    def _rule_classify(self, title: str, content: str) -> ClassifyResult:
        """基于关键词的规则分类。"""
        text = title + " " + content
        best_type = NewsType.NOISE
        best_score = 0.0

        for ntype, rule in self.RULES.items():
            score = 0.0
            keywords = rule.get("keywords", [])
            anti_keywords = rule.get("anti_keywords", [])

            hit_count = sum(1 for kw in keywords if kw in text)
            if hit_count > 0:
                # 单关键词命中 = 0.8，多关键词叠加
                score = 0.8 + 0.1 * (hit_count - 1)
                score = min(score, 1.0)

            for akw in anti_keywords:
                if akw in text:
                    score *= 0.5

            if score > best_score:
                best_score = score
                best_type = ntype

        return ClassifyResult(
            news_type=best_type,
            confidence=min(best_score, 1.0),
            method="rule",
            reasoning="",
        )

    def _llm_classify(self, title: str, content: str,
                      stock_code: str) -> ClassifyResult:
        """用 LLM 分类（仅在规则不确定时调用）。"""
        if not PROMPT_PATH.exists():
            logger.warning("news_classify.md prompt 文件不存在，回退规则分类")
            return self._rule_classify(title, content)

        prompt_template = PROMPT_PATH.read_text(encoding="utf-8")
        prompt = prompt_template.replace("{{TITLE}}", title)
        prompt = prompt.replace("{{CONTENT}}", content[:500])
        prompt = prompt.replace("{{STOCK_CODE}}", stock_code)

        try:
            response = self.llm_client.messages.create(
                model="glm-4-plus",
                max_tokens=200,
                temperature=0.1,
                messages=[{"role": "user", "content": prompt}],
            )
            text = response.content[0].text

            # 清理 JSON
            if "```json" in text:
                text = text.split("```json")[1].split("```")[0]
            elif "```" in text:
                text = text.split("```")[1].split("```")[0]

            data = json.loads(text.strip())
            return ClassifyResult(
                news_type=NewsType(data.get("news_type", "noise")),
                confidence=float(data.get("confidence", 0.5)),
                method="llm",
                reasoning=data.get("reasoning", ""),
            )
        except Exception as e:
            logger.error("LLM 新闻分类失败: %s", e)
            return self._rule_classify(title, content)


# 新闻类型权重表（供 narrative_velocity 因子使用）
NEWS_TYPE_WEIGHTS: dict[str, float] = {
    "theme_ignite": 3.0,
    "theme_ferment": 1.5,
    "catalyst_real": 2.0,
    "catalyst_expect": 1.0,
    "good_realize": -0.5,
    "negative": -2.0,
    "noise": 0.0,
}
