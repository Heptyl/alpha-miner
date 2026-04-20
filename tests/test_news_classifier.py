"""新闻分类器测试。"""

import pytest

from src.narrative.news_classifier import (
    NewsClassifier,
    NewsType,
    ClassifyResult,
    NEWS_TYPE_WEIGHTS,
)


class TestRuleClassify:
    """规则引擎分类测试。"""

    def setup_method(self):
        self.clf = NewsClassifier()  # 无 LLM

    def test_theme_ignite_keyword(self):
        """包含'首次'+'新赛道'→ theme_ignite。"""
        r = self.clf.classify("公司首次进入AI芯片新赛道")
        assert r.news_type == NewsType.THEME_IGNITE
        assert r.confidence >= 0.7
        assert r.method == "rule"

    def test_catalyst_real_keyword(self):
        """包含'中标'→ catalyst_real。"""
        r = self.clf.classify("某某公司中标5亿元项目")
        assert r.news_type == NewsType.CATALYST_REAL
        assert r.confidence >= 0.7

    def test_negative_keyword(self):
        """包含'处罚'→ negative。"""
        r = self.clf.classify("某公司因违规被处罚")
        assert r.news_type == NewsType.NEGATIVE

    def test_good_realize_keyword(self):
        """包含'落地'→ good_realize。"""
        r = self.clf.classify("重大投资项目正式落地")
        assert r.news_type == NewsType.GOOD_REALIZE

    def test_catalyst_expect_keyword(self):
        """包含'有望'→ catalyst_expect。"""
        r = self.clf.classify("行业有望迎来爆发期")
        assert r.news_type == NewsType.CATALYST_EXPECT

    def test_theme_ferment_keyword(self):
        """包含'发酵'→ theme_ferment。"""
        r = self.clf.classify("AI概念持续发酵扩散")
        assert r.news_type == NewsType.THEME_FERMENT

    def test_noise_default(self):
        """无关键词→ noise。"""
        r = self.clf.classify("今日天气晴朗")
        assert r.news_type == NewsType.NOISE

    def test_anti_keyword_reduces_score(self):
        """anti_keywords 降低 theme_ignite 得分。"""
        # '持续' 是 THEME_IGNITE 的 anti_keyword
        r = self.clf.classify("持续突破新赛道")
        # 因为 anti_keyword，可能降级到 THEME_FERMENT（也有'持续'关键词）
        assert r.news_type in (NewsType.THEME_FERMENT, NewsType.THEME_IGNITE)

    def test_content_contributes(self):
        """正文内容也参与分类。"""
        r = self.clf.classify("公司公告", content="净利润增长50%")
        assert r.news_type == NewsType.CATALYST_REAL

    def test_high_confidence_skips_llm(self):
        """高置信度时不调 LLM（即使提供了 llm_client）。"""
        class FakeLLM:
            def messages(self):
                return self
            def create(self, **kw):
                raise RuntimeError("不应调用 LLM")

        clf = NewsClassifier(llm_client=FakeLLM())
        r = clf.classify("公司首次进入新赛道")
        assert r.method == "rule"  # 没走到 LLM


class TestClassifyBatch:
    """批量分类测试。"""

    def test_batch_classify(self):
        clf = NewsClassifier()
        items = [
            {"title": "公司中标大订单", "content": ""},
            {"title": "今日无消息", "content": ""},
        ]
        results = clf.classify_batch(items)
        assert len(results) == 2
        assert results[0].news_type == NewsType.CATALYST_REAL
        assert results[1].news_type == NewsType.NOISE


class TestClassifyResult:
    """ClassifyResult 数据结构测试。"""

    def test_to_dict(self):
        r = ClassifyResult(
            news_type=NewsType.THEME_IGNITE,
            confidence=0.9,
            method="rule",
            reasoning="",
        )
        d = r.to_dict()
        assert d["news_type"] == "theme_ignite"
        assert d["confidence"] == 0.9
        assert d["method"] == "rule"


class TestNewsTypeWeights:
    """权重表完整性测试。"""

    def test_all_types_have_weights(self):
        """每个 NewsType 都有对应权重。"""
        for nt in NewsType:
            assert nt.value in NEWS_TYPE_WEIGHTS

    def test_negative_types_have_negative_weights(self):
        """利空类型权重为负。"""
        assert NEWS_TYPE_WEIGHTS["good_realize"] < 0
        assert NEWS_TYPE_WEIGHTS["negative"] < 0

    def test_positive_types_have_positive_weights(self):
        """利好类型权重为正。"""
        assert NEWS_TYPE_WEIGHTS["theme_ignite"] > 0
        assert NEWS_TYPE_WEIGHTS["catalyst_real"] > 0

    def test_noise_weight_is_zero(self):
        assert NEWS_TYPE_WEIGHTS["noise"] == 0.0


class TestLLMFallback:
    """LLM 失败时回退到规则引擎。"""

    def test_llm_failure_falls_back_to_rule(self):
        """LLM 调用失败时回退规则分类。"""
        class BrokenLLM:
            class messages:
                @staticmethod
                def create(**kw):
                    raise ConnectionError("LLM 不可达")

        clf = NewsClassifier(llm_client=BrokenLLM())
        # 低置信度标题（触发 LLM 调用）→ LLM 失败 → 回退规则
        r = clf.classify("某公司发布产品公告")
        assert r.method == "rule"
