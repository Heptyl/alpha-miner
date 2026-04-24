"""测试新闻分类器的功能正确性。"""
import pytest

from src.narrative.news_classifier import (
    NewsClassifier, NewsType, ClassifyResult, NEWS_TYPE_WEIGHTS,
)


@pytest.fixture
def classifier():
    return NewsClassifier(llm_client=None)


class TestNewsClassifierRules:
    """规则分类必须正确识别各类型新闻。"""

    def test_theme_ignite(self, classifier):
        """题材点燃：首次/突破/新赛道。"""
        result = classifier.classify("国务院发布重磅政策，AI新赛道迎来突破性发展")
        assert result.news_type == NewsType.THEME_IGNITE
        assert result.confidence >= 0.7

    def test_theme_ferment(self, classifier):
        """题材发酵：持续/扩散/产业链。"""
        result = classifier.classify("AI概念股持续发酵，产业链相关公司受关注")
        assert result.news_type == NewsType.THEME_FERMENT
        assert result.confidence >= 0.7

    def test_catalyst_real(self, classifier):
        """实质性利好：中标/签约/订单。"""
        result = classifier.classify("某公司中标10亿大单，业绩大增")
        assert result.news_type == NewsType.CATALYST_REAL
        assert result.confidence >= 0.7

    def test_catalyst_expect(self, classifier):
        """预期类利好：有望/规划/传闻。"""
        result = classifier.classify("业内传闻：某公司有望获得重大订单")
        assert result.news_type == NewsType.CATALYST_EXPECT

    def test_good_realize(self, classifier):
        """利好兑现：正式发布/落地/投产。"""
        result = classifier.classify("某产品正式发布并投产")
        assert result.news_type == NewsType.GOOD_REALIZE

    def test_negative(self, classifier):
        """利空：处罚/违规/减持。"""
        result = classifier.classify("某公司涉嫌违规被立案调查")
        assert result.news_type == NewsType.NEGATIVE
        assert result.confidence >= 0.7

    def test_noise(self, classifier):
        """无关新闻归为 noise。"""
        result = classifier.classify("今天天气晴朗，适合出行")
        assert result.news_type == NewsType.NOISE
        assert result.confidence == 0.0

    def test_empty_title(self, classifier):
        """空标题不崩溃，返回 noise。"""
        result = classifier.classify("")
        assert isinstance(result, ClassifyResult)
        assert result.news_type == NewsType.NOISE


class TestNewsClassifierBatch:
    """批量分类测试。"""

    def test_batch_classify(self, classifier):
        """批量分类返回等长结果。"""
        items = [
            {"title": "AI突破性技术首次亮相", "content": "划时代产品"},
            {"title": "某公司中标5亿合同", "content": "实质性利好"},
            {"title": "今日股市小幅震荡", "content": ""},
        ]
        results = classifier.classify_batch(items)
        assert len(results) == len(items)
        assert all(isinstance(r, ClassifyResult) for r in results)

    def test_batch_empty(self, classifier):
        """空列表返回空列表。"""
        results = classifier.classify_batch([])
        assert results == []


class TestNewsTypeWeights:
    """新闻类型权重表完整性。"""

    def test_all_types_have_weights(self):
        """每种 NewsType 必须有权重。"""
        for nt in NewsType:
            assert nt.value in NEWS_TYPE_WEIGHTS, \
                f"{nt.value} 缺少权重"

    def test_ignite_weight_highest(self):
        """theme_ignite 权重应该最高。"""
        assert NEWS_TYPE_WEIGHTS["theme_ignite"] >= max(
            v for k, v in NEWS_TYPE_WEIGHTS.items()
            if k != "theme_ignite"
        )

    def test_negative_weight_is_negative(self):
        """negative 权重必须为负。"""
        assert NEWS_TYPE_WEIGHTS["negative"] < 0

    def test_noise_weight_is_zero(self):
        """noise 权重应该为 0。"""
        assert NEWS_TYPE_WEIGHTS["noise"] == 0.0

    def test_confidence_range(self, classifier):
        """所有分类结果的 confidence 在 [0, 1]。"""
        test_titles = [
            "AI突破", "中标合同", "处罚违规", "天气晴朗",
            "持续发酵", "正式投产", "规划预期", "",
        ]
        for title in test_titles:
            result = classifier.classify(title)
            assert 0.0 <= result.confidence <= 1.0, \
                f"'{title}' 的 confidence={result.confidence} 越界"
