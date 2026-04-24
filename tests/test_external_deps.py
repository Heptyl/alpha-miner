"""测试外部依赖的容错。"""
import pytest
import importlib
import sys


class TestExternalDependencies:
    """确保所有模块在缺少外部依赖时不会崩溃。"""

    def test_news_import_without_fin_sentiment(self):
        """akshare_news 在没有 fin_sentiment 时仍可导入。"""
        # 临时移除 fin_sentiment 的路径
        original_path = sys.path.copy()
        try:
            # 强制重新导入
            if "src.data.sources.akshare_news" in sys.modules:
                del sys.modules["src.data.sources.akshare_news"]
            if "fin_sentiment" in sys.modules:
                del sys.modules["fin_sentiment"]

            # 移除可能包含 fin_sentiment 的路径
            sys.path = [p for p in sys.path
                        if "a-share-sentiment" not in p]

            # 导入不应该崩溃
            try:
                import src.data.sources.akshare_news as news_mod
                # 应该有 fallback 情感分析
                assert hasattr(news_mod, "_sentiment"), \
                    "akshare_news 缺少 _sentiment 函数"
            except ImportError as e:
                if "fin_sentiment" in str(e):
                    pytest.fail(
                        "akshare_news 在没有 fin_sentiment 时导入失败。"
                        "需要加 try/except fallback。"
                    )
                raise
        finally:
            sys.path = original_path

    def test_sentiment_fallback_produces_valid_score(self):
        """情感分析 fallback 必须返回 0-1 之间的值。"""
        from src.data.sources.akshare_news import _sentiment

        score = _sentiment("这是一条测试新闻")
        assert isinstance(score, float), f"情感分数类型错误: {type(score)}"
        assert 0.0 <= score <= 1.0, f"情感分数越界: {score}"

    def test_sentiment_handles_empty_input(self):
        """情感分析处理空输入不崩溃。"""
        from src.data.sources.akshare_news import _sentiment

        assert _sentiment("") == 0.5 or isinstance(_sentiment(""), float)
        assert _sentiment(None) == 0.5 or isinstance(_sentiment(None), float)

    def test_all_source_modules_importable(self):
        """所有数据源模块都可以无错导入。"""
        modules = [
            "src.data.sources.akshare_price",
            "src.data.sources.akshare_zt_pool",
            "src.data.sources.akshare_lhb",
            "src.data.sources.akshare_fund_flow",
            "src.data.sources.akshare_concept",
            "src.data.sources.akshare_news",
        ]
        for mod_name in modules:
            try:
                importlib.import_module(mod_name)
            except ImportError as e:
                pytest.fail(f"{mod_name} 导入失败: {e}")

    def test_narrative_modules_importable(self):
        """叙事引擎模块都可以无错导入。"""
        modules = [
            "src.narrative.news_classifier",
            "src.narrative.script_engine",
            "src.narrative.replay_engine",
        ]
        for mod_name in modules:
            try:
                importlib.import_module(mod_name)
            except ImportError as e:
                pytest.fail(f"{mod_name} 导入失败: {e}")
