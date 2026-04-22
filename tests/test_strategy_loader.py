"""策略加载器测试。"""

import os
import tempfile

import pytest
import yaml

from src.strategy.loader import (
    load_strategies, load_strategy_by_name, save_strategies, list_strategy_names,
)
from src.strategy.schema import EntryRule, ExitRule, Strategy


@pytest.fixture
def yaml_file():
    """创建临时策略 YAML 文件。"""
    strategies = [
        {
            "name": "策略A",
            "description": "测试策略A",
            "entry": {"conditions": [{"factor": "consecutive_board", "op": ">=", "value": 1}]},
            "exit": {"take_profit_pct": 5.0, "stop_loss_pct": -3.0, "max_hold_days": 3},
            "tags": ["打板"],
        },
        {
            "name": "策略B",
            "description": "测试策略B",
            "entry": {"conditions": [{"factor": "turnover_rank", "op": ">=", "value": 0.5}]},
            "exit": {"take_profit_pct": 7.0, "stop_loss_pct": -5.0, "max_hold_days": 5},
            "tags": ["低吸"],
        },
    ]
    with tempfile.NamedTemporaryFile(suffix=".yaml", delete=False, mode="w") as f:
        yaml.dump({"strategies": strategies}, f, allow_unicode=True)
        path = f.name
    yield path
    os.unlink(path)


class TestLoadStrategies:
    def test_load_all(self, yaml_file):
        result = load_strategies(yaml_file)
        assert len(result) == 2
        assert all(isinstance(s, Strategy) for s in result)

    def test_entry_exit_parsed(self, yaml_file):
        result = load_strategies(yaml_file)
        s = result[0]
        assert isinstance(s.entry, EntryRule)
        assert isinstance(s.exit, ExitRule)
        assert s.entry.conditions[0]["factor"] == "consecutive_board"
        assert s.exit.take_profit_pct == 5.0

    def test_empty_file(self):
        with tempfile.NamedTemporaryFile(suffix=".yaml", delete=False, mode="w") as f:
            f.write("")
            path = f.name
        result = load_strategies(path)
        assert result == []
        os.unlink(path)

    def test_missing_file(self):
        result = load_strategies("/nonexistent/path.yaml")
        assert result == []

    def test_no_strategies_key(self):
        with tempfile.NamedTemporaryFile(suffix=".yaml", delete=False, mode="w") as f:
            yaml.dump({"other_key": []}, f)
            path = f.name
        result = load_strategies(path)
        assert result == []
        os.unlink(path)

    def test_malformed_entry_skipped(self):
        """格式错误的策略被跳过而非报错。"""
        data = {
            "strategies": [
                {"name": "good", "description": "", "entry": {"conditions": []}, "exit": {}},
                {"bad_key_only": True},  # 缺少必要字段
            ]
        }
        with tempfile.NamedTemporaryFile(suffix=".yaml", delete=False, mode="w") as f:
            yaml.dump(data, f, allow_unicode=True)
            path = f.name
        result = load_strategies(path)
        assert len(result) == 1
        assert result[0].name == "good"
        os.unlink(path)


class TestLoadByName:
    def test_found(self, yaml_file):
        s = load_strategy_by_name("策略A", yaml_file)
        assert s is not None
        assert s.name == "策略A"

    def test_not_found(self, yaml_file):
        s = load_strategy_by_name("不存在", yaml_file)
        assert s is None


class TestSaveStrategies:
    def test_roundtrip(self):
        strategies = [
            Strategy(name="s1", description="d1", entry=EntryRule(), exit=ExitRule(), tags=["a"]),
            Strategy(name="s2", description="d2", entry=EntryRule(), exit=ExitRule()),
        ]
        with tempfile.NamedTemporaryFile(suffix=".yaml", delete=False) as f:
            path = f.name
        save_strategies(strategies, path)
        loaded = load_strategies(path)
        assert len(loaded) == 2
        assert loaded[0].name == "s1"
        assert loaded[0].tags == ["a"]
        assert loaded[1].name == "s2"
        os.unlink(path)


class TestListNames:
    def test_list(self, yaml_file):
        names = list_strategy_names(yaml_file)
        assert names == ["策略A", "策略B"]

    def test_empty(self):
        with tempfile.NamedTemporaryFile(suffix=".yaml", delete=False, mode="w") as f:
            f.write("")
            path = f.name
        assert list_strategy_names(path) == []
        os.unlink(path)
