"""因子注册表 — 从 factors.yaml 加载所有因子。"""

import importlib
from pathlib import Path
from typing import Optional

import yaml

from src.factors.base import BaseFactor


class FactorRegistry:
    """因子注册表，管理所有已注册的因子。

    每个实例独立持有 _factors，无单例状态泄露。
    """

    def __init__(self):
        self._factors: dict[str, BaseFactor] = {}

    def load_from_yaml(self, yaml_path: str = "config/factors.yaml") -> None:
        """从 YAML 文件加载因子配置。"""
        path = Path(yaml_path)
        if not path.exists():
            return

        config = yaml.safe_load(path.read_text(encoding="utf-8"))

        for category in ["formula_factors", "narrative_factors"]:
            for factor_def in config.get(category, []):
                try:
                    module = importlib.import_module(factor_def["module"])
                    cls = getattr(module, factor_def["class"])
                    factor = cls()
                    factor.name = factor_def["name"]
                    self._factors[factor_def["name"]] = factor
                except (ImportError, AttributeError) as e:
                    print(f"Warning: 无法加载因子 {factor_def['name']}: {e}")

    def get_factor(self, name: str) -> BaseFactor:
        """获取指定名称的因子实例。"""
        if not self._factors:
            self.load_from_yaml()
        if name not in self._factors:
            raise KeyError(f"因子 '{name}' 未注册。可用因子: {list(self._factors.keys())}")
        return self._factors[name]

    def list_factors(self) -> list[str]:
        """列出所有已注册的因子名称。"""
        if not self._factors:
            self.load_from_yaml()
        return list(self._factors.keys())

    def register(self, name: str, factor: BaseFactor) -> None:
        """手动注册一个因子。"""
        self._factors[name] = factor

    def clear(self) -> None:
        """清空注册表（主要用于测试）。"""
        self._factors = {}
