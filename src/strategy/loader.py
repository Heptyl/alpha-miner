"""策略加载器 — 从 YAML 文件加载预置策略。"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import yaml

from src.strategy.schema import Strategy


DEFAULT_STRATEGIES_PATH = Path(__file__).parent.parent.parent / "knowledge_base" / "strategies.yaml"


def load_strategies(path: Optional[str | Path] = None) -> list[Strategy]:
    """从 YAML 文件加载策略列表。

    YAML 格式:
        strategies:
          - name: ...
            entry: ...
            ...

    Returns:
        策略列表，按 YAML 中顺序排列。
    """
    p = Path(path) if path else DEFAULT_STRATEGIES_PATH
    if not p.exists():
        return []

    raw = yaml.safe_load(p.read_text(encoding="utf-8"))
    if not raw or "strategies" not in raw:
        return []

    strategies = []
    for item in raw["strategies"]:
        try:
            s = Strategy.from_dict(item)
            strategies.append(s)
        except Exception:
            continue  # 跳过格式错误的条目

    return strategies


def load_strategy_by_name(name: str, path: Optional[str | Path] = None) -> Optional[Strategy]:
    """按名称加载单个策略。"""
    for s in load_strategies(path):
        if s.name == name:
            return s
    return None


def save_strategies(strategies: list[Strategy], path: Optional[str | Path] = None) -> None:
    """保存策略列表到 YAML 文件。"""
    p = Path(path) if path else DEFAULT_STRATEGIES_PATH
    p.parent.mkdir(parents=True, exist_ok=True)
    data = {"strategies": [s.to_dict() for s in strategies]}
    p.write_text(
        yaml.dump(data, allow_unicode=True, default_flow_style=False),
        encoding="utf-8",
    )


def list_strategy_names(path: Optional[str | Path] = None) -> list[str]:
    """列出所有策略名称。"""
    return [s.name for s in load_strategies(path)]
