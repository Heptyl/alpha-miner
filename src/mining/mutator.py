"""因子变异器 — 根据失败原因做定向变异。

变异操作：阈值调整、条件增删、方向反转、regime 过滤、因子杂交。
"""

from copy import deepcopy

import numpy as np


class FactorMutator:
    """对失败因子进行定向变异。"""

    def mutate(self, factor_config: dict, failure_diagnosis: dict) -> list[dict]:
        """根据失败原因生成 2-3 个变异版本。

        Args:
            factor_config: 因子配置 {"name", "factor_type", "conditions"/"expression", ...}
            failure_diagnosis: FailureDiagnosis 的 asdict()

        Returns:
            变异后的因子配置列表
        """
        mutations = []
        diagnosis = failure_diagnosis.get("diagnosis", "")

        if diagnosis == "too_strict":
            mutations.append(self._loosen_thresholds(factor_config, ratio=0.8))
            mutations.append(self._remove_weakest_condition(factor_config))

        elif diagnosis == "too_loose":
            mutations.append(self._tighten_thresholds(factor_config, ratio=1.2))
            mutations.append(self._add_condition_from_knowledge(factor_config))

        elif diagnosis == "reversed":
            mutations.append(self._reverse_direction(factor_config))

        elif diagnosis == "noisy_but_directional":
            mutations.append(self._add_regime_filter(factor_config, "board_rally"))
            mutations.append(self._add_regime_filter(factor_config, "theme_rotation"))

        elif diagnosis == "redundant":
            corr_factor = failure_diagnosis.get("details", {}).get("correlated_with", "")
            mutations.append(self._differentiate_from(factor_config, corr_factor))

        elif diagnosis == "inconsistent":
            mutations.append(self._add_smoothing(factor_config, window=3))
            mutations.append(self._add_regime_filter(factor_config, "normal"))

        elif diagnosis == "no_signal":
            # 尝试方向反转 + 窗口调整
            mutations.append(self._reverse_direction(factor_config))
            mutations.append(self._change_lookback(factor_config, multiplier=2.0))

        # 默认：至少返回一个窗口变异
        if not mutations:
            mutations.append(self._change_lookback(factor_config, multiplier=0.5))
            mutations.append(self._change_lookback(factor_config, multiplier=2.0))

        return mutations

    def _loosen_thresholds(self, config: dict, ratio: float = 0.8) -> dict:
        """放宽条件阈值（数值条件 × ratio）。"""
        new_config = deepcopy(config)
        new_config["name"] = f"{config.get('name', 'unknown')}_loose"
        conditions = new_config.get("conditions", [])

        new_conditions = []
        for cond in conditions:
            # 容错：字符串条件跳过数值调整，保留原样
            if isinstance(cond, str):
                new_conditions.append(cond)
                continue
            cond = dict(cond)
            if "value" in cond and isinstance(cond["value"], (int, float)):
                op = cond.get("operator", ">")
                if op in (">", ">="):
                    cond["value"] = cond["value"] * ratio
                elif op in ("<", "<="):
                    cond["value"] = cond["value"] * (1 + (1 - ratio))
            new_conditions.append(cond)
        new_config["conditions"] = new_conditions
        return new_config

    def _tighten_thresholds(self, config: dict, ratio: float = 1.2) -> dict:
        """收紧条件阈值。"""
        new_config = deepcopy(config)
        new_config["name"] = f"{config.get('name', 'unknown')}_tight"
        conditions = new_config.get("conditions", [])

        new_conditions = []
        for cond in conditions:
            # 容错：字符串条件跳过数值调整，保留原样
            if isinstance(cond, str):
                new_conditions.append(cond)
                continue
            cond = dict(cond)
            if "value" in cond and isinstance(cond["value"], (int, float)):
                op = cond.get("operator", ">")
                if op in (">", ">="):
                    cond["value"] = cond["value"] * ratio
                elif op in ("<", "<="):
                    cond["value"] = cond["value"] * (2 - ratio)
            new_conditions.append(cond)
        new_config["conditions"] = new_conditions
        return new_config

    def _remove_weakest_condition(self, config: dict) -> dict:
        """移除最后一个条件（假设条件按重要性排序）。"""
        new_config = deepcopy(config)
        new_config["name"] = f"{config.get('name', 'unknown')}_less_cond"
        conditions = new_config.get("conditions", [])
        if len(conditions) > 1:
            new_config["conditions"] = conditions[:-1]
        return new_config

    def _add_condition_from_knowledge(self, config: dict) -> dict:
        """从知识库补充一个条件。"""
        new_config = deepcopy(config)
        new_config["name"] = f"{config.get('name', 'unknown')}_extra_cond"
        conditions = new_config.get("conditions", [])
        # 添加一个通用过滤条件
        conditions.append({
            "name": "volume_filter",
            "table": "daily_price",
            "column": "volume",
            "operator": ">",
            "value": 1000,
        })
        new_config["conditions"] = conditions
        return new_config

    def _reverse_direction(self, config: dict) -> dict:
        """反转因子方向。"""
        new_config = deepcopy(config)
        new_config["name"] = f"{config.get('name', 'unknown')}_reversed"
        new_config["reverse"] = True
        return new_config

    def _add_regime_filter(self, config: dict, regime: str) -> dict:
        """加入 regime 前置条件。"""
        new_config = deepcopy(config)
        new_config["name"] = f"{config.get('name', 'unknown')}_{regime}"
        new_config["regime_filter"] = regime
        return new_config

    def _differentiate_from(self, config: dict, corr_factor: str) -> dict:
        """与相关因子做差异化。"""
        new_config = deepcopy(config)
        new_config["name"] = f"{config.get('name', 'unknown')}_diff_{corr_factor}"
        # 添加额外时间窗口条件
        new_config["extra_filter"] = f"与 {corr_factor} 做差异化的额外条件"
        return new_config

    def _add_smoothing(self, config: dict, window: int = 3) -> dict:
        """增加平滑窗口。"""
        new_config = deepcopy(config)
        new_config["name"] = f"{config.get('name', 'unknown')}_smooth{window}"
        new_config["smoothing_window"] = window
        return new_config

    def _change_lookback(self, config: dict, multiplier: float = 2.0) -> dict:
        """调整 lookback 天数。"""
        new_config = deepcopy(config)
        current = config.get("lookback_days", 1)
        new_config["lookback_days"] = max(1, int(current * multiplier))
        new_config["name"] = f"{config.get('name', 'unknown')}_lb{new_config['lookback_days']}"
        return new_config
