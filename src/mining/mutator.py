"""因子变异器 — 根据失败原因做定向变异。

变异操作：阈值调整、条件增删、方向反转、regime 过滤、因子杂交。
每个变异配置都带 mutation_type 字段，标识变异操作类型。
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
            变异后的因子配置列表，每个带 mutation_type 字段
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

        elif diagnosis == "wrong_direction":
            mutations.append(self._reverse_direction(factor_config))
            mutations.append(self._change_lookback(factor_config, multiplier=2.0))

        elif diagnosis == "noisy_but_directional":
            mutations.append(self._add_regime_filter(factor_config, "board_rally"))
            mutations.append(self._add_regime_filter(factor_config, "theme_rotation"))

        elif diagnosis == "regime_dependent":
            details = failure_diagnosis.get("details", {})
            best_regime = details.get("best_regime", "board_rally")
            # 生成 regime 限定版 + 情绪限定版
            mutations.append(self._add_regime_filter(factor_config, best_regime))
            # 如果有 regime_breakdown，对每个有效 regime 也生成变体
            regime_breakdown = details.get("regime_breakdown", [])
            effective_regimes = [
                r["regime"] for r in regime_breakdown
                if isinstance(r, dict) and r.get("effective") and r["regime"] != best_regime
            ]
            for regime in effective_regimes[:1]:  # 最多再多加一个
                mutations.append(self._add_regime_filter(factor_config, regime))

        elif diagnosis == "emotion_dependent":
            best_emotion = failure_diagnosis.get("details", {}).get("best_emotion", "normal")
            emotion_zt_map = {
                "strong": (60, 999),
                "normal": (20, 60),
                "weak": (0, 20),
            }
            min_zt, max_zt = emotion_zt_map.get(best_emotion, (20, 60))
            mutations.append(self._add_zt_count_filter(factor_config, min_zt, max_zt))

        elif diagnosis == "time_decayed":
            mutations.append(self._change_lookback(factor_config, multiplier=0.3))
            mutations.append(self._reverse_direction(factor_config))

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
        new_config["mutation_type"] = "loosen_thresholds"
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
        new_config["mutation_type"] = "tighten_thresholds"
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
        new_config["mutation_type"] = "remove_condition"
        conditions = new_config.get("conditions", [])
        if len(conditions) > 1:
            new_config["conditions"] = conditions[:-1]
        return new_config

    def _add_condition_from_knowledge(self, config: dict) -> dict:
        """从知识库补充一个条件。"""
        new_config = deepcopy(config)
        new_config["name"] = f"{config.get('name', 'unknown')}_extra_cond"
        new_config["mutation_type"] = "add_condition"
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
        new_config["mutation_type"] = "reverse_direction"
        new_config["reverse"] = True
        # 翻转 direction 字段
        if "direction" in new_config:
            d = new_config["direction"]
            if d == "ascending":
                new_config["direction"] = "descending"
            elif d == "descending":
                new_config["direction"] = "ascending"
        return new_config

    def _add_regime_filter(self, config: dict, regime: str) -> dict:
        """加入 regime 前置条件。"""
        new_config = deepcopy(config)
        new_config["name"] = f"{config.get('name', 'unknown')}_{regime}"
        new_config["mutation_type"] = "regime_filter"
        new_config["regime_filter"] = regime
        return new_config

    def _differentiate_from(self, config: dict, corr_factor: str) -> dict:
        """与相关因子做差异化。"""
        new_config = deepcopy(config)
        new_config["name"] = f"{config.get('name', 'unknown')}_diff_{corr_factor}"
        new_config["mutation_type"] = "differentiate"
        # 添加额外时间窗口条件
        new_config["extra_filter"] = f"与 {corr_factor} 做差异化的额外条件"
        return new_config

    def _add_smoothing(self, config: dict, window: int = 3) -> dict:
        """增加平滑窗口。"""
        new_config = deepcopy(config)
        new_config["name"] = f"{config.get('name', 'unknown')}_smooth{window}"
        new_config["mutation_type"] = "smoothing"
        new_config["smoothing_window"] = window
        return new_config

    def _add_zt_count_filter(self, config: dict, min_zt: int, max_zt: int) -> dict:
        """添加涨停数过滤条件（基于情绪分桶）。"""
        new_config = deepcopy(config)
        name = config.get("name", "unknown")
        new_config["name"] = f"{name}_zt_{min_zt}_{max_zt}"
        new_config["mutation_type"] = "zt_count_filter"
        new_config["zt_count_filter"] = {"min": min_zt, "max": max_zt}
        return new_config

    def _change_lookback(self, config: dict, multiplier: float = 2.0) -> dict:
        """调整 lookback 天数。"""
        new_config = deepcopy(config)
        current = config.get("lookback_days", 1)
        new_config["lookback_days"] = max(1, int(current * multiplier))
        new_config["name"] = f"{config.get('name', 'unknown')}_lb{new_config['lookback_days']}"
        new_config["mutation_type"] = "change_lookback"
        return new_config
