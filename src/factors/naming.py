"""因子命名映射 — 统一加载 config/factor_aliases.yaml。

所有中文展示（主UI / 审视简报 / 微信推送 / 日报）从这里取因子的
中文名、一句话说明与详细说明，避免各处散落硬编码。

设计约束：
- 依赖极轻（仅 yaml），不 import 因子实现（不拖 pandas），可在任何脚本中低成本使用；
- 映射缺失时优雅回退英文名，绝不抛错——挖掘引擎会产出映射表外的新因子名。

用法:
    from src.factors.naming import FactorNaming
    naming = FactorNaming()
    naming.cn("lhb_institution")      # -> "机构买入"
    naming.label("lhb_institution")   # -> "机构买入 (lhb_institution)"
    naming.table()                    # -> 完整映射表（含 role，供 UI 渲染）
"""

from __future__ import annotations

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_ALIASES = ROOT / "config" / "factor_aliases.yaml"
DEFAULT_FACTORS = ROOT / "config" / "factors.yaml"

CATEGORY_CN = {"formula": "公式因子", "narrative": "叙事因子"}
ROLE_CN = {"alpha": "收益来源", "filter": "风控过滤"}


def _load_yaml(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError:
        return {}


class FactorNaming:
    """因子英文名 → 中文名/说明 的只读映射。"""

    def __init__(self, aliases_path: str | Path = DEFAULT_ALIASES,
                 factors_path: str | Path = DEFAULT_FACTORS):
        self._meta: dict[str, dict] = _load_yaml(Path(aliases_path)).get("factors") or {}
        # role/factor_type 以 factors.yaml 为准（直接读 yaml，不经 registry，避免拖重依赖）
        self._defs: dict[str, dict] = {}
        cfg = _load_yaml(Path(factors_path))
        for cat in ("formula_factors", "narrative_factors"):
            for d in cfg.get(cat) or []:
                if d.get("name"):
                    self._defs[d["name"]] = d

    # ---- 单字段查询（缺失回退英文名/空串，不抛错） ----

    def cn(self, name: str) -> str:
        """中文短名；无映射时回退英文名。"""
        return (self._meta.get(name) or {}).get("cn") or name

    def label(self, name: str) -> str:
        """'中文名 (english)'；无映射时只回英文名。"""
        cn = self.cn(name)
        return name if cn == name else f"{cn} ({name})"

    def desc(self, name: str) -> str:
        return (self._meta.get(name) or {}).get("desc") or ""

    def detail(self, name: str) -> str:
        return ((self._meta.get(name) or {}).get("detail") or "").strip()

    def note(self, name: str) -> str:
        return ((self._meta.get(name) or {}).get("note") or "").strip()

    def role(self, name: str) -> str:
        return (self._defs.get(name) or {}).get("role", "alpha")

    # ---- 整表（UI 渲染用） ----

    def names(self) -> list[str]:
        """映射表中的全部因子英文名（保持 yaml 顺序）。"""
        return list(self._meta.keys())

    def table(self) -> list[dict]:
        """完整映射表：en/cn/desc/detail/note/category/role（含中文化字段）。

        以 aliases 表为序；factors.yaml 中有而 aliases 缺失的因子也补入（回退英文名），
        保证注册因子全覆盖。
        """
        order = list(self._meta.keys())
        order += [n for n in self._defs if n not in self._meta]
        rows = []
        for name in order:
            meta = self._meta.get(name) or {}
            category = meta.get("category", "")
            role = self.role(name)
            rows.append({
                "en": name,
                "cn": self.cn(name),
                "desc": meta.get("desc", ""),
                "detail": self.detail(name),
                "note": self.note(name),
                "category": category,
                "category_cn": CATEGORY_CN.get(category, category),
                "role": role,
                "role_cn": ROLE_CN.get(role, role),
            })
        return rows


_default: FactorNaming | None = None


def get_naming() -> FactorNaming:
    """进程级缓存的默认实例。"""
    global _default
    if _default is None:
        _default = FactorNaming()
    return _default


def cn_name(name: str) -> str:
    """便捷函数：因子英文名 → 中文短名。"""
    return get_naming().cn(name)
