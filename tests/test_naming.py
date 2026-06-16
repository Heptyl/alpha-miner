"""因子命名映射的行为测试 — 映射表必须全覆盖注册因子，缺失时优雅回退。"""

import sys
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.factors.naming import FactorNaming, cn_name  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent


def _registered_names() -> list[str]:
    cfg = yaml.safe_load((ROOT / "config" / "factors.yaml").read_text(encoding="utf-8"))
    out = []
    for cat in ("formula_factors", "narrative_factors"):
        out += [d["name"] for d in cfg.get(cat, [])]
    return out


def test_mapping_covers_all_registered_factors():
    """factors.yaml 里每个注册因子都必须有中文名与详细说明。"""
    naming = FactorNaming()
    for name in _registered_names():
        assert naming.cn(name) != name, f"{name} 缺中文名"
        assert naming.desc(name), f"{name} 缺一句话说明"
        assert naming.detail(name), f"{name} 缺详细说明"


def test_unknown_factor_falls_back_to_english():
    """挖掘引擎产出的新因子名不在映射表内 → 回退英文名，不抛错。"""
    naming = FactorNaming()
    assert naming.cn("mined_factor_xyz") == "mined_factor_xyz"
    assert naming.label("mined_factor_xyz") == "mined_factor_xyz"
    assert naming.desc("mined_factor_xyz") == ""
    assert cn_name("mined_factor_xyz") == "mined_factor_xyz"


def test_label_combines_cn_and_en():
    naming = FactorNaming()
    label = naming.label("lhb_institution")
    assert "lhb_institution" in label
    assert naming.cn("lhb_institution") in label
    assert naming.cn("lhb_institution") != "lhb_institution"


def test_table_merges_role_from_factors_yaml():
    """决策D：theme_crowding 在 factors.yaml 中是 filter，table() 必须如实合并。"""
    rows = {r["en"]: r for r in FactorNaming().table()}
    assert rows["theme_crowding"]["role"] == "filter"
    assert rows["lhb_institution"]["role"] == "alpha"
    # 注册因子全部出现在表里
    for name in _registered_names():
        assert name in rows


def test_missing_files_do_not_crash():
    naming = FactorNaming(aliases_path="nonexistent.yaml",
                          factors_path="nonexistent2.yaml")
    assert naming.cn("zt_dt_ratio") == "zt_dt_ratio"
    assert naming.table() == []
