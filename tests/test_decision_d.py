"""决策D：因子角色(alpha/filter) + 叙事母题标注层接口 测试。"""

from src.data.storage import Storage
from src.factors.registry import FactorRegistry


def test_theme_crowding_is_filter():
    reg = FactorRegistry()
    assert reg.get_role("theme_crowding") == "filter"
    assert reg.get_role("lhb_institution") == "alpha"


def test_list_factors_by_role():
    reg = FactorRegistry()
    alphas = reg.list_factors(role="alpha")
    filters = reg.list_factors(role="filter")
    assert "theme_crowding" in filters
    assert "theme_crowding" not in alphas
    assert "lhb_institution" in alphas
    # 无参仍返回全部（向后兼容）
    assert set(reg.list_factors()) == set(alphas) | set(filters)


def test_narrative_archetype_table(tmp_path):
    s = Storage(str(tmp_path / "t.db"))
    s.init_db()
    cols = s.execute("PRAGMA table_info(narrative_archetype)")
    names = {c["name"] for c in cols}
    assert {"archetype", "schelling_source", "quant_blindspot", "is_human_supply"} <= names
