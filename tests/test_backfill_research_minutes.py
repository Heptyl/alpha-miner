from scripts.backfill_research_minutes import is_a_share, to_baostock_code


def test_universe_excludes_indices_and_non_a_share_codes():
    assert is_a_share("600000")
    assert is_a_share("300750")
    assert is_a_share("688981")
    assert not is_a_share("399001")
    assert not is_a_share("899050")


def test_baostock_code_conversion():
    assert to_baostock_code("600000") == "sh.600000"
    assert to_baostock_code("688981") == "sh.688981"
    assert to_baostock_code("000001") == "sz.000001"
