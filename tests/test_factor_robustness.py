"""测试因子计算健壮性 — 空数据/NaN/边界条件。"""
import pytest
import pandas as pd
import numpy as np
from datetime import datetime

from src.data.storage import Storage


@pytest.fixture
def db(tmp_path):
    db = Storage(str(tmp_path / "test.db"))
    db.init_db()
    return db


@pytest.fixture
def populated_db(db):
    """有数据的数据库。"""
    snap = datetime(2024, 6, 14, 10, 0, 0)
    codes = [f"00000{i}" for i in range(5)]

    # daily_price
    rows = []
    for code in codes:
        rows.append({
            "stock_code": code, "trade_date": "2024-06-14",
            "open": 10.0, "high": 11.0, "low": 9.0, "close": 10.5,
            "volume": 1000000, "amount": 10500000, "turnover_rate": 5.0,
        })
    db.insert("daily_price", pd.DataFrame(rows), snapshot_time=snap)

    # zt_pool
    db.insert("zt_pool", pd.DataFrame([
        {"stock_code": "000000", "trade_date": "2024-06-14",
         "consecutive_zt": 3, "amount": 50000, "circulation_mv": 200000,
         "open_count": 0, "zt_stats": "3/3"},
    ]), snapshot_time=snap)

    # fund_flow
    db.insert("fund_flow", pd.DataFrame([
        {"stock_code": "000000", "trade_date": "2024-06-14",
         "super_large_net": 500000, "large_net": 500000,
         "medium_net": -200000, "small_net": -300000,
         "main_net": 1000000},
        {"stock_code": "000001", "trade_date": "2024-06-14",
         "super_large_net": -300000, "large_net": -200000,
         "medium_net": 100000, "small_net": -100000,
         "main_net": -500000},
    ]), snapshot_time=snap)

    return db


class TestFormulaFactorsRobust:
    """公式因子在空数据/边界条件下必须返回有效值，不崩溃。"""

    def test_zt_ratio_empty_db(self, db):
        """zt_dt_ratio 空数据库返回默认值。"""
        from src.factors.formula.zt_ratio import ZtDtRatioFactor
        f = ZtDtRatioFactor()
        result = f.compute(["000001"], datetime(2024, 6, 14), db)
        assert isinstance(result, pd.Series)
        assert len(result) == 1
        # 空数据应返回 0.5（中性）
        assert result.iloc[0] == 0.5

    def test_zt_ratio_with_data(self, populated_db):
        """zt_dt_ratio 有数据时返回合理比例。"""
        from src.factors.formula.zt_ratio import ZtDtRatioFactor
        f = ZtDtRatioFactor()
        result = f.compute(["000000"], datetime(2024, 6, 14, 12, 0, 0), populated_db)
        assert isinstance(result, pd.Series)
        val = result.iloc[0]
        assert 0.0 <= val <= 1.0, f"zt_dt_ratio={val} 超出 [0,1]"

    def test_turnover_rank_empty(self, db):
        """turnover_rank 空数据不崩溃。"""
        from src.factors.formula.turnover_rank import TurnoverRankFactor
        f = TurnoverRankFactor()
        result = f.compute(["000001", "000002"], datetime(2024, 6, 14), db)
        assert isinstance(result, pd.Series)
        assert len(result) >= 1

    def test_turnover_rank_with_data(self, populated_db):
        """turnover_rank 有数据返回排名值。"""
        from src.factors.formula.turnover_rank import TurnoverRankFactor
        f = TurnoverRankFactor()
        result = f.compute(
            [f"00000{i}" for i in range(5)],
            datetime(2024, 6, 14, 12, 0, 0),
            populated_db,
        )
        assert isinstance(result, pd.Series)
        # 排名值应在 [0, 1] 范围
        assert (result >= 0).all() and (result <= 1).all(), \
            f"turnover_rank 值超出 [0,1]: {result.values}"

    def test_consecutive_board_empty(self, db):
        """consecutive_board 空数据不崩溃。"""
        from src.factors.formula.consecutive_board import ConsecutiveBoardFactor
        f = ConsecutiveBoardFactor()
        result = f.compute(["000001"], datetime(2024, 6, 14), db)
        assert isinstance(result, pd.Series)
        assert len(result) == 1

    def test_main_flow_intensity_empty(self, db):
        """main_flow_intensity 空数据不崩溃。"""
        from src.factors.formula.main_flow_intensity import MainFlowIntensityFactor
        f = MainFlowIntensityFactor()
        result = f.compute(["000001"], datetime(2024, 6, 14), db)
        assert isinstance(result, pd.Series)

    def test_main_flow_intensity_with_data(self, populated_db):
        """main_flow_intensity 有数据返回合理值。"""
        from src.factors.formula.main_flow_intensity import MainFlowIntensityFactor
        f = MainFlowIntensityFactor()
        result = f.compute(
            ["000000", "000001"],
            datetime(2024, 6, 14, 12, 0, 0),
            populated_db,
        )
        assert isinstance(result, pd.Series)
        # 000000 主力净流入正，000001 负，应有区分
        if len(result) == 2:
            assert result.iloc[0] != result.iloc[1], \
                "两只资金流向不同的股票应该有不同因子值"


class TestNarrativeFactorsRobust:
    """叙事因子在无新闻数据时不应崩溃。"""

    def test_narrative_velocity_empty(self, db):
        """narrative_velocity 无新闻不崩溃。"""
        from src.factors.narrative.narrative_velocity import NarrativeVelocityFactor
        f = NarrativeVelocityFactor()
        result = f.compute(["000001"], datetime(2024, 6, 14), db)
        assert isinstance(result, pd.Series)

    def test_theme_lifecycle_empty(self, db):
        """theme_lifecycle 无概念数据不崩溃。"""
        from src.factors.narrative.theme_lifecycle import ThemeLifecycleFactor
        f = ThemeLifecycleFactor()
        result = f.compute(["000001"], datetime(2024, 6, 14), db)
        assert isinstance(result, pd.Series)

    def test_leader_clarity_empty(self, db):
        """leader_clarity 无涨停池数据不崩溃。"""
        from src.factors.narrative.leader_clarity import LeaderClarityFactor
        f = LeaderClarityFactor()
        result = f.compute(["000001"], datetime(2024, 6, 14), db)
        assert isinstance(result, pd.Series)

    def test_theme_crowding_empty(self, db):
        """theme_crowding 无数据不崩溃。"""
        from src.factors.narrative.theme_crowding import ThemeCrowdingFactor
        f = ThemeCrowdingFactor()
        result = f.compute(["000001"], datetime(2024, 6, 14), db)
        assert isinstance(result, pd.Series)


class TestFactorRegistry:
    """因子注册表完整性。"""

    def test_registry_lists_all_factors(self):
        """注册表必须能列出所有已注册因子。"""
        from src.factors.registry import FactorRegistry
        reg = FactorRegistry()
        reg.load_from_yaml()
        factors = reg.list_factors()
        assert len(factors) >= 8, f"注册因子只有 {len(factors)} 个，期望 >= 8"

    def test_each_factor_has_required_attributes(self):
        """每个因子必须有 name / factor_type / compute。"""
        from src.factors.registry import FactorRegistry
        reg = FactorRegistry()
        reg.load_from_yaml()
        for name in reg.list_factors():
            f = reg.get_factor(name)
            assert f.name, f"{f.__class__.__name__} 缺少 name"
            assert f.factor_type in ("market", "stock"), \
                f"{f.name} 的 factor_type={f.factor_type} 不合法"
            assert hasattr(f, "compute"), f"{f.name} 缺少 compute 方法"
