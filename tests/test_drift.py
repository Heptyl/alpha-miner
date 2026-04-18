"""Drift 模块测试 — IC Tracker + CUSUM + Regime。"""

from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pytest

from src.data.storage import Storage


# ============================================================
# 通用 fixture
# ============================================================

AS_OF = datetime(2024, 6, 14, 15, 0, 0)
SNAP = datetime(2024, 6, 13, 10, 0, 0)


def _make_db(tmp_path, with_forward=True):
    """创建带因子值和价格数据的测试数据库。"""
    storage = Storage(str(tmp_path / "test.db"))
    storage.init_db()

    codes = ["000001", "000002", "000003", "000004", "000005"]
    base_prices = [10.0, 20.0, 15.0, 8.0, 25.0]

    # 插入 10 天的 daily_price
    for i in range(10):
        d = (datetime(2024, 6, 3) + timedelta(days=i)).strftime("%Y-%m-%d")
        snap_t = datetime(2024, 6, 2, 10, 0, 0)
        rows = []
        for j, code in enumerate(codes):
            noise = np.random.RandomState(i * 10 + j).randn() * 0.5
            close = base_prices[j] + i * 0.1 + noise
            rows.append({
                "stock_code": code, "trade_date": d,
                "open": close - 0.1, "high": close + 0.2,
                "low": close - 0.2, "close": close,
                "volume": 1000, "amount": close * 1000,
                "turnover_rate": 3.0 + i * 0.1,
            })
        storage.insert("daily_price", pd.DataFrame(rows), snapshot_time=snap_t)

    # 插入 factor_values（模拟与收益正相关）
    np.random.seed(42)
    for i in range(10):
        d = (datetime(2024, 6, 3) + timedelta(days=i)).strftime("%Y-%m-%d")
        snap_t = datetime(2024, 6, 2, 10, 0, 0)
        rows = []
        for code in codes:
            # 因子值有一定噪声但总体与未来收益正相关
            rows.append({
                "factor_name": "test_factor",
                "stock_code": code,
                "trade_date": d,
                "factor_value": np.random.randn() * 0.5 + i * 0.01,
            })
        storage.insert("factor_values", pd.DataFrame(rows), snapshot_time=snap_t)

    return storage


# ============================================================
# ICTracker Tests
# ============================================================

class TestICTracker:
    def test_compute_ic_series(self, tmp_path):
        """IC 时序应返回合理的结果。"""
        from src.drift.ic_tracker import ICTracker

        db = _make_db(tmp_path)
        tracker = ICTracker(db)

        ic_df = tracker.compute_ic_series(
            "test_factor", "2024-06-03", "2024-06-12",
            forward_days=1, window=5,
        )
        # 应有 IC 记录
        assert len(ic_df) > 0
        assert "ic" in ic_df.columns
        assert "ic_ma" in ic_df.columns
        assert "icir" in ic_df.columns

    def test_compute_spearman_ic(self):
        """Spearman IC 应该在 [-1, 1] 范围。"""
        from src.drift.ic_tracker import ICTracker

        tracker = ICTracker.__new__(ICTracker)
        tracker.db = None

        fv = pd.Series([1.0, 2.0, 3.0, 4.0, 5.0, 6.0], index=[f"00{i}" for i in range(6)])
        fr = pd.Series([0.1, 0.2, 0.15, 0.3, 0.25, 0.4], index=[f"00{i}" for i in range(6)])
        ic = tracker._compute_spearman_ic(fv, fr)
        assert -1 <= ic <= 1
        assert ic > 0  # 正相关

    def test_current_status_no_data(self, tmp_path):
        """无数据时返回 no_data 状态。"""
        from src.drift.ic_tracker import ICTracker

        storage = Storage(str(tmp_path / "empty.db"))
        storage.init_db()
        tracker = ICTracker(storage)

        status = tracker.current_status("nonexistent")
        assert status["status"] == "no_data"
