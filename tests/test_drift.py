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


# ============================================================
# CUSUM Tests
# ============================================================

class TestCUSUM:
    def test_no_changepoint(self):
        """平稳序列无变点。"""
        from src.drift.cusum import detect_changepoints

        np.random.seed(42)
        series = pd.Series(np.random.randn(50) * 0.1)
        result = detect_changepoints(series, threshold=2.0, min_segment=10)
        assert len(result.changepoints) == 0
        assert result.series_length == 50

    def test_detect_changepoint(self):
        """有均值漂移的序列应检测到变点。"""
        from src.drift.cusum import detect_changepoints

        np.random.seed(42)
        series = pd.Series(np.concatenate([
            np.random.randn(25) + 0,   # 均值 0
            np.random.randn(25) + 3,   # 均值 3 (跳变)
        ]))
        result = detect_changepoints(series, threshold=1.0, min_segment=10)
        assert len(result.changepoints) >= 1
        # 变点应在 25 附近
        assert any(abs(cp - 25) < 10 for cp in result.changepoints)

    def test_short_series(self):
        """短序列不检测。"""
        from src.drift.cusum import detect_changepoints

        series = pd.Series([1.0, 2.0, 3.0])
        result = detect_changepoints(series)
        assert len(result.changepoints) == 0


# ============================================================
# Regime Tests
# ============================================================

class TestRegime:
    def test_normal_regime(self, tmp_path):
        """无极端数据时返回 normal。"""
        from src.drift.regime import RegimeDetector

        storage = Storage(str(tmp_path / "test.db"))
        storage.init_db()

        # 少量正常数据（混合涨跌）
        storage.insert("daily_price", pd.DataFrame([
            {"stock_code": "000001", "trade_date": "2024-06-14",
             "open": 10.0, "high": 10.5, "low": 9.5, "close": 10.1,
             "volume": 1000, "amount": 10100, "turnover_rate": 2.0},
            {"stock_code": "000002", "trade_date": "2024-06-14",
             "open": 20.0, "high": 20.5, "low": 19.5, "close": 19.8,
             "volume": 2000, "amount": 39600, "turnover_rate": 3.0},
            {"stock_code": "000003", "trade_date": "2024-06-14",
             "open": 15.0, "high": 15.5, "low": 14.5, "close": 15.2,
             "volume": 1500, "amount": 22800, "turnover_rate": 2.5},
            {"stock_code": "000004", "trade_date": "2024-06-14",
             "open": 8.0, "high": 8.5, "low": 7.5, "close": 7.8,
             "volume": 800, "amount": 6240, "turnover_rate": 1.5},
        ]), snapshot_time=SNAP)

        detector = RegimeDetector(storage)
        regime = detector.detect(AS_OF)
        assert regime.regime == "normal"

    def test_board_rally_regime(self, tmp_path):
        """连板潮 regime。"""
        from src.drift.regime import RegimeDetector

        storage = Storage(str(tmp_path / "test.db"))
        storage.init_db()

        # 30+ 涨停，最高4+连板
        zt_rows = [
            {"stock_code": f"0000{i:02d}", "trade_date": "2024-06-14",
             "consecutive_zt": 4 if i == 0 else 1, "amount": 50000,
             "circulation_mv": 200000, "open_count": 0, "zt_stats": "4/4"}
            for i in range(35)
        ]
        storage.insert("zt_pool", pd.DataFrame(zt_rows), snapshot_time=SNAP)

        # daily_price 也需要（混合涨跌避免触发 broad_move）
        price_rows = []
        for i in range(40):
            close = 10.5 if i % 2 == 0 else 9.8  # 交替涨跌
            price_rows.append({
                "stock_code": f"0000{i:02d}", "trade_date": "2024-06-14",
                "open": 10.0, "high": max(close, 10.5), "low": min(close, 9.5),
                "close": close, "volume": 1000, "amount": close * 1000,
                "turnover_rate": 3.0,
            })
        storage.insert("daily_price", pd.DataFrame(price_rows), snapshot_time=SNAP)

        detector = RegimeDetector(storage)
        regime = detector.detect(AS_OF)
        assert regime.regime == "board_rally"
        assert regime.confidence > 0
