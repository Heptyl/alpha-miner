"""ML模块测试 — Alpha158因子 + 标签构建 + Walk-Forward回测"""
import sys
import warnings

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, ".")

warnings.filterwarnings("ignore")


# ── Alpha158 因子测试 ──────────────────────────────────────────────────


class TestAlpha158:
    """Alpha158因子计算模块测试"""

    @pytest.fixture
    def sample_df(self):
        """构造模拟K线数据"""
        np.random.seed(42)
        n = 100
        dates = pd.date_range("2026-01-01", periods=n, freq="D")
        codes = ["000001", "000002", "600000"]
        rows = []
        for code in codes:
            base_price = 10.0 if code == "000001" else (20.0 if code == "000002" else 50.0)
            close = base_price + np.cumsum(np.random.randn(n) * 0.5)
            for i in range(n):
                c = close[i]
                rows.append({
                    "stock_code": code,
                    "trade_date": dates[i].strftime("%Y-%m-%d"),
                    "open": c * (1 + np.random.randn() * 0.01),
                    "high": c * (1 + abs(np.random.randn()) * 0.02),
                    "low": c * (1 - abs(np.random.randn()) * 0.02),
                    "close": c,
                    "volume": 1e6 * (1 + np.random.randn() * 0.3),
                    "amount": c * 1e6 * (1 + np.random.randn() * 0.3),
                    "turnover_rate": 2.0 + np.random.randn() * 0.5,
                })
        return pd.DataFrame(rows)

    def test_compute_produces_factors(self, sample_df):
        from src.factors.alpha158 import compute_alpha158
        result = compute_alpha158(sample_df)
        assert len(result) == len(sample_df)
        assert "KMID" in result.columns
        assert "ROC_5" in result.columns
        assert "VSTD_5" in result.columns

    def test_factor_count(self, sample_df):
        from src.factors.alpha158 import compute_alpha158, list_alpha158_factors
        result = compute_alpha158(sample_df)
        factor_list = list_alpha158_factors()
        # 至少80%的因子应该存在于结果中
        present = sum(1 for f in factor_list if f in result.columns)
        assert present >= len(factor_list) * 0.8, f"Only {present}/{len(factor_list)} factors present"

    def test_no_inf_values(self, sample_df):
        from src.factors.alpha158 import compute_alpha158
        result = compute_alpha158(sample_df)
        numeric_cols = result.select_dtypes(include=[np.number]).columns
        inf_count = np.isinf(result[numeric_cols].values).sum()
        assert inf_count == 0, f"Found {inf_count} inf values"

    def test_km_range(self, sample_df):
        """KMID 应该在合理范围内"""
        from src.factors.alpha158 import compute_alpha158
        result = compute_alpha158(sample_df)
        kmid = result["KMID"].dropna()
        assert (kmid > 0).all(), "KMID should be positive"
        assert (kmid < 2).all(), "KMID should be < 2 for normal stocks"


# ── 标签构建测试 ────────────────────────────────────────────────────────


class TestLabeler:
    """标签构建模块测试"""

    @pytest.fixture
    def db_with_data(self, tmp_path):
        """创建临时数据库"""
        import sqlite3
        db_path = str(tmp_path / "test.db")
        conn = sqlite3.connect(db_path)
        # 创建表
        conn.execute("""
            CREATE TABLE daily_price (
                stock_code TEXT, trade_date TEXT, open REAL, high REAL,
                low REAL, close REAL, volume REAL, amount REAL,
                turnover_rate REAL, pre_close REAL
            )
        """)
        # 插入测试数据
        for code in ["000001", "000002"]:
            for i in range(20):
                date = f"2026-01-{i+1:02d}"
                price = 10 + i * 0.5
                conn.execute(
                    "INSERT INTO daily_price VALUES (?,?,?,?,?,?,?,?,?,?)",
                    (code, date, price, price + 0.5, price - 0.5, price,
                     1e6, price * 1e6, 2.0, price - 0.5)
                )
        conn.commit()
        conn.close()
        return db_path

    def test_build_labels(self, db_with_data):
        from src.ml.labeler import build_labels
        labels = build_labels(db_path=db_with_data)
        assert len(labels) > 0
        assert "ret_1d" in labels.columns
        assert "label_1d" in labels.columns
        assert "rank_1d" in labels.columns

    def test_label_values(self, db_with_data):
        from src.ml.labeler import build_labels
        labels = build_labels(db_path=db_with_data)
        assert set(labels["label_1d"].unique()).issubset({0, 1, np.nan})


# ── Walk-Forward 回测测试 ──────────────────────────────────────────────


class TestWalkForward:
    """Walk-Forward回测框架测试"""

    def test_empty_data(self):
        from src.ml.walk_forward import WalkForwardBacktest
        wf = WalkForwardBacktest(train_days=10, test_days=5)
        empty = pd.DataFrame(columns=["stock_code", "trade_date", "ret_1d"])
        result = wf.run(empty, empty)
        assert result["metrics"]["total_trades"] == 0

    def test_synthetic_backtest(self):
        """用合成数据测试完整流程"""
        from src.ml.walk_forward import WalkForwardBacktest

        np.random.seed(42)
        n_days = 100
        n_stocks = 50
        dates = [f"2026-01-{i+1:02d}" for i in range(n_days)]
        codes = [f"{i:06d}" for i in range(n_stocks)]

        rows_feat = []
        rows_label = []
        for d in dates:
            for c in codes:
                feat1 = np.random.randn()
                feat2 = np.random.randn()
                ret = 0.3 * feat1 + 0.2 * feat2 + np.random.randn() * 0.5
                # 特征: 只含 stock_code, trade_date, 特征列
                rows_feat.append({
                    "stock_code": c, "trade_date": d,
                    "feat1": feat1, "feat2": feat2,
                })
                # 标签: stock_code, trade_date, 目标列
                rows_label.append({
                    "stock_code": c, "trade_date": d,
                    "ret_1d": ret, "ret_3d": ret * 2, "ret_5d": ret * 3,
                    "label_1d": int(ret > 0), "label_3d": int(ret > 0),
                    "label_5d": int(ret > 0), "rank_1d": np.random.rand(),
                })
        features = pd.DataFrame(rows_feat)
        labels = pd.DataFrame(rows_label)

        wf = WalkForwardBacktest(
            train_days=30,
            test_days=10,
            step_days=10,
            top_n=10,
            model_output_dir=None,
        )
        result = wf.run(features, labels)

        assert result["metrics"]["total_trades"] > 0
        assert 0 <= result["metrics"]["win_rate"] <= 1
        assert "feature_importance" in result
