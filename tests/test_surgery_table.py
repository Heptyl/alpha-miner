"""因子手术台测试。"""

from datetime import timedelta

import pytest

from src.mining.surgery_table import (
    EmotionIC,
    FactorSurgeryTable,
    GoldenWindow,
    RegimeIC,
    SurgeryReport,
    TimeSegmentIC,
)


# ============================================================
# 辅助函数
# ============================================================

def _make_date(index: int, start: str = "2025-01-06") -> str:
    """从起始日起生成第 index 个工作日的日期字符串。"""
    from datetime import datetime
    d = datetime.strptime(start, "%Y-%m-%d")
    added = 0
    while added < index:
        d += timedelta(days=1)
        if d.weekday() < 5:
            added += 1
    return d.strftime("%Y-%m-%d")


def _build_ic_series(
    n: int = 20,
    base_ic: float = 0.05,
    regime: str = "normal",
    zt_count: int = 40,
    ic_override: list[float] | None = None,
    regime_override: list[str] | None = None,
    zt_override: list[int] | None = None,
    start_date: str = "2025-01-06",
) -> list[dict]:
    """构建测试用 IC 序列。"""
    series = []
    for i in range(n):
        ic = ic_override[i] if ic_override else base_ic
        r = regime_override[i] if regime_override else regime
        zt = zt_override[i] if zt_override else zt_count
        series.append({
            "date": _make_date(i, start_date),
            "ic": ic,
            "regime": r,
            "zt_count": zt,
            "sample_size": 100,
        })
    return series


table = FactorSurgeryTable()


# ============================================================
# 测试
# ============================================================

class TestUniversallyEffective:
    """所有IC都 > 0.05，应诊断为 universally_effective。"""

    def test_diagnosis(self):
        # zt_count 分散在多个桶，避免触发 emotion_dependent
        zt_override = [80, 30, 10] * 7  # 21 个，够 20
        series = _build_ic_series(n=20, base_ic=0.05, zt_override=zt_override[:20])
        report = table.analyze(series, "test_factor")
        assert report.diagnosis == "universally_effective"

    def test_overall_stats(self):
        zt_override = [80, 30, 10] * 7
        series = _build_ic_series(n=20, base_ic=0.05, zt_override=zt_override[:20])
        report = table.analyze(series, "test_factor")
        assert report.overall_ic > 0.03
        # 零方差 → ICIR=inf，一定 > 0.5
        assert report.overall_icir > 0.5

    def test_best_regime_none(self):
        zt_override = [80, 30, 10] * 7
        series = _build_ic_series(n=20, base_ic=0.05, zt_override=zt_override[:20])
        report = table.analyze(series, "test_factor")
        assert report.best_regime is None
        assert report.best_emotion is None


class TestRegimeDependent:
    """仅 board_rally regime 有好IC，其他 regime IC ≈ 0。"""

    def test_diagnosis(self):
        regime_override = ["board_rally"] * 5 + ["normal"] * 15
        ic_override = [0.05] * 5 + [0.005] * 15
        series = _build_ic_series(
            n=20,
            ic_override=ic_override,
            regime_override=regime_override,
        )
        report = table.analyze(series, "test_factor")
        assert report.diagnosis == "regime_dependent"

    def test_best_regime(self):
        regime_override = ["board_rally"] * 5 + ["normal"] * 15
        ic_override = [0.05] * 5 + [0.005] * 15
        series = _build_ic_series(
            n=20,
            ic_override=ic_override,
            regime_override=regime_override,
        )
        report = table.analyze(series, "test_factor")
        assert report.best_regime == "board_rally"

    def test_regime_breakdown_effective(self):
        regime_override = ["board_rally"] * 5 + ["normal"] * 15
        ic_override = [0.05] * 5 + [0.005] * 15
        series = _build_ic_series(
            n=20,
            ic_override=ic_override,
            regime_override=regime_override,
        )
        report = table.analyze(series, "test_factor")
        regime_map = {r.regime: r for r in report.regime_breakdown}
        assert regime_map["board_rally"].effective is True
        assert regime_map["normal"].effective is False


class TestEmotionDependent:
    """仅 strong (zt_count > 60) 有好IC。"""

    def test_diagnosis(self):
        # 将 strong(高IC) 和 weak(低IC) 均匀分布在整个时间段
        # 避免前半/后半差异触发 time_decayed
        zt_override = [80, 10, 80, 10, 80, 10, 80, 10, 80, 10,
                       80, 10, 80, 10, 80, 10, 80, 10, 80, 10]
        ic_override = [0.05, 0.005, 0.05, 0.005, 0.05, 0.005, 0.05, 0.005, 0.05, 0.005,
                       0.05, 0.005, 0.05, 0.005, 0.05, 0.005, 0.05, 0.005, 0.05, 0.005]
        regime_override = ["normal"] * 20
        series = _build_ic_series(
            n=20,
            ic_override=ic_override,
            regime_override=regime_override,
            zt_override=zt_override,
        )
        report = table.analyze(series, "test_factor")
        assert report.diagnosis == "emotion_dependent"

    def test_best_emotion(self):
        zt_override = [80, 10, 80, 10, 80, 10, 80, 10, 80, 10,
                       80, 10, 80, 10, 80, 10, 80, 10, 80, 10]
        ic_override = [0.05, 0.005, 0.05, 0.005, 0.05, 0.005, 0.05, 0.005, 0.05, 0.005,
                       0.05, 0.005, 0.05, 0.005, 0.05, 0.005, 0.05, 0.005, 0.05, 0.005]
        regime_override = ["normal"] * 20
        series = _build_ic_series(
            n=20,
            ic_override=ic_override,
            regime_override=regime_override,
            zt_override=zt_override,
        )
        report = table.analyze(series, "test_factor")
        assert report.best_emotion == "strong"


class TestTimeDecayed:
    """前半段IC > 0.05，后半段IC ≈ 0。"""

    def test_diagnosis(self):
        ic_override = [0.06] * 10 + [0.005] * 10
        # zt_count 分散，避免 emotion_dependent
        zt_override = [80, 30, 10, 50, 5] * 4
        series = _build_ic_series(n=20, ic_override=ic_override, zt_override=zt_override[:20])
        report = table.analyze(series, "test_factor")
        assert report.diagnosis == "time_decayed"

    def test_time_decay_segments(self):
        ic_override = [0.06] * 10 + [0.005] * 10
        zt_override = [80, 30, 10, 50, 5] * 4
        series = _build_ic_series(n=20, ic_override=ic_override, zt_override=zt_override[:20])
        report = table.analyze(series, "test_factor")
        seg_map = {s.segment: s for s in report.time_decay}
        assert seg_map["first_half"].ic_mean > 0.03
        assert seg_map["second_half"].ic_mean < 0.015


class TestNoSignal:
    """所有IC ≈ 0。"""

    def test_diagnosis(self):
        series = _build_ic_series(n=20, base_ic=0.001)
        report = table.analyze(series, "test_factor")
        assert report.diagnosis == "no_signal"

    def test_overall_ic_near_zero(self):
        series = _build_ic_series(n=20, base_ic=0.001)
        report = table.analyze(series, "test_factor")
        assert abs(report.overall_ic) < 0.01


class TestGoldenWindowDetection:
    """5个连续高IC日 → 检测到黄金窗口。"""

    def test_golden_window_found(self):
        ic_override = [0.06] * 5 + [0.01] * 15
        series = _build_ic_series(n=20, ic_override=ic_override)
        report = table.analyze(series, "test_factor")
        assert len(report.golden_windows) >= 1

    def test_golden_window_boundaries(self):
        ic_override = [0.06] * 5 + [0.01] * 15
        series = _build_ic_series(n=20, ic_override=ic_override, start_date="2025-01-06")
        report = table.analyze(series, "test_factor")
        assert len(report.golden_windows) >= 1
        window = report.golden_windows[0]
        assert window.start_date == _make_date(0)
        assert window.end_date == _make_date(4)

    def test_golden_window_avg_ic(self):
        ic_override = [0.06] * 5 + [0.01] * 15
        series = _build_ic_series(n=20, ic_override=ic_override)
        report = table.analyze(series, "test_factor")
        window = report.golden_windows[0]
        assert window.avg_ic >= 0.045  # threshold * 1.5 = 0.045

    def test_no_golden_window_below_threshold(self):
        """IC 不够高时不应产生黄金窗口。"""
        ic_override = [0.03] * 5 + [0.01] * 15  # 0.03 < 0.045
        series = _build_ic_series(n=20, ic_override=ic_override)
        report = table.analyze(series, "test_factor")
        assert len(report.golden_windows) == 0


class TestEmptySeries:
    """空IC序列不应崩溃。"""

    def test_diagnosis(self):
        report = table.analyze([], "test_factor")
        assert report.diagnosis == "no_signal"

    def test_no_crash(self):
        report = table.analyze([], "test_factor")
        assert report.overall_ic == 0.0
        assert report.overall_icir == 0.0
        assert report.regime_breakdown == []
        assert report.emotion_breakdown == []
        assert report.time_decay == []
        assert report.golden_windows == []

    def test_suggestion_not_empty(self):
        report = table.analyze([], "test_factor")
        assert len(report.suggestion) > 0


class TestReportSuggestion:
    """每种诊断都有人类可读的建议。"""

    @pytest.mark.parametrize(
        "diagnosis",
        [
            "universally_effective",
            "regime_dependent",
            "emotion_dependent",
            "time_decayed",
            "no_signal",
        ],
    )
    def test_suggestion_exists(self, diagnosis: str):
        """每种诊断应给出非空建议。"""
        # 构造能触发特定诊断的IC序列
        if diagnosis == "universally_effective":
            zt_override = [80, 30, 10] * 7
            series = _build_ic_series(n=20, base_ic=0.05, zt_override=zt_override[:20])
        elif diagnosis == "regime_dependent":
            ic_override = [0.05] * 5 + [0.005] * 15
            regime_override = ["board_rally"] * 5 + ["normal"] * 15
            series = _build_ic_series(
                n=20, ic_override=ic_override, regime_override=regime_override,
            )
        elif diagnosis == "emotion_dependent":
            zt_override = [80, 10, 80, 10, 80, 10, 80, 10, 80, 10,
                           80, 10, 80, 10, 80, 10, 80, 10, 80, 10]
            ic_override = [0.05, 0.005, 0.05, 0.005, 0.05, 0.005, 0.05, 0.005, 0.05, 0.005,
                           0.05, 0.005, 0.05, 0.005, 0.05, 0.005, 0.05, 0.005, 0.05, 0.005]
            regime_override = ["normal"] * 20
            series = _build_ic_series(
                n=20,
                ic_override=ic_override,
                regime_override=regime_override,
                zt_override=zt_override,
            )
        elif diagnosis == "time_decayed":
            ic_override = [0.06] * 10 + [0.005] * 10
            zt_override = [80, 30, 10, 50, 5] * 4
            series = _build_ic_series(n=20, ic_override=ic_override, zt_override=zt_override[:20])
        else:  # no_signal
            series = _build_ic_series(n=20, base_ic=0.001)

        report = table.analyze(series, "test_factor")
        assert report.diagnosis == diagnosis
        assert isinstance(report.suggestion, str)
        assert len(report.suggestion) > 0


class TestDataclassStructure:
    """验证数据结构的完整性。"""

    def test_regime_ic_fields(self):
        r = RegimeIC(regime="normal", ic_mean=0.05, icir=0.6, sample_days=10, effective=True)
        assert r.regime == "normal"
        assert r.effective is True

    def test_emotion_ic_fields(self):
        e = EmotionIC(bucket="strong", ic_mean=0.04, icir=0.5, sample_days=8, effective=True)
        assert e.bucket == "strong"

    def test_time_segment_ic_fields(self):
        t = TimeSegmentIC(segment="first_half", ic_mean=0.05, sample_days=10)
        assert t.segment == "first_half"

    def test_golden_window_fields(self):
        w = GoldenWindow(start_date="2025-01-06", end_date="2025-01-10", regime="board_rally", avg_ic=0.06)
        assert w.start_date == "2025-01-06"

    def test_surgery_report_fields(self):
        r = SurgeryReport(factor_name="test", overall_ic=0.05, overall_icir=0.6)
        assert r.diagnosis == "no_signal"  # default
        assert r.golden_windows == []       # default
