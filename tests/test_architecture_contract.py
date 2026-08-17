"""Protect the few product rules that keep Alpha Miner simple and honest."""

from pathlib import Path

ARCHITECTURE = Path(__file__).resolve().parents[1] / "ARCHITECTURE.md"


def _text() -> str:
    return ARCHITECTURE.read_text(encoding="utf-8")


def test_architecture_stays_small_with_one_engine_database_entry_and_card():
    text = _text()
    assert len(text.splitlines()) <= 120
    for rule in ("一个引擎", "一个数据库", "一个 USER 入口", "一种玩法卡"):
        assert rule in text
    assert "<=5秒" in text
    assert "不得新增独立 CLI 或第四套引擎" in text


def test_user_buy_points_are_forward_and_before_limit_up():
    text = _text()
    for rule in (
        "D-1 涨停/连板候选",
        "D 日 9:25",
        "09:31 用可成交代理",
        "上涨途中分钟量价冲板",
        "股票尚未涨停",
        "高位炸板/题材退潮卖出回避",
        "主要买点必须在涨停前",
    ):
        assert rule in text


def test_intraday_data_gate_cannot_be_replaced_by_daily_outcome():
    text = _text()
    for required in (
        "9:25 带时间戳快照",
        "09:31 分钟 VWAP/成交",
        "1/5 分钟量价序列",
        "数据未具备",
    ):
        assert required in text
    assert "涨停价回封只能作为研究/成交审计代理" in text
    assert "不得用盘后日线、`open_count` 或最终封板结果" in text
    assert "不得反向生成当天买入" in text


def test_windows_collects_server_backtests_and_user_never_waits():
    text = _text()
    assert "Windows 负责免费数据的前向采集与发布" in text
    assert "服务器负责离线回测和慢实验" in text
    assert "慢任务永远不阻塞 USER" in text


def test_paper_and_locked_test_discipline_remain_separate_from_admission():
    text = _text()
    assert "PAPER 与实盘准入" in text
    assert "不能退化为 `WATCH_ONLY`" in text
    assert "未准入仓位仍为 0" in text
    assert "锁定测试只评估一次" in text
    assert "失败候选及后代不得复用" in text
    assert "按**信号日**处理" in text
    for rule in ("成本", "滑点", "不可成交"):
        assert rule in text
