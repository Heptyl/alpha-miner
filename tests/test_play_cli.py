"""USER read-only CLI and successful collection integration tests."""

from __future__ import annotations

import hashlib
import os
import sqlite3
import subprocess
import sys
import time
from dataclasses import replace
from pathlib import Path

import pytest
from click.testing import CliRunner

from src.data.limit_up_history import CollectionCheck
from src.data.storage import Storage
from src.mining.playbook import PlayCard, save_play_card

ROOT = Path(__file__).resolve().parents[1]
WAITING_MESSAGE = "暂无预计算玩法卡，等待后台任务生成"


def _storage(tmp_path: Path, name: str = "play-cli.db") -> Storage:
    storage = Storage(str(tmp_path / name))
    storage.init_db()
    return storage


def _card() -> PlayCard:
    return PlayCard(
        play_id="three_to_four_reseal",
        play_name="三进四可成交回封",
        behavior_logic="注意力瀑布后，回封检验接力资金是否重新一致。",
        signal_trade_date="2026-08-17",
        candidates=[
            {
                "stock_code": "000001",
                "stock_name": "测试股",
                "board_count": 3,
                "paper_status": "PLANNED",
            }
        ],
        trigger_rule="D日四板开板回封后按涨停收盘价代理入场。",
        abandon_rule="一字板、无量、未回封或队列不可达均放弃。",
        exit_rule="遵守T+1，D+1开盘代理退出。",
        historical_evidence={
            "signal_days": 40,
            "candidate_count": 60,
            "proxy_trigger_count": 6,
            "trigger_rate": 0.07228915662650602,
            "win_rate": 0.55,
            "avg_net_return_pct": 1.2,
            "profit_loss_ratio": 1.7200812363795115,
            "max_drawdown_pct": 6.56657247706422,
            "total_cost_bps": 20,
            "metrics_available": True,
            "data_limitations": "零样本时metrics_available=false。",
            "extension_note": None,
            "extension_text": "稳定展示",
        },
        paper_status="PLANNED",
        admission_status="NOT_ADMITTED",
        generated_at="2026-08-17T16:20:00+08:00",
    )


def _run_play(db_path: Path) -> tuple[subprocess.CompletedProcess[str], float]:
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    started = time.perf_counter()
    result = subprocess.run(
        [sys.executable, "-m", "cli", "play", "--db", str(db_path)],
        cwd=ROOT,
        env=env,
        capture_output=True,
        encoding="utf-8",
        text=True,
        timeout=5,
        check=False,
    )
    return result, time.perf_counter() - started


def _fingerprint(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    return digest, path.stat().st_mtime_ns


def test_cli_prints_complete_non_admitted_paper_without_modifying_database(tmp_path):
    storage = _storage(tmp_path)
    save_play_card(storage, _card())
    path = Path(storage.db_path)
    before = _fingerprint(path)

    result, elapsed = _run_play(path)

    assert result.returncode == 0, result.stderr
    assert result.stderr == ""
    for text in (
        "玩法：三进四可成交回封",
        "=== 今日计划 ===",
        "行为逻辑：",
        "数据日：2026-08-17",
        "PAPER/未准入（模拟记录，不是实盘建议）",
        "000001 测试股，3板",
        "明日模拟动作：若四板开板回封且代理可成交，则按涨停价模拟买入；"
        "否则自动记录未触发/未成交",
        "触发：",
        "放弃：",
        "卖出：",
        "历史证据：",
        "信号日：40",
        "历史候选：60",
        "代理触发：6",
        "触发率：7.23%",
        "胜率：55.00%",
        "平均成本后收益：1.2000%",
        "盈亏比：1.7201",
        "最大回撤：6.5666%",
        "成本：20bp",
        "指标可用：是",
        "extension_note：暂无",
        "extension_text：稳定展示",
        "首批PAPER计划尚未到结算日；下一成功采集后自动更新结果",
        "数据限制：零样本时指标不可用。",
    ):
        assert text in result.stdout
    assert "metrics_available" not in result.stdout
    assert "True" not in result.stdout
    assert "等待触发" not in result.stdout
    assert "观望" not in result.stdout
    assert "WATCH_ONLY" not in result.stdout
    assert elapsed < 5
    assert _fingerprint(path) == before


def test_cli_shows_latest_plan_and_recent_candidate_results_read_only(tmp_path):
    storage = _storage(tmp_path, "lifecycle-output.db")
    result_card = replace(
        _card(),
        signal_trade_date="2026-08-16",
        paper_status="TRIGGERED",
        candidates=[
            {
                "stock_code": "BUYSELL",
                "stock_name": "完成股",
                "board_count": 3,
                "paper_status": "COMPLETED",
                "entry_trade_date": "2026-08-17",
                "entry_price": 10.0,
                "exit_trade_date": "2026-08-18",
                "exit_price": 11.0,
                "net_return_pct": 9.8,
            },
            {
                "stock_code": "NOFIRE",
                "stock_name": "未触发股",
                "board_count": 3,
                "paper_status": "NOT_TRIGGERED",
                "result_reason": "D日未成为四板",
            },
            {
                "stock_code": "NOFILL",
                "stock_name": "未成交股",
                "board_count": 3,
                "paper_status": "UNFILLED",
                "result_reason": "D日一字板，代理不可成交",
            },
            {
                "stock_code": "WAITSELL",
                "stock_name": "待卖股",
                "board_count": 3,
                "paper_status": "TRIGGERED",
                "entry_trade_date": "2026-08-17",
                "entry_price": 12.0,
            },
        ],
    )
    save_play_card(storage, result_card)
    save_play_card(storage, _card())
    path = Path(storage.db_path)
    before = _fingerprint(path)

    result, elapsed = _run_play(path)

    assert result.returncode == 0, result.stderr
    for text in (
        "=== 今日计划 ===",
        "=== 最近PAPER结果 ===",
        "模拟买入：2026-08-17 @ 10.0000",
        "模拟卖出：2026-08-18 @ 11.0000",
        "成本后收益：+9.8000%",
        "未触发：D日未成为四板",
        "未成交：D日一字板，代理不可成交",
        "已模拟买入：2026-08-17 @ 12.0000；计划D+1开盘模拟卖出",
    ):
        assert text in result.stdout
    assert "WATCH_ONLY" not in result.stdout
    assert "等待卖出" not in result.stdout
    assert elapsed < 5
    assert _fingerprint(path) == before


@pytest.mark.parametrize("database_kind", ["missing", "no_table", "empty_table"])
def test_cli_waits_cleanly_when_no_precomputed_card(tmp_path, database_kind):
    path = tmp_path / f"{database_kind}.db"
    if database_kind == "no_table":
        sqlite3.connect(path).close()
    elif database_kind == "empty_table":
        _storage(tmp_path, path.name)

    result, _ = _run_play(path)

    assert result.returncode == 0
    assert result.stderr == ""
    assert result.stdout.strip() == WAITING_MESSAGE
    if database_kind == "missing":
        assert not path.exists()


def test_user_source_has_only_read_only_dependencies():
    source = (ROOT / "cli" / "play.py").read_text(encoding="utf-8")
    assert "mode=ro" in source
    assert "Storage" not in source
    forbidden = (
        "requests",
        "anthropic",
        "openai",
        "collector",
        "BacktestEngine",
        "EvolutionEngine",
        "LimitUpEvolutionEngine",
        "StrategyEvolver",
        "pandas",
    )
    assert all(term not in source for term in forbidden)


def test_user_runtime_does_not_load_heavy_or_external_modules(tmp_path):
    storage = _storage(tmp_path, "runtime-imports.db")
    save_play_card(storage, _card())
    script = (
        "import sys; "
        "from cli.play import run; "
        f"assert run({storage.db_path!r}) == 0; "
        "blocked=('pandas','src.data.collector','src.mining.evolution',"
        "'src.mining.limit_up_evolution','src.strategy.evolver'); "
        "assert not [name for name in blocked if name in sys.modules]"
    )
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=ROOT,
        env=env,
        capture_output=True,
        encoding="utf-8",
        text=True,
        timeout=5,
        check=False,
    )

    assert result.returncode == 0, result.stderr


def _mock_collection(status: str, *, record_ok: bool = False):
    attempts = 0

    def collect(trade_date: str, db: Storage):
        nonlocal attempts
        attempts += 1
        if record_ok:
            db.execute_write(
                """
                INSERT INTO zt_pool
                    (stock_code, trade_date, name, consecutive_zt, open_count, snapshot_time)
                VALUES ('CURRENT', ?, '当前候选', 3, 1, ?)
                """,
                (trade_date, f"{trade_date} 16:{attempts:02d}:00"),
            )
            db.execute_write(
                """
                INSERT INTO limit_up_collection_runs
                    (trade_date, attempted_at, price_rows, zt_rows, status, detail)
                VALUES (?, ?, 5000, 50, 'ok', '')
                """,
                (trade_date, f"{trade_date} 16:{attempts:02d}:30"),
            )
        check = CollectionCheck(
            trade_date=trade_date,
            price_rows=5000 if status == "ok" else 0,
            zt_rows=50 if status == "ok" else 0,
            status=status,
            detail="mock collection",
        )
        return {"zt_pool": check.zt_rows}, check

    return collect


def test_collect_success_builds_one_idempotent_card(tmp_path, monkeypatch):
    from cli import limit_up

    path = tmp_path / "collect-ok.db"
    monkeypatch.setattr(limit_up, "_collect_and_audit", _mock_collection("ok", record_ok=True))
    runner = CliRunner()

    first = runner.invoke(limit_up.main, ["collect", "--db", str(path)])
    second = runner.invoke(limit_up.main, ["collect", "--db", str(path)])

    assert first.exit_code == 0, first.output
    assert second.exit_code == 0, second.output
    rows = Storage(str(path)).execute(
        "SELECT play_id, paper_status, admission_status FROM play_cards"
    )
    assert rows == [
        {
            "play_id": "three_to_four_reseal",
            "paper_status": "PLANNED",
            "admission_status": "NOT_ADMITTED",
        }
    ]


@pytest.mark.parametrize("status", ["skipped", "missing"])
def test_collect_skipped_or_failed_does_not_build_card(tmp_path, monkeypatch, status):
    from cli import limit_up

    path = tmp_path / f"collect-{status}.db"
    monkeypatch.setattr(limit_up, "_collect_and_audit", _mock_collection(status))

    result = CliRunner().invoke(limit_up.main, ["collect", "--db", str(path)])

    assert (result.exit_code == 0) is (status == "skipped")
    assert Storage(str(path)).execute("SELECT COUNT(*) AS n FROM play_cards") == [{"n": 0}]


def test_collect_card_build_failure_is_nonzero_and_clear(tmp_path, monkeypatch):
    from cli import limit_up

    path = tmp_path / "collect-build-failure.db"
    monkeypatch.setattr(limit_up, "_collect_and_audit", _mock_collection("ok", record_ok=True))

    def fail(*args, **kwargs):
        raise ValueError("synthetic build failure")

    monkeypatch.setattr(limit_up, "build_three_to_four_card", fail)
    result = CliRunner().invoke(limit_up.main, ["collect", "--db", str(path)])

    assert result.exit_code != 0
    assert "三进四PAPER玩法卡生成失败" in result.output
    assert "synthetic build failure" in result.output
    assert Storage(str(path)).execute("SELECT COUNT(*) AS n FROM play_cards") == [{"n": 0}]


def test_collect_settles_before_building_today_card(tmp_path, monkeypatch):
    from cli import limit_up

    path = tmp_path / "collect-order.db"
    monkeypatch.setattr(limit_up, "_collect_and_audit", _mock_collection("ok", record_ok=True))
    calls = []

    def settle(storage):
        calls.append("settle")
        return []

    original_build = limit_up.build_three_to_four_card

    def build(storage, **kwargs):
        calls.append("build")
        return original_build(storage, **kwargs)

    monkeypatch.setattr(limit_up, "settle_three_to_four_cards", settle)
    monkeypatch.setattr(limit_up, "build_three_to_four_card", build)

    result = CliRunner().invoke(limit_up.main, ["collect", "--db", str(path)])

    assert result.exit_code == 0, result.output
    assert calls == ["settle", "build"]


def test_collect_settlement_failure_is_nonzero_and_clear(tmp_path, monkeypatch):
    from cli import limit_up

    path = tmp_path / "collect-settlement-failure.db"
    monkeypatch.setattr(limit_up, "_collect_and_audit", _mock_collection("ok", record_ok=True))

    def fail(*args, **kwargs):
        raise ValueError("synthetic settlement failure")

    monkeypatch.setattr(limit_up, "settle_three_to_four_cards", fail)
    result = CliRunner().invoke(limit_up.main, ["collect", "--db", str(path)])

    assert result.exit_code != 0
    assert "三进四PAPER模拟交易结算失败" in result.output
    assert "synthetic settlement failure" in result.output
    assert Storage(str(path)).execute("SELECT COUNT(*) AS n FROM play_cards") == [{"n": 0}]
