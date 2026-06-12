"""P1a 测试: 策略定义表 + daemon运行账本 + run_id贯穿"""
import json
import os
import tempfile
from datetime import datetime
from unittest.mock import patch, MagicMock

import pytest


@pytest.fixture(autouse=True)
def _tmp_db(tmp_path, monkeypatch):
    """每个测试使用独立临时数据库"""
    db_path = tmp_path / "test_alpha_miner.db"
    monkeypatch.setattr("src.trader.daemon_db.DB_PATH", db_path)
    monkeypatch.setattr("src.trader.daemon_config.DB_PATH", db_path)
    from src.trader.daemon_db import init_tables
    init_tables()
    yield db_path


def _db_conn(db_path):
    import sqlite3
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


# ── Test 1: 相同定义重复注册幂等 ──

def test_1_idempotent_registration():
    from src.trader.daemon_db import register_strategy_definition, _get_conn, DB_PATH
    defn = {"stop_loss_pct": -0.06, "trailing_pct": 0.05}
    h1 = register_strategy_definition("B_crash_v2", "B", "rule_v2", "sell_v2", defn)
    h2 = register_strategy_definition("B_crash_v2", "B", "rule_v2", "sell_v2", defn)
    assert h1 == h2, "相同定义二次注册应返回相同hash"

    conn = _db_conn(DB_PATH)
    rows = conn.execute("SELECT COUNT(*) FROM strategy_definitions WHERE strategy_version='B_crash_v2'").fetchone()
    conn.close()
    assert rows[0] == 1, "相同定义不应产生重复行"


# ── Test 2: 同版本不同hash拒绝 ──

def test_2_hash_mismatch_rejected():
    from src.trader.daemon_db import register_strategy_definition
    defn_v1 = {"stop_loss_pct": -0.06}
    defn_v2 = {"stop_loss_pct": -0.08}
    register_strategy_definition("A_v1_test", "A", "r1", "e1", defn_v1)
    with pytest.raises(ValueError, match="冲突"):
        register_strategy_definition("A_v1_test", "A", "r1", "e1", defn_v2)


def test_2b_semantic_json_match_is_idempotent():
    from src.trader.daemon_db import (
        DB_PATH, _compute_config_hash, register_strategy_definition,
    )

    definition = {"b": 2, "a": 1}
    compact_json = json.dumps(
        definition, sort_keys=True, ensure_ascii=False, separators=(",", ":")
    )
    compact_hash = __import__("hashlib").sha256(compact_json.encode()).hexdigest()
    conn = _db_conn(DB_PATH)
    conn.execute(
        "INSERT INTO strategy_definitions "
        "(strategy_version,strategy_code,entry_rule_id,exit_rule_id,"
        "definition_json,config_hash,created_at) VALUES (?,?,?,?,?,?,?)",
        ("semantic_v1", "A", "r1", "e1", compact_json, compact_hash, "2026-06-10"),
    )
    conn.commit()
    conn.close()

    result = register_strategy_definition("semantic_v1", "A", "r1", "e1", definition)
    assert result == compact_hash
    assert result != _compute_config_hash(definition)


# ── Test 3: 两次启动上下文产生不同run_id ──

def test_3_different_run_ids():
    from src.trader.daemon_db import (
        create_daemon_run, update_daemon_run,
        set_run_context, get_current_run_id,
        _current_run_id,
    )
    import src.trader.daemon_db as _mod

    rid1 = create_daemon_run(1001, "hash1")
    set_run_context(rid1, "hash1")
    assert get_current_run_id() == rid1
    update_daemon_run(rid1, "stopped")

    rid2 = create_daemon_run(1002, "hash2")
    set_run_context(rid2, "hash2")
    assert get_current_run_id() == rid2
    assert rid1 != rid2, "两次启动必须产生不同run_id"

    # cleanup
    _mod._current_run_id = None
    _mod._current_config_hash = None


# ── Test 4: 新记录可追溯到同一run ──

def test_4_records_traceable_to_same_run():
    from src.trader.daemon_db import (
        set_run_context, get_current_run_id, get_current_config_hash,
        save_candidate_snapshots, record_shadow_signal, _get_conn, DB_PATH,
    )
    import src.trader.daemon_db as _mod

    _mod._current_run_id = "test-run-001"
    _mod._current_config_hash = "test-hash-abc"

    # candidate snapshot
    save_candidate_snapshots([{
        "code": "000001", "name": "Test", "_strategy": "A",
        "score": 80, "signal_type": "test", "reason": "test",
    }])

    # shadow signal
    record_shadow_signal(
        candidate={"code": "000002", "name": "Shadow", "score": 70},
        quote={"code": "000002", "name": "Shadow", "price": 10.0},
        buy_signal={"signal_type": "shadow_test"},
        market={"phase": "正常"},
    )

    conn = _db_conn(DB_PATH)

    snap = conn.execute("SELECT run_id, config_hash FROM daemon_candidate_snapshots WHERE code='000001'").fetchone()
    assert snap["run_id"] == "test-run-001"
    assert snap["config_hash"] == "test-hash-abc"

    shadow = conn.execute("SELECT run_id, config_hash FROM daemon_shadow_signals WHERE code='000002'").fetchone()
    assert shadow["run_id"] == "test-run-001"
    assert shadow["config_hash"] == "test-hash-abc"

    conn.close()

    # cleanup
    _mod._current_run_id = None
    _mod._current_config_hash = None


# ── Test 5: 历史空run_id记录保持不变 ──

def test_5_historical_records_unchanged():
    from src.trader.daemon_db import DB_PATH
    conn = _db_conn(DB_PATH)
    # 手动插入一条无run_id的历史记录(模拟旧数据)
    conn.execute("""
        INSERT INTO daemon_trades
        (code, name, action, trade_time, trade_date, price, shares,
         amount, commission, stamp_duty, reason, signal_type, period)
        VALUES ('600000', '历史', 'buy', '2026-01-01 10:00:00', '2026-01-01',
                10.0, 100, 1000, 2.5, 0, 'test', 'test', 1)
    """)
    conn.commit()

    # 查询: run_id应为空字符串(默认值, 不是NULL)
    row = conn.execute("SELECT run_id, config_hash FROM daemon_trades WHERE code='600000'").fetchone()
    assert row["run_id"] == "", "历史记录run_id应为空字符串"
    assert row["config_hash"] == "", "历史记录config_hash应为空字符串"
    conn.close()


# ── Test 6: 重启/异常结束正确关闭run ──

def test_6_run_lifecycle():
    from src.trader.daemon_db import (
        create_daemon_run, update_daemon_run, DB_PATH,
    )
    conn = _db_conn(DB_PATH)

    # 正常停止
    rid = create_daemon_run(9999, "lifecycle_hash")
    row = conn.execute("SELECT status, stopped_at FROM daemon_runs WHERE run_id=?", (rid,)).fetchone()
    assert row["status"] == "starting"
    assert row["stopped_at"] is None

    update_daemon_run(rid, "running")
    row = conn.execute("SELECT status, stopped_at FROM daemon_runs WHERE run_id=?", (rid,)).fetchone()
    assert row["status"] == "running"
    assert row["stopped_at"] is None

    update_daemon_run(rid, "stopped")
    row = conn.execute("SELECT status, stopped_at FROM daemon_runs WHERE run_id=?", (rid,)).fetchone()
    assert row["status"] == "stopped"
    assert row["stopped_at"] is not None

    # 异常停止
    rid2 = create_daemon_run(8888, "crash_hash")
    update_daemon_run(rid2, "running")
    update_daemon_run(rid2, "crashed")
    row2 = conn.execute("SELECT status, stopped_at FROM daemon_runs WHERE run_id=?", (rid2,)).fetchone()
    assert row2["status"] == "crashed"
    assert row2["stopped_at"] is not None

    conn.close()


def test_7_run_snapshot_and_risk_mode():
    from src.trader.daemon_db import (
        build_strategy_snapshot, create_daemon_run, _compute_config_hash, DB_PATH,
    )
    from src.trader.daemon_config import RISK_MODE

    snapshot = build_strategy_snapshot()
    config_hash = _compute_config_hash(snapshot)
    run_id = create_daemon_run(7777, config_hash)

    conn = _db_conn(DB_PATH)
    row = conn.execute(
        "SELECT risk_mode, config_hash, strategy_snapshot_json "
        "FROM daemon_runs WHERE run_id=?",
        (run_id,),
    ).fetchone()
    conn.close()

    assert row["risk_mode"] == RISK_MODE
    assert row["config_hash"] == config_hash
    assert json.loads(row["strategy_snapshot_json"]) == snapshot


def test_8_pending_run_context_reaches_execute_buy():
    from src.trader.daemon_signals import _do_execute_signal

    signal = {
        "action": "buy",
        "code": "000001",
        "name": "测试股",
        "reason": "test",
        "signal_type": "首阴日内(策略A)",
        "extra": {
            "run_id": "origin-run",
            "config_hash": "origin-hash",
            "run_mode": "paper",
        },
    }
    with patch(
        "src.trader.daemon_signals.get_realtime",
        return_value={"000001": {"price": 10.0}},
    ), patch(
        "src.trader.daemon_signals.get_held_positions",
        return_value=[],
    ), patch(
        "src.trader.daemon_signals.get_account",
        return_value={"cash": 100000},
    ), patch("src.trader.trading_daemon.execute_buy") as execute_buy:
        execute_buy.return_value = {"shares": 100}
        result = _do_execute_signal(signal)

    assert result["success"] is True
    assert execute_buy.call_args.kwargs["run_id"] == "origin-run"
    assert execute_buy.call_args.kwargs["config_hash"] == "origin-hash"


def test_9_shadow_strategy_c_cannot_create_paper_orders():
    from src.trader.trading_daemon import _strategy_allows_paper_orders

    with patch(
        "src.trader.trading_daemon.get_strategy_metadata",
        return_value={"run_mode": "shadow"},
    ):
        assert _strategy_allows_paper_orders("C") is False

    with patch(
        "src.trader.trading_daemon.get_strategy_metadata",
        return_value={"run_mode": "paper"},
    ):
        assert _strategy_allows_paper_orders("C") is True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
