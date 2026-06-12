import json
import sqlite3

from scripts import release_gate


def test_safe_state_requires_no_live_process_or_pending_signal(tmp_path, monkeypatch):
    pause = tmp_path / "daemon.pause"
    pid = tmp_path / "daemon.pid"
    pending = tmp_path / "pending.json"
    pause.touch()
    pending.write_text("[]", encoding="utf-8")

    monkeypatch.setattr(release_gate, "PAUSE_FILE", pause)
    monkeypatch.setattr(release_gate, "PID_FILE", pid)
    monkeypatch.setattr(release_gate, "PENDING_FILE", pending)

    ok, detail = release_gate.check_safe_state()
    assert ok is True
    assert "active_pending=0" in detail

    pending.write_text(json.dumps([{"status": "pending"}]), encoding="utf-8")
    ok, _ = release_gate.check_safe_state()
    assert ok is False


def test_maintenance_pause_is_a_code_release_requirement(tmp_path, monkeypatch):
    pause = tmp_path / "daemon.pause"
    monkeypatch.setattr(release_gate, "PAUSE_FILE", pause)

    assert release_gate.check_maintenance_pause()[0] is False
    pause.touch()
    assert release_gate.check_maintenance_pause()[0] is True


def test_latest_market_data_rejects_partial_day(tmp_path, monkeypatch):
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        "CREATE TABLE daily_price("
        "trade_date TEXT, stock_code TEXT, open REAL, high REAL, low REAL, "
        "close REAL, pre_close REAL)"
    )
    conn.executemany(
        "INSERT INTO daily_price VALUES(?,?,?,?,?,?,?)",
        [("2026-06-10", f"{i:06d}", 10, 11, 9, 10, 9.5) for i in range(67)],
    )
    conn.commit()
    conn.close()
    monkeypatch.setattr(release_gate, "DB_PATH", db_path)

    ok, detail = release_gate.check_latest_market_data()
    assert ok is False
    assert "stocks=67" in detail


def test_build_report_only_adds_market_data_for_paper(monkeypatch):
    monkeypatch.setattr(release_gate, "check_compile", lambda: (True, "ok"))
    monkeypatch.setattr(release_gate, "check_cli", lambda: (True, "ok"))
    monkeypatch.setattr(release_gate, "check_tests", lambda mode: (True, mode))
    monkeypatch.setattr(release_gate, "check_database", lambda: (True, "ok"))
    monkeypatch.setattr(release_gate, "check_safe_state", lambda: (True, "ok"))
    monkeypatch.setattr(release_gate, "check_maintenance_pause", lambda: (True, "ok"))
    monkeypatch.setattr(release_gate, "check_risk_and_modes", lambda: (True, "ok"))
    monkeypatch.setattr(release_gate, "check_traceability", lambda: (True, "ok"))
    monkeypatch.setattr(
        release_gate,
        "check_latest_market_data",
        lambda: (False, "partial"),
    )
    monkeypatch.setattr(
        release_gate,
        "check_active_strategy_data",
        lambda: (True, "ok"),
    )
    monkeypatch.setattr(release_gate, "check_ml_prediction", lambda: (True, "ok"))

    code_report = release_gate.build_report("code", "none")
    paper_report = release_gate.build_report("paper", "none")

    assert code_report["status"] == "pass"
    assert paper_report["status"] == "fail"


def test_paper_report_allows_degraded_unused_ml(monkeypatch):
    monkeypatch.setattr(release_gate, "check_compile", lambda: (True, "ok"))
    monkeypatch.setattr(release_gate, "check_cli", lambda: (True, "ok"))
    monkeypatch.setattr(release_gate, "check_tests", lambda mode: (True, mode))
    monkeypatch.setattr(release_gate, "check_database", lambda: (True, "ok"))
    monkeypatch.setattr(release_gate, "check_safe_state", lambda: (True, "ok"))
    monkeypatch.setattr(release_gate, "check_risk_and_modes", lambda: (True, "ok"))
    monkeypatch.setattr(release_gate, "check_traceability", lambda: (True, "ok"))
    monkeypatch.setattr(
        release_gate,
        "check_latest_market_data",
        lambda: (True, "ok"),
    )
    monkeypatch.setattr(
        release_gate,
        "check_active_strategy_data",
        lambda: (True, "ok"),
    )
    monkeypatch.setattr(
        release_gate,
        "check_ml_prediction",
        lambda: (False, "constant scores"),
    )

    report = release_gate.build_report("paper", "none")

    assert report["status"] == "pass"
    ml_check = next(item for item in report["checks"] if item["name"] == "ml_prediction")
    assert ml_check["status"] == "warn"
    assert ml_check["blocking"] is False


def test_active_strategy_data_requires_latest_zt_pool(tmp_path, monkeypatch):
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE daily_price(trade_date TEXT)")
    conn.execute(
        "CREATE TABLE zt_pool(trade_date TEXT, consecutive_zt INTEGER)"
    )
    conn.execute("INSERT INTO daily_price VALUES('2026-06-10')")
    conn.executemany(
        "INSERT INTO zt_pool VALUES('2026-06-09', ?)",
        [(2,), *[(1,)] * 20],
    )
    conn.commit()
    conn.close()
    monkeypatch.setattr(release_gate, "DB_PATH", db_path)

    ok, detail = release_gate.check_active_strategy_data()

    assert ok is False
    assert "market_date=2026-06-10" in detail
    assert "zt_date=2026-06-09" in detail


def test_ml_prediction_must_match_latest_market_date(tmp_path, monkeypatch):
    db_path = tmp_path / "test.db"
    prediction_path = tmp_path / "prediction.json"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE daily_price(trade_date TEXT)")
    conn.execute("INSERT INTO daily_price VALUES('2026-06-10')")
    conn.commit()
    conn.close()
    prediction_path.write_text(
        json.dumps(
            {
                "date": "2026-05-15",
                "all_top": [{"score": i / 100} for i in range(20)],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(release_gate, "DB_PATH", db_path)
    monkeypatch.setattr(release_gate, "PREDICTION_FILE", prediction_path)

    ok, detail = release_gate.check_ml_prediction()

    assert ok is False
    assert "prediction_date=2026-05-15" in detail


def test_ml_prediction_rejects_constant_scores(tmp_path, monkeypatch):
    db_path = tmp_path / "test.db"
    prediction_path = tmp_path / "prediction.json"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE daily_price(trade_date TEXT)")
    conn.execute("INSERT INTO daily_price VALUES('2026-06-10')")
    conn.commit()
    conn.close()
    prediction_path.write_text(
        json.dumps(
            {
                "date": "2026-06-10",
                "all_top": [{"score": -0.045461}] * 20,
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(release_gate, "DB_PATH", db_path)
    monkeypatch.setattr(release_gate, "PREDICTION_FILE", prediction_path)

    ok, detail = release_gate.check_ml_prediction()

    assert ok is False
    assert "unique_scores=1" in detail
