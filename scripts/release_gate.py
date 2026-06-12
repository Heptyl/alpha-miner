#!/usr/bin/env python3
"""Deterministic release gate for Alpha Miner test builds."""

from __future__ import annotations

import argparse
import json
import math
import os
import sqlite3
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Callable


ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "data" / "alpha_miner.db"
PAUSE_FILE = ROOT / "output" / "trader" / "daemon_logs" / "daemon.pause"
PID_FILE = ROOT / "output" / "trader" / "daemon_logs" / "daemon.pid"
PENDING_FILE = ROOT / "output" / "trader" / "signals" / "pending_signals.json"
PREDICTION_FILE = ROOT / "output" / "ml" / "latest_prediction.json"
DEFAULT_REPORT = ROOT / "output" / "release" / "release_gate.json"


@dataclass
class GateCheck:
    name: str
    status: str
    detail: str
    blocking: bool = True


def _run(
    name: str,
    fn: Callable[[], tuple[bool, str]],
    *,
    blocking: bool = True,
) -> GateCheck:
    try:
        ok, detail = fn()
        status = "pass" if ok else ("fail" if blocking else "warn")
        return GateCheck(name, status, detail, blocking=blocking)
    except Exception as exc:
        status = "fail" if blocking else "warn"
        return GateCheck(
            name,
            status,
            f"{type(exc).__name__}: {exc}",
            blocking=blocking,
        )


def _command(cmd: list[str], timeout: int) -> tuple[bool, str]:
    result = subprocess.run(
        cmd,
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=timeout,
        env={**os.environ, "PYTHONPATH": str(ROOT)},
    )
    output = (result.stdout + "\n" + result.stderr).strip()
    tail = "\n".join(output.splitlines()[-8:])
    return result.returncode == 0, tail or f"exit={result.returncode}"


def check_compile() -> tuple[bool, str]:
    return _command(
        [sys.executable, "-m", "compileall", "-q", "src", "cli", "web"],
        timeout=120,
    )


def check_cli() -> tuple[bool, str]:
    commands = [
        [sys.executable, "-m", "cli", "help"],
        [sys.executable, "-m", "cli", "mine", "--help"],
        [sys.executable, "-m", "cli", "recommend", "--help"],
        [sys.executable, "-m", "src.trader.trading_daemon", "--help"],
    ]
    for cmd in commands:
        ok, detail = _command(cmd, timeout=30)
        if not ok:
            return False, f"{' '.join(cmd)}: {detail}"
    return True, f"{len(commands)} CLI entry points passed"


def check_tests(mode: str) -> tuple[bool, str]:
    if mode == "none":
        return True, "tests skipped by request"
    if mode == "quick":
        targets = [
            "tests/test_p1a_strategy_ledger.py",
            "tests/test_p1b_brain_fail_closed.py",
            "tests/test_daemon_split.py",
            "tests/test_market_emotion_sources.py",
            "tests/test_cli_smoke.py",
            "tests/test_release_gate.py",
        ]
    else:
        targets = ["tests/", "-m", "not live", "--ignore=tests/test_collect_live.py"]
    return _command([sys.executable, "-m", "pytest", *targets, "-q"], timeout=600)


def check_database() -> tuple[bool, str]:
    if not DB_PATH.exists():
        return False, f"missing database: {DB_PATH}"
    conn = sqlite3.connect(str(DB_PATH))
    try:
        integrity = conn.execute("PRAGMA quick_check").fetchone()[0]
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        required = {
            "daily_price",
            "daemon_account",
            "daemon_positions",
            "daemon_trades",
            "daemon_runs",
            "strategy_definitions",
            "daemon_shadow_signals",
            "strategy_performance_daily",
        }
        missing = sorted(required - tables)
        if integrity != "ok" or missing:
            return False, f"quick_check={integrity}, missing={missing}"
        return True, f"quick_check=ok, required_tables={len(required)}"
    finally:
        conn.close()


def check_safe_state() -> tuple[bool, str]:
    pending = []
    if PENDING_FILE.exists():
        pending = json.loads(PENDING_FILE.read_text(encoding="utf-8"))
    active = [
        item for item in pending
        if item.get("status", "pending") in {"pending", "executing"}
    ]
    live_pid = None
    if PID_FILE.exists():
        raw = PID_FILE.read_text(encoding="utf-8").strip()
        if raw:
            pid = int(raw)
            try:
                os.kill(pid, 0)
                live_pid = pid
            except ProcessLookupError:
                pass
    ok = live_pid is None and not active
    return ok, (
        f"pause={PAUSE_FILE.exists()}, live_pid={live_pid}, "
        f"active_pending={len(active)}"
    )


def check_maintenance_pause() -> tuple[bool, str]:
    return PAUSE_FILE.exists(), f"pause={PAUSE_FILE.exists()}"


def check_risk_and_modes() -> tuple[bool, str]:
    from src.trader.daemon_config import RISK_MODE, STRATEGY_RUN_MODES

    expected = {
        "A": "paper",
        "B": "shadow",
        "C": "shadow",
        "C1": "shadow",
        "C2": "shadow",
    }
    ok = RISK_MODE == "paper" and STRATEGY_RUN_MODES == expected
    return ok, f"risk={RISK_MODE}, modes={STRATEGY_RUN_MODES}"


def check_traceability() -> tuple[bool, str]:
    from src.trader.daemon_db import build_strategy_snapshot

    snapshot = build_strategy_snapshot()
    missing = []
    for code, definition in snapshot["strategies"].items():
        for key in ("strategy_version", "entry_rule_id", "exit_rule_id", "run_mode"):
            if not definition.get(key):
                missing.append(f"{code}.{key}")
    return not missing, f"strategies={len(snapshot['strategies'])}, missing={missing}"


def check_latest_market_data() -> tuple[bool, str]:
    conn = sqlite3.connect(str(DB_PATH))
    try:
        row = conn.execute(
            """
            SELECT trade_date, COUNT(DISTINCT stock_code),
                   SUM(CASE WHEN open>0 AND high>0 AND low>0 AND close>0 THEN 1 ELSE 0 END),
                   SUM(CASE WHEN pre_close>0 THEN 1 ELSE 0 END)
            FROM daily_price
            GROUP BY trade_date
            ORDER BY trade_date DESC
            LIMIT 1
            """
        ).fetchone()
    finally:
        conn.close()
    if not row:
        return False, "daily_price is empty"
    trade_date, stocks, valid_ohlc, valid_pre_close = row
    ohlc_ratio = valid_ohlc / stocks if stocks else 0
    pre_close_ratio = valid_pre_close / stocks if stocks else 0
    age = (date.today() - datetime.strptime(trade_date, "%Y-%m-%d").date()).days
    ok = stocks >= 3500 and ohlc_ratio >= 0.95 and pre_close_ratio >= 0.95 and age <= 4
    return ok, (
        f"date={trade_date}, stocks={stocks}, ohlc={ohlc_ratio:.1%}, "
        f"pre_close={pre_close_ratio:.1%}, age_days={age}"
    )


def check_active_strategy_data() -> tuple[bool, str]:
    """Validate persisted inputs used by the only paper strategy, strategy A."""
    conn = sqlite3.connect(str(DB_PATH))
    try:
        latest_market_date = conn.execute(
            "SELECT MAX(trade_date) FROM daily_price"
        ).fetchone()[0]
        latest_zt = conn.execute(
            """
            SELECT trade_date, COUNT(*),
                   SUM(CASE WHEN consecutive_zt >= 2 THEN 1 ELSE 0 END)
            FROM zt_pool
            GROUP BY trade_date
            ORDER BY trade_date DESC
            LIMIT 1
            """
        ).fetchone()
    finally:
        conn.close()

    latest_zt_date, zt_count, leader_count = latest_zt or (None, 0, 0)
    leader_count = leader_count or 0
    ok = bool(
        latest_market_date
        and latest_zt_date == latest_market_date
        and zt_count >= 10
        and leader_count >= 1
    )
    return ok, (
        f"paper=A, market_date={latest_market_date}, zt_date={latest_zt_date}, "
        f"zt_count={zt_count}, leaders_2plus={leader_count}"
    )


def check_ml_prediction() -> tuple[bool, str]:
    if not PREDICTION_FILE.exists():
        return False, f"missing prediction: {PREDICTION_FILE}"
    payload = json.loads(PREDICTION_FILE.read_text(encoding="utf-8"))
    prediction_date = payload.get("date")
    candidates = payload.get("all_top") or payload.get("predictions") or []
    scores = [
        float(item["score"])
        for item in candidates
        if item.get("score") is not None and math.isfinite(float(item["score"]))
    ]
    unique_scores = len({round(score, 8) for score in scores})
    score_range = max(scores) - min(scores) if scores else 0.0
    conn = sqlite3.connect(str(DB_PATH))
    try:
        latest_market_date = conn.execute(
            "SELECT MAX(trade_date) FROM daily_price"
        ).fetchone()[0]
    finally:
        conn.close()
    ok = bool(
        prediction_date
        and prediction_date == latest_market_date
        and len(candidates) >= 5
        and len(scores) == len(candidates)
        and unique_scores >= 3
        and score_range > 1e-8
    )
    return ok, (
        f"prediction_date={prediction_date}, market_date={latest_market_date}, "
        f"candidates={len(candidates)}, unique_scores={unique_scores}, "
        f"score_range={score_range:.8g}"
    )


def build_report(level: str, tests: str) -> dict:
    started = time.time()
    checks = [
        _run("compile", check_compile),
        _run("cli", check_cli),
        _run("tests", lambda: check_tests(tests)),
        _run("database", check_database),
        _run("safe_state", check_safe_state),
        _run("risk_and_modes", check_risk_and_modes),
        _run("traceability", check_traceability),
    ]
    if level == "code":
        checks.append(_run("maintenance_pause", check_maintenance_pause))
    if level == "paper":
        checks.append(_run("latest_market_data", check_latest_market_data))
        checks.append(_run("active_strategy_data", check_active_strategy_data))
        # ML is a research artifact and is not consumed by any paper strategy.
        checks.append(_run("ml_prediction", check_ml_prediction, blocking=False))
    passed = all(
        item.status != "fail"
        for item in checks
        if item.blocking
    )
    return {
        "schema_version": 1,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "level": level,
        "tests": tests,
        "status": "pass" if passed else "fail",
        "duration_s": round(time.time() - started, 2),
        "checks": [asdict(item) for item in checks],
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--level", choices=("code", "paper"), default="code")
    parser.add_argument("--tests", choices=("none", "quick", "full"), default="quick")
    parser.add_argument("--json-out", type=Path, default=DEFAULT_REPORT)
    args = parser.parse_args()

    report = build_report(args.level, args.tests)
    args.json_out.parent.mkdir(parents=True, exist_ok=True)
    args.json_out.write_text(
        json.dumps(report, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print(f"release_gate level={args.level} status={report['status']}")
    for item in report["checks"]:
        print(f"  {item['status'].upper():4} {item['name']}: {item['detail']}")
    print(f"report={args.json_out}")
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
