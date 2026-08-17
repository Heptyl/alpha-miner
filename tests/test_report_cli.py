"""盘后简报 CLI 的编码、只读与稀疏数据回归。"""

import os
import re
import shutil
import sqlite3
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
REPORT_DATE = (date.today() - timedelta(days=1)).isoformat()
MOJIBAKE_MARKERS = ("ÈÕ", "æ—", "ç”", "锟斤拷", "�")


def _isolated_workspace(tmp_path: Path) -> Path:
    """创建仅供子进程读取的配置和空 schema，不接触真实 data/reports。"""
    shutil.copytree(PROJECT_ROOT / "config", tmp_path / "config")
    db_path = tmp_path / "data" / "test.db"
    db_path.parent.mkdir(parents=True)
    conn = sqlite3.connect(db_path)
    try:
        schema = (PROJECT_ROOT / "src" / "data" / "schema.sql").read_text(
            encoding="utf-8"
        )
        conn.executescript(schema)
    finally:
        conn.close()
    return db_path


def _run_report(
    tmp_path: Path,
    db_path: Path,
    *args: str,
) -> tuple[subprocess.CompletedProcess[bytes], str, str]:
    env = os.environ.copy()
    env.pop("PYTHONIOENCODING", None)
    env["OPENBLAS_NUM_THREADS"] = "1"
    env["OMP_NUM_THREADS"] = "1"
    env["PYTHONPATH"] = os.pathsep.join(
        filter(None, [str(PROJECT_ROOT), env.get("PYTHONPATH", "")])
    )
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "cli",
            "report",
            "--brief",
            "--date",
            REPORT_DATE,
            "--db",
            str(db_path),
            *args,
        ],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        timeout=60,
        check=False,
    )
    stdout = result.stdout.decode("utf-8")
    stderr = result.stderr.decode("utf-8")
    for marker in MOJIBAKE_MARKERS:
        assert marker not in stdout
        assert marker not in stderr
    return result, stdout, stderr


def _assert_clean_exit(result: subprocess.CompletedProcess[bytes], stderr: str) -> None:
    assert result.returncode == 0, stderr
    assert "Traceback" not in stderr
    assert "UnicodeEncodeError" not in stderr
    assert "Mean of empty slice" not in stderr
    assert "RuntimeWarning" not in stderr


def _insert_market_emotion(
    db_path: Path, *, zt_count: int, dt_count: int, highest_board: int
) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT INTO market_emotion "
            "(trade_date, zt_count, dt_count, highest_board, snapshot_time) "
            "VALUES (?, ?, ?, ?, ?)",
            (
                REPORT_DATE,
                zt_count,
                dt_count,
                highest_board,
                f"{REPORT_DATE} 16:00:00",
            ),
        )
        conn.commit()
    finally:
        conn.close()


def _insert_one_sided_ic(db_path: Path) -> None:
    _insert_market_emotion(db_path, zt_count=10, dt_count=1, highest_board=2)
    conn = sqlite3.connect(db_path)
    try:
        codes = [f"00000{i}" for i in range(1, 6)]
        first_prices = [10.0] * 5
        second_prices = [15.0, 14.0, 13.0, 12.0, 11.0]
        third_prices = [22.5, 19.6, 16.9, 14.4, 12.1]
        report_day = date.fromisoformat(REPORT_DATE)
        factor_dates = [
            (report_day - timedelta(days=offset)).isoformat()
            for offset in (5, 4, 3)
        ]
        for trade_date, prices in (
            (factor_dates[0], first_prices),
            (factor_dates[1], second_prices),
            (factor_dates[2], third_prices),
        ):
            conn.executemany(
                "INSERT INTO daily_price "
                "(stock_code, trade_date, close, snapshot_time) VALUES (?, ?, ?, ?)",
                [
                    (code, trade_date, price, f"{trade_date} 16:00:00")
                    for code, price in zip(codes, prices)
                ],
            )
        for trade_date in factor_dates[:2]:
            conn.executemany(
                "INSERT INTO factor_values "
                "(factor_name, stock_code, trade_date, factor_value, snapshot_time) "
                "VALUES (?, ?, ?, ?, ?)",
                [
                    ("zt_dt_ratio", code, trade_date, value, f"{trade_date} 16:00:00")
                    for code, value in zip(codes, range(1, 6))
                ],
            )
        conn.commit()
    finally:
        conn.close()


def test_brief_utf8_stdout_and_default_read_only(tmp_path: Path):
    db_path = _isolated_workspace(tmp_path)
    _insert_market_emotion(db_path, zt_count=100, dt_count=0, highest_board=7)

    result, stdout, stderr = _run_report(tmp_path, db_path)

    _assert_clean_exit(result, stderr)
    assert REPORT_DATE in stdout
    assert "市场温度" in stdout
    assert "因子数据不足" in stdout
    assert "市场情绪参考仓位上限：80%（非交易建议）" in stdout
    assert "当前可执行新增仓位：0%" in stdout
    assert "建议仓位：80%" not in stdout
    assert "可操作" not in stdout
    assert not (tmp_path / "reports").exists()


def test_holdings_utf8_stdout_reports_unavailable_risk(tmp_path: Path):
    db_path = _isolated_workspace(tmp_path)

    result, stdout, stderr = _run_report(
        tmp_path, db_path, "--holdings", "600000,000001"
    )

    _assert_clean_exit(result, stderr)
    assert "持仓风险预警" in stdout
    assert re.findall(r"持仓预警：([0-9]+)", stdout) == ["600000", "000001"]
    assert "数据不足，持仓风险不可判断" in stdout
    assert not (tmp_path / "reports").exists()


@pytest.mark.parametrize(
    ("holdings", "invalid_item"),
    [
        ("600000,1", "'1'"),
        ("600000,,000001", "''"),
        ("600000,ABCDEF", "'ABCDEF'"),
    ],
)
def test_invalid_holding_code_is_rejected_before_storage(
    tmp_path: Path, holdings: str, invalid_item: str
):
    db_path = tmp_path / "storage_must_not_be_created" / "test.db"

    result, _, stderr = _run_report(
        tmp_path, db_path, "--holdings", holdings
    )

    assert result.returncode == 2
    assert "持仓代码必须是 6 位数字字符串" in stderr
    assert invalid_item in stderr
    assert "Traceback" not in stderr
    assert not db_path.parent.exists()
    assert not (tmp_path / "reports").exists()


def test_explicit_save_writes_rich_utf8_report(tmp_path: Path):
    db_path = _isolated_workspace(tmp_path)
    _insert_one_sided_ic(db_path)
    save_path = tmp_path / "saved" / "brief.txt"

    result, stdout, stderr = _run_report(tmp_path, db_path, "--save", str(save_path))

    _assert_clean_exit(result, stderr)
    assert "简报已保存" in stdout
    assert "┌" in stdout
    assert "趋势未知/数据不足" in stdout
    assert "趋势?" not in stdout
    raw_report = save_path.read_bytes()
    saved_text = raw_report.decode("utf-8")
    assert REPORT_DATE in saved_text
    assert "市场温度" in saved_text
    assert "┌" in saved_text


def test_sparse_one_sided_ic_has_no_runtime_warning(tmp_path: Path):
    db_path = _isolated_workspace(tmp_path)
    _insert_one_sided_ic(db_path)

    result, stdout, stderr = _run_report(tmp_path, db_path)

    _assert_clean_exit(result, stderr)
    assert "zt_dt_ratio" in stdout
    assert "候选" in stdout
    assert "数据不足" in stdout
    assert "☁️" in stdout
    assert "[弱]" not in stdout
    assert "趋势未知/数据不足" in stdout
    assert "趋势?" not in stdout
    assert "┌" in stdout
