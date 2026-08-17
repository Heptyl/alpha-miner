"""Windows scheduled-task runner regression tests with an isolated fake uv command."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(os.name != "nt", reason="Windows PowerShell runner")

ROOT = Path(__file__).resolve().parents[1]
RUNNER = ROOT / "scripts" / "run_limit_up_collection.ps1"
POWERSHELL = shutil.which("powershell.exe")

COLLECT_STDOUT = "采集标准输出：完成"
COLLECT_STDERR = "采集标准错误：可诊断"
STATUS_STDOUT = "状态标准输出：通过"
STATUS_STDERR = "状态标准错误：可诊断"


def _write_fake_uv(path: Path, driver: Path) -> None:
    path.write_text(
        """@echo off\r
"%FAKE_PYTHON%" "%FAKE_DRIVER%" %*\r
exit /b %ERRORLEVEL%\r
""",
        encoding="ascii",
    )
    driver.write_text(
        f"""import os
import sys
from pathlib import Path

arguments = sys.argv[1:]
if arguments[-2:] == ["zt", "collect"]:
    print({COLLECT_STDOUT!r}, flush=True)
    print({COLLECT_STDERR!r}, file=sys.stderr, flush=True)
    raise SystemExit(int(os.environ["FAKE_COLLECT_EXIT"]))
if arguments[-3:] == ["zt", "status", "--strict"]:
    Path(os.environ["FAKE_STATUS_MARKER"]).write_text("called", encoding="ascii")
    print({STATUS_STDOUT!r}, flush=True)
    print({STATUS_STDERR!r}, file=sys.stderr, flush=True)
    raise SystemExit(int(os.environ["FAKE_STATUS_EXIT"]))
print("unexpected fake uv arguments", file=sys.stderr, flush=True)
raise SystemExit(99)
""",
        encoding="utf-8",
    )


def _write_scheduled_host(path: Path) -> None:
    path.write_text(
        """param(
    [string]$Runner,
    [string]$UvCommand,
    [string]$ProjectRoot,
    [string]$LogFile
)
[Console]::OutputEncoding = [System.Text.Encoding]::GetEncoding(936)
& $Runner -UvCommand $UvCommand -ProjectRoot $ProjectRoot -LogFile $LogFile
exit $LASTEXITCODE
""",
        encoding="ascii",
    )


def _run_runner(
    tmp_path: Path,
    *,
    collect_exit: int,
    status_exit: int,
    legacy_log: bool = False,
) -> tuple[subprocess.CompletedProcess[str], str, Path]:
    fake_uv = tmp_path / "fake_uv.cmd"
    driver = tmp_path / "fake_uv_driver.py"
    scheduled_host = tmp_path / "scheduled_host.ps1"
    marker = tmp_path / "status-called.txt"
    log = tmp_path / "runner.log"
    _write_fake_uv(fake_uv, driver)
    _write_scheduled_host(scheduled_host)
    if legacy_log:
        log.write_bytes("legacy-line\r\n".encode("utf-16"))

    env = os.environ.copy()
    env.update(
        {
            "FAKE_COLLECT_EXIT": str(collect_exit),
            "FAKE_DRIVER": str(driver),
            "FAKE_PYTHON": sys.executable,
            "FAKE_STATUS_EXIT": str(status_exit),
            "FAKE_STATUS_MARKER": str(marker),
        }
    )
    result = subprocess.run(
        [
            str(POWERSHELL),
            "-NoProfile",
            "-NonInteractive",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(scheduled_host),
            "-Runner",
            str(RUNNER),
            "-UvCommand",
            str(fake_uv),
            "-ProjectRoot",
            str(tmp_path),
            "-LogFile",
            str(log),
        ],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="strict",
        timeout=30,
        check=False,
    )
    return result, log.read_bytes().decode("utf-8"), marker


def test_runner_logs_native_stdout_stderr_and_continues_to_status(tmp_path: Path):
    result, log, marker = _run_runner(
        tmp_path,
        collect_exit=0,
        status_exit=0,
        legacy_log=True,
    )

    assert result.returncode == 0, result.stderr
    assert marker.exists()
    assert sorted(log.splitlines()) == sorted(
        ["legacy-line", COLLECT_STDOUT, COLLECT_STDERR, STATUS_STDOUT, STATUS_STDERR]
    )


def test_runner_propagates_collect_failure_without_running_status(tmp_path: Path):
    result, log, marker = _run_runner(tmp_path, collect_exit=7, status_exit=0)

    assert result.returncode == 7
    assert not marker.exists()
    assert sorted(log.splitlines()) == sorted([COLLECT_STDOUT, COLLECT_STDERR])


def test_runner_propagates_strict_status_failure(tmp_path: Path):
    result, log, marker = _run_runner(tmp_path, collect_exit=0, status_exit=9)

    assert result.returncode == 9
    assert marker.exists()
    assert sorted(log.splitlines()) == sorted(
        [COLLECT_STDOUT, COLLECT_STDERR, STATUS_STDOUT, STATUS_STDERR]
    )
