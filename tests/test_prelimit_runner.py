"""Windows runner/setup contracts for the two forward pre-limit phases."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(os.name != "nt", reason="Windows PowerShell runner")

ROOT = Path(__file__).resolve().parents[1]
RUNNER = ROOT / "scripts" / "run_prelimit_capture.ps1"
SETUP = ROOT / "scripts" / "setup_prelimit_tasks.ps1"
POWERSHELL = shutil.which("powershell.exe")

CAPTURE_STDOUT = "竞价快照标准输出：完成"
CAPTURE_STDERR = "竞价快照标准错误：可诊断"
STATUS_STDOUT = "竞价状态标准输出：完整"
STATUS_STDERR = "竞价状态标准错误：可诊断"


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
if "capture-prelimit" in arguments:
    phase = arguments[arguments.index("--phase") + 1]
    Path(os.environ["FAKE_PHASE_MARKER"]).write_text(phase, encoding="ascii")
    print({CAPTURE_STDOUT!r}, flush=True)
    print({CAPTURE_STDERR!r}, file=sys.stderr, flush=True)
    raise SystemExit(int(os.environ["FAKE_CAPTURE_EXIT"]))
if arguments[-2:] == ["zt", "prelimit-status"]:
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
    [string]$Phase,
    [string]$UvCommand,
    [string]$ProjectRoot,
    [string]$LogFile
)
[Console]::OutputEncoding = [System.Text.Encoding]::GetEncoding(936)
& $Runner -Phase $Phase -UvCommand $UvCommand -ProjectRoot $ProjectRoot -LogFile $LogFile
exit $LASTEXITCODE
""",
        encoding="ascii",
    )


def _run_runner(
    tmp_path: Path,
    *,
    phase: str = "auction",
    capture_exit: int,
    status_exit: int,
    legacy_log: bool = False,
):
    fake_uv = tmp_path / "fake_uv.cmd"
    driver = tmp_path / "fake_uv_driver.py"
    scheduled_host = tmp_path / "scheduled_host.ps1"
    phase_marker = tmp_path / "phase.txt"
    status_marker = tmp_path / "status.txt"
    log = tmp_path / "runner.log"
    _write_fake_uv(fake_uv, driver)
    _write_scheduled_host(scheduled_host)
    if legacy_log:
        log.write_bytes("legacy-line\r\n".encode("utf-16"))
    env = os.environ.copy()
    env.update(
        {
            "FAKE_CAPTURE_EXIT": str(capture_exit),
            "FAKE_DRIVER": str(driver),
            "FAKE_PHASE_MARKER": str(phase_marker),
            "FAKE_PYTHON": sys.executable,
            "FAKE_STATUS_EXIT": str(status_exit),
            "FAKE_STATUS_MARKER": str(status_marker),
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
            "-Phase",
            phase,
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
    return result, log.read_bytes().decode("utf-8"), phase_marker, status_marker


def test_runner_success_logs_utf8_forwards_phase_and_runs_status(tmp_path):
    result, log, phase_marker, status_marker = _run_runner(
        tmp_path,
        phase="open",
        capture_exit=0,
        status_exit=0,
        legacy_log=True,
    )

    assert result.returncode == 0, result.stderr
    assert phase_marker.read_text(encoding="ascii") == "open"
    assert status_marker.exists()
    assert sorted(log.splitlines()) == sorted(
        ["legacy-line", CAPTURE_STDOUT, CAPTURE_STDERR, STATUS_STDOUT, STATUS_STDERR]
    )


def test_runner_propagates_capture_failure_without_status(tmp_path):
    result, log, phase_marker, status_marker = _run_runner(
        tmp_path,
        capture_exit=7,
        status_exit=0,
    )

    assert result.returncode == 7
    assert phase_marker.read_text(encoding="ascii") == "auction"
    assert not status_marker.exists()
    assert sorted(log.splitlines()) == sorted([CAPTURE_STDOUT, CAPTURE_STDERR])


def test_runner_propagates_status_failure(tmp_path):
    result, log, _, status_marker = _run_runner(
        tmp_path,
        capture_exit=0,
        status_exit=9,
    )

    assert result.returncode == 9
    assert status_marker.exists()
    assert sorted(log.splitlines()) == sorted(
        [CAPTURE_STDOUT, CAPTURE_STDERR, STATUS_STDOUT, STATUS_STDERR]
    )


def test_setup_definition_and_show_do_not_register_tasks():
    defaults = subprocess.run(
        [
            str(POWERSHELL),
            "-NoProfile",
            "-NonInteractive",
            "-File",
            str(SETUP),
            "-Action",
            "definition",
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=30,
        check=False,
    )
    assert defaults.returncode == 0, defaults.stderr
    assert "auction" in defaults.stdout and "09:26" in defaults.stdout
    assert "open" in defaults.stdout and "09:31" in defaults.stdout

    definition = subprocess.run(
        [
            str(POWERSHELL),
            "-NoProfile",
            "-NonInteractive",
            "-File",
            str(SETUP),
            "-Action",
            "definition",
            "-AuctionAt",
            "09:24",
            "-OpenAt",
            "09:32",
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=30,
        check=False,
    )
    assert definition.returncode == 0, definition.stderr
    assert "AlphaMiner-Prelimit-Auction0925" in definition.stdout
    assert "AlphaMiner-Prelimit-Open0931" in definition.stdout
    assert "auction" in definition.stdout and "09:24" in definition.stdout
    assert "open" in definition.stdout and "09:32" in definition.stdout

    show = subprocess.run(
        [
            str(POWERSHELL),
            "-NoProfile",
            "-NonInteractive",
            "-File",
            str(SETUP),
            "-Action",
            "show",
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=30,
        check=False,
    )
    assert show.returncode == 0, show.stderr
    assert "is not installed" in show.stdout

    source = SETUP.read_text(encoding="utf-8")
    assert "Register-ScheduledTask" in source
    assert "AlphaMiner-Prelimit-Auction0925" in source
    assert "AlphaMiner-Prelimit-Open0931" in source
