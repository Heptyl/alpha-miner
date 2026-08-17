"""Strict retirement contracts for the removed legacy dashboard product surface."""

import importlib.util
import os
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
RETIRED_COMMANDS = ("recommend", "signal", "strategy", "query")


def _run_cli(*args: str, cwd: Path | None = None) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env["OPENBLAS_NUM_THREADS"] = "1"
    env["OMP_NUM_THREADS"] = "1"
    env.pop("PYTHONIOENCODING", None)
    env["PYTHONPATH"] = str(ROOT) + os.pathsep + env.get("PYTHONPATH", "")
    return subprocess.run(
        [sys.executable, "-m", "cli", *args],
        cwd=cwd or ROOT,
        env=env,
        capture_output=True,
        encoding="utf-8",
        text=True,
        timeout=20,
        check=False,
    )


def test_dashboard_module_path_is_retired():
    assert not (ROOT / "scripts" / "dashboard.py").exists()


def test_dashboard_launcher_path_is_retired():
    assert not (ROOT / "dashboard.bat").exists()


def test_obsolete_cron_installer_path_is_retired():
    assert not (ROOT / "scripts" / "setup_cron.sh").exists()


@pytest.mark.parametrize("command", RETIRED_COMMANDS)
def test_deleted_cli_module_specs_are_absent(command):
    assert importlib.util.find_spec(f"cli.{command}") is None


def test_root_dispatch_has_no_retired_routes():
    source = (ROOT / "cli" / "__main__.py").read_text(encoding="utf-8")
    for command in RETIRED_COMMANDS:
        assert f'elif sub == "{command}"' not in source
        assert f"from cli.{command} import main" not in source


@pytest.mark.parametrize("command", RETIRED_COMMANDS)
def test_retired_root_commands_fail_closed(command):
    result = _run_cli(command)
    assert result.returncode == 2
    assert result.stdout == ""


@pytest.mark.parametrize("command", RETIRED_COMMANDS)
def test_retired_route_errors_name_command_and_help(command):
    result = _run_cli(command, "--help")
    assert result.returncode == 2
    assert f"Unknown command: {command}" in result.stderr
    assert "python -m cli --help" in result.stderr


def test_user_help_excludes_every_retired_command():
    result = _run_cli("--help")
    assert result.returncode == 0, result.stderr
    for command in RETIRED_COMMANDS:
        assert command not in result.stdout


def test_default_cli_remains_read_only(tmp_path):
    result = _run_cli(cwd=tmp_path)
    assert result.returncode == 0, result.stderr
    assert not (tmp_path / "data").exists()
    assert not (tmp_path / "reports").exists()


def test_explicit_play_remains_read_only(tmp_path):
    result = _run_cli("play", cwd=tmp_path)
    assert result.returncode == 0, result.stderr
    assert not (tmp_path / "data").exists()
    assert not (tmp_path / "reports").exists()


def test_zt_route_remains_available():
    result = _run_cli("zt", "--help")
    assert result.returncode == 0, result.stderr
    assert "collect" in result.stdout
    assert "status" in result.stdout


def test_report_route_remains_available():
    result = _run_cli("report", "--help")
    assert result.returncode == 0, result.stderr
    assert "--brief" in result.stdout
    assert "--holdings" in result.stdout


def test_mine_route_remains_available():
    result = _run_cli("mine", "--help")
    assert result.returncode == 0, result.stderr


def test_retired_operational_routes_are_unregistered_and_fail_closed():
    source = (ROOT / "cli" / "__main__.py").read_text(encoding="utf-8")
    for command in ("daily", "backtest", "drift", "script", "replay"):
        assert f'sub == "{command}"' not in source
        result = _run_cli(command, "--help")
        assert result.returncode == 2
        assert result.stdout == ""
        assert f"Unknown command: {command}" in result.stderr


def test_maintenance_guide_records_retirement_and_keeps_holdings_report():
    guide = (ROOT / "USER_GUIDE.md").read_text(encoding="utf-8")
    assert "已退役" in guide
    for command in RETIRED_COMMANDS:
        assert f"`{command}`" in guide
    assert 'report --brief --holdings "600000,000001"' in guide
