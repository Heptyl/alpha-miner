"""CLI 冒烟测试 — 每个子命令至少能启动不崩溃。"""
import subprocess
import sys

import os

import pytest


def run_cli(*args, timeout=10):
    """运行 CLI 命令，返回 (exit_code, stdout, stderr)。"""
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    result = subprocess.run(
        [sys.executable, "-m", "cli"] + list(args),
        capture_output=True,
        text=True,
        timeout=timeout,
        cwd=project_root,
    )
    return result.returncode, result.stdout, result.stderr


class TestCLISmoke:
    """CLI 子命令冒烟测试（不需要网络/真实数据）。"""

    def test_no_args_shows_usage(self):
        """无参数应显示用法提示。"""
        code, out, err = run_cli()
        # 不是 segfault (exit code 139) 或未处理异常就行
        assert code in (0, 1, 2), f"退出码异常: {code}\nstderr: {err}"

    def test_collect_help(self):
        """collect --help。"""
        code, out, err = run_cli("collect", "--help")
        assert code == 0, f"collect --help 失败:\n{err}"
        assert "用法" in out or "Usage" in out or "usage" in out.lower()

    def test_report_help(self):
        """report --help。"""
        code, out, err = run_cli("report", "--help")
        assert code == 0, f"report --help 失败:\n{err}"

    def test_mine_help(self):
        """mine --help。"""
        code, out, err = run_cli("mine", "--help")
        assert code == 0, f"mine --help 失败:\n{err}"

    def test_drift_help(self):
        """drift --help。"""
        code, out, err = run_cli("drift", "--help")
        assert code == 0, f"drift --help 失败:\n{err}"

    def test_backtest_help(self):
        """backtest --help。"""
        code, out, err = run_cli("backtest", "--help")
        assert code == 0, f"backtest --help 失败:\n{err}"

    def test_strategy_help(self):
        """strategy --help。"""
        code, out, err = run_cli("strategy", "--help")
        assert code == 0, f"strategy --help 失败:\n{err}"

    def test_strategy_list(self):
        """strategy list 必须输出策略列表。"""
        code, out, err = run_cli("strategy", "list")
        assert code == 0, f"strategy list 失败:\n{err}"
        assert len(out.strip()) > 0, "strategy list 输出为空"

    def test_replay_help(self):
        """replay --help。"""
        code, out, err = run_cli("replay", "--help")
        assert code == 0, f"replay --help 失败:\n{err}"

    def test_script_help(self):
        """script --help。"""
        code, out, err = run_cli("script", "--help")
        assert code == 0, f"script --help 失败:\n{err}"
