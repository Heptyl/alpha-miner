"""CLI 冒烟测试 — 每个子命令至少能启动不崩溃。"""

import os
import subprocess
import sys

import pytest


def run_cli(*args, timeout=20, cwd=None):
    """运行 CLI 命令，返回 (exit_code, stdout, stderr)。"""
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    env = os.environ.copy()
    env["OPENBLAS_NUM_THREADS"] = "1"
    env["OMP_NUM_THREADS"] = "1"
    env.pop("PYTHONIOENCODING", None)
    env["PYTHONPATH"] = project_root + os.pathsep + env.get("PYTHONPATH", "")
    result = subprocess.run(
        [sys.executable, "-m", "cli"] + list(args),
        capture_output=True,
        encoding="utf-8",
        text=True,
        timeout=timeout,
        cwd=cwd or project_root,
        env=env,
    )
    return result.returncode, result.stdout, result.stderr


class TestCLISmoke:
    """CLI 子命令冒烟测试（不需要网络/真实数据）。"""

    def test_no_args_runs_readonly_play_in_isolated_directory(self, tmp_path):
        """无参数直接运行只读play，且不得创建默认数据库。"""
        code, out, err = run_cli(cwd=tmp_path)
        assert code == 0, err
        assert "暂无预计算玩法卡，等待后台任务生成" in out
        assert not any(marker in out for marker in ("ÈÕ", "æ—", "ç”"))
        assert not (tmp_path / "data").exists()

    @pytest.mark.parametrize("argument", ["--help", "-h", "help"])
    def test_user_help_only_lists_three_public_paths(self, argument):
        code, out, err = run_cli(argument)
        assert code == 0, err
        assert "play" in out
        assert "zt status" in out
        assert "report --brief --holdings" in out
        assert "日常主入口" in out
        assert "详细数据诊断" in out
        assert "持仓检查" in out
        assert not any(marker in out for marker in ("ÈÕ", "æ—", "ç”"))
        for hidden in (
            "collect",
            "mine",
            "drift",
            "backtest",
            "strategy",
            "recommend",
            "signal",
            "query",
        ):
            assert hidden not in out

    def test_unknown_command_is_nonzero_and_points_to_help(self):
        code, out, err = run_cli("不存在")
        assert code != 0
        assert out == ""
        assert "Unknown command: 不存在" in err
        assert "python -m cli --help" in err

    def test_collect_help(self):
        """collect --help。"""
        code, out, err = run_cli("collect", "--help")
        assert code == 0, f"collect --help 失败:\n{err}"
        assert "用法" in out or "Usage" in out or "usage" in out.lower()

    def test_report_help(self):
        """report --help。"""
        code, out, err = run_cli("report", "--help")
        assert code == 0, f"report --help 失败:\n{err}"
        assert "默认不保存" in out

    def test_mine_help(self):
        """mine --help。"""
        code, out, err = run_cli("mine", "--help")
        assert code == 0, f"mine --help 失败:\n{err}"

    @pytest.mark.parametrize(
        "command",
        [
            "recommend",
            "signal",
            "strategy",
            "query",
            "daily",
            "backtest",
            "drift",
            "script",
            "replay",
        ],
    )
    def test_retired_root_commands_are_unknown(self, command):
        code, out, err = run_cli(command, "--help")
        assert code == 2
        assert out == ""
        assert f"Unknown command: {command}" in err
        assert "python -m cli --help" in err

    def test_limit_up_help_lists_collection_loop(self):
        """zt --help 应暴露严格采集与状态入口。"""
        code, out, err = run_cli("zt", "--help")
        assert code == 0, f"zt --help 失败:\n{err}"
        assert "collect" in out
        assert "status" in out

    def test_retired_strategy_subcommand_is_unknown(self):
        code, out, err = run_cli("strategy", "list")
        assert code == 2
        assert out == ""
        assert "Unknown command: strategy" in err
