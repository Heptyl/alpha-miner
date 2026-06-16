"""主控制台 dashboard 的行为测试 — 参数白名单校验 / 串行任务执行 / 页面渲染。

不绑定端口、不发 HTTP 请求：直接测 build_argv / JobRunner / render_page，
子进程用当前解释器跑微型 python -c 片段，几百毫秒内结束。
"""

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

import dashboard  # noqa: E402


# ---------------- 命令白名单与参数校验 ----------------

def test_command_ids_unique():
    ids = [c["id"] for c in dashboard.COMMANDS]
    assert len(ids) == len(set(ids))


def test_build_argv_substitutes_dates():
    cmd = dashboard.CMD_BY_ID["drift"]
    argv, err = dashboard.build_argv(cmd, {"date": "2026-06-05"})
    assert err == ""
    assert "2026-06-05" in argv
    assert "{date}" not in " ".join(argv)


def test_build_argv_rejects_bad_date():
    cmd = dashboard.CMD_BY_ID["drift"]
    argv, err = dashboard.build_argv(cmd, {"date": "2026-6-5; rm -rf /"})
    assert argv is None and "date" in err


def test_build_argv_rejects_unknown_factor():
    cmd = dashboard.CMD_BY_ID["gate"]
    argv, err = dashboard.build_argv(
        cmd, {"factor": "evil_$(whoami)", "date": "2026-06-05"})
    assert argv is None and "因子" in err


def test_build_argv_accepts_registered_factor():
    cmd = dashboard.CMD_BY_ID["gate"]
    argv, err = dashboard.build_argv(
        cmd, {"factor": "lhb_institution", "date": "2026-06-05"})
    assert err == ""
    assert "lhb_institution" in argv


def test_build_argv_requires_all_params():
    cmd = dashboard.CMD_BY_ID["checkup"]
    argv, err = dashboard.build_argv(cmd, {"start": "2026-04-01"})
    assert argv is None and "end" in err


# ---------------- 任务执行器 ----------------

def _wait_done(runner, timeout=15.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        st = runner.status(0)
        if not st["running"]:
            return st
        time.sleep(0.05)
    raise AssertionError("job did not finish in time")


def test_job_runner_captures_output_and_rc():
    runner = dashboard.JobRunner(dashboard.ROOT)
    ok, _ = runner.start("echo", ["-c", "print('hello-from-job')"])
    assert ok
    st = _wait_done(runner)
    assert st["status"] == "done" and st["rc"] == 0
    assert any("hello-from-job" in ln for ln in st["lines"])


def test_job_runner_rejects_concurrent_jobs():
    runner = dashboard.JobRunner(dashboard.ROOT)
    ok, _ = runner.start("slow", ["-c", "import time; time.sleep(8)"])
    assert ok
    ok2, msg = runner.start("second", ["-c", "print(1)"])
    assert not ok2 and "忙" in msg
    runner.stop()
    st = _wait_done(runner)
    assert st["status"] in ("done", "failed")   # 被终止 → 非0退出，如实呈现


def test_job_runner_reports_failure():
    runner = dashboard.JobRunner(dashboard.ROOT)
    runner.start("fail", ["-c", "import sys; sys.exit(3)"])
    st = _wait_done(runner)
    assert st["status"] == "failed" and st["rc"] == 3


def test_status_since_returns_incremental_lines():
    runner = dashboard.JobRunner(dashboard.ROOT)
    runner.start("multi", ["-c", "print('L1'); print('L2'); print('L3')"])
    st = _wait_done(runner)
    total = st["next"]
    assert total >= 3
    tail = runner.status(total - 1)
    assert len(tail["lines"]) == 1


# ---------------- 页面渲染 ----------------

def test_render_page_contains_commands_and_cn_factors():
    page = dashboard.render_page()
    assert "一键每日全流程" in page
    assert "机构买入" in page          # 因子中文名出现
    assert "lhb_institution" in page   # 英文名同样可见
    assert "风控过滤" in page          # 决策D role 中文化
    for c in dashboard.COMMANDS:       # 每个白名单命令都有按钮
        assert f"runCmd('{c['id']}')" in page
