"""主控制台 dashboard 的行为测试 — 参数白名单校验 / 串行任务执行 / 页面渲染。

不绑定端口、不发 HTTP 请求：直接测 build_argv / JobRunner / render_page，
子进程用当前解释器跑微型 python -c 片段，几百毫秒内结束。
"""

import json
import sqlite3
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
    for c in dashboard.COMMANDS:       # 每个白名单命令都有按钮（主按钮或折叠区）
        assert f"runCmd('{c['id']}')" in page


def test_render_page_has_health_banner_and_brief_iframe():
    page = dashboard.render_page()
    assert 'class="banner' in page          # 健康横幅存在
    assert any(ic in page for ic in ("🟢", "🟡", "🔴"))   # 三色灯之一
    assert 'id="briefframe"' in page        # 简报内嵌 iframe
    for cid in dashboard.PRIMARY_IDS:        # 四个主按钮都在首屏
        assert cid in page


# ---------------- 系统健康判断 ----------------

def test_system_state_bad_when_db_missing(monkeypatch, tmp_path):
    monkeypatch.setattr(dashboard, "DB_PATH", tmp_path / "nope.db")
    st = dashboard.system_state()
    assert st["level"] == "bad" and "不存在" in st["verdict"]


def test_system_state_flags_mining_silence(monkeypatch, tmp_path):
    """挖掘日志最后记录超阈值 → bad，verdict 点名静默天数。"""
    from datetime import date, datetime, timedelta

    # 构造一个有数据但挖掘早已静默的 fixture DB
    db = tmp_path / "t.db"
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE daily_price (trade_date TEXT)")
    conn.execute("INSERT INTO daily_price VALUES (?)", (date.today().isoformat(),))
    conn.execute("CREATE TABLE regime_state (trade_date TEXT, regime_type TEXT)")
    conn.commit(); conn.close()

    log = tmp_path / "mining.jsonl"
    old = (datetime.now() - timedelta(days=40)).isoformat()
    log.write_text(json.dumps({"timestamp": old, "name": "x"}) + "\n", encoding="utf-8")

    monkeypatch.setattr(dashboard, "DB_PATH", db)
    monkeypatch.setattr(dashboard, "MINING_LOG", log)
    st = dashboard.system_state(date.today())
    assert st["level"] == "bad"
    assert "静默" in st["verdict"]


def test_system_state_ok_when_fresh(monkeypatch, tmp_path):
    from datetime import date, datetime

    db = tmp_path / "t.db"
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE daily_price (trade_date TEXT)")
    conn.execute("INSERT INTO daily_price VALUES (?)", (date.today().isoformat(),))
    conn.execute("CREATE TABLE regime_state (trade_date TEXT, regime_type TEXT)")
    conn.execute("INSERT INTO regime_state VALUES (?, ?)",
                 (date.today().isoformat(), "normal"))
    conn.commit(); conn.close()

    log = tmp_path / "mining.jsonl"
    log.write_text(json.dumps({"timestamp": datetime.now().isoformat()}) + "\n",
                   encoding="utf-8")
    brief = tmp_path / "latest.html"
    brief.write_text("<html></html>", encoding="utf-8")

    monkeypatch.setattr(dashboard, "DB_PATH", db)
    monkeypatch.setattr(dashboard, "MINING_LOG", log)
    monkeypatch.setattr(dashboard, "BRIEF_LATEST", brief)
    st = dashboard.system_state(date.today())
    assert st["level"] == "ok"
    # 状态条覆盖关键维度
    keys = {i["k"] for i in st["items"]}
    assert {"行情数据", "挖掘管线"} <= keys
