from __future__ import annotations

import json
import shutil
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

import pytest

from src.brain.controller import BrainController
from src.brain.executor import AgentExecution, AgentInvocation, CodexAgentExecutor
from src.brain.runtime import BrainLockedError, RunLock, write_json

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCHEMA_SOURCE = PROJECT_ROOT / "control" / "schemas"


def git(repo: Path, *args: str) -> str:
    completed = subprocess.run(
        ["git", *args],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    return completed.stdout.strip()


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    root = tmp_path / "repo"
    root.mkdir()
    git(root, "init")
    git(root, "config", "user.name", "Brain Test Automation")
    git(root, "config", "user.email", "brain-test@example.invalid")

    (root / ".gitignore").write_text("/.brain/\n", encoding="utf-8")
    (root / "business.txt").write_text("base\n", encoding="utf-8")
    (root / "dirty.txt").write_text("owner baseline\n", encoding="utf-8")
    (root / "forbidden.txt").write_text("protected\n", encoding="utf-8")
    shutil.copytree(SCHEMA_SOURCE, root / "control" / "schemas")
    git(root, "add", ".")
    git(root, "commit", "-m", "fixture baseline")

    (root / "dirty.txt").write_text("owner uncommitted task-1 work\n", encoding="utf-8")
    return root


def write_task(repo: Path, *, allowed_paths: list[str] | None = None) -> Path:
    task = {
        "objective": "Implement the bounded fixture change.",
        "scope": ["Change only the fixture business file."],
        "allowed_paths": allowed_paths or ["business.txt"],
        "acceptance_criteria": ["criterion-a", "criterion-b"],
        "prohibitions": ["No commit, push, deploy, reset, clean, checkout, or stash."],
        "round": 1,
    }
    path = repo / "TASK.json"
    path.write_text(json.dumps(task), encoding="utf-8")
    return path


def rd_result(round_number: int, changed_files: list[str]) -> dict[str, object]:
    return {
        "round": round_number,
        "summary": f"RD round {round_number} complete.",
        "changed_files": changed_files,
        "test_commands": [
            {"command": "pytest fixture", "exit_code": 0, "evidence": "fixture passed"}
        ],
        "evidence": ["bounded evidence"],
        "risks": [],
    }


def pm_review(
    round_number: int,
    decision: str,
    *,
    failed: list[str] | None = None,
    evidence: list[str] | None = None,
    revision_scope: list[str] | None = None,
) -> dict[str, object]:
    return {
        "round": round_number,
        "decision": decision,
        "failed_acceptance_items": failed or [],
        "evidence": evidence if evidence is not None else ["acceptance evidence"],
        "revision_scope": revision_scope or [],
    }


Mutation = Callable[[AgentInvocation], None]


@dataclass
class FakeStep:
    role: str
    output: dict[str, object] | str
    mutate: Mutation | None = None
    exit_code: int = 0
    termination_reason: str | None = None
    error_summary: str | None = None


class FakeExecutor:
    def __init__(self, steps: list[FakeStep]) -> None:
        self.steps = list(steps)
        self.calls: list[AgentInvocation] = []

    def run(self, invocation: AgentInvocation) -> AgentExecution:
        assert self.steps, "unexpected agent invocation"
        step = self.steps.pop(0)
        assert invocation.role == step.role
        self.calls.append(invocation)
        if step.mutate:
            step.mutate(invocation)
        invocation.output_path.parent.mkdir(parents=True, exist_ok=True)
        if isinstance(step.output, str):
            invocation.output_path.write_text(step.output, encoding="utf-8")
        else:
            invocation.output_path.write_text(json.dumps(step.output), encoding="utf-8")
        return AgentExecution(
            exit_code=step.exit_code,
            termination_reason=step.termination_reason,
            error_summary=step.error_summary,
        )


def write_business(repo: Path, value: str) -> Mutation:
    def mutate(_invocation: AgentInvocation) -> None:
        (repo / "business.txt").write_text(value, encoding="utf-8")

    return mutate


def new_controller(repo: Path, executor: FakeExecutor) -> BrainController:
    return BrainController(repo, executor=executor)


def test_rd_complete_then_pm_accepts(repo: Path) -> None:
    task = write_task(repo)
    executor = FakeExecutor(
        [
            FakeStep("rd", rd_result(1, ["business.txt"]), write_business(repo, "done\n")),
            FakeStep("pm", pm_review(1, "accepted")),
        ]
    )

    result = new_controller(repo, executor).run(task)

    assert result["state"] == "accepted"
    assert [call.role for call in executor.calls] == ["rd", "pm"]
    run_dir = repo / ".brain" / "runs" / result["run_id"] / "round-01"
    assert (run_dir / "TASK.json").is_file()
    assert (run_dir / "RD_RESULT.json").is_file()
    assert (run_dir / "PM_REVIEW.json").is_file()
    baseline = json.loads((run_dir / "GIT_BASELINE.json").read_text(encoding="utf-8"))
    assert "dirty.txt" in baseline["dirty_paths"]


def test_pm_rejects_new_rd_repairs_then_pm_accepts(repo: Path) -> None:
    task = write_task(repo)
    executor = FakeExecutor(
        [
            FakeStep("rd", rd_result(1, ["business.txt"]), write_business(repo, "v1\n")),
            FakeStep(
                "pm",
                pm_review(
                    1,
                    "changes_requested",
                    failed=["criterion-b"],
                    evidence=["v1 lacks the required marker"],
                    revision_scope=["Update business.txt only."],
                ),
            ),
            FakeStep("rd", rd_result(2, ["business.txt"]), write_business(repo, "v2\n")),
            FakeStep("pm", pm_review(2, "accepted")),
        ]
    )

    result = new_controller(repo, executor).run(task)

    assert result["state"] == "accepted"
    assert [call.role for call in executor.calls] == ["rd", "pm", "rd", "pm"]
    assert len({id(call) for call in executor.calls}) == 4
    assert all("--ephemeral" in call.command for call in executor.calls)
    assert all("resume" not in call.command for call in executor.calls)
    revision_file = (
        repo / ".brain" / "runs" / result["run_id"] / "round-02" / "TASK.json"
    )
    revision = json.loads(revision_file.read_text(encoding="utf-8"))
    assert revision["acceptance_criteria"] == ["criterion-b"]
    assert revision["scope"] == ["Update business.txt only."]
    assert revision["revision"]["evidence"] == ["v1 lacks the required marker"]
    assert "RD round 1 complete" not in revision_file.read_text(encoding="utf-8")


def test_three_consecutive_rejections_block(repo: Path) -> None:
    task = write_task(repo)
    steps: list[FakeStep] = []
    for round_number in range(1, 4):
        steps.extend(
            [
                FakeStep(
                    "rd",
                    rd_result(round_number, ["business.txt"]),
                    write_business(repo, f"round-{round_number}\n"),
                ),
                FakeStep(
                    "pm",
                    pm_review(
                        round_number,
                        "changes_requested",
                        failed=["criterion-a"],
                        evidence=[f"round {round_number} still fails"],
                        revision_scope=["business.txt only"],
                    ),
                ),
            ]
        )
    executor = FakeExecutor(steps)

    result = new_controller(repo, executor).run(task)

    assert result["state"] == "blocked"
    assert result["current_round"] == 3
    assert result["reason"] == "maximum revision rounds reached"
    assert len(executor.calls) == 6


def test_pm_is_always_read_only_with_approval_never(repo: Path) -> None:
    task = write_task(repo)
    executor = FakeExecutor(
        [FakeStep("rd", rd_result(1, [])), FakeStep("pm", pm_review(1, "accepted"))]
    )

    result = new_controller(repo, executor).run(task)

    assert result["state"] == "accepted"
    rd_call, pm_call = executor.calls
    assert rd_call.command[:2] == ("codex", "exec")
    assert "--dangerously-bypass-approvals-and-sandbox" in rd_call.command
    assert pm_call.command[:4] == ("codex", "--ask-for-approval", "never", "exec")
    sandbox_index = pm_call.command.index("--sandbox")
    assert pm_call.command[sandbox_index + 1] == "read-only"
    assert "--dangerously-bypass-approvals-and-sandbox" not in pm_call.command

    def illegal_pm_write(_invocation: AgentInvocation) -> None:
        (repo / "forbidden.txt").write_text("PM attempted a write\n", encoding="utf-8")

    violating_executor = FakeExecutor(
        [
            FakeStep("rd", rd_result(1, [])),
            FakeStep("pm", pm_review(1, "accepted"), illegal_pm_write),
        ]
    )
    violating_result = new_controller(repo, violating_executor).run(task)
    assert violating_result["state"] == "blocked"
    assert "PM read-only invariant violated" in violating_result["reason"]


def test_change_outside_allowed_paths_blocks_before_pm(repo: Path) -> None:
    task = write_task(repo)

    def cross_scope(_invocation: AgentInvocation) -> None:
        (repo / "forbidden.txt").write_text("changed\n", encoding="utf-8")

    executor = FakeExecutor([FakeStep("rd", rd_result(1, ["forbidden.txt"]), cross_scope)])

    result = new_controller(repo, executor).run(task)

    assert result["state"] == "blocked"
    assert "TASK.allowed_paths" in result["reason"]
    assert [call.role for call in executor.calls] == ["rd"]


def test_malformed_or_schema_nonconforming_agent_json_blocks(repo: Path) -> None:
    task = write_task(repo)
    invalid_outputs: list[tuple[dict[str, object] | str, str]] = [
        ("{not-json", "invalid JSON in RD_RESULT.json"),
        (
            {
                "round": 1,
                "summary": "Missing required fields.",
                "changed_files": [],
            },
            "missing required fields",
        ),
    ]
    for output, expected_reason in invalid_outputs:
        executor = FakeExecutor([FakeStep("rd", output)])
        result = new_controller(repo, executor).run(task)
        assert result["state"] == "blocked"
        assert expected_reason in result["reason"]
        assert [call.role for call in executor.calls] == ["rd"]


def test_agent_output_schemas_use_codex_structured_output_subset() -> None:
    business_keywords = {
        "minimum",
        "maximum",
        "exclusiveMinimum",
        "exclusiveMaximum",
        "multipleOf",
        "minLength",
        "maxLength",
        "pattern",
        "format",
        "minItems",
        "maxItems",
        "uniqueItems",
        "oneOf",
        "allOf",
        "not",
        "dependentRequired",
        "dependentSchemas",
        "if",
        "then",
        "else",
        "const",
    }

    def keys(value: object) -> set[str]:
        if isinstance(value, dict):
            return set(value).union(*(keys(child) for child in value.values()))
        if isinstance(value, list):
            return set().union(*(keys(child) for child in value))
        return set()

    for name in ("RD_RESULT", "PM_REVIEW"):
        schema = json.loads((SCHEMA_SOURCE / f"{name}.schema.json").read_text(encoding="utf-8"))
        assert keys(schema).isdisjoint(business_keywords)


def test_controller_enforces_business_rules_removed_from_output_schemas(repo: Path) -> None:
    task = write_task(repo)
    empty_summary = rd_result(1, [])
    empty_summary["summary"] = ""
    duplicate_paths = rd_result(1, ["business.txt", "business.txt"])
    rd_cases = [
        (empty_summary, "at least 1 character"),
        (duplicate_paths, "must contain unique items"),
    ]
    for output, expected_reason in rd_cases:
        result = new_controller(repo, FakeExecutor([FakeStep("rd", output)])).run(task)
        assert result["state"] == "blocked"
        assert expected_reason in result["reason"]

    invalid_pm_cases = [
        (
            pm_review(
                1,
                "accepted",
                failed=["criterion-a"],
                revision_scope=["business.txt only"],
            ),
            "must not contain failed items",
        ),
        (pm_review(1, "changes_requested"), "at least 1 failed acceptance item"),
    ]
    for output, expected_reason in invalid_pm_cases:
        executor = FakeExecutor(
            [FakeStep("rd", rd_result(1, [])), FakeStep("pm", output)]
        )
        result = new_controller(repo, executor).run(task)
        assert result["state"] == "blocked"
        assert expected_reason in result["reason"]


def test_nonzero_agent_error_is_redacted_bounded_and_persisted(
    repo: Path, tmp_path: Path
) -> None:
    raw_dialog = "ORIGINAL USER DIALOG MUST NOT PERSIST"
    command = (
        "import sys; "
        f"sys.stderr.write('user\\n{raw_dialog}\\n'); "
        "sys.stderr.write('ERROR: request failed token=very-secret-token\\n'); "
        "sys.stderr.write('endpoint=https://example.invalid/private\\n'); "
        "sys.stderr.write('address=198.51.100.42:443\\n'); "
        "sys.stderr.write('path=C:\\\\Users\\\\owner\\\\private\\\\file.txt\\n'); "
        "sys.stderr.write('detail=' + 'x' * 5000 + '\\n'); "
        "raise SystemExit(1)"
    )
    invocation = AgentInvocation(
        role="rd",
        round=1,
        command=(sys.executable, "-c", command),
        prompt="",
        output_path=tmp_path / "unused-output.json",
        schema_path=tmp_path / "unused-schema.json",
        permissions={"approval": "test", "sandbox": "test"},
        cwd=tmp_path,
        run_id="stderr-run",
        stop_path=tmp_path / "none.json",
        timeout_seconds=10,
    )

    execution = CodexAgentExecutor().run(invocation)

    assert execution.exit_code == 1
    assert execution.error_summary is not None
    assert len(execution.error_summary.encode("utf-8")) <= 4096
    assert raw_dialog not in execution.error_summary
    assert "very-secret-token" not in execution.error_summary
    assert "example.invalid" not in execution.error_summary
    assert "198.51.100.42" not in execution.error_summary
    assert "C:\\Users\\owner" not in execution.error_summary
    assert "<REDACTED_SECRET>" in execution.error_summary
    assert "<REDACTED_ADDRESS>" in execution.error_summary
    assert "<REDACTED_USER_PATH>" in execution.error_summary

    task = write_task(repo)
    controller_execution = FakeExecutor(
        [
            FakeStep(
                "rd",
                rd_result(1, []),
                exit_code=execution.exit_code,
                error_summary=execution.error_summary,
            )
        ]
    )
    state = new_controller(repo, controller_execution).run(task)
    persisted = json.loads((repo / ".brain" / "state.json").read_text(encoding="utf-8"))
    assert state["state"] == "blocked"
    assert state["agent_error"] == persisted["agent_error"]
    assert state["agent_error"]["role"] == "RD"
    assert state["agent_error"]["exit_code"] == 1
    assert state["agent_error"]["summary"] == execution.error_summary
    assert len(state["agent_error"]["summary"].encode("utf-8")) <= 4096


def test_git_head_change_blocks_before_pm(repo: Path) -> None:
    task = write_task(repo)

    def commit_change(_invocation: AgentInvocation) -> None:
        (repo / "business.txt").write_text("committed by fake agent\n", encoding="utf-8")
        git(repo, "add", "business.txt")
        git(repo, "commit", "-m", "simulated forbidden agent commit")

    initial_head = git(repo, "rev-parse", "HEAD")
    executor = FakeExecutor(
        [FakeStep("rd", rd_result(1, ["business.txt"]), commit_change)]
    )

    result = new_controller(repo, executor).run(task)

    assert result["state"] == "blocked"
    assert result["reason"] == "Git HEAD changed during an agent run"
    assert git(repo, "rev-parse", "HEAD") != initial_head
    assert [call.role for call in executor.calls] == ["rd"]


def test_existing_dirty_outside_scope_is_preserved_and_allowed_dirty_can_change(
    repo: Path,
) -> None:
    task = write_task(repo)
    dirty_before = (repo / "dirty.txt").read_bytes()
    task_before = task.read_bytes()
    executor = FakeExecutor(
        [
            FakeStep("rd", rd_result(1, ["business.txt"]), write_business(repo, "done\n")),
            FakeStep("pm", pm_review(1, "accepted")),
        ]
    )

    result = new_controller(repo, executor).run(task)

    assert result["state"] == "accepted"
    assert (repo / "dirty.txt").read_bytes() == dirty_before
    assert task.read_bytes() == task_before
    status = git(repo, "status", "--porcelain=v1", "-uall")
    assert "dirty.txt" in status
    assert "TASK.json" in status

    dirty_task = write_task(repo, allowed_paths=["dirty.txt"])

    def overwrite_preexisting_dirty(_invocation: AgentInvocation) -> None:
        (repo / "dirty.txt").write_text("owner work was overwritten\n", encoding="utf-8")

    allowed_executor = FakeExecutor(
        [
            FakeStep("rd", rd_result(1, ["dirty.txt"]), overwrite_preexisting_dirty),
            FakeStep("pm", pm_review(1, "accepted")),
        ]
    )
    allowed_result = new_controller(repo, allowed_executor).run(dirty_task)
    assert allowed_result["state"] == "accepted"
    assert (repo / "dirty.txt").read_text(encoding="utf-8") == "owner work was overwritten\n"
    assert (repo / "business.txt").read_text(encoding="utf-8") == "done\n"


def test_verification_and_acceptance_evidence_gates(repo: Path) -> None:
    task = write_task(repo)

    no_tests = rd_result(1, [])
    no_tests["test_commands"] = []
    no_tests_executor = FakeExecutor([FakeStep("rd", no_tests)])
    no_tests_result = new_controller(repo, no_tests_executor).run(task)
    assert no_tests_result["state"] == "blocked"
    assert "at least 1" in no_tests_result["reason"]
    assert [call.role for call in no_tests_executor.calls] == ["rd"]

    failed_tests = rd_result(1, [])
    failed_tests["test_commands"] = [
        {"command": "pytest fixture", "exit_code": 1, "evidence": "fixture failed"}
    ]
    failed_executor = FakeExecutor([FakeStep("rd", failed_tests)])
    failed_result = new_controller(repo, failed_executor).run(task)
    assert failed_result["state"] == "blocked"
    assert "failed verification command" in failed_result["reason"]
    assert [call.role for call in failed_executor.calls] == ["rd"]

    empty_pm_evidence = FakeExecutor(
        [
            FakeStep("rd", rd_result(1, [])),
            FakeStep("pm", pm_review(1, "accepted", evidence=[])),
        ]
    )
    empty_evidence_result = new_controller(repo, empty_pm_evidence).run(task)
    assert empty_evidence_result["state"] == "blocked"
    assert "at least 1" in empty_evidence_result["reason"]

    timeout_executor = FakeExecutor(
        [FakeStep("rd", rd_result(1, []), exit_code=124, termination_reason="timeout")]
    )
    timeout_result = new_controller(repo, timeout_executor).run(task)
    assert timeout_result["state"] == "blocked"
    assert timeout_result["reason"] == "RD agent exceeded the configured timeout"


def test_dry_run_starts_no_model_and_writes_nothing(repo: Path) -> None:
    task = write_task(repo)
    before = {
        path.relative_to(repo).as_posix(): path.read_bytes()
        for path in repo.rglob("*")
        if path.is_file() and ".git" not in path.parts
    }
    executor = FakeExecutor([])

    result = new_controller(repo, executor).run(task, dry_run=True)

    after = {
        path.relative_to(repo).as_posix(): path.read_bytes()
        for path in repo.rglob("*")
        if path.is_file() and ".git" not in path.parts
    }
    assert result["dry_run"] is True
    assert result["models_started"] == 0
    assert result["business_files_modified"] is False
    assert result["agent_timeout_seconds"] == 1800.0
    assert executor.calls == []
    assert before == after
    assert not (repo / ".brain").exists()
    assert result["agents"][0]["permissions"] == {
        "approval": "bypassed",
        "sandbox": "bypassed",
    }
    assert result["agents"][1]["permissions"] == {
        "approval": "never",
        "sandbox": "read-only",
    }


def test_stop_and_workspace_lock_are_effective(repo: Path) -> None:
    task = write_task(repo)
    held_lock = RunLock(repo / ".brain" / "brain.lock", "held-by-test")
    held_lock.acquire()
    try:
        with pytest.raises(BrainLockedError, match="another brain run"):
            new_controller(repo, FakeExecutor([])).run(task)
    finally:
        held_lock.release()

    executor = FakeExecutor([])
    controller = new_controller(repo, executor)

    def request_stop(_invocation: AgentInvocation) -> None:
        response = controller.stop()
        assert response["stop_requested"] is True

    executor.steps.append(FakeStep("rd", rd_result(1, []), request_stop))
    result = controller.run(task)

    assert result["state"] == "blocked"
    assert result["reason"] == "stop requested; active agent process terminated"
    assert [call.role for call in executor.calls] == ["rd"]
    assert not (repo / ".brain" / "brain.lock").exists()
    assert not (repo / ".brain" / "stop.json").exists()


def test_real_executor_times_out_and_stops_a_running_process(tmp_path: Path) -> None:
    executor = CodexAgentExecutor()

    def invocation(run_id: str, stop_path: Path, timeout_seconds: float) -> AgentInvocation:
        return AgentInvocation(
            role="rd",
            round=1,
            command=(sys.executable, "-c", "import time; time.sleep(60)"),
            prompt="",
            output_path=tmp_path / "unused-output.json",
            schema_path=tmp_path / "unused-schema.json",
            permissions={"approval": "test", "sandbox": "test"},
            cwd=tmp_path,
            run_id=run_id,
            stop_path=stop_path,
            timeout_seconds=timeout_seconds,
        )

    timeout_started = time.monotonic()
    timeout_result = executor.run(invocation("timeout-run", tmp_path / "none.json", 0.2))
    assert timeout_result.exit_code == 124
    assert timeout_result.termination_reason == "timeout"
    assert timeout_result.error_summary == "Agent exited without a safe stderr diagnostic."
    assert time.monotonic() - timeout_started < 5

    stop_path = tmp_path / "stop.json"
    stop_invocation = invocation("stop-run", stop_path, 30)
    stop_started = time.monotonic()
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(executor.run, stop_invocation)
        time.sleep(0.2)
        write_json(stop_path, {"run_id": "stop-run"})
        stop_result = future.result(timeout=10)
    assert stop_result.exit_code == 130
    assert stop_result.termination_reason == "stop_requested"
    assert stop_result.error_summary == "Agent exited without a safe stderr diagnostic."
    assert time.monotonic() - stop_started < 5


def test_brain_ps1_dry_run_displays_commands_without_model(tmp_path: Path) -> None:
    powershell = shutil.which("powershell") or shutil.which("pwsh")
    if powershell is None:
        pytest.skip("PowerShell is not installed")
    task = {
        "objective": "Inspect the launch plan only.",
        "scope": ["Dry run."],
        "allowed_paths": ["brain.ps1"],
        "acceptance_criteria": ["No model starts."],
        "prohibitions": ["No business file writes."],
        "round": 1,
    }
    task_path = tmp_path / "TASK.json"
    task_path.write_text(json.dumps(task), encoding="utf-8")
    status_before = git(PROJECT_ROOT, "status", "--porcelain=v1", "-uall")

    completed = subprocess.run(
        [
            powershell,
            "-NoProfile",
            "-File",
            str(PROJECT_ROOT / "brain.ps1"),
            "run",
            "-Task",
            str(task_path),
            "-DryRun",
        ],
        cwd=tmp_path,
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )

    result = json.loads(completed.stdout)
    assert result["models_started"] == 0
    assert result["business_files_modified"] is False
    assert "--dangerously-bypass-approvals-and-sandbox" in result["agents"][0]["command"]
    assert "--ask-for-approval never" in result["agents"][1]["command"]
    assert "--sandbox read-only" in result["agents"][1]["command"]
    assert git(PROJECT_ROOT, "status", "--porcelain=v1", "-uall") == status_before
