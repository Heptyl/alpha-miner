"""Finite RD -> PM controller with schema, Git, lock, and retry guards."""

from __future__ import annotations

import copy
import os
import uuid
from pathlib import Path
from typing import Any

from .executor import (
    AgentExecution,
    AgentExecutor,
    CodexAgentExecutor,
    build_invocation,
    normalize_error_summary,
)
from .git_guard import (
    GitGuardError,
    assert_paths_allowed,
    assert_protected_dirty_preserved,
    assert_same_head,
    capture_snapshot,
    changed_paths,
    normalize_task_path,
)
from .runtime import BrainLockedError, EventLog, RunLock, read_json, utc_now, write_json
from .schema import SchemaValidationError, load_and_validate_json

STATES = {
    "pending",
    "running_rd",
    "awaiting_pm",
    "changes_requested",
    "accepted",
    "blocked",
}

TRANSITIONS = {
    "pending": {"running_rd", "blocked"},
    "running_rd": {"awaiting_pm", "blocked"},
    "awaiting_pm": {"changes_requested", "accepted", "blocked"},
    "changes_requested": {"running_rd", "blocked"},
    "accepted": set(),
    "blocked": set(),
}

DEFAULT_AGENT_TIMEOUT_SECONDS = 1800.0


class AgentExecutionError(RuntimeError):
    """Raised when a non-interactive agent process exits unsuccessfully."""


class StopRequested(RuntimeError):
    """Raised when the stop command interrupts or preempts an agent run."""


class BrainController:
    """Coordinate isolated RD and PM processes through bounded JSON handoffs."""

    def __init__(
        self,
        root: Path,
        *,
        executor: AgentExecutor | None = None,
        max_rounds: int = 3,
        agent_timeout_seconds: float = DEFAULT_AGENT_TIMEOUT_SECONDS,
        schema_dir: Path | None = None,
        brain_dir: Path | None = None,
    ) -> None:
        if max_rounds < 1:
            raise ValueError("max_rounds must be at least one")
        if agent_timeout_seconds <= 0:
            raise ValueError("agent_timeout_seconds must be greater than zero")
        self.root = root.resolve()
        self.executor = executor or CodexAgentExecutor()
        self.max_rounds = max_rounds
        self.agent_timeout_seconds = agent_timeout_seconds
        self.schema_dir = (schema_dir or self.root / "control" / "schemas").resolve()
        self.brain_dir = (brain_dir or self.root / ".brain").resolve()
        self.state_path = self.brain_dir / "state.json"
        self.lock_path = self.brain_dir / "brain.lock"
        self.stop_path = self.brain_dir / "stop.json"
        self._state: dict[str, Any] = {}
        self._events: EventLog | None = None

    def run(self, task_path: Path, *, dry_run: bool = False) -> dict[str, Any]:
        """Run the bounded state machine, or return its launch plan without side effects."""
        if dry_run:
            task = self._load_initial_task(task_path)
            return self._dry_run_plan(task)

        run_id = f"{utc_now().replace(':', '').replace('+', '-')}-{uuid.uuid4().hex[:8]}"
        lock = RunLock(self.lock_path, run_id)
        lock.acquire()
        try:
            self.stop_path.unlink(missing_ok=True)
            initial_snapshot = capture_snapshot(self.root)
            run_dir = self.brain_dir / "runs" / run_id
            self._events = EventLog(run_dir / "events.jsonl")
            self._state = {
                "run_id": run_id,
                "state": "pending",
                "current_round": 0,
                "max_rounds": self.max_rounds,
                "agent_timeout_seconds": self.agent_timeout_seconds,
                "initial_git_head": initial_snapshot.head,
                "started_at": utc_now(),
                "updated_at": utc_now(),
                "reason": None,
            }
            self._persist_state()
            self._events.append("state_initialized", to_state="pending", round=0)
        except BaseException:
            lock.release()
            raise

        try:
            task = self._load_initial_task(task_path)
            allowed = tuple(normalize_task_path(path) for path in task["allowed_paths"])
            current_task = copy.deepcopy(task)

            for round_number in range(1, self.max_rounds + 1):
                lock.assert_owned()
                self._raise_if_stop_requested(run_id)
                self._state["current_round"] = round_number
                self._persist_state()

                round_dir = run_dir / f"round-{round_number:02d}"
                round_dir.mkdir(parents=True, exist_ok=False)
                task_file = round_dir / "TASK.json"
                write_json(task_file, current_task)
                load_and_validate_json(task_file, self._schema("TASK"))

                round_start = capture_snapshot(self.root)
                assert_same_head(initial_snapshot, round_start)
                assert_protected_dirty_preserved(initial_snapshot, round_start, allowed)
                write_json(round_dir / "GIT_BASELINE.json", round_start.persisted())

                self._transition("running_rd")
                rd_output = round_dir / "RD_RESULT.json"
                rd_invocation = build_invocation(
                    role="rd",
                    round_number=round_number,
                    root=self.root,
                    prompt=self._rd_prompt(task_file, rd_output),
                    schema_path=self._schema("RD_RESULT"),
                    output_path=rd_output,
                    run_id=run_id,
                    stop_path=self.stop_path,
                    timeout_seconds=self.agent_timeout_seconds,
                )
                rd_execution = self.executor.run(rd_invocation)
                self._events.append(
                    "agent_finished",
                    role="rd",
                    round=round_number,
                    exit_code=rd_execution.exit_code,
                )
                self._record_agent_error("RD", rd_execution)
                lock.assert_owned()
                self._raise_if_stop_requested(run_id)
                self._verify_agent_execution("RD", rd_execution)

                after_rd = capture_snapshot(self.root)
                assert_same_head(round_start, after_rd)
                rd_changes = changed_paths(round_start, after_rd)
                assert_paths_allowed(rd_changes, allowed)
                assert_protected_dirty_preserved(initial_snapshot, after_rd, allowed)

                rd_result = load_and_validate_json(rd_output, self._schema("RD_RESULT"))
                self._verify_rd_result(rd_result, round_number, rd_changes, allowed)

                self._transition("awaiting_pm")
                pm_output = round_dir / "PM_REVIEW.json"
                pm_invocation = build_invocation(
                    role="pm",
                    round_number=round_number,
                    root=self.root,
                    prompt=self._pm_prompt(task_file, rd_output, pm_output),
                    schema_path=self._schema("PM_REVIEW"),
                    output_path=pm_output,
                    run_id=run_id,
                    stop_path=self.stop_path,
                    timeout_seconds=self.agent_timeout_seconds,
                )
                pm_before = after_rd
                pm_execution = self.executor.run(pm_invocation)
                self._events.append(
                    "agent_finished",
                    role="pm",
                    round=round_number,
                    exit_code=pm_execution.exit_code,
                )
                self._record_agent_error("PM", pm_execution)
                lock.assert_owned()
                self._raise_if_stop_requested(run_id)
                self._verify_agent_execution("PM", pm_execution)

                after_pm = capture_snapshot(self.root)
                assert_same_head(pm_before, after_pm)
                pm_changes = changed_paths(pm_before, after_pm)
                if pm_changes:
                    raise GitGuardError(
                        "PM read-only invariant violated: " + ", ".join(sorted(pm_changes))
                    )
                assert_protected_dirty_preserved(initial_snapshot, after_pm, allowed)

                pm_review = load_and_validate_json(pm_output, self._schema("PM_REVIEW"))
                self._verify_pm_review(pm_review, round_number)
                if pm_review["decision"] == "accepted":
                    self._transition("accepted")
                    return dict(self._state)

                self._transition("changes_requested")
                if round_number >= self.max_rounds:
                    self._block("maximum revision rounds reached", "MaxRoundsExceeded")
                    return dict(self._state)
                current_task = self._revision_task(task, pm_review, round_number + 1)

            raise RuntimeError("state machine exhausted unexpectedly")
        except (
            AgentExecutionError,
            BrainLockedError,
            GitGuardError,
            OSError,
            SchemaValidationError,
            StopRequested,
        ) as exc:
            self._block(str(exc), type(exc).__name__)
            return dict(self._state)
        finally:
            self.stop_path.unlink(missing_ok=True)
            lock.release()

    def status(self) -> dict[str, Any]:
        if not self.state_path.exists():
            return {"state": "pending", "run_id": None, "running": self.lock_path.exists()}
        state = read_json(self.state_path)
        state["running"] = self.lock_path.exists()
        return state

    def stop(self) -> dict[str, Any]:
        """Request that the active executor terminate its current agent process."""
        if not self.lock_path.exists():
            return {"stop_requested": False, "reason": "no active brain run"}
        lock_data = read_json(self.lock_path)
        request = {
            "run_id": lock_data.get("run_id"),
            "requested_at": utc_now(),
            "requester_pid": os.getpid(),
        }
        write_json(self.stop_path, request)
        return {"stop_requested": True, "run_id": lock_data.get("run_id")}

    def _load_initial_task(self, task_path: Path) -> dict[str, Any]:
        task = load_and_validate_json(task_path.resolve(), self._schema("TASK"))
        if task["round"] != 1:
            raise SchemaValidationError("initial TASK.round must be 1")
        task["allowed_paths"] = [normalize_task_path(path) for path in task["allowed_paths"]]
        return task

    def _dry_run_plan(self, task: dict[str, Any]) -> dict[str, Any]:
        dry_dir = self.brain_dir / "runs" / "DRY_RUN" / "round-01"
        task_file = dry_dir / "TASK.json"
        rd_output = dry_dir / "RD_RESULT.json"
        pm_output = dry_dir / "PM_REVIEW.json"
        rd = build_invocation(
            role="rd",
            round_number=1,
            root=self.root,
            prompt=self._rd_prompt(task_file, rd_output),
            schema_path=self._schema("RD_RESULT"),
            output_path=rd_output,
            run_id="DRY_RUN",
            stop_path=self.stop_path,
            timeout_seconds=self.agent_timeout_seconds,
        )
        pm = build_invocation(
            role="pm",
            round_number=1,
            root=self.root,
            prompt=self._pm_prompt(task_file, rd_output, pm_output),
            schema_path=self._schema("PM_REVIEW"),
            output_path=pm_output,
            run_id="DRY_RUN",
            stop_path=self.stop_path,
            timeout_seconds=self.agent_timeout_seconds,
        )
        return {
            "dry_run": True,
            "models_started": 0,
            "business_files_modified": False,
            "max_rounds": self.max_rounds,
            "agent_timeout_seconds": self.agent_timeout_seconds,
            "task": {
                "round": task["round"],
                "allowed_paths": task["allowed_paths"],
                "acceptance_criteria_count": len(task["acceptance_criteria"]),
            },
            "agents": [
                {
                    "role": rd.role.upper(),
                    "fresh_session": True,
                    "permissions": rd.permissions,
                    "command": rd.display_command(self.root),
                },
                {
                    "role": pm.role.upper(),
                    "fresh_session": True,
                    "permissions": pm.permissions,
                    "command": pm.display_command(self.root),
                },
            ],
        }

    def _verify_rd_result(
        self,
        result: dict[str, Any],
        round_number: int,
        actual_changes: set[str],
        allowed: tuple[str, ...],
    ) -> None:
        _validate_rd_business_constraints(result)
        if result["round"] != round_number:
            raise SchemaValidationError("RD_RESULT.round does not match TASK.round")
        failed_verifications = sum(
            command["exit_code"] != 0 for command in result["test_commands"]
        )
        if failed_verifications:
            raise SchemaValidationError(
                f"RD_RESULT contains {failed_verifications} failed verification command(s)"
            )
        declared: set[str] = set()
        for value in result["changed_files"]:
            path = normalize_task_path(value)
            if any(character in path for character in "*?["):
                raise SchemaValidationError("RD_RESULT.changed_files must contain concrete paths")
            declared.add(path)
        assert_paths_allowed(declared, allowed)
        if declared != actual_changes:
            raise GitGuardError(
                "RD_RESULT.changed_files does not match observed worktree changes"
            )

    @staticmethod
    def _verify_pm_review(review: dict[str, Any], round_number: int) -> None:
        _validate_pm_business_constraints(review)
        if review["round"] != round_number:
            raise SchemaValidationError("PM_REVIEW.round does not match TASK.round")

    def _record_agent_error(self, role: str, execution: AgentExecution) -> None:
        if execution.exit_code == 0:
            return
        summary = normalize_error_summary(
            execution.error_summary or "Agent exited without a safe stderr diagnostic."
        )
        self._state["agent_error"] = {
            "role": role,
            "exit_code": execution.exit_code,
            "summary": summary,
        }
        self._persist_state()

    @staticmethod
    def _verify_agent_execution(role: str, execution: AgentExecution) -> None:
        if execution.termination_reason == "stop_requested":
            raise StopRequested("stop requested; active agent process terminated")
        if execution.termination_reason == "timeout":
            raise AgentExecutionError(f"{role} agent exceeded the configured timeout")
        if execution.exit_code != 0:
            raise AgentExecutionError(f"{role} agent exited with code {execution.exit_code}")

    @staticmethod
    def _revision_task(
        original_task: dict[str, Any],
        review: dict[str, Any],
        round_number: int,
    ) -> dict[str, Any]:
        """Build the next short task without previous chat, results, or raw logs."""
        failed = copy.deepcopy(review["failed_acceptance_items"])
        evidence = copy.deepcopy(review["evidence"])
        scope = copy.deepcopy(review["revision_scope"])
        return {
            "objective": "修复 PM 指出的失败验收项；不得扩大范围。",
            "scope": scope,
            "allowed_paths": copy.deepcopy(original_task["allowed_paths"]),
            "acceptance_criteria": failed,
            "prohibitions": copy.deepcopy(original_task["prohibitions"]),
            "round": round_number,
            "revision": {
                "failed_acceptance_items": failed,
                "evidence": evidence,
                "revision_scope": scope,
            },
        }

    def _rd_prompt(self, task_file: Path, output_file: Path) -> str:
        return (
            "Activate $alpha-miner-rd for this independent session. Read AGENT_ROLES.md and "
            ".agents/skills/alpha-miner-rd/SKILL.md completely. Execute only the bounded "
            f"engineering task in {self._relative(task_file)}. Preserve pre-existing dirty changes "
            "outside TASK.allowed_paths; change in-scope dirty files only as required. Do not "
            "commit, push, deploy, clean, reset, checkout, stash, delete unrelated "
            "files, or access external systems. Return only JSON matching the supplied output "
            f"schema; it will be written to {self._relative(output_file)}. Evidence must be short "
            "and include at least one executed verification command; every exit code must be zero. "
            "It must not contain secrets, server addresses, personal names, absolute user paths, "
            "raw chat, or raw logs."
        )

    def _pm_prompt(self, task_file: Path, rd_file: Path, output_file: Path) -> str:
        return (
            "Activate $alpha-miner-pm for this independent read-only session. Read AGENT_ROLES.md "
            "and .agents/skills/alpha-miner-pm/SKILL.md completely. Review only the acceptance "
            f"criteria in {self._relative(task_file)} against {self._relative(rd_file)} and the "
            "current worktree. Do not modify any file or external system. Return only JSON matching "
            f"the supplied output schema; it will be written to {self._relative(output_file)}. If "
            "accepting, include concise non-empty acceptance evidence. If "
            "requesting changes, include only failed acceptance items, necessary concise evidence, "
            "and the minimum repair scope. Never include secrets, server addresses, personal names, "
            "absolute user paths, raw chat, or raw logs."
        )

    def _relative(self, path: Path) -> str:
        return path.resolve().relative_to(self.root).as_posix()

    def _schema(self, name: str) -> Path:
        return self.schema_dir / f"{name}.schema.json"

    def _transition(self, next_state: str) -> None:
        if next_state not in STATES:
            raise RuntimeError(f"unknown state: {next_state}")
        current = self._state["state"]
        if next_state not in TRANSITIONS[current]:
            raise RuntimeError(f"invalid state transition: {current} -> {next_state}")
        self._state["state"] = next_state
        self._state["updated_at"] = utc_now()
        self._persist_state()
        if self._events:
            self._events.append(
                "state_transition",
                from_state=current,
                to_state=next_state,
                round=self._state["current_round"],
            )

    def _block(self, reason: str, reason_code: str) -> None:
        if self._state.get("state") not in {"accepted", "blocked"}:
            self._state["reason"] = reason
            self._transition("blocked")
            if self._events:
                self._events.append(
                    "run_blocked",
                    to_state="blocked",
                    round=self._state["current_round"],
                    reason_code=reason_code,
                )

    def _persist_state(self) -> None:
        write_json(self.state_path, self._state)

    def _raise_if_stop_requested(self, run_id: str) -> None:
        if not self.stop_path.exists():
            return
        request = read_json(self.stop_path)
        if request.get("run_id") == run_id:
            raise StopRequested("stop requested; active agent process terminated")


def _validate_rd_business_constraints(result: dict[str, Any]) -> None:
    _validate_minimum("RD_RESULT.round", result["round"], 1)
    _validate_text("RD_RESULT.summary", result["summary"], 1, 2000)
    _validate_string_list(
        "RD_RESULT.changed_files",
        result["changed_files"],
        maximum_items=100,
        maximum_length=300,
        unique=True,
    )
    _validate_item_count("RD_RESULT.test_commands", result["test_commands"], 1, 50)
    for index, command in enumerate(result["test_commands"]):
        _validate_text(
            f"RD_RESULT.test_commands[{index}].command", command["command"], 1, 500
        )
        _validate_text(
            f"RD_RESULT.test_commands[{index}].evidence", command["evidence"], 1, 1000
        )
    _validate_string_list(
        "RD_RESULT.evidence", result["evidence"], maximum_items=20, maximum_length=1000
    )
    _validate_string_list(
        "RD_RESULT.risks", result["risks"], maximum_items=20, maximum_length=1000
    )


def _validate_pm_business_constraints(review: dict[str, Any]) -> None:
    _validate_minimum("PM_REVIEW.round", review["round"], 1)
    _validate_string_list(
        "PM_REVIEW.failed_acceptance_items",
        review["failed_acceptance_items"],
        maximum_items=50,
        maximum_length=1000,
    )
    _validate_string_list(
        "PM_REVIEW.evidence",
        review["evidence"],
        minimum_items=1,
        maximum_items=20,
        maximum_length=1000,
    )
    _validate_string_list(
        "PM_REVIEW.revision_scope",
        review["revision_scope"],
        maximum_items=50,
        maximum_length=500,
    )
    if review["decision"] == "accepted":
        if review["failed_acceptance_items"] or review["revision_scope"]:
            raise SchemaValidationError(
                "accepted PM_REVIEW must not contain failed items or revision scope"
            )
        return
    if not review["failed_acceptance_items"]:
        raise SchemaValidationError(
            "changes_requested PM_REVIEW must contain at least 1 failed acceptance item"
        )
    if not review["revision_scope"]:
        raise SchemaValidationError(
            "changes_requested PM_REVIEW must contain at least 1 revision scope item"
        )


def _validate_string_list(
    path: str,
    values: list[str],
    *,
    minimum_items: int = 0,
    maximum_items: int,
    maximum_length: int,
    unique: bool = False,
) -> None:
    _validate_item_count(path, values, minimum_items, maximum_items)
    if unique and len(values) != len(set(values)):
        raise SchemaValidationError(f"{path} must contain unique items")
    for index, value in enumerate(values):
        _validate_text(f"{path}[{index}]", value, 1, maximum_length)


def _validate_item_count(path: str, values: list[Any], minimum: int, maximum: int) -> None:
    if len(values) < minimum:
        raise SchemaValidationError(f"{path} must contain at least {minimum} item(s)")
    if len(values) > maximum:
        raise SchemaValidationError(f"{path} must contain at most {maximum} item(s)")


def _validate_text(path: str, value: str, minimum: int, maximum: int) -> None:
    if len(value) < minimum:
        raise SchemaValidationError(f"{path} must contain at least {minimum} character(s)")
    if len(value) > maximum:
        raise SchemaValidationError(f"{path} must contain at most {maximum} character(s)")


def _validate_minimum(path: str, value: int, minimum: int) -> None:
    if value < minimum:
        raise SchemaValidationError(f"{path} must be at least {minimum}")
