"""Codex process invocation for fresh, non-interactive RD and PM agents."""

from __future__ import annotations

import json
import re
import shlex
import subprocess
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, TextIO

MAX_AGENT_ERROR_SUMMARY_BYTES = 4096
_MAX_DIAGNOSTIC_CAPTURE_CHARACTERS = 16 * 1024
_ANSI_ESCAPE = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
_CONTROL_CHARACTER = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")
_REDACTIONS: tuple[tuple[re.Pattern[str], str], ...] = (
    (re.compile(r"(?i)\bsk-[A-Za-z0-9_-]{8,}\b"), "<REDACTED_SECRET>"),
    (
        re.compile(
            r"(?i)\b(password|passwd|token|secret|api[_-]?key|authorization)\b"
            r"\s*[:=]\s*(?:bearer\s+)?[^\s,;}\]]+"
        ),
        r"\1=<REDACTED_SECRET>",
    ),
    (re.compile(r"(?i)\bbearer\s+[A-Za-z0-9._~+/-]+=*"), "Bearer <REDACTED>"),
    (
        re.compile(r"\beyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\b"),
        "<REDACTED_SECRET>",
    ),
    (re.compile(r"(?i)https?://\S+"), "<REDACTED_ADDRESS>"),
    (
        re.compile(r"(?i)\b[A-Z]:\\Users\\[^\r\n\"']+"),
        "<REDACTED_USER_PATH>",
    ),
    (re.compile(r"(?i)(?:/home/|/Users/)[^\s\"']+"), "<REDACTED_USER_PATH>"),
    (re.compile(r"(?i)\\\\[^\\\s]+\\[^\s\"']+"), "<REDACTED_ADDRESS>"),
    (
        re.compile(r"(?i)\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b"),
        "<REDACTED_ADDRESS>",
    ),
    (
        re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}(?::\d+)?\b"),
        "<REDACTED_ADDRESS>",
    ),
    (
        re.compile(
            r"(?i)\b(?:localhost|[A-Z0-9-]+(?:\.[A-Z0-9-]+)+"
            r"\.(?:com|net|org|io|ai|dev))(?::\d+)?\b"
        ),
        "<REDACTED_ADDRESS>",
    ),
)


@dataclass(frozen=True)
class AgentInvocation:
    role: str
    round: int
    command: tuple[str, ...]
    prompt: str
    output_path: Path
    schema_path: Path
    permissions: dict[str, str]
    cwd: Path
    run_id: str
    stop_path: Path
    timeout_seconds: float

    def display_command(self, root: Path) -> str:
        """Render a dry-run command with workspace paths redacted."""
        root_text = str(root.resolve())
        rendered: list[str] = []
        for argument in self.command:
            safe = argument
            if safe == root_text:
                safe = "$WORKSPACE"
            elif safe.startswith(root_text + str(Path("/"))):
                safe = "$WORKSPACE/" + Path(safe).relative_to(root).as_posix()
            elif safe.startswith(root_text + "\\"):
                safe = "$WORKSPACE/" + Path(safe).relative_to(root).as_posix()
            rendered.append(shlex.quote(safe))
        return " ".join(rendered)


@dataclass(frozen=True)
class AgentExecution:
    exit_code: int
    termination_reason: str | None = None
    error_summary: str | None = None


class AgentExecutor(Protocol):
    def run(self, invocation: AgentInvocation) -> AgentExecution:
        """Execute one isolated invocation and return only non-sensitive metadata."""


class CodexAgentExecutor:
    """Run Codex without a shell and without persisting its raw stdout/stderr."""

    def run(self, invocation: AgentInvocation) -> AgentExecution:
        if _stop_requested(invocation):
            return AgentExecution(
                exit_code=130,
                termination_reason="stop_requested",
                error_summary="Agent start skipped because a stop was already requested.",
            )

        process = subprocess.Popen(
            list(invocation.command),
            cwd=invocation.cwd,
            stdin=subprocess.PIPE,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        collector = _StderrDiagnosticCollector()
        stderr_thread = threading.Thread(
            target=collector.read,
            args=(process.stderr,),
            name=f"brain-{invocation.role}-stderr",
            daemon=True,
        )
        stderr_thread.start()
        try:
            if process.stdin is not None:
                try:
                    process.stdin.write(invocation.prompt)
                    process.stdin.close()
                except BrokenPipeError:
                    pass

            deadline = time.monotonic() + invocation.timeout_seconds
            while True:
                exit_code = process.poll()
                if exit_code is not None:
                    return _execution_result(exit_code, None, collector, stderr_thread)
                if _stop_requested(invocation):
                    _terminate(process)
                    return _execution_result(
                        130, "stop_requested", collector, stderr_thread
                    )
                if time.monotonic() >= deadline:
                    _terminate(process)
                    return _execution_result(124, "timeout", collector, stderr_thread)
                time.sleep(0.05)
        except BaseException:
            if process.poll() is None:
                _terminate(process)
            raise


class _StderrDiagnosticCollector:
    """Drain stderr, retaining only a bounded diagnostic suffix after an error marker."""

    def __init__(self) -> None:
        self._parts: list[str] = []
        self._characters = 0
        self._capturing = False

    def read(self, stream: TextIO | None) -> None:
        if stream is None:
            return
        for line in stream:
            if not self._capturing and _starts_diagnostic(line):
                self._capturing = True
            if not self._capturing or self._characters >= _MAX_DIAGNOSTIC_CAPTURE_CHARACTERS:
                continue
            remaining = _MAX_DIAGNOSTIC_CAPTURE_CHARACTERS - self._characters
            retained = line[:remaining]
            self._parts.append(retained)
            self._characters += len(retained)

    def summary(self) -> str:
        if not self._parts:
            return "Agent exited without a safe stderr diagnostic."
        return normalize_error_summary("".join(self._parts))


def normalize_error_summary(value: str) -> str:
    """Redact a diagnostic and cap its persisted representation at 4 KiB UTF-8."""
    sanitized = _ANSI_ESCAPE.sub("", value)
    sanitized = _CONTROL_CHARACTER.sub("?", sanitized)
    for pattern, replacement in _REDACTIONS:
        sanitized = pattern.sub(replacement, sanitized)
    sanitized = sanitized.strip()
    if not sanitized:
        sanitized = "Agent exited without a safe stderr diagnostic."
    return _truncate_utf8(sanitized, MAX_AGENT_ERROR_SUMMARY_BYTES)


def _starts_diagnostic(line: str) -> bool:
    normalized = _ANSI_ESCAPE.sub("", line).lstrip().casefold()
    return normalized.startswith(("error", "fatal", "panic")) or (
        normalized.startswith("thread ") and "panicked" in normalized
    )


def _execution_result(
    exit_code: int,
    termination_reason: str | None,
    collector: _StderrDiagnosticCollector,
    stderr_thread: threading.Thread,
) -> AgentExecution:
    stderr_thread.join(timeout=5)
    summary = collector.summary() if exit_code != 0 else None
    return AgentExecution(
        exit_code=exit_code,
        termination_reason=termination_reason,
        error_summary=summary,
    )


def _truncate_utf8(value: str, maximum_bytes: int) -> str:
    encoded = value.encode("utf-8")
    if len(encoded) <= maximum_bytes:
        return value
    suffix = b"\n<TRUNCATED>"
    prefix = encoded[: maximum_bytes - len(suffix)].decode("utf-8", errors="ignore")
    return prefix + suffix.decode("ascii")


def build_invocation(
    *,
    role: str,
    round_number: int,
    root: Path,
    prompt: str,
    schema_path: Path,
    output_path: Path,
    run_id: str,
    stop_path: Path,
    timeout_seconds: float,
) -> AgentInvocation:
    """Build the exact permissions and output contract for an RD or PM process."""
    if timeout_seconds <= 0:
        raise ValueError("agent timeout must be greater than zero")
    common = (
        "--cd",
        str(root.resolve()),
        "--ephemeral",
        "--color",
        "never",
        "--output-schema",
        str(schema_path.resolve()),
        "--output-last-message",
        str(output_path.resolve()),
        "-",
    )
    if role == "rd":
        command = ("codex", "exec", "--dangerously-bypass-approvals-and-sandbox", *common)
        permissions = {"approval": "bypassed", "sandbox": "bypassed"}
    elif role == "pm":
        command = (
            "codex",
            "--ask-for-approval",
            "never",
            "exec",
            "--sandbox",
            "read-only",
            *common,
        )
        permissions = {"approval": "never", "sandbox": "read-only"}
    else:
        raise ValueError(f"unsupported agent role: {role}")
    return AgentInvocation(
        role=role,
        round=round_number,
        command=command,
        prompt=prompt,
        output_path=output_path,
        schema_path=schema_path,
        permissions=permissions,
        cwd=root.resolve(),
        run_id=run_id,
        stop_path=stop_path.resolve(),
        timeout_seconds=timeout_seconds,
    )


def _stop_requested(invocation: AgentInvocation) -> bool:
    try:
        request = json.loads(invocation.stop_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False
    return isinstance(request, dict) and request.get("run_id") == invocation.run_id


def _terminate(process: subprocess.Popen[str]) -> None:
    try:
        process.terminate()
    except OSError:
        return
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)
