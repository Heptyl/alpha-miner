"""Static contract for the local wrappers that may invoke remote compute."""

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SERVER_RUN = ROOT / "scripts" / "server_run.sh"
REMOTE_COMPUTE = ROOT / "scripts" / "remote_compute.ps1"

SERVER_ACTIONS = {
    "build",
    "evolve",
    "snapshot",
    "activate-data",
    "test",
    "python",
    "status",
}
WINDOWS_ACTIONS = {
    "sync",
    "build",
    "evolve",
    "snapshot",
    "publish-data",
    "status",
}
REMOTE_MAPPING = {
    "build": "build",
    "evolve": "evolve",
    "snapshot": "snapshot",
    "publish-data": "activate-data",
}
RETIRED_ACTIONS = {"collect", "evolve-limit-up", "daily"}


def _server_actions(source: str) -> set[str]:
    labels = re.findall(r"^  ([a-z][a-z-]*)\)$", source, flags=re.MULTILINE)
    assert len(labels) == len(set(labels)), "server action labels must be unique"
    return set(labels)


def _validate_set(source: str) -> set[str]:
    match = re.search(r"\[ValidateSet\(([^)]*)\)\]", source)
    assert match, "remote wrapper must declare a PowerShell ValidateSet"
    values = re.findall(r"'([^']+)'", match.group(1))
    assert len(values) == len(set(values)), "ValidateSet actions must be unique"
    return set(values)


def _remote_mapping(source: str) -> dict[str, str]:
    match = re.search(
        r"\$remoteAction = switch \(\$Action\) \{(.*?)\n\}",
        source,
        flags=re.DOTALL,
    )
    assert match, "remote action mapping must remain explicit"
    pairs = re.findall(
        r"^\s+'([^']+)'\s+\{\s+'([^']+)'\s+\}\s*$",
        match.group(1),
        flags=re.MULTILINE,
    )
    assert len(pairs) == len(dict(pairs)), "remote mapping keys must be unique"
    assert re.search(r"^\s+default\s+\{\s+'status'\s+\}\s*$", match.group(1), re.MULTILINE)
    return dict(pairs)


def test_server_action_allowlist_and_development_only_evolution_are_exact():
    source = SERVER_RUN.read_text(encoding="utf-8")

    assert _server_actions(source) == SERVER_ACTIONS
    assert source.count("run_python -m cli.mine evolve") == 1
    assert source.count("research_stage=DEVELOPMENT_ONLY") == 1
    assert "formal holdout/admission is not available" in source
    assert "cannot claim accepted or a formal holdout result" in source

    forbidden_invocations = (
        "run_python -m cli.collect",
        "run_python -m cli.limit_up evolve",
        "run_python -m cli daily",
    )
    assert not any(invocation in source for invocation in forbidden_invocations)
    assert _server_actions(source).isdisjoint(RETIRED_ACTIONS)


def test_windows_wrapper_allowlist_and_mapping_are_exact():
    source = REMOTE_COMPUTE.read_text(encoding="utf-8")

    assert _validate_set(source) == WINDOWS_ACTIONS
    assert _remote_mapping(source) == REMOTE_MAPPING
    assert _validate_set(source).isdisjoint(RETIRED_ACTIONS)
    assert set(_remote_mapping(source).values()).issubset(SERVER_ACTIONS)


def test_windows_wrapper_has_one_remote_execution_boundary():
    source = REMOTE_COMPUTE.read_text(encoding="utf-8")

    assert source.count("bash scripts/server_run.sh $remoteAction") == 1
    assert len(re.findall(r"^ssh \$SshTarget \$remoteCommand$", source, re.MULTILINE)) == 1
    assert "if ($Action -eq 'sync')" in source
    assert "if ($Action -eq 'publish-data')" in source
