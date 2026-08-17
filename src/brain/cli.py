"""Command-line entry point used by brain.ps1."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Sequence

from .controller import BrainController
from .runtime import BrainLockedError
from .schema import SchemaValidationError


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="brain")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("status", help="show the latest controller state")
    run = commands.add_parser("run", help="run an RD/PM task loop")
    run.add_argument("--task", required=True, type=Path, help="path to TASK.json")
    run.add_argument("--dry-run", action="store_true", help="show commands without agents or writes")
    commands.add_parser("stop", help="terminate the active agent and stop its run")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    root = Path.cwd()
    controller = BrainController(root)
    try:
        if args.command == "status":
            result = controller.status()
        elif args.command == "stop":
            result = controller.stop()
        else:
            result = controller.run(args.task, dry_run=args.dry_run)
    except (BrainLockedError, SchemaValidationError, OSError, RuntimeError) as exc:
        result = {"state": "blocked", "reason": str(exc)}
        print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
        return 2

    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 2 if result.get("state") == "blocked" else 0


if __name__ == "__main__":
    raise SystemExit(main())
