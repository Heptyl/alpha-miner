"""Sandbox subprocess using the same PIT/AST contract as the main process."""

from __future__ import annotations

import json
import locale
import os
import re
import sys
import traceback
from datetime import datetime
from pathlib import Path

from src.data.pit import compile_compute_source
from src.data.storage import Storage
from src.mining.backtester import FactorBacktester

_LEGACY_AS_OF_PREFIX = re.compile(
    r"^import os; os\.environ\['SANDBOX_AS_OF'\] = '([0-9]{4}-[0-9]{2}-[0-9]{2})'\s*\n"
)


def main() -> None:
    code_path = sys.argv[1]
    db_path = sys.argv[2]
    factor_name = sys.argv[3]
    try:
        payload = Path(code_path).read_bytes()
        try:
            code = payload.decode("utf-8")
        except UnicodeDecodeError:
            # Sandbox's existing NamedTemporaryFile uses the Windows locale
            # encoding; keep the runner compatible without weakening AST checks.
            code = payload.decode(locale.getpreferredencoding(False))
        # Sandbox.evaluate historically injected this exact trusted prefix. Strip it
        # before AST validation; it is never executed and cannot expose os.
        prefix = _LEGACY_AS_OF_PREFIX.match(code)
        injected_as_of = prefix.group(1) if prefix else None
        if prefix:
            code = code[prefix.end() :]
        compute = compile_compute_source(code, factor_name)
        if os.environ.get("SANDBOX_VALIDATE_ONLY") == "1":
            print(
                json.dumps(
                    {
                        "validated": True,
                        "factor_name": factor_name,
                        "pit_contract": "query/query_range only",
                    },
                    ensure_ascii=False,
                )
            )
            return

        as_of_text = os.environ.get("SANDBOX_AS_OF") or injected_as_of
        end_at = datetime.strptime(as_of_text, "%Y-%m-%d") if as_of_text else None
        result = FactorBacktester(Storage(db_path)).run(
            compute,
            factor_name=factor_name,
            lookback_days=20,
            end_at=end_at,
        )
        payload = {
            "ic_result": result.to_dict(),
            "factor_name": factor_name,
            "num_valid_days": result.total_days,
            "research_stage": "DEVELOPMENT_ONLY",
            "holdout_opened": False,
        }
        print(json.dumps(payload, ensure_ascii=False))
    except Exception:
        print(json.dumps({"error": traceback.format_exc()[-800:]}, ensure_ascii=False))


if __name__ == "__main__":
    main()
