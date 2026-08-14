"""Create a consistent SQLite backup for atomic activation on the server."""

import argparse
import os
import sqlite3
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", default="data/alpha_miner.db")
    parser.add_argument("--target", required=True)
    args = parser.parse_args()

    source = Path(args.source).resolve()
    target = Path(args.target).resolve()
    if not source.is_file():
        raise FileNotFoundError(source)
    target.parent.mkdir(parents=True, exist_ok=True)
    uploading = target.with_name(f"{target.name}.{os.getpid()}.uploading")

    source_db = sqlite3.connect(source)
    target_db = sqlite3.connect(uploading)
    try:
        source_db.backup(target_db)
        check = target_db.execute("PRAGMA quick_check").fetchone()[0]
        if check != "ok":
            raise RuntimeError(f"SQLite quick_check failed: {check}")
    finally:
        target_db.close()
        source_db.close()

    os.replace(uploading, target)
    print(f"Published consistent database: {target} ({target.stat().st_size} bytes)")


if __name__ == "__main__":
    main()
