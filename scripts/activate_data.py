"""Validate and atomically activate an uploaded server database."""

import os
import shutil
import sqlite3
from pathlib import Path


def quick_check(path: Path) -> tuple[str, str | None]:
    db = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True)
    try:
        check = db.execute("PRAGMA quick_check").fetchone()[0]
        latest = db.execute("SELECT MAX(trade_date) FROM daily_price").fetchone()[0]
        return check, latest
    finally:
        db.close()


def main() -> None:
    root = Path.cwd().resolve()
    incoming = root / "incoming" / "alpha_miner.db"
    destination = root / "data" / "alpha_miner.db"
    previous = root / "data" / "alpha_miner.previous.db"

    if not incoming.is_file():
        raise FileNotFoundError(incoming)
    check, latest = quick_check(incoming)
    if check != "ok":
        raise RuntimeError(f"Uploaded SQLite quick_check failed: {check}")

    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        previous_upload = previous.with_suffix(".db.uploading")
        shutil.copy2(destination, previous_upload)
        os.replace(previous_upload, previous)
    os.replace(incoming, destination)
    print(f"Activated database: latest_trade_date={latest}; previous={previous}")


if __name__ == "__main__":
    main()
