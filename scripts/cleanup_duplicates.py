#!/usr/bin/env python3
"""清理数据库中的重复数据（保留每组最新 snapshot_time）。

使用方法:
    # 预览模式（不删除，只显示统计）
    uv run python scripts/cleanup_duplicates.py --dry-run

    # 执行清理（先自动备份）
    uv run python scripts/cleanup_duplicates.py

    # 只清理特定表
    uv run python scripts/cleanup_duplicates.py --tables factor_values ic_series
"""

import argparse
import shutil
import sqlite3
from pathlib import Path

DB_PATH = Path("data/alpha_miner.db")

DEDUP_RULES = {
    "factor_values": ["factor_name", "stock_code", "trade_date"],
    "ic_series": ["factor_name", "trade_date", "forward_days"],
    "market_emotion": ["trade_date"],
    "concept_daily": ["concept_name", "trade_date"],
}


def count_duplicates(conn: sqlite3.Connection, table: str, keys: list[str]) -> int:
    """统计重复行数（总行数 - 唯一键组数）。"""
    key_cols = ", ".join(keys)
    row = conn.execute(
        f"SELECT COUNT(*) - COUNT(DISTINCT {key_cols}) FROM [{table}]"
    ).fetchone()
    return row[0]


def cleanup_table(conn: sqlite3.Connection, table: str, keys: list[str]) -> int:
    """删除重复行，保留每组最新 snapshot_time。"""
    key_cols = ", ".join(keys)
    # 保留每组中 rowid 最大的（最新插入的）
    conn.execute(f"""
        DELETE FROM [{table}] WHERE rowid NOT IN (
            SELECT MAX(rowid) FROM [{table}] GROUP BY {key_cols}
        )
    """)
    deleted = conn.total_changes
    conn.commit()
    return deleted


def main():
    parser = argparse.ArgumentParser(description="清理数据库重复数据")
    parser.add_argument("--dry-run", action="store_true", help="只显示统计，不删除")
    parser.add_argument("--tables", nargs="+", choices=list(DEDUP_RULES.keys()), help="只清理指定表")
    args = parser.parse_args()

    if not DB_PATH.exists():
        print(f"数据库不存在: {DB_PATH}")
        return

    target_tables = {k: v for k, v in DEDUP_RULES.items() if args.tables is None or k in args.tables}

    conn = sqlite3.connect(str(DB_PATH))
    try:
        print(f"数据库: {DB_PATH} ({DB_PATH.stat().st_size / 1024 / 1024:.1f} MB)")
        print()

        total_before = 0
        total_dupes = 0

        for table, keys in target_tables.items():
            count = conn.execute(f"SELECT COUNT(*) FROM [{table}]").fetchone()[0]
            dupes = count_duplicates(conn, table, keys)
            total_before += count
            total_dupes += dupes
            status = f"{dupes} duplicates" if dupes > 0 else "clean"
            print(f"  {table}: {count} rows ({status}, key: {', '.join(keys)})")

        print(f"\n  总计: {total_before} rows, {total_dupes} duplicates")

        if total_dupes == 0:
            print("\n无重复数据，无需清理。")
            return

        if args.dry_run:
            print(f"\n[dry-run] 将删除 {total_dupes} 行重复数据。")
            return

        # 备份
        backup_path = DB_PATH.with_suffix(".db.bak")
        shutil.copy2(str(DB_PATH), str(backup_path))
        print(f"\n备份: {backup_path}")

        # 清理
        total_deleted = 0
        for table, keys in target_tables.items():
            dupes = count_duplicates(conn, table, keys)
            if dupes > 0:
                deleted = cleanup_table(conn, table, keys)
                total_deleted += deleted
                print(f"  {table}: deleted {deleted} rows")

        # VACUUM
        conn.close()
        conn = sqlite3.connect(str(DB_PATH))
        conn.execute("VACUUM")
        conn.close()

        new_size = DB_PATH.stat().st_size / 1024 / 1024
        print(f"\n清理完成: deleted {total_deleted} rows, DB {new_size:.1f} MB")
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    main()
