"""数据存储层 — 所有数据访问的唯一入口。

核心功能：
1. 初始化数据库（执行 schema.sql）
2. 时间隔离查询：query(table, as_of) 自动过滤 snapshot_time < as_of
3. 范围查询：query_range(table, as_of, lookback_days)
4. 数据写入：insert(table, df) 自动添加 snapshot_time
"""

import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd


class Storage:
    """SQLite 存储层，提供时间隔离的数据访问。"""

    def __init__(self, db_path: str = "data/alpha_miner.db"):
        self.db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)

    def _get_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def init_db(self) -> None:
        """执行 schema.sql 建表。"""
        schema_path = Path(__file__).parent / "schema.sql"
        schema_sql = schema_path.read_text(encoding="utf-8")
        conn = self._get_conn()
        try:
            conn.executescript(schema_sql)
            # 为 news 表新增 news_type / classify_confidence 列（幂等）
            for stmt in [
                "ALTER TABLE news ADD COLUMN news_type TEXT DEFAULT 'noise'",
                "ALTER TABLE news ADD COLUMN classify_confidence REAL DEFAULT 0.0",
            ]:
                try:
                    conn.execute(stmt)
                except sqlite3.OperationalError:
                    pass  # 列已存在
            conn.commit()
        finally:
            conn.close()

    def query(
        self,
        table: str,
        as_of: datetime,
        where: str = "",
        params: tuple = (),
    ) -> pd.DataFrame:
        """时间隔离查询。

        自动注入 WHERE snapshot_time < as_of 条件，
        确保不会读取到 as_of 之后写入的数据。
        """
        as_of_str = as_of.strftime("%Y-%m-%d %H:%M:%S")
        sql = f"SELECT * FROM {table} WHERE snapshot_time < ?"
        all_params = [as_of_str]

        if where:
            sql += f" AND ({where})"
            all_params.extend(params)

        conn = self._get_conn()
        try:
            df = pd.read_sql_query(sql, conn, params=all_params)
            return df
        finally:
            conn.close()

    def query_range(
        self,
        table: str,
        as_of: datetime,
        lookback_days: int,
        date_col: str = "trade_date",
        where: str = "",
        params: tuple = (),
    ) -> pd.DataFrame:
        """查询 as_of 前 N 日数据（按 trade_date 过滤）。

        注意：snapshot_time 过滤和 trade_date 过滤是独立的 AND 条件。
        生产环境中 as_of 应该 >= 数据采集时间。
        """
        as_of_str = as_of.strftime("%Y-%m-%d %H:%M:%S")
        start_date = (as_of - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
        end_date = as_of.strftime("%Y-%m-%d")

        sql = f"SELECT * FROM {table} WHERE snapshot_time < ? AND {date_col} >= ? AND {date_col} <= ?"
        all_params = [as_of_str, start_date, end_date]

        if where:
            sql += f" AND ({where})"
            all_params.extend(params)

        conn = self._get_conn()
        try:
            df = pd.read_sql_query(sql, conn, params=all_params)
            return df
        finally:
            conn.close()

    def insert(
        self,
        table: str,
        df: pd.DataFrame,
        snapshot_time: Optional[datetime] = None,
    ) -> int:
        """将 DataFrame 写入数据库，自动添加 snapshot_time 列。

        Args:
            table: 目标表名
            df: 要写入的数据
            snapshot_time: 可选，手动指定 snapshot_time（主要用于测试）
        """
        if df.empty:
            return 0

        df = df.copy()
        if snapshot_time is not None:
            now_str = snapshot_time.strftime("%Y-%m-%d %H:%M:%S")
        else:
            now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        df["snapshot_time"] = now_str

        conn = self._get_conn()
        try:
            df.to_sql(table, conn, if_exists="append", index=False)
            conn.commit()
            return len(df)
        finally:
            conn.close()

    def execute(self, sql: str, params: tuple = ()) -> list[dict]:
        """执行原始 SQL 查询，返回字典列表。"""
        conn = self._get_conn()
        try:
            cursor = conn.execute(sql, params)
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()

    def execute_write(self, sql: str, params: tuple = ()) -> None:
        """执行写操作 SQL。"""
        conn = self._get_conn()
        try:
            conn.execute(sql, params)
            conn.commit()
        finally:
            conn.close()
