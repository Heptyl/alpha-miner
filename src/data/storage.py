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
        self.backtest_mode = False  # True 时所有 query 自动 bypass snapshot_time 过滤
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)

    def _get_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    def init_db(self) -> None:
        """执行 schema.sql 建表。"""
        schema_path = Path(__file__).parent / "schema.sql"
        schema_sql = schema_path.read_text(encoding="utf-8")
        conn = self._get_conn()
        try:
            conn.executescript(schema_sql)
            # 为 zt_pool 新增 name / industry 列（幂等）
            for stmt in [
                "ALTER TABLE daily_price ADD COLUMN pre_close REAL",
                "ALTER TABLE zt_pool ADD COLUMN name TEXT DEFAULT ''",
                "ALTER TABLE zt_pool ADD COLUMN industry TEXT DEFAULT ''",
                "ALTER TABLE strong_pool ADD COLUMN name TEXT DEFAULT ''",
                "ALTER TABLE strong_pool ADD COLUMN industry TEXT DEFAULT ''",
            ]:
                try:
                    conn.execute(stmt)
                except sqlite3.OperationalError:
                    pass  # 列已存在
            # 为 market_emotion 新增活跃度/涨跌家数列（幂等）
            for stmt in [
                "ALTER TABLE market_emotion ADD COLUMN up_count INTEGER DEFAULT 0",
                "ALTER TABLE market_emotion ADD COLUMN down_count INTEGER DEFAULT 0",
                "ALTER TABLE market_emotion ADD COLUMN activity TEXT DEFAULT '0%'",
            ]:
                try:
                    conn.execute(stmt)
                except sqlite3.OperationalError:
                    pass  # 列已存在
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
        bypass_snapshot: bool = False,
    ) -> pd.DataFrame:
        """时间隔离查询。

        自动注入 WHERE snapshot_time < as_of 条件，
        确保不会读取到 as_of 之后写入的数据。

        Args:
            bypass_snapshot: True 时跳过 snapshot_time 过滤，仅用 where 条件。
                用于回测场景：数据是后来采集的但 trade_date 是历史日期。
        """
        if bypass_snapshot or self.backtest_mode:
            if where:
                sql = f"SELECT * FROM {table} WHERE ({where})"
                all_params = list(params)
            else:
                sql = f"SELECT * FROM {table}"
                all_params = []
        else:
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

        if self.backtest_mode:
            sql = f"SELECT * FROM {table} WHERE {date_col} >= ? AND {date_col} <= ?"
            all_params = [start_date, end_date]
        else:
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
        dedup: bool = False,
    ) -> int:
        """将 DataFrame 写入数据库，自动添加 snapshot_time 列。

        Args:
            table: 目标表名
            df: 要写入的数据
            snapshot_time: 可选，手动指定 snapshot_time（主要用于测试）
            dedup: True 时，写入前先删除同日旧数据（只保留最新快照）。
                   适用于 daily_price 等不需要保留盘中多快照的表。
        """
        if df.empty:
            return 0

        df = df.copy()
        if snapshot_time is not None:
            now_str = snapshot_time.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        else:
            now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        df["snapshot_time"] = now_str

        conn = self._get_conn()
        try:
            if dedup and "trade_date" in df.columns:
                # 先确认表中有 trade_date 列
                table_cols = {r[1] for r in conn.execute(f"PRAGMA table_info([{table}])").fetchall()}
                if "trade_date" in table_cols:
                    dates = df["trade_date"].unique()
                    placeholders = ",".join(["?"] * len(dates))
                    conn.execute(
                        f"DELETE FROM [{table}] WHERE trade_date IN ({placeholders})",
                        tuple(dates),
                    )
                    conn.commit()

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
