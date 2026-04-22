"""策略持久化 — 将 Strategy/Report/Trade 存入数据库。"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Optional

from src.data.storage import Storage
from src.strategy.schema import Strategy, StrategyReport, Trade


class StrategyStore:
    """策略持久化管理器。"""

    def __init__(self, db: Storage):
        self.db = db

    # ── 策略定义 ─────────────────────────────────────────

    def save_strategy(self, strategy: Strategy) -> int:
        """保存或更新策略定义，返回 row id。"""
        yaml_body = strategy.to_yaml()
        tags_json = json.dumps(strategy.tags, ensure_ascii=False)
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        rows = self.db.execute(
            "SELECT id FROM strategy_defs WHERE name = ?", (strategy.name,)
        )
        if rows:
            self.db.execute_write(
                "UPDATE strategy_defs SET description=?, yaml_body=?, parent=?, "
                "version=?, source=?, tags=?, snapshot_time=? WHERE name=?",
                (strategy.description, yaml_body, strategy.parent,
                 strategy.version, strategy.source, tags_json, now, strategy.name)
            )
            return rows[0]["id"]
        else:
            self.db.execute_write(
                "INSERT INTO strategy_defs (name, description, yaml_body, parent, "
                "version, source, tags, created_at, snapshot_time) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (strategy.name, strategy.description, yaml_body, strategy.parent,
                 strategy.version, strategy.source, tags_json,
                 strategy.created_at or now, now)
            )
            rows = self.db.execute(
                "SELECT id FROM strategy_defs WHERE name = ?", (strategy.name,)
            )
            return rows[0]["id"] if rows else -1

    def load_strategy(self, name: str) -> Optional[Strategy]:
        """按名称加载策略。"""
        rows = self.db.execute(
            "SELECT yaml_body FROM strategy_defs WHERE name = ?", (name,)
        )
        if not rows:
            return None
        return Strategy.from_yaml(rows[0]["yaml_body"])

    def list_strategies(self) -> list[dict]:
        """列出所有策略概要。"""
        return self.db.execute(
            "SELECT name, description, parent, version, source, tags, created_at "
            "FROM strategy_defs ORDER BY created_at DESC"
        )

    def delete_strategy(self, name: str) -> bool:
        """删除策略及其关联数据。"""
        self.db.execute_write(
            "DELETE FROM strategy_trades WHERE strategy_name = ?", (name,)
        )
        self.db.execute_write(
            "DELETE FROM strategy_reports WHERE strategy_name = ?", (name,)
        )
        self.db.execute_write(
            "DELETE FROM strategy_defs WHERE name = ?", (name,)
        )
        return True

    # ── 回测报告 ─────────────────────────────────────────

    def save_report(self, report: StrategyReport) -> int:
        """保存回测报告及关联交易记录，返回 report id。"""
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # 先确保策略存在
        strategy_exists = self.db.execute(
            "SELECT 1 FROM strategy_defs WHERE name = ?",
            (report.strategy_name,)
        )
        if not strategy_exists:
            # 自动创建一个占位策略
            placeholder = Strategy(
                name=report.strategy_name,
                description="auto-created from report",
                entry=None,
                exit=None,
            )
            self.save_strategy(placeholder)

        self.db.execute_write(
            "INSERT INTO strategy_reports "
            "(strategy_name, backtest_start, backtest_end, total_trades, "
            "win_rate, total_return_pct, sharpe_ratio, max_drawdown_pct, "
            "profit_loss_ratio, report_yaml, snapshot_time) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (report.strategy_name, report.backtest_start, report.backtest_end,
             report.total_trades, report.win_rate, report.total_return_pct,
             report.sharpe_ratio, report.max_drawdown_pct, report.profit_loss_ratio,
             report.to_yaml(), now)
        )

        rows = self.db.execute(
            "SELECT id FROM strategy_reports WHERE strategy_name = ? "
            "ORDER BY id DESC LIMIT 1", (report.strategy_name,)
        )
        report_id = rows[0]["id"] if rows else -1

        # 保存交易记录
        for trade in report.trades:
            self._save_trade(trade, now)

        return report_id

    def load_latest_report(self, strategy_name: str) -> Optional[StrategyReport]:
        """加载策略的最新回测报告。"""
        rows = self.db.execute(
            "SELECT report_yaml FROM strategy_reports "
            "WHERE strategy_name = ? ORDER BY id DESC LIMIT 1",
            (strategy_name,)
        )
        if not rows or not rows[0]["report_yaml"]:
            return None
        return StrategyReport.from_yaml(rows[0]["report_yaml"])

    def _save_trade(self, trade: Trade, snapshot_time: str) -> None:
        """保存单笔交易记录。"""
        self.db.execute_write(
            "INSERT INTO strategy_trades "
            "(strategy_name, stock_code, entry_date, entry_price, "
            "exit_date, exit_price, return_pct, hold_days, exit_reason, "
            "regime_at_entry, snapshot_time) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (trade.strategy_name, trade.stock_code, trade.entry_date,
             trade.entry_price, trade.exit_date, trade.exit_price,
             trade.return_pct, trade.hold_days, trade.exit_reason,
             trade.regime_at_entry, snapshot_time)
        )

    # ── 查询 ─────────────────────────────────────────────

    def get_trades(self, strategy_name: str,
                   start_date: str = "", end_date: str = "") -> list[dict]:
        """查询策略的交易记录。"""
        sql = "SELECT * FROM strategy_trades WHERE strategy_name = ?"
        params: list = [strategy_name]
        if start_date:
            sql += " AND entry_date >= ?"
            params.append(start_date)
        if end_date:
            sql += " AND entry_date <= ?"
            params.append(end_date)
        sql += " ORDER BY entry_date"
        return self.db.execute(sql, tuple(params))

    def get_report_summary(self, strategy_name: str) -> Optional[dict]:
        """获取策略最新报告摘要。"""
        rows = self.db.execute(
            "SELECT strategy_name, backtest_start, backtest_end, total_trades, "
            "win_rate, total_return_pct, sharpe_ratio, max_drawdown_pct, "
            "profit_loss_ratio FROM strategy_reports "
            "WHERE strategy_name = ? ORDER BY id DESC LIMIT 1",
            (strategy_name,)
        )
        return rows[0] if rows else None
