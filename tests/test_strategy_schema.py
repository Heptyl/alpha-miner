"""策略数据结构序列化/反序列化测试。"""

import pytest
import yaml

from src.strategy.schema import (
    EntryRule, ExitRule, PositionRule, Strategy, Trade, StrategyReport,
)


# ── EntryRule ────────────────────────────────────────────

class TestEntryRule:
    def test_default_values(self):
        e = EntryRule()
        assert e.regime_filter == []
        assert e.conditions == []
        assert e.timing == "next_open"

    def test_to_dict_roundtrip(self):
        e = EntryRule(
            regime_filter=["board_rally"],
            conditions=[{"factor": "consecutive_board", "op": ">=", "value": 1}],
            timing="next_open",
        )
        d = e.to_dict()
        e2 = EntryRule.from_dict(d)
        assert e2.regime_filter == ["board_rally"]
        assert e2.conditions[0]["factor"] == "consecutive_board"
        assert e2.timing == "next_open"

    def test_from_dict_ignores_extra_keys(self):
        """from_dict 应忽略 YAML 中存在但 dataclass 中不存在的键。"""
        d = {
            "regime_filter": [],
            "conditions": [],
            "timing": "next_open",
            "unknown_key": "should_be_ignored",
        }
        e = EntryRule.from_dict(d)
        assert e.timing == "next_open"


# ── ExitRule ─────────────────────────────────────────────

class TestExitRule:
    def test_default_values(self):
        e = ExitRule()
        assert e.take_profit_pct == 5.0
        assert e.stop_loss_pct == -3.0
        assert e.max_hold_days == 3
        assert e.trailing_stop_pct is None
        assert e.exit_conditions == []

    def test_to_dict_roundtrip(self):
        e = ExitRule(
            take_profit_pct=7.0,
            stop_loss_pct=-4.0,
            max_hold_days=5,
            trailing_stop_pct=2.0,
            exit_conditions=[{"factor": "theme_lifecycle", "op": "<", "value": 0.2,
                              "reason": "题材衰退"}],
        )
        d = e.to_dict()
        e2 = ExitRule.from_dict(d)
        assert e2.take_profit_pct == 7.0
        assert e2.trailing_stop_pct == 2.0
        assert len(e2.exit_conditions) == 1


# ── PositionRule ─────────────────────────────────────────

class TestPositionRule:
    def test_default_values(self):
        p = PositionRule()
        assert p.single_position_pct == 20.0
        assert p.max_holdings == 3
        assert p.total_position_pct == 80.0

    def test_to_dict_roundtrip(self):
        p = PositionRule(single_position_pct=15.0, max_holdings=4, total_position_pct=60.0)
        p2 = PositionRule.from_dict(p.to_dict())
        assert p2.single_position_pct == 15.0
        assert p2.max_holdings == 4


# ── Strategy ─────────────────────────────────────────────

class TestStrategy:
    def _make_strategy(self) -> Strategy:
        return Strategy(
            name="测试策略",
            description="测试用",
            entry=EntryRule(
                regime_filter=["board_rally"],
                conditions=[
                    {"factor": "consecutive_board", "op": ">=", "value": 1},
                    {"factor": "turnover_rank", "op": ">=", "value": 0.3},
                ],
            ),
            exit=ExitRule(take_profit_pct=7.0, stop_loss_pct=-3.0, max_hold_days=3),
            position=PositionRule(single_position_pct=25.0, max_holdings=3),
            tags=["打板", "龙头"],
            source="knowledge_base",
        )

    def test_to_dict_roundtrip(self):
        s = self._make_strategy()
        d = s.to_dict()
        s2 = Strategy.from_dict(d)
        assert s2.name == "测试策略"
        assert s2.entry.regime_filter == ["board_rally"]
        assert len(s2.entry.conditions) == 2
        assert s2.exit.take_profit_pct == 7.0
        assert s2.position.max_holdings == 3
        assert s2.tags == ["打板", "龙头"]

    def test_to_yaml_and_back(self):
        s = self._make_strategy()
        yaml_str = s.to_yaml()
        s2 = Strategy.from_yaml(yaml_str)
        assert s2.name == "测试策略"
        assert s2.entry.conditions[0]["factor"] == "consecutive_board"

    def test_from_yaml_file_format(self):
        """模拟 strategies.yaml 中单条策略的格式。"""
        yaml_str = """
strategy:
  name: 首板打板_龙头确认
  description: 题材启动期，龙头首板次日低开介入
  entry:
    regime_filter: [board_rally, theme_rotation]
    conditions:
      - {factor: consecutive_board, op: ">=", value: 1}
    timing: next_open
  exit:
    take_profit_pct: 7.0
    stop_loss_pct: -3.0
    max_hold_days: 3
  position:
    single_position_pct: 25.0
    max_holdings: 3
  tags: [打板, 龙头, 首板]
  version: 1
  source: manual
"""
        s = Strategy.from_yaml(yaml_str)
        assert s.name == "首板打板_龙头确认"
        assert s.entry.regime_filter == ["board_rally", "theme_rotation"]
        assert s.exit.take_profit_pct == 7.0
        assert s.position.single_position_pct == 25.0

    def test_created_at_auto_filled(self):
        s = Strategy(name="x", description="", entry=EntryRule(), exit=ExitRule())
        assert s.created_at != ""

    def test_from_dict_nested_conversion(self):
        """entry/exit/position 传入 dict 时自动转为 dataclass。"""
        d = {
            "name": "nested",
            "description": "test",
            "entry": {"conditions": [{"factor": "x", "op": ">", "value": 0}]},
            "exit": {"take_profit_pct": 10.0},
            "position": {"max_holdings": 5},
        }
        s = Strategy.from_dict(d)
        assert isinstance(s.entry, EntryRule)
        assert isinstance(s.exit, ExitRule)
        assert isinstance(s.position, PositionRule)
        assert s.exit.take_profit_pct == 10.0


# ── Trade ────────────────────────────────────────────────

class TestTrade:
    def test_to_dict_roundtrip(self):
        t = Trade(
            strategy_name="测试策略",
            stock_code="000001",
            entry_date="2024-01-01",
            entry_price=10.0,
            exit_date="2024-01-03",
            exit_price=10.5,
            return_pct=5.0,
            hold_days=2,
            exit_reason="take_profit:5.0%",
            regime_at_entry="board_rally",
        )
        d = t.to_dict()
        t2 = Trade.from_dict(d)
        assert t2.strategy_name == "测试策略"
        assert t2.entry_price == 10.0
        assert t2.return_pct == 5.0
        assert t2.regime_at_entry == "board_rally"


# ── StrategyReport ───────────────────────────────────────

class TestStrategyReport:
    def test_to_dict_roundtrip(self):
        r = StrategyReport(
            strategy_name="test",
            backtest_start="2024-01-01",
            backtest_end="2024-06-30",
            total_trades=10,
            win_rate=0.6,
            profit_loss_ratio=1.5,
            trades=[
                Trade(strategy_name="test", stock_code="000001", return_pct=5.0),
            ],
        )
        d = r.to_dict()
        r2 = StrategyReport.from_dict(d)
        assert r2.total_trades == 10
        assert r2.win_rate == 0.6
        assert len(r2.trades) == 1
        assert isinstance(r2.trades[0], Trade)

    def test_to_yaml_and_back(self):
        r = StrategyReport(
            strategy_name="yaml_test",
            backtest_start="2024-01-01",
            backtest_end="2024-06-30",
            total_trades=5,
        )
        yaml_str = r.to_yaml()
        r2 = StrategyReport.from_yaml(yaml_str)
        assert r2.strategy_name == "yaml_test"
        assert r2.total_trades == 5

    def test_regime_stats_roundtrip(self):
        r = StrategyReport(
            strategy_name="regime_test",
            backtest_start="2024-01-01",
            backtest_end="2024-06-30",
            regime_stats={"board_rally": {"trades": 15, "win_rate": 0.73}},
        )
        d = r.to_dict()
        r2 = StrategyReport.from_dict(d)
        assert "board_rally" in r2.regime_stats
        assert r2.regime_stats["board_rally"]["win_rate"] == 0.73
