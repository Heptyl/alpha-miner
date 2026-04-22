"""策略数据结构定义。

包含: Strategy, EntryRule, ExitRule, PositionRule, Trade, StrategyReport。
支持 dataclass ↔ YAML 序列化/反序列化。
"""

from __future__ import annotations

import copy
import dataclasses
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

import yaml


# ── 入场/出场/仓位规则 ──────────────────────────────────

@dataclass
class EntryRule:
    """入场规则: 满足全部条件才触发买入信号。"""
    regime_filter: list[str] = field(default_factory=list)
    conditions: list[dict] = field(default_factory=list)
    timing: str = "next_open"

    def to_dict(self) -> dict:
        return dataclasses.asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> EntryRule:
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})


@dataclass
class ExitRule:
    """出场规则: 任一条件触发即卖出。"""
    take_profit_pct: float = 5.0
    stop_loss_pct: float = -3.0
    max_hold_days: int = 3
    trailing_stop_pct: Optional[float] = None
    exit_conditions: list[dict] = field(default_factory=list)

    def to_dict(self) -> dict:
        return dataclasses.asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> ExitRule:
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})


@dataclass
class PositionRule:
    """仓位规则。"""
    single_position_pct: float = 20.0
    max_holdings: int = 3
    total_position_pct: float = 80.0

    def to_dict(self) -> dict:
        return dataclasses.asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> PositionRule:
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})


# ── 策略主体 ────────────────────────────────────────────

@dataclass
class Strategy:
    """一个完整的可回测策略。"""
    name: str
    description: str
    entry: EntryRule
    exit: ExitRule
    position: PositionRule = field(default_factory=PositionRule)
    tags: list[str] = field(default_factory=list)
    version: int = 1
    source: str = "manual"
    parent: Optional[str] = None
    created_at: str = ""

    def __post_init__(self):
        if not self.created_at:
            self.created_at = datetime.now().strftime("%Y-%m-%d %H:%M")

    def to_dict(self) -> dict:
        d = dataclasses.asdict(self)
        return d

    @classmethod
    def from_dict(cls, d: dict) -> Strategy:
        """从字典(或 YAML 加载的 dict)重建 Strategy。"""
        d = copy.deepcopy(d)
        if "entry" in d and isinstance(d["entry"], dict):
            d["entry"] = EntryRule.from_dict(d["entry"])
        if "exit" in d and isinstance(d["exit"], dict):
            d["exit"] = ExitRule.from_dict(d["exit"])
        if "position" in d and isinstance(d["position"], dict):
            d["position"] = PositionRule.from_dict(d["position"])
        # 过滤掉非字段 key (如 source/description 等非 dataclass 字段外的)
        valid_keys = {f.name for f in dataclasses.fields(cls)}
        filtered = {k: v for k, v in d.items() if k in valid_keys}
        return cls(**filtered)

    def to_yaml(self) -> str:
        return yaml.dump({"strategy": self.to_dict()}, allow_unicode=True, default_flow_style=False)

    @classmethod
    def from_yaml(cls, yaml_str: str) -> Strategy:
        d = yaml.safe_load(yaml_str)
        key = "strategy" if "strategy" in d else "strategies"
        if key in d and isinstance(d[key], dict):
            return cls.from_dict(d[key])
        raise ValueError(f"YAML 中未找到 'strategy' 键: {list(d.keys())}")


# ── 交易记录 ────────────────────────────────────────────

@dataclass
class Trade:
    """一笔完整的交易记录(回测产生)。"""
    strategy_name: str
    stock_code: str
    stock_name: str = ""
    entry_date: str = ""
    entry_price: float = 0.0
    entry_reason: str = ""
    exit_date: str = ""
    exit_price: float = 0.0
    exit_reason: str = ""
    return_pct: float = 0.0
    hold_days: int = 0
    max_drawdown_pct: float = 0.0
    regime_at_entry: str = ""
    emotion_at_entry: str = ""

    def to_dict(self) -> dict:
        return dataclasses.asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> Trade:
        valid_keys = {f.name for f in dataclasses.fields(cls)}
        return cls(**{k: v for k, v in d.items() if k in valid_keys})


# ── 回测报告 ────────────────────────────────────────────

@dataclass
class StrategyReport:
    """策略回测结果。"""
    strategy_name: str
    backtest_start: str
    backtest_end: str
    total_trades: int = 0
    win_count: int = 0
    loss_count: int = 0
    win_rate: float = 0.0
    avg_win_pct: float = 0.0
    avg_loss_pct: float = 0.0
    profit_loss_ratio: float = 0.0
    max_consecutive_loss: int = 0
    max_drawdown_pct: float = 0.0
    total_return_pct: float = 0.0
    sharpe_ratio: float = 0.0
    trades: list[Trade] = field(default_factory=list)
    regime_stats: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        d = dataclasses.asdict(self)
        return d

    @classmethod
    def from_dict(cls, d: dict) -> StrategyReport:
        d = copy.deepcopy(d)
        if "trades" in d and isinstance(d["trades"], list):
            d["trades"] = [Trade.from_dict(t) if isinstance(t, dict) else t for t in d["trades"]]
        valid_keys = {f.name for f in dataclasses.fields(cls)}
        filtered = {k: v for k, v in d.items() if k in valid_keys}
        return cls(**filtered)

    def to_yaml(self) -> str:
        return yaml.dump({"report": self.to_dict()}, allow_unicode=True, default_flow_style=False)

    @classmethod
    def from_yaml(cls, yaml_str: str) -> StrategyReport:
        d = yaml.safe_load(yaml_str)
        if "report" in d:
            return cls.from_dict(d["report"])
        return cls.from_dict(d)
