"""Fail-closed point-in-time data facade for generated research compute code."""

from __future__ import annotations

import ast
import builtins
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Any
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

SHANGHAI = ZoneInfo("Asia/Shanghai")


class PITMode(str, Enum):
    FORWARD = "FORWARD"
    RETRO_DEVELOPMENT = "RETRO_DEVELOPMENT"


class PointInTimeError(RuntimeError):
    """Raised when research code asks for data outside its bound information set."""


class ResearchCodeError(ValueError):
    """Raised when generated compute code violates the executable contract."""


@dataclass(frozen=True)
class _TablePolicy:
    event_column: str
    event_kind: str
    availability_column: str
    retro_allowed: bool = True


_POLICIES = {
    "daily_price": _TablePolicy("trade_date", "date", "snapshot_time"),
    "zt_pool": _TablePolicy("trade_date", "date", "snapshot_time"),
    "zb_pool": _TablePolicy("trade_date", "date", "snapshot_time"),
    "strong_pool": _TablePolicy("trade_date", "date", "snapshot_time"),
    "lhb_detail": _TablePolicy("trade_date", "date", "snapshot_time"),
    "fund_flow": _TablePolicy("trade_date", "date", "snapshot_time"),
    "concept_daily": _TablePolicy("trade_date", "date", "snapshot_time"),
    "market_emotion": _TablePolicy("trade_date", "date", "snapshot_time"),
    "regime_state": _TablePolicy("trade_date", "date", "snapshot_time"),
    "factor_values": _TablePolicy("trade_date", "date", "snapshot_time"),
    "news": _TablePolicy("publish_time", "datetime", "snapshot_time", False),
    "prelimit_snapshots": _TablePolicy("observed_at", "datetime", "snapshot_time", False),
    "minute_bars_5m": _TablePolicy("bar_time", "datetime", "first_fetched_at"),
}

_SAFE_WHERE = re.compile(r"^[A-Za-z0-9_?(),.\s<>=!]+$")
_BANNED_WHERE = re.compile(
    r"\b(SELECT|INSERT|UPDATE|DELETE|DROP|ALTER|PRAGMA|ATTACH|UNION|WITH)\b|;|--|/\*",
    re.IGNORECASE,
)


class PointInTimeView:
    """A decision-bound facade whose public surface is only query/query_range."""

    __slots__ = ("__storage", "__decision_at", "__mode")
    _PUBLIC = frozenset({"query", "query_range"})

    def __init__(self, storage: Any, decision_at: datetime, mode: PITMode | str):
        object.__setattr__(self, "_PointInTimeView__storage", storage)
        object.__setattr__(
            self,
            "_PointInTimeView__decision_at",
            _normalize_time(decision_at, "decision_at"),
        )
        try:
            normalized_mode = PITMode(mode)
        except ValueError as exc:
            raise PointInTimeError(f"未知PIT模式：{mode}") from exc
        object.__setattr__(self, "_PointInTimeView__mode", normalized_mode)

    def __getattribute__(self, name: str):
        if name not in object.__getattribute__(self, "_PUBLIC"):
            raise AttributeError(f"PointInTimeView仅暴露query/query_range，不提供{name}")
        return object.__getattribute__(self, name)

    def __setattr__(self, name: str, value: Any) -> None:
        raise AttributeError("PointInTimeView不可修改")

    def query(
        self,
        table: str,
        as_of: datetime,
        where: str = "",
        params: tuple = (),
    ) -> pd.DataFrame:
        return object.__getattribute__(self, "_query_bound")(
            table, as_of, where, params
        )

    def query_range(
        self,
        table: str,
        as_of: datetime,
        lookback_days: int,
        date_col: str | None = None,
        where: str = "",
        params: tuple = (),
    ) -> pd.DataFrame:
        if isinstance(lookback_days, bool) or not isinstance(lookback_days, int):
            raise PointInTimeError("lookback_days必须是正整数")
        if lookback_days < 1:
            raise PointInTimeError("lookback_days必须是正整数")
        policy = _policy_for(
            table, object.__getattribute__(self, "_PointInTimeView__mode")
        )
        if date_col is not None and date_col != policy.event_column:
            raise PointInTimeError("date_col必须等于该表受控事件时间列")
        effective = object.__getattribute__(self, "_effective_as_of")(as_of)
        start = effective - timedelta(days=lookback_days)
        lower = (
            f"date([{policy.event_column}]) >= date(?)"
            if policy.event_kind == "date"
            else f"datetime([{policy.event_column}]) >= datetime(?)"
        )
        combined = f"({lower})" + (f" AND ({where})" if where else "")
        return object.__getattribute__(self, "_query_bound")(
            table,
            effective,
            combined,
            (_sql_time(start), *params),
            internal_where=True,
        )

    def _effective_as_of(self, as_of: datetime) -> datetime:
        requested = _normalize_time(as_of, "as_of")
        decision = object.__getattribute__(self, "_PointInTimeView__decision_at")
        if requested > decision:
            raise PointInTimeError("as_of晚于绑定decision_at")
        return requested

    def _query_bound(
        self,
        table: str,
        as_of: datetime,
        where: str,
        params: tuple,
        *,
        internal_where: bool = False,
    ) -> pd.DataFrame:
        mode = object.__getattribute__(self, "_PointInTimeView__mode")
        policy = _policy_for(table, mode)
        effective = object.__getattribute__(self, "_effective_as_of")(as_of)
        if where and not internal_where:
            _validate_where(where)
        elif internal_where:
            # The caller-controlled suffix remains subject to the same grammar.
            suffix = where.split(" AND (", 1)[1][:-1] if " AND (" in where else ""
            if suffix:
                _validate_where(suffix)

        event_clause = (
            f"date([{policy.event_column}]) <= date(?)"
            if policy.event_kind == "date"
            else f"datetime([{policy.event_column}]) <= datetime(?)"
        )
        clauses = [event_clause]
        sql_params: list[Any] = [_sql_time(effective)]
        if mode == PITMode.FORWARD:
            clauses.append(f"datetime([{policy.availability_column}]) <= datetime(?)")
            sql_params.append(_sql_time(effective))
        if where:
            clauses.append(f"({where})")
            sql_params.extend(params)
        sql = f"SELECT * FROM [{table}] WHERE " + " AND ".join(clauses)
        storage = object.__getattribute__(self, "_PointInTimeView__storage")
        frame = pd.DataFrame(storage.execute(sql, tuple(sql_params)))
        frame.attrs["pit_mode"] = mode.value
        frame.attrs["decision_at"] = object.__getattribute__(
            self, "_PointInTimeView__decision_at"
        ).isoformat(timespec="seconds")
        frame.attrs["research_label"] = (
            "RETRO_DEVELOPMENT_ONLY"
            if mode == PITMode.RETRO_DEVELOPMENT
            else "FORWARD_POINT_IN_TIME"
        )
        return frame


def validate_compute_source(code: str) -> ast.Module:
    """Reject generated code that can escape the PIT query-only contract."""
    try:
        tree = ast.parse(code)
    except SyntaxError as exc:
        raise ResearchCodeError(f"因子代码语法错误：{exc.msg}") from exc
    compute_defs = [
        node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == "compute"
    ]
    if len(compute_defs) != 1:
        raise ResearchCodeError("代码必须且只能定义一个compute函数")
    args = compute_defs[0].args
    if [arg.arg for arg in args.args] != ["universe", "as_of", "db"] or args.vararg or args.kwarg:
        raise ResearchCodeError("compute签名必须为compute(universe, as_of, db)")

    banned_calls = {
        "open",
        "eval",
        "exec",
        "compile",
        "__import__",
        "getattr",
        "setattr",
        "delattr",
        "globals",
        "locals",
        "vars",
        "input",
    }
    banned_attributes = {
        "execute",
        "execute_write",
        "_get_conn",
        "insert",
        "init_db",
        "db_path",
        "backtest_mode",
        "bypass_snapshot",
        "read_sql",
        "read_sql_query",
        "to_sql",
    }
    for node in ast.walk(tree):
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            raise ResearchCodeError("生成代码禁止import")
        if isinstance(node, ast.Attribute):
            if node.attr.startswith("_") or node.attr in banned_attributes:
                raise ResearchCodeError(f"禁止访问属性：{node.attr}")
            if isinstance(node.value, ast.Name) and node.value.id == "db" and node.attr not in {
                "query",
                "query_range",
            }:
                raise ResearchCodeError(f"db仅允许query/query_range，不允许{node.attr}")
        if isinstance(node, ast.Call):
            if isinstance(node.func, ast.Name) and node.func.id in banned_calls:
                raise ResearchCodeError(f"禁止调用：{node.func.id}")
            if any(keyword.arg == "bypass_snapshot" for keyword in node.keywords):
                raise ResearchCodeError("禁止bypass_snapshot")
        if isinstance(node, ast.Name) and node.id in {"__builtins__", "__import__"}:
            raise ResearchCodeError(f"禁止访问：{node.id}")
        if isinstance(node, (ast.Global, ast.Nonlocal)):
            raise ResearchCodeError("生成代码禁止global/nonlocal")
    return tree


def compile_compute_source(code: str, factor_name: str = "unknown"):
    """Compile validated code in the same restricted namespace for both paths."""
    tree = validate_compute_source(code)
    safe_builtins = {
        "abs": abs,
        "all": all,
        "any": any,
        "bool": bool,
        "dict": dict,
        "enumerate": enumerate,
        "Exception": Exception,
        "float": float,
        "int": int,
        "isinstance": isinstance,
        "len": len,
        "list": list,
        "max": max,
        "min": min,
        "range": range,
        "round": round,
        "set": set,
        "sorted": sorted,
        "str": str,
        "sum": sum,
        "tuple": tuple,
        "ValueError": ValueError,
        "zip": zip,
        # Required by C-extension internals such as datetime.strftime. Generated
        # source cannot reference __import__/__builtins__ because AST rejects both.
        "__import__": builtins.__import__,
    }
    namespace = {
        "__builtins__": safe_builtins,
        "__name__": f"factor_{factor_name}",
        "datetime": datetime,
        "timedelta": timedelta,
        "np": np,
        "numpy": np,
        "pd": pd,
        "pandas": pd,
        "re": re,
    }
    exec(compile(tree, f"<factor:{factor_name}>", "exec"), namespace)  # noqa: S102
    return namespace["compute"]


def _policy_for(table: str, mode: PITMode) -> _TablePolicy:
    if table not in _POLICIES:
        raise PointInTimeError(f"表{table}没有可靠事件/可用时间契约，默认拒绝")
    policy = _POLICIES[table]
    if mode == PITMode.RETRO_DEVELOPMENT and not policy.retro_allowed:
        raise PointInTimeError(f"表{table}不允许RETRO_DEVELOPMENT")
    return policy


def _validate_where(where: str) -> None:
    if not _SAFE_WHERE.fullmatch(where) or _BANNED_WHERE.search(where):
        raise PointInTimeError("where仅允许参数化条件表达式，禁止SQL语句/字面量")


def _normalize_time(value: datetime, field: str) -> datetime:
    if not isinstance(value, datetime):
        raise PointInTimeError(f"{field}必须是datetime")
    if value.tzinfo is None:
        return value.replace(tzinfo=SHANGHAI)
    return value.astimezone(SHANGHAI)


def _sql_time(value: datetime) -> str:
    """Match the repository's naive Asia/Shanghai SQLite timestamp format."""
    return value.astimezone(SHANGHAI).strftime("%Y-%m-%d %H:%M:%S")
