"""Fast, read-only USER entrypoint for precomputed play cards."""

from __future__ import annotations

import argparse
import importlib.util
import json
import math
import sqlite3
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

# USER read-only sandboxes must not depend on writable __pycache__ directories.
sys.dont_write_bytecode = True


WAITING_MESSAGE = "暂无预计算玩法卡，等待后台任务生成"


class ReadOnlyPlayCardDatabase:
    """Small SQLite adapter that can only execute reads in ``mode=ro``."""

    def __init__(self, db_path: str | Path):
        self.path = Path(db_path).resolve()
        self.uri = f"{self.path.as_uri()}?mode=ro"

    def execute(self, sql: str, params: tuple = ()) -> list[dict[str, Any]]:
        connection = sqlite3.connect(self.uri, uri=True, timeout=2)
        connection.row_factory = sqlite3.Row
        try:
            cursor = connection.execute(sql, params)
            return [dict(row) for row in cursor.fetchall()]
        finally:
            connection.close()


_SHANGHAI = timezone(timedelta(hours=8))
_POST_CLOSE = (15, 40)


def _status_text(card: Any) -> str:
    position = (
        "实盘仓位0"
        if card.admission_status != "ADMITTED"
        else "实盘仓位按准入规则"
    )
    evidence = card.historical_evidence
    research = evidence.get("research_status")
    usage = evidence.get("usage_status")
    prefix = (
        f"{research or '未标注'}/{usage or '未标注'}｜"
        if research or usage
        else ""
    )
    return f"{prefix}PAPER/{card.paper_status}｜{card.admission_status}｜{position}｜不是实盘建议"


def _candidate_text(candidate: dict[str, Any]) -> str:
    code = str(candidate.get("stock_code") or "未知代码")
    name = str(candidate.get("stock_name") or "")
    board_count = candidate.get("board_count")
    suffix = f"（{board_count}板）" if board_count is not None else ""
    if "allowed_open_low" in candidate and "allowed_open_high" in candidate:
        reason = candidate.get("selection_reason") or "预注册条件成立"
        return (
            f"{code} {name}（{candidate.get('industry') or '未知行业'}；"
            f"D收盘{_format_candidate_number(candidate.get('signal_close'))}；"
            f"允许开盘{_format_candidate_number(candidate.get('allowed_open_low'))}–"
            f"{_format_candidate_number(candidate.get('allowed_open_high'))}；"
            f"宽度{candidate.get('previous_zt_breadth')}→"
            f"{candidate.get('current_zt_breadth')}；"
            f"成交额{_format_candidate_number(candidate.get('signal_amount'))}；"
            f"为何入选：{reason}）"
        ).strip()
    return f"{code} {name}{suffix}".strip()


def _format_candidate_number(value: Any) -> str:
    if _is_finite_number(value):
        return f"{float(value):.4f}"
    return "暂无"


def _format_signed_percent(value: Any) -> str:
    if _is_finite_number(value):
        return f"{float(value):+.4f}%"
    return "暂无"


def _evidence_lines(evidence: dict[str, Any]) -> list[str]:
    counts = []
    for key, label in (
        ("signal_days", "信号日"),
        ("candidate_count", "候选"),
        ("proxy_trigger_count", "代理触发"),
        ("completed_count", "已完成"),
        ("unfinished_count", "未完成"),
        ("untradable_count", "不可成交"),
        ("independent_signal_days", "development独立收益日"),
    ):
        if key in evidence:
            counts.append(f"{label}{_format_evidence_value(key, evidence[key])}")
    lines = ["历史开发证据：" + "｜".join(counts)] if counts else []

    metrics = []
    for key, label in (
        ("trigger_rate", "触发率"),
        ("win_rate", "胜率"),
        ("avg_net_return_pct", "平均成本后收益"),
        ("profit_loss_ratio", "盈亏比"),
        ("max_drawdown_pct", "最大回撤"),
        ("development_mean_net_return_pct", "D+3开盘成本后均值"),
        ("development_ci95_pct", "95%CI"),
        ("holm_significant", "Holm显著"),
        ("late_period_mean_net_return_pct", "后段均值"),
    ):
        if key in evidence:
            value = (
                _win_rate_text(evidence)
                if key == "win_rate"
                else _format_evidence_value(key, evidence[key])
            )
            metrics.append(f"{label}{value}")
    if metrics:
        lines.append("历史开发指标：" + "｜".join(metrics))

    limitations = evidence.get("data_limitations")
    boundary = "样本/holdout不足或未记录；仅作PAPER验证，未证明统计优势。"
    if isinstance(limitations, str) and limitations.strip():
        boundary += " " + limitations.replace("metrics_available=false", "指标不可用")
    lines.append("证据结论：" + boundary)
    return lines


def _win_rate_text(evidence: dict[str, Any]) -> str:
    formatted = _format_evidence_value("win_rate", evidence.get("win_rate"))
    wins = evidence.get("win_count")
    trades = evidence.get("evaluated_count")
    if _is_non_negative_integer(wins) and _is_non_negative_integer(trades):
        if int(trades) > 0 and int(wins) <= int(trades):
            audited_rate = int(wins) / int(trades) * 100
            return f"{audited_rate:.2f}%（{int(wins)}/{int(trades)}）"
    return f"{formatted}（分子/分母未记录）"


def _is_non_negative_integer(value: Any) -> bool:
    return _is_finite_number(value) and float(value).is_integer() and float(value) >= 0


def _format_evidence_value(key: str, value: Any) -> str:
    count_keys = {
        "signal_days",
        "candidate_count",
        "proxy_trigger_count",
        "completed_count",
        "unfinished_count",
        "untradable_count",
        "independent_signal_days",
        "current_candidate_count",
    }
    if key == "metrics_available" and isinstance(value, bool):
        return "是" if value else "否"
    if key in count_keys and _is_finite_number(value):
        number = float(value)
        return str(int(number)) if number.is_integer() else f"{number:g}"
    if key in {"trigger_rate", "win_rate"} and _is_finite_number(value):
        return f"{float(value) * 100:.2f}%"
    if key in {
        "avg_net_return_pct",
        "max_drawdown_pct",
        "development_mean_net_return_pct",
        "late_period_mean_net_return_pct",
    } and _is_finite_number(value):
        return f"{float(value):.4f}%"
    if key == "development_ci95_pct" and isinstance(value, list) and len(value) == 2:
        if all(_is_finite_number(item) for item in value):
            return f"[{float(value[0]):.4f}%, {float(value[1]):.4f}%]"
    if key == "profit_loss_ratio" and _is_finite_number(value):
        return f"{float(value):.4f}"
    if key == "total_cost_bps" and _is_finite_number(value):
        return f"{float(value):g}bp"
    if value is None:
        return "暂无"
    if isinstance(value, bool):
        return "是" if value else "否"
    if _is_finite_number(value):
        return f"{float(value):g}"
    if isinstance(value, (int, float)):
        return "暂无"
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    if key == "data_limitations" and isinstance(value, str):
        return value.replace("metrics_available=false", "指标不可用")
    return str(value)


def _is_finite_number(value: Any) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(float(value))
    )


def _data_health(database: ReadOnlyPlayCardDatabase, card: Any) -> str:
    try:
        rows = database.execute(
            """
            SELECT status, attempted_at
            FROM limit_up_collection_runs
            WHERE trade_date = ?
            ORDER BY attempted_at DESC, id DESC
            LIMIT 1
            """,
            (card.signal_trade_date,),
        )
    except sqlite3.Error:
        return "无法验证（采集审计表未具备）"
    if not rows:
        return "无法验证（该数据日没有采集审计）"
    row = rows[0]
    if row.get("status") != "ok":
        return f"无法验证（最新采集审计状态{row.get('status') or '未知'}）"
    if not _is_post_close_audit(card.signal_trade_date, row.get("attempted_at")):
        return "无法验证（最新成功审计未满足15:40盘后门槛）"
    source = card.historical_evidence.get("previous_day_audit_source")
    suffix = f"；前一交易日来源{source}" if source else ""
    return f"已验证（最新采集审计ok且满足盘后门槛{suffix}）"


def _is_post_close_audit(trade_date: str, attempted_at: Any) -> bool:
    try:
        signal_day = date.fromisoformat(trade_date)
        attempted = datetime.fromisoformat(str(attempted_at).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return False
    local = (
        attempted.replace(tzinfo=_SHANGHAI)
        if attempted.tzinfo is None
        else attempted.astimezone(_SHANGHAI)
    )
    if local.date() != signal_day:
        return local.date() > signal_day
    return (local.hour, local.minute) >= _POST_CLOSE


def _future_market_dates(
    database: ReadOnlyPlayCardDatabase, signal_trade_date: str
) -> list[str]:
    try:
        rows = database.execute(
            """
            SELECT DISTINCT trade_date
            FROM daily_price
            WHERE trade_date > ?
            ORDER BY trade_date
            LIMIT 3
            """,
            (signal_trade_date,),
        )
    except sqlite3.Error:
        return []
    return [str(row["trade_date"]) for row in rows if row.get("trade_date")]


def _plan_dates(card: Any, future_dates: list[str]) -> str:
    entry = future_dates[0] if future_dates else "下一交易日（绝对日期未具备）"
    exit_index = 2 if card.play_id == "theme_new_entrant_diffusion_v1" else 1
    if len(future_dates) > exit_index:
        exit_date = future_dates[exit_index]
    elif exit_index == 2:
        exit_date = "入场后的第二个后续交易日（绝对日期未具备）"
    else:
        exit_date = "入场后的下一交易日（绝对日期未具备）"
    return f"模拟入场{entry}；模拟卖出{exit_date}"


def _entry_text(card: Any) -> str:
    if card.play_id == "three_to_four_reseal":
        return (
            "满足触发后按D日涨停收盘价代理记录；这是盘后研究/成交审计代理，"
            "不是盘中人工买点"
        )
    return "满足触发后按下一交易日开盘价记录PAPER模拟买入"


def _print_card(card: Any, database: ReadOnlyPlayCardDatabase) -> None:
    print(f"玩法：{card.play_name}")
    print(f"状态：{_status_text(card)}")
    print(f"数据日：{card.signal_trade_date}｜数据健康：{_data_health(database, card)}")
    print(f"行为逻辑：{card.behavior_logic}")
    if card.candidates:
        candidates = "；".join(_candidate_text(candidate) for candidate in card.candidates)
        print(f"候选（{len(card.candidates)}只）：{candidates}")
    else:
        reason = card.historical_evidence.get("empty_reason")
        print(f"候选（0只）：{reason or '本日没有符合预注册条件的PAPER候选'}")
    future_dates = _future_market_dates(database, card.signal_trade_date)
    print(f"计划日期：{_plan_dates(card, future_dates)}")
    print(f"触发：{card.trigger_rule}")
    print(f"放弃：{card.abandon_rule}")
    print(f"模拟入场：{_entry_text(card)}")
    print(f"模拟卖出：{card.exit_rule}")
    cost = _format_evidence_value(
        "total_cost_bps", card.historical_evidence.get("total_cost_bps")
    )
    print(f"成本：{cost}")
    for line in _evidence_lines(card.historical_evidence):
        print(line)


def _completed_results(cards: list[Any]) -> list[tuple[Any, dict[str, Any]]]:
    return [
        (card, candidate)
        for card in cards
        for candidate in card.candidates
        if candidate.get("paper_status") == "COMPLETED"
    ]


def _print_recent_results(cards: list[Any]) -> None:
    completed = _completed_results(cards)
    if not completed:
        print("最近PAPER结果：尚无card lifecycle COMPLETED结果；不会用历史开发样本冒充结算")
        return
    print("最近PAPER结果（仅card lifecycle COMPLETED）：")
    for card, candidate in completed:
        print(
            f"  {card.signal_trade_date} {_candidate_text(candidate)}｜"
            f"模拟买入{candidate.get('entry_trade_date') or '日期缺失'} @ "
            f"{_format_candidate_number(candidate.get('entry_price'))}｜"
            f"模拟卖出{candidate.get('exit_trade_date') or '日期缺失'} @ "
            f"{_format_candidate_number(candidate.get('exit_price'))}｜"
            f"成本后收益{_format_signed_percent(candidate.get('net_return_pct'))}"
        )


def run(db_path: str) -> int:
    """Read and print cards without creating or modifying any file."""
    path = Path(db_path)
    if not path.is_file():
        print(WAITING_MESSAGE)
        return 0
    database = ReadOnlyPlayCardDatabase(path)
    try:
        latest_cards, result_cards = _load_play_card_sections(database)
    except (KeyError, OSError, TypeError, ValueError, sqlite3.Error):
        print(WAITING_MESSAGE)
        return 0
    if not latest_cards:
        print(WAITING_MESSAGE)
        return 0
    print("Alpha Miner PAPER玩法（只读预计算）")
    print("\n今日计划")
    for card in latest_cards:
        _print_card(card, database)
    print()
    _print_recent_results(latest_cards + result_cards)
    return 0


def _load_play_card_sections(
    database: ReadOnlyPlayCardDatabase,
) -> tuple[list[Any], list[Any]]:
    """Load the contract module without executing heavy ``src.mining`` imports."""
    module_name = "_alpha_miner_readonly_playbook"
    module = sys.modules.get(module_name)
    if module is None:
        playbook_path = Path(__file__).resolve().parents[1] / "src" / "mining" / "playbook.py"
        spec = importlib.util.spec_from_file_location(module_name, playbook_path)
        if spec is None or spec.loader is None:
            raise ImportError("cannot load playbook contract")
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        try:
            spec.loader.exec_module(module)
        except Exception:
            sys.modules.pop(module_name, None)
            raise
    latest_cards = module.load_latest_play_cards(database)
    before_date = latest_cards[0].signal_trade_date if latest_cards else None
    result_cards = module.load_recent_result_cards(
        database,
        before_date=before_date,
        limit=1,
    )
    return latest_cards, result_cards


def main() -> None:
    parser = argparse.ArgumentParser(description="只读查看最新预计算玩法卡")
    parser.add_argument("--db", default="data/alpha_miner.db", help="SQLite 数据库路径")
    args = parser.parse_args()
    raise SystemExit(run(args.db))


if __name__ == "__main__":
    main()
