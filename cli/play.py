"""Fast, read-only USER entrypoint for precomputed play cards."""

from __future__ import annotations

import argparse
import importlib.util
import json
import math
import sqlite3
import sys
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


def _admission_label(card: Any) -> str:
    if card.admission_status == "NOT_ADMITTED":
        return "PAPER/未准入（模拟记录，不是实盘建议）"
    if card.admission_status == "ADMISSION_ELIGIBLE":
        return "PAPER/准入候选（仍需负责人验收）"
    return "PAPER/已准入"


def _candidate_text(candidate: dict[str, Any]) -> str:
    code = str(candidate.get("stock_code") or "未知代码")
    name = str(candidate.get("stock_name") or "")
    board_count = candidate.get("board_count")
    known = {
        "stock_code",
        "stock_name",
        "board_count",
        "paper_status",
        "result_trade_date",
        "result_reason",
        "entry_trade_date",
        "entry_price",
        "entry_proxy",
        "exit_trade_date",
        "exit_price",
        "exit_proxy",
        "total_cost_bps",
        "net_return_pct",
        "industry",
        "signal_close",
        "allowed_open_low",
        "allowed_open_high",
        "previous_zt_breadth",
        "current_zt_breadth",
        "signal_amount",
        "selection_reason",
        "abandon_conditions",
        "entry_gap_pct",
    }
    suffix = f"，{board_count}板" if board_count is not None else ""
    extras = {key: value for key, value in candidate.items() if key not in known}
    extra_text = (
        "，" + json.dumps(extras, ensure_ascii=False, sort_keys=True)
        if extras
        else ""
    )
    if "allowed_open_low" in candidate and "allowed_open_high" in candidate:
        details = (
            f"，{candidate.get('industry') or '未知行业'}，D收盘"
            f"{_format_candidate_number(candidate.get('signal_close'))}，"
            f"D+1允许开盘{_format_candidate_number(candidate.get('allowed_open_low'))}–"
            f"{_format_candidate_number(candidate.get('allowed_open_high'))}，"
            f"宽度{candidate.get('previous_zt_breadth')}→"
            f"{candidate.get('current_zt_breadth')}，"
            f"D成交额{_format_candidate_number(candidate.get('signal_amount'))}；"
            f"为何入选：{candidate.get('selection_reason') or '预注册条件成立'}"
        )
        return f"{code} {name}{details}{extra_text}".strip()
    return f"{code} {name}{suffix}{extra_text}".strip()


def _candidate_action(candidate: dict[str, Any]) -> str:
    status = str(candidate.get("paper_status") or "PLANNED")
    reason = str(candidate.get("result_reason") or "")
    if status == "NOT_TRIGGERED":
        return f"未触发：{reason or 'D日条件未成立'}"
    if status == "UNFILLED":
        return f"未成交：{reason or 'D日代理不可成交'}"
    if status == "TRIGGERED":
        exit_plan = (
            "计划D+3开盘（入场后的第二个后续开盘）模拟卖出"
            if candidate.get("allowed_open_low") is not None
            else "计划D+1开盘模拟卖出"
        )
        return (
            f"已模拟买入：{candidate.get('entry_trade_date') or 'D日'} @ "
            f"{_format_candidate_number(candidate.get('entry_price'))}；{exit_plan}"
        )
    if status == "COMPLETED":
        return (
            f"模拟买入：{candidate.get('entry_trade_date') or 'D日'} @ "
            f"{_format_candidate_number(candidate.get('entry_price'))}；"
            f"模拟卖出：{candidate.get('exit_trade_date') or 'D+1'} @ "
            f"{_format_candidate_number(candidate.get('exit_price'))}；"
            f"成本后收益：{_format_signed_percent(candidate.get('net_return_pct'))}"
        )
    if candidate.get("allowed_open_low") is not None:
        return (
            "明日模拟动作：若开盘位于预设[-2%, +5%]区间且非涨停开盘，"
            "则按开盘价模拟买入；否则自动记录未成交"
        )
    return (
        "明日模拟动作：若四板开板回封且代理可成交，则按涨停价模拟买入；"
        "否则自动记录未触发/未成交"
    )


def _format_candidate_number(value: Any) -> str:
    if _is_finite_number(value):
        return f"{float(value):.4f}"
    return "暂无"


def _format_signed_percent(value: Any) -> str:
    if _is_finite_number(value):
        return f"{float(value):+.4f}%"
    return "暂无"


def _evidence_text(evidence: dict[str, Any]) -> str:
    labels = {
        "signal_days": "信号日",
        "candidate_count": "历史候选",
        "proxy_trigger_count": "代理触发",
        "completed_count": "已完成",
        "unfinished_count": "未完成",
        "untradable_count": "不可成交",
        "trigger_rate": "触发率",
        "win_rate": "胜率",
        "avg_net_return_pct": "平均成本后收益",
        "profit_loss_ratio": "盈亏比",
        "max_drawdown_pct": "最大回撤",
        "total_cost_bps": "成本",
        "metrics_available": "指标可用",
        "entry_proxy": "入场代理",
        "exit_proxy": "退出代理",
        "data_limitations": "数据限制",
        "research_status": "研究状态",
        "usage_status": "用途状态",
        "independent_signal_days": "development独立收益日",
        "development_mean_net_return_pct": "development D+3开盘成本后均值",
        "development_ci95_pct": "development 95%置信区间",
        "holm_significant": "Holm校正显著",
        "late_period_mean_net_return_pct": "development后段均值",
        "current_candidate_count": "本日候选",
        "previous_day_audit_source": "前一交易日证据来源",
        "empty_reason": "本日0候选原因",
    }
    ordered = [key for key in labels if key in evidence]
    ordered.extend(sorted(key for key in evidence if key not in labels))
    return "\n".join(
        f"  - {labels.get(key, key)}：{_format_evidence_value(key, evidence[key])}"
        for key in ordered
    )


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


def _print_card(card: Any, index: int) -> None:
    print(f"\n===== 玩法卡 {index} =====")
    print(f"玩法：{card.play_name}")
    print(f"行为逻辑：{card.behavior_logic}")
    print(f"数据日：{card.signal_trade_date}")
    print(f"PAPER/准入：{_admission_label(card)}")
    research_status = card.historical_evidence.get("research_status")
    usage_status = card.historical_evidence.get("usage_status")
    if research_status or usage_status:
        print(
            "研究边界："
            f"{research_status or '未标注'} / {usage_status or '未标注'} / "
            f"{card.admission_status}"
        )
    print("候选：")
    if card.candidates:
        for candidate in card.candidates:
            print(f"  - {_candidate_text(candidate)}")
            print(f"    {_candidate_action(candidate)}")
    else:
        reason = card.historical_evidence.get("empty_reason")
        if reason:
            print(f"  - 本日0只符合条件的PAPER候选：{reason}")
        else:
            print("  - 今日暂无符合条件的PAPER候选")
    print(f"触发：{card.trigger_rule}")
    print(f"放弃：{card.abandon_rule}")
    print(f"卖出：{card.exit_rule}")
    print("历史证据：")
    print(_evidence_text(card.historical_evidence))


def run(db_path: str) -> int:
    """Read and print cards without creating or modifying any file."""
    path = Path(db_path)
    if not path.is_file():
        print(WAITING_MESSAGE)
        return 0
    try:
        latest_cards, result_cards = _load_play_card_sections(
            ReadOnlyPlayCardDatabase(path)
        )
    except (KeyError, OSError, TypeError, ValueError, sqlite3.Error):
        print(WAITING_MESSAGE)
        return 0
    if not latest_cards:
        print(WAITING_MESSAGE)
        return 0
    print("Alpha Miner 今日预计算玩法")
    print("\n=== 今日计划 ===")
    for index, card in enumerate(latest_cards, 1):
        _print_card(card, index)
    if result_cards:
        print("\n=== 最近PAPER结果 ===")
        for index, card in enumerate(result_cards, 1):
            _print_card(card, index)
    else:
        print("\n首批PAPER计划尚未到结算日；下一成功采集后自动更新结果")
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
