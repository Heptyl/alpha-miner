"""事件研究回测 — 因子组合触发条件的横截面 T+1/T+3/T+5 收益研究。

与 BacktestEngine 的区别（互补，不替代）：
- BacktestEngine 是路径依赖的交易模拟：持有到止盈/止损/移动止损/时间出场。
- EventStudy 是固定窗口事件研究：触发样本买入后固定持有 N 个完整交易日，
  不设出场逻辑，输出每个窗口的收益分布/胜率/盈亏比，按 regime 分层 + 分段稳定性。
  用途：体检"满足某因子组合条件的样本，买入后还赚不赚钱"，是验收门改造（决策A）的输入。

口径（已与使用者确认）：
- 买入价 = 信号日(T0)次日(T0+1)开盘价。贴合打板实盘（收盘后选股，次日开盘进）。
- T+N 收益 = (买入后第 N 个完整交易日收盘 / 买入开盘) - 1，与 win_rate_backtest 一致。
  即 T+1 = 买入日再持有一个完整交易日的收盘（含一个完整隔夜，对齐"持板过夜风险"语义）。
- 完整交易日 = daily_price 当日股票数 >= min_stocks，剔除 backfill 残采日（实测正常~5200/残日 257~688）。
- 买入日一字/高开涨停（open 较 T0 收盘高开 >= 9.5%）的样本剔除：实盘买不进。
- 时间隔离：历史为 backfill 数据，强制 backtest_mode（bypass snapshot_time），否则取不到数。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

import numpy as np

from src.data.storage import Storage
from src.drift.regime import RegimeDetector, PricingRegimeDetector
from src.strategy.schema import EntryRule

_OPS = {
    ">=": lambda a, b: a >= b,
    "<=": lambda a, b: a <= b,
    ">": lambda a, b: a > b,
    "<": lambda a, b: a < b,
    "==": lambda a, b: abs(a - b) < 1e-9,
}


@dataclass
class WindowStats:
    """单个持有窗口的收益统计。"""
    forward_days: int
    n: int = 0
    win_rate: float = 0.0
    avg_ret: float = 0.0
    median_ret: float = 0.0
    pnl_ratio: float = 0.0          # 平均盈利 / |平均亏损|
    p10: float = 0.0
    p90: float = 0.0

    def to_dict(self) -> dict:
        return {
            "forward_days": self.forward_days,
            "n": self.n,
            "win_rate": round(self.win_rate, 4),
            "avg_ret": round(self.avg_ret, 4),
            "median_ret": round(self.median_ret, 4),
            "pnl_ratio": round(self.pnl_ratio, 3),
            "p10": round(self.p10, 4),
            "p90": round(self.p90, 4),
        }


@dataclass
class EventStudyResult:
    """事件研究结果。"""
    label: str
    start: str
    end: str
    n_signals: int = 0
    windows: dict = field(default_factory=dict)        # forward_days -> WindowStats
    by_regime: dict = field(default_factory=dict)      # regime -> {fw -> WindowStats}
    by_segment: list = field(default_factory=list)     # [{seg, start, end, n, win_rate_{fw}...}]
    error: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "label": self.label,
            "start": self.start,
            "end": self.end,
            "n_signals": self.n_signals,
            "windows": {k: v.to_dict() for k, v in self.windows.items()},
            "by_regime": {
                r: {k: v.to_dict() for k, v in d.items()} for r, d in self.by_regime.items()
            },
            "by_segment": self.by_segment,
            "error": self.error,
        }


@dataclass
class GateResult:
    """事件研究两段胜率准入门结果（决策A：验收 IC体系 → 事件研究胜率）。"""
    passed: bool
    forward_days: int
    win_threshold: float
    wr_long: float = 0.0       # 近 long_days 段 T+forward 胜率
    wr_short: float = 0.0      # 近 short_days 段 T+forward 胜率
    n_long: int = 0
    n_short: int = 0
    pnl_long: float = 0.0
    long_span: str = ""
    short_span: str = ""
    reason: str = ""

    def to_dict(self) -> dict:
        return {
            "passed": self.passed,
            "forward_days": self.forward_days,
            "win_threshold": self.win_threshold,
            "wr_long": round(self.wr_long, 4),
            "wr_short": round(self.wr_short, 4),
            "n_long": self.n_long,
            "n_short": self.n_short,
            "pnl_long": round(self.pnl_long, 3),
            "long_span": self.long_span,
            "short_span": self.short_span,
            "reason": self.reason,
        }


def _agg(rets: list[float], forward_days: int) -> WindowStats:
    """把一组收益率聚合成 WindowStats。"""
    arr = np.array([r for r in rets if r is not None and not np.isnan(r)], dtype=float)
    s = WindowStats(forward_days=forward_days, n=len(arr))
    if len(arr) == 0:
        return s
    wins = arr[arr > 0]
    losses = arr[arr <= 0]
    s.win_rate = float(len(wins) / len(arr))
    s.avg_ret = float(arr.mean())
    s.median_ret = float(np.median(arr))
    avg_win = float(wins.mean()) if len(wins) else 0.0
    avg_loss = abs(float(losses.mean())) if len(losses) else 0.0
    s.pnl_ratio = (avg_win / avg_loss) if avg_loss > 0 else (float("inf") if avg_win > 0 else 0.0)
    s.p10 = float(np.percentile(arr, 10))
    s.p90 = float(np.percentile(arr, 90))
    return s


class EventStudy:
    """因子组合触发条件的横截面事件研究回测器。"""

    def __init__(self, db: Storage, min_stocks: int = 4000,
                 universe_source: str = "zt_pool", limit_up_gap: float = 0.095,
                 regime_mode: str = "emotion"):
        self.db = db
        self.db.backtest_mode = True   # backfill 数据：必须 bypass snapshot_time
        self.min_stocks = min_stocks
        self.universe_source = universe_source
        self.limit_up_gap = limit_up_gap
        self.regime_mode = regime_mode   # "emotion"(现有) / "pricing"(决策B 定价权)
        self._regime = RegimeDetector(db)
        self._pricing = PricingRegimeDetector(db)

    # ── 公共入口 ──────────────────────────────────────────

    def run(self, entry: EntryRule, start: str, end: str,
            forward_windows: tuple[int, ...] = (1, 3, 5),
            label: str = "event_study") -> EventStudyResult:
        """对 [start, end] 内每个完整交易日筛选触发样本，做事件研究。"""
        result = EventStudyResult(label=label, start=start, end=end)
        forward_windows = tuple(sorted(set(forward_windows)))
        max_fw = max(forward_windows)

        dates = self._complete_dates()
        if len(dates) < max_fw + 2:
            result.error = f"完整交易日不足: {len(dates)}"
            return result
        idx = {d: i for i, d in enumerate(dates)}

        records: list[dict] = []   # {date, code, regime, r<fw>...}
        for d in dates:
            if not (start <= d <= end):
                continue
            i = idx[d]
            if i + 1 + max_fw >= len(dates):   # 需要买入日 + 最长持有窗都有完整数据
                continue
            buy_date = dates[i + 1]

            samples = self._signal_samples(entry, d)
            if not samples:
                continue

            regime = self._safe_regime(d)
            signal_close = self._prices(d, "close")
            buy_open = self._prices(buy_date, "open")
            sell_close = {w: self._prices(dates[i + 1 + w], "close") for w in forward_windows}

            for code in samples:
                bp = buy_open.get(code)
                sc = signal_close.get(code)
                if not bp or bp <= 0:
                    continue
                # 买入日一字/高开涨停 → 实盘买不进，剔除
                if sc and sc > 0 and (bp / sc - 1.0) >= self.limit_up_gap:
                    continue
                rec = {"date": d, "code": code, "regime": regime}
                has_any = False
                for w in forward_windows:
                    sp = sell_close[w].get(code)
                    if sp and sp > 0:
                        rec[f"r{w}"] = bp and (sp / bp - 1.0)
                        has_any = True
                    else:
                        rec[f"r{w}"] = np.nan
                if has_any:
                    records.append(rec)

        result.n_signals = len(records)
        if not records:
            result.error = "无有效触发样本"
            return result

        # ── 整体窗口统计 ──
        for w in forward_windows:
            result.windows[w] = _agg([r[f"r{w}"] for r in records], w)

        # ── 按 regime 分层 ──
        regimes: dict[str, list[dict]] = {}
        for r in records:
            regimes.setdefault(r["regime"], []).append(r)
        for rg, recs in regimes.items():
            result.by_regime[rg] = {
                w: _agg([x[f"r{w}"] for x in recs], w) for w in forward_windows
            }

        # ── 按信号日三等分看稳定性 ──
        result.by_segment = self._segment_stability(records, forward_windows, n_seg=3)
        return result

    def two_stage_gate(self, entry: EntryRule, end_date: str,
                       win_threshold: float = 0.55, long_days: int = 60,
                       short_days: int = 30, forward: int = 1,
                       min_samples: int = 20) -> GateResult:
        """决策A验收门：近 long_days 段 与 近 short_days 段 T+forward 胜率两段都过。

        替代原 IC/ICIR/win_rate(IC符号) 验收门。两段都达标才合格，单段不算，
        以抵抗短窗+小样本的多重检验偶然击穿。门槛与样本量留人工闸门。
        """
        gate = GateResult(passed=False, forward_days=forward, win_threshold=win_threshold)
        dates = [d for d in self._complete_dates() if d <= end_date]
        if len(dates) < forward + 2:
            gate.reason = f"完整交易日不足: {len(dates)}"
            return gate

        long_start = dates[max(0, len(dates) - long_days)]
        short_start = dates[max(0, len(dates) - short_days)]
        r_long = self.run(entry, long_start, end_date, (forward,), label="gate_long")
        r_short = self.run(entry, short_start, end_date, (forward,), label="gate_short")

        if r_long.windows.get(forward):
            gate.wr_long = r_long.windows[forward].win_rate
            gate.pnl_long = r_long.windows[forward].pnl_ratio
        if r_short.windows.get(forward):
            gate.wr_short = r_short.windows[forward].win_rate
        gate.n_long, gate.n_short = r_long.n_signals, r_short.n_signals
        gate.long_span = f"{long_start}~{end_date}"
        gate.short_span = f"{short_start}~{end_date}"

        if gate.n_long < min_samples or gate.n_short < min_samples:
            gate.reason = f"样本不足(长{gate.n_long}/短{gate.n_short} < {min_samples})"
            return gate
        gate.passed = gate.wr_long >= win_threshold and gate.wr_short >= win_threshold
        gate.reason = "两段达标" if gate.passed else (
            f"长段{gate.wr_long:.1%}{'≥' if gate.wr_long >= win_threshold else '<'}阈值, "
            f"短段{gate.wr_short:.1%}{'≥' if gate.wr_short >= win_threshold else '<'}阈值"
        )
        return gate

    # ── 内部 ──────────────────────────────────────────────

    def _complete_dates(self) -> list[str]:
        """完整交易日历：剔除 daily_price 当日股票数 < min_stocks 的残采日。"""
        rows = self.db.execute(
            "SELECT trade_date AS d, COUNT(DISTINCT stock_code) AS c "
            "FROM daily_price GROUP BY trade_date ORDER BY trade_date"
        )
        return [r["d"] for r in rows if r["c"] >= self.min_stocks]

    def _universe(self, date: str) -> set[str]:
        rows = self.db.execute(
            f"SELECT DISTINCT stock_code AS s FROM {self.universe_source} WHERE trade_date = ?",
            (date,),
        )
        return {r["s"] for r in rows}

    def _signal_samples(self, entry: EntryRule, date: str) -> set[str]:
        """该日满足 entry 全部因子条件的股票（限定在 universe 内）。"""
        universe = self._universe(date)
        if not universe:
            return set()
        conds = entry.conditions or []
        if not conds:
            return universe

        # 批量取该日因子值，按 snapshot 升序让最新覆盖旧
        rows = self.db.execute(
            "SELECT stock_code AS s, factor_name AS f, factor_value AS v "
            "FROM factor_values WHERE trade_date = ? ORDER BY snapshot_time ASC",
            (date,),
        )
        fv: dict[str, dict[str, float]] = {}
        for r in rows:
            fv.setdefault(r["s"], {})[r["f"]] = r["v"]

        matched = set()
        for code in universe:
            frow = fv.get(code)
            if frow is None:
                continue
            ok = True
            for c in conds:
                val = frow.get(c["factor"])
                if val is None or not _OPS.get(c["op"], lambda a, b: False)(val, c["value"]):
                    ok = False
                    break
            if ok:
                matched.add(code)
        return matched

    def _prices(self, date: str, col: str) -> dict[str, float]:
        # col 为内部固定字面量 (open/close)，无注入风险。
        # 按 snapshot 升序，dict 覆盖 → 每股保留最新快照（backfill 可能多次写入同日）。
        rows = self.db.execute(
            f"SELECT stock_code AS s, {col} AS v FROM daily_price "
            "WHERE trade_date = ? ORDER BY snapshot_time ASC",
            (date,),
        )
        return {r["s"]: r["v"] for r in rows}

    def _safe_regime(self, date: str) -> str:
        try:
            as_of = datetime.strptime(date, "%Y-%m-%d").replace(hour=15)
            if self.regime_mode == "pricing":
                return self._pricing.detect(as_of).regime
            return self._regime.detect(as_of).regime
        except Exception:
            return "unknown"

    def _segment_stability(self, records: list[dict], forward_windows: tuple[int, ...],
                           n_seg: int = 3) -> list[dict]:
        """按信号日排序三等分，每段算各窗口胜率，看是否稳定。"""
        if not records:
            return []
        ordered = sorted(records, key=lambda r: r["date"])
        size = max(1, len(ordered) // n_seg)
        segments = []
        for s in range(n_seg):
            lo = s * size
            hi = len(ordered) if s == n_seg - 1 else (s + 1) * size
            chunk = ordered[lo:hi]
            if not chunk:
                continue
            seg = {
                "seg": s + 1,
                "start": chunk[0]["date"],
                "end": chunk[-1]["date"],
                "n": len(chunk),
            }
            for w in forward_windows:
                st = _agg([x[f"r{w}"] for x in chunk], w)
                seg[f"win_rate_{w}"] = round(st.win_rate, 4)
                seg[f"avg_ret_{w}"] = round(st.avg_ret, 4)
            segments.append(seg)
        return segments


def entry_from_factor(factor: str, op: str = ">=", value: float = 0.0,
                      regime_filter: Optional[list[str]] = None) -> EntryRule:
    """单因子阈值快捷构造 EntryRule（体检 9 因子用）。"""
    return EntryRule(
        regime_filter=regime_filter or [],
        conditions=[{"factor": factor, "op": op, "value": value}],
    )
