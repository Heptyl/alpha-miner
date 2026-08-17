"""每日审视简报生成器 — 只读 SQLite/JSONL，产出自包含静态 HTML。

设计铁律（见 alpha-miner_审视简报UI_CC实施brief.md）：
- 只读：本脚本永不写 DB、不改任何系统状态，唯一写入是 reports/brief/ 下的 HTML。
- 诚实：数据缺失就显式标注缺失，禁止占位假数据。
- 自包含：产出 HTML 无任何外部资源引用（含 svg xmlns），断网可开。

用法：
    uv run python scripts/generate_brief.py [--date YYYY-MM-DD]
"""

from __future__ import annotations

import argparse
import json
import shutil
import sqlite3
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, select_autoescape
from markupsafe import Markup

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.factors.naming import get_naming  # noqa: E402  (依赖 ROOT 入 sys.path)


@dataclass
class BriefConfig:
    db_path: str = str(ROOT / "data" / "alpha_miner.db")
    mining_log_path: str = str(ROOT / "data" / "mining_log.jsonl")
    candidate_pool_path: str = str(ROOT / "data" / "candidate_pool.jsonl")
    out_dir: str = str(ROOT / "reports" / "brief")
    reports_dir: str = str(ROOT / "reports")
    template_dir: str = str(ROOT / "templates")

    # 漂移判定：短窗胜率较长窗下滑超过 drift_drop_pp 个百分点 → 告警
    drift_short_window: int = 20
    drift_long_window: int = 60
    drift_drop_pp: float = 8.0

    # 数据滞后超过 N 个交易日（按工作日近似）→ 区块变红
    stale_red_trading_days: int = 1
    # 挖掘管线静默超过 N 天 → 告警卡
    mining_silence_days: int = 7

    # 自动预检（v1 只做三条，宁缺毋滥）
    precheck_min_sample: int = 100
    precheck_corr_threshold: float = 0.6
    precheck_segment_min_winrate: float = 0.50

    # 存活因子口径：窗口内 IC 符号胜率 > 55%
    alive_win_rate: float = 0.55
    max_cards: int = 5


# 关键表与其日期列；expected_lag=True 表示结构性滞后（不计入红色规则）
KEY_TABLES: list[tuple[str, str, bool]] = [
    ("daily_price", "trade_date", False),
    ("zt_pool", "trade_date", False),
    ("zb_pool", "trade_date", False),
    ("lhb_detail", "trade_date", False),
    ("fund_flow", "trade_date", False),
    ("news", "publish_time", False),
    ("factor_values", "trade_date", False),
    ("ic_series", "trade_date", True),   # IC 需要次日收益，天然滞后
    ("regime_state", "trade_date", False),
    ("concept_daily", "trade_date", False),
]

# 第 0 步审计结论（2026-06-11）：信号链路 → 数据源映射，作为常驻初始内容
SIGNAL_CHAIN = [
    {"factor": "narrative_velocity（叙事速度）",
     "source": "news 表 — akshare stock_news_em 新闻文本 + 规则分类",
     "quality": "低",
     "note": "唯一真正消费新闻文本的因子；该表覆盖少、noise 占比高、日量波动大"},
    {"factor": "theme_lifecycle（题材生命周期）",
     "source": "concept_daily + concept_mapping（量价/涨停行为数据）",
     "quality": "中", "note": "名为叙事，实际消费行为数据"},
    {"factor": "theme_crowding / leader_clarity",
     "source": "zt_pool + concept_mapping（涨停行为数据）",
     "quality": "中", "note": "名为叙事，实际消费行为数据"},
    {"factor": "次日信号（SignalEngine）",
     "source": "zt_pool + factor_values + concept_mapping + daily_price",
     "quality": "—", "note": "候选只从当日涨停池产生，权重硬编码"},
    {"factor": "日报候选（DailyBrief）",
     "source": "上述 + ic_series + regime_state",
     "quality": "—", "note": "因子健康度依赖 ic_series，注意其滞后"},
]


# ----------------------------------------------------------------------
# 基础工具
# ----------------------------------------------------------------------

def _weekday_lag(latest: str, today: date) -> int | None:
    """latest（YYYY-MM-DD...）到 today 之间的工作日数，近似交易日滞后。"""
    try:
        d = date.fromisoformat(str(latest)[:10])
    except (ValueError, TypeError):
        return None
    if d >= today:
        return 0
    lag, cur = 0, d + timedelta(days=1)
    while cur <= today:
        if cur.weekday() < 5:
            lag += 1
        cur += timedelta(days=1)
    return lag


def _pearson(xs: list[float], ys: list[float]) -> float | None:
    n = min(len(xs), len(ys))
    if n < 10:
        return None
    xs, ys = xs[-n:], ys[-n:]
    mx, my = sum(xs) / n, sum(ys) / n
    sxy = sum((a - mx) * (b - my) for a, b in zip(xs, ys))
    sxx = sum((a - mx) ** 2 for a in xs)
    syy = sum((b - my) ** 2 for b in ys)
    if sxx <= 0 or syy <= 0:
        return None
    return sxy / (sxx * syy) ** 0.5


def _win_rate(vals: list[float]) -> float:
    return sum(1 for v in vals if v > 0) / len(vals) if vals else 0.0


def _sparkline(vals: list[float], w: int = 140, h: int = 26) -> str:
    """内联 SVG 折线（无 xmlns，保持 HTML 自包含）。"""
    if len(vals) < 2:
        return ""
    lo, hi = min(vals), max(vals)
    span = (hi - lo) or 1.0
    pts = " ".join(
        f"{i * w / (len(vals) - 1):.1f},{h - 2 - (v - lo) / span * (h - 4):.1f}"
        for i, v in enumerate(vals)
    )
    zero_y = h - 2 - (0 - lo) / span * (h - 4)
    zero = (f'<line x1="0" y1="{zero_y:.1f}" x2="{w}" y2="{zero_y:.1f}" '
            f'stroke="#ccc" stroke-width="1"/>') if lo <= 0 <= hi else ""
    return (f'<svg class="spark" width="{w}" height="{h}">{zero}'
            f'<polyline points="{pts}" fill="none" stroke="#3a6ea5" '
            f'stroke-width="1.5"/></svg>')


def _read_jsonl(path: str) -> list[dict]:
    p = Path(path)
    if not p.exists():
        return []
    out = []
    with open(p, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return out


# ----------------------------------------------------------------------
# 数据采集（全部只读，缺什么如实报什么）
# ----------------------------------------------------------------------

def _table_health(conn: sqlite3.Connection, today: date,
                  cfg: BriefConfig) -> tuple[list[dict], bool, bool]:
    """返回 (各表状态, 是否整体变红, 是否 DB 为空)。"""
    existing = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'")}
    rows_out, red, any_data = [], False, False

    for name, datecol, expected_lag in KEY_TABLES:
        if name not in existing:
            rows_out.append({"name": name, "rows": "—", "latest": "表不存在",
                             "lag_text": "数据缺失", "status": "bad"})
            red = True
            continue
        n = conn.execute(f"SELECT COUNT(*) FROM [{name}]").fetchone()[0]
        if n == 0:
            rows_out.append({"name": name, "rows": 0, "latest": "空表",
                             "lag_text": "数据缺失", "status": "bad"})
            red = True
            continue
        any_data = True
        latest = conn.execute(
            f"SELECT MAX(substr([{datecol}],1,10)) FROM [{name}]").fetchone()[0]
        lag = _weekday_lag(latest, today)
        if lag is None:
            rows_out.append({"name": name, "rows": n, "latest": str(latest),
                             "lag_text": "日期不可解析", "status": "warn"})
            continue
        if expected_lag:
            status = "ok" if lag <= 5 else "warn"
            lag_text = f"{lag} 个工作日（结构性滞后，预期内）" if lag else "0"
        elif lag > cfg.stale_red_trading_days:
            status, lag_text = "bad", f"{lag} 个工作日 ⚠"
            red = True
        elif lag == cfg.stale_red_trading_days:
            status, lag_text = "warn", f"{lag} 个工作日"
        else:
            status, lag_text = "ok", "最新"
        rows_out.append({"name": name, "rows": n, "latest": str(latest),
                         "lag_text": lag_text, "status": status})

    return rows_out, red, not any_data


def _news_stats(conn: sqlite3.Connection) -> dict | None:
    try:
        days = conn.execute(
            "SELECT substr(publish_time,1,10) d, COUNT(*), COUNT(DISTINCT stock_code) "
            "FROM news GROUP BY d ORDER BY d DESC LIMIT 10").fetchall()
        if not days:
            return None
        total, noise = conn.execute(
            "SELECT COUNT(*), SUM(CASE WHEN news_type='noise' THEN 1 ELSE 0 END) "
            "FROM news").fetchone()
        return {
            "avg_per_day": round(sum(r[1] for r in days) / len(days)),
            "avg_stocks": round(sum(r[2] for r in days) / len(days)),
            "noise_pct": f"{(noise or 0) / total:.0%}" if total else "—",
        }
    except sqlite3.Error:
        return None


def _load_ic_series(conn: sqlite3.Connection) -> dict[str, list[float]]:
    """按因子取 IC 序列（时间升序）。多 forward_days 时取最小的那个口径。"""
    try:
        fds = [r[0] for r in conn.execute(
            "SELECT DISTINCT forward_days FROM ic_series "
            "WHERE forward_days IS NOT NULL").fetchall()]
        where = f"WHERE forward_days = {min(fds)}" if fds else ""
        rows = conn.execute(
            f"SELECT factor_name, trade_date, ic_value FROM ic_series {where} "
            "ORDER BY factor_name, trade_date").fetchall()
    except sqlite3.Error:
        return {}
    out: dict[str, list[float]] = {}
    for fname, _, ic in rows:
        if ic is not None:
            out.setdefault(fname, []).append(float(ic))
    return out


def _collect_last_run(cfg: BriefConfig) -> str:
    """从产出物时间戳推断流水线上次运行（无统一退出状态记录，只能推断）。"""
    candidates: list[tuple[float, str]] = []
    db = Path(cfg.db_path)
    if db.exists():
        candidates.append((db.stat().st_mtime, "DB 文件"))
    rep = Path(cfg.reports_dir)
    if rep.exists():
        txts = sorted(rep.glob("*.txt"), key=lambda p: p.stat().st_mtime)
        if txts:
            candidates.append((txts[-1].stat().st_mtime, f"日报 {txts[-1].name}"))
    if not candidates:
        return "无法推断（无产出物）"
    ts, src = max(candidates)
    when = datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M")
    return f"{when}（依据 {src} 时间戳推断）"


# ----------------------------------------------------------------------
# 待决策事件（4.2）
# ----------------------------------------------------------------------

def _precheck_flags(evaluation: dict, ic_by_factor: dict[str, list[float]],
                    cfg: BriefConfig) -> list[str]:
    """三条自动预检。TODO: regime 分层预检（依赖尚不存在的 pricing_regime 标签）。"""
    flags: list[str] = []

    # 1. 样本量
    ss = evaluation.get("sample_size")
    if ss is None:
        flags.append("样本量无记录")
    elif ss < cfg.precheck_min_sample:
        flags.append(f"样本不足（{ss} < {cfg.precheck_min_sample}）")

    cand_ic = [float(v) for v in (evaluation.get("ic_series") or [])]

    # 2. 因子冗余（与现有因子 IC 序列相关性）
    if len(cand_ic) >= 10 and ic_by_factor:
        for fname, series in ic_by_factor.items():
            c = _pearson(cand_ic, series)
            if c is not None and c > cfg.precheck_corr_threshold:
                flags.append(f"疑似换皮（与 {fname} 相关性 {c:.2f}）")
                break
    else:
        flags.append("冗余检查不可用（IC 序列不足）")

    # 3. 三段稳定性
    if len(cand_ic) >= 15:
        seg = len(cand_ic) // 3
        for i in range(3):
            part = cand_ic[i * seg:(i + 1) * seg] if i < 2 else cand_ic[2 * seg:]
            wr = _win_rate(part)
            if wr < cfg.precheck_segment_min_winrate:
                flags.append(f"不稳定（第 {i + 1} 段胜率 {wr:.0%}）")
                break
    else:
        flags.append("稳定性检查不可用（IC 序列不足 15 点）")

    return flags


def _candidate_cards(cfg: BriefConfig, ic_by_factor: dict[str, list[float]]) -> list[dict]:
    """候选池中 pending 的因子 → 待放行卡片。"""
    pool = _read_jsonl(cfg.candidate_pool_path)
    state: dict[str, dict] = {}
    for entry in pool:               # 同名后写覆盖先写（与 CandidatePool 一致）
        if entry.get("name"):
            state[entry["name"]] = entry
    pending = [e for e in state.values() if e.get("status") == "pending"]
    if not pending:
        return []

    # 用挖掘日志里同名最后一条记录补评估数据
    log_by_name: dict[str, dict] = {}
    for rec in _read_jsonl(cfg.mining_log_path):
        if rec.get("name"):
            log_by_name[rec["name"]] = rec

    cards = []
    for entry in pending:
        name = entry["name"]
        rec = log_by_name.get(name, {})
        ev = rec.get("evaluation") or {}
        flags = _precheck_flags(ev, ic_by_factor, cfg)
        hypothesis = (entry.get("config") or {}).get("prediction") \
            or (rec.get("config") or {}).get("prediction") or "（假说无记录）"
        wr = ev.get("win_rate")
        ss = ev.get("sample_size")
        why = (f"假说：{hypothesis}。回测胜率 {f'{wr:.0%}' if wr is not None else '无记录'}"
               f" / 盈亏比 无记录 / 样本 {ss if ss is not None else '无记录'}；"
               f"观察池进度 {entry.get('days_passed', 0)}/5 天通过，"
               f"连续失败 {entry.get('days_failed', 0)}/3 天")
        cards.append({
            "title": f"候选因子待放行: {name}",
            "severity": "yellow",
            "flags": flags,
            "what": f"进化引擎产出候选因子并送入观察池（入池 {entry.get('entry_date', '?')}，"
                    f"最近核查 {entry.get('check_date', '?')}）",
            "why": why,
            "actions": [
                f"uv run python -m cli.mine surgery --factor {name}   # 解剖 IC 序列，诊断有效性来源",
                "uv run python -m cli.mine history   # 查看完整挖掘记录",
                "# 注：当前无人工放行 CLI，观察池按 5 天通过/3 天连败自动晋升或淘汰",
            ],
        })
    return cards


def _drift_cards(ic_by_factor: dict[str, list[float]], today: date,
                 cfg: BriefConfig) -> tuple[list[dict], int, str | None]:
    """漂移告警卡。返回 (卡片, 放过数, 数据不足说明)。"""
    cards, passed, insufficient = [], 0, []
    for name, series in sorted(ic_by_factor.items()):
        if len(series) < cfg.drift_long_window:
            insufficient.append((name, len(series)))
            continue
        long_wr = _win_rate(series[-cfg.drift_long_window:])
        short_wr = _win_rate(series[-cfg.drift_short_window:])
        drop = (long_wr - short_wr) * 100
        if drop > cfg.drift_drop_pp:
            cards.append({
                "title": f"因子漂移: {get_naming().label(name)}",
                "severity": "red",
                "flags": [],
                "what": f"对 {name} 的 IC 符号胜率做了短窗/长窗对比"
                        f"（{cfg.drift_short_window} 日 vs {cfg.drift_long_window} 日）",
                "why": f"近 {cfg.drift_short_window} 日胜率 {short_wr:.0%}，"
                       f"较近 {cfg.drift_long_window} 日的 {long_wr:.0%} "
                       f"下滑 {drop:.0f} 个百分点（阈值 {cfg.drift_drop_pp:.0f}pp）"
                       f"，该因子可能正在失效",
                "actions": [
                    f"uv run python -m cli.mine surgery --factor {name}   # 解剖 IC 序列",
                    f"# 确认失效后可在 config/factors.yaml 中将 {name} 的 role 改为 filter 或移除",
                ],
            })
        else:
            passed += 1
    note = None
    if insufficient:
        worst = max(n for _, n in insufficient)
        note = (f"另有 {len(insufficient)} 个因子 IC 历史不足 {cfg.drift_long_window} 日"
                f"（当前最多 {worst} 点），漂移判定不可用——如实跳过，不降级判定。")
    return cards, passed, note


def _failure_cards(cfg: BriefConfig, today: date,
                   stale_tables: list[str]) -> tuple[list[dict], int]:
    """采集/挖掘任务失败（由产出物推断）。"""
    cards, passed = [], 0

    # 挖掘管线静默
    log = _read_jsonl(cfg.mining_log_path)
    if not log:
        cards.append({
            "title": "挖掘日志缺失或为空",
            "severity": "red", "flags": [],
            "what": f"检查了挖掘日志 {Path(cfg.mining_log_path).name}",
            "why": "文件不存在或无任何记录，无法确认进化引擎是否在工作",
            "actions": ["uv run python -m cli.mine evolve --generations 3 --population 5"],
        })
    else:
        try:
            last_ts = datetime.fromisoformat(log[-1].get("timestamp", ""))
            silence = (today - last_ts.date()).days
        except (ValueError, TypeError):
            last_ts, silence = None, None
        if silence is not None and silence > cfg.mining_silence_days:
            cards.append({
                "title": f"挖掘管线静默 {silence} 天",
                "severity": "red", "flags": [],
                "what": "检查了挖掘日志的最后一条记录时间",
                "why": f"最后一条挖掘记录在 {last_ts.date().isoformat()}，距今 {silence} 天"
                       f"（阈值 {cfg.mining_silence_days} 天）。进化引擎可能早已停摆，"
                       f"且历史记录全部 accepted=false",
                "actions": [
                    "uv run python -m cli.mine evolve --generations 3 --population 5",
                    "tail -20 logs/mining_cron.log   # 查看上次失败原因（曾出现 evolve exit=1）",
                ],
            })
        else:
            passed += 1

    # 采集滞后（同时已在 4.1 变红，这里给出可执行动作）
    if stale_tables:
        cards.append({
            "title": "数据采集疑似未运行",
            "severity": "red", "flags": [],
            "what": "比对了各关键表最新数据日期与今天",
            "why": f"以下表滞后超过 {cfg.stale_red_trading_days} 个交易日："
                   f"{'、'.join(stale_tables)}（按工作日近似，未剔除节假日）",
            "actions": [
                "uv run python -m cli.collect --today",
            ],
        })
    else:
        passed += 1

    return cards, passed


# ----------------------------------------------------------------------
# 系统行为快照（4.3）
# ----------------------------------------------------------------------

def _pytest_text() -> str:
    """读最近一次 pytest 缓存产出；无则如实显示未运行。"""
    cache = ROOT / ".pytest_cache" / "v" / "cache"
    nodeids = cache / "nodeids"
    if not nodeids.exists():
        return "未运行（无 pytest 产出）"
    try:
        n_total = len(json.loads(nodeids.read_text(encoding="utf-8")))
        lf = cache / "lastfailed"
        n_fail = len(json.loads(lf.read_text(encoding="utf-8"))) if lf.exists() else 0
        if n_fail:
            return f"上次运行 {n_fail} 个失败 / 收集 {n_total} 项"
        return f"上次收集 {n_total} 项，无失败记录"
    except (json.JSONDecodeError, OSError):
        return "产出不可解析"


def _snapshot(conn: sqlite3.Connection | None, ic_by_factor: dict[str, list[float]],
              max_stale_text: str, cfg: BriefConfig) -> dict:
    regime_text = "无数据"
    if conn is not None:
        try:
            row = conn.execute(
                "SELECT trade_date, regime_type, confidence FROM regime_state "
                "ORDER BY trade_date DESC LIMIT 1").fetchone()
            if row:
                regime_text = f"{row[1]}（{row[0]}，置信度 {row[2]:.0%}）"
        except sqlite3.Error:
            pass

    factors, alive = [], 0
    for name, series in sorted(ic_by_factor.items()):
        window = min(cfg.drift_long_window, len(series))
        wr = _win_rate(series[-window:])
        if wr > cfg.alive_win_rate:
            alive += 1
        factors.append({
            "name": get_naming().label(name),
            "win_text": f"{wr:.0%}（近 {window} 日）",
            "points": len(series),
            "spark": Markup(_sparkline(series[-30:])),
        })
    if factors:
        full = all(f["points"] >= cfg.drift_long_window for f in factors)
        alive_text = f"{alive} / {len(factors)}（胜率 > {cfg.alive_win_rate:.0%}）"
        if not full:
            alive_text += " · 历史不足 60 日，按可用窗口计"
    else:
        alive_text = "无 IC 数据"

    return {"regime_text": regime_text, "alive_text": alive_text,
            "pytest_text": _pytest_text(), "fresh_text": max_stale_text,
            "factors": factors}


# ----------------------------------------------------------------------
# 主流程
# ----------------------------------------------------------------------

def generate(cfg: BriefConfig, today: date | None = None) -> Path:
    today = today or date.today()
    out_dir = Path(cfg.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # 上一期间隔（在写入本期之前扫描）
    prev = sorted(p for p in out_dir.glob("????-??-??.html"))
    if prev:
        prev_date = date.fromisoformat(prev[-1].stem)
        gap = (today - prev_date).days
        gap_text = f"距上期（{prev_date.isoformat()}）{gap} 天" if gap > 0 else "今日已生成过，本次覆盖"
    else:
        gap_text = "首期简报"

    # ---- 读 DB（只读模式）----
    conn = None
    db_exists = Path(cfg.db_path).exists()
    if db_exists:
        conn = sqlite3.connect(f"file:{cfg.db_path}?mode=ro", uri=True)

    if conn is not None:
        tables, health_red, db_empty = _table_health(conn, today, cfg)
        news = _news_stats(conn)
        ic_by_factor = _load_ic_series(conn)
    else:
        tables = [{"name": n, "rows": "—", "latest": "DB 文件不存在",
                   "lag_text": "数据缺失", "status": "bad"} for n, _, _ in KEY_TABLES]
        health_red, db_empty, news, ic_by_factor = True, True, None, {}

    stale_tables = [t["name"] for t in tables if t["status"] == "bad"
                    and "工作日" in str(t["lag_text"])]

    # ---- 待决策事件 ----
    cards: list[dict] = []
    m_passed = 0
    drift_note = None
    if not db_empty:
        cards += _candidate_cards(cfg, ic_by_factor)
        d_cards, d_passed, drift_note = _drift_cards(ic_by_factor, today, cfg)
        cards += d_cards
        m_passed += d_passed
        f_cards, f_passed = _failure_cards(cfg, today, stale_tables)
        cards += f_cards
        m_passed += f_passed
        m_passed += sum(1 for t in tables if t["status"] == "ok")

    n_pending = len(cards)
    cards_truncated = max(0, n_pending - cfg.max_cards)
    cards = cards[:cfg.max_cards]

    # ---- 置顶警告 ----
    top_alerts = []
    if db_empty and db_exists:
        pass  # banner 已覆盖
    if not db_exists:
        top_alerts.append(f"DB 文件不存在：{cfg.db_path}")
    if stale_tables and not db_empty:
        top_alerts.append(
            f"关键表数据滞后超过 {cfg.stale_red_trading_days} 个交易日："
            f"{'、'.join(stale_tables)} — 以下全部区块的可信度受损")

    # ---- 快照 ----
    bad_lags = [t for t in tables if t["status"] == "bad"]
    if db_empty:
        fresh_text = "数据缺失"
    elif bad_lags:
        fresh_text = f"{len(bad_lags)} 张关键表滞后/缺失"
    else:
        fresh_text = "正常（≤1 个交易日）"
    snapshot = _snapshot(conn, ic_by_factor if not db_empty else {}, fresh_text, cfg)

    health = {
        "red": health_red,
        "tables": tables,
        "collect_text": _collect_last_run(cfg),
        "chain": SIGNAL_CHAIN if not db_empty else [],
        "news": news,
        "lag_note": "滞后按工作日近似计算，未剔除法定节假日；ic_series 因需要次日收益属结构性滞后。",
    }

    if conn is not None:
        conn.close()

    # ---- 渲染 ----
    env = Environment(loader=FileSystemLoader(cfg.template_dir),
                      autoescape=select_autoescape(["html", "j2"]))
    html = env.get_template("brief.html.j2").render(
        report_date=today.isoformat(),
        generated_at=datetime.now().strftime("%Y-%m-%d %H:%M"),
        gap_text=gap_text,
        n_pending=n_pending,
        m_passed=m_passed,
        db_empty=db_empty,
        top_alerts=top_alerts,
        stale_threshold=cfg.stale_red_trading_days,
        health=health,
        cards=cards,
        cards_truncated=cards_truncated,
        drift_note=drift_note,
        snapshot=snapshot,
    )

    dated = out_dir / f"{today.isoformat()}.html"
    dated.write_text(html, encoding="utf-8")
    shutil.copyfile(dated, out_dir / "latest.html")
    print(f"[brief] written: {dated}")          # ASCII only: GBK console safety
    print(f"[brief] latest:  {out_dir / 'latest.html'}")
    return dated


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate daily review brief (read-only)")
    parser.add_argument("--date", type=str, default=None, help="YYYY-MM-DD, default today")
    parser.add_argument("--db", type=str, default=None, help="override db path")
    args = parser.parse_args()
    cfg = BriefConfig()
    if args.db:
        cfg.db_path = args.db
    generate(cfg, date.fromisoformat(args.date) if args.date else None)


if __name__ == "__main__":
    main()
