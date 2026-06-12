"""daemon_db.py — 数据库操作、账户管理、持仓查询

从 trading_daemon.py 拆分出的纯数据层。
"""

from __future__ import annotations

import json
import sqlite3
import logging
import hashlib
import uuid
from datetime import datetime, date, timedelta
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.trader.daemon_config import (
    DB_PATH, CURRENT_PERIOD, INITIAL_CAPITAL,
    COMMISSION_RATE, STAMP_DUTY_RATE, MIN_COMMISSION, SLIPPAGE,
    STRATEGY_VERSION_MAP, STRATEGY_RUN_MODES,
    STRATEGY_ENTRY_RULES, STRATEGY_EXIT_RULES,
)
from src.trader.realtime_quote import get_realtime

logger = logging.getLogger("trading_daemon")


def _get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")  # WAL模式推荐, 减少写锁竞争
    return conn


from contextlib import contextmanager

@contextmanager
def db_connection():
    """Context manager for DB connections — ensures conn.close() on any exit path."""
    conn = _get_conn()
    try:
        yield conn
    finally:
        conn.close()




def init_tables():
    """创建/升级盘中交易所需的表"""
    conn = _get_conn()
    try:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS daemon_positions (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                code            TEXT NOT NULL,
                name            TEXT DEFAULT '',
                buy_time        TEXT NOT NULL,          -- 精确到秒 2026-05-12 10:35:22
                buy_date        TEXT NOT NULL,          -- 2026-05-12
                buy_price       REAL NOT NULL,
                shares          INTEGER NOT NULL,
                cost            REAL NOT NULL,          -- 含手续费
                commission      REAL DEFAULT 0,
                ml_score        REAL DEFAULT 0,
                signal_type     TEXT DEFAULT '',        -- breakout/pullback/oversold
                signal_reason   TEXT DEFAULT '',
                highest_price   REAL DEFAULT 0,
                hold_days       INTEGER DEFAULT 0,
                status          TEXT DEFAULT 'held',    -- held/closed
                sell_time       TEXT,
                sell_date       TEXT,
                sell_price      REAL DEFAULT 0,
                sell_reason     TEXT DEFAULT '',
                pnl             REAL DEFAULT 0,
                pnl_pct         REAL DEFAULT 0,
                sell_commission REAL DEFAULT 0,
                sell_stamp_duty REAL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS daemon_trades (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                code        TEXT NOT NULL,
                name        TEXT DEFAULT '',
                action      TEXT NOT NULL,      -- buy/sell
                trade_time  TEXT NOT NULL,      -- 精确到秒
                trade_date  TEXT NOT NULL,
                price       REAL NOT NULL,
                shares      INTEGER NOT NULL,
                amount      REAL NOT NULL,
                commission  REAL DEFAULT 0,
                stamp_duty  REAL DEFAULT 0,
                reason      TEXT DEFAULT '',
                signal_type TEXT DEFAULT '',
                ml_score    REAL DEFAULT 0,
                pnl         REAL DEFAULT 0,
                pnl_pct     REAL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS daemon_account (
                date            TEXT PRIMARY KEY,
                cash            REAL NOT NULL,
                market_value    REAL DEFAULT 0,
                total_assets    REAL NOT NULL,
                daily_pnl       REAL DEFAULT 0,
                cumulative_pnl  REAL DEFAULT 0,
                total_trades    INTEGER DEFAULT 0,
                win_trades      INTEGER DEFAULT 0,
                positions_count INTEGER DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS daemon_log (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                log_time    TEXT NOT NULL,
                log_level   TEXT DEFAULT 'INFO',
                module      TEXT DEFAULT '',
                message     TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS daemon_candidate_snapshots (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                snapshot_time TEXT NOT NULL,
                trade_date    TEXT NOT NULL,
                period        INTEGER DEFAULT 1,
                strategy      TEXT DEFAULT '',
                code          TEXT NOT NULL,
                name          TEXT DEFAULT '',
                score         REAL DEFAULT 0,
                signal_type   TEXT DEFAULT '',
                reason        TEXT DEFAULT '',
                raw_json      TEXT DEFAULT ''
            );

            CREATE UNIQUE INDEX IF NOT EXISTS idx_candidate_snapshot_daily
            ON daemon_candidate_snapshots(trade_date, period, strategy, code);

            CREATE TABLE IF NOT EXISTS daemon_exit_observations (
                id                      INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at              TEXT NOT NULL,
                sell_date               TEXT NOT NULL,
                code                    TEXT NOT NULL,
                name                    TEXT DEFAULT '',
                strategy                TEXT DEFAULT '',
                sell_reason             TEXT DEFAULT '',
                sell_price              REAL DEFAULT 0,
                buy_price               REAL DEFAULT 0,
                shares                  INTEGER DEFAULT 0,
                pnl_pct_at_sell         REAL DEFAULT 0,
                highest_price_before_sell REAL DEFAULT 0,
                market_phase            TEXT DEFAULT '',
                raw_json                TEXT DEFAULT '',
                future_checked_until    TEXT,
                future_max_ret_1d       REAL,
                future_close_ret_1d     REAL,
                future_max_ret_2d       REAL,
                future_close_ret_2d     REAL,
                future_max_ret_3d       REAL,
                future_close_ret_3d     REAL
            );

            CREATE TABLE IF NOT EXISTS daemon_shadow_signals (
                id                 INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at         TEXT NOT NULL,
                trade_date         TEXT NOT NULL,
                period             INTEGER DEFAULT 1,
                strategy_code      TEXT DEFAULT '',
                strategy_version   TEXT DEFAULT '',
                run_mode           TEXT DEFAULT 'shadow',
                entry_rule_id      TEXT DEFAULT '',
                exit_rule_id       TEXT DEFAULT '',
                code               TEXT NOT NULL,
                name               TEXT DEFAULT '',
                trigger_price      REAL DEFAULT 0,
                candidate_score    REAL DEFAULT 0,
                signal_type        TEXT DEFAULT '',
                signal_reason      TEXT DEFAULT '',
                market_phase       TEXT DEFAULT '',
                decision           TEXT DEFAULT 'shadow',
                block_reason       TEXT DEFAULT '',
                raw_json           TEXT DEFAULT '',
                future_checked_until TEXT,
                future_max_ret_1d  REAL,
                future_close_ret_1d REAL,
                future_max_ret_2d  REAL,
                future_close_ret_2d REAL,
                future_max_ret_3d  REAL,
                future_close_ret_3d REAL
            );

            CREATE UNIQUE INDEX IF NOT EXISTS idx_shadow_signal_daily
            ON daemon_shadow_signals(trade_date, period, strategy_version, code);

            CREATE TABLE IF NOT EXISTS strategy_performance_daily (
                id                INTEGER PRIMARY KEY AUTOINCREMENT,
                trade_date         TEXT NOT NULL,
                period             INTEGER DEFAULT 1,
                strategy_code      TEXT DEFAULT '',
                strategy_version   TEXT DEFAULT '',
                run_mode           TEXT DEFAULT '',
                trades             INTEGER DEFAULT 0,
                wins               INTEGER DEFAULT 0,
                losses             INTEGER DEFAULT 0,
                win_rate           REAL DEFAULT 0,
                gross_profit       REAL DEFAULT 0,
                gross_loss         REAL DEFAULT 0,
                profit_factor      REAL DEFAULT 0,
                expectancy_pct     REAL DEFAULT 0,
                avg_win_pct        REAL DEFAULT 0,
                avg_loss_pct       REAL DEFAULT 0,
                max_loss_pct       REAL DEFAULT 0,
                net_pnl            REAL DEFAULT 0,
                recommendation     TEXT DEFAULT '',
                created_at         TEXT NOT NULL
            );

            CREATE UNIQUE INDEX IF NOT EXISTS idx_strategy_perf_daily
            ON strategy_performance_daily(trade_date, period, strategy_version);

            CREATE TABLE IF NOT EXISTS strategy_definitions (
                strategy_version  TEXT PRIMARY KEY,
                strategy_code     TEXT NOT NULL,
                entry_rule_id     TEXT DEFAULT '',
                exit_rule_id      TEXT DEFAULT '',
                definition_json   TEXT NOT NULL,
                config_hash       TEXT NOT NULL,
                created_at        TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS daemon_runs (
                run_id                  TEXT PRIMARY KEY,
                started_at              TEXT NOT NULL,
                stopped_at              TEXT,
                pid                     INTEGER,
                period                  INTEGER DEFAULT 1,
                risk_mode               TEXT DEFAULT '',
                status                  TEXT DEFAULT 'starting',
                git_commit              TEXT DEFAULT '',
                config_hash             TEXT DEFAULT '',
                strategy_snapshot_json  TEXT DEFAULT ''
            );
        """)

        # 升级: 补充缺失列
        for table, col, col_type in [
            ("daemon_positions", "highest_price", "REAL DEFAULT 0"),
            ("daemon_positions", "signal_type", "TEXT DEFAULT ''"),
            ("daemon_positions", "signal_reason", "TEXT DEFAULT ''"),
            ("daemon_positions", "sell_commission", "REAL DEFAULT 0"),
            ("daemon_positions", "sell_stamp_duty", "REAL DEFAULT 0"),
            ("daemon_positions", "period", "INTEGER DEFAULT 1"),
            ("daemon_positions", "strategy_code", "TEXT DEFAULT ''"),
            ("daemon_positions", "strategy_version", "TEXT DEFAULT ''"),
            ("daemon_positions", "run_mode", "TEXT DEFAULT 'paper'"),
            ("daemon_positions", "entry_rule_id", "TEXT DEFAULT ''"),
            ("daemon_positions", "exit_rule_id", "TEXT DEFAULT ''"),
            ("daemon_positions", "candidate_score", "REAL DEFAULT 0"),
            ("daemon_positions", "market_phase", "TEXT DEFAULT ''"),
            ("daemon_account", "period", "INTEGER DEFAULT 1"),
            ("daemon_trades", "period", "INTEGER DEFAULT 1"),
            ("daemon_trades", "strategy_code", "TEXT DEFAULT ''"),
            ("daemon_trades", "strategy_version", "TEXT DEFAULT ''"),
            ("daemon_trades", "run_mode", "TEXT DEFAULT 'paper'"),
            ("daemon_trades", "entry_rule_id", "TEXT DEFAULT ''"),
            ("daemon_trades", "exit_rule_id", "TEXT DEFAULT ''"),
            ("daemon_trades", "candidate_score", "REAL DEFAULT 0"),
            ("daemon_trades", "market_phase", "TEXT DEFAULT ''"),
            ("daemon_candidate_snapshots", "strategy_version", "TEXT DEFAULT ''"),
            ("daemon_candidate_snapshots", "run_mode", "TEXT DEFAULT ''"),
            ("daemon_candidate_snapshots", "entry_rule_id", "TEXT DEFAULT ''"),
            # P1a: run_id + config_hash 贯穿
            ("daemon_positions", "run_id", "TEXT DEFAULT ''"),
            ("daemon_positions", "config_hash", "TEXT DEFAULT ''"),
            ("daemon_trades", "run_id", "TEXT DEFAULT ''"),
            ("daemon_trades", "config_hash", "TEXT DEFAULT ''"),
            ("daemon_candidate_snapshots", "run_id", "TEXT DEFAULT ''"),
            ("daemon_candidate_snapshots", "config_hash", "TEXT DEFAULT ''"),
            ("daemon_shadow_signals", "run_id", "TEXT DEFAULT ''"),
            ("daemon_shadow_signals", "config_hash", "TEXT DEFAULT ''"),
        ]:
            existing = {r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}
            if col not in existing:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {col_type}")

        conn.commit()
    finally:
        conn.close()


def get_strategy_metadata(strategy_code: str) -> dict:
    """Return stable strategy metadata used for accounting and promotion rules."""
    code = (strategy_code or "A").upper()
    return {
        "strategy_code": code,
        "strategy_version": STRATEGY_VERSION_MAP.get(code, f"{code}_unknown"),
        "run_mode": STRATEGY_RUN_MODES.get(code, "shadow"),
        "entry_rule_id": STRATEGY_ENTRY_RULES.get(code, ""),
        "exit_rule_id": STRATEGY_EXIT_RULES.get(code, ""),
    }


# ---------------------------------------------------------------------------
# 策略定义注册
# ---------------------------------------------------------------------------


def _compute_config_hash(definition: dict) -> str:
    """Deterministic SHA-256 of a normalized JSON definition."""
    canonical = json.dumps(definition, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(canonical.encode()).hexdigest()


def register_strategy_definition(
    strategy_version: str, strategy_code: str,
    entry_rule_id: str, exit_rule_id: str,
    definition: dict,
) -> str:
    """Register a strategy definition. Returns config_hash.

    Idempotent: same definition produces same hash, no-op on re-register.
    Raises ValueError if same version with different hash already exists.
    """
    config_hash = _compute_config_hash(definition)
    definition_json = json.dumps(definition, sort_keys=True, ensure_ascii=False)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    conn = _get_conn()
    try:
        existing = conn.execute(
            "SELECT config_hash, definition_json FROM strategy_definitions "
            "WHERE strategy_version = ?",
            (strategy_version,),
        ).fetchone()
        if existing:
            if existing["config_hash"] == config_hash:
                return config_hash  # idempotent
            try:
                if json.loads(existing["definition_json"]) == definition:
                    return existing["config_hash"]
            except (TypeError, ValueError, json.JSONDecodeError):
                pass
            raise ValueError(
                f"策略定义冲突: {strategy_version} 已存在 hash={existing['config_hash'][:12]}, "
                f"新 hash={config_hash[:12]}"
            )
        conn.execute(
            "INSERT INTO strategy_definitions "
            "(strategy_version, strategy_code, entry_rule_id, exit_rule_id, "
            " definition_json, config_hash, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (strategy_version, strategy_code, entry_rule_id, exit_rule_id,
             definition_json, config_hash, now),
        )
        conn.commit()
        return config_hash
    finally:
        conn.close()


def build_strategy_snapshot() -> dict:
    """Return the normalized strategy and execution configuration."""
    from src.trader.daemon_config import (
        STRATEGY_VERSION_MAP, STRATEGY_RUN_MODES,
        STRATEGY_ENTRY_RULES, STRATEGY_EXIT_RULES,
        SELL_PARAMS, STRATEGY_C_CONFIG, STRATEGY_B_CONFIG,
        RISK_MODE, CURRENT_PERIOD, INITIAL_CAPITAL,
        A_INITIAL_CAPITAL, A_MAX_POSITIONS, A_POSITION_RATIO,
        B_INITIAL_CAPITAL, B_MAX_POSITIONS, B_POSITION_RATIO,
        C_INITIAL_CAPITAL, C_MAX_POSITIONS, C_POSITION_RATIO,
        MIN_CASH_RATIO, MAX_SINGLE_RATIO, DAILY_LOSS_LIMIT,
        EBB_COOLDOWN_MINUTES,
    )
    from src.agent.trading_brain import BUY_THRESHOLD, PASS_THRESHOLD

    strategies = {}
    for code, version in STRATEGY_VERSION_MAP.items():
        entry_rule = STRATEGY_ENTRY_RULES.get(code, "")
        exit_rule = STRATEGY_EXIT_RULES.get(code, "")
        run_mode = STRATEGY_RUN_MODES.get(code, "shadow")
        sell_params = SELL_PARAMS.get(code, {})

        definition = {
            "strategy_code": code,
            "strategy_version": version,
            "run_mode": run_mode,
            "entry_rule_id": entry_rule,
            "exit_rule_id": exit_rule,
            "sell_params": sell_params,
        }
        if code == "C":
            definition["strategy_c_config"] = STRATEGY_C_CONFIG
        elif code == "B":
            definition["strategy_b_config"] = STRATEGY_B_CONFIG
        strategies[code] = definition

    return {
        "risk_mode": RISK_MODE,
        "period": CURRENT_PERIOD,
        "strategies": strategies,
        "portfolio": {
            "initial_capital": INITIAL_CAPITAL,
            "A": {
                "initial_capital": A_INITIAL_CAPITAL,
                "max_positions": A_MAX_POSITIONS,
                "position_ratio": A_POSITION_RATIO,
            },
            "B": {
                "initial_capital": B_INITIAL_CAPITAL,
                "max_positions": B_MAX_POSITIONS,
                "position_ratio": B_POSITION_RATIO,
            },
            "C": {
                "initial_capital": C_INITIAL_CAPITAL,
                "max_positions": C_MAX_POSITIONS,
                "position_ratio": C_POSITION_RATIO,
            },
            "min_cash_ratio": MIN_CASH_RATIO,
            "max_single_ratio": MAX_SINGLE_RATIO,
        },
        "risk": {
            "daily_loss_limit": DAILY_LOSS_LIMIT,
            "ebb_cooldown_minutes": EBB_COOLDOWN_MINUTES,
        },
        "trading_brain": {
            "buy_threshold": BUY_THRESHOLD,
            "pass_threshold": PASS_THRESHOLD,
        },
    }


def register_all_strategy_definitions() -> str:
    """Register all strategies and return the complete runtime config hash."""
    snapshot = build_strategy_snapshot()

    for code, definition in snapshot["strategies"].items():
        version = definition["strategy_version"]
        entry_rule = definition["entry_rule_id"]
        exit_rule = definition["exit_rule_id"]
        ch = register_strategy_definition(version, code, entry_rule, exit_rule, definition)

    return _compute_config_hash(snapshot)


# ---------------------------------------------------------------------------
# Daemon 运行账本
# ---------------------------------------------------------------------------


def create_daemon_run(pid: int, config_hash: str,
                      strategy_snapshot: str = "") -> str:
    """Create a new daemon_runs row. Returns run_id."""
    import os
    import subprocess as _sp

    run_id = str(uuid.uuid4())
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    git_commit = ""
    try:
        r = _sp.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True, text=True, timeout=5,
        )
        if r.returncode == 0:
            git_commit = r.stdout.strip()
    except Exception:
        pass

    from src.trader.daemon_config import RISK_MODE

    if not strategy_snapshot:
        strategy_snapshot = json.dumps(
            build_strategy_snapshot(),
            sort_keys=True,
            ensure_ascii=False,
            separators=(",", ":"),
        )

    conn = _get_conn()
    try:
        stale_rows = conn.execute(
            "SELECT run_id, pid FROM daemon_runs WHERE status IN ('starting', 'running')"
        ).fetchall()
        for stale in stale_rows:
            stale_pid = stale["pid"]
            if not stale_pid:
                is_stale = True
            else:
                try:
                    os.kill(stale_pid, 0)
                    is_stale = False
                except ProcessLookupError:
                    is_stale = True
                except PermissionError:
                    is_stale = False
            if is_stale:
                conn.execute(
                    "UPDATE daemon_runs SET status='crashed', stopped_at=? WHERE run_id=?",
                    (now, stale["run_id"]),
                )
        conn.execute(
            "INSERT INTO daemon_runs "
            "(run_id, started_at, pid, period, risk_mode, status, "
            " git_commit, config_hash, strategy_snapshot_json) "
            "VALUES (?, ?, ?, ?, ?, 'starting', ?, ?, ?)",
            (run_id, now, pid, CURRENT_PERIOD,
             RISK_MODE, git_commit, config_hash, strategy_snapshot),
        )
        conn.commit()
        return run_id
    finally:
        conn.close()


def update_daemon_run(run_id: str, status: str) -> None:
    """Update daemon_runs status. Sets stopped_at when terminal."""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn = _get_conn()
    try:
        if status in ("stopped", "crashed"):
            conn.execute(
                "UPDATE daemon_runs SET status = ?, stopped_at = ? WHERE run_id = ?",
                (status, now, run_id),
            )
        else:
            conn.execute(
                "UPDATE daemon_runs SET status = ? WHERE run_id = ?",
                (status, run_id),
            )
        conn.commit()
    finally:
        conn.close()


# Module-level run context for the current daemon process
_current_run_id: str | None = None
_current_config_hash: str | None = None


def get_current_run_id() -> str:
    """Return current daemon run_id, or empty string if not in daemon context."""
    return _current_run_id or ""


def get_current_config_hash() -> str:
    """Return current config_hash, or empty string if not in daemon context."""
    return _current_config_hash or ""


def set_run_context(run_id: str, config_hash: str) -> None:
    """Set the module-level run context (called once at daemon startup)."""
    global _current_run_id, _current_config_hash
    _current_run_id = run_id
    _current_config_hash = config_hash


def infer_strategy_code(signal_type: str = "", candidate: dict | None = None) -> str:
    """Infer A/B/C from candidate metadata or legacy signal text."""
    if candidate and candidate.get("_strategy"):
        return str(candidate["_strategy"]).upper()
    sig = signal_type or ""
    if "首阴" in sig or "策略A" in sig:
        return "A"
    if "C1" in sig or "关注度动量" in sig:
        return "C1"
    if "C2" in sig or "恐慌反转" in sig:
        return "C2"
    if "趋势牛股" in sig or "基本面" in sig or "策略C" in sig:
        return "C"
    if "暴跌日狙击" in sig or "回踩低吸" in sig or "低开反弹" in sig or "策略B" in sig:
        return "B"
    return "A"


def record_shadow_signal(
    candidate: dict, quote: dict | None, buy_signal: dict | None,
    market: dict | None, decision: str = "shadow", block_reason: str = ""
) -> None:
    """Persist a non-paper hypothesis signal for later forward-return review."""
    strategy_code = infer_strategy_code(
        (buy_signal or {}).get("signal_type", ""),
        candidate,
    )
    meta = get_strategy_metadata(strategy_code)
    now = datetime.now()
    trade_date = now.strftime("%Y-%m-%d")
    q = quote or {}
    signal = buy_signal or {}
    code = candidate.get("code") or q.get("code")
    if not code:
        return

    raw = {
        "candidate": candidate,
        "quote": q,
        "buy_signal": signal,
        "market": market or {},
    }
    conn = _get_conn()
    try:
        conn.execute(
            """
            INSERT INTO daemon_shadow_signals
            (created_at, trade_date, period, strategy_code, strategy_version,
             run_mode, entry_rule_id, exit_rule_id, code, name, trigger_price,
             candidate_score, signal_type, signal_reason, market_phase,
             decision, block_reason, raw_json, run_id, config_hash)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(trade_date, period, strategy_version, code) DO UPDATE SET
                created_at=excluded.created_at,
                run_mode=excluded.run_mode,
                name=excluded.name,
                trigger_price=excluded.trigger_price,
                candidate_score=excluded.candidate_score,
                signal_type=excluded.signal_type,
                signal_reason=excluded.signal_reason,
                market_phase=excluded.market_phase,
                decision=excluded.decision,
                block_reason=excluded.block_reason,
                raw_json=excluded.raw_json,
                run_id=excluded.run_id,
                config_hash=excluded.config_hash
            """,
            (
                now.strftime("%Y-%m-%d %H:%M:%S"),
                trade_date,
                CURRENT_PERIOD,
                meta["strategy_code"],
                meta["strategy_version"],
                meta["run_mode"],
                meta["entry_rule_id"],
                meta["exit_rule_id"],
                code,
                q.get("name", candidate.get("name", code)),
                float(q.get("price", candidate.get("price", 0)) or 0),
                float(candidate.get("score", 0) or 0),
                signal.get("signal_type", candidate.get("signal_type", "")),
                signal.get("signal_reason", candidate.get("reason", "")),
                (market or {}).get("phase", ""),
                decision,
                block_reason,
                json.dumps(raw, ensure_ascii=False, default=str),
                get_current_run_id(),
                get_current_config_hash(),
            ),
        )
        conn.commit()
    except Exception as e:
        logger.debug(f"shadow信号保存失败: {e}")
    finally:
        conn.close()


def get_shadow_signals_by_date(
    trade_date: str, strategy_code: str | None = None
) -> list[dict]:
    """Query shadow signals for a given trade_date, optionally filtered by strategy."""
    conn = _get_conn()
    try:
        if strategy_code:
            rows = conn.execute(
                "SELECT * FROM daemon_shadow_signals "
                "WHERE trade_date = ? AND strategy_code = ? ORDER BY created_at",
                (trade_date, strategy_code),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM daemon_shadow_signals "
                "WHERE trade_date = ? ORDER BY created_at",
                (trade_date,),
            ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def upsert_strategy_performance(
    trade_date: str, strategy_code: str, strategy_version: str,
    run_mode: str = "shadow", trades: int = 0, wins: int = 0, losses: int = 0,
    win_rate: float = 0, gross_profit: float = 0, gross_loss: float = 0,
    profit_factor: float = 0, expectancy_pct: float = 0,
    avg_win_pct: float = 0, avg_loss_pct: float = 0,
    max_loss_pct: float = 0, net_pnl: float = 0, recommendation: str = "",
) -> None:
    """Insert or update a daily strategy performance summary row."""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn = _get_conn()
    try:
        conn.execute(
            """
            INSERT INTO strategy_performance_daily
            (trade_date, period, strategy_code, strategy_version, run_mode,
             trades, wins, losses, win_rate, gross_profit, gross_loss,
             profit_factor, expectancy_pct, avg_win_pct, avg_loss_pct,
             max_loss_pct, net_pnl, recommendation, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(trade_date, period, strategy_version) DO UPDATE SET
                run_mode=excluded.run_mode,
                trades=excluded.trades,
                wins=excluded.wins,
                losses=excluded.losses,
                win_rate=excluded.win_rate,
                gross_profit=excluded.gross_profit,
                gross_loss=excluded.gross_loss,
                profit_factor=excluded.profit_factor,
                expectancy_pct=excluded.expectancy_pct,
                avg_win_pct=excluded.avg_win_pct,
                avg_loss_pct=excluded.avg_loss_pct,
                max_loss_pct=excluded.max_loss_pct,
                net_pnl=excluded.net_pnl,
                recommendation=excluded.recommendation,
                created_at=excluded.created_at
            """,
            (
                trade_date, CURRENT_PERIOD, strategy_code, strategy_version,
                run_mode, trades, wins, losses, win_rate, gross_profit,
                gross_loss, profit_factor, expectancy_pct, avg_win_pct,
                avg_loss_pct, max_loss_pct, net_pnl, recommendation, now,
            ),
        )
        conn.commit()
    except Exception as e:
        logger.warning(f"策略绩效写入失败: {e}")
    finally:
        conn.close()



def insert_exit_observation(
    sell_date: str, code: str, name: str, strategy: str,
    sell_reason: str, sell_price: float, buy_price: float, shares: int,
    pnl_pct_at_sell: float, highest_price_before_sell: float,
    market_phase: str = "", raw_json: str = ""
) -> None:
    """卖出时记录退出观察点, 用于事后分析是否卖早了

    仅对以下sell_reason类型记录:
    - trailing止盈 / 移动止盈 / 退潮相关止盈
    future字段先留NULL, 由 analyze_exit_observations.py 回填
    """
    from datetime import datetime as _dt
    conn = _get_conn()
    try:
        conn.execute("""
            INSERT INTO daemon_exit_observations
            (created_at, sell_date, code, name, strategy, sell_reason,
             sell_price, buy_price, shares, pnl_pct_at_sell,
             highest_price_before_sell, market_phase, raw_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            _dt.now().strftime("%Y-%m-%d %H:%M:%S"),
            sell_date, code, name, strategy, sell_reason,
            sell_price, buy_price, shares, pnl_pct_at_sell,
            highest_price_before_sell, market_phase, raw_json,
        ))
        conn.commit()
    except Exception as e:
        logger.warning(f"退出观察记录失败: {e}")
    finally:
        conn.close()

def save_candidate_snapshots(candidates: list[dict]) -> None:
    """Persist one candidate snapshot per code/strategy/day for later review.

    The daemon scans frequently, so this uses upsert to keep the latest snapshot
    without flooding the database.
    """
    if not candidates:
        return

    now = datetime.now()
    trade_date = now.strftime("%Y-%m-%d")
    snapshot_time = now.strftime("%Y-%m-%d %H:%M:%S")

    conn = _get_conn()
    try:
        for c in candidates:
            # B策略快照过滤: 只记录通过数据验证的候选, 无效数据不污染策略样本
            if c.get("_strategy") == "B" and not c.get("_data_validated"):
                continue
            code = c.get("code")
            if not code:
                continue
            strategy = c.get("_strategy", "")
            meta = get_strategy_metadata(strategy or infer_strategy_code(candidate=c))
            raw = json.dumps(c, ensure_ascii=False, default=str)
            conn.execute(
                """
                INSERT INTO daemon_candidate_snapshots
                (snapshot_time, trade_date, period, strategy, code, name,
                 score, signal_type, reason, raw_json, strategy_version,
                 run_mode, entry_rule_id, run_id, config_hash)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(trade_date, period, strategy, code) DO UPDATE SET
                    snapshot_time=excluded.snapshot_time,
                    name=excluded.name,
                    score=excluded.score,
                    signal_type=excluded.signal_type,
                    reason=excluded.reason,
                    raw_json=excluded.raw_json,
                    strategy_version=excluded.strategy_version,
                    run_mode=excluded.run_mode,
                    entry_rule_id=excluded.entry_rule_id,
                    run_id=excluded.run_id,
                    config_hash=excluded.config_hash
                """,
                (
                    snapshot_time,
                    trade_date,
                    CURRENT_PERIOD,
                    strategy,
                    code,
                    c.get("name", ""),
                    float(c.get("score", 0) or 0),
                    c.get("signal_type", ""),
                    c.get("reason", ""),
                    raw,
                    meta["strategy_version"],
                    meta["run_mode"],
                    meta["entry_rule_id"],
                    get_current_run_id(),
                    get_current_config_hash(),
                ),
            )
        conn.commit()
    except Exception as e:
        logger.debug(f"候选快照保存失败: {e}")
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 账户管理
# ---------------------------------------------------------------------------


def get_account(conn: sqlite3.Connection | None = None) -> dict:
    """获取账户状态, 不存在则初始化
    
    Args:
        conn: 可选的外部连接, 传入时复用该连接(避免嵌套连接死锁)
    """
    _own_conn = conn is None
    if _own_conn:
        conn = _get_conn()
    try:
        row = conn.execute(
            "SELECT * FROM daemon_account WHERE period = ? AND date <= ? ORDER BY date DESC LIMIT 1",
            (CURRENT_PERIOD, date.today().isoformat(),)
        ).fetchone()

        if row is None:
            today = date.today().isoformat()
            conn.execute(
                "INSERT OR IGNORE INTO daemon_account (date, cash, market_value, total_assets, period) "
                "VALUES (?, ?, 0, ?, ?)",
                (today, INITIAL_CAPITAL, INITIAL_CAPITAL, CURRENT_PERIOD),
            )
            if _own_conn:
                conn.commit()
            return {
                "date": today, "cash": INITIAL_CAPITAL,
                "market_value": 0.0, "total_assets": INITIAL_CAPITAL,
                "daily_pnl": 0.0, "cumulative_pnl": 0.0,
                "total_trades": 0, "win_trades": 0, "positions_count": 0,
            }
        return dict(row)
    finally:
        if _own_conn:
            conn.close()




def _update_account_value():
    """每轮扫描后更新持仓市值和总资产"""
    conn = _get_conn()
    try:
        positions = conn.execute(
            "SELECT code, shares FROM daemon_positions WHERE status='held' AND period=?",
            (CURRENT_PERIOD,)
        ).fetchall()
        # 复用同一连接, 避免嵌套连接死锁
        acct = get_account(conn=conn)
        if not positions:
            # 无持仓, 市值为0
            conn.execute(
                "UPDATE daemon_account SET market_value=0, total_assets=cash WHERE date=?",
                (acct["date"],)
            )
            conn.commit()
            return

        # 批量获取实时价格
        codes = [p["code"] for p in positions]
        quotes = get_realtime(codes)
        market_value = 0.0
        for p in positions:
            q = quotes.get(p["code"], {})
            price = q.get("price", 0)
            market_value += price * p["shares"]

        total = acct["cash"] + market_value
        # 今日盈亏 = 今天总资产 - 昨天总资产
        today = date.today().isoformat()
        yesterday_total = _get_yesterday_total(conn, today)
        daily_pnl = total - yesterday_total if yesterday_total > 0 else 0
        conn.execute(
            """UPDATE daemon_account
               SET market_value=?, total_assets=?, positions_count=?, daily_pnl=?
               WHERE date=?""",
            (round(market_value, 2), round(total, 2), len(positions), round(daily_pnl, 2), today)
        )
        conn.commit()
    except Exception as e:
        logger.warning(f"更新市值失败: {e}")
    finally:
        conn.close()




def get_held_positions() -> list[dict]:
    """获取当前持仓"""
    conn = _get_conn()
    try:
        rows = conn.execute(
            "SELECT * FROM daemon_positions WHERE status = 'held' AND period = ? ORDER BY buy_time",
            (CURRENT_PERIOD,)
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 交易执行
# ---------------------------------------------------------------------------


def _calc_commission(amount: float, is_sell: bool = False) -> tuple[float, float]:
    commission = max(amount * COMMISSION_RATE, MIN_COMMISSION)
    stamp_duty = amount * STAMP_DUTY_RATE if is_sell else 0.0
    return round(commission, 2), round(stamp_duty, 2)




def _calc_shares(price: float, max_amount: float) -> int:
    """计算可买股数(100股整数倍)"""
    if price <= 0:
        return 0
    return int(max_amount / price / 100) * 100




def _log_to_db(level: str, module: str, message: str):
    """记录日志到数据库"""
    conn = _get_conn()
    try:
        conn.execute("""
            INSERT INTO daemon_log (log_time, log_level, module, message)
            VALUES (?, ?, ?, ?)
        """, (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), level, module, message))
        conn.commit()
    except Exception as e:
        logger.warning(f"日志写入失败: {e}")
    finally:
        conn.close()




def _is_new_day() -> bool:
    """检查今天是否是新的交易日(需要重置日损)"""
    conn = _get_conn()
    try:
        row = conn.execute("SELECT date FROM daemon_account WHERE period = ? ORDER BY date DESC LIMIT 1", (CURRENT_PERIOD,)).fetchone()
        if row is None:
            return True
        return row["date"] != date.today().isoformat()
    finally:
        conn.close()


def _get_yesterday_total(conn, today: str) -> float:
    """获取前一交易日的total_assets, 用于计算今日盈亏

    如果没有前一天记录(period第一天), 返回0(日盈亏=0)
    """
    try:
        r = conn.execute(
            "SELECT total_assets FROM daemon_account "
            "WHERE period = ? AND date < ? ORDER BY date DESC LIMIT 1",
            (CURRENT_PERIOD, today),
        ).fetchone()
        return float(r[0]) if r else 0
    except Exception:
        return 0


def _reset_daily_pnl():
    """新交易日重置日损 — 插入今天的账户快照"""
    conn = _get_conn()
    try:
        today = date.today().isoformat()
        # 检查今天是否已有记录
        row = conn.execute("SELECT date FROM daemon_account WHERE period = ? AND date = ?", (CURRENT_PERIOD, today,)).fetchone()
        if row:
            conn.execute("UPDATE daemon_account SET daily_pnl = 0 WHERE period = ? AND date = ?", (CURRENT_PERIOD, today,))
        else:
            acct = get_account()
            conn.execute("""
                INSERT OR REPLACE INTO daemon_account (date, cash, market_value, total_assets, daily_pnl, cumulative_pnl, total_trades, win_trades, positions_count, period)
                VALUES (?, ?, ?, ?, 0, ?, ?, ?, ?, ?)
            """, (today, acct["cash"], acct.get("market_value", 0), acct.get("total_assets", acct["cash"]),
                  acct.get("cumulative_pnl", 0), acct.get("total_trades", 0), acct.get("win_trades", 0), acct.get("positions_count", 0), CURRENT_PERIOD))
        conn.commit()
    finally:
        conn.close()




def _count_trading_days(buy_date: str, current_date: str = None) -> int:
    """计算buy_date到current_date之间的交易日数(不含buy_date本身)

    返回值语义: 持仓天数。buy_date当天返回0(买入当天不算持有), 次交易日返回1。
    用途: 判断是否可卖(hold_days>=1)及是否到期(hold_days>=max_hold_days)。
    注意: 如果today不是交易日(周末/假日), 结果不会包含today, hold_days不变。
    """
    if current_date is None:
        current_date = datetime.now().strftime('%Y-%m-%d')
    conn = _get_conn()
    try:
        row = conn.execute(
            "SELECT COUNT(DISTINCT trade_date) FROM daily_price "
            "WHERE trade_date > ? AND trade_date <= ?",
            (buy_date[:10], current_date)
        ).fetchone()
        count = row[0] if row else 0

        # 盘中补偿: daily_price可能滞后(收盘后才跑collect_daily)
        # 如果current_date比daily_price最新日期还新，说明今天的数据还没入库
        # daemon盘中运行=今天是交易日，需要把今天算进去
        latest = conn.execute(
            "SELECT MAX(trade_date) FROM daily_price"
        ).fetchone()
        if latest and latest[0] and current_date > latest[0]:
            count += 1  # 今天是交易日(daemon在运行)，数据还没入库

        return count
    finally:
        conn.close()
