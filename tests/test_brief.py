"""每日审视简报的行为验收测试 — 对应实施 brief 第 6 节的 5 条标准。

简报是只读产物：测试只构造 fixture 数据源（SQLite + JSONL），
调用 generate() 后对产出 HTML 做字符串断言，不验证内部实现。
"""

import json
import sqlite3
import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.generate_brief import BriefConfig, generate

# 固定"今天"为周二，保证交易日滞后计算可预期
TODAY = date(2026, 6, 9)


# ----------------------------------------------------------------------
# fixture 构造
# ----------------------------------------------------------------------

def _last_weekdays(end: date, n: int) -> list[str]:
    """返回截至 end（含）的最近 n 个工作日，升序。"""
    days: list[str] = []
    d = end
    while len(days) < n:
        if d.weekday() < 5:
            days.append(d.isoformat())
        d -= timedelta(days=1)
    return list(reversed(days))


def _create_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE daily_price (trade_date TEXT, stock_code TEXT, close REAL);
        CREATE TABLE zt_pool (trade_date TEXT, stock_code TEXT);
        CREATE TABLE zb_pool (trade_date TEXT, stock_code TEXT);
        CREATE TABLE lhb_detail (trade_date TEXT, stock_code TEXT);
        CREATE TABLE fund_flow (trade_date TEXT, stock_code TEXT);
        CREATE TABLE news (publish_time TEXT, stock_code TEXT, news_type TEXT);
        CREATE TABLE factor_values (trade_date TEXT, stock_code TEXT,
                                    factor_name TEXT, factor_value REAL);
        CREATE TABLE ic_series (factor_name TEXT, trade_date TEXT,
                                ic_value REAL, forward_days INTEGER,
                                snapshot_time TEXT);
        CREATE TABLE regime_state (trade_date TEXT, regime_type TEXT,
                                   confidence REAL);
        CREATE TABLE concept_daily (trade_date TEXT, concept_name TEXT);
        """
    )


def _insert_ic(conn: sqlite3.Connection, factor: str, signs: list[int]) -> None:
    """按符号序列写入 ic_series，幅度固定，日期为最近 len(signs) 个工作日。"""
    dates = _last_weekdays(TODAY, len(signs))
    rows = [
        (factor, d, 0.05 * s if s > 0 else -0.04, 1, f"{d} 16:00:00")
        for d, s in zip(dates, signs)
    ]
    conn.executemany("INSERT INTO ic_series VALUES (?,?,?,?,?)", rows)


def _signs(n: int, win_rate: float, period: int = 20) -> list[int]:
    """确定性符号序列：每 period 个点中前 round(period*win_rate) 个为正。"""
    k = round(period * win_rate)
    return [1 if i % period < k else -1 for i in range(n)]


def _make_normal_db(db_path: Path, drift: bool = False) -> None:
    conn = sqlite3.connect(db_path)
    _create_schema(conn)
    today_s = TODAY.isoformat()

    # 各关键表都有当日数据（新鲜）
    conn.executemany("INSERT INTO daily_price VALUES (?,?,?)",
                     [(d, "000001", 10.0) for d in _last_weekdays(TODAY, 90)])
    conn.execute("INSERT INTO zt_pool VALUES (?,?)", (today_s, "000001"))
    conn.execute("INSERT INTO zb_pool VALUES (?,?)", (today_s, "000002"))
    conn.execute("INSERT INTO lhb_detail VALUES (?,?)", (today_s, "000001"))
    conn.execute("INSERT INTO fund_flow VALUES (?,?)", (today_s, "000001"))
    conn.executemany("INSERT INTO news VALUES (?,?,?)", [
        (f"{today_s} 09:30:00", "000001", "noise"),
        (f"{today_s} 10:00:00", "000001", "theme_ignite"),
    ])
    conn.execute("INSERT INTO factor_values VALUES (?,?,?,?)",
                 (today_s, "000001", "stable_factor", 0.5))
    conn.execute("INSERT INTO regime_state VALUES (?,?,?)",
                 (today_s, "normal", 1.0))
    conn.execute("INSERT INTO concept_daily VALUES (?,?)", (today_s, "AI"))

    # 稳定因子：80 点，长短窗胜率都 65%，不触发漂移
    _insert_ic(conn, "stable_factor", _signs(80, 0.65))

    if drift:
        # 漂移因子：前 60 点胜率 75%，最近 20 点胜率 50%
        # → 长窗(60) ≈ 66.7%，短窗(20) = 50%，下滑 16.7pp > 阈值 8pp
        signs = _signs(60, 0.75) + _signs(20, 0.50)
        _insert_ic(conn, "drifting_factor", signs)

    conn.commit()
    conn.close()


def _write_candidate(tmp: Path, name: str, sample_size: int) -> None:
    """写入候选池 pending 条目 + 对应 mining_log 评估记录。"""
    pool_entry = {
        "name": name, "config": {"prediction": "测试假说：封板强度领先次日收益"},
        "code": "def compute(): pass", "status": "pending",
        "entry_date": TODAY.isoformat(), "check_date": TODAY.isoformat(),
        "daily_ic": 0.04, "days_checked": 2, "days_passed": 2, "days_failed": 0,
    }
    (tmp / "candidate_pool.jsonl").write_text(
        json.dumps(pool_entry, ensure_ascii=False) + "\n", encoding="utf-8")

    # 候选 IC 序列：30 点，每段 10 点中 6 正（各段胜率 60%，稳定）
    ic_series = [0.05 if i % 10 < 6 else -0.04 for i in range(30)]
    log_entry = {
        "timestamp": f"{TODAY.isoformat()}T15:00:00",
        "name": name, "source": "knowledge", "generation": 1,
        "config": {"prediction": "测试假说：封板强度领先次日收益"},
        "code": "def compute(): pass",
        "evaluation": {"ic_mean": 0.05, "icir": 0.8, "win_rate": 0.60,
                       "sample_size": sample_size, "ic_series": ic_series},
        "accepted": False, "error": None,
    }
    (tmp / "mining_log.jsonl").write_text(
        json.dumps(log_entry, ensure_ascii=False) + "\n", encoding="utf-8")


def _config(tmp: Path) -> BriefConfig:
    return BriefConfig(
        db_path=str(tmp / "test.db"),
        mining_log_path=str(tmp / "mining_log.jsonl"),
        research_ledger_path=str(tmp / "research_ledger.db"),
        out_dir=str(tmp / "brief"),
        reports_dir=str(tmp / "reports"),
    )


def _generate_html(tmp: Path) -> str:
    out = generate(_config(tmp), today=TODAY)
    return Path(out).read_text(encoding="utf-8")


# ----------------------------------------------------------------------
# 5 条行为验收
# ----------------------------------------------------------------------

def test_empty_db(tmp_path: Path):
    """1. 空库：正常退出，HTML 含「数据缺失」红色警告，无假数据。"""
    sqlite3.connect(tmp_path / "test.db").close()  # 空库，无任何表
    html = _generate_html(tmp_path)
    assert "数据缺失" in html
    assert "DB 为空" in html
    # 不得出现编造的因子健康数据
    assert "stable_factor" not in html


def test_normal_day(tmp_path: Path):
    """2. 正常日：含 4.1–4.4 全部区块，待处理计数与 fixture 事件数一致。"""
    _make_normal_db(tmp_path / "test.db")
    _write_candidate(tmp_path, "cand_alpha", sample_size=200)
    html = _generate_html(tmp_path)

    assert "数据源健康" in html      # 4.1
    assert "需要你的注意" in html    # 4.2
    assert "系统行为快照" in html    # 4.3
    assert "仓位上限 40%" in html    # 4.4 风控脚注
    # fixture 中唯一事件 = 1 个待观察候选因子
    assert "1 项待你处理" in html
    # 双产出：日期文件 + latest
    assert (tmp_path / "brief" / f"{TODAY.isoformat()}.html").exists()
    assert (tmp_path / "brief" / "latest.html").exists()


def test_drift_trigger(tmp_path: Path):
    """3. 漂移触发：短窗胜率较长窗下滑 >8pp 的因子必须进入待决策区。"""
    _make_normal_db(tmp_path / "test.db", drift=True)
    html = _generate_html(tmp_path)
    assert "因子漂移" in html
    assert "drifting_factor" in html
    # 稳定因子不应出现漂移卡
    assert "因子漂移: stable_factor" not in html


def test_legacy_candidate_pool_is_not_a_current_evidence_source(tmp_path: Path):
    """4. Retired JSON candidates cannot be rendered as ledger evidence."""
    _make_normal_db(tmp_path / "test.db")
    _write_candidate(tmp_path, "cand_small", sample_size=80)
    html = _generate_html(tmp_path)
    assert "cand_small" not in html
    assert "统一研究账本未初始化" in html
    assert "HOLDOUT_NOT_OPENED" in html


def test_brief_reads_latest_unified_development_evidence(tmp_path: Path):
    _make_normal_db(tmp_path / "test.db")
    ledger = sqlite3.connect(tmp_path / "research_ledger.db")
    ledger.executescript(
        """
        CREATE TABLE research_candidates(candidate_hash TEXT PRIMARY KEY, candidate_name TEXT);
        CREATE TABLE research_evidence(
            sequence_id INTEGER PRIMARY KEY,
            candidate_hash TEXT,
            event_type TEXT,
            payload_json TEXT
        );
        INSERT INTO research_candidates VALUES('abc', 'H1 test');
        """
    )
    ledger.execute(
        "INSERT INTO research_evidence VALUES(1,'abc','DEVELOPMENT_RESULT',?)",
        (json.dumps({"signal_days": 3, "candidate_count": 2, "filled_count": 1}),),
    )
    ledger.commit()
    ledger.close()

    html = _generate_html(tmp_path)
    assert "统一研究证据: H1 test" in html
    assert "独立信号日 3" in html
    assert "DEVELOPMENT_ONLY" in html
    assert "不能称为发现" in html


def test_brief_does_not_publish_an_opened_or_pending_holdout(tmp_path: Path):
    _make_normal_db(tmp_path / "test.db")
    ledger = sqlite3.connect(tmp_path / "research_ledger.db")
    ledger.executescript(
        """
        CREATE TABLE research_candidates(candidate_hash TEXT PRIMARY KEY, candidate_name TEXT);
        CREATE TABLE research_evidence(
            sequence_id INTEGER PRIMARY KEY, candidate_hash TEXT,
            event_type TEXT, payload_json TEXT
        );
        INSERT INTO research_candidates VALUES('abc', 'H1 test');
        INSERT INTO research_evidence VALUES(1,'abc','HOLDOUT_OPENED','{}');
        """
    )
    ledger.commit()
    ledger.close()
    html = _generate_html(tmp_path)
    assert "INCONCLUSIVE_CRASH" in html
    assert "不得重开或据此准入" in html
    assert "只解释账本，不发布玩法" in html


def test_self_contained(tmp_path: Path):
    """5. 自包含：HTML 中不得出现 http:// 或 https:// 资源引用。"""
    _make_normal_db(tmp_path / "test.db")
    _write_candidate(tmp_path, "cand_alpha", sample_size=200)
    html = _generate_html(tmp_path)
    assert "http://" not in html
    assert "https://" not in html
