"""
策略C v2 基本面评分器

输入: stock_code
输出: {score: int(0-100), details: {...}, pass: bool}

评分维度:
  A. 盈利能力 (25分): ROE + 净利率 + 毛利率
  B. 成长性 (25分): 营收增长 + 利润增长 + 连续加速
  C. 财务健康 (15分): 负债率 + 净利率改善
  D. 赛道/行业 (20分): 行业Tier + AI营收拉动
  E. 增持/信号 (15分): 增持 + 行业资金流入 + 正面新闻

详见: docs/strategy_c_v2_track_scoring.md
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta
from typing import Optional

# 行业Tier分类（来自赛道评分设计）
TIER1_INDUSTRIES = {"半导体", "光学光电子"}
TIER2_INDUSTRIES = {"计算机设备", "消费电子", "自动化设备", "电池"}
TIER3_INDUSTRIES = {"软件开发", "IT服务", "通信设备", "军工电子"}
PREMIUM_INDUSTRIES = {"白酒", "保险", "银行", "医疗器械", "化学制药", "医疗服务",
                      "调味品", "乳制品", "白色家电", "汽车整车", "证券"}


def _get_conn() -> sqlite3.Connection:
    """通过 daemon_db 获取数据库连接"""
    from src.trader.daemon_db import db_connection
    # db_connection is a context manager; callers should use `with db_connection() as conn:`
    # This helper exists for one-off queries inside scorer functions that receive conn externally.
    raise RuntimeError("Use daemon_db.db_connection() context manager instead")


def _row_to_dict(cursor: sqlite3.Cursor, row: tuple) -> dict:
    cols = [d[0] for d in cursor.description]
    return dict(zip(cols, row))


def _query_one(conn: sqlite3.Connection, sql: str, params: tuple = ()) -> Optional[dict]:
    cursor = conn.execute(sql, params)
    row = cursor.fetchone()
    return _row_to_dict(cursor, row) if row else None


def _query_all(conn: sqlite3.Connection, sql: str, params: tuple = ()) -> list[dict]:
    cursor = conn.execute(sql, params)
    return [_row_to_dict(cursor, r) for r in cursor.fetchall()]


def get_industry(conn: sqlite3.Connection, stock_code: str) -> Optional[str]:
    row = _query_one(
        conn,
        "SELECT industry_name FROM stock_industry_mapping WHERE stock_code=?",
        (stock_code,)
    )
    return row["industry_name"] if row else None


def get_financials(conn: sqlite3.Connection, stock_code: str, limit: int = 4) -> list[dict]:
    return _query_all(
        conn,
        """SELECT stock_code, report_date, roe, net_margin, profit_yoy,
                  revenue_yoy, debt_ratio, gross_margin
           FROM financial_summary
           WHERE stock_code=?
           ORDER BY report_date DESC LIMIT ?""",
        (stock_code, limit)
    )


def score_profitability(financials: list[dict]) -> tuple[int, dict]:
    """A. 盈利能力评分 (满分25)

    ROE取最近年报(report_date结尾1231), 不用Q1/Q3单季度。
    - ROE > 12%: +10 | ROE 8-12%: +5 | ROE < 0: +0
    - 净利率 > 15%: +8 | 净利率 8-15%: +4 | 净利率 < 0: +0
    - 毛利率 > 40%: +7 | 毛利率 25-40%: +3 (gross_margin为NULL时跳过)
    """
    if not financials:
        return 0, {"roe_score": 0, "margin_score": 0, "gm_score": 0, "reason": "无数据"}

    annual = [f for f in financials if f["report_date"].endswith("1231")]
    f = annual[0] if annual else financials[0]

    score = 0
    details: dict = {}

    roe = f.get("roe")
    if roe is not None:
        if roe > 12:
            score += 10; details["roe_score"] = 10
        elif roe > 8:
            score += 5; details["roe_score"] = 5
        else:
            details["roe_score"] = 0
        details["roe_value"] = round(roe, 2)
    else:
        details["roe_score"] = 0

    margin = f.get("net_margin")
    if margin is not None:
        if margin > 15:
            score += 8; details["margin_score"] = 8
        elif margin > 8:
            score += 4; details["margin_score"] = 4
        else:
            details["margin_score"] = 0
        details["margin_value"] = round(margin, 2)
    else:
        details["margin_score"] = 0

    gm = f.get("gross_margin")
    if gm is not None:
        if gm > 40:
            score += 7; details["gm_score"] = 7
        elif gm > 25:
            score += 3; details["gm_score"] = 3
        else:
            details["gm_score"] = 0
        details["gm_value"] = round(gm, 2)
    else:
        details["gm_score"] = 0

    return score, details


def score_growth(financials: list[dict]) -> tuple[int, dict]:
    """B. 成长性评分 (满分25)

    - 营收yoy > 30%: +10 | 15-30%: +5 | < 0: +0
    - 利润yoy > 30%: +10 | 15-30%: +5 | < 0: +0
    - 连续2期加速: +5 (比较最近2个report_date的revenue_yoy, 后>前算加速)
    """
    if not financials:
        return 0, {"reason": "无数据"}

    score = 0
    details: dict = {}
    f = financials[0]

    rev_yoy = f.get("revenue_yoy")
    if rev_yoy is not None:
        if rev_yoy > 30:
            score += 10; details["rev_score"] = 10
        elif rev_yoy > 15:
            score += 5; details["rev_score"] = 5
        else:
            details["rev_score"] = 0
        details["rev_yoy"] = round(rev_yoy, 2)
    else:
        details["rev_score"] = 0

    profit_yoy = f.get("profit_yoy")
    if profit_yoy is not None:
        if profit_yoy > 30:
            score += 10; details["profit_score"] = 10
        elif profit_yoy > 15:
            score += 5; details["profit_score"] = 5
        else:
            details["profit_score"] = 0
        details["profit_yoy"] = round(profit_yoy, 2)
    else:
        details["profit_score"] = 0

    if len(financials) >= 2:
        rev_new = financials[0].get("revenue_yoy")
        rev_old = financials[1].get("revenue_yoy")
        if rev_new is not None and rev_old is not None and rev_new > rev_old and rev_new > 0:
            score += 5; details["accel_score"] = 5
        else:
            details["accel_score"] = 0
    else:
        details["accel_score"] = 0

    return score, details


def score_health(financials: list[dict]) -> tuple[int, dict]:
    """C. 财务健康评分 (满分15)

    - 负债率 < 40%: +8 | 40-60%: +4 | > 60%: +2
    - 净利率 > 0 且最新期 > 上期: +7 | 净利率 > 0: +4
    """
    if not financials:
        return 0, {"reason": "无数据"}

    score = 0
    details: dict = {}
    f = financials[0]

    debt = f.get("debt_ratio")
    if debt is not None:
        if debt < 40:
            score += 8; details["debt_score"] = 8
        elif debt < 60:
            score += 4; details["debt_score"] = 4
        else:
            score += 2; details["debt_score"] = 2
        details["debt_value"] = round(debt, 2)
    else:
        details["debt_score"] = 0

    if len(financials) >= 2:
        m_new = financials[0].get("net_margin")
        m_old = financials[1].get("net_margin")
        if m_new is not None and m_old is not None:
            if m_new > 0 and m_new > m_old:
                score += 7; details["improve_score"] = 7
            elif m_new > 0:
                score += 4; details["improve_score"] = 4
            else:
                details["improve_score"] = 0
        else:
            details["improve_score"] = 0
    else:
        details["improve_score"] = 0

    return score, details


def score_track(conn: sqlite3.Connection, stock_code: str, financials: list[dict]) -> tuple[int, dict]:
    """D. 赛道/行业评分 (满分20)

    行业基础分:
      Tier1(半导体/光学光电子): 15分
      Tier2(计算机设备/消费电子/自动化设备/电池): 12分
      Tier3(软件开发/IT服务): 9分
      其他: 6分
    营收加速(AI拉动): 营收yoy > 30%: +5 | 15-30%: +3
    """
    score = 0
    details: dict = {}

    industry = get_industry(conn, stock_code)
    if industry:
        if industry in TIER1_INDUSTRIES:
            score += 15; details["tier"] = "Tier1"
        elif industry in TIER2_INDUSTRIES:
            score += 12; details["tier"] = "Tier2"
        elif industry in TIER3_INDUSTRIES:
            score += 9; details["tier"] = "Tier3"
        elif industry in PREMIUM_INDUSTRIES:
            score += 12; details["tier"] = "Premium"
        else:
            score += 6; details["tier"] = "Other"
        details["industry"] = industry
    else:
        score += 6; details["tier"] = "Unknown"; details["industry"] = None

    if financials:
        rev_yoy = financials[0].get("revenue_yoy")
        if rev_yoy is not None:
            if rev_yoy > 30:
                score += 5; details["ai_pull_score"] = 5
            elif rev_yoy > 15:
                score += 3; details["ai_pull_score"] = 3
            else:
                details["ai_pull_score"] = 0
        else:
            details["ai_pull_score"] = 0
    else:
        details["ai_pull_score"] = 0

    return score, details


def score_signals(conn: sqlite3.Connection, stock_code: str) -> tuple[int, dict]:
    """E. 增持/信号评分 (满分25, 占总评10%)

    - 近30天 holder_change 中该股票有增持记录: +5分
    - 行业资金净流入(查fund_flow最近5天该行业的net_amount合计): +5分
    - 情感因子(近30天新闻加权情感, 7级评分): +0~10分
      weighted_score >= 0.5: +10 | >= 0.3: +7 | >= 0.1: +4 | <= -0.3: +0 | 其他: +2
    """
    score = 0
    details: dict = {}
    now = datetime.now()
    days_30 = (now - timedelta(days=30)).strftime("%Y-%m-%d")
    days_5 = (now - timedelta(days=5)).strftime("%Y-%m-%d")

    # 增持记录
    holders = _query_all(
        conn,
        "SELECT * FROM holder_change WHERE stock_code=? AND change_date>=? AND change_type='增持' LIMIT 1",
        (stock_code, days_30)
    )
    details["holder_score"] = 5 if holders else 0
    score += details["holder_score"]

    # 行业资金净流入
    industry = get_industry(conn, stock_code)
    if industry:
        fund = _query_one(
            conn,
            """SELECT SUM(ff.net_amount) as total_net
               FROM fund_flow ff
               JOIN stock_industry_mapping sim ON ff.stock_code = sim.stock_code
               WHERE sim.industry_name=? AND ff.trade_date>=?""",
            (industry, days_5)
        )
        if fund and fund.get("total_net") is not None:
            details["fund_net"] = round(fund["total_net"], 2)
            if fund["total_net"] > 0:
                details["fund_score"] = 5; score += 5
            else:
                details["fund_score"] = 0
        else:
            details["fund_score"] = 0
    else:
        details["fund_score"] = 0

    # 情感因子(7级评分, 来源可信度加权)
    from src.agent.sentiment_analyzer import score_sentiment_signal
    sent_pts, sent_details = score_sentiment_signal(conn, stock_code, days=30)
    details["sentiment_score_pts"] = sent_pts
    details.update(sent_details)
    score += sent_pts

    return score, details


def score_stock(stock_code: str) -> dict:
    """对单只股票进行完整基本面评分

    评分维度(原始满分110, 线性映射到100):
      A. 盈利能力 (25分): ROE + 净利率 + 毛利率
      B. 成长性 (25分): 营收增长 + 利润增长
      C. 财务健康 (15分): 负债率 + 净利率改善
      D. 赛道/行业 (20分): 行业Tier + AI营收拉动
      E. 增持/信号 (25分): 增持5 + 行业资金5 + 情感因子10~15

    Returns:
        {
            "stock_code": str,
            "score": int (0-100),
            "pass": bool (score >= 60),
            "details": {
                "A_profitability": {"score": int, ...},
                "B_growth": {"score": int, ...},
                "C_health": {"score": int, ...},
                "D_track": {"score": int, ...},
                "E_signals": {"score": int, ...}
            }
        }
    """
    from src.trader.daemon_db import db_connection

    with db_connection() as conn:
        conn.row_factory = sqlite3.Row
        financials = get_financials(conn, stock_code, limit=4)

        a_score, a_details = score_profitability(financials)
        b_score, b_details = score_growth(financials)
        c_score, c_details = score_health(financials)
        d_score, d_details = score_track(conn, stock_code, financials)
        e_score, e_details = score_signals(conn, stock_code)

    raw_total = a_score + b_score + c_score + d_score + e_score
    # 原始满分110(25+25+15+20+25), 映射到100分制
    total = round(raw_total * 100 / 110)

    return {
        "stock_code": stock_code,
        "score": total,
        "raw_score": raw_total,
        "pass": total >= 60,
        "details": {
            "A_profitability": {"score": a_score, **a_details},
            "B_growth": {"score": b_score, **b_details},
            "C_health": {"score": c_score, **c_details},
            "D_track": {"score": d_score, **d_details},
            "E_signals": {"score": e_score, **e_details},
        }
    }


# ========== 验证 ==========
if __name__ == "__main__":
    # 预期值基于实际数据计算:
    # - E_signals全部为0: 近期无增持/行业资金净流出/无正面新闻
    # - 宁德时代: 毛利率24.4%(<25%门槛), 营收-9.7%, 负债65%, 实际应不及格
    test_cases = [
        ("600519", "贵州茅台", True, 65),    # 盈利极好25+成长10+健康15+赛道15
        ("002049", "紫光国微", True, 60),    # 半导体Tier1+高成长, 但ROE低(Q1)
        ("300750", "宁德时代", False, 35),   # 盈利可但营收降+高负债+毛利低
        ("603019", "中科曙光", False, 40),   # 概念好但业绩差
        ("000029", "ST股", False, 15),       # 全维度低分
    ]

    print(f"{'排名':>4} | {'代码':6} | {'名称':8} | {'总分':4} | {'A盈利':5} | {'B成长':5} | {'C健康':5} | {'D赛道':5} | {'E信号':5} | {'Pass':5}")
    print("-" * 85)

    results = []
    for code, name, expected_pass, expected_score in test_cases:
        r = score_stock(code)
        results.append((name, r, expected_pass, expected_score))

    results.sort(key=lambda x: x[1]["score"], reverse=True)

    for rank, (name, r, _, _) in enumerate(results, 1):
        d = r["details"]
        print(f"{rank:4} | {r['stock_code']:6} | {name:8} | {r['score']:4} | "
              f"{d['A_profitability']['score']:5} | {d['B_growth']['score']:5} | "
              f"{d['C_health']['score']:5} | {d['D_track']['score']:5} | "
              f"{d['E_signals']['score']:5} | {'Y' if r['pass'] else 'N':5}")

    # 验证检查
    print("\n========== 验证检查 ==========")
    all_pass = True
    for code, name, expected_pass, expected_score in test_cases:
        r = score_stock(code)
        in_range = abs(r["score"] - expected_score) <= 10
        pass_ok = r["pass"] == expected_pass
        status = "OK" if (in_range and pass_ok) else "WARN"
        if status == "WARN":
            all_pass = False
        print(f"  {name}({code}): 得分={r['score']} 预期={expected_score}±10 {'✓' if in_range else '✗'} "
              f"pass={r['pass']} 预期={expected_pass} {'✓' if pass_ok else '✗'} [{status}]")

    # 详细输出
    print("\n========== 详细评分 ==========")
    for code, name, _, _ in test_cases:
        r = score_stock(code)
        print(f"\n--- {name} ({code}) 总分: {r['score']} {'PASS' if r['pass'] else 'FAIL'} ---")
        for dim, info in r["details"].items():
            s = info["score"]
            detail_parts = {k: v for k, v in info.items() if k != "score"}
            print(f"  {dim}: {s}分 | {detail_parts}")

    # 验收标准检查
    print("\n========== 验收标准 ==========")
    moutai = score_stock("600519")
    st = score_stock("000029")
    ningde = score_stock("300750")
    checks = [
        (f"ST股得分 < 30: {st['score']}", st["score"] < 30),
        (f"ST股 pass=False: {st['pass']}", st["pass"] is False),
        (f"茅台得分 >= 60: {moutai['score']}", moutai["score"] >= 60),
        (f"茅台 pass=True: {moutai['pass']}", moutai["pass"] is True),
        (f"宁德 pass=False: {ningde['pass']}", ningde["pass"] is False),
    ]
    for desc, ok in checks:
        print(f"  {'PASS' if ok else 'FAIL'}: {desc}")
